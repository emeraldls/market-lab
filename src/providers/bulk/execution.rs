use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bulk_keychain::{
    Action, Cancel, Hash, OnFill, Order, OrderItem, Pubkey, RangeOco, SignedTransaction, Signer,
    Stop, TakeProfit, TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::credentials::ActiveBulkCredential;
use crate::domain::execution::{
    AccountSnapshot, ExecutionReceipt, ExecutionVenue, Fill, LeverageSetting, MarginSummary,
    OpenOrder, OrderKind, OrderRecord, OrderSide, Position, PositionDirection, TradePlan,
    VenueCapabilities,
};

use super::catalog;
use super::client::BulkClient;
use super::market_data::normalize_timestamp_ms;

static LAST_NONCE: AtomicU64 = AtomicU64::new(0);

pub struct BulkExecutionAdapter {
    client: BulkClient,
}

impl BulkExecutionAdapter {
    pub fn capabilities() -> VenueCapabilities {
        VenueCapabilities {
            venue: ExecutionVenue::Bulk,
            order_kinds: vec![OrderKind::Market, OrderKind::Limit],
            time_in_forces: vec![
                crate::domain::execution::TimeInForce::Gtc,
                crate::domain::execution::TimeInForce::Ioc,
                crate::domain::execution::TimeInForce::Alo,
            ],
            reduce_only: true,
            deterministic_order_ids: true,
            delegated_agent_signing: true,
            native_protective_triggers: true,
            native_oco: true,
            native_on_fill: true,
        }
    }

    pub fn new() -> Result<Self> {
        Ok(Self {
            client: BulkClient::new()?,
        })
    }

    pub async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        let response: Vec<FullAccountEnvelope> = self
            .client
            .post(
                "account",
                &AccountQuery {
                    query_type: "fullAccount",
                    user: account,
                },
            )
            .await?;
        let full = response
            .into_iter()
            .find_map(|entry| entry.full_account)
            .context("BULK account response omitted fullAccount")?;

        Ok(AccountSnapshot {
            venue: ExecutionVenue::Bulk,
            account: account.to_string(),
            fetched_at_ms: now_ms()?,
            margin: full.margin.into(),
            positions: full
                .positions
                .into_iter()
                .filter(|position| position.size != 0.0)
                .map(Position::try_from)
                .collect::<Result<Vec<_>>>()?,
            open_orders: full
                .open_orders
                .into_iter()
                .map(OpenOrder::try_from)
                .collect::<Result<Vec<_>>>()?,
            leverage_settings: full
                .leverage_settings
                .into_iter()
                .map(LeverageSetting::try_from)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        let response: Vec<OpenOrderEnvelope> = self
            .client
            .post(
                "account",
                &AccountQuery {
                    query_type: "openOrders",
                    user: account,
                },
            )
            .await?;
        response
            .into_iter()
            .map(|entry| {
                entry
                    .open_order
                    .context("BULK open-orders response omitted openOrder")
                    .and_then(OpenOrder::try_from)
            })
            .collect()
    }

    pub async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        let response: Vec<FillEnvelope> = self
            .client
            .post(
                "account",
                &AccountQuery {
                    query_type: "fills",
                    user: account,
                },
            )
            .await?;
        response
            .into_iter()
            .map(|entry| {
                entry
                    .fills
                    .context("BULK fills response omitted fills")
                    .and_then(|fill| fill.into_fill(account))
            })
            .collect()
    }

    pub async fn order_history(&self, account: &str) -> Result<Vec<OrderRecord>> {
        let response: Vec<OrderHistoryEnvelope> = self
            .client
            .post(
                "account",
                &AccountQuery {
                    query_type: "orderHistory",
                    user: account,
                },
            )
            .await?;
        response
            .into_iter()
            .map(|entry| {
                entry
                    .order_history
                    .context("BULK order-history response omitted orderHistory")
                    .and_then(OrderRecord::try_from)
            })
            .collect()
    }

    pub async fn submit_trade(
        &self,
        credential: ActiveBulkCredential,
        plan: &TradePlan,
    ) -> Result<ExecutionReceipt> {
        validate_trade_plan(plan)?;
        let account = credential.account;
        if account.to_base58() != plan.account {
            bail!("trade plan account no longer matches the configured BULK account");
        }
        let mut signer = Signer::new(credential.agent);

        if !plan.reduce_only {
            let leverage_action = Action::UpdateUserSettings(
                bulk_keychain::UserSettings::set_leverage(plan.venue_symbol.clone(), plan.leverage),
            );
            let leverage_tx = signer
                .sign_action(&leverage_action, next_nonce()?, &account)
                .context("failed to sign BULK leverage update")?;
            let leverage_response: Value = self.client.post("order", &leverage_tx).await?;
            validate_transaction_response(&leverage_response, "leverage update")?;
        }

        let signed = sign_trade_order(&mut signer, &account, plan, next_nonce()?)?;
        let optimistic_order_id = signed.order_id.clone();
        let response: Value = self.client.post("order", &signed).await?;
        receipt_from_response(&plan.account, optimistic_order_id, response)
    }

    pub async fn cancel_order(
        &self,
        credential: ActiveBulkCredential,
        venue_symbol: &str,
        order_id: &str,
    ) -> Result<ExecutionReceipt> {
        let account = credential.account;
        let hash = Hash::from_base58(order_id).context("invalid BULK order id")?;
        let action = Action::Order {
            orders: vec![OrderItem::Cancel(Cancel::new(venue_symbol, hash))],
        };
        let mut signer = Signer::new(credential.agent);
        let signed = signer
            .sign_action(&action, next_nonce()?, &account)
            .context("failed to sign BULK order cancellation")?;
        let response: Value = self.client.post("order", &signed).await?;
        receipt_from_response(&account.to_base58(), Some(order_id.to_string()), response)
    }
}

fn sign_trade_order(
    signer: &mut Signer,
    account: &Pubkey,
    plan: &TradePlan,
    nonce: u64,
) -> Result<SignedTransaction> {
    let mut order = match plan.order_kind {
        OrderKind::Market => Order::market(
            plan.venue_symbol.clone(),
            plan.side == OrderSide::Buy,
            plan.size,
        ),
        OrderKind::Limit => Order::limit(
            plan.venue_symbol.clone(),
            plan.side == OrderSide::Buy,
            plan.price
                .context("limit trade plan is missing its price")?,
            plan.size,
            bulk_tif(
                plan.time_in_force
                    .context("limit trade plan is missing its TIF")?,
            ),
        ),
    };
    if plan.reduce_only {
        order = order.reduce_only();
    }
    let mut orders = vec![OrderItem::Order(order)];
    let mut protection = Vec::new();
    match (plan.stop_loss_price, plan.take_profit_price) {
        (Some(stop_loss), Some(take_profit)) => {
            protection.push(OrderItem::RangeOco(RangeOco {
                symbol: plan.venue_symbol.clone(),
                is_buy: plan.direction == PositionDirection::Long,
                size: plan.size,
                collar_min: stop_loss.min(take_profit),
                collar_max: stop_loss.max(take_profit),
                limit_min: f64::NAN,
                limit_max: f64::NAN,
                iso: false,
            }));
        }
        (Some(stop_loss), None) => {
            protection.push(OrderItem::Stop(Stop {
                symbol: plan.venue_symbol.clone(),
                is_buy: plan.direction == PositionDirection::Short,
                size: plan.size,
                trigger_price: stop_loss,
                limit_price: f64::NAN,
                iso: false,
            }));
        }
        (None, Some(take_profit)) => {
            protection.push(OrderItem::TakeProfit(TakeProfit {
                symbol: plan.venue_symbol.clone(),
                is_buy: plan.direction == PositionDirection::Long,
                size: plan.size,
                trigger_price: take_profit,
                limit_price: f64::NAN,
                iso: false,
            }));
        }
        (None, None) => {}
    }
    if !protection.is_empty() {
        orders.push(OrderItem::OnFill(OnFill {
            p: 0,
            actions: protection,
        }));
    }
    signer
        .sign_action(&Action::Order { orders }, nonce, account)
        .context("failed to sign BULK order")
}

fn validate_trade_plan(plan: &TradePlan) -> Result<()> {
    if plan.venue != ExecutionVenue::Bulk {
        bail!("BULK adapter received a plan for another execution venue");
    }
    let market = catalog::market(&plan.internal_symbol)?;
    if !market.is_trading() {
        bail!("BULK market `{}` is not trading", market.symbol);
    }
    if plan.venue_symbol != market.symbol {
        bail!("trade plan symbol mapping does not match the embedded BULK catalog");
    }
    if !plan.size.is_finite() || plan.size <= 0.0 || !is_step_aligned(plan.size, market.lot_size) {
        bail!(
            "trade plan size is not aligned to BULK lot size {} for {}",
            market.lot_size,
            market.internal_symbol
        );
    }
    if !plan.leverage.is_finite()
        || plan.leverage < 1.0
        || plan.leverage > f64::from(market.max_leverage)
    {
        bail!(
            "trade plan leverage must be between 1 and {} for {}",
            market.max_leverage,
            market.internal_symbol
        );
    }
    if !plan.reference_price.is_finite() || plan.reference_price <= 0.0 {
        bail!("trade plan has an invalid reference price");
    }
    if plan.size * plan.reference_price < market.min_notional {
        bail!(
            "trade plan notional is below BULK minimum {} for {}",
            market.min_notional,
            market.internal_symbol
        );
    }
    validate_protection(plan, market.tick_size)?;
    match plan.order_kind {
        OrderKind::Market => {
            if !market.supports_order_type("MARKET") {
                bail!(
                    "BULK market `{}` does not support market orders",
                    market.symbol
                );
            }
            if plan.price.is_some() || plan.time_in_force.is_some() {
                bail!("market trade plan cannot include price or time in force");
            }
        }
        OrderKind::Limit => {
            if !market.supports_order_type("LIMIT") {
                bail!(
                    "BULK market `{}` does not support limit orders",
                    market.symbol
                );
            }
            let price = plan
                .price
                .context("limit trade plan is missing its price")?;
            if !price.is_finite() || price <= 0.0 || !is_step_aligned(price, market.tick_size) {
                bail!(
                    "trade plan price is not aligned to BULK tick size {} for {}",
                    market.tick_size,
                    market.internal_symbol
                );
            }
            let tif = plan
                .time_in_force
                .context("limit trade plan is missing its TIF")?;
            let tif = match tif {
                crate::domain::execution::TimeInForce::Gtc => "GTC",
                crate::domain::execution::TimeInForce::Ioc => "IOC",
                crate::domain::execution::TimeInForce::Alo => "ALO",
            };
            if !market
                .time_in_forces
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(tif))
            {
                bail!("BULK market `{}` does not support TIF {tif}", market.symbol);
            }
        }
    }
    Ok(())
}

fn validate_protection(plan: &TradePlan, tick_size: f64) -> Result<()> {
    if plan.reduce_only && (plan.stop_loss_price.is_some() || plan.take_profit_price.is_some()) {
        bail!("protective SL/TP cannot be attached to a reduce-only order");
    }
    let entry_price = plan.price.unwrap_or(plan.reference_price);
    for (name, price) in [
        ("stop-loss", plan.stop_loss_price),
        ("take-profit", plan.take_profit_price),
    ] {
        if let Some(price) = price
            && (!price.is_finite() || price <= 0.0 || !is_step_aligned(price, tick_size))
        {
            bail!("trade plan {name} is not aligned to BULK tick size {tick_size}");
        }
    }
    match plan.direction {
        PositionDirection::Long => {
            if plan
                .stop_loss_price
                .is_some_and(|price| price >= entry_price)
            {
                bail!("long stop-loss must be below the entry price {entry_price}");
            }
            if plan
                .take_profit_price
                .is_some_and(|price| price <= entry_price)
            {
                bail!("long take-profit must be above the entry price {entry_price}");
            }
        }
        PositionDirection::Short => {
            if plan
                .stop_loss_price
                .is_some_and(|price| price <= entry_price)
            {
                bail!("short stop-loss must be above the entry price {entry_price}");
            }
            if plan
                .take_profit_price
                .is_some_and(|price| price >= entry_price)
            {
                bail!("short take-profit must be below the entry price {entry_price}");
            }
        }
    }
    Ok(())
}

fn is_step_aligned(value: f64, step: f64) -> bool {
    let units = value / step;
    (units - units.round()).abs() <= 1e-8_f64.max(units.abs() * 1e-12)
}

fn bulk_tif(tif: crate::domain::execution::TimeInForce) -> TimeInForce {
    match tif {
        crate::domain::execution::TimeInForce::Gtc => TimeInForce::Gtc,
        crate::domain::execution::TimeInForce::Ioc => TimeInForce::Ioc,
        crate::domain::execution::TimeInForce::Alo => TimeInForce::Alo,
    }
}

fn validate_transaction_response(response: &Value, operation: &str) -> Result<()> {
    if response.get("status").and_then(Value::as_str) != Some("ok") {
        bail!("BULK rejected {operation}: {}", response_message(response));
    }
    let statuses = response
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .with_context(|| format!("BULK {operation} response omitted statuses"))?;
    if let Some(error) = statuses.iter().find_map(status_error) {
        bail!("BULK rejected {operation}: {error}");
    }
    Ok(())
}

fn receipt_from_response(
    account: &str,
    optimistic_order_id: Option<String>,
    response: Value,
) -> Result<ExecutionReceipt> {
    validate_transaction_response(&response, "order")?;
    let status = response
        .pointer("/response/data/statuses/0")
        .context("BULK order response contained no status")?;
    let (name, details) = status
        .as_object()
        .and_then(|object| object.iter().next())
        .context("BULK order response contained an invalid status")?;
    let order_id = details
        .get("oid")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or(optimistic_order_id);
    let terminal = matches!(
        name.as_str(),
        "filled"
            | "partiallyFilled"
            | "cancelled"
            | "cancelledRiskLimit"
            | "cancelledSelfCrossing"
            | "cancelledReduceOnly"
            | "cancelledIOC"
            | "rejectedCrossing"
            | "rejectedDuplicate"
            | "rejectedRiskLimit"
            | "rejectedInvalid"
            | "error"
    );
    Ok(ExecutionReceipt {
        venue: ExecutionVenue::Bulk,
        account: account.to_string(),
        order_id,
        status: name.clone(),
        terminal,
        submitted_at_ms: now_ms()?,
        raw_status: status.clone(),
    })
}

fn status_error(status: &Value) -> Option<String> {
    let object = status.as_object()?;
    let (name, details) = object.iter().next()?;
    (name == "error" || name.starts_with("rejected") || name.ends_with("Failed"))
        .then(|| response_message(details))
}

fn response_message(value: &Value) -> String {
    value
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/error/message").and_then(Value::as_str))
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn next_nonce() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos();
    let now = u64::try_from(now).context("current timestamp does not fit in a BULK nonce")?;
    let mut previous = LAST_NONCE.load(Ordering::Relaxed);
    loop {
        let candidate = now.max(previous.saturating_add(1));
        match LAST_NONCE.compare_exchange_weak(
            previous,
            candidate,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(candidate),
            Err(observed) => previous = observed,
        }
    }
}

fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("current timestamp does not fit in u64")
}

#[derive(Serialize)]
struct AccountQuery<'a> {
    #[serde(rename = "type")]
    query_type: &'a str,
    user: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FullAccountEnvelope {
    full_account: Option<BulkFullAccount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkFullAccount {
    margin: BulkMargin,
    #[serde(default)]
    positions: Vec<BulkPosition>,
    #[serde(default)]
    open_orders: Vec<BulkOpenOrder>,
    #[serde(default)]
    leverage_settings: Vec<BulkLeverageSetting>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkMargin {
    total_balance: f64,
    available_balance: f64,
    margin_used: f64,
    notional: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    fees: f64,
    funding: f64,
}

impl From<BulkMargin> for MarginSummary {
    fn from(value: BulkMargin) -> Self {
        Self {
            total_balance: value.total_balance,
            available_balance: value.available_balance,
            margin_used: value.margin_used,
            notional: value.notional,
            realized_pnl: value.realized_pnl,
            unrealized_pnl: value.unrealized_pnl,
            fees: value.fees,
            funding: value.funding,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkPosition {
    symbol: String,
    size: f64,
    price: f64,
    fair_price: f64,
    notional: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    leverage: f64,
    liquidation_price: f64,
    fees: f64,
    funding: f64,
    maintenance_margin: f64,
}

impl TryFrom<BulkPosition> for Position {
    type Error = anyhow::Error;

    fn try_from(value: BulkPosition) -> Result<Self> {
        let (internal_symbol, venue_symbol, catalog_supported) =
            normalize_account_symbol(&value.symbol)?;
        Ok(Self {
            venue: ExecutionVenue::Bulk,
            internal_symbol,
            venue_symbol,
            catalog_supported,
            direction: if value.size >= 0.0 {
                PositionDirection::Long
            } else {
                PositionDirection::Short
            },
            size: value.size.abs(),
            entry_price: value.price,
            mark_price: value.fair_price,
            notional: value.notional.abs(),
            realized_pnl: value.realized_pnl,
            unrealized_pnl: value.unrealized_pnl,
            leverage: value.leverage,
            liquidation_price: value.liquidation_price,
            fees: value.fees,
            funding: value.funding,
            maintenance_margin: value.maintenance_margin,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpenOrderEnvelope {
    open_order: Option<BulkOpenOrder>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkOpenOrder {
    #[serde(alias = "sym")]
    symbol: String,
    #[serde(alias = "oid")]
    order_id: String,
    #[serde(alias = "px")]
    price: f64,
    #[serde(alias = "origSz")]
    original_size: f64,
    #[serde(alias = "sz")]
    size: f64,
    #[serde(alias = "fillSz")]
    filled_size: f64,
    #[serde(default)]
    vwap: f64,
    #[serde(default)]
    is_buy: Option<bool>,
    #[serde(alias = "mk")]
    maker: bool,
    #[serde(alias = "r")]
    reduce_only: bool,
    tif: String,
    status: String,
    #[serde(alias = "ts")]
    timestamp: u64,
}

impl TryFrom<BulkOpenOrder> for OpenOrder {
    type Error = anyhow::Error;

    fn try_from(value: BulkOpenOrder) -> Result<Self> {
        let (internal_symbol, venue_symbol, catalog_supported) =
            normalize_account_symbol(&value.symbol)?;
        let signed_size = if value.size != 0.0 {
            value.size
        } else {
            value.original_size
        };
        let is_buy = value.is_buy.unwrap_or(signed_size >= 0.0);
        Ok(Self {
            venue: ExecutionVenue::Bulk,
            internal_symbol,
            venue_symbol,
            catalog_supported,
            order_id: value.order_id,
            side: if is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            price: value.price,
            original_size: value.original_size.abs(),
            remaining_size: value.size.abs(),
            filled_size: value.filled_size.abs(),
            vwap: value.vwap,
            maker: value.maker,
            reduce_only: value.reduce_only,
            time_in_force: value.tif,
            status: value.status,
            ts_ms: normalize_timestamp_ms(value.timestamp),
        })
    }
}

#[derive(Deserialize)]
struct FillEnvelope {
    fills: Option<BulkFill>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkFill {
    maker: String,
    taker: String,
    order_id_maker: String,
    order_id_taker: String,
    is_buy: bool,
    symbol: String,
    amount: f64,
    price: f64,
    #[serde(default, alias = "reasonCode")]
    reason: Option<BulkFillReason>,
    slot: u64,
    timestamp: u64,
}

impl BulkFill {
    fn into_fill(self, account: &str) -> Result<Fill> {
        let (internal_symbol, venue_symbol, catalog_supported) =
            normalize_account_symbol(&self.symbol)?;
        let is_maker = self.maker == account;
        let is_taker = self.taker == account;
        if !is_maker && !is_taker {
            bail!("BULK returned a fill that does not belong to account {account}");
        }
        Ok(Fill {
            venue: ExecutionVenue::Bulk,
            internal_symbol,
            venue_symbol,
            catalog_supported,
            side: if self.is_buy {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            amount: self.amount,
            price: self.price,
            reason: self
                .reason
                .map(BulkFillReason::into_display)
                .unwrap_or_else(|| "unknown".to_string()),
            order_id: Some(if is_maker {
                self.order_id_maker
            } else {
                self.order_id_taker
            }),
            maker: is_maker,
            slot: self.slot,
            ts_ms: normalize_timestamp_ms(self.timestamp),
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BulkFillReason {
    Name(String),
    Code(i64),
}

impl BulkFillReason {
    fn into_display(self) -> String {
        match self {
            Self::Name(reason) => reason,
            Self::Code(0) => "normal".to_string(),
            Self::Code(code) => format!("code:{code}"),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkLeverageSetting {
    symbol: String,
    leverage: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderHistoryEnvelope {
    order_history: Option<BulkOrderHistory>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkOrderHistory {
    order_id: String,
    symbol: String,
    side: String,
    order_type: String,
    tif: String,
    price: f64,
    vwap: f64,
    original_size: f64,
    executed_size: f64,
    reduce_only: bool,
    status: String,
    reason: Option<String>,
    slot: u64,
    timestamp: u64,
}

impl TryFrom<BulkOrderHistory> for OrderRecord {
    type Error = anyhow::Error;

    fn try_from(value: BulkOrderHistory) -> Result<Self> {
        let (internal_symbol, venue_symbol, catalog_supported) =
            normalize_account_symbol(&value.symbol)?;
        let side = match value.side.to_ascii_lowercase().as_str() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            side => bail!("BULK order history returned unknown side `{side}`"),
        };
        Ok(Self {
            venue: ExecutionVenue::Bulk,
            internal_symbol,
            venue_symbol,
            catalog_supported,
            order_id: value.order_id,
            side,
            order_kind: value.order_type,
            time_in_force: value.tif,
            price: value.price,
            vwap: value.vwap,
            original_size: value.original_size,
            executed_size: value.executed_size,
            reduce_only: value.reduce_only,
            status: value.status,
            reason: value.reason,
            slot: value.slot,
            ts_ms: normalize_timestamp_ms(value.timestamp),
        })
    }
}

impl TryFrom<BulkLeverageSetting> for LeverageSetting {
    type Error = anyhow::Error;

    fn try_from(value: BulkLeverageSetting) -> Result<Self> {
        let (internal_symbol, venue_symbol, catalog_supported) =
            normalize_account_symbol(&value.symbol)?;
        Ok(Self {
            internal_symbol,
            venue_symbol,
            catalog_supported,
            leverage: value.leverage,
        })
    }
}

fn normalize_account_symbol(symbol: &str) -> Result<(String, String, bool)> {
    if let Ok(market) = catalog::market(symbol) {
        return Ok((market.internal_symbol.clone(), market.symbol.clone(), true));
    }
    let venue_symbol = symbol.trim().to_ascii_uppercase().replace('/', "-");
    let mut parts = venue_symbol.split('-');
    let (Some(base), Some(quote), None) = (parts.next(), parts.next(), parts.next()) else {
        bail!("BULK account returned malformed symbol `{symbol}`");
    };
    if base.is_empty() || quote.is_empty() {
        bail!("BULK account returned malformed symbol `{symbol}`");
    }
    let internal_quote = if quote == "USD" { "USDT" } else { quote };
    Ok((format!("{base}/{internal_quote}"), venue_symbol, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_account_timestamps_and_symbols() {
        let order = BulkOpenOrder {
            symbol: "BTC-USD".to_string(),
            order_id: "oid".to_string(),
            price: 100_000.0,
            original_size: 0.1,
            size: 0.05,
            filled_size: 0.05,
            vwap: 100_000.0,
            is_buy: Some(true),
            maker: true,
            reduce_only: false,
            tif: "gtc".to_string(),
            status: "working".to_string(),
            timestamp: 1_699_564_800_000_000_000,
        };
        let normalized = OpenOrder::try_from(order).expect("order converts");
        assert_eq!(normalized.internal_symbol, "BTC/USDT");
        assert_eq!(normalized.ts_ms, 1_699_564_800_000);
        assert!(normalized.catalog_supported);
    }

    #[test]
    fn decodes_compact_open_order_shape() {
        let order: BulkOpenOrder = serde_json::from_str(
            r#"{
                "ot": "limit",
                "status": "resting",
                "sym": "BTC-USD",
                "oid": "oid",
                "px": 65000.0,
                "origSz": -0.001,
                "sz": -0.00075,
                "fillSz": -0.00025,
                "vwap": 65000.0,
                "tif": "gtc",
                "r": false,
                "mk": true,
                "ts": 1699564800000000000
            }"#,
        )
        .expect("compact order decodes");

        let normalized = OpenOrder::try_from(order).expect("compact order normalizes");
        assert_eq!(normalized.side, OrderSide::Sell);
        assert_eq!(normalized.original_size, 0.001);
        assert_eq!(normalized.remaining_size, 0.00075);
        assert_eq!(normalized.filled_size, 0.00025);
        assert_eq!(normalized.ts_ms, 1_699_564_800_000);
    }

    #[test]
    fn decodes_numeric_fill_reason_code() {
        let fill: BulkFill = serde_json::from_str(
            r#"{
                "maker": "account",
                "taker": "counterparty",
                "orderIdMaker": "oid",
                "orderIdTaker": "other-oid",
                "isBuy": true,
                "symbol": "BTC-USD",
                "amount": 0.001,
                "price": 65000.0,
                "reasonCode": 0,
                "slot": 123,
                "timestamp": 1699564800000000000
            }"#,
        )
        .expect("fill decodes");

        let normalized = fill.into_fill("account").expect("fill normalizes");
        assert_eq!(normalized.reason, "normal");
        assert_eq!(normalized.order_id.as_deref(), Some("oid"));
    }

    #[test]
    fn preserves_account_markets_outside_embedded_execution_catalog() {
        let (internal, venue, supported) =
            normalize_account_symbol("GOLD-USD").expect("symbol normalizes");
        assert_eq!(internal, "GOLD/USDT");
        assert_eq!(venue, "GOLD-USD");
        assert!(!supported);
    }

    #[test]
    fn agent_signs_trade_for_main_account() {
        let master = bulk_keychain::Keypair::generate();
        let account = master.pubkey();
        let agent = bulk_keychain::Keypair::generate();
        let agent_public_key = agent.pubkey().to_base58();
        let mut signer = Signer::new(agent);
        let plan = TradePlan {
            created_at_ms: 1_784_158_000_000,
            venue: ExecutionVenue::Bulk,
            account: account.to_base58(),
            internal_symbol: "BTC/USDT".to_string(),
            venue_symbol: "BTC-USD".to_string(),
            direction: PositionDirection::Long,
            side: OrderSide::Buy,
            order_kind: OrderKind::Limit,
            time_in_force: Some(crate::domain::execution::TimeInForce::Gtc),
            requested_size: Some(0.001),
            requested_notional: None,
            size: 0.001,
            price: Some(65_000.0),
            reference_price: 65_000.0,
            estimated_notional: 65.0,
            leverage: 5.0,
            reduce_only: false,
            stop_loss_price: None,
            take_profit_price: None,
            rules: crate::domain::execution::MarketRules {
                tick_size: 0.001,
                lot_size: 0.000001,
                min_notional: 1.0,
                max_leverage: 40,
            },
        };

        let signed = sign_trade_order(&mut signer, &account, &plan, 1_784_158_000_000_000_000)
            .expect("agent signs order");

        assert_eq!(signed.account, account.to_base58());
        assert_eq!(signed.signer, agent_public_key);
        assert!(signed.order_id.is_some());
        assert_eq!(
            signed.actions[0].pointer("/l/c").and_then(Value::as_str),
            Some("BTC-USD")
        );

        let protected_plan = TradePlan {
            stop_loss_price: Some(64_000.0),
            take_profit_price: Some(67_000.0),
            ..plan
        };
        let protected = sign_trade_order(
            &mut signer,
            &account,
            &protected_plan,
            1_784_158_000_000_000_001,
        )
        .expect("agent signs native on-fill protection");
        assert_eq!(protected.actions.len(), 2);
        assert_eq!(
            protected.actions[1]
                .pointer("/of/p")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            protected.actions[1]
                .pointer("/of/actions/0/rng/pmin")
                .and_then(Value::as_f64),
            Some(64_000.0)
        );
        assert_eq!(
            protected.actions[1]
                .pointer("/of/actions/0/rng/pmax")
                .and_then(Value::as_f64),
            Some(67_000.0)
        );
    }
}
