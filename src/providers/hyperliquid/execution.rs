use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::credentials::{self, ActiveHyperliquidCredential};
use crate::domain::execution::{
    AccountSnapshot, ExecutionReceipt, ExecutionVenue, Fill, LeverageSetting, MarginSummary,
    OpenOrder, OrderKind, OrderSide, Position, PositionDirection, TimeInForce, TradePlan,
    VenueCapabilities,
};

use super::HyperliquidNetwork;
use super::client::HyperliquidClient;
use super::exchange::{
    ExchangeDataStatus, ExchangeResponseStatus, HyperliquidExchangeClient, OrderGrouping,
    OrderRequest, WireOrder, raw_response, wire_number,
};
use super::markets;

const MARKET_SLIPPAGE: f64 = 0.005;

type LeverageKey = (HyperliquidNetwork, String, u32);
type LeverageValue = (u32, bool);
type ResolvedMarkets = HashMap<String, ResolvedMarket>;

static LEVERAGE_SETTINGS: OnceLock<Mutex<HashMap<LeverageKey, LeverageValue>>> = OnceLock::new();
static TESTNET_MARKETS: OnceLock<Mutex<Option<ResolvedMarkets>>> = OnceLock::new();

fn leverage_settings() -> &'static Mutex<HashMap<LeverageKey, LeverageValue>> {
    LEVERAGE_SETTINGS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn testnet_markets() -> &'static Mutex<Option<ResolvedMarkets>> {
    TESTNET_MARKETS.get_or_init(|| Mutex::new(None))
}

#[derive(Clone, Copy)]
struct ResolvedMarket {
    asset: u32,
    size_precision: u8,
    lot_size: f64,
    max_leverage: u32,
    cross_margin: bool,
}

pub struct HyperliquidExecutionAdapter {
    exchange: HyperliquidExchangeClient,
    account: String,
    network: HyperliquidNetwork,
}

impl HyperliquidExecutionAdapter {
    pub fn capabilities() -> VenueCapabilities {
        VenueCapabilities {
            venue: ExecutionVenue::Hyperliquid,
            order_kinds: vec![OrderKind::Market, OrderKind::Limit],
            time_in_forces: vec![TimeInForce::Gtc, TimeInForce::Ioc, TimeInForce::Alo],
            reduce_only: true,
            deterministic_order_ids: false,
            delegated_agent_signing: true,
            native_protective_triggers: true,
            native_oco: false,
            native_on_fill: false,
        }
    }

    pub async fn new(network: HyperliquidNetwork) -> Result<Self> {
        Self::with_credential(
            credentials::active_hyperliquid_credential(network)?,
            network,
        )
        .await
    }

    pub async fn with_credential(
        credential: ActiveHyperliquidCredential,
        network: HyperliquidNetwork,
    ) -> Result<Self> {
        let exchange = HyperliquidExchangeClient::new(credential.agent, network)?;
        Ok(Self {
            exchange,
            account: credential.account,
            network,
        })
    }

    pub async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        ensure_account(account, &self.account)?;
        let raw: ClearinghouseState = HyperliquidClient::for_network(self.network)?
            .info(&serde_json::json!({
                "type": "clearinghouseState",
                "user": account
            }))
            .await?;
        let contexts = load_mark_prices(self.network).await?;
        let positions = raw
            .asset_positions
            .into_iter()
            .filter(|position| position.position.size().is_ok_and(|size| size != 0.0))
            .map(|position| position.into_position(&contexts))
            .collect::<Result<Vec<_>>>()?;
        let unrealized_pnl = positions
            .iter()
            .map(|position| position.unrealized_pnl)
            .sum();
        let funding = positions.iter().map(|position| position.funding).sum();
        let total_balance = parse(&raw.margin_summary.account_value, "account value")?;
        let available_balance = parse(&raw.withdrawable, "withdrawable balance")?;
        let margin_used = parse(&raw.margin_summary.total_margin_used, "total margin used")?;
        let notional = parse(&raw.margin_summary.total_ntl_pos, "total notional")?;
        let leverage_settings = positions
            .iter()
            .map(|position| LeverageSetting {
                internal_symbol: position.internal_symbol.clone(),
                venue_symbol: position.venue_symbol.clone(),
                registry_supported: position.registry_supported,
                leverage: position.leverage,
            })
            .collect();
        Ok(AccountSnapshot {
            venue: ExecutionVenue::Hyperliquid,
            account: account.to_string(),
            fetched_at_ms: raw.time.unwrap_or(now_ms()?),
            margin: MarginSummary {
                total_balance,
                available_balance,
                margin_used,
                notional,
                realized_pnl: 0.0,
                unrealized_pnl,
                fees: 0.0,
                funding,
            },
            positions,
            open_orders: self.open_orders(account).await?,
            leverage_settings,
        })
    }

    pub async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        ensure_account(account, &self.account)?;
        let raw: Vec<HyperliquidOpenOrder> = HyperliquidClient::for_network(self.network)?
            .info(&serde_json::json!({
                "type": "frontendOpenOrders",
                "user": account
            }))
            .await?;
        raw.into_iter()
            .map(HyperliquidOpenOrder::into_order)
            .collect()
    }

    pub async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        ensure_account(account, &self.account)?;
        let raw: Vec<HyperliquidFill> = HyperliquidClient::for_network(self.network)?
            .info(&serde_json::json!({
                "type": "userFills",
                "user": account,
                "aggregateByTime": true
            }))
            .await?;
        raw.into_iter().map(HyperliquidFill::into_fill).collect()
    }

    pub async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt> {
        validate_trade_plan(plan)?;
        if self.network != HyperliquidNetwork::from_testnet(plan.testnet) {
            bail!("Hyperliquid trade plan network does not match the execution adapter");
        }
        ensure_account(&plan.account, &self.account)?;
        let market = markets::market(&plan.internal_symbol)?;
        let resolved = self.resolve_market(&market.venue_symbol).await?;
        validate_resolved_trade_plan(plan, &resolved)?;
        if !plan.reduce_only {
            self.ensure_leverage(resolved.asset, plan.leverage, resolved.cross_margin)
                .await?;
        }

        let entry_price = match plan.order_kind {
            OrderKind::Market => {
                let guarded = if plan.side == OrderSide::Buy {
                    plan.reference_price * (1.0 + MARKET_SLIPPAGE)
                } else {
                    plan.reference_price * (1.0 - MARKET_SLIPPAGE)
                };
                normalize_price(
                    guarded,
                    resolved.size_precision,
                    plan.side == OrderSide::Buy,
                )
            }
            OrderKind::Limit => plan.price.context("limit plan is missing its price")?,
        };
        let mut orders = vec![OrderRequest {
            asset: resolved.asset,
            is_buy: plan.side == OrderSide::Buy,
            reduce_only: plan.reduce_only,
            limit_px: wire_number(entry_price),
            size: wire_number(plan.size),
            order_type: WireOrder::Limit {
                tif: match plan.order_kind {
                    OrderKind::Market => "Ioc".to_string(),
                    OrderKind::Limit => hyperliquid_tif(
                        plan.time_in_force
                            .context("limit plan is missing its TIF")?,
                    )
                    .to_string(),
                },
            },
        }];
        let protection_side = plan.direction == PositionDirection::Short;
        for (price, kind) in [(plan.stop_loss_price, "sl"), (plan.take_profit_price, "tp")] {
            if let Some(price) = price {
                orders.push(OrderRequest {
                    asset: resolved.asset,
                    is_buy: protection_side,
                    reduce_only: true,
                    limit_px: wire_number(price),
                    size: wire_number(plan.size),
                    order_type: WireOrder::Trigger {
                        is_market: true,
                        trigger_px: wire_number(price),
                        tpsl: kind.to_string(),
                    },
                });
            }
        }

        let grouping = if orders.len() > 1 {
            OrderGrouping::NormalTpSl
        } else {
            OrderGrouping::None
        };
        let response = self
            .exchange
            .order(orders, grouping)
            .await
            .with_context(|| {
                format!(
                    "failed to submit Hyperliquid {} order",
                    self.network.label()
                )
            })?;
        receipt_from_response(&plan.account, response, "order")
    }

    pub async fn configure_leverage(&self, internal_symbol: &str, leverage: f64) -> Result<()> {
        let market = markets::market(internal_symbol)?;
        let resolved = self.resolve_market(&market.venue_symbol).await?;
        if !leverage.is_finite()
            || leverage < 1.0
            || leverage > f64::from(resolved.max_leverage)
            || leverage.fract().abs() > f64::EPSILON
        {
            bail!(
                "Hyperliquid {} leverage must be a whole number between 1 and {} for {}",
                self.network.label(),
                resolved.max_leverage,
                market.symbol
            );
        }
        self.ensure_leverage(resolved.asset, leverage, resolved.cross_margin)
            .await
    }

    async fn ensure_leverage(&self, asset: u32, leverage: f64, is_cross: bool) -> Result<()> {
        let leverage = leverage.round() as u32;
        let key = (self.network, self.account.to_ascii_lowercase(), asset);
        let expected = (leverage, is_cross);
        let mut settings = leverage_settings().lock().await;
        if settings.get(&key) == Some(&expected) {
            return Ok(());
        }

        let response = self
            .exchange
            .update_leverage(asset, leverage, is_cross)
            .await
            .with_context(|| {
                format!(
                    "failed to update Hyperliquid {} leverage",
                    self.network.label()
                )
            })?;
        require_default_response(response, "leverage update")?;
        settings.insert(key, expected);
        Ok(())
    }

    pub async fn cancel_order(
        &self,
        venue_symbol: &str,
        order_id: &str,
    ) -> Result<ExecutionReceipt> {
        let oid = order_id
            .parse::<u64>()
            .context("Hyperliquid order id must be an unsigned integer")?;
        let market = markets::market(venue_symbol)?;
        let asset = self.resolve_market(&market.venue_symbol).await?.asset;
        let response = self.exchange.cancel(asset, oid).await.with_context(|| {
            format!(
                "failed to cancel Hyperliquid {} order",
                self.network.label()
            )
        })?;
        let mut receipt = receipt_from_response(&self.account, response, "cancellation")?;
        receipt.order_id = Some(order_id.to_string());
        Ok(receipt)
    }

    async fn resolve_market(&self, venue_symbol: &str) -> Result<ResolvedMarket> {
        match self.network {
            HyperliquidNetwork::Mainnet => resolved_mainnet_market(venue_symbol),
            HyperliquidNetwork::Testnet => resolved_testnet_market(venue_symbol).await,
        }
    }
}

fn validate_trade_plan(plan: &TradePlan) -> Result<()> {
    if plan.venue != ExecutionVenue::Hyperliquid {
        bail!("Hyperliquid adapter received a plan for another execution venue");
    }
    let market = markets::market(&plan.internal_symbol)?;
    let rules = market.execution_rules()?;
    if plan.venue_symbol != market.venue_symbol || !market.is_available() {
        bail!("trade plan does not match an active Hyperliquid perpetual");
    }
    if !is_step_aligned(plan.size, rules.lot_size) || plan.size <= 0.0 {
        bail!(
            "trade plan size is not aligned to Hyperliquid lot size {} for {}",
            rules.lot_size,
            market.symbol
        );
    }
    if !plan.leverage.is_finite()
        || plan.leverage < 1.0
        || plan.leverage > f64::from(rules.max_leverage)
        || plan.leverage.fract().abs() > f64::EPSILON
    {
        bail!(
            "Hyperliquid leverage must be a whole number between 1 and {} for {}",
            rules.max_leverage,
            market.symbol
        );
    }
    if plan.size * plan.reference_price < rules.min_notional {
        bail!(
            "trade plan notional is below Hyperliquid minimum {} for {}",
            rules.min_notional,
            market.symbol
        );
    }
    if let Some(price) = plan.price {
        validate_price(price, rules.size_precision)?;
    }
    for price in [plan.stop_loss_price, plan.take_profit_price]
        .into_iter()
        .flatten()
    {
        validate_price(price, rules.size_precision)?;
    }
    Ok(())
}

fn validate_resolved_trade_plan(plan: &TradePlan, market: &ResolvedMarket) -> Result<()> {
    if !is_step_aligned(plan.size, market.lot_size) || plan.size <= 0.0 {
        bail!(
            "trade plan size is not aligned to Hyperliquid {} lot size {}",
            if plan.testnet { "testnet" } else { "mainnet" },
            market.lot_size
        );
    }
    if plan.leverage > f64::from(market.max_leverage) {
        bail!(
            "Hyperliquid {} leverage exceeds the market maximum of {}",
            if plan.testnet { "testnet" } else { "mainnet" },
            market.max_leverage
        );
    }
    if let Some(price) = plan.price {
        validate_price(price, market.size_precision)?;
    }
    for price in [plan.stop_loss_price, plan.take_profit_price]
        .into_iter()
        .flatten()
    {
        validate_price(price, market.size_precision)?;
    }
    Ok(())
}

fn resolved_mainnet_market(venue_symbol: &str) -> Result<ResolvedMarket> {
    let market = markets::market(venue_symbol)?;
    let rules = market.execution_rules()?;
    Ok(ResolvedMarket {
        asset: market
            .venue_id
            .context("Hyperliquid market snapshot omitted the native asset id")?,
        size_precision: rules.size_precision,
        lot_size: rules.lot_size,
        max_leverage: rules.max_leverage as u32, //TODO: look into if casting is needed
        cross_margin: rules.cross_margin,
    })
}

async fn resolved_testnet_market(venue_symbol: &str) -> Result<ResolvedMarket> {
    let cached = {
        let markets = testnet_markets().lock().await;
        markets
            .as_ref()
            .and_then(|markets| markets.get(venue_symbol))
            .copied()
    };
    if let Some(market) = cached {
        return Ok(market);
    }

    let metadata: HyperliquidMetadata =
        HyperliquidClient::for_network(HyperliquidNetwork::Testnet)?
            .info(&serde_json::json!({ "type": "meta" }))
            .await
            .context("failed to resolve Hyperliquid testnet execution metadata")?;
    let markets = metadata
        .universe
        .into_iter()
        .enumerate()
        .filter(|(_, market)| !market.is_delisted)
        .map(|(asset, market)| {
            let asset =
                u32::try_from(asset).context("Hyperliquid testnet asset index exceeds u32")?;
            Ok((
                market.name,
                ResolvedMarket {
                    asset,
                    size_precision: market.sz_decimals,
                    lot_size: 10_f64.powi(-i32::from(market.sz_decimals)),
                    max_leverage: market.max_leverage,
                    cross_margin: !market.only_isolated,
                },
            ))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    let resolved = markets.get(venue_symbol).copied().with_context(|| {
        format!("Hyperliquid testnet does not provide native perpetual `{venue_symbol}`")
    })?;
    *testnet_markets().lock().await = Some(markets);
    Ok(resolved)
}

pub fn normalize_price(price: f64, size_precision: u8, round_up: bool) -> f64 {
    let max_decimals = 6_u8.saturating_sub(size_precision);
    let magnitude = price.abs().log10().floor() as i32;
    let significant_decimals = (4 - magnitude).max(0) as u8;
    let decimals = max_decimals.min(significant_decimals);
    let scale = 10_f64.powi(i32::from(decimals));
    if round_up {
        (price * scale).ceil() / scale
    } else {
        (price * scale).floor() / scale
    }
}

pub(crate) fn validate_price(price: f64, size_precision: u8) -> Result<()> {
    if !price.is_finite() || price <= 0.0 {
        bail!("Hyperliquid order price must be finite and positive");
    }
    let down = normalize_price(price, size_precision, false);
    if (price - down).abs() > 1e-10_f64.max(price.abs() * 1e-12) {
        bail!(
            "price {price} violates Hyperliquid's five-significant-figure and decimal-place rules"
        );
    }
    Ok(())
}

fn hyperliquid_tif(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Gtc => "Gtc",
        TimeInForce::Ioc => "Ioc",
        TimeInForce::Alo => "Alo",
    }
}

fn require_default_response(response: ExchangeResponseStatus, operation: &str) -> Result<()> {
    match response {
        ExchangeResponseStatus::Ok(response) if response.data.is_none() => Ok(()),
        ExchangeResponseStatus::Ok(response) => {
            for status in response.data.into_iter().flat_map(|data| data.statuses) {
                if let ExchangeDataStatus::Error(error) = status {
                    bail!("Hyperliquid rejected {operation}: {error}");
                }
            }
            Ok(())
        }
        ExchangeResponseStatus::Err(error) => bail!("Hyperliquid rejected {operation}: {error}"),
    }
}

fn receipt_from_response(
    account: &str,
    response: ExchangeResponseStatus,
    operation: &str,
) -> Result<ExecutionReceipt> {
    let response = match response {
        ExchangeResponseStatus::Ok(response) => response,
        ExchangeResponseStatus::Err(error) => {
            bail!("Hyperliquid rejected {operation}: {error}")
        }
    };
    let raw_status = raw_response(&ExchangeResponseStatus::Ok(response.clone()));
    let statuses = response
        .data
        .context("Hyperliquid exchange response omitted order statuses")?
        .statuses;
    if statuses.is_empty() {
        bail!("Hyperliquid exchange response contained no order statuses");
    }
    for status in &statuses {
        if let ExchangeDataStatus::Error(error) = status {
            bail!("Hyperliquid rejected {operation}: {error}");
        }
    }
    let first = &statuses[0];
    let (order_id, status, terminal) = match first {
        ExchangeDataStatus::Filled(order) => (Some(order.oid.to_string()), "filled", true),
        ExchangeDataStatus::Resting(order) => (Some(order.oid.to_string()), "resting", false),
        ExchangeDataStatus::Success => (None, "cancelled", true),
        ExchangeDataStatus::WaitingForFill => (None, "waitingForFill", false),
        ExchangeDataStatus::WaitingForTrigger => (None, "waitingForTrigger", false),
        ExchangeDataStatus::Error(_) => unreachable!("errors handled above"),
    };
    Ok(ExecutionReceipt {
        venue: ExecutionVenue::Hyperliquid,
        account: account.to_string(),
        order_id,
        status: status.to_string(),
        terminal,
        submitted_at_ms: now_ms()?,
        raw_status,
    })
}

fn ensure_account(account: &str, configured: &str) -> Result<()> {
    if account.eq_ignore_ascii_case(configured) {
        Ok(())
    } else {
        bail!("request account no longer matches the configured Hyperliquid account")
    }
}

fn parse(value: &str, name: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("invalid Hyperliquid {name} `{value}`"))
}

fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn is_step_aligned(value: f64, step: f64) -> bool {
    let units = value / step;
    (units - units.round()).abs() <= 1e-8_f64.max(units.abs() * 1e-12)
}

async fn load_mark_prices(network: HyperliquidNetwork) -> Result<HashMap<String, f64>> {
    let value: serde_json::Value = HyperliquidClient::for_network(network)?
        .info(&serde_json::json!({ "type": "metaAndAssetCtxs" }))
        .await?;
    let entries = value
        .as_array()
        .context("Hyperliquid metaAndAssetCtxs must be an array")?;
    let universe = entries
        .first()
        .and_then(|meta| meta.get("universe"))
        .and_then(serde_json::Value::as_array)
        .context("Hyperliquid metadata omitted universe")?;
    let contexts = entries
        .get(1)
        .and_then(serde_json::Value::as_array)
        .context("Hyperliquid metadata omitted asset contexts")?;
    let mut prices = HashMap::new();
    for (asset, context) in universe.iter().zip(contexts) {
        let Some(name) = asset.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(mark) = context.get("markPx").and_then(serde_json::Value::as_str) else {
            continue;
        };
        prices.insert(name.to_string(), parse(mark, "mark price")?);
    }
    Ok(prices)
}

#[derive(Debug, Deserialize)]
struct HyperliquidMetadata {
    universe: Vec<HyperliquidMetadataMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidMetadataMarket {
    name: String,
    sz_decimals: u8,
    max_leverage: u32,
    #[serde(default)]
    only_isolated: bool,
    #[serde(default)]
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearinghouseState {
    margin_summary: HyperliquidMarginSummary,
    withdrawable: String,
    asset_positions: Vec<HyperliquidAssetPosition>,
    time: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidMarginSummary {
    account_value: String,
    total_ntl_pos: String,
    total_margin_used: String,
}

#[derive(Debug, Deserialize)]
struct HyperliquidAssetPosition {
    position: HyperliquidPosition,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidPosition {
    coin: String,
    entry_px: Option<String>,
    leverage: HyperliquidLeverage,
    liquidation_px: Option<String>,
    position_value: String,
    szi: String,
    unrealized_pnl: String,
    cum_funding: HyperliquidFunding,
}

#[derive(Debug, Deserialize)]
struct HyperliquidLeverage {
    value: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidFunding {
    since_open: String,
}

impl HyperliquidPosition {
    fn size(&self) -> Result<f64> {
        parse(&self.szi, "position size")
    }
}

impl HyperliquidAssetPosition {
    fn into_position(self, marks: &HashMap<String, f64>) -> Result<Position> {
        let signed_size = self.position.size()?;
        let market = markets::market(&self.position.coin).ok();
        let mark = marks.get(&self.position.coin).copied().unwrap_or_default();
        let internal_symbol = market.as_ref().map_or_else(
            || format!("{}/USDT", self.position.coin),
            |market| market.symbol.clone(),
        );
        Ok(Position {
            venue: ExecutionVenue::Hyperliquid,
            internal_symbol,
            venue_symbol: self.position.coin.clone(),
            registry_supported: market.is_some(),
            direction: if signed_size > 0.0 {
                PositionDirection::Long
            } else {
                PositionDirection::Short
            },
            size: signed_size.abs(),
            entry_price: self
                .position
                .entry_px
                .as_deref()
                .map_or(Ok(0.0), |value| parse(value, "entry price"))?,
            mark_price: mark,
            notional: parse(&self.position.position_value, "position value")?.abs(),
            realized_pnl: 0.0,
            unrealized_pnl: parse(&self.position.unrealized_pnl, "unrealized PnL")?,
            leverage: f64::from(self.position.leverage.value),
            liquidation_price: self
                .position
                .liquidation_px
                .as_deref()
                .map_or(Ok(0.0), |value| parse(value, "liquidation price"))?,
            fees: 0.0,
            funding: parse(&self.position.cum_funding.since_open, "cumulative funding")?,
            maintenance_margin: 0.0,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidOpenOrder {
    coin: String,
    limit_px: String,
    oid: u64,
    side: String,
    sz: String,
    timestamp: u64,
    #[serde(default)]
    orig_sz: Option<String>,
    #[serde(default)]
    reduce_only: bool,
    #[serde(default)]
    order_type: String,
    #[serde(default)]
    tif: String,
}

impl HyperliquidOpenOrder {
    fn into_order(self) -> Result<OpenOrder> {
        let market = markets::market(&self.coin).ok();
        let remaining = parse(&self.sz, "open order size")?;
        let original = self
            .orig_sz
            .as_deref()
            .map_or(Ok(remaining), |value| parse(value, "original order size"))?;
        Ok(OpenOrder {
            venue: ExecutionVenue::Hyperliquid,
            internal_symbol: market.as_ref().map_or_else(
                || format!("{}/USDT", self.coin),
                |market| market.symbol.clone(),
            ),
            venue_symbol: self.coin,
            registry_supported: market.is_some(),
            order_id: self.oid.to_string(),
            side: side(&self.side)?,
            price: parse(&self.limit_px, "open order price")?,
            original_size: original,
            remaining_size: remaining,
            filled_size: (original - remaining).max(0.0),
            vwap: 0.0,
            maker: self.tif.eq_ignore_ascii_case("Alo"),
            reduce_only: self.reduce_only,
            time_in_force: self.tif,
            status: if self.order_type.to_ascii_lowercase().contains("trigger") {
                "triggerWaiting".to_string()
            } else {
                "resting".to_string()
            },
            ts_ms: self.timestamp,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidFill {
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
    dir: String,
    oid: u64,
    crossed: bool,
    #[serde(default)]
    fee: Option<String>,
}

impl HyperliquidFill {
    fn into_fill(self) -> Result<Fill> {
        let market = markets::market(&self.coin).ok();
        Ok(Fill {
            venue: ExecutionVenue::Hyperliquid,
            internal_symbol: market.as_ref().map_or_else(
                || format!("{}/USDT", self.coin),
                |market| market.symbol.clone(),
            ),
            venue_symbol: self.coin,
            registry_supported: market.is_some(),
            side: side(&self.side)?,
            amount: parse(&self.sz, "fill size")?,
            price: parse(&self.px, "fill price")?,
            reason: self.dir,
            order_id: Some(self.oid.to_string()),
            maker: !self.crossed,
            // Hyperliquid reports costs as positive values and rebates as
            // negative values. Market Lab uses the opposite signed convention.
            fee: self
                .fee
                .as_deref()
                .map(|fee| parse(fee, "fill fee").map(|fee| -fee))
                .transpose()?,
            slot: 0,
            ts_ms: self.time,
        })
    }
}

fn side(value: &str) -> Result<OrderSide> {
    match value.to_ascii_uppercase().as_str() {
        "B" | "BUY" => Ok(OrderSide::Buy),
        "A" | "S" | "SELL" => Ok(OrderSide::Sell),
        _ => bail!("unknown Hyperliquid order side `{value}`"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_prices_to_hyperliquid_wire_rules() {
        assert_eq!(normalize_price(66_632.064, 5, false), 66_632.0);
        assert_eq!(normalize_price(1_927.806, 4, true), 1_927.9);
        assert_eq!(normalize_price(0.60914, 2, false), 0.6091);
    }

    #[test]
    fn maps_hyperliquid_sides() {
        assert_eq!(side("B").expect("buy"), OrderSide::Buy);
        assert_eq!(side("A").expect("sell"), OrderSide::Sell);
    }

    #[test]
    fn recovered_hyperliquid_fills_preserve_signed_fees() {
        let raw: HyperliquidFill = serde_json::from_value(serde_json::json!({
            "coin": "BTC",
            "px": "66536.625",
            "sz": "0.00376",
            "side": "B",
            "time": 1_784_700_000_000_u64,
            "dir": "Close Short",
            "oid": 56_814_363_179_u64,
            "crossed": true,
            "fee": "0.187391"
        }))
        .expect("fill payload");
        let fill = raw.into_fill().expect("normalized fill");

        assert_eq!(fill.fee, Some(-0.187391));
    }
}
