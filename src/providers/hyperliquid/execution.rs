use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::credentials::{self, ActiveHyperliquidCredential};
use crate::domain::execution::{
    AccountSnapshot, CancelPlan, ExecutionOutcome, ExecutionReceipt, ExecutionVenue, Fill,
    LeverageSetting, MarginSummary, OpenOrder, OrderKind, OrderSide, Position, PositionDirection,
    SpotBalance, TimeInForce, TradePlan, VenueCapabilities,
};

use super::client::HyperliquidClient;
use super::exchange::{
    CancelRequest, ExchangeDataStatus, ExchangeResponseStatus, HyperliquidExchangeClient,
    OrderGrouping, OrderRequest, WireOrder, raw_response, wire_number,
};
use super::markets;
use super::{HyperliquidNetwork, HyperliquidProduct};

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
    max_price_decimals: u8,
}

pub struct HyperliquidExecutionAdapter {
    exchange: HyperliquidExchangeClient,
    account: String,
    network: HyperliquidNetwork,
    product: HyperliquidProduct,
}

impl HyperliquidExecutionAdapter {
    pub fn capabilities() -> VenueCapabilities {
        Self::capabilities_for(HyperliquidProduct::Perpetual)
    }

    pub fn capabilities_for(product: HyperliquidProduct) -> VenueCapabilities {
        VenueCapabilities {
            venue: venue_for_product(product),
            order_kinds: vec![OrderKind::Market, OrderKind::Limit],
            time_in_forces: vec![TimeInForce::Gtc, TimeInForce::Ioc, TimeInForce::Alo],
            reduce_only: product == HyperliquidProduct::Perpetual,
            deterministic_order_ids: false,
            delegated_agent_signing: true,
            native_protective_triggers: product == HyperliquidProduct::Perpetual,
            native_oco: false,
            native_on_fill: false,
        }
    }

    pub async fn new(network: HyperliquidNetwork) -> Result<Self> {
        Self::new_for(HyperliquidProduct::Perpetual, network).await
    }

    pub async fn new_for(product: HyperliquidProduct, network: HyperliquidNetwork) -> Result<Self> {
        Self::with_credential(
            credentials::active_hyperliquid_credential(network)?,
            network,
            product,
        )
        .await
    }

    pub async fn with_credential(
        credential: ActiveHyperliquidCredential,
        network: HyperliquidNetwork,
        product: HyperliquidProduct,
    ) -> Result<Self> {
        let exchange = HyperliquidExchangeClient::new(credential.agent, network)?;
        Ok(Self {
            exchange,
            account: credential.account,
            network,
            product,
        })
    }

    pub async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        ensure_account(account, &self.account)?;
        if self.product == HyperliquidProduct::Spot {
            return self.spot_account_snapshot(account).await;
        }
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
            venue: self.venue(),
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
            spot_balances: Vec::new(),
            open_orders: self.open_orders(account).await?,
            leverage_settings,
        })
    }

    async fn spot_account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        let raw: SpotClearinghouseState = HyperliquidClient::for_network(self.network)?
            .info(&serde_json::json!({
                "type": "spotClearinghouseState",
                "user": account
            }))
            .await?;
        let balances = raw
            .balances
            .into_iter()
            .map(|balance| normalize_spot_balance(self.network, balance))
            .collect::<Result<Vec<_>>>()?;
        let quote = balances.iter().find(|balance| balance.asset == "USDT");
        Ok(AccountSnapshot {
            venue: self.venue(),
            account: account.to_string(),
            fetched_at_ms: raw.time.unwrap_or(now_ms()?),
            margin: MarginSummary {
                total_balance: quote.map_or(0.0, |balance| balance.total),
                available_balance: quote.map_or(0.0, |balance| balance.available),
                margin_used: quote.map_or(0.0, |balance| balance.held),
                notional: 0.0,
                realized_pnl: 0.0,
                unrealized_pnl: 0.0,
                fees: 0.0,
                funding: 0.0,
            },
            positions: Vec::new(),
            spot_balances: balances,
            open_orders: self.open_orders(account).await?,
            leverage_settings: Vec::new(),
        })
    }

    const fn venue(&self) -> ExecutionVenue {
        venue_for_product(self.product)
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
            .filter_map(|order| order.into_order(self.product, self.network).transpose())
            .collect()
    }

    pub async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        ensure_account(account, &self.account)?;
        let raw: Vec<HyperliquidFill> = HyperliquidClient::for_network(self.network)?
            .info(&user_fills_request(account))
            .await?;
        raw.into_iter()
            .filter_map(|fill| fill.into_fill(self.product, self.network).transpose())
            .collect()
    }

    pub async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt> {
        validate_trade_plan(plan, self.product, self.network)?;
        if self.network != HyperliquidNetwork::from_testnet(plan.testnet) {
            bail!("Hyperliquid trade plan network does not match the execution adapter");
        }
        ensure_account(&plan.account, &self.account)?;
        let resolved = self.resolve_market(&plan.internal_symbol).await?;
        validate_resolved_trade_plan(plan, &resolved)?;
        if self.product == HyperliquidProduct::Perpetual && !plan.reduce_only {
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
                normalize_price_for(
                    guarded,
                    resolved.size_precision,
                    resolved.max_price_decimals,
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
        receipt_from_response(self.venue(), &plan.account, response, "order")
    }

    pub async fn submit_trades(&self, plans: &[TradePlan]) -> Result<Vec<ExecutionOutcome>> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }
        let mut orders = Vec::with_capacity(plans.len());
        for plan in plans {
            validate_trade_plan(plan, self.product, self.network)?;
            if self.network != HyperliquidNetwork::from_testnet(plan.testnet) {
                bail!("Hyperliquid trade plan network does not match the execution adapter");
            }
            ensure_account(&plan.account, &self.account)?;
            if plan.stop_loss_price.is_some() || plan.take_profit_price.is_some() {
                bail!("Hyperliquid batch orders do not support attached protection");
            }
            let resolved = self.resolve_market(&plan.internal_symbol).await?;
            validate_resolved_trade_plan(plan, &resolved)?;
            if self.product == HyperliquidProduct::Perpetual && !plan.reduce_only {
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
                    normalize_price_for(
                        guarded,
                        resolved.size_precision,
                        resolved.max_price_decimals,
                        plan.side == OrderSide::Buy,
                    )
                }
                OrderKind::Limit => plan.price.context("limit plan is missing its price")?,
            };
            orders.push(OrderRequest {
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
            });
        }
        let response = self.exchange.order(orders, OrderGrouping::None).await?;
        Ok(batch_outcomes_from_response(
            self.venue(),
            &self.account,
            response,
            plans.len(),
            "order",
        ))
    }

    pub async fn configure_leverage(&self, internal_symbol: &str, leverage: f64) -> Result<()> {
        if self.product == HyperliquidProduct::Spot {
            if (leverage - 1.0).abs() <= f64::EPSILON {
                return Ok(());
            }
            bail!("Hyperliquid spot has fixed 1x leverage");
        }
        let market = markets::market(internal_symbol)?;
        let resolved = self.resolve_market(&market.symbol).await?;
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
        self.cancel_order_with_priority(venue_symbol, order_id, false)
            .await
    }

    pub async fn cancel_order_fast(
        &self,
        venue_symbol: &str,
        order_id: &str,
    ) -> Result<ExecutionReceipt> {
        self.cancel_order_with_priority(venue_symbol, order_id, true)
            .await
    }

    async fn cancel_order_with_priority(
        &self,
        venue_symbol: &str,
        order_id: &str,
        fast: bool,
    ) -> Result<ExecutionReceipt> {
        let oid = order_id
            .parse::<u64>()
            .context("Hyperliquid order id must be an unsigned integer")?;
        let market = markets::market_for_wire(self.product, self.network, venue_symbol)?;
        let asset = self.resolve_market(&market.symbol).await?.asset;
        let response = if fast {
            self.exchange.cancel_fast(asset, oid).await
        } else {
            self.exchange.cancel(asset, oid).await
        }
        .with_context(|| {
            format!(
                "failed to cancel Hyperliquid {} order",
                self.network.label()
            )
        })?;
        let mut receipt =
            receipt_from_response(self.venue(), &self.account, response, "cancellation")?;
        receipt.order_id = Some(order_id.to_string());
        Ok(receipt)
    }

    pub async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        self.cancel_orders_with_priority(plans, false).await
    }

    pub async fn cancel_orders_fast(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        self.cancel_orders_with_priority(plans, true).await
    }

    async fn cancel_orders_with_priority(
        &self,
        plans: &[CancelPlan],
        fast: bool,
    ) -> Result<Vec<ExecutionOutcome>> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }
        let mut cancels = Vec::with_capacity(plans.len());
        for plan in plans {
            if self.network != HyperliquidNetwork::from_testnet(plan.testnet) {
                bail!("Hyperliquid cancellation network does not match the execution adapter");
            }
            ensure_account(&plan.account, &self.account)?;
            let oid = plan
                .order_id
                .parse::<u64>()
                .context("Hyperliquid order id must be an unsigned integer")?;
            let market = markets::market_for_wire(self.product, self.network, &plan.venue_symbol)?;
            let asset = self.resolve_market(&market.symbol).await?.asset;
            cancels.push(CancelRequest { asset, oid });
        }
        let response = if fast {
            self.exchange.cancel_many_fast(cancels).await?
        } else {
            self.exchange.cancel_many(cancels).await?
        };
        let mut outcomes = batch_outcomes_from_response(
            self.venue(),
            &self.account,
            response,
            plans.len(),
            "cancellation",
        );
        for (outcome, plan) in outcomes.iter_mut().zip(plans) {
            if let Some(receipt) = outcome.receipt.as_mut() {
                receipt.order_id = Some(plan.order_id.clone());
            }
        }
        Ok(outcomes)
    }

    async fn resolve_market(&self, symbol: &str) -> Result<ResolvedMarket> {
        match (self.product, self.network) {
            (HyperliquidProduct::Spot, network) => resolved_spot_market(network, symbol),
            (HyperliquidProduct::Perpetual, HyperliquidNetwork::Mainnet) => {
                resolved_mainnet_market(symbol)
            }
            (HyperliquidProduct::Perpetual, HyperliquidNetwork::Testnet) => {
                let market = markets::market(symbol)?;
                resolved_testnet_market(&market.venue_symbol).await
            }
        }
    }
}

fn validate_trade_plan(
    plan: &TradePlan,
    product: HyperliquidProduct,
    network: HyperliquidNetwork,
) -> Result<()> {
    if plan.venue != venue_for_product(product) {
        bail!("Hyperliquid adapter received a plan for another execution venue");
    }
    let (market, variant) = markets::network_market(product, network, &plan.internal_symbol)?;
    let rules = &variant.execution;
    if plan.venue_symbol != variant.venue_symbol || !market.is_available() {
        bail!(
            "trade plan does not match an active Hyperliquid {} market",
            product.exchange()
        );
    }
    if product == HyperliquidProduct::Spot {
        if (plan.leverage - 1.0).abs() > f64::EPSILON {
            bail!("Hyperliquid spot has fixed 1x leverage");
        }
        if plan.reduce_only {
            bail!("Hyperliquid spot orders do not support reduce-only");
        }
        if plan.stop_loss_price.is_some() || plan.take_profit_price.is_some() {
            bail!("Hyperliquid spot does not support attached SL/TP orders");
        }
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
        validate_price_for(price, rules.size_precision, product.max_price_decimals())?;
    }
    for price in [plan.stop_loss_price, plan.take_profit_price]
        .into_iter()
        .flatten()
    {
        validate_price_for(price, rules.size_precision, product.max_price_decimals())?;
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
        validate_price_for(price, market.size_precision, market.max_price_decimals)?;
    }
    for price in [plan.stop_loss_price, plan.take_profit_price]
        .into_iter()
        .flatten()
    {
        validate_price_for(price, market.size_precision, market.max_price_decimals)?;
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
        max_leverage: u32::from(rules.max_leverage),
        cross_margin: rules.cross_margin,
        max_price_decimals: HyperliquidProduct::Perpetual.max_price_decimals(),
    })
}

fn resolved_spot_market(network: HyperliquidNetwork, symbol: &str) -> Result<ResolvedMarket> {
    let (_market, variant) = markets::network_market(HyperliquidProduct::Spot, network, symbol)?;
    let rules = &variant.execution;
    Ok(ResolvedMarket {
        asset: variant.venue_id,
        size_precision: rules.size_precision,
        lot_size: rules.lot_size,
        max_leverage: 1,
        cross_margin: false,
        max_price_decimals: HyperliquidProduct::Spot.max_price_decimals(),
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
                    max_price_decimals: HyperliquidProduct::Perpetual.max_price_decimals(),
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
    normalize_price_for(price, size_precision, 6, round_up)
}

pub fn normalize_price_for(
    price: f64,
    size_precision: u8,
    max_price_decimals: u8,
    round_up: bool,
) -> f64 {
    let max_decimals = max_price_decimals.saturating_sub(size_precision);
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
    validate_price_for(price, size_precision, 6)
}

pub(crate) fn validate_price_for(
    price: f64,
    size_precision: u8,
    max_price_decimals: u8,
) -> Result<()> {
    if !price.is_finite() || price <= 0.0 {
        bail!("Hyperliquid order price must be finite and positive");
    }
    let down = normalize_price_for(price, size_precision, max_price_decimals, false);
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
    venue: ExecutionVenue,
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
        venue,
        account: account.to_string(),
        order_id,
        status: status.to_string(),
        terminal,
        submitted_at_ms: now_ms()?,
        raw_status,
    })
}

fn batch_outcomes_from_response(
    venue: ExecutionVenue,
    account: &str,
    response: ExchangeResponseStatus,
    expected: usize,
    operation: &str,
) -> Vec<ExecutionOutcome> {
    let response = match response {
        ExchangeResponseStatus::Ok(response) => response,
        ExchangeResponseStatus::Err(error) => {
            let error = format!("Hyperliquid rejected {operation}: {error}");
            return (0..expected)
                .map(|_| ExecutionOutcome::failure(error.clone()))
                .collect();
        }
    };
    let Some(data) = response.data else {
        let error = format!("Hyperliquid exchange response omitted {operation} statuses");
        return (0..expected)
            .map(|_| ExecutionOutcome::failure(error.clone()))
            .collect();
    };
    if expected > 1
        && data.statuses.len() == 1
        && let ExchangeDataStatus::Error(error) = &data.statuses[0]
    {
        let error = format!("Hyperliquid rejected {operation}: {error}");
        return (0..expected)
            .map(|_| ExecutionOutcome::failure(error.clone()))
            .collect();
    }
    if data.statuses.len() != expected {
        let error = format!(
            "Hyperliquid exchange response returned {} {operation} statuses for {expected} requests",
            data.statuses.len()
        );
        return (0..expected)
            .map(|_| ExecutionOutcome::failure(error.clone()))
            .collect();
    }
    data.statuses
        .into_iter()
        .map(|status| {
            let raw_status = serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
            let (order_id, name, terminal) = match status {
                ExchangeDataStatus::Filled(order) => (Some(order.oid.to_string()), "filled", true),
                ExchangeDataStatus::Resting(order) => {
                    (Some(order.oid.to_string()), "resting", false)
                }
                ExchangeDataStatus::Success => (None, "cancelled", true),
                ExchangeDataStatus::WaitingForFill => (None, "waitingForFill", false),
                ExchangeDataStatus::WaitingForTrigger => (None, "waitingForTrigger", false),
                ExchangeDataStatus::Error(error) => {
                    return ExecutionOutcome::failure(format!(
                        "Hyperliquid rejected {operation}: {error}"
                    ));
                }
            };
            match now_ms() {
                Ok(submitted_at_ms) => ExecutionOutcome::success(ExecutionReceipt {
                    venue,
                    account: account.to_string(),
                    order_id,
                    status: name.to_string(),
                    terminal,
                    submitted_at_ms,
                    raw_status,
                }),
                Err(error) => ExecutionOutcome::failure(format!("{error:#}")),
            }
        })
        .collect()
}

fn ensure_account(account: &str, configured: &str) -> Result<()> {
    if account.eq_ignore_ascii_case(configured) {
        Ok(())
    } else {
        bail!("request account no longer matches the configured Hyperliquid account")
    }
}

const fn venue_for_product(product: HyperliquidProduct) -> ExecutionVenue {
    match product {
        HyperliquidProduct::Spot => ExecutionVenue::HyperliquidSpot,
        HyperliquidProduct::Perpetual => ExecutionVenue::Hyperliquid,
    }
}

fn normalize_spot_balance(
    network: HyperliquidNetwork,
    balance: HyperliquidSpotBalance,
) -> Result<SpotBalance> {
    let (asset, venue_asset, registry_supported) =
        resolve_spot_token(network, Some(balance.token), &balance.coin)?;
    let total = parse(&balance.total, "spot balance total")?;
    let held = parse(&balance.hold, "spot balance hold")?;
    Ok(SpotBalance {
        venue: ExecutionVenue::HyperliquidSpot,
        asset,
        venue_asset,
        token_index: balance.token,
        registry_supported,
        total,
        held,
        available: (total - held).max(0.0),
        entry_notional: balance
            .entry_ntl
            .as_deref()
            .map_or(Ok(0.0), |value| parse(value, "spot entry notional"))?,
    })
}

fn canonical_spot_asset(network: HyperliquidNetwork, venue_asset: &str) -> Result<String> {
    resolve_spot_token(network, None, venue_asset).map(|(asset, _, _)| asset)
}

fn resolve_spot_token(
    network: HyperliquidNetwork,
    token_index: Option<u32>,
    venue_asset: &str,
) -> Result<(String, String, bool)> {
    if let Ok(markets) = crate::markets::exchange_markets(HyperliquidProduct::Spot.exchange()) {
        for market in markets {
            let Ok(variant) = market.network_variant(network.label()) else {
                continue;
            };
            if variant.base_token_index == token_index && token_index.is_some()
                || variant.venue_base_asset.eq_ignore_ascii_case(venue_asset)
            {
                return Ok((market.base_asset.clone(), variant.venue_base_asset, true));
            }
            if variant.quote_token_index == token_index && token_index.is_some()
                || variant.venue_quote_asset.eq_ignore_ascii_case(venue_asset)
            {
                return Ok((market.quote_asset.clone(), variant.venue_quote_asset, true));
            }
        }
    }
    let canonical = if venue_asset.eq_ignore_ascii_case("USDC") {
        "USDT".to_string()
    } else {
        venue_asset.to_ascii_uppercase()
    };
    Ok((canonical, venue_asset.to_string(), false))
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
struct SpotClearinghouseState {
    balances: Vec<HyperliquidSpotBalance>,
    time: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidSpotBalance {
    coin: String,
    token: u32,
    hold: String,
    total: String,
    #[serde(default)]
    entry_ntl: Option<String>,
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
    fn into_order(
        self,
        product: HyperliquidProduct,
        network: HyperliquidNetwork,
    ) -> Result<Option<OpenOrder>> {
        let Ok(market) = markets::market_for_wire(product, network, &self.coin) else {
            return Ok(None);
        };
        let remaining = parse(&self.sz, "open order size")?;
        let original = self
            .orig_sz
            .as_deref()
            .map_or(Ok(remaining), |value| parse(value, "original order size"))?;
        Ok(Some(OpenOrder {
            venue: venue_for_product(product),
            internal_symbol: market.symbol.clone(),
            venue_symbol: self.coin,
            registry_supported: true,
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
        }))
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
    #[serde(default)]
    fee_token: Option<String>,
}

impl HyperliquidFill {
    fn into_fill(
        self,
        product: HyperliquidProduct,
        network: HyperliquidNetwork,
    ) -> Result<Option<Fill>> {
        let Ok(market) = markets::market_for_wire(product, network, &self.coin) else {
            return Ok(None);
        };
        Ok(Some(Fill {
            venue: venue_for_product(product),
            internal_symbol: market.symbol.clone(),
            venue_symbol: self.coin,
            registry_supported: true,
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
            fee_asset: self
                .fee_token
                .as_deref()
                .map(|asset| canonical_spot_asset(network, asset))
                .transpose()?,
            slot: 0,
            ts_ms: self.time,
        }))
    }
}

fn user_fills_request(account: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "userFills",
        "user": account,
        // Recovery must preserve the same partial-fill granularity as the
        // userEvents stream so the fill ledger can identify duplicates.
        "aggregateByTime": false
    })
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
    fn spot_prices_use_the_eight_decimal_place_budget() {
        assert_eq!(normalize_price_for(0.00001234567, 0, 8, false), 0.00001234);
        validate_price_for(0.00001234, 0, 8).expect("spot price is valid");
        assert!(validate_price_for(0.00001234, 0, 6).is_err());
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
        let fill = raw
            .into_fill(HyperliquidProduct::Perpetual, HyperliquidNetwork::Mainnet)
            .expect("normalized fill")
            .expect("perpetual fill");

        assert_eq!(fill.fee, Some(-0.187391));
    }

    #[test]
    fn spot_fills_resolve_pair_identity_and_fee_asset() {
        let raw: HyperliquidFill = serde_json::from_value(serde_json::json!({
            "coin": "@142",
            "px": "66536",
            "sz": "0.00376",
            "side": "B",
            "time": 1_784_700_000_000_u64,
            "dir": "Buy",
            "oid": 56_814_363_179_u64,
            "crossed": false,
            "fee": "0.187391",
            "feeToken": "USDC"
        }))
        .expect("spot fill payload");
        let fill = raw
            .into_fill(HyperliquidProduct::Spot, HyperliquidNetwork::Mainnet)
            .expect("normalized fill")
            .expect("spot fill");

        assert_eq!(fill.venue, ExecutionVenue::HyperliquidSpot);
        assert_eq!(fill.internal_symbol, "BTC/USDT");
        assert_eq!(fill.venue_symbol, "@142");
        assert_eq!(fill.fee, Some(-0.187391));
        assert_eq!(fill.fee_asset.as_deref(), Some("USDT"));
    }

    #[test]
    fn spot_balances_are_distinct_from_perpetual_positions() {
        let balance = normalize_spot_balance(
            HyperliquidNetwork::Mainnet,
            HyperliquidSpotBalance {
                coin: "UBTC".to_string(),
                token: 197,
                hold: "0.25".to_string(),
                total: "1.5".to_string(),
                entry_ntl: Some("90000".to_string()),
            },
        )
        .expect("spot balance");

        assert_eq!(balance.asset, "BTC");
        assert_eq!(balance.venue_asset, "UBTC");
        assert_eq!(balance.total, 1.5);
        assert_eq!(balance.held, 0.25);
        assert_eq!(balance.available, 1.25);
        assert!(balance.registry_supported);
    }

    #[test]
    fn recovery_requests_individual_hyperliquid_fills() {
        let request = user_fills_request("0xabc");

        assert_eq!(request["type"], "userFills");
        assert_eq!(request["user"], "0xabc");
        assert_eq!(request["aggregateByTime"], false);
    }

    #[test]
    fn batch_response_preserves_each_order_outcome() {
        let response: ExchangeResponseStatus = serde_json::from_value(serde_json::json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        { "resting": { "oid": 42 } },
                        { "error": "Post only order would have immediately matched" }
                    ]
                }
            }
        }))
        .expect("valid response");
        let outcomes = batch_outcomes_from_response(
            ExecutionVenue::Hyperliquid,
            "0xabc",
            response,
            2,
            "order",
        );

        assert_eq!(
            outcomes[0]
                .receipt
                .as_ref()
                .and_then(|receipt| receipt.order_id.as_deref()),
            Some("42")
        );
        assert!(outcomes[0].error.is_none());
        assert!(
            outcomes[1]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("Post only"))
        );
    }

    #[test]
    fn batch_level_rejection_is_applied_to_every_order() {
        let outcomes = batch_outcomes_from_response(
            ExecutionVenue::Hyperliquid,
            "0xabc",
            ExchangeResponseStatus::Err("invalid nonce".to_string()),
            3,
            "order",
        );

        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|outcome| {
            outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("invalid nonce"))
        }));
    }
}
