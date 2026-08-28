use anyhow::{Context, Result, bail};
use async_trait::async_trait;

use crate::credentials;
use crate::domain::execution::{
    AccountSnapshot, CancelPlan, ExecutionOutcome, ExecutionReceipt, ExecutionVenue, Fill,
    OpenOrder, Position, TradePlan, VenueCapabilities,
};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::bulk::ws::BulkAccountStream;
use crate::providers::hyperlink::ws::HyperlinkAccountStream;
use crate::providers::hyperliquid::execution::HyperliquidExecutionAdapter;
use crate::providers::hyperliquid::ws::{HyperliquidAccountStream, HyperliquidTradingClient};
use crate::providers::hyperliquid::{HyperliquidNetwork, HyperliquidProduct};
use crate::venues::{AuthBackend, ExecutionBackend};

/// Common contract implemented by every execution exchange.
///
/// Runtime, bots, strategies, and scripts depend on this contract only. A new
/// exchange implements the contract and is registered in `ExecutionAdapter`;
/// no orchestration module needs an exchange-specific branch.
#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    fn venue_capabilities(&self) -> VenueCapabilities;
    fn validate_order_id(&self, order_id: &str) -> Result<()>;
    async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot>;
    async fn account_snapshot_for_market(
        &self,
        account: &str,
        _symbol: &str,
    ) -> Result<AccountSnapshot> {
        self.account_snapshot(account).await
    }
    async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>>;
    async fn open_orders_for_market(&self, account: &str, _symbol: &str) -> Result<Vec<OpenOrder>> {
        self.open_orders(account).await
    }
    async fn fills(&self, account: &str) -> Result<Vec<Fill>>;
    async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt>;
    async fn cancel_order(&self, plan: &CancelPlan) -> Result<ExecutionReceipt>;
    async fn submit_trades(&self, plans: &[TradePlan]) -> Result<Vec<ExecutionOutcome>>;
    async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>>;

    async fn recover_account_gap(
        &self,
        account: &str,
        since_ms: u64,
    ) -> Result<AccountGapRecovery> {
        let orders = self
            .open_orders(account)
            .await?
            .into_iter()
            .map(|order| RecoveredOrder {
                order_id: order.order_id.clone(),
                status: order.status.clone(),
                ts_ms: order.ts_ms,
                data: serde_json::to_value(order).expect("OpenOrder serializes"),
            })
            .collect();
        let fills = self
            .fills(account)
            .await?
            .into_iter()
            .filter(|fill| fill.ts_ms >= since_ms)
            .collect();
        Ok(AccountGapRecovery { orders, fills })
    }

    async fn cancel_order_fast(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        self.cancel_order(plan).await
    }

    async fn cancel_orders_fast(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        self.cancel_orders(plans).await
    }

    async fn max_leverage(&self, _symbol: &str) -> Result<u32> {
        anyhow::bail!("this execution provider does not expose leverage metadata")
    }

    async fn configure_leverage(&self, _symbol: &str, _leverage: f64) -> Result<()> {
        anyhow::bail!("this execution provider configures leverage during order execution")
    }

    async fn submit_user_outcome(
        &self,
        _action: crate::providers::hyperliquid::exchange::UserOutcomeAction,
    ) -> Result<serde_json::Value> {
        anyhow::bail!("this execution provider does not support outcome actions")
    }
}

pub struct RecoveredOrder {
    pub order_id: String,
    pub status: String,
    pub ts_ms: u64,
    pub data: serde_json::Value,
}

pub struct AccountGapRecovery {
    pub orders: Vec<RecoveredOrder>,
    pub fills: Vec<Fill>,
}

#[async_trait]
impl ExecutionProvider for BulkExecutionAdapter {
    fn venue_capabilities(&self) -> VenueCapabilities {
        Self::capabilities()
    }

    fn validate_order_id(&self, order_id: &str) -> Result<()> {
        bulk_keychain::Hash::from_base58(order_id)
            .context("invalid BULK order id")
            .map(|_| ())
    }

    async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        Self::account_snapshot(self, account).await
    }

    async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        Self::open_orders(self, account).await
    }

    async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        Self::fills(self, account).await
    }

    async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt> {
        Self::submit_trade(
            self,
            credentials::active_bulk_credential_for_account(&plan.account)?,
            plan,
        )
        .await
    }

    async fn cancel_order(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        Self::cancel_order(
            self,
            credentials::active_bulk_credential_for_account(&plan.account)?,
            &plan.venue_symbol,
            &plan.order_id,
        )
        .await
    }

    async fn submit_trades(&self, plans: &[TradePlan]) -> Result<Vec<ExecutionOutcome>> {
        let Some(first) = plans.first() else {
            return Ok(Vec::new());
        };
        Self::submit_trades(
            self,
            credentials::active_bulk_credential_for_account(&first.account)?,
            plans,
        )
        .await
    }

    async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        let Some(first) = plans.first() else {
            return Ok(Vec::new());
        };
        Self::cancel_orders(
            self,
            credentials::active_bulk_credential_for_account(&first.account)?,
            plans,
        )
        .await
    }

    async fn recover_account_gap(
        &self,
        account: &str,
        since_ms: u64,
    ) -> Result<AccountGapRecovery> {
        let orders = self
            .order_history(account)
            .await?
            .into_iter()
            .filter(|order| order.ts_ms >= since_ms)
            .map(|order| RecoveredOrder {
                order_id: order.order_id.clone(),
                status: order.status.clone(),
                ts_ms: order.ts_ms,
                data: serde_json::to_value(order).expect("OrderRecord serializes"),
            })
            .collect();
        let fills = self
            .fills(account)
            .await?
            .into_iter()
            .filter(|fill| fill.ts_ms >= since_ms)
            .collect();
        Ok(AccountGapRecovery { orders, fills })
    }

    async fn max_leverage(&self, symbol: &str) -> Result<u32> {
        Ok(u32::from(
            crate::providers::bulk::markets::market(symbol)?
                .execution_rules()?
                .max_leverage,
        ))
    }
}

#[async_trait]
impl ExecutionProvider for HyperliquidExecutionAdapter {
    fn venue_capabilities(&self) -> VenueCapabilities {
        self.capabilities_for_route()
    }

    fn validate_order_id(&self, order_id: &str) -> Result<()> {
        order_id
            .parse::<u64>()
            .context("Hyperliquid order id must be an unsigned integer")
            .map(|_| ())
    }

    async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        Self::account_snapshot(self, account).await
    }

    async fn account_snapshot_for_market(
        &self,
        account: &str,
        symbol: &str,
    ) -> Result<AccountSnapshot> {
        Self::account_snapshot_for_market(self, account, symbol).await
    }

    async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        Self::open_orders(self, account).await
    }

    async fn open_orders_for_market(&self, account: &str, symbol: &str) -> Result<Vec<OpenOrder>> {
        Self::open_orders_for_market(self, account, symbol).await
    }

    async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        Self::fills(self, account).await
    }

    async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt> {
        Self::submit_trade(self, plan).await
    }

    async fn cancel_order(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        Self::cancel_order(self, &plan.venue_symbol, &plan.order_id).await
    }

    async fn cancel_order_fast(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        Self::cancel_order_fast(self, &plan.venue_symbol, &plan.order_id).await
    }

    async fn submit_trades(&self, plans: &[TradePlan]) -> Result<Vec<ExecutionOutcome>> {
        Self::submit_trades(self, plans).await
    }

    async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        Self::cancel_orders(self, plans).await
    }

    async fn cancel_orders_fast(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        Self::cancel_orders_fast(self, plans).await
    }

    async fn max_leverage(&self, symbol: &str) -> Result<u32> {
        Self::max_leverage(self, symbol).await
    }

    async fn configure_leverage(&self, symbol: &str, leverage: f64) -> Result<()> {
        Self::configure_leverage(self, symbol, leverage).await
    }

    async fn submit_user_outcome(
        &self,
        action: crate::providers::hyperliquid::exchange::UserOutcomeAction,
    ) -> Result<serde_json::Value> {
        Self::submit_user_outcome(self, action).await
    }
}

pub struct ExecutionAdapter {
    provider: Box<dyn ExecutionProvider>,
}

#[async_trait]
trait AccountEvents: Send {
    async fn next_event(&mut self) -> Result<serde_json::Value>;

    async fn next_bot_events(&mut self) -> Result<Vec<serde_json::Value>> {
        Ok(vec![self.next_event().await?])
    }
}

#[async_trait]
impl AccountEvents for BulkAccountStream {
    async fn next_event(&mut self) -> Result<serde_json::Value> {
        Self::next_event(self).await
    }
}

#[async_trait]
impl AccountEvents for HyperliquidAccountStream {
    async fn next_event(&mut self) -> Result<serde_json::Value> {
        Self::next_event(self).await
    }

    async fn next_bot_events(&mut self) -> Result<Vec<serde_json::Value>> {
        normalize_hyperliquid_account_events(Self::next_event(self).await?)
    }
}

#[async_trait]
impl AccountEvents for HyperlinkAccountStream {
    async fn next_event(&mut self) -> Result<serde_json::Value> {
        Self::next_event(self).await
    }

    async fn next_bot_events(&mut self) -> Result<Vec<serde_json::Value>> {
        normalize_hyperliquid_account_events(Self::next_event(self).await?)
    }
}

/// Construction and streaming boundary for one execution transport.
///
/// Adding a separate exchange means implementing `ExecutionProvider`,
/// `AccountEvents`, and this factory, then registering the factory once in
/// `execution_factory`. Runtime and command modules remain unchanged.
#[async_trait]
trait ExecutionProviderFactory: Send + Sync {
    fn capabilities(&self, venue: ExecutionVenue) -> VenueCapabilities;

    async fn adapter(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<Box<dyn ExecutionProvider>>;

    async fn account_stream(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        account: &str,
    ) -> Result<Box<dyn AccountEvents>>;

    fn normalize_runtime_event(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        account: &str,
        raw: serde_json::Value,
    ) -> Result<AccountRuntimeEvent>;

    async fn connect_transport(&self, testnet: bool) -> Result<()>;
}

struct BulkFactory;
struct HyperliquidFactory;
struct HyperlinkFactory;

static BULK_FACTORY: BulkFactory = BulkFactory;
static HYPERLIQUID_FACTORY: HyperliquidFactory = HyperliquidFactory;
static HYPERLINK_FACTORY: HyperlinkFactory = HyperlinkFactory;

fn execution_factory(venue: ExecutionVenue) -> &'static dyn ExecutionProviderFactory {
    match venue.execution_backend() {
        ExecutionBackend::Bulk => &BULK_FACTORY,
        ExecutionBackend::Hyperliquid => &HYPERLIQUID_FACTORY,
        ExecutionBackend::Hyperlink => &HYPERLINK_FACTORY,
    }
}

#[async_trait]
impl ExecutionProviderFactory for BulkFactory {
    fn capabilities(&self, _venue: ExecutionVenue) -> VenueCapabilities {
        BulkExecutionAdapter::capabilities()
    }

    async fn adapter(
        &self,
        _venue: ExecutionVenue,
        _testnet: bool,
        _account_name: &str,
    ) -> Result<Box<dyn ExecutionProvider>> {
        Ok(Box::new(BulkExecutionAdapter::new()?))
    }

    async fn account_stream(
        &self,
        _venue: ExecutionVenue,
        _testnet: bool,
        account: &str,
    ) -> Result<Box<dyn AccountEvents>> {
        Ok(Box::new(BulkAccountStream::connect(account).await?))
    }

    fn normalize_runtime_event(
        &self,
        _venue: ExecutionVenue,
        _testnet: bool,
        account: &str,
        raw: serde_json::Value,
    ) -> Result<AccountRuntimeEvent> {
        let updates = normalize_bulk_runtime_updates(&raw, account)?;
        Ok(AccountRuntimeEvent {
            raw,
            updates,
            refresh_positions: false,
        })
    }

    async fn connect_transport(&self, _testnet: bool) -> Result<()> {
        BulkExecutionAdapter::new()?.connect_trading().await
    }
}

#[async_trait]
impl ExecutionProviderFactory for HyperliquidFactory {
    fn capabilities(&self, venue: ExecutionVenue) -> VenueCapabilities {
        HyperliquidExecutionAdapter::capabilities_for(
            HyperliquidProduct::from_venue(venue)
                .expect("registered Hyperliquid venue has a product"),
        )
    }

    async fn adapter(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<Box<dyn ExecutionProvider>> {
        Ok(Box::new(
            HyperliquidExecutionAdapter::new_for_account(
                HyperliquidProduct::from_venue(venue)?,
                HyperliquidNetwork::from_testnet(testnet),
                account_name,
            )
            .await?,
        ))
    }

    async fn account_stream(
        &self,
        _venue: ExecutionVenue,
        testnet: bool,
        account: &str,
    ) -> Result<Box<dyn AccountEvents>> {
        Ok(Box::new(
            HyperliquidAccountStream::connect_on(
                account,
                HyperliquidNetwork::from_testnet(testnet),
            )
            .await?,
        ))
    }

    fn normalize_runtime_event(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        _account: &str,
        raw: serde_json::Value,
    ) -> Result<AccountRuntimeEvent> {
        let updates = normalize_hyperliquid_runtime_updates(venue, testnet, &raw)?;
        let refresh_positions = !venue.is_outcome()
            && raw.get("channel").and_then(serde_json::Value::as_str) == Some("user");
        Ok(AccountRuntimeEvent {
            raw,
            updates,
            refresh_positions,
        })
    }

    async fn connect_transport(&self, testnet: bool) -> Result<()> {
        HyperliquidTradingClient::shared(HyperliquidNetwork::from_testnet(testnet))
            .connect()
            .await
    }
}

#[async_trait]
impl ExecutionProviderFactory for HyperlinkFactory {
    fn capabilities(&self, venue: ExecutionVenue) -> VenueCapabilities {
        let product = HyperliquidProduct::from_venue(venue.market_data_id())
            .expect("registered HyperLink venue has a market-data product");
        let mut capabilities = HyperliquidExecutionAdapter::capabilities_for(product);
        capabilities.venue = venue;
        capabilities
    }

    async fn adapter(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<Box<dyn ExecutionProvider>> {
        if testnet {
            bail!("HyperLink does not support testnet");
        }
        if !account_name.trim().is_empty() && !account_name.eq_ignore_ascii_case("main") {
            bail!("HyperLink subaccounts are not supported");
        }
        Ok(Box::new(
            HyperliquidExecutionAdapter::new_hyperlink_for(HyperliquidProduct::from_venue(
                venue.market_data_id(),
            )?)
            .await?,
        ))
    }

    async fn account_stream(
        &self,
        venue: ExecutionVenue,
        _testnet: bool,
        account: &str,
    ) -> Result<Box<dyn AccountEvents>> {
        let credential = credentials::active_hyperlink_credential()?;
        if !credential.account.eq_ignore_ascii_case(account) {
            bail!("HyperLink stream account does not match the configured account");
        }
        Ok(Box::new(
            HyperlinkAccountStream::connect(venue, account, &credential.agent).await?,
        ))
    }

    fn normalize_runtime_event(
        &self,
        venue: ExecutionVenue,
        testnet: bool,
        account: &str,
        raw: serde_json::Value,
    ) -> Result<AccountRuntimeEvent> {
        HYPERLIQUID_FACTORY.normalize_runtime_event(venue, testnet, account, raw)
    }

    async fn connect_transport(&self, _testnet: bool) -> Result<()> {
        HyperliquidTradingClient::shared_hyperlink().connect().await
    }
}

pub struct AccountEventStream {
    venue: ExecutionVenue,
    testnet: bool,
    account: String,
    connection: Box<dyn AccountEvents>,
}

#[derive(Debug)]
pub enum AccountRuntimeUpdate {
    Positions {
        positions: Vec<Position>,
        incremental: bool,
    },
    Order {
        order_id: String,
        status: String,
        event_ms: u64,
        data: serde_json::Value,
    },
    Fill(Fill),
    ScriptEvent(serde_json::Value),
}

#[derive(Debug)]
pub struct AccountRuntimeEvent {
    pub raw: serde_json::Value,
    pub updates: Vec<AccountRuntimeUpdate>,
    pub refresh_positions: bool,
}

impl AccountEventStream {
    pub async fn connect(venue: ExecutionVenue, testnet: bool, account: &str) -> Result<Self> {
        venue.spec()?.validate_network(testnet)?;
        let connection = execution_factory(venue)
            .account_stream(venue, testnet, account)
            .await?;
        Ok(Self {
            venue,
            testnet,
            account: account.to_string(),
            connection,
        })
    }

    pub async fn next_event(&mut self) -> Result<serde_json::Value> {
        self.connection.next_event().await
    }

    pub async fn next_runtime_event(&mut self) -> Result<AccountRuntimeEvent> {
        let raw = self.next_event().await?;
        normalize_runtime_account_event(self.venue, self.testnet, &self.account, raw)
    }

    pub async fn next_bot_events(&mut self) -> Result<Vec<serde_json::Value>> {
        self.connection.next_bot_events().await
    }
}

fn normalize_runtime_account_event(
    venue: ExecutionVenue,
    testnet: bool,
    account: &str,
    raw: serde_json::Value,
) -> Result<AccountRuntimeEvent> {
    execution_factory(venue).normalize_runtime_event(venue, testnet, account, raw)
}

fn normalize_bulk_runtime_updates(
    data: &serde_json::Value,
    account: &str,
) -> Result<Vec<AccountRuntimeUpdate>> {
    let mut updates = Vec::new();
    if let Some(positions) = BulkExecutionAdapter::account_event_positions(data)? {
        updates.push(AccountRuntimeUpdate::Positions {
            positions,
            incremental: data.get("type").and_then(serde_json::Value::as_str)
                == Some("positionUpdate"),
        });
    }
    match data.get("type").and_then(serde_json::Value::as_str) {
        Some("accountSnapshot") => {
            for order in data
                .get("openOrders")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(order_id) = order.get("orderId").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                updates.push(AccountRuntimeUpdate::Order {
                    order_id: order_id.to_string(),
                    status: order
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("resting")
                        .to_string(),
                    event_ms: 0,
                    data: order.clone(),
                });
            }
        }
        Some("orderUpdate") => {
            updates.push(AccountRuntimeUpdate::Order {
                order_id: data
                    .get("oid")
                    .and_then(serde_json::Value::as_str)
                    .context("BULK orderUpdate omitted oid")?
                    .to_string(),
                status: data
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .context("BULK orderUpdate omitted status")?
                    .to_string(),
                event_ms: data
                    .get("ts")
                    .and_then(serde_json::Value::as_u64)
                    .map(crate::providers::bulk::market_data::normalize_timestamp_ms)
                    .unwrap_or_default(),
                data: data.clone(),
            });
        }
        _ => {}
    }
    if let Some(fill) = BulkExecutionAdapter::account_event_fill(data, account)? {
        updates.push(AccountRuntimeUpdate::Fill(fill));
    }
    updates.push(AccountRuntimeUpdate::ScriptEvent(data.clone()));
    Ok(updates)
}

fn normalize_hyperliquid_runtime_updates(
    venue: ExecutionVenue,
    testnet: bool,
    data: &serde_json::Value,
) -> Result<Vec<AccountRuntimeUpdate>> {
    let mut updates = Vec::new();
    match data.get("channel").and_then(serde_json::Value::as_str) {
        Some("orderUpdates") => {
            for update in data
                .get("data")
                .and_then(serde_json::Value::as_array)
                .context("Hyperliquid orderUpdates omitted its update list")?
            {
                let order = update
                    .get("order")
                    .context("Hyperliquid order update omitted order")?;
                let Some(coin) = order.get("coin").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                if !hyperliquid_event_matches_venue(venue, testnet, coin) {
                    continue;
                }
                updates.push(AccountRuntimeUpdate::Order {
                    order_id: json_identifier(
                        order
                            .get("oid")
                            .context("Hyperliquid order update omitted oid")?,
                    )?,
                    status: normalize_hyperliquid_order_status(
                        update
                            .get("status")
                            .and_then(serde_json::Value::as_str)
                            .context("Hyperliquid order update omitted status")?,
                    )
                    .to_string(),
                    event_ms: update
                        .get("statusTimestamp")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                    data: update.clone(),
                });
            }
        }
        Some("user") => {
            for fill in data
                .pointer("/data/fills")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                let Some(coin) = fill.get("coin").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                if !hyperliquid_event_matches_venue(venue, testnet, coin) {
                    continue;
                }
                let product = HyperliquidProduct::from_venue(venue.market_data_id())?;
                if let Some(mut domain_fill) =
                    crate::providers::hyperliquid::execution::account_event_fill(
                        product,
                        HyperliquidNetwork::from_testnet(testnet),
                        fill,
                    )?
                {
                    domain_fill.venue = venue;
                    updates.push(AccountRuntimeUpdate::Fill(domain_fill));
                }
                let symbol = crate::providers::hyperliquid::markets::market_for_wire(
                    product,
                    HyperliquidNetwork::from_testnet(testnet),
                    coin,
                )
                .map(|market| market.symbol.clone())
                .unwrap_or_else(|_| coin.to_string());
                let mut normalized = serde_json::json!({
                    "type": "fill",
                    "venue": venue,
                    "symbol": symbol,
                    "price": fill.get("px").cloned().unwrap_or(serde_json::Value::Null),
                    "size": fill.get("sz").cloned().unwrap_or(serde_json::Value::Null),
                    "side": fill.get("side").cloned().unwrap_or(serde_json::Value::Null),
                    "fee": json_number(fill.get("fee"), "fill fee")?.map(|fee| -fee),
                    "feeAsset": fill.get("feeToken").cloned().unwrap_or(serde_json::Value::Null),
                    "timestamp": fill.get("time").cloned().unwrap_or(serde_json::Value::Null),
                    "raw": fill,
                });
                if let Some(oid) = fill.get("oid") {
                    normalized.as_object_mut().expect("object").insert(
                        "orderId".to_string(),
                        serde_json::Value::String(json_identifier(oid)?),
                    );
                }
                updates.push(AccountRuntimeUpdate::ScriptEvent(normalized));
            }
        }
        Some("allDexsClearinghouseState") => {
            let product = HyperliquidProduct::from_venue(venue.market_data_id())?;
            if let Some(mut positions) =
                crate::providers::hyperliquid::execution::all_dex_account_event_positions(
                    product,
                    HyperliquidNetwork::from_testnet(testnet),
                    data,
                )?
            {
                for position in &mut positions {
                    position.venue = venue;
                }
                updates.push(AccountRuntimeUpdate::Positions {
                    positions,
                    incremental: false,
                });
            }
        }
        _ => {}
    }
    Ok(updates)
}

fn hyperliquid_event_matches_venue(venue: ExecutionVenue, testnet: bool, coin: &str) -> bool {
    if venue.is_outcome() {
        return crate::providers::hyperliquid::outcomes::parse_wire_symbol(coin).is_ok();
    }
    let Ok(product) = HyperliquidProduct::from_venue(venue.market_data_id()) else {
        return false;
    };
    crate::providers::hyperliquid::markets::market_for_wire(
        product,
        HyperliquidNetwork::from_testnet(testnet),
        coin,
    )
    .is_ok()
}

pub(crate) fn normalize_hyperliquid_account_events(
    value: serde_json::Value,
) -> Result<Vec<serde_json::Value>> {
    use serde_json::Value;

    match value.get("channel").and_then(Value::as_str) {
        Some("orderUpdates") => value
            .get("data")
            .and_then(Value::as_array)
            .context("Hyperliquid orderUpdates omitted its update list")?
            .iter()
            .map(normalize_hyperliquid_order_update)
            .collect(),
        Some("user") => value
            .pointer("/data/fills")
            .and_then(Value::as_array)
            .map_or_else(
                || Ok(Vec::new()),
                |fills| fills.iter().map(normalize_hyperliquid_fill).collect(),
            ),
        _ => Ok(Vec::new()),
    }
}

fn normalize_hyperliquid_order_update(update: &serde_json::Value) -> Result<serde_json::Value> {
    use serde_json::Value;

    let order = update
        .get("order")
        .context("Hyperliquid order update omitted order")?;
    let order_id = json_identifier(
        order
            .get("oid")
            .context("Hyperliquid order update omitted oid")?,
    )?;
    let raw_status = update
        .get("status")
        .and_then(Value::as_str)
        .context("Hyperliquid order update omitted status")?;
    let size = json_number(order.get("sz"), "order size")?.unwrap_or_default();
    let original_size = json_number(order.get("origSz"), "original order size")?.unwrap_or(size);
    let is_buy = order.get("side").and_then(Value::as_str) != Some("A");
    Ok(serde_json::json!({
        "type": "orderUpdate",
        "oid": order_id,
        "status": normalize_hyperliquid_order_status(raw_status),
        "ts": update.get("statusTimestamp").and_then(Value::as_u64).unwrap_or_default(),
        "px": json_number(order.get("limitPx"), "order price")?.unwrap_or_default(),
        "origSz": original_size,
        "sz": if is_buy { size } else { -size },
        "isBuy": is_buy,
    }))
}

fn normalize_hyperliquid_fill(fill: &serde_json::Value) -> Result<serde_json::Value> {
    use serde_json::Value;

    let order_id = json_identifier(fill.get("oid").context("Hyperliquid fill omitted oid")?)?;
    let raw_fee = json_number(fill.get("fee"), "fill fee")?;
    Ok(serde_json::json!({
        "type": "fill",
        "orderId": order_id,
        "timestamp": fill.get("time").and_then(Value::as_u64).unwrap_or_default(),
        "isBuy": fill.get("side").and_then(Value::as_str) == Some("B"),
        "size": json_number(fill.get("sz"), "fill size")?.unwrap_or_default(),
        "price": json_number(fill.get("px"), "fill price")?.unwrap_or_default(),
        "fee": raw_fee.map(|fee| -fee),
    }))
}

fn normalize_hyperliquid_order_status(status: &str) -> &str {
    if status.eq_ignore_ascii_case("open") {
        "resting"
    } else if status.eq_ignore_ascii_case("filled") {
        "filled"
    } else if status.eq_ignore_ascii_case("canceled") || status.eq_ignore_ascii_case("cancelled") {
        "cancelled"
    } else if status.eq_ignore_ascii_case("rejected") || status.ends_with("Canceled") {
        "rejected"
    } else {
        status
    }
}

fn json_identifier(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        _ => bail!("Hyperliquid order id is neither a string nor an integer"),
    }
}

fn json_number(value: Option<&serde_json::Value>, field: &str) -> Result<Option<f64>> {
    value
        .map(|value| match value {
            serde_json::Value::Number(number) => number
                .as_f64()
                .with_context(|| format!("invalid Hyperliquid {field}")),
            serde_json::Value::String(number) => number
                .parse::<f64>()
                .with_context(|| format!("invalid Hyperliquid {field} `{number}`")),
            serde_json::Value::Null => Ok(0.0),
            _ => bail!("invalid Hyperliquid {field}"),
        })
        .transpose()
}

pub async fn connect_execution_transport(venue: ExecutionVenue, testnet: bool) -> Result<()> {
    venue.spec()?.validate_network(testnet)?;
    execution_factory(venue).connect_transport(testnet).await
}

pub async fn connect_execution_transports(
    venues: &[ExecutionVenue],
    testnet: bool,
) -> Vec<(ExecutionVenue, Result<()>)> {
    let transports = execution_transports(venues);

    futures_util::future::join_all(transports.into_iter().map(|venue| async move {
        let venue_testnet = testnet
            && venue
                .spec()
                .is_ok_and(|spec| spec.network == crate::venues::NetworkPolicy::Selectable);
        (
            venue,
            connect_execution_transport(venue, venue_testnet).await,
        )
    }))
    .await
}

fn execution_transports(venues: &[ExecutionVenue]) -> Vec<ExecutionVenue> {
    let mut transports = Vec::new();
    for venue in venues {
        let transport = match venue.execution_backend() {
            ExecutionBackend::Bulk => ExecutionVenue::Bulk,
            ExecutionBackend::Hyperliquid => ExecutionVenue::Hyperliquid,
            ExecutionBackend::Hyperlink => ExecutionVenue::Hyperlink,
        };
        if !transports.contains(&transport) {
            transports.push(transport);
        }
    }
    transports
}

impl ExecutionAdapter {
    pub async fn new(venue: ExecutionVenue, testnet: bool) -> Result<Self> {
        Self::new_for_account(venue, testnet, "main").await
    }

    pub async fn new_for_account(
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<Self> {
        let spec = venue.spec()?;
        spec.validate_network(testnet)?;
        let provider = execution_factory(venue)
            .adapter(venue, testnet, account_name)
            .await?;
        Ok(Self { provider })
    }

    pub fn capabilities(venue: ExecutionVenue) -> VenueCapabilities {
        execution_factory(venue).capabilities(venue)
    }

    pub fn venue_capabilities(&self) -> VenueCapabilities {
        self.provider.venue_capabilities()
    }

    pub fn validate_order_id(&self, order_id: &str) -> Result<()> {
        self.provider.validate_order_id(order_id)
    }

    pub fn configured_account(venue: ExecutionVenue) -> Result<String> {
        Self::configured_account_for(venue, false, "main")
    }

    pub fn configured_account_for(
        venue: ExecutionVenue,
        testnet: bool,
        account_name: &str,
    ) -> Result<String> {
        let spec = venue.spec()?;
        spec.validate_network(testnet)?;
        match spec.auth {
            AuthBackend::Bulk => credentials::bulk_account_for(account_name),
            AuthBackend::Hyperliquid => credentials::hyperliquid_account_for(
                HyperliquidNetwork::from_testnet(testnet),
                account_name,
            ),
            AuthBackend::Hyperlink => credentials::hyperlink_account_for(account_name),
        }
    }

    pub fn configured_accounts(
        venue: ExecutionVenue,
        testnet: bool,
    ) -> Result<Vec<(String, String)>> {
        let spec = venue.spec()?;
        spec.validate_network(testnet)?;
        match spec.auth {
            AuthBackend::Bulk => credentials::bulk_accounts(),
            AuthBackend::Hyperliquid => {
                credentials::hyperliquid_accounts(HyperliquidNetwork::from_testnet(testnet))
            }
            AuthBackend::Hyperlink => credentials::hyperlink_accounts(),
        }
    }

    pub async fn account_snapshot(&self, account: &str) -> Result<AccountSnapshot> {
        self.provider.account_snapshot(account).await
    }

    pub async fn account_snapshot_for_market(
        &self,
        account: &str,
        symbol: &str,
    ) -> Result<AccountSnapshot> {
        self.provider
            .account_snapshot_for_market(account, symbol)
            .await
    }

    pub async fn max_leverage(&self, symbol: &str) -> Result<u32> {
        self.provider.max_leverage(symbol).await
    }

    pub async fn configure_leverage(&self, symbol: &str, leverage: f64) -> Result<()> {
        self.provider.configure_leverage(symbol, leverage).await
    }

    pub async fn open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        self.provider.open_orders(account).await
    }

    pub async fn open_orders_for_market(
        &self,
        account: &str,
        symbol: &str,
    ) -> Result<Vec<OpenOrder>> {
        self.provider.open_orders_for_market(account, symbol).await
    }

    pub async fn fills(&self, account: &str) -> Result<Vec<Fill>> {
        self.provider.fills(account).await
    }

    pub async fn recover_account_gap(
        &self,
        account: &str,
        since_ms: u64,
    ) -> Result<AccountGapRecovery> {
        self.provider.recover_account_gap(account, since_ms).await
    }

    pub async fn submit_trade(&self, plan: &TradePlan) -> Result<ExecutionReceipt> {
        self.provider.submit_trade(plan).await
    }

    pub async fn submit_user_outcome(
        &self,
        action: crate::providers::hyperliquid::exchange::UserOutcomeAction,
    ) -> Result<serde_json::Value> {
        self.provider.submit_user_outcome(action).await
    }

    pub async fn cancel_order(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        self.provider.cancel_order(plan).await
    }

    pub async fn cancel_order_fast(&self, plan: &CancelPlan) -> Result<ExecutionReceipt> {
        self.provider.cancel_order_fast(plan).await
    }

    pub async fn submit_trades(&self, plans: &[TradePlan]) -> Result<Vec<ExecutionOutcome>> {
        self.provider.submit_trades(plans).await
    }

    pub async fn cancel_orders(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        self.provider.cancel_orders(plans).await
    }

    pub async fn cancel_orders_fast(&self, plans: &[CancelPlan]) -> Result<Vec<ExecutionOutcome>> {
        self.provider.cancel_orders_fast(plans).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperlink_capabilities_follow_the_registered_product() {
        let perpetual = ExecutionAdapter::capabilities(ExecutionVenue::Hyperlink);
        assert_eq!(perpetual.venue, ExecutionVenue::Hyperlink);
        assert!(perpetual.reduce_only);
        assert!(perpetual.integer_leverage);

        let spot = ExecutionAdapter::capabilities(ExecutionVenue::HyperlinkSpot);
        assert_eq!(spot.venue, ExecutionVenue::HyperlinkSpot);
        assert!(!spot.reduce_only);
        assert!(!spot.integer_leverage);
        assert!(!spot.configure_leverage_before_orders);
    }

    #[test]
    fn hyperlink_hip3_state_replaces_positions_across_dexes() {
        let event = serde_json::json!({
            "channel": "allDexsClearinghouseState",
            "data": {
                "user": "0xabc",
                "clearinghouseStates": [[
                    "xyz",
                    {
                        "marginSummary": {
                            "accountValue": "1000",
                            "totalNtlPos": "2000",
                            "totalMarginUsed": "200"
                        },
                        "withdrawable": "800",
                        "assetPositions": [{
                            "position": {
                                "coin": "TSLA",
                                "entryPx": "190",
                                "leverage": { "type": "cross", "value": 10 },
                                "liquidationPx": "100",
                                "positionValue": "2000",
                                "szi": "10",
                                "unrealizedPnl": "100",
                                "cumFunding": { "sinceOpen": "-1" }
                            }
                        }],
                        "time": 1_785_000_000_000_u64
                    }
                ]]
            }
        });

        let updates =
            normalize_hyperliquid_runtime_updates(ExecutionVenue::Hyperlink, false, &event)
                .expect("valid HyperLink all-dex state");
        let AccountRuntimeUpdate::Positions {
            positions,
            incremental,
        } = &updates[0]
        else {
            panic!("expected a position snapshot");
        };
        assert!(!incremental);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].venue, ExecutionVenue::Hyperlink);
        assert_eq!(positions[0].internal_symbol, "xyz:TSLA");
        assert_eq!(positions[0].venue_symbol, "xyz:TSLA");
        assert_eq!(positions[0].mark_price, 200.0);
    }

    #[test]
    fn execution_transport_selection_is_targeted_and_deduplicated() {
        assert_eq!(
            execution_transports(&[
                ExecutionVenue::Hyperlink,
                ExecutionVenue::HyperlinkSpot,
                ExecutionVenue::Hyperliquid,
                ExecutionVenue::Hyperliquid,
            ]),
            [ExecutionVenue::Hyperlink, ExecutionVenue::Hyperliquid]
        );
    }
}
