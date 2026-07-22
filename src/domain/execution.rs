use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionVenue {
    Bulk,
    Hyperliquid,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionDirection {
    Long,
    Short,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl From<PositionDirection> for OrderSide {
    fn from(direction: PositionDirection) -> Self {
        match direction {
            PositionDirection::Long => Self::Buy,
            PositionDirection::Short => Self::Sell,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Market,
    Limit,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Alo,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VenueCapabilities {
    pub venue: ExecutionVenue,
    pub order_kinds: Vec<OrderKind>,
    pub time_in_forces: Vec<TimeInForce>,
    pub reduce_only: bool,
    pub deterministic_order_ids: bool,
    pub delegated_agent_signing: bool,
    pub native_protective_triggers: bool,
    pub native_oco: bool,
    pub native_on_fill: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradePlan {
    pub created_at_ms: u64,
    pub venue: ExecutionVenue,
    pub account: String,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub direction: PositionDirection,
    pub side: OrderSide,
    pub order_kind: OrderKind,
    pub time_in_force: Option<TimeInForce>,
    pub requested_size: Option<f64>,
    pub size: f64,
    pub price: Option<f64>,
    pub reference_price: f64,
    pub requested_margin: Option<f64>,
    pub estimated_margin: f64,
    pub estimated_exposure: f64,
    /// BULK does not expose a pre-trade portfolio-liquidation simulation.
    pub projected_liquidation_price: Option<f64>,
    pub leverage: f64,
    pub reduce_only: bool,
    #[serde(default)]
    pub stop_loss_price: Option<f64>,
    #[serde(default)]
    pub take_profit_price: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccountSnapshot {
    pub venue: ExecutionVenue,
    pub account: String,
    pub fetched_at_ms: u64,
    pub margin: MarginSummary,
    pub positions: Vec<Position>,
    pub open_orders: Vec<OpenOrder>,
    pub leverage_settings: Vec<LeverageSetting>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MarginSummary {
    pub total_balance: f64,
    pub available_balance: f64,
    pub margin_used: f64,
    pub notional: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub funding: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Position {
    pub venue: ExecutionVenue,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub registry_supported: bool,
    pub direction: PositionDirection,
    pub size: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub notional: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub leverage: f64,
    pub liquidation_price: f64,
    pub fees: f64,
    pub funding: f64,
    pub maintenance_margin: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenOrder {
    pub venue: ExecutionVenue,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub registry_supported: bool,
    pub order_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub original_size: f64,
    pub remaining_size: f64,
    pub filled_size: f64,
    pub vwap: f64,
    pub maker: bool,
    pub reduce_only: bool,
    pub time_in_force: String,
    pub status: String,
    pub ts_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Fill {
    pub venue: ExecutionVenue,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub registry_supported: bool,
    pub side: OrderSide,
    pub amount: f64,
    pub price: f64,
    pub reason: String,
    pub order_id: Option<String>,
    pub maker: bool,
    /// Signed venue fee: negative is a cost and positive is a rebate.
    #[serde(default)]
    pub fee: Option<f64>,
    pub slot: u64,
    pub ts_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderRecord {
    pub venue: ExecutionVenue,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub registry_supported: bool,
    pub order_id: String,
    pub side: OrderSide,
    pub order_kind: String,
    pub time_in_force: String,
    pub price: f64,
    pub vwap: f64,
    pub original_size: f64,
    pub executed_size: f64,
    pub reduce_only: bool,
    pub status: String,
    pub reason: Option<String>,
    pub slot: u64,
    pub ts_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LeverageSetting {
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub registry_supported: bool,
    pub leverage: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExecutionReceipt {
    pub venue: ExecutionVenue,
    pub account: String,
    pub order_id: Option<String>,
    pub status: String,
    pub terminal: bool,
    pub submitted_at_ms: u64,
    pub raw_status: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CancelPlan {
    pub created_at_ms: u64,
    pub venue: ExecutionVenue,
    pub account: String,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub order_id: String,
}
