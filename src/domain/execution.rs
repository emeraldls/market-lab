use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionVenue {
    #[serde(rename = "bulkf")]
    Bulk,
    #[serde(rename = "hyperliquidf")]
    Hyperliquid,
    #[serde(rename = "hyperliquidf-xyz")]
    HyperliquidXyz,
    #[serde(rename = "hyperliquid")]
    HyperliquidSpot,
    #[serde(rename = "hyperliquid-outcomes")]
    HyperliquidOutcomes,
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
    /// Routes Hyperliquid execution to testnet. Ignored by other venues.
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
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
    /// Perpetual leverage. Spot plans omit this field entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leverage: Option<f64>,
    pub reduce_only: bool,
    #[serde(default)]
    pub stop_loss_price: Option<f64>,
    #[serde(default)]
    pub take_profit_price: Option<f64>,
    /// Dynamic market-definition identity. Outcome orders refuse to execute
    /// if the venue rotates the selected contract before submission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_fingerprint: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccountSnapshot {
    pub venue: ExecutionVenue,
    pub account: String,
    pub fetched_at_ms: u64,
    pub margin: MarginSummary,
    pub positions: Vec<Position>,
    #[serde(default)]
    pub spot_balances: Vec<SpotBalance>,
    #[serde(default)]
    pub outcome_holdings: Vec<OutcomeHolding>,
    pub open_orders: Vec<OpenOrder>,
    pub leverage_settings: Vec<LeverageSetting>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SpotBalance {
    pub venue: ExecutionVenue,
    pub asset: String,
    pub venue_asset: String,
    pub token_index: u32,
    pub registry_supported: bool,
    pub total: f64,
    pub held: f64,
    pub available: f64,
    pub entry_notional: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeHolding {
    pub venue: ExecutionVenue,
    pub symbol: String,
    pub outcome_id: u32,
    pub side: u8,
    pub side_name: String,
    pub question_id: Option<u32>,
    pub question_name: Option<String>,
    pub outcome_name: String,
    pub quote_token: String,
    pub venue_asset: String,
    pub total: f64,
    pub held: f64,
    pub available: f64,
    pub entry_notional: f64,
    pub metadata_fingerprint: String,
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
    #[serde(default)]
    pub fee_asset: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_size: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filled_size: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub average_fill_price: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionOutcome {
    #[serde(default)]
    pub receipt: Option<ExecutionReceipt>,
    #[serde(default)]
    pub error: Option<String>,
}

impl ExecutionOutcome {
    pub fn success(receipt: ExecutionReceipt) -> Self {
        Self {
            receipt: Some(receipt),
            error: None,
        }
    }

    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            receipt: None,
            error: Some(error.into()),
        }
    }

    pub fn into_result(self) -> Result<ExecutionReceipt, String> {
        match (self.receipt, self.error) {
            (Some(receipt), None) => Ok(receipt),
            (None, Some(error)) => Err(error),
            (Some(_), Some(error)) => Err(format!(
                "execution outcome contained both a receipt and an error: {error}"
            )),
            (None, None) => Err("execution outcome omitted both receipt and error".to_string()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CancelPlan {
    pub created_at_ms: u64,
    pub venue: ExecutionVenue,
    /// Routes Hyperliquid execution to testnet. Ignored by other venues.
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
    pub account: String,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub order_id: String,
}

// Persisted plans created before mainnet became the default were always
// Hyperliquid testnet plans. Keep them on testnet when they are deserialized.
const fn legacy_hyperliquid_testnet() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_perps_use_the_canonical_id() {
        let encoded = serde_json::to_value(ExecutionVenue::Bulk).expect("venue serializes");
        assert_eq!(encoded, serde_json::json!("bulkf"));
        assert!(serde_json::from_value::<ExecutionVenue>(serde_json::json!("bulk")).is_err());
    }

    #[test]
    fn hyperliquid_perps_use_the_canonical_id_and_read_legacy_jobs() {
        let encoded = serde_json::to_value(ExecutionVenue::Hyperliquid).expect("venue serializes");
        assert_eq!(encoded, serde_json::json!("hyperliquidf"));

        let spot = serde_json::to_value(ExecutionVenue::HyperliquidSpot).expect("venue serializes");
        assert_eq!(spot, serde_json::json!("hyperliquid"));

        let xyz =
            serde_json::to_value(ExecutionVenue::HyperliquidXyz).expect("XYZ venue serializes");
        assert_eq!(xyz, serde_json::json!("hyperliquidf-xyz"));

        let outcomes = serde_json::to_value(ExecutionVenue::HyperliquidOutcomes)
            .expect("outcome venue serializes");
        assert_eq!(outcomes, serde_json::json!("hyperliquid-outcomes"));
    }

    #[test]
    fn spot_trade_plans_omit_leverage() {
        let plan = TradePlan {
            created_at_ms: 1,
            venue: ExecutionVenue::HyperliquidSpot,
            testnet: true,
            account: "0xabc".to_string(),
            internal_symbol: "HYPE/USDC".to_string(),
            venue_symbol: "@1035".to_string(),
            direction: PositionDirection::Long,
            side: OrderSide::Buy,
            order_kind: OrderKind::Market,
            time_in_force: None,
            requested_size: None,
            size: 1.0,
            price: None,
            reference_price: 44.0,
            requested_margin: Some(44.0),
            estimated_margin: 44.0,
            estimated_exposure: 44.0,
            projected_liquidation_price: None,
            leverage: None,
            reduce_only: false,
            stop_loss_price: None,
            take_profit_price: None,
            market_fingerprint: None,
        };

        let encoded = serde_json::to_value(plan).expect("spot plan serializes");
        assert!(encoded.get("leverage").is_none());
    }
}
