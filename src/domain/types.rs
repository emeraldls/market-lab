use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ms: u64,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopOfBook {
    pub timestamp_ms: u64,
    pub best_bid: Option<OrderBookLevel>,
    pub best_ask: Option<OrderBookLevel>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlippageEstimate {
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub notional: f64,
    pub at: u64,
    pub avg_fill_price: f64,
    pub best_price: f64,
    pub slippage_abs: f64,
    pub slippage_bps: f64,
    pub levels_consumed: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImbalanceEstimate {
    pub exchange: String,
    pub symbol: String,
    pub at: u64,
    pub depth: u16,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub imbalance: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VampEstimate {
    pub exchange: String,
    pub symbol: String,
    pub at: u64,
    pub dollar_depth: f64,
    pub ask_vwap: f64,
    pub bid_vwap: f64,
    pub vamp: f64,
    pub ask_levels_consumed: u16,
    pub bid_levels_consumed: u16,
    pub max_reachable_quote_ask: f64,
    pub max_reachable_quote_bid: f64,
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider: String,
    pub status: String,
    pub details: serde_json::Value,
}
