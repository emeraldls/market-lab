//! Extra context carried by contract trades, without another script stream.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct OnchainTrade {
    pub chain_id: u64,
    pub market: String,
    pub kind: String,
    pub block_number: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub timestamp_ms: u64,
    pub side: String,
    pub sender: String,
    pub recipient: String,
    pub base: TradeAsset,
    pub quote: TradeAsset,
    pub base_amount: String,
    pub quote_amount: String,
    pub fee: TradeFee,
    /// Marginal quote/base price at the end of this confirmed block, not the fill price.
    pub current_price: Option<f64>,
    pub volume: TradeVolume,
    /// Normalized contract snapshot at block_hash. All trades in a block share its final state.
    pub state: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TradeAsset {
    /// None denotes the chain's native asset.
    pub address: Option<String>,
    pub symbol: String,
    pub decimals: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TradeFee {
    pub asset: TradeAsset,
    pub amount: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TradeVolume {
    /// Inclusive first observed block. Totals are session volume, NOT rolling 24h volume.
    pub from_block: u64,
    pub through_log_index: u64,
    pub trades: u64,
    pub base: String,
    pub quote: String,
}
