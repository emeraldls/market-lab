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
    pub avg_fill_price: f64,
    pub best_price: f64,
    pub slippage_abs: f64,
    pub slippage_bps: f64,
    pub levels_consumed: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImbalanceEstimate {
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub imbalance: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpreadEstimate {
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread_abs: f64,
    pub spread_bps: f64,
    pub mid: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DepthEstimate {
    pub bid_base: f64,
    pub ask_base: f64,
    pub bid_quote: f64,
    pub ask_quote: f64,
    pub total_quote: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VampEstimate {
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
pub struct OhlcvtCandle {
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub vb: f64,
    pub vs: f64,
    pub tb: u64,
    pub ts: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandleSeries {
    pub exchange: String,
    pub symbol: String,
    pub tf: String,
    pub from: u64,
    pub to: u64,
    pub points: usize,
    pub data: Vec<OhlcvtCandle>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OhlcvCandle {
    pub t: u64,
    pub close_time: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub volume: f64,
    pub trades: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OhlcvSeries {
    pub exchange: String,
    pub symbol: String,
    pub tf: String,
    pub from: u64,
    pub to: u64,
    pub points: usize,
    pub data: Vec<OhlcvCandle>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeBar {
    pub t: u64,
    pub close_time: u64,
    pub volume: f64,
    pub trades: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeBarSeries {
    pub exchange: String,
    pub symbol: String,
    pub tf: String,
    pub from: u64,
    pub to: u64,
    pub points: usize,
    pub data: Vec<VolumeBar>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradeTick {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ms: u64,
    pub price: f64,
    pub size: f64,
    pub taker_buy: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeDeltaTick {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ms: u64,
    pub delta: f64,
    pub cumulative_delta: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketTicker {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ms: u64,
    pub price_change: f64,
    pub price_change_percent: f64,
    pub last_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub mark_price: f64,
    pub oracle_price: f64,
    pub open_interest: f64,
    pub funding_rate: f64,
    pub regime: i32,
    pub regime_dt: u64,
    pub regime_vol: f64,
    pub regime_mv: f64,
    pub fair_book_price: f64,
    pub fair_vol: f64,
    pub fair_bias: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenInterestSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ms: u64,
    pub open_interest: f64,
    pub mark_price: f64,
    pub notional: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FundingRateSnapshot {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ms: u64,
    pub current: f64,
    pub annualized: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketStatistics {
    pub symbol: String,
    pub volume: f64,
    pub quote_volume: f64,
    pub open_interest: f64,
    pub funding_rate: f64,
    pub funding_rate_annualized: f64,
    pub last_price: f64,
    pub mark_price: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExchangeStatistics {
    pub exchange: String,
    pub timestamp_ms: u64,
    pub period: String,
    pub total_volume_usd: f64,
    pub total_open_interest_usd: f64,
    pub markets: Vec<MarketStatistics>,
    pub funding: Vec<FundingRateSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VdCandle {
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub n: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VdSeries {
    pub exchange: String,
    pub symbol: String,
    pub tf: String,
    pub from: u64,
    pub to: u64,
    pub bucket: u8,
    pub points: usize,
    pub data: Vec<VdCandle>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OiCandle {
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub n: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OiSeries {
    pub exchange: String,
    pub symbol: String,
    pub tf: String,
    pub from: u64,
    pub to: u64,
    pub points: usize,
    pub data: Vec<OiCandle>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeProfile {
    pub t: u64,
    pub p: Vec<f64>,
    pub b: Vec<f64>,
    pub s: Vec<f64>,
    pub pg: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeProfileSeries {
    pub exchange: String,
    pub symbol: String,
    pub tf: String,
    pub from: u64,
    pub to: u64,
    pub points: usize,
    pub data: Vec<VolumeProfile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CvdStudyResult {
    pub points: usize,
    pub first_close: f64,
    pub last_close: f64,
    pub delta: f64,
    pub candles: Vec<VdCandle>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub provider: String,
    pub status: String,
    pub details: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemStatus {
    pub app: String,
    pub version: String,
    pub provider: String,
    pub command_groups: Vec<String>,
    pub sources: Vec<String>,
    pub studies: Vec<String>,
    pub strategies: Vec<String>,
    pub provider_health: ProviderHealth,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpgradeStatus {
    pub app: String,
    pub current_version: String,
    pub latest_version: String,
    pub target: String,
    pub up_to_date: bool,
    pub updated: bool,
    pub asset_url: String,
}
