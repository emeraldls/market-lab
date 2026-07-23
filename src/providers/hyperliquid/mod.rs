pub mod client;
pub mod exchange;
pub mod execution;
pub mod market_data;
pub mod markets;
pub mod signing;
pub mod ws;

pub const EXCHANGE: &str = "hyperliquid";
pub const HTTP_URL: &str = "https://api.hyperliquid-testnet.xyz";
pub const WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
