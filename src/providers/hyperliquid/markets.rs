use std::sync::Arc;

use anyhow::Result;

pub use crate::markets::Market as HyperliquidMarket;

pub fn market(symbol: &str) -> Result<Arc<HyperliquidMarket>> {
    crate::markets::exchange_market("hyperliquid", symbol)
}
