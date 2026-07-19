use anyhow::Result;
use std::sync::Arc;

pub use crate::markets::Market as BulkMarket;

/// BULK-specific convenience over the provider-neutral market registry.
pub fn market(symbol: &str) -> Result<Arc<BulkMarket>> {
    crate::markets::exchange_market("bulk", symbol)
}
