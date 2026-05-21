use anyhow::{Result, bail};

use crate::domain::types::{ImbalanceEstimate, OrderBookSnapshot};

pub fn estimate_imbalance(
    book: &OrderBookSnapshot,
    at: u64,
    depth: u16,
) -> Result<ImbalanceEstimate> {
    if depth == 0 {
        bail!("depth must be >= 1");
    }

    let n = depth as usize;
    let bid_volume: f64 = book.bids.iter().take(n).map(|l| l.quantity).sum();
    let ask_volume: f64 = book.asks.iter().take(n).map(|l| l.quantity).sum();
    let denom = bid_volume + ask_volume;

    if denom <= 0.0 {
        bail!("empty book volumes at requested depth");
    }

    let imbalance = (bid_volume - ask_volume) / denom;

    Ok(ImbalanceEstimate {
        exchange: book.exchange.clone(),
        symbol: book.symbol.clone(),
        at,
        depth,
        bid_volume,
        ask_volume,
        imbalance,
    })
}
