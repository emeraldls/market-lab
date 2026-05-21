use anyhow::{Result, bail};

use crate::domain::types::{OrderBookLevel, OrderBookSnapshot, VampEstimate};

pub fn estimate_vamp(book: &OrderBookSnapshot, at: u64, dollar_depth: f64) -> Result<VampEstimate> {
    if dollar_depth <= 0.0 {
        bail!("dollar_depth must be > 0");
    }

    let max_reachable_quote_ask = total_quote_capacity(&book.asks);
    let max_reachable_quote_bid = total_quote_capacity(&book.bids);

    let ask_fill = side_vwap_for_quote_notional(&book.asks, dollar_depth)?;
    let bid_fill = side_vwap_for_quote_notional(&book.bids, dollar_depth)?;

    let ask_vwap = ask_fill.vwap;
    let bid_vwap = bid_fill.vwap;
    let vamp = (ask_vwap + bid_vwap) / 2.0;

    let complete = ask_fill.filled_quote >= dollar_depth && bid_fill.filled_quote >= dollar_depth;

    Ok(VampEstimate {
        exchange: book.exchange.clone(),
        symbol: book.symbol.clone(),
        at,
        dollar_depth,
        ask_vwap,
        bid_vwap,
        vamp,
        ask_levels_consumed: ask_fill.levels_consumed,
        bid_levels_consumed: bid_fill.levels_consumed,
        max_reachable_quote_ask,
        max_reachable_quote_bid,
        complete,
    })
}

fn total_quote_capacity(levels: &[OrderBookLevel]) -> f64 {
    levels.iter().map(|l| l.price * l.quantity).sum()
}

struct SideFill {
    vwap: f64,
    levels_consumed: u16,
    filled_quote: f64,
}

fn side_vwap_for_quote_notional(levels: &[OrderBookLevel], target_quote: f64) -> Result<SideFill> {
    if levels.is_empty() {
        bail!("orderbook side is empty");
    }

    let mut remaining_quote = target_quote;
    let mut total_quote = 0.0_f64;
    let mut total_base = 0.0_f64;
    let mut levels_consumed = 0_u16;

    for level in levels {
        if remaining_quote <= 0.0 {
            break;
        }

        let level_quote_capacity = level.price * level.quantity;
        let take_quote = remaining_quote.min(level_quote_capacity);
        let take_base = take_quote / level.price;

        total_quote += take_quote;
        total_base += take_base;
        remaining_quote -= take_quote;
        levels_consumed += 1;
    }

    if total_base <= 0.0 {
        bail!("computed base fill is zero");
    }

    Ok(SideFill {
        vwap: total_quote / total_base,
        levels_consumed,
        filled_quote: total_quote,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_vamp_from_symmetric_book() {
        let book = OrderBookSnapshot {
            exchange: "x".to_string(),
            symbol: "btc/usd".to_string(),
            timestamp_ms: 1,
            bids: vec![OrderBookLevel {
                price: 99.0,
                quantity: 10.0,
            }],
            asks: vec![OrderBookLevel {
                price: 101.0,
                quantity: 10.0,
            }],
        };

        let out = estimate_vamp(&book, 1, 100.0).expect("vamp should compute");
        assert!(out.vamp > 99.0 && out.vamp < 101.0);
        assert!(out.complete);
    }

    #[test]
    fn marks_incomplete_when_depth_is_insufficient() {
        let book = OrderBookSnapshot {
            exchange: "x".to_string(),
            symbol: "btc/usd".to_string(),
            timestamp_ms: 1,
            bids: vec![OrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            }],
            asks: vec![OrderBookLevel {
                price: 101.0,
                quantity: 1.0,
            }],
        };

        let out = estimate_vamp(&book, 1, 1_000.0).expect("vamp should still compute");
        assert!(!out.complete);
        assert!(out.max_reachable_quote_bid < 1_000.0);
        assert!(out.max_reachable_quote_ask < 1_000.0);
    }
}
