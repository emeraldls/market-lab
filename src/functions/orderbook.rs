use anyhow::{Result, bail};

use crate::domain::enums::Side;
use crate::domain::types::{
    DepthEstimate, ImbalanceEstimate, OrderBookLevel, OrderBookSnapshot, SlippageEstimate,
    SpreadEstimate, VampEstimate,
};

pub fn spread(book: &OrderBookSnapshot) -> Result<SpreadEstimate> {
    let best_bid = book
        .bids
        .first()
        .map(|level| level.price)
        .ok_or_else(|| anyhow::anyhow!("bids are empty"))?;
    let best_ask = book
        .asks
        .first()
        .map(|level| level.price)
        .ok_or_else(|| anyhow::anyhow!("asks are empty"))?;
    let spread_abs = best_ask - best_bid;
    let mid = (best_ask + best_bid) / 2.0;
    let spread_bps = if mid > 0.0 {
        (spread_abs / mid) * 10_000.0
    } else {
        0.0
    };

    Ok(SpreadEstimate {
        best_bid,
        best_ask,
        spread_abs,
        spread_bps,
        mid,
    })
}

pub fn depth(book: &OrderBookSnapshot, levels: u16) -> Result<DepthEstimate> {
    if levels == 0 {
        bail!("levels must be >= 1");
    }

    let levels = levels as usize;
    let bid_base = book
        .bids
        .iter()
        .take(levels)
        .map(|level| level.quantity)
        .sum::<f64>();
    let ask_base = book
        .asks
        .iter()
        .take(levels)
        .map(|level| level.quantity)
        .sum::<f64>();
    let bid_quote = book
        .bids
        .iter()
        .take(levels)
        .map(|level| level.price * level.quantity)
        .sum::<f64>();
    let ask_quote = book
        .asks
        .iter()
        .take(levels)
        .map(|level| level.price * level.quantity)
        .sum::<f64>();

    Ok(DepthEstimate {
        bid_base,
        ask_base,
        bid_quote,
        ask_quote,
        total_quote: bid_quote + ask_quote,
    })
}

pub fn imbalance(book: &OrderBookSnapshot, levels: u16) -> Result<ImbalanceEstimate> {
    if levels == 0 {
        bail!("depth must be >= 1");
    }

    let levels = levels as usize;
    let bid_volume = book
        .bids
        .iter()
        .take(levels)
        .map(|level| level.quantity)
        .sum::<f64>();
    let ask_volume = book
        .asks
        .iter()
        .take(levels)
        .map(|level| level.quantity)
        .sum::<f64>();
    let total = bid_volume + ask_volume;
    if total <= 0.0 {
        bail!("empty book volumes at requested depth");
    }

    Ok(ImbalanceEstimate {
        bid_volume,
        ask_volume,
        imbalance: (bid_volume - ask_volume) / total,
    })
}

pub fn slippage(book: &OrderBookSnapshot, notional: f64, side: Side) -> Result<SlippageEstimate> {
    if !notional.is_finite() || notional <= 0.0 {
        bail!("notional must be > 0");
    }
    let levels = match side {
        Side::Buy => &book.asks,
        Side::Sell => &book.bids,
    };
    if levels.is_empty() {
        bail!("orderbook side is empty");
    }

    let best_price = levels[0].price;
    let fill = fill_quote_notional(levels, notional)?;
    if fill.filled_quote + f64::EPSILON < notional {
        bail!("insufficient depth to fill notional={notional}");
    }
    let slippage_abs = match side {
        Side::Buy => fill.vwap - best_price,
        Side::Sell => best_price - fill.vwap,
    };
    let slippage_bps = if best_price > 0.0 {
        (slippage_abs / best_price) * 10_000.0
    } else {
        0.0
    };

    Ok(SlippageEstimate {
        avg_fill_price: fill.vwap,
        best_price,
        slippage_abs,
        slippage_bps,
        levels_consumed: fill.levels_consumed,
    })
}

pub fn vamp(book: &OrderBookSnapshot, dollar_depth: f64) -> Result<VampEstimate> {
    if !dollar_depth.is_finite() || dollar_depth <= 0.0 {
        bail!("dollar_depth must be > 0");
    }

    let ask_fill = fill_quote_notional(&book.asks, dollar_depth)?;
    let bid_fill = fill_quote_notional(&book.bids, dollar_depth)?;
    let ask_vwap = ask_fill.vwap;
    let bid_vwap = bid_fill.vwap;

    Ok(VampEstimate {
        ask_vwap,
        bid_vwap,
        vamp: (ask_vwap + bid_vwap) / 2.0,
        ask_levels_consumed: ask_fill.levels_consumed,
        bid_levels_consumed: bid_fill.levels_consumed,
        max_reachable_quote_ask: quote_capacity(&book.asks),
        max_reachable_quote_bid: quote_capacity(&book.bids),
        complete: ask_fill.filled_quote >= dollar_depth && bid_fill.filled_quote >= dollar_depth,
    })
}

struct QuoteFill {
    vwap: f64,
    levels_consumed: u16,
    filled_quote: f64,
}

fn fill_quote_notional(levels: &[OrderBookLevel], target_quote: f64) -> Result<QuoteFill> {
    if levels.is_empty() {
        bail!("orderbook side is empty");
    }

    let mut remaining = target_quote;
    let mut total_quote = 0.0;
    let mut total_base = 0.0;
    let mut levels_consumed = 0;
    for level in levels {
        if remaining <= 0.0 {
            break;
        }
        let take_quote = remaining.min(level.price * level.quantity);
        total_quote += take_quote;
        total_base += take_quote / level.price;
        remaining -= take_quote;
        levels_consumed += 1;
    }
    if total_base <= 0.0 {
        bail!("computed base fill is zero");
    }

    Ok(QuoteFill {
        vwap: total_quote / total_base,
        levels_consumed,
        filled_quote: total_quote,
    })
}

fn quote_capacity(levels: &[OrderBookLevel]) -> f64 {
    levels
        .iter()
        .map(|level| level.price * level.quantity)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: "test".to_string(),
            symbol: "BTC/USD".to_string(),
            timestamp_ms: 1,
            bids: vec![
                OrderBookLevel {
                    price: 99.0,
                    quantity: 2.0,
                },
                OrderBookLevel {
                    price: 98.0,
                    quantity: 2.0,
                },
            ],
            asks: vec![
                OrderBookLevel {
                    price: 101.0,
                    quantity: 2.0,
                },
                OrderBookLevel {
                    price: 102.0,
                    quantity: 2.0,
                },
            ],
        }
    }

    #[test]
    fn computes_shared_orderbook_functions() {
        let book = book();
        assert_eq!(spread(&book).expect("spread").spread_bps, 200.0);
        assert_eq!(depth(&book, 1).expect("depth").total_quote, 400.0);
        assert_eq!(imbalance(&book, 2).expect("imbalance").imbalance, 0.0);
        assert!(
            slippage(&book, 250.0, Side::Buy)
                .expect("slippage")
                .slippage_bps
                > 0.0
        );
        assert!(vamp(&book, 250.0).expect("vamp").complete);
    }
}
