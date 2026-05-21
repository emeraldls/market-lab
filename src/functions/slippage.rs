use anyhow::{Result, bail};

use crate::domain::enums::Side;
use crate::domain::types::{OrderBookSnapshot, SlippageEstimate};

pub fn estimate_slippage(
    book: &OrderBookSnapshot,
    notional: f64,
    at: u64,
    side: Side,
) -> Result<SlippageEstimate> {
    let levels = match side {
        Side::Buy => &book.asks,
        Side::Sell => &book.bids,
    };

    if levels.is_empty() {
        bail!("orderbook side is empty");
    }

    let best_price = levels[0].price;
    let mut remaining_quote = notional;
    let mut total_base = 0.0_f64;
    let mut total_quote = 0.0_f64;
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

    if remaining_quote > 0.0 {
        bail!("insufficient depth to fill notional={notional}");
    }

    if total_base <= 0.0 {
        bail!("computed base fill is zero");
    }

    let avg_fill_price = total_quote / total_base;
    let slippage_abs = match side {
        Side::Buy => avg_fill_price - best_price,
        Side::Sell => best_price - avg_fill_price,
    };
    let slippage_bps = if best_price > 0.0 {
        (slippage_abs / best_price) * 10_000.0
    } else {
        0.0
    };

    Ok(SlippageEstimate {
        exchange: book.exchange.clone(),
        symbol: book.symbol.clone(),
        side: match side {
            Side::Buy => "buy".to_string(),
            Side::Sell => "sell".to_string(),
        },
        notional,
        at,
        avg_fill_price,
        best_price,
        slippage_abs,
        slippage_bps,
        levels_consumed,
    })
}
