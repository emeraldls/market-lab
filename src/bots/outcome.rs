use anyhow::{Result, bail};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OutcomeQuotes {
    /// User-facing probability midpoint, expressed on the selected primary side.
    pub reference_price: f64,
    /// Primary-side bid. The venue order sells the complement at `1 - bid`.
    pub normalized_bid: f64,
    /// Primary-side ask. The venue order sells the primary side at this price.
    pub normalized_ask: f64,
    pub complement_sell_price: f64,
    pub primary_sell_price: f64,
}

/// Builds a two-sided quote from the book of the side selected by the user.
///
/// Hyperliquid exposes separate tokens for the two binary sides. Selling the
/// complement at `1 - bid` provides the visible primary bid; selling the
/// primary token at `ask` provides the visible primary ask. One split pair
/// therefore earns `ask - bid` before fees when both orders fill.
pub fn quote_prices(
    best_bid: f64,
    best_ask: f64,
    spread_bps: f64,
    tick_size: f64,
    price_precision: u8,
) -> Result<OutcomeQuotes> {
    if !best_bid.is_finite()
        || !best_ask.is_finite()
        || best_bid <= 0.0
        || best_ask >= 1.0
        || best_ask <= best_bid
    {
        bail!("outcome market maker requires a valid primary-side book inside (0, 1)");
    }
    if !spread_bps.is_finite() || spread_bps < 0.0 {
        bail!("outcome market-maker spread must be zero or greater");
    }
    if !tick_size.is_finite() || tick_size <= 0.0 || tick_size >= 1.0 {
        bail!("outcome market maker requires a tick inside (0, 1)");
    }

    let reference_price = (best_bid + best_ask) / 2.0;
    let half_spread = spread_bps / 20_000.0;
    let raw_bid = reference_price * (1.0 - half_spread);
    let raw_ask = reference_price * (1.0 + half_spread);
    let normalized_bid = floor_to_tick(
        raw_bid.min(best_ask - tick_size),
        tick_size,
        price_precision,
    );
    let normalized_ask = ceil_to_tick(
        raw_ask.max(best_bid + tick_size),
        tick_size,
        price_precision,
    );
    if normalized_bid <= 0.0 || normalized_ask >= 1.0 || normalized_ask <= normalized_bid {
        bail!("outcome market maker could not construct a valid quote pair");
    }

    // Round both venue sell orders upward. This never concedes more than the
    // requested normalized bid/ask and preserves non-negative gross spread.
    let complement_sell_price = ceil_to_tick(1.0 - normalized_bid, tick_size, price_precision);
    let primary_sell_price = normalized_ask;
    if complement_sell_price <= 0.0
        || complement_sell_price >= 1.0
        || primary_sell_price + complement_sell_price <= 1.0
    {
        bail!("outcome market maker produced an invalid complementary quote pair");
    }

    Ok(OutcomeQuotes {
        reference_price,
        normalized_bid: round_to_precision(1.0 - complement_sell_price, price_precision),
        normalized_ask,
        complement_sell_price,
        primary_sell_price,
    })
}

pub fn gross_profit_per_pair(quotes: OutcomeQuotes) -> f64 {
    quotes.primary_sell_price + quotes.complement_sell_price - 1.0
}

fn floor_to_tick(value: f64, tick_size: f64, precision: u8) -> f64 {
    round_to_precision((value / tick_size).floor() * tick_size, precision)
}

fn ceil_to_tick(value: f64, tick_size: f64, precision: u8) -> f64 {
    round_to_precision((value / tick_size).ceil() * tick_size, precision)
}

fn round_to_precision(value: f64, precision: u8) -> f64 {
    let scale = 10_f64.powi(i32::from(precision));
    (value * scale).round() / scale
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_one_normalized_book_to_two_sell_orders() {
        let quotes = quote_prices(0.48, 0.52, 200.0, 0.01, 2).expect("valid quotes");

        assert_eq!(quotes.normalized_bid, 0.49);
        assert_eq!(quotes.normalized_ask, 0.51);
        assert_eq!(quotes.complement_sell_price, 0.51);
        assert_eq!(quotes.primary_sell_price, 0.51);
        assert!((gross_profit_per_pair(quotes) - 0.02).abs() < 1e-12);
    }

    #[test]
    fn both_fills_return_one_dollar_plus_the_visible_spread() {
        let quotes = quote_prices(0.96196, 0.969, 20.0, 0.00001, 5).expect("valid quotes");
        let proceeds = quotes.primary_sell_price + quotes.complement_sell_price;

        assert!((proceeds - (1.0 + quotes.normalized_ask - quotes.normalized_bid)).abs() < 1e-12);
        assert!(proceeds > 1.0);
    }

    #[test]
    fn rejects_books_outside_probability_bounds() {
        assert!(quote_prices(0.0, 0.5, 10.0, 0.01, 2).is_err());
        assert!(quote_prices(0.5, 1.0, 10.0, 0.01, 2).is_err());
    }
}
