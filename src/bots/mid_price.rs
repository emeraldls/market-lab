use anyhow::{Result, bail};

use crate::domain::types::OrderBookLevel;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MidPriceQuotes {
    pub reference_price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MidPriceQuoteSizes {
    pub inventory_ratio: f64,
    pub tilt: f64,
    pub bid_size: f64,
    pub ask_size: f64,
}

pub fn quote_prices(
    best_bid: OrderBookLevel,
    best_ask: OrderBookLevel,
    spread_bps: f64,
    tick_size: f64,
    price_precision: u8,
) -> Result<MidPriceQuotes> {
    if !best_bid.price.is_finite()
        || !best_ask.price.is_finite()
        || best_bid.price <= 0.0
        || best_ask.price <= best_bid.price
    {
        bail!("mid-price bot requires a valid, uncrossed top of book");
    }
    if !spread_bps.is_finite() || spread_bps < 0.0 {
        bail!("mid-price bot spread must be zero or greater");
    }
    if !tick_size.is_finite() || tick_size <= 0.0 {
        bail!("mid-price bot requires a positive tick size");
    }

    let reference_price = (best_bid.price + best_ask.price) / 2.0;
    let half_spread = spread_bps / 20_000.0;
    let raw_bid = reference_price * (1.0 - half_spread);
    let raw_ask = reference_price * (1.0 + half_spread);

    // The touch constraint keeps a stale but still valid quote from knowingly
    // crossing the observed book. ALO remains the final venue-side guard.
    let bid_ceiling = best_ask.price - tick_size;
    let ask_floor = best_bid.price + tick_size;
    let bid_price = floor_to_tick(raw_bid.min(bid_ceiling), tick_size, price_precision);
    let ask_price = ceil_to_tick(raw_ask.max(ask_floor), tick_size, price_precision);
    if bid_price <= 0.0 || ask_price <= bid_price {
        bail!("mid-price bot could not construct a valid post-only quote pair");
    }

    Ok(MidPriceQuotes {
        reference_price,
        bid_price,
        ask_price,
    })
}

/// Computes continuously replenished quote sizes around a hard one-sided
/// inventory limit. At flat inventory each side receives half of the limit.
/// Long inventory shrinks the bid and grows the ask; short inventory does the
/// opposite. Directional bias adds a user-controlled tilt before the hard
/// inventory headroom is applied.
pub fn quote_sizes(
    max_inventory_size: f64,
    inventory_size: f64,
    directional_bias_percent: f64,
) -> Result<MidPriceQuoteSizes> {
    if !max_inventory_size.is_finite() || max_inventory_size <= 0.0 {
        bail!("mid-price bot maximum inventory must be greater than zero");
    }
    if !inventory_size.is_finite() {
        bail!("mid-price bot inventory must be finite");
    }
    if !directional_bias_percent.is_finite()
        || !(-100.0..=100.0).contains(&directional_bias_percent)
    {
        bail!("mid-price bot directional bias must be between -100 and 100 percent");
    }

    let inventory_ratio = (inventory_size / max_inventory_size).clamp(-1.0, 1.0);
    let directional_bias = directional_bias_percent / 100.0;
    let tilt = (directional_bias - inventory_ratio).clamp(-1.0, 1.0);
    let base_size = max_inventory_size / 2.0;
    let bid_headroom = (max_inventory_size - inventory_size).max(0.0);
    let ask_headroom = (max_inventory_size + inventory_size).max(0.0);

    Ok(MidPriceQuoteSizes {
        inventory_ratio,
        tilt,
        bid_size: (base_size * (1.0 + tilt)).min(bid_headroom),
        ask_size: (base_size * (1.0 - tilt)).min(ask_headroom),
    })
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

    fn level(price: f64) -> OrderBookLevel {
        OrderBookLevel {
            price,
            quantity: 1.0,
        }
    }

    #[test]
    fn zero_spread_joins_an_existing_one_tick_touch() {
        let quotes = quote_prices(level(100.0), level(100.25), 0.0, 0.25, 2)
            .expect("quotes should be valid");

        assert_eq!(quotes.reference_price, 100.125);
        assert_eq!(quotes.bid_price, 100.0);
        assert_eq!(quotes.ask_price, 100.25);
    }

    #[test]
    fn positive_spread_is_centered_around_the_midpoint() {
        let quotes =
            quote_prices(level(99.0), level(101.0), 50.0, 0.5, 1).expect("quotes should be valid");

        assert_eq!(quotes.reference_price, 100.0);
        assert_eq!(quotes.bid_price, 99.5);
        assert_eq!(quotes.ask_price, 100.5);
    }

    #[test]
    fn rejects_crossed_books() {
        assert!(quote_prices(level(101.0), level(100.0), 2.0, 0.25, 2).is_err());
    }

    #[test]
    fn rejects_negative_spreads() {
        assert!(quote_prices(level(100.0), level(100.25), -1.0, 0.25, 2).is_err());
    }

    #[test]
    fn flat_inventory_quotes_half_of_the_limit_on_each_side() {
        let sizes = quote_sizes(10.0, 0.0, 0.0).expect("sizes should be valid");

        assert_eq!(sizes.inventory_ratio, 0.0);
        assert_eq!(sizes.tilt, 0.0);
        assert_eq!(sizes.bid_size, 5.0);
        assert_eq!(sizes.ask_size, 5.0);
    }

    #[test]
    fn long_inventory_shrinks_bid_and_grows_ask() {
        let sizes = quote_sizes(1_600.0, 400.0, 0.0).expect("sizes should be valid");

        assert_eq!(sizes.inventory_ratio, 0.25);
        assert_eq!(sizes.tilt, -0.25);
        assert_eq!(sizes.bid_size, 600.0);
        assert_eq!(sizes.ask_size, 1_000.0);
    }

    #[test]
    fn maximum_inventory_disables_the_risk_increasing_side() {
        let long = quote_sizes(1_600.0, 1_600.0, 0.0).expect("sizes should be valid");
        let short = quote_sizes(1_600.0, -1_600.0, 0.0).expect("sizes should be valid");

        assert_eq!(long.bid_size, 0.0);
        assert_eq!(long.ask_size, 1_600.0);
        assert_eq!(short.bid_size, 1_600.0);
        assert_eq!(short.ask_size, 0.0);
    }

    #[test]
    fn directional_bias_combines_with_inventory_skew() {
        let sizes = quote_sizes(1_000.0, 250.0, 50.0).expect("sizes should be valid");

        assert_eq!(sizes.inventory_ratio, 0.25);
        assert_eq!(sizes.tilt, 0.25);
        assert_eq!(sizes.bid_size, 625.0);
        assert_eq!(sizes.ask_size, 375.0);
    }
}
