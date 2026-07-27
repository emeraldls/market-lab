use anyhow::{Result, bail};

use crate::domain::execution::OrderSide;

pub const MAX_GRID_LEVELS_PER_SIDE: u16 = 100;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GridQuote {
    pub level: u16,
    pub side: OrderSide,
    pub price: f64,
    pub paired_price: f64,
    pub size: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct GridSpec {
    /// Fixed center captured when the bot starts.
    pub center_price: f64,
    pub levels_per_side: u16,
    /// Distance between adjacent fixed grid prices.
    pub step_bps: f64,
    /// Total working capacity across both initial ladders. Half is allocated
    /// to bids and half to asks.
    pub max_inventory_size: f64,
    pub tick_size: f64,
    pub price_precision: u8,
}

/// Builds the initial orders for a classic paired grid.
///
/// Every returned BUY is paired with a SELL one grid step above it. Every
/// returned SELL is paired with a BUY one grid step below it. Those prices stay
/// fixed for the lifetime of the bot.
pub fn quote_grid(spec: GridSpec) -> Result<Vec<GridQuote>> {
    validate_spec(spec)?;
    let size = spec.max_inventory_size / (2.0 * f64::from(spec.levels_per_side));
    let mut quotes = Vec::with_capacity(usize::from(spec.levels_per_side) * 2);

    for level in 1..=spec.levels_per_side {
        let lower_index = -i32::from(level);
        let upper_index = lower_index + 1;
        quotes.push(GridQuote {
            level,
            side: OrderSide::Buy,
            price: buy_price(spec, lower_index),
            paired_price: sell_price(spec, upper_index),
            size,
        });

        let upper_index = i32::from(level);
        let lower_index = upper_index - 1;
        quotes.push(GridQuote {
            level,
            side: OrderSide::Sell,
            price: sell_price(spec, upper_index),
            paired_price: buy_price(spec, lower_index),
            size,
        });
    }

    for quote in &quotes {
        let valid_pair = quote.price > 0.0
            && quote.paired_price > 0.0
            && match quote.side {
                OrderSide::Buy => quote.paired_price > quote.price,
                OrderSide::Sell => quote.paired_price < quote.price,
            };
        if !valid_pair {
            bail!(
                "grid range produced an invalid price at level {}; reduce the level count or step",
                quote.level
            );
        }
    }

    Ok(quotes)
}

fn buy_price(spec: GridSpec, index: i32) -> f64 {
    floor_to_tick(
        price_at_index(spec.center_price, spec.step_bps, index),
        spec.tick_size,
        spec.price_precision,
    )
}

fn sell_price(spec: GridSpec, index: i32) -> f64 {
    ceil_to_tick(
        price_at_index(spec.center_price, spec.step_bps, index),
        spec.tick_size,
        spec.price_precision,
    )
}

fn price_at_index(center_price: f64, step_bps: f64, index: i32) -> f64 {
    center_price * (1.0 + f64::from(index) * step_bps / 10_000.0)
}

fn validate_spec(spec: GridSpec) -> Result<()> {
    if !spec.center_price.is_finite() || spec.center_price <= 0.0 {
        bail!("grid center price must be greater than zero");
    }
    if !(1..=MAX_GRID_LEVELS_PER_SIDE).contains(&spec.levels_per_side) {
        bail!("grid levels per side must be between 1 and {MAX_GRID_LEVELS_PER_SIDE}");
    }
    if !spec.step_bps.is_finite() || spec.step_bps <= 0.0 {
        bail!("grid step must be greater than zero");
    }
    if !spec.max_inventory_size.is_finite() || spec.max_inventory_size <= 0.0 {
        bail!("grid maximum inventory size must be greater than zero");
    }
    if !spec.tick_size.is_finite() || spec.tick_size <= 0.0 {
        bail!("grid tick size must be greater than zero");
    }
    Ok(())
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

    fn spec() -> GridSpec {
        GridSpec {
            center_price: 100.0,
            levels_per_side: 3,
            step_bps: 100.0,
            max_inventory_size: 6.0,
            tick_size: 0.01,
            price_precision: 2,
        }
    }

    #[test]
    fn builds_fixed_orders_on_both_sides_of_the_center() {
        let quotes = quote_grid(spec()).expect("grid should be valid");

        assert_eq!(
            quotes,
            vec![
                GridQuote {
                    level: 1,
                    side: OrderSide::Buy,
                    price: 99.0,
                    paired_price: 100.0,
                    size: 1.0,
                },
                GridQuote {
                    level: 1,
                    side: OrderSide::Sell,
                    price: 101.0,
                    paired_price: 100.0,
                    size: 1.0,
                },
                GridQuote {
                    level: 2,
                    side: OrderSide::Buy,
                    price: 98.0,
                    paired_price: 99.0,
                    size: 1.0,
                },
                GridQuote {
                    level: 2,
                    side: OrderSide::Sell,
                    price: 102.0,
                    paired_price: 101.0,
                    size: 1.0,
                },
                GridQuote {
                    level: 3,
                    side: OrderSide::Buy,
                    price: 97.0,
                    paired_price: 98.0,
                    size: 1.0,
                },
                GridQuote {
                    level: 3,
                    side: OrderSide::Sell,
                    price: 103.0,
                    paired_price: 102.0,
                    size: 1.0,
                },
            ]
        );
    }

    #[test]
    fn allocates_half_the_inventory_capacity_to_each_initial_side() {
        let quotes = quote_grid(spec()).expect("grid should be valid");
        let bid_size = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Buy)
            .map(|quote| quote.size)
            .sum::<f64>();
        let ask_size = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Sell)
            .map(|quote| quote.size)
            .sum::<f64>();

        assert_eq!(bid_size, 3.0);
        assert_eq!(ask_size, 3.0);
    }

    #[test]
    fn every_initial_order_has_one_profitable_adjacent_pair() {
        for quote in quote_grid(spec()).expect("grid should be valid") {
            match quote.side {
                OrderSide::Buy => assert!(quote.paired_price > quote.price),
                OrderSide::Sell => assert!(quote.paired_price < quote.price),
            }
        }
    }

    #[test]
    fn coarse_ticks_keep_each_pair_directionally_valid() {
        let quotes = quote_grid(GridSpec {
            center_price: 100.0,
            levels_per_side: 1,
            step_bps: 0.01,
            max_inventory_size: 1.0,
            tick_size: 10.0,
            price_precision: 0,
        })
        .expect("directional rounding should preserve the pair");

        assert_eq!(quotes[0].price, 90.0);
        assert_eq!(quotes[0].paired_price, 100.0);
        assert_eq!(quotes[1].price, 110.0);
        assert_eq!(quotes[1].paired_price, 100.0);
    }

    #[test]
    fn rejects_a_range_that_reaches_zero() {
        let result = quote_grid(GridSpec {
            center_price: 100.0,
            levels_per_side: 2,
            step_bps: 5_000.0,
            max_inventory_size: 1.0,
            tick_size: 0.01,
            price_precision: 2,
        });

        assert!(result.is_err());
    }
}
