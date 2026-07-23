use anyhow::{Result, bail};

use crate::bots::mid_price::quote_sizes;
use crate::domain::execution::OrderSide;
use crate::domain::types::OrderBookLevel;

pub const MAX_GRID_LEVELS_PER_SIDE: u16 = 100;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GridQuote {
    pub level: u16,
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct GridSpec {
    /// Best bid captured when the current grid was anchored.
    pub anchor_bid: OrderBookLevel,
    /// Best ask captured when the current grid was anchored.
    pub anchor_ask: OrderBookLevel,
    /// Current best bid, used only to keep generated quotes maker-safe.
    pub best_bid: OrderBookLevel,
    /// Current best ask, used only to keep generated quotes maker-safe.
    pub best_ask: OrderBookLevel,
    pub levels_per_side: u16,
    /// Level one is this far behind the anchored touch. Every following level
    /// adds the same distance.
    pub step_bps: f64,
    pub max_inventory_size: f64,
    pub inventory_size: f64,
    /// Bot-owned weighted average entry. When inventory exists, the reducing
    /// ladder cannot quote through this price in normal grid mode.
    pub exposure_price: Option<f64>,
    pub tick_size: f64,
    pub price_precision: u8,
}

pub fn quote_grid(spec: GridSpec) -> Result<Vec<GridQuote>> {
    validate_spec(spec)?;
    let sizes = quote_sizes(spec.max_inventory_size, spec.inventory_size, 0.0)?;
    let level_count = f64::from(spec.levels_per_side);
    let bid_size = sizes.bid_size / level_count;
    let ask_size = sizes.ask_size / level_count;
    let mut quotes = Vec::with_capacity(usize::from(spec.levels_per_side) * 2);

    for level in 1..=spec.levels_per_side {
        let distance_bps = spec.step_bps * f64::from(level);
        let mut bid = floor_to_tick(
            spec.anchor_bid.price * (1.0 - distance_bps / 10_000.0),
            spec.tick_size,
            spec.price_precision,
        );
        let mut ask = ceil_to_tick(
            spec.anchor_ask.price * (1.0 + distance_bps / 10_000.0),
            spec.tick_size,
            spec.price_precision,
        );

        if let Some(exposure_price) = spec.exposure_price {
            if spec.inventory_size > 0.0 {
                // Long inventory is reduced by asks. Require at least one grid
                // step of gross edge per level while normal grid mode is active.
                ask = ask.max(ceil_to_tick(
                    exposure_price * (1.0 + distance_bps / 10_000.0),
                    spec.tick_size,
                    spec.price_precision,
                ));
            } else if spec.inventory_size < 0.0 {
                // Short inventory is reduced by bids. Never buy it back above
                // the configured profit ladder while normal mode is active.
                bid = bid.min(floor_to_tick(
                    exposure_price * (1.0 - distance_bps / 10_000.0),
                    spec.tick_size,
                    spec.price_precision,
                ));
            }
        }

        // A fixed grid can temporarily become stale while the market traverses
        // it. Never turn a resting maker level into a crossing order.
        if bid_size > 0.0 && bid > 0.0 && bid < spec.best_ask.price {
            quotes.push(GridQuote {
                level,
                side: OrderSide::Buy,
                price: bid,
                size: bid_size,
            });
        }
        if ask_size > 0.0 && ask > spec.best_bid.price {
            quotes.push(GridQuote {
                level,
                side: OrderSide::Sell,
                price: ask,
                size: ask_size,
            });
        }
    }

    Ok(quotes)
}

/// Builds only the passive side needed to reduce grid inventory during a soft
/// reset. Unlike a two-sided quote calculation, this remains valid when the
/// venue's current spread cannot produce both midpoint prices simultaneously.
pub fn passive_mid_price(
    side: OrderSide,
    best_bid: OrderBookLevel,
    best_ask: OrderBookLevel,
    tick_size: f64,
    price_precision: u8,
) -> Result<f64> {
    validate_book(best_bid, best_ask, "live")?;
    if !tick_size.is_finite() || tick_size <= 0.0 {
        bail!("grid tick size must be greater than zero");
    }

    let midpoint = (best_bid.price + best_ask.price) / 2.0;
    let price = match side {
        OrderSide::Buy => floor_to_tick(
            midpoint.min(best_ask.price - tick_size),
            tick_size,
            price_precision,
        ),
        OrderSide::Sell => ceil_to_tick(
            midpoint.max(best_bid.price + tick_size),
            tick_size,
            price_precision,
        ),
    };
    let maker_safe = match side {
        OrderSide::Buy => price > 0.0 && price < best_ask.price,
        OrderSide::Sell => price > best_bid.price,
    };
    if !maker_safe {
        bail!(
            "grid could not construct a passive midpoint {} quote",
            match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            }
        );
    }
    Ok(price)
}

pub fn recenter_range_bps(levels_per_side: u16, step_bps: f64) -> Result<f64> {
    validate_levels_and_step(levels_per_side, step_bps)?;
    Ok(f64::from(levels_per_side) * step_bps)
}

pub fn should_recenter(
    anchor_mid: f64,
    fair_price: f64,
    levels_per_side: u16,
    step_bps: f64,
) -> Result<bool> {
    if !anchor_mid.is_finite() || anchor_mid <= 0.0 {
        bail!("grid anchor midpoint must be greater than zero");
    }
    if !fair_price.is_finite() || fair_price <= 0.0 {
        bail!("grid fair price must be greater than zero");
    }
    let range_bps = recenter_range_bps(levels_per_side, step_bps)?;
    Ok((fair_price - anchor_mid).abs() / anchor_mid * 10_000.0 > range_bps)
}

pub fn soft_reset_triggered(
    inventory_size: f64,
    exposure_price: f64,
    mark_price: f64,
    reset_threshold_pct: f64,
) -> Result<bool> {
    if !inventory_size.is_finite() {
        bail!("grid inventory must be finite");
    }
    if !exposure_price.is_finite() || exposure_price <= 0.0 {
        bail!("grid exposure price must be greater than zero");
    }
    if !mark_price.is_finite() || mark_price <= 0.0 {
        bail!("grid mark price must be greater than zero");
    }
    if !reset_threshold_pct.is_finite() || !(0.0..=1.0).contains(&reset_threshold_pct) {
        bail!("grid reset threshold must be between 0 and 1 percent");
    }
    if inventory_size.abs() <= f64::EPSILON || reset_threshold_pct == 0.0 {
        return Ok(false);
    }

    let threshold = reset_threshold_pct / 100.0;
    Ok(if inventory_size > 0.0 {
        mark_price <= exposure_price * (1.0 - threshold)
    } else {
        mark_price >= exposure_price * (1.0 + threshold)
    })
}

fn validate_spec(spec: GridSpec) -> Result<()> {
    validate_book(spec.anchor_bid, spec.anchor_ask, "anchor")?;
    validate_book(spec.best_bid, spec.best_ask, "live")?;
    validate_levels_and_step(spec.levels_per_side, spec.step_bps)?;
    if !spec.tick_size.is_finite() || spec.tick_size <= 0.0 {
        bail!("grid tick size must be greater than zero");
    }
    if spec
        .exposure_price
        .is_some_and(|price| !price.is_finite() || price <= 0.0)
    {
        bail!("grid exposure price must be greater than zero");
    }
    if spec.inventory_size.abs() > f64::EPSILON && spec.exposure_price.is_none() {
        bail!("grid exposure price is required while inventory is open");
    }
    Ok(())
}

fn validate_book(bid: OrderBookLevel, ask: OrderBookLevel, label: &str) -> Result<()> {
    if !bid.price.is_finite()
        || !ask.price.is_finite()
        || bid.price <= 0.0
        || ask.price <= bid.price
    {
        bail!("grid requires a valid, uncrossed {label} top of book");
    }
    Ok(())
}

fn validate_levels_and_step(levels_per_side: u16, step_bps: f64) -> Result<()> {
    if !(1..=MAX_GRID_LEVELS_PER_SIDE).contains(&levels_per_side) {
        bail!("grid levels per side must be between 1 and {MAX_GRID_LEVELS_PER_SIDE}");
    }
    if !step_bps.is_finite() || step_bps <= 0.0 {
        bail!("grid step must be greater than zero");
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

    fn level(price: f64) -> OrderBookLevel {
        OrderBookLevel {
            price,
            quantity: 1.0,
        }
    }

    fn spec(inventory_size: f64) -> GridSpec {
        GridSpec {
            anchor_bid: level(65_995.05),
            anchor_ask: level(66_004.95),
            best_bid: level(65_995.05),
            best_ask: level(66_004.95),
            levels_per_side: 3,
            step_bps: 2.0,
            max_inventory_size: 1.0,
            inventory_size,
            exposure_price: (inventory_size != 0.0).then_some(66_000.0),
            tick_size: 0.01,
            price_precision: 2,
        }
    }

    #[test]
    fn levels_are_anchored_behind_the_live_touch() {
        let quotes = quote_grid(spec(0.0)).expect("grid should be valid");

        assert_eq!(quotes.len(), 6);
        assert_eq!(quotes[0].price, 65_981.85);
        assert_eq!(quotes[1].price, 66_018.16);
        assert_eq!(quotes[2].price, 65_968.65);
        assert_eq!(quotes[3].price, 66_031.36);
        assert_eq!(quotes[4].price, 65_955.45);
        assert_eq!(quotes[5].price, 66_044.56);
    }

    #[test]
    fn flat_inventory_is_split_equally_across_both_ladders() {
        let quotes = quote_grid(spec(0.0)).expect("grid should be valid");
        let bids = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Buy)
            .collect::<Vec<_>>();
        let asks = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Sell)
            .collect::<Vec<_>>();

        assert!(
            bids.windows(2)
                .all(|levels| { (levels[0].size - levels[1].size).abs() < f64::EPSILON })
        );
        assert!(
            asks.windows(2)
                .all(|levels| { (levels[0].size - levels[1].size).abs() < f64::EPSILON })
        );
        assert!((bids.iter().map(|quote| quote.size).sum::<f64>() - 0.5).abs() < 1e-12);
        assert!((asks.iter().map(|quote| quote.size).sum::<f64>() - 0.5).abs() < 1e-12);
    }

    #[test]
    fn inventory_automatically_skews_the_two_ladders() {
        let quotes = quote_grid(spec(0.25)).expect("grid should be valid");
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

        assert!((bid_size - 0.375).abs() < 1e-12);
        assert!((ask_size - 0.625).abs() < 1e-12);
    }

    #[test]
    fn maximum_inventory_disables_the_risk_increasing_ladder() {
        let long = quote_grid(spec(1.0)).expect("long grid should be valid");
        let short = quote_grid(spec(-1.0)).expect("short grid should be valid");

        assert!(long.iter().all(|quote| quote.side == OrderSide::Sell));
        assert!(short.iter().all(|quote| quote.side == OrderSide::Buy));
    }

    #[test]
    fn long_reducing_ladder_is_locked_above_average_entry() {
        let mut locked = spec(0.25);
        locked.anchor_bid = level(98.0);
        locked.anchor_ask = level(100.0);
        locked.best_bid = level(98.0);
        locked.best_ask = level(100.0);
        locked.exposure_price = Some(105.0);

        let asks = quote_grid(locked)
            .expect("grid should be valid")
            .into_iter()
            .filter(|quote| quote.side == OrderSide::Sell)
            .collect::<Vec<_>>();

        assert_eq!(asks[0].price, 105.03);
        assert_eq!(asks[1].price, 105.05);
        assert_eq!(asks[2].price, 105.07);
    }

    #[test]
    fn short_reducing_ladder_is_locked_below_average_entry() {
        let mut locked = spec(-0.25);
        locked.anchor_bid = level(99.0);
        locked.anchor_ask = level(100.0);
        locked.best_bid = level(99.0);
        locked.best_ask = level(100.0);
        locked.exposure_price = Some(95.0);

        let bids = quote_grid(locked)
            .expect("grid should be valid")
            .into_iter()
            .filter(|quote| quote.side == OrderSide::Buy)
            .collect::<Vec<_>>();

        assert_eq!(bids[0].price, 94.98);
        assert_eq!(bids[1].price, 94.96);
        assert_eq!(bids[2].price, 94.94);
    }

    #[test]
    fn soft_reset_only_triggers_on_adverse_exposure_movement() {
        assert!(!soft_reset_triggered(1.0, 100.0, 99.51, 0.5).expect("valid long"));
        assert!(soft_reset_triggered(1.0, 100.0, 99.5, 0.5).expect("valid long"));
        assert!(!soft_reset_triggered(-1.0, 100.0, 100.49, 0.5).expect("valid short"));
        assert!(soft_reset_triggered(-1.0, 100.0, 100.5, 0.5).expect("valid short"));
        assert!(!soft_reset_triggered(0.0, 100.0, 50.0, 0.5).expect("flat"));
    }

    #[test]
    fn soft_reset_midpoint_pricing_requires_only_the_reducing_side() {
        let bid = level(100.0);
        let ask = level(100.25);

        assert_eq!(
            passive_mid_price(OrderSide::Buy, bid, ask, 0.25, 2).expect("passive buy"),
            100.0
        );
        assert_eq!(
            passive_mid_price(OrderSide::Sell, bid, ask, 0.25, 2).expect("passive sell"),
            100.25
        );
    }

    #[test]
    fn soft_reset_rejects_thresholds_above_one_percent() {
        let error = soft_reset_triggered(1.0, 100.0, 98.0, 1.01)
            .expect_err("thresholds above one percent must be rejected");

        assert!(error.to_string().contains("between 0 and 1 percent"));
    }

    #[test]
    fn recentering_uses_the_derived_full_grid_range() {
        assert_eq!(recenter_range_bps(3, 2.0).expect("valid range"), 6.0);
        assert!(!should_recenter(100.0, 100.059, 3, 2.0).expect("inside grid"));
        assert!(should_recenter(100.0, 100.061, 3, 2.0).expect("outside grid"));
    }
}
