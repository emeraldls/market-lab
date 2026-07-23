use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::domain::types::OiCandle;

const MINUTE_MS: u64 = 60_000;
pub const DIRECTIONAL_CONTEXT_WINDOW_SECS: u64 = 15 * 60;
const DIRECTIONAL_NOISE_FLOOR_PCT: f64 = 0.05;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectionalBias {
    Buy,
    Sell,
    Neutral,
}

impl DirectionalBias {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
            Self::Neutral => "neutral",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectionalRegime {
    UpwardPositionExpansion,
    DownwardPositionExpansion,
    ShortCoveringOrDeleveraging,
    LongUnwindingOrPossibleLiquidations,
    Neutral,
}

impl DirectionalRegime {
    pub fn label(self) -> &'static str {
        match self {
            Self::UpwardPositionExpansion => "upward position expansion",
            Self::DownwardPositionExpansion => "downward position expansion",
            Self::ShortCoveringOrDeleveraging => "short covering / deleveraging",
            Self::LongUnwindingOrPossibleLiquidations => "long unwinding / possible liquidations",
            Self::Neutral => "neutral",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectionalConfidence {
    Moderate,
    Low,
    None,
}

impl DirectionalConfidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Moderate => "moderate",
            Self::Low => "low",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OpenInterestWindow {
    pub open: f64,
    pub close: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectionalContext {
    pub window_secs: u64,
    pub price_change_pct: f64,
    pub open_interest_change_pct: f64,
    pub agreeing_sources: usize,
    pub total_sources: usize,
    pub regime: DirectionalRegime,
    pub bias: DirectionalBias,
    pub confidence: DirectionalConfidence,
}

impl DirectionalContext {
    pub fn assess(
        price_open: f64,
        price_close: f64,
        oi_windows: &[OpenInterestWindow],
    ) -> Result<Self> {
        if !price_open.is_finite()
            || price_open <= 0.0
            || !price_close.is_finite()
            || price_close <= 0.0
        {
            bail!("directional context received invalid execution-venue prices");
        }
        if oi_windows.is_empty() {
            bail!("directional context requires recent open-interest data");
        }

        let mut aggregate_open = 0.0;
        let mut aggregate_close = 0.0;
        let mut source_changes = Vec::with_capacity(oi_windows.len());
        for window in oi_windows {
            if !window.open.is_finite()
                || window.open <= 0.0
                || !window.close.is_finite()
                || window.close < 0.0
            {
                bail!("directional context received invalid open-interest values");
            }
            aggregate_open += window.open;
            aggregate_close += window.close;
            source_changes.push((window.close / window.open - 1.0) * 100.0);
        }

        let price_change_pct = (price_close / price_open - 1.0) * 100.0;
        let open_interest_change_pct = (aggregate_close / aggregate_open - 1.0) * 100.0;
        let price_direction = significant_direction(price_change_pct);
        let oi_direction = significant_direction(open_interest_change_pct);
        let (regime, bias) = match (price_direction, oi_direction) {
            (1, 1) => (
                DirectionalRegime::UpwardPositionExpansion,
                DirectionalBias::Buy,
            ),
            (-1, 1) => (
                DirectionalRegime::DownwardPositionExpansion,
                DirectionalBias::Sell,
            ),
            (1, -1) => (
                DirectionalRegime::ShortCoveringOrDeleveraging,
                DirectionalBias::Buy,
            ),
            (-1, -1) => (
                DirectionalRegime::LongUnwindingOrPossibleLiquidations,
                DirectionalBias::Sell,
            ),
            _ => (DirectionalRegime::Neutral, DirectionalBias::Neutral),
        };
        let agreeing_sources = match oi_direction {
            1 => source_changes
                .iter()
                .filter(|change| significant_direction(**change) == 1)
                .count(),
            -1 => source_changes
                .iter()
                .filter(|change| significant_direction(**change) == -1)
                .count(),
            _ => 0,
        };
        let total_sources = oi_windows.len();
        let confidence = if bias == DirectionalBias::Neutral {
            DirectionalConfidence::None
        } else if oi_direction > 0 && agreeing_sources == total_sources {
            DirectionalConfidence::Moderate
        } else {
            DirectionalConfidence::Low
        };

        Ok(Self {
            window_secs: DIRECTIONAL_CONTEXT_WINDOW_SECS,
            price_change_pct,
            open_interest_change_pct,
            agreeing_sources,
            total_sources,
            regime,
            bias,
            confidence,
        })
    }
}

fn significant_direction(change_pct: f64) -> i8 {
    if change_pct >= DIRECTIONAL_NOISE_FLOOR_PCT {
        1
    } else if change_pct <= -DIRECTIONAL_NOISE_FLOOR_PCT {
        -1
    } else {
        0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenInterestProvider {
    Mmt,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenInterestSource {
    pub exchange: String,
    pub provider: OpenInterestProvider,
}

impl OpenInterestSource {
    pub fn selector(&self) -> String {
        match self.provider {
            OpenInterestProvider::Mmt => format!("{}@mmt", self.exchange),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenInterestSourceSelector {
    sources: Vec<OpenInterestSource>,
}

impl OpenInterestSourceSelector {
    pub fn parse(values: &[String], symbol: &str) -> Result<Self> {
        if values.is_empty() {
            bail!("OIWAP requires --oi-sources exchange@mmt[,exchange@mmt...]");
        }

        let mut sources = Vec::with_capacity(values.len());
        let mut exchanges = HashSet::with_capacity(values.len());
        for value in values {
            let value = value.trim().to_ascii_lowercase();
            let parts = value.split('@').collect::<Vec<_>>();
            let source = match parts.as_slice() {
                [exchange, "mmt"] if !exchange.is_empty() => {
                    crate::markets::provider_market("mmt", exchange, symbol)?;
                    if !crate::markets::is_futures_exchange(exchange)? {
                        bail!(
                            "open interest source `{exchange}@mmt` is spot; OIWAP requires futures or perpetual exchanges"
                        );
                    }
                    OpenInterestSource {
                        exchange: (*exchange).to_string(),
                        provider: OpenInterestProvider::Mmt,
                    }
                }
                [exchange] if !exchange.is_empty() => bail!(
                    "standalone historical OI adapter for `{exchange}` is not implemented; use `{exchange}@mmt`"
                ),
                [_, provider] => bail!(
                    "unsupported OI provider `{provider}` in `{value}`; only `exchange@mmt` is valid"
                ),
                _ => bail!("invalid OI source `{value}`; use `exchange@mmt`"),
            };
            if !exchanges.insert(source.exchange.clone()) {
                bail!(
                    "open interest exchange `{}` is selected more than once",
                    source.exchange
                );
            }
            sources.push(source);
        }
        Ok(Self { sources })
    }

    pub fn sources(&self) -> &[OpenInterestSource] {
        &self.sources
    }
}

#[derive(Clone, Copy, Debug)]
struct LiveCandle {
    ts_ms: u64,
    activity: f64,
}

#[derive(Debug)]
pub struct LiveOpenInterestActivity {
    first_full_minute_ms: u64,
    current: HashMap<String, LiveCandle>,
}

impl LiveOpenInterestActivity {
    pub fn new(start_ms: u64) -> Self {
        Self {
            first_full_minute_ms: start_ms.div_ceil(MINUTE_MS) * MINUTE_MS,
            current: HashMap::new(),
        }
    }

    /// Applies an update to the currently forming MMT OI candle. It returns the adjustment to
    /// that exchange's cumulative activity, allowing a current candle to be revised without
    /// double-counting it. The partial minute in which the strategy started is never counted.
    pub fn apply(&mut self, exchange: &str, candle: OiCandle) -> Result<Option<(u64, f64)>> {
        let ts_ms = crate::providers::mmt::utils::normalize_to_ms(candle.t);
        let activity = open_interest_activity(exchange, &candle)?;
        if ts_ms < self.first_full_minute_ms {
            return Ok(None);
        }

        let next = LiveCandle { ts_ms, activity };
        let Some(current) = self.current.get_mut(exchange) else {
            self.current.insert(exchange.to_string(), next);
            return Ok(Some((ts_ms, activity)));
        };
        if ts_ms < current.ts_ms {
            return Ok(None);
        }
        if ts_ms == current.ts_ms {
            let adjustment = activity - current.activity;
            *current = next;
            return Ok(Some((ts_ms, adjustment)));
        }

        *current = next;
        Ok(Some((ts_ms, activity)))
    }
}

pub fn open_interest_activity(exchange: &str, candle: &OiCandle) -> Result<f64> {
    if !candle.o.is_finite() || candle.o < 0.0 || !candle.c.is_finite() || candle.c < 0.0 {
        bail!("{exchange}@mmt produced invalid open interest OHLC values");
    }
    Ok((candle.c - candle.o).abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(t: u64, open: f64, close: f64) -> OiCandle {
        OiCandle {
            t,
            o: open,
            h: open.max(close),
            l: open.min(close),
            c: close,
            n: 1,
        }
    }

    #[test]
    fn selector_requires_mmt_futures_sources() {
        let selector = OpenInterestSourceSelector::parse(
            &["binancef@mmt".to_string(), "hyperliquid@mmt".to_string()],
            "BTC/USDT",
        )
        .expect("futures OI selector");
        assert_eq!(selector.sources().len(), 2);

        assert!(
            OpenInterestSourceSelector::parse(&["binance@mmt".to_string()], "BTC/USDT")
                .expect_err("spot OI must fail")
                .to_string()
                .contains("spot")
        );
        assert!(
            OpenInterestSourceSelector::parse(&["binancef".to_string()], "BTC/USDT")
                .expect_err("direct OI must fail")
                .to_string()
                .contains("standalone historical OI adapter")
        );
    }

    #[test]
    fn live_activity_omits_the_starting_partial_minute() {
        let mut activity = LiveOpenInterestActivity::new(74_000);

        assert!(
            activity
                .apply("binancef", candle(60, 100.0, 100.0))
                .unwrap()
                .is_none()
        );
        assert!(
            activity
                .apply("binancef", candle(60, 100.0, 103.0))
                .unwrap()
                .is_none()
        );
        assert!(
            activity
                .apply("binancef", candle(120, 103.0, 105.0))
                .unwrap()
                .is_some()
        );
        let revised = activity
            .apply("binancef", candle(120, 103.0, 104.0))
            .unwrap()
            .expect("current full minute is revised");

        assert_eq!(revised.0, 120_000);
        assert!((revised.1 + 1.0).abs() < 1e-9);
    }

    #[test]
    fn live_activity_uses_absolute_open_to_close_change_per_exchange() {
        let mut activity = LiveOpenInterestActivity::new(0);
        let flat = activity
            .apply("binancef", candle(0, 100.0, 100.0))
            .unwrap()
            .expect("first minute activity");
        assert_eq!(flat, (0, 0.0));
        let up = activity
            .apply("binancef", candle(60, 100.0, 110.0))
            .unwrap()
            .expect("up activity");
        let down = activity
            .apply("binancef", candle(120, 110.0, 90.0))
            .unwrap()
            .expect("down activity");

        assert_eq!(up, (60_000, 10.0));
        assert_eq!(down, (120_000, 20.0));
    }

    #[test]
    fn directional_context_classifies_position_expansion() {
        let bullish = DirectionalContext::assess(
            100.0,
            101.0,
            &[
                OpenInterestWindow {
                    open: 1_000.0,
                    close: 1_020.0,
                },
                OpenInterestWindow {
                    open: 500.0,
                    close: 505.0,
                },
            ],
        )
        .expect("bullish context");
        assert_eq!(bullish.bias, DirectionalBias::Buy);
        assert_eq!(bullish.regime, DirectionalRegime::UpwardPositionExpansion);
        assert_eq!(bullish.agreeing_sources, 2);
        assert_eq!(bullish.confidence, DirectionalConfidence::Moderate);

        let bearish = DirectionalContext::assess(
            100.0,
            99.0,
            &[OpenInterestWindow {
                open: 1_000.0,
                close: 1_020.0,
            }],
        )
        .expect("bearish context");
        assert_eq!(bearish.bias, DirectionalBias::Sell);
        assert_eq!(bearish.regime, DirectionalRegime::DownwardPositionExpansion);
    }

    #[test]
    fn directional_context_treats_falling_oi_as_lower_confidence() {
        let context = DirectionalContext::assess(
            100.0,
            99.0,
            &[OpenInterestWindow {
                open: 1_000.0,
                close: 980.0,
            }],
        )
        .expect("deleveraging context");

        assert_eq!(context.bias, DirectionalBias::Sell);
        assert_eq!(
            context.regime,
            DirectionalRegime::LongUnwindingOrPossibleLiquidations
        );
        assert_eq!(context.confidence, DirectionalConfidence::Low);
    }

    #[test]
    fn directional_context_ignores_small_changes() {
        let context = DirectionalContext::assess(
            100.0,
            100.01,
            &[OpenInterestWindow {
                open: 1_000.0,
                close: 1_000.1,
            }],
        )
        .expect("neutral context");

        assert_eq!(context.bias, DirectionalBias::Neutral);
        assert_eq!(context.regime, DirectionalRegime::Neutral);
        assert_eq!(context.confidence, DirectionalConfidence::None);
    }
}
