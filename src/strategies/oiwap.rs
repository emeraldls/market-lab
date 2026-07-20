use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::domain::types::OiCandle;

const MINUTE_MS: u64 = 60_000;

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
}
