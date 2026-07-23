use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const MINUTE_MS: u64 = 60_000;
const DAY_MS: u64 = 86_400_000;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeProvider {
    Direct,
    Mmt,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeSource {
    pub exchange: String,
    pub provider: VolumeProvider,
}

impl VolumeSource {
    pub fn selector(&self) -> String {
        match self.provider {
            VolumeProvider::Direct => self.exchange.clone(),
            VolumeProvider::Mmt => format!("{}@mmt", self.exchange),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeSourceSelector {
    sources: Vec<VolumeSource>,
}

impl VolumeSourceSelector {
    pub fn parse(values: &[String], execution_venue: &str, symbol: &str) -> Result<Self> {
        let values = if values.is_empty() {
            vec![execution_venue.to_string()]
        } else {
            values.to_vec()
        };
        let mut sources = Vec::with_capacity(values.len());
        let mut exchanges = HashSet::with_capacity(values.len());

        for value in values {
            let value = value.trim().to_ascii_lowercase();
            if value.is_empty() {
                bail!("--volume-sources contains an empty selector");
            }
            let parts = value.split('@').collect::<Vec<_>>();
            let source = match parts.as_slice() {
                [exchange] if !exchange.is_empty() => {
                    crate::markets::exchange_market(exchange, symbol)?;
                    if !matches!(*exchange, "bulk" | "hyperliquid") {
                        bail!(
                            "standalone volume adapter for `{exchange}` is not implemented; use `{exchange}@mmt`"
                        );
                    }
                    VolumeSource {
                        exchange: (*exchange).to_string(),
                        provider: VolumeProvider::Direct,
                    }
                }
                [exchange, "mmt"] if !exchange.is_empty() => {
                    crate::markets::provider_market("mmt", exchange, symbol)?;
                    VolumeSource {
                        exchange: (*exchange).to_string(),
                        provider: VolumeProvider::Mmt,
                    }
                }
                [_, provider] => bail!(
                    "unsupported volume provider `{provider}` in `{value}`; only `exchange@mmt` is valid"
                ),
                _ => bail!(
                    "invalid volume source `{value}`; use a standalone exchange or `exchange@mmt`"
                ),
            };
            if !exchanges.insert(source.exchange.clone()) {
                bail!(
                    "volume exchange `{}` is selected more than once; provider aliases cannot double-count one venue",
                    source.exchange
                );
            }
            sources.push(source);
        }
        Ok(Self { sources })
    }

    pub fn sources(&self) -> &[VolumeSource] {
        &self.sources
    }

    pub fn mmt_exchanges(&self) -> Vec<&str> {
        self.sources
            .iter()
            .filter(|source| source.provider == VolumeProvider::Mmt)
            .map(|source| source.exchange.as_str())
            .collect()
    }

    pub fn direct_exchanges(&self) -> Vec<&str> {
        self.sources
            .iter()
            .filter(|source| source.provider == VolumeProvider::Direct)
            .map(|source| source.exchange.as_str())
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HistoricalVolume {
    pub ts_ms: u64,
    pub volume: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct CurveSegment {
    start_ms: u64,
    end_ms: u64,
    forecast_volume: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VolumeCurve {
    start_ms: u64,
    end_ms: u64,
    total_forecast_volume: f64,
    segments: Vec<CurveSegment>,
}

impl VolumeCurve {
    pub fn build(start_ms: u64, duration_secs: u64, points: &[HistoricalVolume]) -> Result<Self> {
        Self::build_for("VWAP", "volume", start_ms, duration_secs, points)
    }

    pub fn build_for(
        strategy: &str,
        activity: &str,
        start_ms: u64,
        duration_secs: u64,
        points: &[HistoricalVolume],
    ) -> Result<Self> {
        if duration_secs < 60 {
            bail!("{strategy} duration must be at least 60 seconds");
        }
        let duration_ms = duration_secs
            .checked_mul(1_000)
            .with_context(|| format!("{strategy} duration is too large"))?;
        let end_ms = start_ms
            .checked_add(duration_ms)
            .with_context(|| format!("{strategy} deadline overflowed"))?;

        let mut daily_minutes: BTreeMap<(u64, u16), f64> = BTreeMap::new();
        for point in points {
            if !point.volume.is_finite() || point.volume < 0.0 {
                bail!("{strategy} history contains invalid {activity}");
            }
            let day = point.ts_ms / DAY_MS;
            let minute = ((point.ts_ms % DAY_MS) / MINUTE_MS) as u16;
            *daily_minutes.entry((day, minute)).or_default() += point.volume;
        }

        let mut minute_totals: HashMap<u16, (f64, usize)> = HashMap::new();
        for ((_, minute), volume) in daily_minutes {
            let entry = minute_totals.entry(minute).or_default();
            entry.0 += volume;
            entry.1 += 1;
        }

        let mut segments = Vec::new();
        let mut cursor = start_ms / MINUTE_MS * MINUTE_MS;
        while cursor < end_ms {
            let segment_start = cursor.max(start_ms);
            let segment_end = cursor.saturating_add(MINUTE_MS).min(end_ms);
            let minute = ((cursor % DAY_MS) / MINUTE_MS) as u16;
            let average = minute_totals
                .get(&minute)
                .map_or(0.0, |(total, observations)| total / *observations as f64);
            let overlap = (segment_end - segment_start) as f64 / MINUTE_MS as f64;
            segments.push(CurveSegment {
                start_ms: segment_start,
                end_ms: segment_end,
                forecast_volume: average * overlap,
            });
            cursor = cursor.saturating_add(MINUTE_MS);
        }

        let total_forecast_volume = segments
            .iter()
            .map(|segment| segment.forecast_volume)
            .sum::<f64>();
        if total_forecast_volume <= f64::EPSILON {
            bail!(
                "{strategy} could not construct a non-zero {activity} curve for the execution window"
            );
        }

        Ok(Self {
            start_ms,
            end_ms,
            total_forecast_volume,
            segments,
        })
    }

    pub fn start_ms(&self) -> u64 {
        self.start_ms
    }

    pub fn end_ms(&self) -> u64 {
        self.end_ms
    }

    pub fn total_forecast_volume(&self) -> f64 {
        self.total_forecast_volume
    }

    pub fn forecast_elapsed(&self, at_ms: u64) -> f64 {
        if at_ms <= self.start_ms {
            return 0.0;
        }
        if at_ms >= self.end_ms {
            return self.total_forecast_volume;
        }
        self.segments
            .iter()
            .map(|segment| {
                if at_ms >= segment.end_ms {
                    segment.forecast_volume
                } else if at_ms <= segment.start_ms {
                    0.0
                } else {
                    let elapsed = (at_ms - segment.start_ms) as f64;
                    let duration = (segment.end_ms - segment.start_ms) as f64;
                    segment.forecast_volume * elapsed / duration
                }
            })
            .sum()
    }

    pub fn forecast_between(&self, from_ms: u64, to_ms: u64) -> f64 {
        if to_ms <= from_ms {
            return 0.0;
        }
        (self.forecast_elapsed(to_ms) - self.forecast_elapsed(from_ms)).max(0.0)
    }

    pub fn forecast_remaining(&self, at_ms: u64) -> f64 {
        (self.total_forecast_volume - self.forecast_elapsed(at_ms)).max(0.0)
    }

    pub fn target_fraction(&self, at_ms: u64, actual_volume: f64, degraded: bool) -> f64 {
        if at_ms >= self.end_ms {
            return 1.0;
        }
        let forecast_elapsed = self.forecast_elapsed(at_ms);
        let actual = if degraded {
            actual_volume.max(forecast_elapsed)
        } else {
            actual_volume.max(0.0)
        };
        let forecast_remaining = self.forecast_remaining(at_ms);
        let denominator = actual + forecast_remaining;
        if denominator <= f64::EPSILON {
            return 1.0;
        }
        (actual / denominator).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_defaults_to_execution_venue() {
        let selector =
            VolumeSourceSelector::parse(&[], "bulk", "BTC/USDT").expect("BULK default selector");
        assert_eq!(selector.sources()[0].selector(), "bulk");

        let selector = VolumeSourceSelector::parse(&[], "hyperliquid", "BTC/USDT")
            .expect("Hyperliquid default selector");
        assert_eq!(selector.sources()[0].selector(), "hyperliquid");
    }

    #[test]
    fn selector_rejects_one_exchange_through_two_providers() {
        let error = VolumeSourceSelector::parse(
            &["bulk".to_string(), "bulk".to_string()],
            "bulk",
            "BTC/USDT",
        )
        .expect_err("duplicate venue must fail");
        assert!(error.to_string().contains("selected more than once"));
    }

    #[test]
    fn curve_scales_partial_first_and_last_minutes() {
        let start = 12 * 60 * MINUTE_MS + 30_000;
        let points = [
            HistoricalVolume {
                ts_ms: 12 * 60 * MINUTE_MS,
                volume: 100.0,
            },
            HistoricalVolume {
                ts_ms: (12 * 60 + 1) * MINUTE_MS,
                volume: 200.0,
            },
        ];
        let curve = VolumeCurve::build(start, 60, &points).expect("partial curve");
        assert_eq!(curve.segments.len(), 2);
        assert!((curve.total_forecast_volume() - 150.0).abs() < 1e-9);
        assert!((curve.forecast_elapsed(start + 30_000) - 50.0).abs() < 1e-9);
    }

    #[test]
    fn live_volume_moves_target_ahead_of_forecast() {
        let points = [
            HistoricalVolume {
                ts_ms: 0,
                volume: 100.0,
            },
            HistoricalVolume {
                ts_ms: MINUTE_MS,
                volume: 100.0,
            },
        ];
        let curve = VolumeCurve::build(0, 120, &points).expect("curve");
        let normal = curve.target_fraction(60_000, 100.0, false);
        let busy = curve.target_fraction(60_000, 300.0, false);
        assert!((normal - 0.5).abs() < 1e-9);
        assert!(busy > normal);
    }
}
