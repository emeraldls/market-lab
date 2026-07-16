use serde::Serialize;

use crate::domain::types::{
    OhlcvCandle, OhlcvtCandle, OiCandle, OpenInterestSnapshot, VdCandle, VolumeBar,
    VolumeDeltaTick, VolumeProfile,
};

#[derive(Clone, Debug, Serialize)]
pub struct ScriptCandle {
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub volume: f64,
    pub trades: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<u64>,
}

impl ScriptCandle {
    pub fn from_mmt(candle: OhlcvtCandle) -> Self {
        Self {
            t: timestamp_ms(candle.t),
            o: candle.o,
            h: candle.h,
            l: candle.l,
            c: candle.c,
            volume: candle.vb + candle.vs,
            trades: candle.tb + candle.ts,
            close_time: None,
            vb: Some(candle.vb),
            vs: Some(candle.vs),
            tb: Some(candle.tb),
            ts: Some(candle.ts),
        }
    }

    pub fn from_bulk(candle: OhlcvCandle) -> Self {
        Self {
            t: timestamp_ms(candle.t),
            o: candle.o,
            h: candle.h,
            l: candle.l,
            c: candle.c,
            volume: candle.volume,
            trades: candle.trades,
            close_time: Some(timestamp_ms(candle.close_time)),
            vb: None,
            vs: None,
            tb: None,
            ts: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptVolume {
    pub t: u64,
    pub volume: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trades: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_time: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pg: Option<f64>,
}

impl ScriptVolume {
    pub fn from_mmt(profile: VolumeProfile) -> Self {
        let volume = profile.b.iter().sum::<f64>() + profile.s.iter().sum::<f64>();
        let price = profile_price(&profile);
        Self {
            t: timestamp_ms(profile.t),
            volume,
            trades: None,
            close_time: None,
            price,
            p: Some(profile.p),
            b: Some(profile.b),
            s: Some(profile.s),
            pg: Some(profile.pg),
        }
    }

    pub fn from_bulk_candle(candle: OhlcvCandle) -> Self {
        Self {
            t: timestamp_ms(candle.t),
            volume: candle.volume,
            trades: Some(candle.trades),
            close_time: Some(timestamp_ms(candle.close_time)),
            price: Some(candle.c),
            p: None,
            b: None,
            s: None,
            pg: None,
        }
    }

    pub fn from_bulk_bar(bar: VolumeBar, price: Option<f64>) -> Self {
        Self {
            t: timestamp_ms(bar.t),
            volume: bar.volume,
            trades: Some(bar.trades),
            close_time: Some(timestamp_ms(bar.close_time)),
            price,
            p: None,
            b: None,
            s: None,
            pg: None,
        }
    }

    pub fn reference_price(&self) -> Option<f64> {
        self.price
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptOpenInterest {
    pub t: u64,
    pub value: f64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub n: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mark_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notional: Option<f64>,
}

impl ScriptOpenInterest {
    pub fn from_mmt(candle: OiCandle) -> Self {
        Self {
            t: timestamp_ms(candle.t),
            value: candle.c,
            o: candle.o,
            h: candle.h,
            l: candle.l,
            c: candle.c,
            n: candle.n,
            mark_price: None,
            notional: None,
        }
    }

    pub fn from_bulk(snapshot: OpenInterestSnapshot) -> Self {
        Self {
            t: timestamp_ms(snapshot.timestamp_ms),
            value: snapshot.open_interest,
            o: snapshot.open_interest,
            h: snapshot.open_interest,
            l: snapshot.open_interest,
            c: snapshot.open_interest,
            n: 1,
            mark_price: Some(snapshot.mark_price),
            notional: Some(snapshot.notional),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptVolumeDelta {
    pub t: u64,
    pub value: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub o: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub h: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cumulative_delta: Option<f64>,
}

impl ScriptVolumeDelta {
    pub fn from_mmt(candle: VdCandle) -> Self {
        Self {
            t: timestamp_ms(candle.t),
            value: candle.c,
            o: Some(candle.o),
            h: Some(candle.h),
            l: Some(candle.l),
            c: Some(candle.c),
            n: Some(candle.n),
            delta: None,
            cumulative_delta: Some(candle.c),
        }
    }

    pub fn from_bulk(tick: VolumeDeltaTick) -> Self {
        Self {
            t: timestamp_ms(tick.timestamp_ms),
            value: tick.cumulative_delta,
            o: None,
            h: None,
            l: None,
            c: None,
            n: None,
            delta: Some(tick.delta),
            cumulative_delta: Some(tick.cumulative_delta),
        }
    }
}

pub fn timestamp_ms(timestamp: u64) -> u64 {
    if timestamp < 10_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else {
        timestamp
    }
}

fn profile_price(profile: &VolumeProfile) -> Option<f64> {
    profile
        .p
        .iter()
        .enumerate()
        .max_by(|(left_idx, _), (right_idx, _)| {
            let left = profile.b.get(*left_idx).copied().unwrap_or(0.0)
                + profile.s.get(*left_idx).copied().unwrap_or(0.0);
            let right = profile.b.get(*right_idx).copied().unwrap_or(0.0)
                + profile.s.get(*right_idx).copied().unwrap_or(0.0);
            left.partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(_, price)| *price)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_mmt_candles_to_milliseconds_and_common_volume() {
        let candle = ScriptCandle::from_mmt(OhlcvtCandle {
            t: 1_700_000_000,
            o: 1.0,
            h: 2.0,
            l: 0.5,
            c: 1.5,
            vb: 3.0,
            vs: 2.0,
            tb: 4,
            ts: 3,
        });
        assert_eq!(candle.t, 1_700_000_000_000);
        assert_eq!(candle.volume, 5.0);
        assert_eq!(candle.trades, 7);
        assert_eq!(candle.vb, Some(3.0));
    }

    #[test]
    fn bulk_candles_do_not_invent_directional_volume() {
        let candle = ScriptCandle::from_bulk(OhlcvCandle {
            t: 1_700_000_000_000,
            close_time: 1_700_000_060_000,
            o: 1.0,
            h: 2.0,
            l: 0.5,
            c: 1.5,
            volume: 5.0,
            trades: 7,
        });
        assert_eq!(candle.volume, 5.0);
        assert!(candle.vb.is_none());
        assert!(candle.vs.is_none());
    }
}
