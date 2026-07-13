use anyhow::{Result, bail};
use serde::Serialize;

use crate::domain::types::{CvdStudyResult, VdCandle};

#[derive(Clone, Debug, Serialize)]
pub struct SeriesResult {
    pub latest: Option<f64>,
    pub previous: Option<f64>,
    pub points: Vec<Option<f64>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CvdPoint {
    pub t: u64,
    pub delta: f64,
    pub cumulative: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct CvdResult {
    pub latest: Option<f64>,
    pub previous: Option<f64>,
    pub delta: f64,
    pub bucket: u8,
    pub points: Vec<CvdPoint>,
}

#[derive(Default)]
pub struct CvdAccumulator {
    previous_close: Option<f64>,
    cumulative: f64,
}

impl CvdAccumulator {
    pub fn update(&mut self, candle: &VdCandle) -> CvdPoint {
        let previous = self.previous_close.unwrap_or(candle.o);
        let delta = candle.c - previous;
        self.cumulative += delta;
        self.previous_close = Some(candle.c);
        CvdPoint {
            t: candle.t,
            delta,
            cumulative: self.cumulative,
        }
    }
}

pub fn sma(values: &[f64], window: usize) -> Result<SeriesResult> {
    validate_series(values, window)?;
    let mut points = vec![None; values.len()];
    let mut sum = 0.0;
    for (index, value) in values.iter().copied().enumerate() {
        sum += value;
        if index >= window {
            sum -= values[index - window];
        }
        if index + 1 >= window {
            points[index] = Some(sum / window as f64);
        }
    }
    Ok(compact(points))
}

pub fn ema(values: &[f64], window: usize) -> Result<SeriesResult> {
    validate_series(values, window)?;
    let mut points = vec![None; values.len()];
    let alpha = 2.0 / (window as f64 + 1.0);
    let mut previous = None;
    for index in 0..values.len() {
        if index + 1 < window {
            continue;
        }
        let value = if let Some(previous) = previous {
            (values[index] - previous) * alpha + previous
        } else {
            values[index + 1 - window..=index].iter().sum::<f64>() / window as f64
        };
        points[index] = Some(value);
        previous = Some(value);
    }
    Ok(compact(points))
}

pub fn cvd(candles: &[VdCandle], bucket: u8) -> Result<CvdResult> {
    if !(1..=11).contains(&bucket) {
        bail!("bucket must be in range 1..=11");
    }
    let mut accumulator = CvdAccumulator::default();
    let mut points = Vec::with_capacity(candles.len());
    for candle in candles {
        points.push(accumulator.update(candle));
    }

    Ok(CvdResult {
        latest: points.last().map(|point| point.cumulative),
        previous: points
            .len()
            .checked_sub(2)
            .and_then(|index| points.get(index))
            .map(|point| point.cumulative),
        delta: accumulator.cumulative,
        bucket,
        points,
    })
}

pub fn cvd_summary(candles: Vec<VdCandle>, bucket: u8) -> Result<CvdStudyResult> {
    let first_close = candles.first().map(|candle| candle.c).unwrap_or(0.0);
    let last_close = candles.last().map(|candle| candle.c).unwrap_or(0.0);
    let delta = cvd(&candles, bucket)?.delta;
    Ok(CvdStudyResult {
        points: candles.len(),
        first_close,
        last_close,
        delta,
        candles,
    })
}

fn validate_series(values: &[f64], window: usize) -> Result<()> {
    if window == 0 {
        bail!("window must be a positive integer");
    }
    if values.iter().any(|value| !value.is_finite()) {
        bail!("series values must be finite numbers");
    }
    Ok(())
}

fn compact(points: Vec<Option<f64>>) -> SeriesResult {
    let latest = points.last().copied().flatten();
    let previous = points
        .len()
        .checked_sub(2)
        .and_then(|index| points.get(index))
        .copied()
        .flatten();
    SeriesResult {
        latest,
        previous,
        points,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_sma_and_ema() {
        let values = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(sma(&values, 3).expect("sma").latest, Some(3.0));
        assert_eq!(ema(&values, 3).expect("ema").latest, Some(3.0));
    }
}
