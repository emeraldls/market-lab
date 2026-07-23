use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::domain::types::{OhlcvCandle, OhlcvSeries, ProviderHealth, VolumeBar, VolumeBarSeries};
use crate::providers::binance::client::BinanceClient;

const SPOT_EXCHANGE: &str = "binance";
const FUTURES_EXCHANGE: &str = "binancef";
const BINANCE_SPOT_KLINES_LIMIT: usize = 1_000;
const BINANCE_FUTURES_KLINES_LIMIT: usize = 1_500;
const BINANCE_MAX_CANDLES: usize = 5000;

pub struct BinanceProvider;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceMarket {
    Spot,
    Futures,
}

impl BinanceMarket {
    pub const fn exchange(self) -> &'static str {
        match self {
            Self::Spot => SPOT_EXCHANGE,
            Self::Futures => FUTURES_EXCHANGE,
        }
    }

    fn client(self) -> Result<BinanceClient> {
        match self {
            Self::Spot => BinanceClient::spot(),
            Self::Futures => BinanceClient::futures(),
        }
    }

    const fn request_limit(self) -> usize {
        match self {
            Self::Spot => BINANCE_SPOT_KLINES_LIMIT,
            Self::Futures => BINANCE_FUTURES_KLINES_LIMIT,
        }
    }
}

async fn ping_health(market: BinanceMarket) -> Result<ProviderHealth> {
    let details: Value = market.client()?.get("ping", &[] as &[(&str, &str)]).await?;
    Ok(ProviderHealth {
        provider: market.exchange().to_string(),
        status: "ok".to_string(),
        details,
    })
}

impl BinanceProvider {
    pub async fn health(market: BinanceMarket) -> Result<ProviderHealth> {
        ping_health(market).await
    }

    pub async fn candles(
        market: BinanceMarket,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        Self::fetch_candles(market, symbol, interval, from, to, market.request_limit()).await
    }

    pub async fn candles_paginated(
        market: BinanceMarket,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        validate_request(symbol, interval, from, to)?;
        let request_limit = market.request_limit();
        let mut data = Vec::new();
        let mut next_from = from;

        while next_from < to {
            let remaining = BINANCE_MAX_CANDLES.saturating_sub(data.len());
            if remaining == 0 {
                bail!(
                    "{} candles exceed the {BINANCE_MAX_CANDLES}-record safety limit for from={from} to={to}; narrow the range or use a larger timeframe",
                    market.exchange(),
                );
            }
            let limit = request_limit.min(remaining);
            let batch = Self::fetch_candles(market, symbol, interval, next_from, to, limit).await?;
            if batch.data.is_empty() {
                break;
            }
            let batch_len = batch.data.len();
            let last_close_time = batch
                .data
                .last()
                .map(|candle| candle.close_time)
                .context("Binance returned an empty candle batch")?;
            if last_close_time < next_from {
                bail!("Binance candle pagination did not advance");
            }
            next_from = last_close_time.saturating_add(1);
            data.extend(batch.data);
            if batch_len < limit || next_from >= to {
                break;
            }
        }

        data.sort_by_key(|candle| candle.t);
        data.dedup_by_key(|candle| candle.t);
        Ok(OhlcvSeries {
            exchange: market.exchange().to_string(),
            symbol: canonical_symbol(symbol)?,
            tf: interval.to_string(),
            from,
            to,
            points: data.len(),
            data,
        })
    }

    pub async fn volume_bars(
        market: BinanceMarket,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        let candles = Self::candles_paginated(market, symbol, interval, from, to).await?;
        let data = candles
            .data
            .into_iter()
            .map(|candle| VolumeBar {
                t: candle.t,
                close_time: candle.close_time,
                volume: candle.volume,
                trades: candle.trades,
            })
            .collect::<Vec<_>>();
        Ok(VolumeBarSeries {
            exchange: candles.exchange,
            symbol: candles.symbol,
            tf: candles.tf,
            from: candles.from,
            to: candles.to,
            points: data.len(),
            data,
        })
    }

    async fn fetch_candles(
        market: BinanceMarket,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
        limit: usize,
    ) -> Result<OhlcvSeries> {
        validate_request(symbol, interval, from, to)?;
        let symbol = canonical_symbol(symbol)?;
        let query = [
            ("symbol", venue_symbol(&symbol)),
            ("interval", interval.to_string()),
            ("startTime", from.to_string()),
            ("endTime", to.to_string()),
            ("limit", limit.to_string()),
        ];
        let raw: Vec<Vec<Value>> = market.client()?.get("klines", &query).await?;
        let mut data = raw
            .into_iter()
            .enumerate()
            .map(|(index, values)| decode_kline(&values, index))
            .collect::<Result<Vec<_>>>()?;
        data.sort_by_key(|candle| candle.t);
        Ok(OhlcvSeries {
            exchange: market.exchange().to_string(),
            symbol,
            tf: interval.to_string(),
            from,
            to,
            points: data.len(),
            data,
        })
    }
}

pub fn timeframe_from_seconds(seconds: u32) -> Result<&'static str> {
    match seconds {
        60 => Ok("1m"),
        180 => Ok("3m"),
        300 => Ok("5m"),
        900 => Ok("15m"),
        1_800 => Ok("30m"),
        3_600 => Ok("1h"),
        7_200 => Ok("2h"),
        14_400 => Ok("4h"),
        21_600 => Ok("6h"),
        28_800 => Ok("8h"),
        43_200 => Ok("12h"),
        86_400 => Ok("1d"),
        259_200 => Ok("3d"),
        604_800 => Ok("1w"),
        2_592_000 => Ok("1M"),
        _ => bail!("unsupported Binance timeframe seconds: {seconds}"),
    }
}

fn validate_request(symbol: &str, interval: &str, from: u64, to: u64) -> Result<()> {
    canonical_symbol(symbol)?;
    if !matches!(
        interval,
        "1m" | "3m"
            | "5m"
            | "15m"
            | "30m"
            | "1h"
            | "2h"
            | "4h"
            | "6h"
            | "8h"
            | "12h"
            | "1d"
            | "3d"
            | "1w"
            | "1M"
    ) {
        bail!("unsupported Binance candle interval `{interval}`");
    }
    require_app_timestamp_ms(from, "candle start time")?;
    require_app_timestamp_ms(to, "candle end time")?;
    if from >= to {
        bail!("candle start time must be less than end time");
    }
    Ok(())
}

fn canonical_symbol(symbol: &str) -> Result<String> {
    let mut parts = symbol.split('/');
    let Some(base) = parts.next().map(str::trim).filter(|part| !part.is_empty()) else {
        bail!("Binance symbol must look like BASE/QUOTE");
    };
    let Some(quote) = parts.next().map(str::trim).filter(|part| !part.is_empty()) else {
        bail!("Binance symbol must look like BASE/QUOTE");
    };
    if parts.next().is_some()
        || !base.chars().all(|ch| ch.is_ascii_alphanumeric())
        || !quote.chars().all(|ch| ch.is_ascii_alphanumeric())
    {
        bail!("Binance symbol must look like BASE/QUOTE");
    }
    Ok(format!(
        "{}/{}",
        base.to_ascii_uppercase(),
        quote.to_ascii_uppercase()
    ))
}

fn venue_symbol(symbol: &str) -> String {
    symbol.replace('/', "")
}

fn decode_kline(values: &[Value], index: usize) -> Result<OhlcvCandle> {
    if values.len() < 9 {
        bail!(
            "Binance kline at index {index} contained {} fields; expected at least 9",
            values.len()
        );
    }
    Ok(OhlcvCandle {
        t: integer(values, 0, index, "open time")?,
        o: decimal(values, 1, index, "open")?,
        h: decimal(values, 2, index, "high")?,
        l: decimal(values, 3, index, "low")?,
        c: decimal(values, 4, index, "close")?,
        volume: decimal(values, 5, index, "volume")?,
        close_time: integer(values, 6, index, "close time")?,
        trades: integer(values, 8, index, "trade count")?,
    })
}

fn decimal(values: &[Value], field: usize, index: usize, name: &str) -> Result<f64> {
    let raw = values[field]
        .as_str()
        .with_context(|| format!("Binance kline {index} {name} was not a string"))?;
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("Binance kline {index} {name} was invalid"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("Binance kline {index} {name} must be finite and non-negative");
    }
    Ok(value)
}

fn integer(values: &[Value], field: usize, index: usize, name: &str) -> Result<u64> {
    values[field]
        .as_u64()
        .with_context(|| format!("Binance kline {index} {name} was not an unsigned integer"))
}

fn require_app_timestamp_ms(timestamp: u64, label: &str) -> Result<()> {
    if !(10_000_000_000..10_000_000_000_000).contains(&timestamp) {
        bail!("{label} must be milliseconds at the Market Lab boundary");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_exchange_names_distinguish_spot_and_futures() {
        assert_eq!(BinanceMarket::Spot.exchange(), "binance");
        assert_eq!(BinanceMarket::Futures.exchange(), "binancef");
    }

    #[test]
    fn decodes_a_binance_kline_without_silent_defaults() {
        let values = serde_json::json!([
            1_784_764_800_000_u64,
            "66000.1",
            "66100.2",
            "65900.3",
            "66050.4",
            "12.5",
            1_784_764_859_999_u64,
            "825630.0",
            42_u64
        ]);
        let candle = decode_kline(values.as_array().expect("array"), 0).expect("valid kline");
        assert_eq!(candle.t, 1_784_764_800_000);
        assert_eq!(candle.close_time, 1_784_764_859_999);
        assert_eq!(candle.c, 66_050.4);
        assert_eq!(candle.volume, 12.5);
        assert_eq!(candle.trades, 42);
    }

    #[test]
    fn rejects_malformed_klines() {
        let values = serde_json::json!([1_784_764_800_000_u64, "bad"]);
        let error =
            decode_kline(values.as_array().expect("array"), 0).expect_err("must reject payload");
        assert!(error.to_string().contains("expected at least 9"));
    }

    #[test]
    fn normalizes_only_base_quote_symbols() {
        assert_eq!(
            canonical_symbol("btc/usdt").expect("valid symbol"),
            "BTC/USDT"
        );
        assert!(canonical_symbol("BTCUSDT").is_err());
        assert!(canonical_symbol("BTC/USDT/PERP").is_err());
    }
}
