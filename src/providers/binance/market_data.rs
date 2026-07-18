use anyhow::{Context, Result, bail};

use crate::domain::types::{OhlcvtCandle, OhlcvCandle, OhlcvSeries, ProviderHealth};
use crate::providers::binance::client::BinanceClient;

const EXCHANGE: &str = "binance";

pub struct BinanceProvider;

/// Binance kline response: array of arrays
/// [openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, numberOfTrades, ...]
///
/// Note: Binance does not provide buy/sell volume split in klines.
/// `vb`/`vs` are set to base volume and quote volume respectively,
/// and `tb`/`ts` both map to total trades. Strategies relying on
/// order flow imbalance (buy vs sell volume) should not use Binance klines.
#[derive(Debug)]
struct BinanceKline(Vec<serde_json::Value>);

impl From<BinanceKline> for OhlcvtCandle {
    fn from(k: BinanceKline) -> Self {
        Self {
            t: k.0.get(0).and_then(|v| v.as_u64()).unwrap_or(0),
            o: k.0.get(1).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            h: k.0.get(2).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            l: k.0.get(3).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            c: k.0.get(4).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            vb: k.0.get(5).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            vs: k.0.get(7).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            tb: k.0.get(8).and_then(|v| v.as_u64()).unwrap_or(0),
            ts: k.0.get(8).and_then(|v| v.as_u64()).unwrap_or(0),
        }
    }
}

impl From<BinanceKline> for OhlcvCandle {
    fn from(k: BinanceKline) -> Self {
        Self {
            t: k.0.get(0).and_then(|v| v.as_u64()).unwrap_or(0),
            close_time: k.0.get(6).and_then(|v| v.as_u64()).unwrap_or(0),
            o: k.0.get(1).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            h: k.0.get(2).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            l: k.0.get(3).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            c: k.0.get(4).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            volume: k.0.get(5).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            trades: k.0.get(8).and_then(|v| v.as_u64()).unwrap_or(0),
        }
    }
}

impl BinanceProvider {
    pub async fn health() -> Result<ProviderHealth> {
        let client = BinanceClient::new()?;
        // Binance /ping returns {} on success — no query params needed.
        let url = client.url("ping");
        let response = client.http().get(&url).send().await
            .context("failed to call Binance ping")?;
        let status = if response.status().is_success() { "ok" } else { "degraded" };
        let details = response.json::<serde_json::Value>().await
            .unwrap_or(serde_json::json!({}));
        Ok(ProviderHealth {
            provider: EXCHANGE.to_string(),
            status: status.to_string(),
            details,
        })
    }

    pub async fn candles(symbol: &str, interval: &str, from: u64, to: u64) -> Result<OhlcvSeries> {
        Self::fetch_candles(symbol, interval, from, to, false).await
    }

    pub async fn candles_futures(symbol: &str, interval: &str, from: u64, to: u64) -> Result<OhlcvSeries> {
        Self::fetch_candles(symbol, interval, from, to, true).await
    }

    async fn fetch_candles(symbol: &str, interval: &str, from: u64, to: u64, futures: bool) -> Result<OhlcvSeries> {
        require_app_timestamp_ms(from, "candle start time")?;
        require_app_timestamp_ms(to, "candle end time")?;
        if from >= to {
            bail!("candle start time must be less than end time");
        }

        // Binance symbol format: BTCUSDT (no separator)
        let binance_symbol = symbol.replace('/', "").replace('-', "").to_uppercase();

        let query = [
            ("symbol", binance_symbol.clone()),
            ("interval", interval.to_string()),
            ("startTime", from.to_string()),
            ("endTime", to.to_string()),
            ("limit", "1000".to_string()),
        ];

        let client = if futures {
            BinanceClient::new_futures()?
        } else {
            BinanceClient::new()?
        };

        let exchange_label = if futures { "binance_futures" } else { EXCHANGE };

        let raw: Vec<Vec<serde_json::Value>> = client.get("klines", &query).await?;
        let mut data: Vec<OhlcvCandle> = raw.into_iter().map(|k| OhlcvCandle::from(BinanceKline(k))).collect();
        data.sort_by_key(|c| c.t);

        Ok(OhlcvSeries {
            exchange: exchange_label.to_string(),
            symbol: symbol.to_string(),
            tf: interval.to_string(),
            from,
            to,
            points: data.len(),
            data,
        })
    }

    pub async fn candles_paginated(symbol: &str, interval: &str, from: u64, to: u64) -> Result<OhlcvSeries> {
        Self::fetch_paginated(symbol, interval, from, to, false).await
    }

    pub async fn candles_paginated_futures(symbol: &str, interval: &str, from: u64, to: u64) -> Result<OhlcvSeries> {
        Self::fetch_paginated(symbol, interval, from, to, true).await
    }

    async fn fetch_paginated(symbol: &str, interval: &str, from: u64, to: u64, futures: bool) -> Result<OhlcvSeries> {
        // Fetch up to 5000 candles by paginating Binance's 1000-candle limit.
        let binance_symbol = symbol.replace('/', "").replace('-', "").to_uppercase();
        let client = if futures {
            BinanceClient::new_futures()?
        } else {
            BinanceClient::new()?
        };
        let exchange_label = if futures { "binance_futures" } else { EXCHANGE };
        let mut all_data: Vec<OhlcvCandle> = Vec::new();
        let mut current_from = from;

        loop {
            let query = [
                ("symbol", binance_symbol.clone()),
                ("interval", interval.to_string()),
                ("startTime", current_from.to_string()),
                ("endTime", to.to_string()),
                ("limit", "1000".to_string()),
            ];

            let raw: Vec<Vec<serde_json::Value>> = client.get("klines", &query).await?;
            if raw.is_empty() { break; }

            let batch_len = raw.len();
            let batch: Vec<OhlcvCandle> = raw.into_iter().map(|k| OhlcvCandle::from(BinanceKline(k))).collect();
            let last_close_time = batch.last().map(|c| c.close_time).unwrap_or(0);
            all_data.extend(batch);

            // Binance returns at most 1000 per call; if we got fewer, no more data.
            if batch_len < 1000 || last_close_time >= to || all_data.len() >= 5000 { break; }
            current_from = last_close_time + 1;
        }

        all_data.sort_by_key(|c| c.t);
        all_data.dedup_by_key(|c| c.t);

        Ok(OhlcvSeries {
            exchange: exchange_label.to_string(),
            symbol: symbol.to_string(),
            tf: interval.to_string(),
            from,
            to,
            points: all_data.len(),
            data: all_data,
        })
    }
}

fn require_app_timestamp_ms(ts: u64, label: &str) -> Result<()> {
    // Binance expects millisecond timestamps
    if ts < 1_000_000_000_000 {
        bail!("{label} must be in milliseconds since epoch");
    }
    Ok(())
}
