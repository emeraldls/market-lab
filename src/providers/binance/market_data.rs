use anyhow::{Context, Result, bail};

use crate::domain::types::{OhlcvtCandle, OhlcvCandle, OhlcvSeries, ProviderHealth};
use crate::providers::binance::client::BinanceClient;

const EXCHANGE: &str = "binance";
const FUTURES_EXCHANGE: &str = "binance_futures";
/// Maximum candles Binance returns per klines request.
const BINANCE_KLINES_LIMIT: usize = 1000;
/// Hard cap on total candles fetched via pagination to bound runtime and memory.
const BINANCE_MAX_CANDLES: usize = 5000;

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

/// Normalizes a symbol to Binance's uppercase form with separators removed (e.g. "btc/usdt" -> "BTCUSDT").
fn binance_symbol(symbol: &str) -> String {
    symbol.replace('/', "").replace('-', "").to_uppercase()
}

/// Selects the spot or futures client and returns the exchange label to attach to the series.
fn client_for(futures: bool) -> Result<(BinanceClient, &'static str)> {
    if futures {
        Ok((BinanceClient::new_futures()?, FUTURES_EXCHANGE))
    } else {
        Ok((BinanceClient::new()?, EXCHANGE))
    }
}

async fn ping_health(futures: bool) -> Result<ProviderHealth> {
    let (client, label) = client_for(futures)?;
    let url = client.url("ping");
    let response = client.http().get(&url).send().await
        .with_context(|| format!("failed to call Binance {} ping", label))?;
    let status = if response.status().is_success() { "ok" } else { "degraded" };
    let details = response.json::<serde_json::Value>().await
        .unwrap_or(serde_json::json!({}));
    Ok(ProviderHealth {
        provider: label.to_string(),
        status: status.to_string(),
        details,
    })
}

impl BinanceProvider {
    pub async fn health() -> Result<ProviderHealth> {
        ping_health(false).await
    }

    /// Futures health check targeting fapi.binance.com/fapi/v1/ping.
    pub async fn health_futures() -> Result<ProviderHealth> {
        ping_health(true).await
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

        let binance_symbol = binance_symbol(symbol);
        let (client, exchange_label) = client_for(futures)?;

        let query = [
            ("symbol", binance_symbol.clone()),
            ("interval", interval.to_string()),
            ("startTime", from.to_string()),
            ("endTime", to.to_string()),
            ("limit", BINANCE_KLINES_LIMIT.to_string()),
        ];

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
        // Fetch up to BINANCE_MAX_CANDLES candles by paginating Binance's per-request limit.
        let binance_symbol = binance_symbol(symbol);
        let (client, exchange_label) = client_for(futures)?;
        let mut all_data: Vec<OhlcvCandle> = Vec::new();
        let mut current_from = from;
        let mut truncated = false;

        loop {
            let query = [
                ("symbol", binance_symbol.clone()),
                ("interval", interval.to_string()),
                ("startTime", current_from.to_string()),
                ("endTime", to.to_string()),
                ("limit", BINANCE_KLINES_LIMIT.to_string()),
            ];

            let raw: Vec<Vec<serde_json::Value>> = client.get("klines", &query).await?;
            if raw.is_empty() { break; }

            let batch_len = raw.len();
            let batch: Vec<OhlcvCandle> = raw.into_iter().map(|k| OhlcvCandle::from(BinanceKline(k))).collect();
            let last_close_time = batch.last().map(|c| c.close_time).unwrap_or(0);
            all_data.extend(batch);

            // Binance returns at most BINANCE_KLINES_LIMIT per call; if we got fewer, no more data.
            if batch_len < BINANCE_KLINES_LIMIT || last_close_time >= to {
                break;
            }
            if all_data.len() >= BINANCE_MAX_CANDLES {
                // Stop paginating but flag that the requested range was not fully covered.
                truncated = true;
                break;
            }
            current_from = last_close_time + 1;
        }

        all_data.sort_by_key(|c| c.t);
        all_data.dedup_by_key(|c| c.t);

        if truncated {
            bail!(
                "Binance {} candles truncated at {} records; requested range from={from} to={to} was not fully covered. \
                 Narrow the range or use a larger timeframe.",
                exchange_label, BINANCE_MAX_CANDLES,
            );
        }

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
