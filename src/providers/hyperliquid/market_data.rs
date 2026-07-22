use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::domain::types::{
    ExchangeStatistics, FundingRateSnapshot, MarketStatistics, MarketTicker, OhlcvCandle,
    OhlcvSeries, OpenInterestSnapshot, OrderBookLevel, OrderBookSnapshot, ProviderHealth,
    TopOfBook, VolumeBar, VolumeBarSeries,
};

use super::EXCHANGE;
use super::client::HyperliquidClient;
use super::markets;

pub struct HyperliquidProvider;

impl HyperliquidProvider {
    pub fn capabilities() -> serde_json::Value {
        serde_json::json!({
            "network": "testnet",
            "products": ["native_perpetuals"],
            "authentication": {
                "market_data_requires_api_key": false,
                "execution_requires_agent_wallet": true
            },
            "historical": { "candles": true, "volume_bars": true },
            "snapshots": {
                "orderbook": true, "ticker": true, "statistics": true,
                "open_interest": true, "funding": true
            },
            "streams": { "orderbook": true, "trades": true, "candles": true }
        })
    }

    pub async fn health() -> Result<ProviderHealth> {
        let _: serde_json::Value = HyperliquidClient::new()?
            .info(&serde_json::json!({ "type": "meta" }))
            .await?;
        Ok(ProviderHealth {
            provider: EXCHANGE.to_string(),
            status: "ok".to_string(),
            details: serde_json::json!({
                "network": "testnet",
                "public_market_data": true,
                "requires_api_key": false,
                "capabilities": Self::capabilities()
            }),
        })
    }

    pub async fn candles(symbol: &str, interval: &str, from: u64, to: u64) -> Result<OhlcvSeries> {
        let market = require_market(symbol)?;
        require_timestamp_ms(from, "candle start time")?;
        require_timestamp_ms(to, "candle end time")?;
        if from >= to {
            bail!("candle start time must be less than end time");
        }
        validate_interval(interval)?;
        let raw: Vec<HyperliquidCandle> = HyperliquidClient::new()?
            .info(&serde_json::json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": market.provider_symbol,
                    "interval": interval,
                    "startTime": from,
                    "endTime": to
                }
            }))
            .await?;
        let mut data = raw
            .into_iter()
            .map(HyperliquidCandle::into_candle)
            .collect::<Result<Vec<_>>>()?;
        data.sort_by_key(|candle| candle.t);
        Ok(OhlcvSeries {
            exchange: EXCHANGE.to_string(),
            symbol: market.symbol.clone(),
            tf: interval.to_string(),
            from,
            to,
            points: data.len(),
            data,
        })
    }

    pub async fn volume_bars(
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        let candles = Self::candles(symbol, interval, from, to).await?;
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

    pub async fn live_orderbook(
        symbol: &str,
        depth: u16,
        _aggregation: Option<f64>,
    ) -> Result<OrderBookSnapshot> {
        if depth == 0 || depth > 20 {
            bail!("Hyperliquid orderbook depth must be between 1 and 20");
        }
        let market = require_market(symbol)?;
        let raw: HyperliquidBook = HyperliquidClient::new()?
            .info(&serde_json::json!({
                "type": "l2Book",
                "coin": market.provider_symbol
            }))
            .await?;
        raw.into_snapshot(&market.symbol, &market.venue_symbol, depth)
    }

    pub async fn ticker(symbol: &str) -> Result<MarketTicker> {
        let market = require_market(symbol)?;
        let (meta, contexts) = meta_and_contexts().await?;
        let index = meta
            .universe
            .iter()
            .position(|candidate| candidate.name == market.provider_symbol)
            .with_context(|| format!("Hyperliquid omitted {} context", market.provider_symbol))?;
        let context = contexts
            .get(index)
            .context("Hyperliquid metadata and asset contexts are out of sync")?;
        context.to_ticker(&market.symbol)
    }

    pub async fn open_interest(symbol: &str) -> Result<OpenInterestSnapshot> {
        let ticker = Self::ticker(symbol).await?;
        Ok(OpenInterestSnapshot {
            exchange: ticker.exchange,
            symbol: ticker.symbol,
            timestamp_ms: ticker.timestamp_ms,
            open_interest: ticker.open_interest,
            mark_price: ticker.mark_price,
            notional: ticker.open_interest * ticker.mark_price,
        })
    }

    pub async fn funding(symbol: &str) -> Result<FundingRateSnapshot> {
        let ticker = Self::ticker(symbol).await?;
        Ok(FundingRateSnapshot {
            exchange: ticker.exchange,
            symbol: ticker.symbol,
            timestamp_ms: ticker.timestamp_ms,
            current: ticker.funding_rate,
            annualized: Some(ticker.funding_rate * 24.0 * 365.0),
        })
    }

    pub async fn statistics(period: &str, symbol: Option<&str>) -> Result<ExchangeStatistics> {
        if !matches!(period.to_ascii_lowercase().as_str(), "1d" | "24h") {
            bail!("Hyperliquid statistics currently supports only period 1d");
        }
        let selected = symbol.map(require_market).transpose()?;
        let (meta, contexts) = meta_and_contexts().await?;
        let timestamp_ms = now_ms()?;
        let mut markets_out = Vec::new();
        let mut funding = Vec::new();
        for (asset, context) in meta.universe.iter().zip(contexts.iter()) {
            if asset.is_delisted
                || selected
                    .as_ref()
                    .is_some_and(|market| market.provider_symbol != asset.name)
            {
                continue;
            }
            let market = match markets::market(&asset.name) {
                Ok(market) => market,
                Err(_) => continue,
            };
            let mark = parse(&context.mark_px, "mark price")?;
            let volume = parse(&context.day_base_vlm, "day base volume")?;
            let quote_volume = parse(&context.day_ntl_vlm, "day notional volume")?;
            let open_interest = parse(&context.open_interest, "open interest")?;
            let rate = parse(&context.funding, "funding rate")?;
            markets_out.push(MarketStatistics {
                symbol: market.symbol.clone(),
                volume,
                quote_volume,
                open_interest,
                funding_rate: rate,
                funding_rate_annualized: rate * 24.0 * 365.0,
                last_price: context
                    .mid_px
                    .as_deref()
                    .map_or(Ok(mark), |value| parse(value, "mid price"))?,
                mark_price: mark,
            });
            funding.push(FundingRateSnapshot {
                exchange: EXCHANGE.to_string(),
                symbol: market.symbol.clone(),
                timestamp_ms,
                current: rate,
                annualized: Some(rate * 24.0 * 365.0),
            });
        }
        Ok(ExchangeStatistics {
            exchange: EXCHANGE.to_string(),
            timestamp_ms,
            period: "1d".to_string(),
            total_volume_usd: markets_out.iter().map(|market| market.quote_volume).sum(),
            total_open_interest_usd: markets_out
                .iter()
                .map(|market| market.open_interest * market.mark_price)
                .sum(),
            markets: markets_out,
            funding,
        })
    }

    pub async fn inspect_historical() -> Result<OrderBookSnapshot> {
        bail!("Hyperliquid does not provide historical orderbook inspection")
    }

    pub async fn replay_historical() -> Result<Vec<TopOfBook>> {
        bail!("Hyperliquid does not provide historical orderbook replay")
    }
}

fn require_market(symbol: &str) -> Result<std::sync::Arc<markets::HyperliquidMarket>> {
    let market = markets::market(symbol)?;
    if !market.is_available() {
        bail!(
            "Hyperliquid market `{}` is not trading",
            market.venue_symbol
        );
    }
    Ok(market)
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
        28_800 => Ok("8h"),
        43_200 => Ok("12h"),
        86_400 => Ok("1d"),
        259_200 => Ok("3d"),
        604_800 => Ok("1w"),
        2_592_000 => Ok("1M"),
        _ => bail!("unsupported Hyperliquid timeframe seconds: {seconds}"),
    }
}

fn validate_interval(interval: &str) -> Result<()> {
    if matches!(
        interval,
        "1m" | "3m"
            | "5m"
            | "15m"
            | "30m"
            | "1h"
            | "2h"
            | "4h"
            | "8h"
            | "12h"
            | "1d"
            | "3d"
            | "1w"
            | "1M"
    ) {
        Ok(())
    } else {
        bail!("unsupported Hyperliquid candle interval `{interval}`")
    }
}

fn require_timestamp_ms(timestamp: u64, name: &str) -> Result<()> {
    if !(10_000_000_000..10_000_000_000_000).contains(&timestamp) {
        bail!("{name} must be milliseconds at the Market Lab boundary");
    }
    Ok(())
}

fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn parse(value: &str, name: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("invalid Hyperliquid {name} `{value}`"))
}

async fn meta_and_contexts() -> Result<(HyperliquidMeta, Vec<HyperliquidContext>)> {
    let value: serde_json::Value = HyperliquidClient::new()?
        .info(&serde_json::json!({ "type": "metaAndAssetCtxs" }))
        .await?;
    let entries = value
        .as_array()
        .context("Hyperliquid metaAndAssetCtxs must be an array")?;
    if entries.len() != 2 {
        bail!("Hyperliquid metaAndAssetCtxs must contain two entries");
    }
    Ok((
        serde_json::from_value(entries[0].clone())?,
        serde_json::from_value(entries[1].clone())?,
    ))
}

#[derive(Debug, Deserialize)]
struct HyperliquidMeta {
    universe: Vec<HyperliquidAsset>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidAsset {
    name: String,
    #[serde(default)]
    is_delisted: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidContext {
    funding: String,
    open_interest: String,
    prev_day_px: String,
    day_ntl_vlm: String,
    day_base_vlm: String,
    oracle_px: String,
    mark_px: String,
    mid_px: Option<String>,
}

impl HyperliquidContext {
    fn to_ticker(&self, symbol: &str) -> Result<MarketTicker> {
        let mark = parse(&self.mark_px, "mark price")?;
        let previous = parse(&self.prev_day_px, "previous-day price")?;
        let last = self
            .mid_px
            .as_deref()
            .map_or(Ok(mark), |value| parse(value, "mid price"))?;
        Ok(MarketTicker {
            exchange: EXCHANGE.to_string(),
            symbol: symbol.to_string(),
            timestamp_ms: now_ms()?,
            price_change: last - previous,
            price_change_percent: if previous == 0.0 {
                0.0
            } else {
                (last - previous) / previous * 100.0
            },
            last_price: last,
            high_price: 0.0,
            low_price: 0.0,
            volume: parse(&self.day_base_vlm, "day base volume")?,
            quote_volume: parse(&self.day_ntl_vlm, "day notional volume")?,
            mark_price: mark,
            oracle_price: parse(&self.oracle_px, "oracle price")?,
            open_interest: parse(&self.open_interest, "open interest")?,
            funding_rate: parse(&self.funding, "funding rate")?,
            regime: 0,
            regime_dt: 0,
            regime_vol: 0.0,
            regime_mv: 0.0,
            fair_book_price: last,
            fair_vol: 0.0,
            fair_bias: 0.0,
        })
    }
}

#[derive(Debug, Deserialize)]
struct HyperliquidCandle {
    #[serde(rename = "t")]
    open_time: u64,
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    trades: u64,
}

impl HyperliquidCandle {
    fn into_candle(self) -> Result<OhlcvCandle> {
        Ok(OhlcvCandle {
            t: self.open_time,
            close_time: self.close_time,
            o: parse(&self.open, "candle open")?,
            h: parse(&self.high, "candle high")?,
            l: parse(&self.low, "candle low")?,
            c: parse(&self.close, "candle close")?,
            volume: parse(&self.volume, "candle volume")?,
            trades: self.trades,
        })
    }
}

#[derive(Debug, Deserialize)]
struct HyperliquidBook {
    coin: String,
    levels: Vec<Vec<HyperliquidLevel>>,
    time: u64,
}

#[derive(Debug, Deserialize)]
struct HyperliquidLevel {
    px: String,
    sz: String,
}

impl HyperliquidBook {
    fn into_snapshot(
        self,
        symbol: &str,
        venue_symbol: &str,
        depth: u16,
    ) -> Result<OrderBookSnapshot> {
        if self.coin != venue_symbol || self.levels.len() != 2 {
            bail!("Hyperliquid returned an invalid orderbook snapshot");
        }
        let mut sides = self.levels.into_iter();
        let parse_side = |levels: Vec<HyperliquidLevel>| {
            levels
                .into_iter()
                .take(depth as usize)
                .map(|level| {
                    Ok(OrderBookLevel {
                        price: parse(&level.px, "book price")?,
                        quantity: parse(&level.sz, "book size")?,
                    })
                })
                .collect::<Result<Vec<_>>>()
        };
        Ok(OrderBookSnapshot {
            exchange: EXCHANGE.to_string(),
            symbol: symbol.to_string(),
            timestamp_ms: self.time,
            bids: parse_side(sides.next().expect("book length checked"))?,
            asks: parse_side(sides.next().expect("book length checked"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_supported_timeframes() {
        assert_eq!(timeframe_from_seconds(60).expect("1m"), "1m");
        assert_eq!(timeframe_from_seconds(604_800).expect("1w"), "1w");
        assert!(timeframe_from_seconds(10).is_err());
    }

    #[test]
    fn parses_candle_strings_at_app_boundary() {
        let raw: HyperliquidCandle = serde_json::from_value(serde_json::json!({
            "t": 1_700_000_000_000_u64, "T": 1_700_000_059_999_u64,
            "o": "100", "h": "102", "l": "99", "c": "101", "v": "12.5", "n": 7
        }))
        .expect("fixture parses");
        let candle = raw.into_candle().expect("candle converts");
        assert_eq!(candle.c, 101.0);
        assert_eq!(candle.trades, 7);
    }
}
