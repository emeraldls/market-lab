use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::domain::types::{
    ExchangeStatistics, FundingRateSnapshot, MarketStatistics, MarketTicker, OhlcvCandle,
    OhlcvSeries, OpenInterestSnapshot, OrderBookLevel, OrderBookSnapshot, ProviderHealth,
    TopOfBook, VolumeBar, VolumeBarSeries,
};

use super::catalog;
use super::client::BulkClient;

const EXCHANGE: &str = "bulk";

pub struct BulkProvider;

impl BulkProvider {
    pub fn capabilities() -> serde_json::Value {
        serde_json::json!({
            "authentication": {
                "market_data_requires_api_key": false,
                "execution_requires_agent_wallet": true
            },
            "symbols": {
                "internal_format": "BASE/USDT",
                "venue_format": "BASE-USD",
                "catalog": "embedded"
            },
            "time": {
                "app_boundary": "milliseconds",
                "provider_conversion": "adapter-only"
            },
            "execution": {
                "venue": "bulk",
                "orders": ["market", "limit"],
                "time_in_force": ["GTC", "IOC", "ALO"],
                "reduce_only": true,
                "agent_signing": true,
                "runtime": "mlabd"
            },
            "historical": {
                "candles": true,
                "volume_bars": true,
                "open_interest": false,
                "funding": false,
                "orderbook": false,
                "volume_delta": false
            },
            "snapshots": {
                "orderbook": true,
                "ticker": true,
                "statistics": true,
                "open_interest": true,
                "funding": true
            },
            "streams": {
                "candles": true,
                "orderbook": true,
                "ticker": true,
                "open_interest": true,
                "funding": true,
                "trades": true,
                "trade_derived_volume_delta": true
            }
        })
    }

    pub async fn health() -> Result<ProviderHealth> {
        let ticker = Self::ticker("BTC/USDT").await?;
        Ok(ProviderHealth {
            provider: EXCHANGE.to_string(),
            status: "ok".to_string(),
            details: serde_json::json!({
                "public_market_data": true,
                "requires_api_key": false,
                "timestamp_ms": ticker.timestamp_ms,
                "capabilities": Self::capabilities(),
            }),
        })
    }

    pub async fn candles(symbol: &str, interval: &str, from: u64, to: u64) -> Result<OhlcvSeries> {
        let market = require_market(symbol)?;
        require_app_timestamp_ms(from, "candle start time")?;
        require_app_timestamp_ms(to, "candle end time")?;
        if from >= to {
            bail!("candle start time must be less than end time");
        }

        let query = [
            ("symbol", market.symbol.clone()),
            ("interval", interval.to_string()),
            ("startTime", from.to_string()),
            ("endTime", to.to_string()),
        ];
        let client = BulkClient::new()?;
        let raw: Vec<BulkKline> = client.get("klines", &query).await?;
        let mut data = raw.into_iter().map(OhlcvCandle::from).collect::<Vec<_>>();
        data.sort_by_key(|candle| candle.t);

        Ok(OhlcvSeries {
            exchange: EXCHANGE.to_string(),
            symbol: market.internal_symbol.clone(),
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
        aggregation: Option<f64>,
    ) -> Result<OrderBookSnapshot> {
        if depth == 0 {
            bail!("orderbook depth must be at least 1");
        }
        let market = require_market(symbol)?;
        let mut query = vec![
            ("type", "l2book".to_string()),
            ("coin", market.symbol.clone()),
            ("nlevels", depth.to_string()),
        ];
        if let Some(aggregation) = aggregation {
            if !aggregation.is_finite() || aggregation <= 0.0 {
                bail!("orderbook aggregation must be greater than zero");
            }
            query.push(("aggregation", aggregation.to_string()));
        }

        let client = BulkClient::new()?;
        let raw: BulkL2Book = client.get("l2book", &query).await?;
        raw.into_snapshot(&market.internal_symbol, depth)
    }

    pub async fn ticker(symbol: &str) -> Result<MarketTicker> {
        let market = require_market(symbol)?;
        let client = BulkClient::new()?;
        let raw: BulkTicker = client
            .get_without_query(&format!("ticker/{}", market.symbol))
            .await?;
        raw.into_ticker(&market.internal_symbol)
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
            annualized: None,
        })
    }

    pub async fn statistics(period: &str, symbol: Option<&str>) -> Result<ExchangeStatistics> {
        let mut query = vec![("period", period.to_string())];
        if let Some(symbol) = symbol {
            query.push(("symbol", require_market(symbol)?.symbol.clone()));
        }
        let client = BulkClient::new()?;
        let raw: BulkStatistics = client.get("stats", &query).await?;
        raw.into_statistics()
    }

    pub async fn inspect_historical() -> Result<OrderBookSnapshot> {
        bail!("BULK does not provide historical orderbook inspection")
    }

    pub async fn replay_historical() -> Result<Vec<TopOfBook>> {
        bail!("BULK does not provide historical orderbook replay")
    }
}

fn require_market(symbol: &str) -> Result<&'static catalog::BulkMarket> {
    let market = catalog::market(symbol)?;
    if !market.is_trading() {
        bail!("BULK market `{}` is not trading", market.symbol);
    }
    Ok(market)
}

pub fn timeframe_from_seconds(seconds: u32) -> Result<&'static str> {
    match seconds {
        10 => Ok("10s"),
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
        _ => bail!("unsupported BULK timeframe seconds: {seconds}"),
    }
}

pub fn normalize_timestamp_ms(timestamp: u64) -> u64 {
    match timestamp {
        0..=9_999_999_999 => timestamp.saturating_mul(1_000),
        10_000_000_000..=9_999_999_999_999 => timestamp,
        10_000_000_000_000..=9_999_999_999_999_999 => timestamp / 1_000,
        _ => timestamp / 1_000_000,
    }
}

fn require_app_timestamp_ms(timestamp: u64, name: &str) -> Result<()> {
    if !(10_000_000_000..10_000_000_000_000).contains(&timestamp) {
        bail!("{name} must be milliseconds at the Market Lab boundary");
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
pub(crate) struct BulkKline {
    t: u64,
    #[serde(rename = "T")]
    close_time: u64,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    v: f64,
    n: u64,
}

impl From<BulkKline> for OhlcvCandle {
    fn from(value: BulkKline) -> Self {
        Self {
            t: normalize_timestamp_ms(value.t),
            close_time: normalize_timestamp_ms(value.close_time),
            o: value.o,
            h: value.h,
            l: value.l,
            c: value.c,
            volume: value.v,
            trades: value.n,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkL2Book {
    update_type: Option<String>,
    symbol: String,
    levels: Vec<Vec<BulkL2Level>>,
    timestamp: u64,
}

impl BulkL2Book {
    fn into_snapshot(self, internal_symbol: &str, depth: u16) -> Result<OrderBookSnapshot> {
        if self
            .update_type
            .as_deref()
            .is_some_and(|kind| kind != "snapshot")
        {
            bail!("BULK l2book returned a non-snapshot update");
        }
        if self.levels.len() != 2 {
            bail!("BULK l2book must contain exactly bid and ask arrays");
        }
        let expected = catalog::market(internal_symbol)?.symbol.as_str();
        if self.symbol != expected {
            bail!(
                "BULK l2book returned symbol `{}`; expected `{expected}`",
                self.symbol
            );
        }

        let mut sides = self.levels.into_iter();
        let bids = sides
            .next()
            .expect("length checked")
            .into_iter()
            .take(depth as usize)
            .map(OrderBookLevel::from)
            .collect();
        let asks = sides
            .next()
            .expect("length checked")
            .into_iter()
            .take(depth as usize)
            .map(OrderBookLevel::from)
            .collect();

        Ok(OrderBookSnapshot {
            exchange: EXCHANGE.to_string(),
            symbol: internal_symbol.to_string(),
            timestamp_ms: normalize_timestamp_ms(self.timestamp),
            bids,
            asks,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BulkL2Level {
    px: f64,
    sz: f64,
    #[serde(rename = "n")]
    _orders: u64,
}

impl From<BulkL2Level> for OrderBookLevel {
    fn from(value: BulkL2Level) -> Self {
        Self {
            price: value.px,
            quantity: value.sz,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkTicker {
    symbol: String,
    price_change: f64,
    price_change_percent: f64,
    last_price: f64,
    high_price: f64,
    low_price: f64,
    volume: f64,
    quote_volume: f64,
    mark_price: f64,
    oracle_price: f64,
    open_interest: f64,
    funding_rate: f64,
    regime: i32,
    regime_dt: u64,
    regime_vol: f64,
    regime_mv: f64,
    fair_book_px: f64,
    fair_vol: f64,
    fair_bias: f64,
    timestamp: u64,
}

impl BulkTicker {
    pub(crate) fn into_ticker(self, internal_symbol: &str) -> Result<MarketTicker> {
        let market = catalog::market(internal_symbol)?;
        if self.symbol != market.symbol {
            bail!(
                "BULK ticker returned symbol `{}`; expected `{}`",
                self.symbol,
                market.symbol
            );
        }
        Ok(MarketTicker {
            exchange: EXCHANGE.to_string(),
            symbol: market.internal_symbol.clone(),
            timestamp_ms: normalize_timestamp_ms(self.timestamp),
            price_change: self.price_change,
            price_change_percent: self.price_change_percent,
            last_price: self.last_price,
            high_price: self.high_price,
            low_price: self.low_price,
            volume: self.volume,
            quote_volume: self.quote_volume,
            mark_price: self.mark_price,
            oracle_price: self.oracle_price,
            open_interest: self.open_interest,
            funding_rate: self.funding_rate,
            regime: self.regime,
            regime_dt: self.regime_dt,
            regime_vol: self.regime_vol,
            regime_mv: self.regime_mv,
            fair_book_price: self.fair_book_px,
            fair_vol: self.fair_vol,
            fair_bias: self.fair_bias,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkStatistics {
    timestamp: u64,
    period: String,
    volume: BulkTotalUsd,
    open_interest: BulkTotalUsd,
    funding: BulkFunding,
    markets: Vec<BulkMarketStatistics>,
}

impl BulkStatistics {
    fn into_statistics(self) -> Result<ExchangeStatistics> {
        let timestamp_ms = normalize_timestamp_ms(self.timestamp);
        let markets = self
            .markets
            .into_iter()
            .map(BulkMarketStatistics::into_statistics)
            .collect::<Result<Vec<_>>>()?;
        let funding = self
            .funding
            .rates
            .into_iter()
            .map(|(venue_symbol, rate)| {
                let market = catalog::market(&venue_symbol).with_context(|| {
                    format!("BULK stats returned unknown market `{venue_symbol}`")
                })?;
                Ok(FundingRateSnapshot {
                    exchange: EXCHANGE.to_string(),
                    symbol: market.internal_symbol.clone(),
                    timestamp_ms,
                    current: rate.current,
                    annualized: Some(rate.annualized),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ExchangeStatistics {
            exchange: EXCHANGE.to_string(),
            timestamp_ms,
            period: self.period,
            total_volume_usd: self.volume.total_usd,
            total_open_interest_usd: self.open_interest.total_usd,
            markets,
            funding,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkTotalUsd {
    total_usd: f64,
}

#[derive(Debug, Deserialize)]
struct BulkFunding {
    rates: BTreeMap<String, BulkFundingRate>,
}

#[derive(Debug, Deserialize)]
struct BulkFundingRate {
    current: f64,
    annualized: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkMarketStatistics {
    symbol: String,
    volume: f64,
    quote_volume: f64,
    open_interest: f64,
    funding_rate: f64,
    funding_rate_annualized: f64,
    last_price: f64,
    mark_price: f64,
}

impl BulkMarketStatistics {
    fn into_statistics(self) -> Result<MarketStatistics> {
        let market = catalog::market(&self.symbol)
            .with_context(|| format!("BULK stats returned unknown market `{}`", self.symbol))?;
        Ok(MarketStatistics {
            symbol: market.internal_symbol.clone(),
            volume: self.volume,
            quote_volume: self.quote_volume,
            open_interest: self.open_interest,
            funding_rate: self.funding_rate,
            funding_rate_annualized: self.funding_rate_annualized,
            last_price: self.last_price,
            mark_price: self.mark_price,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_seconds_milliseconds_microseconds_and_nanoseconds() {
        assert_eq!(normalize_timestamp_ms(1_700_000_000), 1_700_000_000_000);
        assert_eq!(normalize_timestamp_ms(1_700_000_000_000), 1_700_000_000_000);
        assert_eq!(
            normalize_timestamp_ms(1_700_000_000_000_000),
            1_700_000_000_000
        );
        assert_eq!(
            normalize_timestamp_ms(1_700_000_000_000_000_000),
            1_700_000_000_000
        );
    }

    #[test]
    fn parses_live_shape_fixtures() {
        let klines: Vec<BulkKline> = serde_json::from_str(include_str!("fixtures/klines.json"))
            .expect("kline fixture parses");
        let kline = klines.into_iter().next().expect("fixture has a candle");
        let candle = OhlcvCandle::from(kline);
        assert_eq!(candle.volume, 0.679409);
        assert_eq!(candle.trades, 4965);

        let book: BulkL2Book = serde_json::from_str(include_str!("fixtures/l2book.json"))
            .expect("book fixture parses");
        let snapshot = book.into_snapshot("BTC/USDT", 10).expect("book converts");
        assert_eq!(snapshot.timestamp_ms, 1_784_055_043_011);
        assert_eq!(snapshot.bids[0].price, 64536.7);

        let ticker: BulkTicker = serde_json::from_str(include_str!("fixtures/ticker.json"))
            .expect("ticker fixture parses");
        let ticker = ticker.into_ticker("BTC/USDT").expect("ticker converts");
        assert_eq!(ticker.timestamp_ms, 1_784_056_184_006);
        assert_eq!(ticker.symbol, "BTC/USDT");

        let stats: BulkStatistics = serde_json::from_str(include_str!("fixtures/stats.json"))
            .expect("stats fixture parses");
        let stats = stats.into_statistics().expect("stats convert");
        assert_eq!(stats.period, "1d");
        assert_eq!(stats.markets[0].symbol, "BTC/USDT");
        assert_eq!(stats.funding[0].timestamp_ms, 1_784_056_178_873);
    }

    #[test]
    fn requires_milliseconds_at_the_app_boundary() {
        assert!(require_app_timestamp_ms(1_700_000_000, "time").is_err());
        assert!(require_app_timestamp_ms(1_700_000_000_000, "time").is_ok());
        assert!(require_app_timestamp_ms(1_700_000_000_000_000, "time").is_err());
    }

    #[test]
    fn maps_all_documented_bulk_timeframes() {
        assert_eq!(timeframe_from_seconds(10).expect("10s"), "10s");
        assert_eq!(timeframe_from_seconds(604_800).expect("1w"), "1w");
        assert_eq!(timeframe_from_seconds(2_592_000).expect("1M"), "1M");
        assert!(timeframe_from_seconds(42).is_err());
    }
}
