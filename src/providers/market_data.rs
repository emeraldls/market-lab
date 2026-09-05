use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::domain::execution::ExecutionVenue;
use crate::domain::types::{
    ExchangeStatistics, FundingRateSnapshot, MarketTicker, OhlcvCandle, OhlcvSeries,
    OpenInterestSnapshot, OrderBookSnapshot, ProviderHealth, TopOfBook, TradeTick, VolumeBarSeries,
};
use crate::providers::binance::{BinanceMarket, BinanceProvider};
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::ws::{
    BulkCandleStream, BulkOrderBookStream, BulkTickerStream, BulkTradesStream,
};
use crate::providers::hyperliquid::market_data::HyperliquidProvider;
use crate::providers::hyperliquid::ws::{
    HyperliquidAssetContextStream, HyperliquidCandleStream, HyperliquidOrderBookStream,
    HyperliquidTradesStream,
};
use crate::providers::hyperliquid::{HyperliquidNetwork, HyperliquidProduct};
use crate::venues::ExecutionBackend;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MarketDataCapabilities {
    pub historical_candles: bool,
    pub historical_volume_bars: bool,
    pub live_orderbook: bool,
    pub live_trades: bool,
    pub live_candles: bool,
    pub live_ticker: bool,
}

/// Complete direct-market-data contract used outside provider implementations.
///
/// Commands decide what to render; providers decide how data is fetched. Adding
/// a standalone exchange therefore requires one implementation and one factory
/// registration here, never edits to bots, strategies, jobs, or scripting.
#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    fn exchange(&self) -> &str;
    fn label(&self) -> &str;
    fn capabilities(&self) -> MarketDataCapabilities;
    fn timeframe_from_seconds(&self, seconds: u32) -> Result<&'static str>;

    fn annualized_funding_rate(&self, _current: f64) -> Option<f64> {
        None
    }

    async fn health(&self) -> Result<ProviderHealth>;

    async fn candles(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        let _ = (symbol, interval, from, to);
        bail!("{} does not provide historical candles", self.label())
    }

    async fn volume_bars(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        let _ = (symbol, interval, from, to);
        bail!("{} does not provide historical volume bars", self.label())
    }

    async fn orderbook(
        &self,
        symbol: &str,
        depth: u16,
        aggregation: Option<f64>,
    ) -> Result<OrderBookSnapshot> {
        let _ = (symbol, depth, aggregation);
        bail!("{} does not provide order-book snapshots", self.label())
    }

    async fn ticker(&self, symbol: &str) -> Result<MarketTicker> {
        let _ = symbol;
        bail!("{} does not provide ticker snapshots", self.label())
    }

    async fn open_interest(&self, symbol: &str) -> Result<OpenInterestSnapshot> {
        let _ = symbol;
        bail!("{} does not provide open interest", self.label())
    }

    async fn funding(&self, symbol: &str) -> Result<FundingRateSnapshot> {
        let _ = symbol;
        bail!("{} does not provide funding", self.label())
    }

    async fn statistics(&self, period: &str, symbol: Option<&str>) -> Result<ExchangeStatistics> {
        let _ = (period, symbol);
        bail!("{} does not provide market statistics", self.label())
    }

    async fn connect_orderbook(&self, symbol: &str, depth: u16)
    -> Result<Box<dyn OrderBookEvents>>;

    async fn connect_trades(&self, symbol: &str) -> Result<Box<dyn TradeEvents>>;

    async fn connect_candles(&self, symbol: &str, interval: &str) -> Result<Box<dyn CandleEvents>>;

    async fn connect_ticker(&self, symbol: &str) -> Result<Box<dyn TickerEvents>>;
}

struct BulkMarketData {
    provider: BulkProvider,
}

#[async_trait]
impl MarketDataProvider for BulkMarketData {
    fn exchange(&self) -> &str {
        "bulkf"
    }

    fn label(&self) -> &str {
        "BULK"
    }

    fn capabilities(&self) -> MarketDataCapabilities {
        MarketDataCapabilities {
            historical_candles: true,
            historical_volume_bars: true,
            live_orderbook: true,
            live_trades: true,
            live_candles: true,
            live_ticker: true,
        }
    }

    fn timeframe_from_seconds(&self, seconds: u32) -> Result<&'static str> {
        crate::providers::bulk::market_data::timeframe_from_seconds(seconds)
    }

    async fn health(&self) -> Result<ProviderHealth> {
        self.provider.health().await
    }

    async fn candles(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        self.provider.candles(symbol, interval, from, to).await
    }

    async fn volume_bars(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        self.provider.volume_bars(symbol, interval, from, to).await
    }

    async fn orderbook(
        &self,
        symbol: &str,
        depth: u16,
        aggregation: Option<f64>,
    ) -> Result<OrderBookSnapshot> {
        self.provider
            .live_orderbook(symbol, depth, aggregation)
            .await
    }

    async fn ticker(&self, symbol: &str) -> Result<MarketTicker> {
        self.provider.ticker(symbol).await
    }

    async fn open_interest(&self, symbol: &str) -> Result<OpenInterestSnapshot> {
        self.provider.open_interest(symbol).await
    }

    async fn funding(&self, symbol: &str) -> Result<FundingRateSnapshot> {
        self.provider.funding(symbol).await
    }

    async fn statistics(&self, period: &str, symbol: Option<&str>) -> Result<ExchangeStatistics> {
        self.provider.statistics(period, symbol).await
    }

    async fn connect_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> Result<Box<dyn OrderBookEvents>> {
        Ok(Box::new(
            BulkOrderBookStream::connect(self.provider.network(), symbol, depth).await?,
        ))
    }

    async fn connect_trades(&self, symbol: &str) -> Result<Box<dyn TradeEvents>> {
        Ok(Box::new(
            BulkTradesStream::connect(self.provider.network(), symbol).await?,
        ))
    }

    async fn connect_candles(&self, symbol: &str, interval: &str) -> Result<Box<dyn CandleEvents>> {
        Ok(Box::new(
            BulkCandleStream::connect(self.provider.network(), symbol, interval).await?,
        ))
    }

    async fn connect_ticker(&self, symbol: &str) -> Result<Box<dyn TickerEvents>> {
        Ok(Box::new(
            BulkTickerStream::connect(self.provider.network(), symbol).await?,
        ))
    }
}

struct HyperliquidMarketData {
    exchange: String,
    label: String,
    product: HyperliquidProduct,
    network: HyperliquidNetwork,
}

#[async_trait]
impl MarketDataProvider for HyperliquidMarketData {
    fn exchange(&self) -> &str {
        &self.exchange
    }

    fn label(&self) -> &str {
        &self.label
    }

    fn capabilities(&self) -> MarketDataCapabilities {
        MarketDataCapabilities {
            historical_candles: true,
            historical_volume_bars: true,
            live_orderbook: true,
            live_trades: true,
            live_candles: true,
            live_ticker: true,
        }
    }

    fn timeframe_from_seconds(&self, seconds: u32) -> Result<&'static str> {
        crate::providers::hyperliquid::market_data::timeframe_from_seconds(seconds)
    }

    fn annualized_funding_rate(&self, current: f64) -> Option<f64> {
        Some(current * 24.0 * 365.0)
    }

    async fn health(&self) -> Result<ProviderHealth> {
        HyperliquidProvider::health().await
    }

    async fn candles(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        HyperliquidProvider::candles_for(self.product, symbol, interval, from, to, self.network)
            .await
    }

    async fn volume_bars(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        HyperliquidProvider::volume_bars_for(self.product, symbol, interval, from, to, self.network)
            .await
    }

    async fn orderbook(
        &self,
        symbol: &str,
        depth: u16,
        aggregation: Option<f64>,
    ) -> Result<OrderBookSnapshot> {
        HyperliquidProvider::live_orderbook_for(
            self.product,
            symbol,
            depth,
            aggregation,
            self.network,
        )
        .await
    }

    async fn ticker(&self, symbol: &str) -> Result<MarketTicker> {
        HyperliquidProvider::ticker_for(self.product, symbol, self.network).await
    }

    async fn open_interest(&self, symbol: &str) -> Result<OpenInterestSnapshot> {
        HyperliquidProvider::open_interest_for(self.product, symbol, self.network).await
    }

    async fn funding(&self, symbol: &str) -> Result<FundingRateSnapshot> {
        HyperliquidProvider::funding_for(self.product, symbol, self.network).await
    }

    async fn statistics(&self, period: &str, symbol: Option<&str>) -> Result<ExchangeStatistics> {
        HyperliquidProvider::statistics_for(self.product, period, symbol).await
    }

    async fn connect_orderbook(
        &self,
        symbol: &str,
        depth: u16,
    ) -> Result<Box<dyn OrderBookEvents>> {
        Ok(Box::new(
            HyperliquidOrderBookStream::connect_for(
                self.product,
                symbol,
                depth.min(20),
                self.network,
            )
            .await?,
        ))
    }

    async fn connect_trades(&self, symbol: &str) -> Result<Box<dyn TradeEvents>> {
        Ok(Box::new(
            HyperliquidTradesStream::connect_for(self.product, symbol, self.network).await?,
        ))
    }

    async fn connect_candles(&self, symbol: &str, interval: &str) -> Result<Box<dyn CandleEvents>> {
        Ok(Box::new(
            HyperliquidCandleStream::connect_for(self.product, symbol, interval, self.network)
                .await?,
        ))
    }

    async fn connect_ticker(&self, symbol: &str) -> Result<Box<dyn TickerEvents>> {
        Ok(Box::new(
            HyperliquidAssetContextStream::connect_for(self.product, symbol, self.network).await?,
        ))
    }
}

struct BinanceMarketData {
    market: BinanceMarket,
}

#[async_trait]
impl MarketDataProvider for BinanceMarketData {
    fn exchange(&self) -> &str {
        self.market.exchange()
    }

    fn label(&self) -> &str {
        match self.market {
            BinanceMarket::Spot => "Binance Spot",
            BinanceMarket::Futures => "Binance Futures",
        }
    }

    fn capabilities(&self) -> MarketDataCapabilities {
        MarketDataCapabilities {
            historical_candles: true,
            historical_volume_bars: true,
            ..MarketDataCapabilities::default()
        }
    }

    fn timeframe_from_seconds(&self, seconds: u32) -> Result<&'static str> {
        crate::providers::binance::market_data::timeframe_from_seconds(seconds)
    }

    async fn health(&self) -> Result<ProviderHealth> {
        BinanceProvider::health(self.market).await
    }

    async fn candles(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        BinanceProvider::candles_paginated(self.market, symbol, interval, from, to).await
    }

    async fn volume_bars(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        BinanceProvider::volume_bars(self.market, symbol, interval, from, to).await
    }

    async fn connect_orderbook(
        &self,
        _symbol: &str,
        _depth: u16,
    ) -> Result<Box<dyn OrderBookEvents>> {
        bail!("{} live order books are not implemented", self.label())
    }

    async fn connect_trades(&self, _symbol: &str) -> Result<Box<dyn TradeEvents>> {
        bail!("{} live trades are not implemented", self.label())
    }

    async fn connect_candles(
        &self,
        _symbol: &str,
        _interval: &str,
    ) -> Result<Box<dyn CandleEvents>> {
        bail!("{} live candles are not implemented", self.label())
    }

    async fn connect_ticker(&self, _symbol: &str) -> Result<Box<dyn TickerEvents>> {
        bail!("{} live tickers are not implemented", self.label())
    }
}

pub struct MarketDataAdapter {
    provider: Box<dyn MarketDataProvider>,
}

impl MarketDataAdapter {
    pub fn for_exchange(exchange: &str, testnet: bool) -> Result<Self> {
        if let Ok(venue) = ExecutionVenue::parse(exchange) {
            return Self::for_venue(venue, testnet);
        }
        let provider: Box<dyn MarketDataProvider> =
            match exchange.trim().to_ascii_lowercase().as_str() {
                "binance" => Box::new(BinanceMarketData {
                    market: BinanceMarket::Spot,
                }),
                "binancef" => Box::new(BinanceMarketData {
                    market: BinanceMarket::Futures,
                }),
                _ => bail!("standalone market-data exchange `{exchange}` is not registered"),
            };
        Ok(Self { provider })
    }

    pub fn for_exchange_market(exchange: &str, testnet: bool, symbol: &str) -> Result<Self> {
        if exchange.eq_ignore_ascii_case(crate::providers::hyperliquid::SPOT_EXCHANGE)
            && crate::markets::outcomes::looks_like_symbol(symbol)
        {
            return Ok(Self {
                provider: Box::new(HyperliquidMarketData {
                    exchange: crate::providers::hyperliquid::SPOT_EXCHANGE.to_string(),
                    label: "Hyperliquid Outcomes".to_string(),
                    product: HyperliquidProduct::Outcome,
                    network: HyperliquidNetwork::from_testnet(testnet),
                }),
            });
        }
        Self::for_exchange(exchange, testnet)
    }

    pub fn for_venue(venue: ExecutionVenue, testnet: bool) -> Result<Self> {
        let spec = venue.spec()?;
        spec.validate_network(testnet)?;
        let market_data = spec.market_data_venue.spec()?;
        let provider: Box<dyn MarketDataProvider> = match market_data.execution {
            ExecutionBackend::Bulk => Box::new(BulkMarketData {
                provider: BulkProvider::new(testnet),
            }),
            ExecutionBackend::Hyperliquid | ExecutionBackend::Hyperlink => {
                let network = if spec.execution == ExecutionBackend::Hyperlink {
                    HyperliquidNetwork::Mainnet
                } else {
                    HyperliquidNetwork::from_testnet(testnet)
                };
                Box::new(HyperliquidMarketData {
                    exchange: market_data.id.to_string(),
                    label: market_data.label(),
                    product: HyperliquidProduct::from_venue(market_data.id)?,
                    network,
                })
            }
        };
        Ok(Self { provider })
    }

    pub fn for_execution_market(
        venue: ExecutionVenue,
        testnet: bool,
        symbol: &str,
    ) -> Result<Self> {
        let spec = venue.spec()?;
        spec.validate_network(testnet)?;
        if crate::markets::execution_market(venue, symbol)? == crate::venues::VenueMarket::Outcome {
            if spec.execution != ExecutionBackend::Hyperliquid {
                bail!("{} does not support outcome markets", spec.label());
            }
            return Ok(Self {
                provider: Box::new(HyperliquidMarketData {
                    exchange: crate::providers::hyperliquid::SPOT_EXCHANGE.to_string(),
                    label: "Hyperliquid Outcomes".to_string(),
                    product: HyperliquidProduct::Outcome,
                    network: HyperliquidNetwork::from_testnet(testnet),
                }),
            });
        }
        Self::for_venue(venue, testnet)
    }

    pub fn exchange(&self) -> &str {
        self.provider.exchange()
    }

    pub fn label(&self) -> &str {
        self.provider.label()
    }

    pub fn capabilities(&self) -> MarketDataCapabilities {
        self.provider.capabilities()
    }

    pub fn timeframe_from_seconds(&self, seconds: u32) -> Result<&'static str> {
        self.provider.timeframe_from_seconds(seconds)
    }

    pub async fn health(&self) -> Result<ProviderHealth> {
        self.provider.health().await
    }

    pub async fn candles(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<OhlcvSeries> {
        self.provider.candles(symbol, interval, from, to).await
    }

    pub async fn volume_bars(
        &self,
        symbol: &str,
        interval: &str,
        from: u64,
        to: u64,
    ) -> Result<VolumeBarSeries> {
        self.provider.volume_bars(symbol, interval, from, to).await
    }

    pub async fn orderbook(
        &self,
        symbol: &str,
        depth: u16,
        aggregation: Option<f64>,
    ) -> Result<OrderBookSnapshot> {
        self.provider.orderbook(symbol, depth, aggregation).await
    }

    pub async fn ticker(&self, symbol: &str) -> Result<MarketTicker> {
        self.provider.ticker(symbol).await
    }

    pub async fn open_interest(&self, symbol: &str) -> Result<OpenInterestSnapshot> {
        self.provider.open_interest(symbol).await
    }

    pub async fn funding(&self, symbol: &str) -> Result<FundingRateSnapshot> {
        self.provider.funding(symbol).await
    }

    pub fn funding_from_ticker(&self, ticker: MarketTicker) -> FundingRateSnapshot {
        FundingRateSnapshot {
            exchange: ticker.exchange,
            symbol: ticker.symbol,
            timestamp_ms: ticker.timestamp_ms,
            current: ticker.funding_rate,
            annualized: self.provider.annualized_funding_rate(ticker.funding_rate),
        }
    }

    pub async fn statistics(
        &self,
        period: &str,
        symbol: Option<&str>,
    ) -> Result<ExchangeStatistics> {
        self.provider.statistics(period, symbol).await
    }
}

#[async_trait]
pub trait CandleEvents: Send {
    async fn next_candle(&mut self) -> Result<OhlcvCandle>;
}

#[async_trait]
impl CandleEvents for BulkCandleStream {
    async fn next_candle(&mut self) -> Result<OhlcvCandle> {
        Self::next_candle(self).await
    }
}

#[async_trait]
impl CandleEvents for HyperliquidCandleStream {
    async fn next_candle(&mut self) -> Result<OhlcvCandle> {
        Self::next_candle(self).await
    }
}

pub struct VenueCandleStream {
    inner: Box<dyn CandleEvents>,
}

impl VenueCandleStream {
    pub async fn connect(
        exchange: &str,
        symbol: &str,
        interval: &str,
        testnet: bool,
    ) -> Result<Self> {
        let adapter = MarketDataAdapter::for_exchange_market(exchange, testnet, symbol)?;
        let inner = adapter.provider.connect_candles(symbol, interval).await?;
        Ok(Self { inner })
    }

    pub async fn next_candle(&mut self) -> Result<OhlcvCandle> {
        self.inner.next_candle().await
    }
}

#[async_trait]
pub trait TickerEvents: Send {
    async fn next_ticker(&mut self) -> Result<MarketTicker>;
}

#[async_trait]
impl TickerEvents for BulkTickerStream {
    async fn next_ticker(&mut self) -> Result<MarketTicker> {
        Self::next_ticker(self).await
    }
}

#[async_trait]
impl TickerEvents for HyperliquidAssetContextStream {
    async fn next_ticker(&mut self) -> Result<MarketTicker> {
        Self::next_ticker(self).await
    }
}

pub struct VenueTickerStream {
    inner: Box<dyn TickerEvents>,
}

impl VenueTickerStream {
    pub async fn connect(exchange: &str, symbol: &str, testnet: bool) -> Result<Self> {
        let adapter = MarketDataAdapter::for_exchange_market(exchange, testnet, symbol)?;
        let inner = adapter.provider.connect_ticker(symbol).await?;
        Ok(Self { inner })
    }

    pub async fn next_ticker(&mut self) -> Result<MarketTicker> {
        self.inner.next_ticker().await
    }
}

#[async_trait]
pub trait OrderBookEvents: Send {
    async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot>;
    async fn next_top(&mut self) -> Result<TopOfBook>;
}

#[async_trait]
impl OrderBookEvents for BulkOrderBookStream {
    async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        Self::next_snapshot(self).await
    }

    async fn next_top(&mut self) -> Result<TopOfBook> {
        Self::next_top(self).await
    }
}

#[async_trait]
impl OrderBookEvents for HyperliquidOrderBookStream {
    async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        Self::next_snapshot(self).await
    }

    async fn next_top(&mut self) -> Result<TopOfBook> {
        Self::next_top(self).await
    }
}

pub struct VenueOrderBookStream {
    inner: Box<dyn OrderBookEvents>,
}

impl VenueOrderBookStream {
    pub async fn connect(
        venue: ExecutionVenue,
        symbol: &str,
        depth: u16,
        testnet: bool,
    ) -> Result<Self> {
        let adapter = MarketDataAdapter::for_execution_market(venue, testnet, symbol)?;
        let inner = adapter.provider.connect_orderbook(symbol, depth).await?;
        Ok(Self { inner })
    }

    pub async fn connect_exchange(
        exchange: &str,
        symbol: &str,
        depth: u16,
        testnet: bool,
    ) -> Result<Self> {
        let adapter = MarketDataAdapter::for_exchange_market(exchange, testnet, symbol)?;
        let inner = adapter.provider.connect_orderbook(symbol, depth).await?;
        Ok(Self { inner })
    }

    pub async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        self.inner.next_snapshot().await
    }

    pub async fn next_top(&mut self) -> Result<TopOfBook> {
        self.inner.next_top().await
    }
}

#[async_trait]
pub trait TradeEvents: Send {
    async fn next_trades(&mut self) -> Result<Vec<TradeTick>>;
}

#[async_trait]
impl TradeEvents for BulkTradesStream {
    async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        Self::next_trades(self).await
    }
}

#[async_trait]
impl TradeEvents for HyperliquidTradesStream {
    async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        Self::next_trades(self).await
    }
}

pub struct VenueTradesStream {
    inner: Box<dyn TradeEvents>,
}

impl VenueTradesStream {
    pub async fn connect(venue: ExecutionVenue, symbol: &str, testnet: bool) -> Result<Self> {
        let adapter = MarketDataAdapter::for_execution_market(venue, testnet, symbol)?;
        let inner = adapter.provider.connect_trades(symbol).await?;
        Ok(Self { inner })
    }

    pub async fn connect_exchange(exchange: &str, symbol: &str, testnet: bool) -> Result<Self> {
        let adapter = MarketDataAdapter::for_exchange_market(exchange, testnet, symbol)?;
        let inner = adapter.provider.connect_trades(symbol).await?;
        Ok(Self { inner })
    }

    pub async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        self.inner.next_trades().await
    }
}
