use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use chrono::{SecondsFormat, Utc};
use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::credentials::mmt_api_key;

const SNAPSHOT_SCHEMA_VERSION: u8 = 1;
const BULK_MARKETS_URL: &str = "https://exchange-api.bulk.trade/api/v1/exchangeInfo";
const BINANCE_SPOT_MARKETS_URL: &str = "https://api.binance.com/api/v3/exchangeInfo";
const BINANCE_FUTURES_MARKETS_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
const MMT_MARKETS_URL: &str = "https://eu-central-1.mmt.gg/api/v1/markets";
const MARKET_HTTP_TIMEOUT_SECS: u64 = 15;

static REGISTRY: OnceLock<RwLock<Arc<MarketRegistry>>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderType {
    Standalone,
    Aggregator,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    Spot,
    Futures,
}

impl MarketType {
    pub fn is_futures(self) -> bool {
        self == Self::Futures
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Futures => "futures",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSnapshot {
    pub schema_version: u8,
    pub provider: String,
    pub provider_type: ProviderType,
    pub source_url: String,
    pub fetched_at: String,
    pub exchanges: Vec<ExchangeMarkets>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeMarkets {
    /// Market Lab's canonical exchange identifier.
    pub exchange: String,
    /// Exchange identifier expected by the upstream provider when it differs
    /// from Market Lab's canonical identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_exchange: Option<String>,
    pub name: String,
    pub market_type: MarketType,
    pub markets: Vec<Market>,
}

impl<'de> Deserialize<'de> for ExchangeMarkets {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct WireExchangeMarkets {
            exchange: String,
            #[serde(default)]
            provider_exchange: Option<String>,
            name: String,
            market_type: Option<MarketType>,
            markets: Vec<Market>,
        }

        let wire = WireExchangeMarkets::deserialize(deserializer)?;
        Ok(Self {
            market_type: wire
                .market_type
                .unwrap_or_else(|| classify_exchange_name(&wire.exchange)),
            exchange: wire.exchange,
            provider_exchange: wire.provider_exchange,
            name: wire.name,
            markets: wire.markets,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    /// Market Lab's canonical instrument: BASE for futures, BASE/QUOTE for spot.
    pub symbol: String,
    /// Symbol sent to the selected provider.
    pub provider_symbol: String,
    /// Exchange-native ticker when it differs from the provider symbol.
    pub venue_symbol: String,
    /// Native numeric asset identifier when the venue signs orders by index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    pub base_asset: String,
    pub quote_asset: String,
    pub venue_base_asset: String,
    pub venue_quote_asset: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_increment: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_increment: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionRules>,
    /// Network-specific venue identity. Hyperliquid spot pair IDs and token
    /// names differ between mainnet and testnet even when Market Lab exposes
    /// the same canonical symbol.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub network_variants: BTreeMap<String, NetworkMarket>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkMarket {
    pub provider_symbol: String,
    pub venue_symbol: String,
    pub venue_id: u32,
    pub venue_base_asset: String,
    pub venue_quote_asset: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_token_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_token_index: Option<u32>,
    pub price_increment: f64,
    pub size_increment: f64,
    pub execution: ExecutionRules,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRules {
    pub price_precision: u8,
    pub size_precision: u8,
    pub tick_size: f64,
    pub lot_size: f64,
    pub min_notional: f64,
    pub max_leverage: u16,
    #[serde(default = "default_true")]
    pub cross_margin: bool,
    pub order_types: Vec<String>,
    pub time_in_forces: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExchangeLocation {
    snapshot: usize,
    exchange: usize,
}

#[derive(Debug)]
struct MarketRegistry {
    snapshots: Vec<MarketSnapshot>,
    provider_markets: HashMap<String, HashMap<String, HashMap<String, Arc<Market>>>>,
    provider_exchanges: HashMap<String, HashMap<String, ExchangeLocation>>,
    exchange_markets: HashMap<String, HashMap<String, Arc<Market>>>,
    direct_exchanges: HashMap<String, ExchangeLocation>,
    exchange_types: HashMap<String, MarketType>,
}

#[derive(Debug, Deserialize)]
struct MmtMarketsResponse {
    exchanges: Vec<MmtExchange>,
}

#[derive(Debug, Deserialize)]
struct MmtExchange {
    id: String,
    name: String,
    symbols: Vec<MmtMarket>,
}

#[derive(Debug, Deserialize)]
struct MmtMarket {
    symbol: String,
    exchange_ticker: String,
    base: String,
    quote: String,
    normalised_base: String,
    tick_size: f64,
    step_size: f64,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceMarket {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default)]
    contract_type: Option<String>,
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFilter {
    filter_type: String,
    #[serde(default)]
    tick_size: Option<String>,
    #[serde(default)]
    step_size: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkMarket {
    symbol: String,
    base_asset: String,
    quote_asset: String,
    status: String,
    price_precision: u8,
    size_precision: u8,
    tick_size: f64,
    lot_size: f64,
    min_notional: f64,
    max_leverage: u16,
    order_types: Vec<String>,
    time_in_forces: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidMarket {
    name: String,
    sz_decimals: u8,
    max_leverage: u16,
    #[serde(default)]
    is_delisted: bool,
    #[serde(default)]
    only_isolated: bool,
    #[serde(default)]
    margin_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidAssetContext {
    mark_px: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidSpotMetadata {
    tokens: Vec<HyperliquidSpotToken>,
    universe: Vec<HyperliquidSpotPair>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidSpotToken {
    name: String,
    sz_decimals: u8,
    index: u32,
    token_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidSpotPair {
    name: String,
    tokens: [u32; 2],
    index: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidSpotContext {
    coin: String,
    mark_px: String,
}

impl Market {
    pub fn is_available(&self) -> bool {
        matches!(
            self.status.to_ascii_lowercase().as_str(),
            "active" | "available" | "open" | "trading"
        )
    }

    pub fn execution_rules(&self) -> Result<&ExecutionRules> {
        self.execution.as_ref().with_context(|| {
            format!(
                "{} is available for market data but has no execution rules in this snapshot",
                self.symbol
            )
        })
    }

    pub fn supports_order_type(&self, order_type: &str) -> bool {
        self.execution.as_ref().is_some_and(|rules| {
            rules
                .order_types
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(order_type))
        })
    }

    pub fn network_variant(&self, network: &str) -> Result<NetworkMarket> {
        let network = key(network);
        if let Some(variant) = self.network_variants.get(&network) {
            return Ok(variant.clone());
        }
        if network == "mainnet" {
            return Ok(NetworkMarket {
                provider_symbol: self.provider_symbol.clone(),
                venue_symbol: self.venue_symbol.clone(),
                venue_id: self
                    .venue_id
                    .with_context(|| format!("{} does not have a numeric venue id", self.symbol))?,
                venue_base_asset: self.venue_base_asset.clone(),
                venue_quote_asset: self.venue_quote_asset.clone(),
                base_token_id: None,
                quote_token_id: None,
                base_token_index: None,
                quote_token_index: None,
                price_increment: self
                    .price_increment
                    .context("market snapshot omitted price increment")?,
                size_increment: self
                    .size_increment
                    .context("market snapshot omitted size increment")?,
                execution: self.execution_rules()?.clone(),
            });
        }
        bail!("{} is not available on Hyperliquid {network}", self.symbol)
    }

    fn lookup_symbols(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.symbol.as_str())
            .chain(std::iter::once(self.provider_symbol.as_str()))
            .chain(std::iter::once(self.venue_symbol.as_str()))
            .chain(self.aliases.iter().map(String::as_str))
    }

    fn validate(&self, provider: &str, exchange: &str, market_type: MarketType) -> Result<()> {
        canonical_market_symbol(&self.symbol, market_type)?;
        if self.provider_symbol.trim().is_empty() || self.venue_symbol.trim().is_empty() {
            bail!(
                "{provider}/{exchange} market {} has an empty provider symbol",
                self.symbol
            );
        }
        if self.base_asset.trim().is_empty()
            || self.quote_asset.trim().is_empty()
            || self.venue_base_asset.trim().is_empty()
            || self.venue_quote_asset.trim().is_empty()
            || self.status.trim().is_empty()
        {
            bail!(
                "{provider}/{exchange} market {} has incomplete identity metadata",
                self.symbol
            );
        }
        validate_optional_increment(
            self.price_increment,
            "price",
            provider,
            exchange,
            &self.symbol,
        )?;
        validate_optional_increment(
            self.size_increment,
            "size",
            provider,
            exchange,
            &self.symbol,
        )?;
        if let Some(rules) = &self.execution {
            rules.validate(provider, exchange, &self.symbol)?;
        }
        for (network, variant) in &self.network_variants {
            if network.trim().is_empty()
                || variant.provider_symbol.trim().is_empty()
                || variant.venue_symbol.trim().is_empty()
                || variant.venue_base_asset.trim().is_empty()
                || variant.venue_quote_asset.trim().is_empty()
            {
                bail!(
                    "{provider}/{exchange} market {} has incomplete {network} identity metadata",
                    self.symbol
                );
            }
            validate_optional_increment(
                Some(variant.price_increment),
                "network price",
                provider,
                exchange,
                &self.symbol,
            )?;
            validate_optional_increment(
                Some(variant.size_increment),
                "network size",
                provider,
                exchange,
                &self.symbol,
            )?;
            variant
                .execution
                .validate(provider, exchange, &self.symbol)?;
        }
        Ok(())
    }
}

impl ExecutionRules {
    fn validate(&self, provider: &str, exchange: &str, symbol: &str) -> Result<()> {
        for (name, value) in [
            ("tick size", self.tick_size),
            ("lot size", self.lot_size),
            ("minimum notional", self.min_notional),
        ] {
            if !value.is_finite() || value <= 0.0 {
                bail!("{provider}/{exchange} market {symbol} has invalid {name}");
            }
        }
        if self.max_leverage == 0 || self.order_types.is_empty() {
            bail!("{provider}/{exchange} market {symbol} has incomplete execution rules");
        }
        Ok(())
    }
}

impl MarketSnapshot {
    fn validate(&self) -> Result<()> {
        if self.schema_version != SNAPSHOT_SCHEMA_VERSION {
            bail!(
                "unsupported market snapshot schema version {} for provider {}",
                self.schema_version,
                self.provider
            );
        }
        if matches!(key(&self.provider).as_str(), "hyperliquid" | "hyperliquidf")
            && self.source_url.contains("hyperliquid-testnet")
        {
            bail!(
                "the installed Hyperliquid market snapshot is from testnet; run `mlab markets --exchange hyperliquidf --refresh` to replace it with mainnet markets"
            );
        }
        if self.provider.trim().is_empty()
            || self.source_url.trim().is_empty()
            || self.fetched_at.trim().is_empty()
            || self.exchanges.is_empty()
        {
            bail!(
                "market snapshot for provider {} is incomplete",
                self.provider
            );
        }
        for exchange in &self.exchanges {
            if exchange.exchange.trim().is_empty() || exchange.markets.is_empty() {
                bail!(
                    "market snapshot for provider {} contains an empty exchange",
                    self.provider
                );
            }
            for market in &exchange.markets {
                market.validate(&self.provider, &exchange.exchange, exchange.market_type)?;
            }
        }
        Ok(())
    }
}

impl MarketRegistry {
    #[cfg(not(test))]
    fn load(directory: &Path) -> Result<Self> {
        let entries = fs::read_dir(directory).with_context(|| {
            format!(
                "market snapshots are not installed at {}; run `mlab markets --exchange bulkf --refresh`",
                directory.display()
            )
        })?;
        let mut paths = entries
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.retain(|path| {
            path.extension().and_then(|value| value.to_str()) == Some("json")
                && path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|name| name.ends_with("-markets.json"))
        });
        paths.sort();
        if paths.is_empty() {
            bail!(
                "market snapshots are not installed at {}; run `mlab markets --exchange bulkf --refresh`",
                directory.display()
            );
        }

        let snapshots = paths
            .into_iter()
            .map(|path| {
                let source = fs::read_to_string(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                let mut snapshot = serde_json::from_str::<MarketSnapshot>(&source)
                    .with_context(|| format!("market snapshot {} is malformed", path.display()))?;
                canonicalize_snapshot(&mut snapshot);
                let expected_name = format!("{}-markets.json", key(&snapshot.provider));
                let actual_name = path.file_name().and_then(|value| value.to_str());
                let legacy_hyperliquid_name =
                    snapshot.provider.eq_ignore_ascii_case("hyperliquidf")
                        && actual_name == Some("hyperliquid-markets.json");
                if actual_name != Some(expected_name.as_str()) && !legacy_hyperliquid_name {
                    bail!(
                        "market snapshot {} must be named {expected_name}",
                        path.display()
                    );
                }
                Ok(snapshot)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(snapshots)
    }

    fn new(mut snapshots: Vec<MarketSnapshot>) -> Result<Self> {
        snapshots.iter_mut().for_each(canonicalize_snapshot);
        let mut registry = Self {
            snapshots,
            provider_markets: HashMap::new(),
            provider_exchanges: HashMap::new(),
            exchange_markets: HashMap::new(),
            direct_exchanges: HashMap::new(),
            exchange_types: HashMap::new(),
        };
        registry.build_indexes()?;
        Ok(registry)
    }

    fn build_indexes(&mut self) -> Result<()> {
        for (snapshot_index, snapshot) in self.snapshots.iter().enumerate() {
            snapshot.validate()?;
            let provider = key(&snapshot.provider);
            let provider_exchanges = self.provider_exchanges.entry(provider.clone()).or_default();
            let provider_markets = self.provider_markets.entry(provider).or_default();

            for (exchange_index, exchange) in snapshot.exchanges.iter().enumerate() {
                let exchange_key = key(&exchange.exchange);
                if let Some(existing) = self
                    .exchange_types
                    .insert(exchange_key.clone(), exchange.market_type)
                    && existing != exchange.market_type
                {
                    bail!(
                        "exchange {} has conflicting market types across installed snapshots",
                        exchange.exchange
                    );
                }
                let exchange_location = ExchangeLocation {
                    snapshot: snapshot_index,
                    exchange: exchange_index,
                };
                if provider_exchanges
                    .insert(exchange_key.clone(), exchange_location)
                    .is_some()
                {
                    bail!(
                        "provider {} contains duplicate exchange {}",
                        snapshot.provider,
                        exchange.exchange
                    );
                }

                let markets = provider_markets.entry(exchange_key.clone()).or_default();
                let mut indexed_markets = Vec::with_capacity(exchange.markets.len());
                for market in &exchange.markets {
                    let market = Arc::new(market.clone());
                    insert_market_aliases(
                        markets,
                        Arc::clone(&market),
                        &snapshot.provider,
                        &exchange.exchange,
                    )?;
                    indexed_markets.push(market);
                }

                if snapshot.provider_type == ProviderType::Standalone {
                    if self
                        .direct_exchanges
                        .insert(exchange_key.clone(), exchange_location)
                        .is_some()
                    {
                        bail!(
                            "multiple standalone providers claim exchange {}",
                            exchange.exchange
                        );
                    }
                    let direct_markets = self.exchange_markets.entry(exchange_key).or_default();
                    for market in indexed_markets {
                        insert_market_aliases(
                            direct_markets,
                            market,
                            &snapshot.provider,
                            &exchange.exchange,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn exchange(&self, location: ExchangeLocation) -> (MarketSnapshot, ExchangeMarkets) {
        let snapshot = &self.snapshots[location.snapshot];
        (
            snapshot.clone(),
            snapshot.exchanges[location.exchange].clone(),
        )
    }
}

pub fn provider_market(provider: &str, exchange: &str, symbol: &str) -> Result<Arc<Market>> {
    ensure_public_exchange_id(exchange)?;
    let registry = market_registry()?;
    let provider_key = key(provider);
    let exchange_key = key(exchange);
    let market_type = registry
        .exchange_types
        .get(&exchange_key)
        .copied()
        .with_context(|| {
            format!(
                "exchange `{exchange}` is not present in the installed market snapshots; refresh its markets first"
            )
        })?;
    let symbol_key = symbol_key(&canonical_market_symbol(symbol, market_type)?);
    registry
        .provider_markets
        .get(&provider_key)
        .with_context(|| {
            format!(
                "market snapshot for provider `{provider}` is not installed; run `mlab markets --provider {provider} --exchange {exchange} --refresh`"
            )
        })?
        .get(&exchange_key)
        .with_context(|| {
            format!(
                "provider `{provider}` does not contain exchange `{exchange}` in the local snapshot"
            )
        })?
        .get(&symbol_key)
        .cloned()
        .with_context(|| {
            format!(
                "provider `{provider}` exchange `{exchange}` does not provide `{symbol}` in the local snapshot"
            )
        })
}

pub fn exchange_market(exchange: &str, symbol: &str) -> Result<Arc<Market>> {
    ensure_public_exchange_id(exchange)?;
    let registry = market_registry()?;
    let exchange_key = key(exchange);
    let market_type = registry
        .exchange_types
        .get(&exchange_key)
        .copied()
        .with_context(|| {
            format!(
                "exchange `{exchange}` is not present in the installed market snapshots; refresh its markets first"
            )
        })?;
    let symbol_key = symbol_key(&canonical_market_symbol(symbol, market_type)?);
    registry
        .exchange_markets
        .get(&exchange_key)
        .with_context(|| {
            format!(
                "market snapshot for standalone exchange `{exchange}` is not installed; run `mlab markets --exchange {exchange} --refresh`"
            )
        })?
        .get(&symbol_key)
        .cloned()
        .with_context(|| {
            format!(
                "standalone exchange `{exchange}` does not provide `{symbol}` in the local snapshot"
            )
        })
}

/// Resolve an exchange-native symbol received from a venue. This is separate
/// from [`exchange_market`], whose input is always a user-facing canonical
/// symbol.
pub(crate) fn exchange_wire_market(exchange: &str, symbol: &str) -> Result<Arc<Market>> {
    ensure_public_exchange_id(exchange)?;
    let registry = market_registry()?;
    registry
        .exchange_markets
        .get(&key(exchange))
        .with_context(|| {
            format!(
                "market snapshot for standalone exchange `{exchange}` is not installed; run `mlab markets --exchange {exchange} --refresh`"
            )
        })?
        .get(&symbol_key(symbol))
        .cloned()
        .with_context(|| {
            format!(
                "standalone exchange `{exchange}` returned unknown wire market `{symbol}`"
            )
        })
}

pub fn exchange_markets(exchange: &str) -> Result<Vec<Arc<Market>>> {
    ensure_public_exchange_id(exchange)?;
    let registry = market_registry()?;
    registry
        .exchange_markets
        .get(&key(exchange))
        .with_context(|| {
            format!(
                "market snapshot for standalone exchange `{exchange}` is not installed; run `mlab markets --exchange {exchange} --refresh`"
            )
        })
        .map(|markets| {
            let mut unique = markets
                .values()
                .map(Arc::clone)
                .collect::<Vec<Arc<Market>>>();
            unique.sort_by(|left, right| left.symbol.cmp(&right.symbol));
            unique.dedup_by(|left, right| left.symbol == right.symbol);
            unique
        })
}

pub fn provider_exchange(
    provider: &str,
    exchange: &str,
) -> Result<(MarketSnapshot, ExchangeMarkets)> {
    ensure_public_exchange_id(exchange)?;
    let registry = market_registry()?;
    let location = registry
        .provider_exchanges
        .get(&key(provider))
        .with_context(|| {
            format!(
                "market snapshot for provider `{provider}` is not installed; run `mlab markets --provider {provider} --exchange {exchange} --refresh`"
            )
        })?
        .get(&key(exchange))
        .with_context(|| {
            format!(
                "provider `{provider}` does not contain exchange `{exchange}` in the local snapshot"
            )
        })?;
    Ok(registry.exchange(*location))
}

pub fn upstream_exchange(provider: &str, exchange: &str) -> Result<String> {
    let (_, exchange) = provider_exchange(provider, exchange)?;
    Ok(exchange
        .provider_exchange
        .unwrap_or_else(|| exchange.exchange.clone()))
}

pub fn direct_exchange(exchange: &str) -> Result<(MarketSnapshot, ExchangeMarkets)> {
    ensure_public_exchange_id(exchange)?;
    let registry = market_registry()?;
    let location = registry
        .direct_exchanges
        .get(&key(exchange))
        .with_context(|| {
            format!(
                "market snapshot for standalone exchange `{exchange}` is not installed; run `mlab markets --exchange {exchange} --refresh`"
            )
        })?;
    Ok(registry.exchange(*location))
}

pub fn is_futures_exchange(exchange: &str) -> Result<bool> {
    ensure_public_exchange_id(exchange)?;
    if let Ok(venue) = crate::domain::execution::ExecutionVenue::parse(exchange) {
        return Ok(venue.is_perpetual());
    }
    let registry = market_registry()?;
    registry
        .exchange_types
        .get(&key(exchange))
        .copied()
        .map(MarketType::is_futures)
        .with_context(|| {
            format!(
                "exchange `{exchange}` is not present in the installed market snapshots; refresh its markets first"
            )
        })
}

pub async fn refresh_route(provider: Option<&str>, exchange: &str) -> Result<MarketSnapshot> {
    ensure_public_exchange_id(exchange)?;
    let snapshot = match provider.map(key).as_deref() {
        Some("mmt") => fetch_mmt_snapshot().await?,
        Some(provider) => bail!("market refresh is not implemented for provider `{provider}`"),
        None if exchange.eq_ignore_ascii_case("bulkf") => fetch_bulk_snapshot().await?,
        None if exchange.eq_ignore_ascii_case("hyperliquidf") => {
            fetch_hyperliquid_snapshot().await?
        }
        None if crate::domain::execution::ExecutionVenue::parse(exchange)
            .is_ok_and(|venue| venue.is_hip3()) =>
        {
            let venue = crate::domain::execution::ExecutionVenue::parse(exchange)?;
            let dex = venue.spec()?.dex.context("HIP-3 venue has no DEX name")?;
            fetch_hyperliquid_hip3_snapshot(
                venue.as_str(),
                dex.as_str(),
                &dex.as_str().to_ascii_uppercase(),
                true,
            )
            .await?
        }
        None if exchange.eq_ignore_ascii_case("hyperliquid") => {
            fetch_hyperliquid_spot_snapshot().await?
        }
        None if exchange.eq_ignore_ascii_case("binance") => fetch_binance_snapshot(false).await?,
        None if exchange.eq_ignore_ascii_case("binancef") => fetch_binance_snapshot(true).await?,
        None => bail!("market refresh is not implemented for standalone exchange `{exchange}`"),
    };
    write_snapshot(&snapshot)?;
    reload()?;
    Ok(snapshot)
}

pub async fn refresh_bulk() -> Result<MarketSnapshot> {
    refresh_route(None, "bulkf").await
}

pub async fn refresh_hyperliquid() -> Result<MarketSnapshot> {
    refresh_route(None, "hyperliquidf").await
}

pub async fn refresh_hyperliquid_spot() -> Result<MarketSnapshot> {
    refresh_route(None, "hyperliquid").await
}

pub async fn refresh_binance() -> Result<MarketSnapshot> {
    refresh_route(None, "binance").await
}

pub async fn refresh_binance_futures() -> Result<MarketSnapshot> {
    refresh_route(None, "binancef").await
}

pub async fn refresh_mmt() -> Result<MarketSnapshot> {
    refresh_route(Some("mmt"), "binancef").await
}

pub fn reload() -> Result<()> {
    let registry = Arc::new(load_registry()?);
    if let Some(shared) = REGISTRY.get() {
        *shared
            .write()
            .map_err(|_| anyhow::anyhow!("market registry lock is poisoned"))? = registry;
    } else {
        let _ = REGISTRY.set(RwLock::new(registry));
    }
    Ok(())
}

pub fn snapshot_directory() -> Result<PathBuf> {
    let home = env::var_os("HOME").context("HOME is required for the market snapshot directory")?;
    Ok(PathBuf::from(home).join(".market-lab").join("markets"))
}

fn market_registry() -> Result<Arc<MarketRegistry>> {
    if REGISTRY.get().is_none() {
        let registry = Arc::new(load_registry()?);
        let _ = REGISTRY.set(RwLock::new(registry));
    }
    REGISTRY
        .get()
        .context("failed to initialize market registry")?
        .read()
        .map_err(|_| anyhow::anyhow!("market registry lock is poisoned"))
        .map(|registry| Arc::clone(&registry))
}

#[cfg(not(test))]
fn load_registry() -> Result<MarketRegistry> {
    MarketRegistry::load(&snapshot_directory()?)
}

#[cfg(test)]
fn load_registry() -> Result<MarketRegistry> {
    MarketRegistry::new(test_snapshots())
}

async fn fetch_bulk_snapshot() -> Result<MarketSnapshot> {
    let response = Client::new()
        .get(BULK_MARKETS_URL)
        .timeout(Duration::from_secs(MARKET_HTTP_TIMEOUT_SECS))
        .send()
        .await
        .context("failed to fetch BULK markets")?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .context("failed to decode BULK markets response")?;
    if !status.is_success() {
        bail!("BULK markets returned HTTP {status} body={body}");
    }
    let raw =
        serde_json::from_value::<Vec<BulkMarket>>(body).context("invalid BULK markets response")?;
    let markets = raw
        .into_iter()
        .map(|market| {
            let base_asset = market.base_asset.to_ascii_uppercase();
            let quote_asset = market.quote_asset.to_ascii_uppercase();
            Market {
                symbol: base_asset.clone(),
                provider_symbol: market.symbol.clone(),
                venue_symbol: market.symbol,
                venue_id: None,
                aliases: Vec::new(),
                base_asset: base_asset.clone(),
                quote_asset: quote_asset.clone(),
                venue_base_asset: base_asset,
                venue_quote_asset: quote_asset,
                status: market.status,
                price_increment: Some(market.tick_size),
                size_increment: Some(market.lot_size),
                execution: Some(ExecutionRules {
                    price_precision: market.price_precision,
                    size_precision: market.size_precision,
                    tick_size: market.tick_size,
                    lot_size: market.lot_size,
                    min_notional: market.min_notional,
                    max_leverage: market.max_leverage,
                    cross_margin: true,
                    order_types: market.order_types,
                    time_in_forces: market.time_in_forces,
                }),
                network_variants: BTreeMap::new(),
            }
        })
        .collect();
    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: "bulkf".to_string(),
        provider_type: ProviderType::Standalone,
        source_url: BULK_MARKETS_URL.to_string(),
        fetched_at: fetched_at(),
        exchanges: vec![ExchangeMarkets {
            exchange: "bulkf".to_string(),
            provider_exchange: None,
            name: "BULK".to_string(),
            market_type: MarketType::Futures,
            markets,
        }],
    };
    snapshot.validate()?;
    Ok(snapshot)
}

async fn fetch_binance_snapshot(futures: bool) -> Result<MarketSnapshot> {
    let (provider, name, source_url, market_type) = if futures {
        (
            "binancef",
            "Binance USD-M Futures",
            BINANCE_FUTURES_MARKETS_URL,
            MarketType::Futures,
        )
    } else {
        (
            "binance",
            "Binance Spot",
            BINANCE_SPOT_MARKETS_URL,
            MarketType::Spot,
        )
    };
    let response = Client::new()
        .get(source_url)
        .timeout(Duration::from_secs(MARKET_HTTP_TIMEOUT_SECS))
        .send()
        .await
        .with_context(|| format!("failed to fetch {name} markets"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("failed to decode {name} markets response"))?;
    if !status.is_success() {
        bail!("{name} markets returned HTTP {status} body={body}");
    }
    let raw = serde_json::from_value::<BinanceExchangeInfo>(body)
        .with_context(|| format!("invalid {name} markets response"))?;
    let markets = raw
        .symbols
        .into_iter()
        .filter(|market| {
            market.status.eq_ignore_ascii_case("TRADING")
                && (!futures
                    || market
                        .contract_type
                        .as_deref()
                        .is_some_and(|kind| kind.eq_ignore_ascii_case("PERPETUAL"))
                        && market.quote_asset.eq_ignore_ascii_case("USDT"))
        })
        .map(|market| binance_market(market, market_type))
        .collect::<Result<Vec<_>>>()?;
    if markets.is_empty() {
        bail!("{name} returned no active markets");
    }
    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: provider.to_string(),
        provider_type: ProviderType::Standalone,
        source_url: source_url.to_string(),
        fetched_at: fetched_at(),
        exchanges: vec![ExchangeMarkets {
            exchange: provider.to_string(),
            provider_exchange: None,
            name: name.to_string(),
            market_type,
            markets,
        }],
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn binance_market(market: BinanceMarket, market_type: MarketType) -> Result<Market> {
    let price_increment = binance_filter_increment(&market.filters, "PRICE_FILTER", "tickSize")?;
    let size_increment = binance_filter_increment(&market.filters, "LOT_SIZE", "stepSize")?;
    let base_asset = market.base_asset.to_ascii_uppercase();
    let quote_asset = market.quote_asset.to_ascii_uppercase();
    Ok(Market {
        symbol: if market_type.is_futures() {
            base_asset.clone()
        } else {
            format!("{base_asset}/{quote_asset}")
        },
        provider_symbol: market.symbol.clone(),
        venue_symbol: market.symbol,
        venue_id: None,
        aliases: Vec::new(),
        base_asset: base_asset.clone(),
        quote_asset: quote_asset.clone(),
        venue_base_asset: base_asset,
        venue_quote_asset: quote_asset,
        status: market.status,
        price_increment: Some(price_increment),
        size_increment: Some(size_increment),
        execution: None,
        network_variants: BTreeMap::new(),
    })
}

fn binance_filter_increment(
    filters: &[BinanceFilter],
    filter_type: &str,
    field: &str,
) -> Result<f64> {
    let filter = filters
        .iter()
        .find(|filter| filter.filter_type == filter_type)
        .with_context(|| format!("Binance market omitted {filter_type}"))?;
    let value = match field {
        "tickSize" => filter.tick_size.as_deref(),
        "stepSize" => filter.step_size.as_deref(),
        _ => None,
    }
    .with_context(|| format!("Binance {filter_type} omitted {field}"))?
    .parse::<f64>()
    .with_context(|| format!("Binance {filter_type} returned an invalid {field}"))?;
    if !value.is_finite() || value <= 0.0 {
        bail!("Binance {filter_type} returned a non-positive {field}");
    }
    Ok(value)
}

async fn fetch_hyperliquid_snapshot() -> Result<MarketSnapshot> {
    let response = Client::new()
        .post(HYPERLIQUID_INFO_URL)
        .timeout(Duration::from_secs(MARKET_HTTP_TIMEOUT_SECS))
        .json(&serde_json::json!({ "type": "metaAndAssetCtxs" }))
        .send()
        .await
        .context("failed to fetch Hyperliquid markets")?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .context("failed to decode Hyperliquid markets response")?;
    if !status.is_success() {
        bail!("Hyperliquid markets returned HTTP {status} body={body}");
    }

    let entries = body
        .as_array()
        .context("invalid Hyperliquid metaAndAssetCtxs response")?;
    if entries.len() != 2 {
        bail!("Hyperliquid metaAndAssetCtxs must contain metadata and asset contexts");
    }
    let universe = entries[0]
        .get("universe")
        .cloned()
        .context("Hyperliquid metadata omitted universe")?;
    let raw = serde_json::from_value::<Vec<HyperliquidMarket>>(universe)
        .context("invalid Hyperliquid perpetual universe")?;
    let contexts = serde_json::from_value::<Vec<HyperliquidAssetContext>>(entries[1].clone())
        .context("invalid Hyperliquid perpetual asset contexts")?;
    if raw.len() != contexts.len() {
        bail!("Hyperliquid perpetual metadata and asset contexts are out of sync");
    }

    let markets = raw
        .into_iter()
        .zip(contexts)
        .enumerate()
        .filter(|(_, (market, _))| !market.is_delisted)
        .map(|(asset_index, (market, context))| {
            let mark_price = context
                .mark_px
                .parse::<f64>()
                .with_context(|| format!("invalid Hyperliquid mark price for {}", market.name))?;
            let tick_size = hyperliquid_price_increment(mark_price, market.sz_decimals, 6)?;
            let lot_size = 10_f64.powi(-i32::from(market.sz_decimals));
            let symbol = market.name.to_ascii_uppercase();
            Ok(Market {
                symbol,
                provider_symbol: market.name.clone(),
                venue_symbol: market.name.clone(),
                venue_id: Some(
                    u32::try_from(asset_index)
                        .context("Hyperliquid perpetual asset index exceeds u32")?,
                ),
                aliases: Vec::new(),
                base_asset: market.name.to_ascii_uppercase(),
                quote_asset: "USDC".to_string(),
                venue_base_asset: market.name.to_ascii_uppercase(),
                venue_quote_asset: "USDC".to_string(),
                status: "TRADING".to_string(),
                price_increment: Some(tick_size),
                size_increment: Some(lot_size),
                execution: Some(ExecutionRules {
                    price_precision: decimal_places(tick_size),
                    size_precision: market.sz_decimals,
                    tick_size,
                    lot_size,
                    min_notional: 10.0,
                    max_leverage: market.max_leverage,
                    cross_margin: !market.only_isolated,
                    order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
                    time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
                }),
                network_variants: BTreeMap::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if markets.is_empty() {
        bail!("Hyperliquid returned no active native perpetual markets");
    }

    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: "hyperliquidf".to_string(),
        provider_type: ProviderType::Standalone,
        source_url: HYPERLIQUID_INFO_URL.to_string(),
        fetched_at: fetched_at(),
        exchanges: vec![ExchangeMarkets {
            exchange: "hyperliquidf".to_string(),
            provider_exchange: None,
            name: "Hyperliquid Perpetuals".to_string(),
            market_type: MarketType::Futures,
            markets,
        }],
    };
    snapshot.validate()?;
    Ok(snapshot)
}

#[derive(Debug, Deserialize)]
struct HyperliquidPerpDex {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidPerpMetadata {
    universe: Vec<HyperliquidMarket>,
    collateral_token: u32,
}

#[derive(Clone)]
struct BuiltHyperliquidPerpMarket {
    symbol: String,
    quote_asset: String,
    variant: NetworkMarket,
}

async fn fetch_hyperliquid_hip3_snapshot(
    exchange: &str,
    dex: &str,
    display_name: &str,
    include_testnet: bool,
) -> Result<MarketSnapshot> {
    let (mainnet, testnet) = if include_testnet {
        let (mainnet, testnet) = tokio::join!(
            fetch_hyperliquid_hip3_network(HYPERLIQUID_INFO_URL, "mainnet", dex, display_name,),
            fetch_hyperliquid_hip3_network(
                HYPERLIQUID_TESTNET_INFO_URL,
                "testnet",
                dex,
                display_name,
            ),
        );
        let testnet = match testnet {
            Ok(markets) => markets,
            Err(error) => {
                eprintln!(
                    "warning: Hyperliquid testnet {display_name} metadata is unavailable; saved mainnet markets only: {error:#}"
                );
                BTreeMap::new()
            }
        };
        (mainnet?, testnet)
    } else {
        (
            fetch_hyperliquid_hip3_network(HYPERLIQUID_INFO_URL, "mainnet", dex, display_name)
                .await?,
            BTreeMap::new(),
        )
    };
    if mainnet.is_empty() {
        bail!("Hyperliquid returned no active mainnet {display_name} perpetual markets");
    }

    let mut markets = Vec::with_capacity(mainnet.len());
    for (_, mainnet_market) in mainnet {
        let mut network_variants =
            BTreeMap::from([("mainnet".to_string(), mainnet_market.variant.clone())]);
        if let Some(testnet_market) = testnet.get(&mainnet_market.symbol) {
            network_variants.insert("testnet".to_string(), testnet_market.variant.clone());
        }
        let variant = &mainnet_market.variant;
        markets.push(Market {
            symbol: mainnet_market.symbol.clone(),
            provider_symbol: variant.provider_symbol.clone(),
            venue_symbol: variant.venue_symbol.clone(),
            venue_id: Some(variant.venue_id),
            aliases: vec![variant.venue_symbol.clone()],
            base_asset: mainnet_market.symbol,
            quote_asset: mainnet_market.quote_asset,
            venue_base_asset: variant.venue_base_asset.clone(),
            venue_quote_asset: variant.venue_quote_asset.clone(),
            status: "TRADING".to_string(),
            price_increment: Some(variant.price_increment),
            size_increment: Some(variant.size_increment),
            execution: Some(variant.execution.clone()),
            network_variants,
        });
    }

    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: exchange.to_string(),
        provider_type: ProviderType::Standalone,
        source_url: HYPERLIQUID_INFO_URL.to_string(),
        fetched_at: fetched_at(),
        exchanges: vec![ExchangeMarkets {
            exchange: exchange.to_string(),
            provider_exchange: None,
            name: format!("Hyperliquid {display_name} Perpetuals"),
            market_type: MarketType::Futures,
            markets,
        }],
    };
    snapshot.validate()?;
    Ok(snapshot)
}

async fn fetch_hyperliquid_hip3_network(
    url: &str,
    network: &str,
    dex: &str,
    display_name: &str,
) -> Result<BTreeMap<String, BuiltHyperliquidPerpMarket>> {
    let client = Client::new();
    let dexes = fetch_hyperliquid_info(
        &client,
        url,
        serde_json::json!({ "type": "perpDexs" }),
        &format!("Hyperliquid {network} perpetual DEX list"),
    )
    .await?;
    let dexes = serde_json::from_value::<Vec<Option<HyperliquidPerpDex>>>(dexes)
        .with_context(|| format!("invalid Hyperliquid {network} perpetual DEX list"))?;
    let dex_index = dexes
        .iter()
        .position(|entry| {
            entry
                .as_ref()
                .is_some_and(|entry| entry.name.eq_ignore_ascii_case(dex))
        })
        .with_context(|| {
            format!("Hyperliquid {network} does not expose the {display_name} perpetual DEX")
        })?;
    let metadata = fetch_hyperliquid_info(
        &client,
        url,
        serde_json::json!({ "type": "metaAndAssetCtxs", "dex": dex }),
        &format!("Hyperliquid {network} {display_name} markets"),
    )
    .await?;
    let spot_meta = fetch_hyperliquid_info(
        &client,
        url,
        serde_json::json!({ "type": "spotMeta" }),
        &format!("Hyperliquid {network} spot metadata"),
    )
    .await?;
    decode_hyperliquid_hip3_network(metadata, spot_meta, dex_index, network, dex, display_name)
}

async fn fetch_hyperliquid_info(
    client: &Client,
    url: &str,
    request: Value,
    description: &str,
) -> Result<Value> {
    let response = client
        .post(url)
        .timeout(Duration::from_secs(MARKET_HTTP_TIMEOUT_SECS))
        .json(&request)
        .send()
        .await
        .with_context(|| format!("failed to fetch {description}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("failed to read {description} response"))?;
    if !status.is_success() {
        bail!("{description} returned HTTP {status} body={}", body.trim());
    }
    serde_json::from_str(&body).with_context(|| format!("failed to decode {description} response"))
}

#[cfg(test)]
fn decode_hyperliquid_xyz_network(
    body: Value,
    spot_meta: Value,
    dex_index: usize,
    network: &str,
) -> Result<BTreeMap<String, BuiltHyperliquidPerpMarket>> {
    decode_hyperliquid_hip3_network(body, spot_meta, dex_index, network, "xyz", "XYZ")
}

fn decode_hyperliquid_hip3_network(
    body: Value,
    spot_meta: Value,
    dex_index: usize,
    network: &str,
    dex: &str,
    display_name: &str,
) -> Result<BTreeMap<String, BuiltHyperliquidPerpMarket>> {
    let entries = body.as_array().with_context(|| {
        format!("invalid Hyperliquid {network} {display_name} metaAndAssetCtxs response")
    })?;
    if entries.len() != 2 {
        bail!(
            "Hyperliquid {network} {display_name} metadata must contain metadata and asset contexts"
        );
    }
    let metadata = serde_json::from_value::<HyperliquidPerpMetadata>(entries[0].clone())
        .with_context(|| format!("invalid Hyperliquid {network} {display_name} metadata"))?;
    let contexts = serde_json::from_value::<Vec<HyperliquidAssetContext>>(entries[1].clone())
        .with_context(|| format!("invalid Hyperliquid {network} {display_name} asset contexts"))?;
    if metadata.universe.len() != contexts.len() {
        bail!("Hyperliquid {network} {display_name} metadata and asset contexts are out of sync");
    }
    let spot = serde_json::from_value::<HyperliquidSpotMetadata>(spot_meta)
        .with_context(|| format!("invalid Hyperliquid {network} spot metadata"))?;
    let collateral = spot
        .tokens
        .iter()
        .find(|token| token.index == metadata.collateral_token)
        .with_context(|| {
            format!(
                "Hyperliquid {network} {display_name} references missing collateral token {}",
                metadata.collateral_token
            )
        })?;
    let quote_asset = collateral.name.to_ascii_uppercase();
    let dex_index = u32::try_from(dex_index)
        .with_context(|| format!("Hyperliquid {display_name} DEX index exceeds u32"))?;
    let base_asset_id = 100_000_u32
        .checked_add(dex_index.checked_mul(10_000).with_context(|| {
            format!("Hyperliquid {display_name} DEX index exceeds the asset ID range")
        })?)
        .with_context(|| format!("Hyperliquid {display_name} asset ID base overflowed"))?;
    let mut markets = BTreeMap::new();
    for (asset_index, (market, context)) in metadata.universe.into_iter().zip(contexts).enumerate()
    {
        if market.is_delisted {
            continue;
        }
        let wire_symbol = market.name;
        let prefix = format!("{dex}:");
        let symbol = wire_symbol
            .strip_prefix(&prefix)
            .with_context(|| {
                format!(
                    "Hyperliquid {network} {display_name} market `{wire_symbol}` omitted the {prefix} prefix"
                )
            })?
            .to_ascii_uppercase();
        let mark_price = context.mark_px.parse::<f64>().with_context(|| {
            format!("invalid Hyperliquid {network} {display_name} mark price for {wire_symbol}")
        })?;
        let tick_size = hyperliquid_price_increment(mark_price, market.sz_decimals, 6)?;
        let lot_size = 10_f64.powi(-i32::from(market.sz_decimals));
        let asset_index = u32::try_from(asset_index)
            .with_context(|| format!("Hyperliquid {display_name} market index exceeds u32"))?;
        let venue_id = base_asset_id
            .checked_add(asset_index)
            .with_context(|| format!("Hyperliquid {display_name} asset ID overflowed"))?;
        let cross_margin = !market.only_isolated
            && !market.margin_mode.as_deref().is_some_and(|mode| {
                mode.eq_ignore_ascii_case("noCross") || mode.eq_ignore_ascii_case("strictIsolated")
            });
        let rules = ExecutionRules {
            price_precision: decimal_places(tick_size),
            size_precision: market.sz_decimals,
            tick_size,
            lot_size,
            min_notional: 10.0,
            max_leverage: market.max_leverage,
            cross_margin,
            order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
            time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
        };
        let built = BuiltHyperliquidPerpMarket {
            symbol: symbol.clone(),
            quote_asset: quote_asset.clone(),
            variant: NetworkMarket {
                provider_symbol: wire_symbol.clone(),
                venue_symbol: wire_symbol,
                venue_id,
                venue_base_asset: symbol.clone(),
                venue_quote_asset: quote_asset.clone(),
                base_token_id: None,
                quote_token_id: Some(collateral.token_id.to_ascii_lowercase()),
                base_token_index: None,
                quote_token_index: Some(collateral.index),
                price_increment: tick_size,
                size_increment: lot_size,
                execution: rules,
            },
        };
        if markets.insert(symbol.clone(), built).is_some() {
            bail!("Hyperliquid {network} {display_name} returned duplicate market {symbol}");
        }
    }
    Ok(markets)
}

#[derive(Clone)]
struct BuiltHyperliquidSpotMarket {
    symbol: String,
    aliases: Vec<String>,
    base_asset: String,
    quote_asset: String,
    variant: NetworkMarket,
}

async fn fetch_hyperliquid_spot_snapshot() -> Result<MarketSnapshot> {
    let (mainnet, testnet) = tokio::join!(
        fetch_hyperliquid_spot_network(HYPERLIQUID_INFO_URL, "mainnet"),
        fetch_hyperliquid_spot_network(HYPERLIQUID_TESTNET_INFO_URL, "testnet"),
    );
    let mainnet = mainnet?;
    let testnet = match testnet {
        Ok(markets) => markets,
        Err(error) => {
            eprintln!(
                "warning: Hyperliquid testnet spot metadata is unavailable; \
                 saved mainnet markets only: {error:#}"
            );
            BTreeMap::new()
        }
    };
    if mainnet.is_empty() {
        bail!("Hyperliquid returned no active mainnet spot markets");
    }

    let mut markets = Vec::with_capacity(mainnet.len());
    for (_, mainnet_market) in mainnet {
        let mut network_variants =
            BTreeMap::from([("mainnet".to_string(), mainnet_market.variant.clone())]);
        if let Some(testnet_market) = testnet.get(&mainnet_market.symbol) {
            network_variants.insert("testnet".to_string(), testnet_market.variant.clone());
        }
        let mainnet_variant = &mainnet_market.variant;
        markets.push(Market {
            symbol: mainnet_market.symbol,
            provider_symbol: mainnet_variant.provider_symbol.clone(),
            venue_symbol: mainnet_variant.venue_symbol.clone(),
            venue_id: Some(mainnet_variant.venue_id),
            aliases: mainnet_market.aliases,
            base_asset: mainnet_market.base_asset,
            quote_asset: mainnet_market.quote_asset,
            venue_base_asset: mainnet_variant.venue_base_asset.clone(),
            venue_quote_asset: mainnet_variant.venue_quote_asset.clone(),
            status: "TRADING".to_string(),
            price_increment: Some(mainnet_variant.price_increment),
            size_increment: Some(mainnet_variant.size_increment),
            execution: Some(mainnet_variant.execution.clone()),
            network_variants,
        });
    }

    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: "hyperliquid".to_string(),
        provider_type: ProviderType::Standalone,
        source_url: HYPERLIQUID_INFO_URL.to_string(),
        fetched_at: fetched_at(),
        exchanges: vec![ExchangeMarkets {
            exchange: "hyperliquid".to_string(),
            provider_exchange: None,
            name: "Hyperliquid Spot".to_string(),
            market_type: MarketType::Spot,
            markets,
        }],
    };
    snapshot.validate()?;
    Ok(snapshot)
}

async fn fetch_hyperliquid_spot_network(
    url: &str,
    network: &str,
) -> Result<BTreeMap<String, BuiltHyperliquidSpotMarket>> {
    let response = Client::new()
        .post(url)
        .timeout(Duration::from_secs(MARKET_HTTP_TIMEOUT_SECS))
        .json(&serde_json::json!({ "type": "spotMetaAndAssetCtxs" }))
        .send()
        .await
        .with_context(|| format!("failed to fetch Hyperliquid {network} spot markets"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("failed to read Hyperliquid {network} spot markets response"))?;
    if !status.is_success() {
        bail!(
            "Hyperliquid {network} spot markets returned HTTP {status} body={}",
            body.trim()
        );
    }
    let body = serde_json::from_str::<Value>(&body)
        .with_context(|| format!("failed to decode Hyperliquid {network} spot markets response"))?;
    decode_hyperliquid_spot_network(body, network)
}

fn decode_hyperliquid_spot_network(
    body: Value,
    network: &str,
) -> Result<BTreeMap<String, BuiltHyperliquidSpotMarket>> {
    let entries = body
        .as_array()
        .with_context(|| format!("invalid Hyperliquid {network} spotMetaAndAssetCtxs response"))?;
    if entries.len() != 2 {
        bail!(
            "Hyperliquid {network} spotMetaAndAssetCtxs must contain metadata and asset contexts"
        );
    }
    let metadata = serde_json::from_value::<HyperliquidSpotMetadata>(entries[0].clone())
        .with_context(|| format!("invalid Hyperliquid {network} spot metadata"))?;
    let contexts = serde_json::from_value::<Vec<HyperliquidSpotContext>>(entries[1].clone())
        .with_context(|| format!("invalid Hyperliquid {network} spot asset contexts"))?;
    let mut contexts_by_coin = HashMap::with_capacity(contexts.len());
    for context in contexts {
        let coin = context.coin.clone();
        if contexts_by_coin.insert(coin.clone(), context).is_some() {
            bail!("Hyperliquid {network} returned duplicate spot context for {coin}");
        }
    }
    let tokens = metadata
        .tokens
        .into_iter()
        .map(|token| (token.index, token))
        .collect::<HashMap<_, _>>();
    let venue_token_names = tokens
        .values()
        .map(|token| token.name.to_ascii_uppercase())
        .collect::<HashSet<_>>();
    let mut markets = BTreeMap::new();
    for pair in metadata.universe {
        let context = contexts_by_coin.remove(&pair.name).with_context(|| {
            format!(
                "Hyperliquid {network} spot metadata omitted the asset context for {}",
                pair.name
            )
        })?;
        let base = tokens.get(&pair.tokens[0]).with_context(|| {
            format!(
                "Hyperliquid {network} spot pair {} references missing base token {}",
                pair.name, pair.tokens[0]
            )
        })?;
        let quote = tokens.get(&pair.tokens[1]).with_context(|| {
            format!(
                "Hyperliquid {network} spot pair {} references missing quote token {}",
                pair.name, pair.tokens[1]
            )
        })?;
        let mark_price = context.mark_px.parse::<f64>().with_context(|| {
            format!(
                "invalid Hyperliquid {network} spot mark price for {}",
                pair.name
            )
        })?;
        let normalized_base = canonical_hyperliquid_spot_token(network, base);
        let venue_base = base.name.to_ascii_uppercase();
        let base_asset =
            if normalized_base != venue_base && venue_token_names.contains(&normalized_base) {
                venue_base
            } else {
                normalized_base
            };
        let quote_asset = canonical_hyperliquid_spot_token(network, quote);
        let symbol = format!("{base_asset}/{quote_asset}");
        let lot_size = 10_f64.powi(-i32::from(base.sz_decimals));
        let tick_size = hyperliquid_price_increment(mark_price, base.sz_decimals, 8)?;
        let venue_id = 10_000_u32
            .checked_add(pair.index)
            .context("Hyperliquid spot pair index exceeds the order asset range")?;
        let rules = ExecutionRules {
            price_precision: decimal_places(tick_size),
            size_precision: base.sz_decimals,
            tick_size,
            lot_size,
            min_notional: 10.0,
            max_leverage: 1,
            cross_margin: false,
            order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
            time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
        };
        let mut aliases = vec![
            format!(
                "{}/{}",
                base.name.to_ascii_uppercase(),
                quote.name.to_ascii_uppercase()
            ),
            format!("{base_asset}/{}", quote.name.to_ascii_uppercase()),
            pair.name.clone(),
        ];
        aliases.sort();
        aliases.dedup();
        let built = BuiltHyperliquidSpotMarket {
            symbol: symbol.clone(),
            aliases,
            base_asset,
            quote_asset,
            variant: NetworkMarket {
                provider_symbol: pair.name.clone(),
                venue_symbol: pair.name,
                venue_id,
                venue_base_asset: base.name.to_ascii_uppercase(),
                venue_quote_asset: quote.name.to_ascii_uppercase(),
                base_token_id: Some(base.token_id.to_ascii_lowercase()),
                quote_token_id: Some(quote.token_id.to_ascii_lowercase()),
                base_token_index: Some(base.index),
                quote_token_index: Some(quote.index),
                price_increment: tick_size,
                size_increment: lot_size,
                execution: rules,
            },
        };
        if markets.insert(symbol.clone(), built).is_some() {
            bail!(
                "Hyperliquid {network} exposes multiple spot markets normalized as {symbol}; token-id disambiguation is required"
            );
        }
    }
    Ok(markets)
}

fn canonical_hyperliquid_spot_token(network: &str, token: &HyperliquidSpotToken) -> String {
    let token_id = token.token_id.to_ascii_lowercase();
    let trusted = match (network, token_id.as_str()) {
        ("mainnet", "0x8f254b963e8468305d409b33aa137c67") => Some("BTC"),
        ("mainnet", "0xe1edd30daaf5caac3fe63569e24748da") => Some("ETH"),
        ("mainnet", "0x49b67c39f5566535de22b29b0e51e685") => Some("SOL"),
        ("mainnet", "0x544e60f98a36d7b22c0fb5824b84f795") => Some("PUMP"),
        ("mainnet", "0x7650808198966e4285687d3deb556ccc") => Some("FARTCOIN"),
        ("mainnet", "0xb113d34e351cf195733c98442530c099") => Some("BONK"),
        ("mainnet", "0x2c54c60600e1d786b2dfc139a38a5a99") => Some("XPL"),
        ("mainnet", "0x1c994ad3381d31c86c8c2d74ed89a365") => Some("ZEC"),
        ("mainnet", "0x730fc3855fb77d2aa5a19dd7891dbe80") => Some("AVAX"),
        ("mainnet", "0x85b8124314ae77b78b4b6f20ecd93149") => Some("VIRTUAL"),
        ("mainnet", "0xa7e941cbc468d48b99dc6002f8f5042b") => Some("ANSEM"),
        ("testnet", "0x5314ecc85ee6059955409e0da8d2bd31") => Some("BTC"),
        ("testnet", "0xe4371d8166f362d6578725f11e0a14f3") => Some("ETH"),
        ("testnet", "0x57ead23624b114018cc0e49d01cc7b6b") => Some("SOL"),
        ("testnet", "0xdc348378290f167692e50bfb49c60696") => Some("PUMP"),
        ("testnet", "0x5c1a98b4df03401e19acb16bcf2ffabf") => Some("FARTCOIN"),
        ("testnet", "0x2f5b5d85f4f86f683f681d2fa791adab") => Some("SPX"),
        ("testnet", "0x07258d30c89f37d852314bbdd90ac0ff") => Some("BONK"),
        ("testnet", "0x9d63f24c61da7bd3c67ff78ed1799756") => Some("XPL"),
        ("testnet", "0xc8e8047efa1400eb0b7b9bbee16b759d") => Some("ZEC"),
        ("testnet", "0xcc5da3a373f1e28955ab309b99293e58") => Some("AVAX"),
        ("testnet", "0x0740d272a61e2ee49107c96562b23934") => Some("VIRTUAL"),
        _ => None,
    };
    trusted.map_or_else(|| token.name.to_ascii_uppercase(), ToString::to_string)
}

fn hyperliquid_price_increment(
    mark_price: f64,
    size_decimals: u8,
    max_decimals: u8,
) -> Result<f64> {
    if !mark_price.is_finite()
        || mark_price <= 0.0
        || size_decimals > max_decimals
        || max_decimals > 8
    {
        bail!("invalid Hyperliquid price or size precision");
    }
    let decimal_tick = 10_f64.powi(-(i32::from(max_decimals) - i32::from(size_decimals)));
    let significant_tick = 10_f64.powi(mark_price.log10().floor() as i32 - 4);
    Ok(decimal_tick.max(significant_tick))
}

fn decimal_places(value: f64) -> u8 {
    let mut value = value;
    for decimals in 0..=8 {
        if (value.round() - value).abs() <= 1e-9 {
            return decimals;
        }
        value *= 10.0;
    }
    8
}

const fn default_true() -> bool {
    true
}

async fn fetch_mmt_snapshot() -> Result<MarketSnapshot> {
    let response = Client::new()
        .get(MMT_MARKETS_URL)
        .timeout(Duration::from_secs(MARKET_HTTP_TIMEOUT_SECS))
        .header("X-API-Key", mmt_api_key()?)
        .send()
        .await
        .context("failed to fetch MMT markets")?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .context("failed to decode MMT markets response")?;
    if !status.is_success() {
        bail!("MMT markets returned HTTP {status} body={body}");
    }
    let raw = serde_json::from_value::<MmtMarketsResponse>(body)
        .context("invalid MMT markets response")?;
    let exchanges = raw
        .exchanges
        .into_iter()
        .map(|exchange| {
            let mut markets = BTreeMap::new();
            for market in exchange.symbols {
                let base_asset = market.normalised_base.to_ascii_uppercase();
                let market_type = classify_mmt_exchange(&canonical_mmt_exchange(&exchange.id));
                let quote_asset = market.quote.to_ascii_uppercase();
                let symbol = if market_type.is_futures() {
                    base_asset.clone()
                } else {
                    format!("{base_asset}/{quote_asset}")
                };
                markets.entry(symbol.clone()).or_insert_with(|| Market {
                    symbol,
                    provider_symbol: market.symbol,
                    venue_symbol: market.exchange_ticker,
                    venue_id: None,
                    aliases: Vec::new(),
                    base_asset,
                    quote_asset,
                    venue_base_asset: market.base.to_ascii_uppercase(),
                    venue_quote_asset: market.quote.to_ascii_uppercase(),
                    status: "AVAILABLE".to_string(),
                    price_increment: Some(market.tick_size),
                    size_increment: Some(market.step_size),
                    execution: None,
                    network_variants: BTreeMap::new(),
                });
            }
            let provider_exchange = exchange.id;
            let canonical_exchange = canonical_mmt_exchange(&provider_exchange);
            ExchangeMarkets {
                market_type: classify_mmt_exchange(&canonical_exchange),
                exchange: canonical_exchange,
                provider_exchange: Some(provider_exchange),
                name: exchange.name,
                markets: markets.into_values().collect(),
            }
        })
        .collect();
    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: "mmt".to_string(),
        provider_type: ProviderType::Aggregator,
        source_url: MMT_MARKETS_URL.to_string(),
        fetched_at: fetched_at(),
        exchanges,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn write_snapshot(snapshot: &MarketSnapshot) -> Result<()> {
    snapshot.validate()?;
    let directory = snapshot_directory()?;
    fs::create_dir_all(&directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;
    secure_directory(&directory)?;

    let destination = directory.join(format!("{}-markets.json", key(&snapshot.provider)));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let staging = directory.join(format!(
        ".{}-markets.json.new-{}-{nonce}",
        key(&snapshot.provider),
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging)
            .with_context(|| format!("failed to create {}", staging.display()))?;
        let mut bytes = serde_json::to_vec_pretty(snapshot)?;
        bytes.push(b'\n');
        output.write_all(&bytes)?;
        output.sync_all()?;
        secure_file(&staging)?;
        fs::rename(&staging, &destination).with_context(|| {
            format!(
                "failed to replace market snapshot {}",
                destination.display()
            )
        })?;
        let legacy = if snapshot.provider.eq_ignore_ascii_case("bulkf") {
            Some(directory.join("bulk-markets.json"))
        } else {
            None
        };
        if let Some(legacy) = legacy
            && legacy != destination
            && legacy.exists()
        {
            fs::remove_file(&legacy)
                .with_context(|| format!("failed to remove {}", legacy.display()))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

#[cfg(unix)]
fn secure_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", path.display()))
}

#[cfg(not(unix))]
fn secure_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn secure_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", path.display()))
}

#[cfg(not(unix))]
fn secure_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn insert_market_aliases(
    index: &mut HashMap<String, Arc<Market>>,
    market: Arc<Market>,
    provider: &str,
    exchange: &str,
) -> Result<()> {
    for symbol in market.lookup_symbols() {
        let lookup = symbol_key(symbol);
        if let Some(existing) = index.insert(lookup.clone(), Arc::clone(&market))
            && !Arc::ptr_eq(&existing, &market)
        {
            bail!("{provider}/{exchange} market lookup `{lookup}` resolves to multiple markets");
        }
    }
    Ok(())
}

fn validate_optional_increment(
    value: Option<f64>,
    kind: &str,
    provider: &str,
    exchange: &str,
    symbol: &str,
) -> Result<()> {
    if value.is_some_and(|value| !value.is_finite() || value <= 0.0) {
        bail!("{provider}/{exchange} market {symbol} has invalid {kind} increment");
    }
    Ok(())
}

pub fn canonical_market_symbol(symbol: &str, market_type: MarketType) -> Result<String> {
    let normalized = symbol.trim().to_ascii_uppercase();
    if market_type.is_futures() {
        if normalized.is_empty()
            || normalized.contains(['/', '-'])
            || !normalized
                .chars()
                .all(|character| character.is_alphanumeric() || character == '_')
        {
            bail!("futures symbol must be a base asset, e.g. BTC");
        }
        return Ok(normalized);
    }

    let mut parts = normalized.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(base), Some(quote), None)
            if !base.is_empty()
                && !quote.is_empty()
                && base.chars().all(|character| {
                    character.is_alphanumeric() || character == '-' || character == '_'
                })
                && quote.chars().all(|character| {
                    character.is_alphanumeric() || character == '-' || character == '_'
                }) =>
        {
            Ok(format!("{base}/{quote}"))
        }
        _ => bail!("spot symbol must look like BASE/QUOTE, e.g. HYPE/USDC"),
    }
}

fn fetched_at() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn symbol_key(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn ensure_public_exchange_id(exchange: &str) -> Result<()> {
    if key(exchange) == "bulk" {
        bail!("exchange `bulk` is not available; use `bulkf` for BULK perpetuals");
    }
    Ok(())
}

fn classify_mmt_exchange(exchange: &str) -> MarketType {
    classify_exchange_name(exchange)
}

fn classify_exchange_name(exchange: &str) -> MarketType {
    let exchange = key(exchange);
    let family = exchange.split('-').next().unwrap_or(exchange.as_str());
    if family.ends_with('f') {
        MarketType::Futures
    } else {
        MarketType::Spot
    }
}

fn canonical_mmt_exchange(exchange: &str) -> String {
    let exchange = key(exchange);
    match exchange.strip_prefix("hyperliquid-") {
        Some(suffix) => format!("hyperliquidf-{suffix}"),
        None if exchange == "hyperliquid" => "hyperliquidf".to_string(),
        None => exchange,
    }
}

fn canonicalize_snapshot(snapshot: &mut MarketSnapshot) {
    if snapshot.provider_type == ProviderType::Aggregator
        && snapshot.provider.eq_ignore_ascii_case("mmt")
    {
        for exchange in &mut snapshot.exchanges {
            let upstream = exchange
                .provider_exchange
                .clone()
                .unwrap_or_else(|| exchange.exchange.clone());
            exchange.provider_exchange = Some(upstream.clone());
            exchange.exchange = canonical_mmt_exchange(&upstream);
            exchange.market_type = classify_exchange_name(&exchange.exchange);
        }
    }

    if snapshot.provider_type == ProviderType::Standalone
        && snapshot.provider.eq_ignore_ascii_case("hyperliquid")
        && snapshot
            .exchanges
            .iter()
            .all(|exchange| exchange.market_type == MarketType::Futures)
    {
        snapshot.provider = "hyperliquidf".to_string();
        for exchange in &mut snapshot.exchanges {
            if exchange.exchange.eq_ignore_ascii_case("hyperliquid") {
                exchange.exchange = "hyperliquidf".to_string();
                exchange.name = "Hyperliquid Perpetuals".to_string();
            }
        }
    }

    for exchange in &mut snapshot.exchanges {
        for market in &mut exchange.markets {
            market.base_asset = market.base_asset.trim().to_ascii_uppercase();
            market.quote_asset = market.quote_asset.trim().to_ascii_uppercase();
            if exchange.market_type.is_futures() {
                market.quote_asset = market.venue_quote_asset.trim().to_ascii_uppercase();
                market.symbol = market.base_asset.clone();
                market.aliases.retain(|alias| !alias.contains('/'));
            } else if snapshot.provider.eq_ignore_ascii_case("mmt") {
                market.quote_asset = market.venue_quote_asset.trim().to_ascii_uppercase();
                market.symbol = format!("{}/{}", market.base_asset, market.quote_asset);
            } else if exchange.exchange.eq_ignore_ascii_case("hyperliquid") {
                market.quote_asset = market.venue_quote_asset.trim().to_ascii_uppercase();
                market.symbol = format!("{}/{}", market.base_asset, market.quote_asset);
                market.aliases.retain(|alias| {
                    !alias.eq_ignore_ascii_case(&format!("{}/USDT", market.base_asset))
                        || market.quote_asset == "USDT"
                });
            }
        }
    }
}

#[cfg(test)]
fn test_snapshots() -> Vec<MarketSnapshot> {
    let bulk_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "BTC-USD".to_string(),
        venue_symbol: "BTC-USD".to_string(),
        venue_id: None,
        aliases: vec!["BTC/USD".to_string()],
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "BTC".to_string(),
        venue_quote_asset: "USD".to_string(),
        status: "TRADING".to_string(),
        price_increment: Some(0.001),
        size_increment: Some(0.000001),
        execution: Some(ExecutionRules {
            price_precision: 3,
            size_precision: 6,
            tick_size: 0.001,
            lot_size: 0.000001,
            min_notional: 1.0,
            max_leverage: 40,
            cross_margin: true,
            order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
            time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
        }),
        network_variants: BTreeMap::new(),
    };
    let mmt_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "btc/usd".to_string(),
        venue_symbol: "btc/usd".to_string(),
        venue_id: None,
        aliases: vec!["BTC/USDT".to_string()],
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "BTC".to_string(),
        venue_quote_asset: "USDT".to_string(),
        status: "AVAILABLE".to_string(),
        price_increment: Some(0.1),
        size_increment: Some(0.001),
        execution: None,
        network_variants: BTreeMap::new(),
    };
    let mmt_aave_market = Market {
        symbol: "AAVE/USDT".to_string(),
        provider_symbol: "aave/usd".to_string(),
        venue_symbol: "aave/usd".to_string(),
        venue_id: None,
        aliases: vec!["AAVE/USDT".to_string()],
        base_asset: "AAVE".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "AAVE".to_string(),
        venue_quote_asset: "USDT".to_string(),
        status: "AVAILABLE".to_string(),
        price_increment: Some(0.01),
        size_increment: Some(0.001),
        execution: None,
        network_variants: BTreeMap::new(),
    };
    let hyperliquid_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "BTC".to_string(),
        venue_symbol: "BTC".to_string(),
        venue_id: Some(0),
        aliases: vec!["BTC/USD".to_string(), "BTC/USDC".to_string()],
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "BTC".to_string(),
        venue_quote_asset: "USDC".to_string(),
        status: "TRADING".to_string(),
        price_increment: Some(1.0),
        size_increment: Some(0.00001),
        execution: Some(ExecutionRules {
            price_precision: 0,
            size_precision: 5,
            tick_size: 1.0,
            lot_size: 0.00001,
            min_notional: 10.0,
            max_leverage: 40,
            cross_margin: true,
            order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
            time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
        }),
        network_variants: BTreeMap::new(),
    };
    let hyperliquid_spot_rules = ExecutionRules {
        price_precision: 0,
        size_precision: 5,
        tick_size: 1.0,
        lot_size: 0.00001,
        min_notional: 10.0,
        max_leverage: 1,
        cross_margin: false,
        order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
        time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
    };
    let hyperliquid_spot_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "@142".to_string(),
        venue_symbol: "@142".to_string(),
        venue_id: Some(10_142),
        aliases: vec![
            "BTC/USDC".to_string(),
            "UBTC/USDC".to_string(),
            "@142".to_string(),
        ],
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "UBTC".to_string(),
        venue_quote_asset: "USDC".to_string(),
        status: "TRADING".to_string(),
        price_increment: Some(1.0),
        size_increment: Some(0.00001),
        execution: Some(hyperliquid_spot_rules.clone()),
        network_variants: BTreeMap::from([
            (
                "mainnet".to_string(),
                NetworkMarket {
                    provider_symbol: "@142".to_string(),
                    venue_symbol: "@142".to_string(),
                    venue_id: 10_142,
                    venue_base_asset: "UBTC".to_string(),
                    venue_quote_asset: "USDC".to_string(),
                    base_token_id: Some("0x8f254b963e8468305d409b33aa137c67".to_string()),
                    quote_token_id: Some("0x0".to_string()),
                    base_token_index: Some(197),
                    quote_token_index: Some(0),
                    price_increment: 1.0,
                    size_increment: 0.00001,
                    execution: hyperliquid_spot_rules.clone(),
                },
            ),
            (
                "testnet".to_string(),
                NetworkMarket {
                    provider_symbol: "@10".to_string(),
                    venue_symbol: "@10".to_string(),
                    venue_id: 10_010,
                    venue_base_asset: "UBTC".to_string(),
                    venue_quote_asset: "USDC".to_string(),
                    base_token_id: Some("0x5314ecc85ee6059955409e0da8d2bd31".to_string()),
                    quote_token_id: Some("0x0".to_string()),
                    base_token_index: Some(12),
                    quote_token_index: Some(0),
                    price_increment: 1.0,
                    size_increment: 0.00001,
                    execution: hyperliquid_spot_rules,
                },
            ),
        ]),
    };
    let hyperliquid_xyz_rules = ExecutionRules {
        price_precision: 2,
        size_precision: 3,
        tick_size: 0.01,
        lot_size: 0.001,
        min_notional: 10.0,
        max_leverage: 10,
        cross_margin: true,
        order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
        time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
    };
    let hyperliquid_xyz_market = Market {
        symbol: "TSLA".to_string(),
        provider_symbol: "xyz:TSLA".to_string(),
        venue_symbol: "xyz:TSLA".to_string(),
        venue_id: Some(110_001),
        aliases: vec!["xyz:TSLA".to_string()],
        base_asset: "TSLA".to_string(),
        quote_asset: "USDC".to_string(),
        venue_base_asset: "TSLA".to_string(),
        venue_quote_asset: "USDC".to_string(),
        status: "TRADING".to_string(),
        price_increment: Some(0.01),
        size_increment: Some(0.001),
        execution: Some(hyperliquid_xyz_rules.clone()),
        network_variants: BTreeMap::from([
            (
                "mainnet".to_string(),
                NetworkMarket {
                    provider_symbol: "xyz:TSLA".to_string(),
                    venue_symbol: "xyz:TSLA".to_string(),
                    venue_id: 110_001,
                    venue_base_asset: "TSLA".to_string(),
                    venue_quote_asset: "USDC".to_string(),
                    base_token_id: None,
                    quote_token_id: Some("0x0".to_string()),
                    base_token_index: None,
                    quote_token_index: Some(0),
                    price_increment: 0.01,
                    size_increment: 0.001,
                    execution: hyperliquid_xyz_rules.clone(),
                },
            ),
            (
                "testnet".to_string(),
                NetworkMarket {
                    provider_symbol: "xyz:TSLA".to_string(),
                    venue_symbol: "xyz:TSLA".to_string(),
                    venue_id: 750_001,
                    venue_base_asset: "TSLA".to_string(),
                    venue_quote_asset: "USDC".to_string(),
                    base_token_id: None,
                    quote_token_id: Some("0x0".to_string()),
                    base_token_index: None,
                    quote_token_index: Some(0),
                    price_increment: 0.01,
                    size_increment: 0.001,
                    execution: hyperliquid_xyz_rules,
                },
            ),
        ]),
    };
    let binance_spot_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "BTCUSDT".to_string(),
        venue_symbol: "BTCUSDT".to_string(),
        venue_id: None,
        aliases: Vec::new(),
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "BTC".to_string(),
        venue_quote_asset: "USDT".to_string(),
        status: "TRADING".to_string(),
        price_increment: Some(0.01),
        size_increment: Some(0.00001),
        execution: None,
        network_variants: BTreeMap::new(),
    };
    let binance_futures_market = binance_spot_market.clone();
    vec![
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "bulkf".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: BULK_MARKETS_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "bulkf".to_string(),
                provider_exchange: None,
                name: "BULK".to_string(),
                market_type: MarketType::Futures,
                markets: vec![bulk_market],
            }],
        },
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "hyperliquid".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: HYPERLIQUID_INFO_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "hyperliquid".to_string(),
                provider_exchange: None,
                name: "Hyperliquid".to_string(),
                market_type: MarketType::Futures,
                markets: vec![hyperliquid_market],
            }],
        },
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "hyperliquid".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: HYPERLIQUID_INFO_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "hyperliquid".to_string(),
                provider_exchange: None,
                name: "Hyperliquid Spot".to_string(),
                market_type: MarketType::Spot,
                markets: vec![hyperliquid_spot_market],
            }],
        },
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "hyperliquidf-xyz".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: HYPERLIQUID_INFO_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "hyperliquidf-xyz".to_string(),
                provider_exchange: None,
                name: "Hyperliquid XYZ Perpetuals".to_string(),
                market_type: MarketType::Futures,
                markets: vec![hyperliquid_xyz_market],
            }],
        },
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "binance".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: BINANCE_SPOT_MARKETS_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "binance".to_string(),
                provider_exchange: None,
                name: "Binance Spot".to_string(),
                market_type: MarketType::Spot,
                markets: vec![binance_spot_market],
            }],
        },
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "binancef".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: BINANCE_FUTURES_MARKETS_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "binancef".to_string(),
                provider_exchange: None,
                name: "Binance USD-M Futures".to_string(),
                market_type: MarketType::Futures,
                markets: vec![binance_futures_market],
            }],
        },
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "mmt".to_string(),
            provider_type: ProviderType::Aggregator,
            source_url: MMT_MARKETS_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![
                ExchangeMarkets {
                    exchange: "binancef".to_string(),
                    provider_exchange: Some("binancef".to_string()),
                    name: "binancef".to_string(),
                    market_type: MarketType::Futures,
                    markets: vec![mmt_market.clone(), mmt_aave_market],
                },
                ExchangeMarkets {
                    exchange: "binance".to_string(),
                    provider_exchange: Some("binance".to_string()),
                    name: "binance".to_string(),
                    market_type: MarketType::Spot,
                    markets: vec![mmt_market.clone()],
                },
                ExchangeMarkets {
                    exchange: "okx".to_string(),
                    provider_exchange: Some("okx".to_string()),
                    name: "okx".to_string(),
                    market_type: MarketType::Spot,
                    markets: vec![mmt_market.clone()],
                },
                ExchangeMarkets {
                    exchange: "hyperliquid".to_string(),
                    provider_exchange: None,
                    name: "hyperliquid".to_string(),
                    market_type: MarketType::Futures,
                    markets: vec![mmt_market],
                },
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_build_provider_and_direct_indexes() {
        let registry = MarketRegistry::new(test_snapshots()).expect("snapshots index");
        assert_eq!(registry.snapshots.len(), 7);

        let bulk = exchange_market("bulkf", "btc").expect("BULK market resolves");
        assert_eq!(bulk.symbol, "BTC");
        assert_eq!(bulk.venue_symbol, "BTC-USD");
        assert_eq!(
            bulk.execution_rules().expect("execution rules").lot_size,
            0.000001
        );

        let mmt = provider_market("mmt", "binancef", "btc").expect("MMT market resolves");
        assert_eq!(mmt.provider_symbol, "btc/usd");
        assert!(mmt.execution.is_none());

        let spot = exchange_market("binance", "btc/usdt").expect("Binance spot market resolves");
        assert_eq!(spot.provider_symbol, "BTCUSDT");
        assert!(!is_futures_exchange("binance").expect("Binance spot type resolves"));

        let futures = exchange_market("binancef", "btc").expect("Binance futures market resolves");
        assert_eq!(futures.provider_symbol, "BTCUSDT");
        assert!(is_futures_exchange("binancef").expect("Binance futures type resolves"));

        let xyz = exchange_market("hyperliquidf-xyz", "tsla")
            .expect("standalone XYZ perpetual market resolves");
        assert_eq!(xyz.venue_symbol, "xyz:TSLA");
        assert_eq!(
            xyz.network_variant("testnet")
                .expect("XYZ testnet identity")
                .venue_id,
            750_001
        );
    }

    #[test]
    fn binance_filter_increments_are_strictly_decoded() {
        let filters = vec![
            BinanceFilter {
                filter_type: "PRICE_FILTER".to_string(),
                tick_size: Some("0.10".to_string()),
                step_size: None,
            },
            BinanceFilter {
                filter_type: "LOT_SIZE".to_string(),
                tick_size: None,
                step_size: Some("0.001".to_string()),
            },
        ];
        assert_eq!(
            binance_filter_increment(&filters, "PRICE_FILTER", "tickSize").expect("tick size"),
            0.1
        );
        assert_eq!(
            binance_filter_increment(&filters, "LOT_SIZE", "stepSize").expect("step size"),
            0.001
        );
        assert!(binance_filter_increment(&filters, "MARKET_LOT_SIZE", "stepSize").is_err());
    }

    #[test]
    fn provider_and_direct_routes_are_distinct() {
        assert!(provider_market("mmt", "hyperliquidf", "BTC").is_ok());
        assert_eq!(
            upstream_exchange("mmt", "hyperliquidf").expect("MMT exchange mapping resolves"),
            "hyperliquid"
        );
        let direct =
            exchange_market("hyperliquidf", "BTC").expect("standalone Hyperliquid market resolves");
        assert_eq!(direct.venue_symbol, "BTC");
        assert_eq!(direct.venue_id, Some(0));
        let spot = exchange_market("hyperliquid", "BTC/USDC")
            .expect("standalone Hyperliquid spot market resolves");
        assert_eq!(spot.base_asset, "BTC");
        assert_eq!(spot.venue_base_asset, "UBTC");
        assert_eq!(
            spot.network_variant("mainnet")
                .expect("mainnet spot identity")
                .venue_id,
            10_142
        );
        assert_eq!(
            spot.network_variant("testnet")
                .expect("testnet spot identity")
                .provider_symbol,
            "@10"
        );
        assert!(provider_market("mmt", "missing", "BTC").is_err());
    }

    #[test]
    fn futures_reject_native_and_legacy_pair_aliases() {
        assert!(exchange_market("bulkf", "BTC-USD").is_err());
        assert!(exchange_market("bulkf", "BTC/USD").is_err());
        assert!(exchange_market("bulkf", "BTC/USDT").is_err());
    }

    #[test]
    fn canonical_symbols_keep_only_real_instrument_identity() {
        assert_eq!(
            canonical_market_symbol("btc", MarketType::Futures).expect("futures base"),
            "BTC"
        );
        assert!(canonical_market_symbol("BTC/USDT", MarketType::Futures).is_err());
        assert_eq!(
            canonical_market_symbol("hype/usdh", MarketType::Spot).expect("spot pair"),
            "HYPE/USDH"
        );
        assert!(canonical_market_symbol("HYPE", MarketType::Spot).is_err());
        assert_eq!(
            canonical_market_symbol("币安人生", MarketType::Futures).expect("non-ASCII venue base"),
            "币安人生"
        );
    }

    #[test]
    fn bare_bulk_exchange_id_is_rejected() {
        let error = exchange_market("bulk", "BTC")
            .expect_err("bare bulk must not resolve as a public exchange");
        assert!(error.to_string().contains("use `bulkf`"));
    }

    #[test]
    fn exchange_market_type_is_available_in_constant_time() {
        assert!(is_futures_exchange("bulkf").expect("BULK type resolves"));
        assert!(is_futures_exchange("binancef").expect("Binance Futures type resolves"));
        assert!(is_futures_exchange("hyperliquidf").expect("Hyperliquid type resolves"));
        assert!(is_futures_exchange("hyperliquidf-xyz").expect("XYZ type resolves"));
        assert!(!is_futures_exchange("hyperliquid").expect("Hyperliquid spot type resolves"));
        assert!(!is_futures_exchange("binance").expect("Binance spot type resolves"));
        assert!(is_futures_exchange("missing").is_err());
    }

    #[test]
    fn snapshots_serialize_market_type_and_classify_legacy_exchange_entries() {
        let exchange = ExchangeMarkets {
            exchange: "bybitf".to_string(),
            provider_exchange: None,
            name: "Bybit Futures".to_string(),
            market_type: MarketType::Futures,
            markets: Vec::new(),
        };
        let encoded = serde_json::to_value(&exchange).expect("exchange serializes");
        assert_eq!(encoded["marketType"], "futures");
        assert!(encoded.get("providerExchange").is_none());

        let legacy: ExchangeMarkets = serde_json::from_value(serde_json::json!({
            "exchange": "bybitf",
            "name": "Bybit Futures",
            "markets": []
        }))
        .expect("legacy exchange entry parses");
        assert_eq!(legacy.market_type, MarketType::Futures);

        let mmt_exchange = ExchangeMarkets {
            exchange: "hyperliquidf".to_string(),
            provider_exchange: Some("hyperliquid".to_string()),
            name: "Hyperliquid Perpetuals".to_string(),
            market_type: MarketType::Futures,
            markets: Vec::new(),
        };
        let encoded = serde_json::to_value(&mmt_exchange).expect("MMT exchange serializes");
        assert_eq!(encoded["exchange"], "hyperliquidf");
        assert_eq!(encoded["providerExchange"], "hyperliquid");
    }

    #[test]
    fn mmt_exchange_families_distinguish_spot_and_futures() {
        assert_eq!(classify_mmt_exchange("binance"), MarketType::Spot);
        assert_eq!(classify_mmt_exchange("bybit"), MarketType::Spot);
        assert_eq!(classify_mmt_exchange("okx"), MarketType::Spot);
        assert_eq!(classify_mmt_exchange("binancef"), MarketType::Futures);
        assert_eq!(classify_mmt_exchange("bybitf-inverse"), MarketType::Futures);
        assert_eq!(
            classify_mmt_exchange("hyperliquidf-xyz"),
            MarketType::Futures
        );
        assert_eq!(canonical_mmt_exchange("hyperliquid"), "hyperliquidf");
        assert_eq!(
            canonical_mmt_exchange("hyperliquid-xyz"),
            "hyperliquidf-xyz"
        );
    }

    #[test]
    fn hyperliquid_spot_decode_uses_pair_ids_and_trusted_unit_token_names() {
        let decoded = decode_hyperliquid_spot_network(
            serde_json::json!([
                {
                    "tokens": [
                        {
                            "name": "UBTC",
                            "szDecimals": 5,
                            "index": 12,
                            "tokenId": "0x8f254b963e8468305d409b33aa137c67"
                        },
                        {
                            "name": "USDC",
                            "szDecimals": 8,
                            "index": 0,
                            "tokenId": "0x0"
                        }
                    ],
                    "universe": [
                        { "name": "@142", "tokens": [12, 0], "index": 142 }
                    ]
                },
                [{ "coin": "@142", "markPx": "65000" }]
            ]),
            "mainnet",
        )
        .expect("spot metadata decodes");

        let market = decoded.get("BTC/USDC").expect("BTC spot market");
        assert_eq!(market.variant.provider_symbol, "@142");
        assert_eq!(market.variant.venue_id, 10_142);
        assert_eq!(market.variant.venue_base_asset, "UBTC");
        assert_eq!(market.variant.execution.max_leverage, 1);
    }

    #[test]
    fn hyperliquid_xyz_decode_derives_network_asset_ids_without_reindexing_delisted_markets() {
        let decoded = decode_hyperliquid_xyz_network(
            serde_json::json!([
                {
                    "universe": [
                        {
                            "name": "xyz:DELISTED",
                            "szDecimals": 2,
                            "maxLeverage": 3,
                            "isDelisted": true
                        },
                        {
                            "name": "xyz:TSLA",
                            "szDecimals": 3,
                            "maxLeverage": 10,
                            "onlyIsolated": true
                        }
                    ],
                    "collateralToken": 0
                },
                [
                    { "markPx": "1" },
                    { "markPx": "410.25" }
                ]
            ]),
            serde_json::json!({
                "tokens": [
                    {
                        "name": "USDC",
                        "szDecimals": 8,
                        "index": 0,
                        "tokenId": "0x0"
                    }
                ],
                "universe": []
            }),
            1,
            "mainnet",
        )
        .expect("XYZ metadata decodes");

        let market = decoded.get("TSLA").expect("active XYZ market");
        assert_eq!(market.variant.venue_symbol, "xyz:TSLA");
        assert_eq!(market.variant.venue_id, 110_001);
        assert_eq!(market.quote_asset, "USDC");
        assert!(!market.variant.execution.cross_margin);
    }

    #[test]
    fn hyperliquid_io_decode_preserves_entropy_asset_indices_and_rules() {
        let decoded = decode_hyperliquid_hip3_network(
            serde_json::json!([
                {
                    "universe": [
                        {
                            "name": "io:OAI",
                            "szDecimals": 3,
                            "maxLeverage": 3,
                            "isDelisted": true
                        },
                        {
                            "name": "io:ANTH",
                            "szDecimals": 3,
                            "maxLeverage": 3,
                            "marginMode": "strictIsolated"
                        },
                        {
                            "name": "io:SNDK",
                            "szDecimals": 4,
                            "maxLeverage": 10,
                            "marginMode": "strictIsolated"
                        }
                    ],
                    "collateralToken": 0
                },
                [
                    { "markPx": "1" },
                    { "markPx": "100.25" },
                    { "markPx": "18.125" }
                ]
            ]),
            serde_json::json!({
                "tokens": [
                    {
                        "name": "USDC",
                        "szDecimals": 8,
                        "index": 0,
                        "tokenId": "0x0"
                    }
                ],
                "universe": []
            }),
            10,
            "mainnet",
            "io",
            "EntropyIO",
        )
        .expect("EntropyIO metadata decodes");

        assert!(!decoded.contains_key("OAI"));
        let anth = decoded.get("ANTH").expect("active ANTH market");
        assert_eq!(anth.variant.venue_symbol, "io:ANTH");
        assert_eq!(anth.variant.venue_id, 200_001);
        assert_eq!(anth.variant.execution.max_leverage, 3);
        assert!(!anth.variant.execution.cross_margin);

        let sndk = decoded.get("SNDK").expect("active SNDK market");
        assert_eq!(sndk.variant.venue_id, 200_002);
        assert_eq!(sndk.variant.execution.max_leverage, 10);
    }

    #[test]
    fn hyperliquid_spot_does_not_strip_unknown_leading_u_tokens() {
        let token = HyperliquidSpotToken {
            name: "UNICORN".to_string(),
            sz_decimals: 2,
            index: 99,
            token_id: "0xnot-a-trusted-unit-token".to_string(),
        };

        assert_eq!(
            canonical_hyperliquid_spot_token("mainnet", &token),
            "UNICORN"
        );
    }

    #[test]
    fn hyperliquid_spot_matches_sparse_contexts_by_wire_pair() {
        let decoded = decode_hyperliquid_spot_network(
            serde_json::json!([
                {
                    "tokens": [
                        {
                            "name": "UBTC",
                            "szDecimals": 5,
                            "index": 197,
                            "tokenId": "0x8f254b963e8468305d409b33aa137c67"
                        },
                        {
                            "name": "USDC",
                            "szDecimals": 8,
                            "index": 0,
                            "tokenId": "0x0"
                        }
                    ],
                    "universe": [
                        { "name": "@142", "tokens": [197, 0], "index": 142 }
                    ]
                },
                [
                    { "coin": "PURR/USDC", "markPx": "0.06" },
                    { "coin": "@142", "markPx": "65000" },
                    { "coin": "@708", "markPx": "1" }
                ]
            ]),
            "mainnet",
        )
        .expect("sparse contexts decode");

        assert_eq!(
            decoded
                .get("BTC/USDC")
                .expect("BTC spot market")
                .variant
                .venue_id,
            10_142
        );
    }

    #[test]
    fn native_token_keeps_the_clean_symbol_when_a_unit_alias_collides() {
        let decoded = decode_hyperliquid_spot_network(
            serde_json::json!([
                {
                    "tokens": [
                        {
                            "name": "PUMP",
                            "szDecimals": 0,
                            "index": 26,
                            "tokenId": "0xnative"
                        },
                        {
                            "name": "UPUMP",
                            "szDecimals": 0,
                            "index": 299,
                            "tokenId": "0x544e60f98a36d7b22c0fb5824b84f795"
                        },
                        {
                            "name": "USDC",
                            "szDecimals": 8,
                            "index": 0,
                            "tokenId": "0x0"
                        }
                    ],
                    "universe": [
                        { "name": "@20", "tokens": [26, 0], "index": 20 },
                        { "name": "@188", "tokens": [299, 0], "index": 188 }
                    ]
                },
                [
                    { "coin": "@20", "markPx": "0.003" },
                    { "coin": "@188", "markPx": "0.004" }
                ]
            ]),
            "mainnet",
        )
        .expect("colliding token names decode");

        assert_eq!(
            decoded
                .get("PUMP/USDC")
                .expect("native PUMP")
                .variant
                .venue_base_asset,
            "PUMP"
        );
        assert_eq!(
            decoded
                .get("UPUMP/USDC")
                .expect("unit PUMP")
                .variant
                .venue_base_asset,
            "UPUMP"
        );
    }
}
