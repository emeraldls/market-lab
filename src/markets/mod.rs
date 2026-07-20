use std::collections::{BTreeMap, HashMap};
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
    pub exchange: String,
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
            name: wire.name,
            markets: wire.markets,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    /// Market Lab's canonical BASE/QUOTE symbol.
    pub symbol: String,
    /// Symbol sent to the selected provider.
    pub provider_symbol: String,
    /// Exchange-native ticker when it differs from the provider symbol.
    pub venue_symbol: String,
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
    normalised_quote: String,
    tick_size: f64,
    step_size: f64,
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

    fn lookup_symbols(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.symbol.as_str())
            .chain(std::iter::once(self.provider_symbol.as_str()))
            .chain(std::iter::once(self.venue_symbol.as_str()))
            .chain(self.aliases.iter().map(String::as_str))
    }

    fn validate(&self, provider: &str, exchange: &str) -> Result<()> {
        canonical_symbol(&self.symbol)?;
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
                market.validate(&self.provider, &exchange.exchange)?;
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
                "market snapshots are not installed at {}; run `mlab markets --exchange bulk --refresh`",
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
                "market snapshots are not installed at {}; run `mlab markets --exchange bulk --refresh`",
                directory.display()
            );
        }

        let snapshots = paths
            .into_iter()
            .map(|path| {
                let source = fs::read_to_string(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                let snapshot = serde_json::from_str::<MarketSnapshot>(&source)
                    .with_context(|| format!("market snapshot {} is malformed", path.display()))?;
                let expected_name = format!("{}-markets.json", key(&snapshot.provider));
                if path.file_name().and_then(|value| value.to_str()) != Some(expected_name.as_str())
                {
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

    fn new(snapshots: Vec<MarketSnapshot>) -> Result<Self> {
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
    let registry = market_registry()?;
    let provider_key = key(provider);
    let exchange_key = key(exchange);
    let symbol_key = symbol_key(symbol);
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
    let registry = market_registry()?;
    let exchange_key = key(exchange);
    let symbol_key = symbol_key(symbol);
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

pub fn provider_exchange(
    provider: &str,
    exchange: &str,
) -> Result<(MarketSnapshot, ExchangeMarkets)> {
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

pub fn direct_exchange(exchange: &str) -> Result<(MarketSnapshot, ExchangeMarkets)> {
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
    let snapshot = match provider.map(key).as_deref() {
        Some("mmt") => fetch_mmt_snapshot().await?,
        Some(provider) => bail!("market refresh is not implemented for provider `{provider}`"),
        None if exchange.eq_ignore_ascii_case("bulk") => fetch_bulk_snapshot().await?,
        None => bail!("market refresh is not implemented for standalone exchange `{exchange}`"),
    };
    write_snapshot(&snapshot)?;
    reload()?;
    Ok(snapshot)
}

pub async fn refresh_bulk() -> Result<MarketSnapshot> {
    refresh_route(None, "bulk").await
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
            let internal_quote = internal_quote(&market.quote_asset);
            Market {
                symbol: format!(
                    "{}/{}",
                    market.base_asset.to_ascii_uppercase(),
                    internal_quote
                ),
                provider_symbol: market.symbol.clone(),
                venue_symbol: market.symbol,
                aliases: vec![format!(
                    "{}/{}",
                    market.base_asset.to_ascii_uppercase(),
                    market.quote_asset.to_ascii_uppercase()
                )],
                base_asset: market.base_asset.to_ascii_uppercase(),
                quote_asset: internal_quote,
                venue_base_asset: market.base_asset.to_ascii_uppercase(),
                venue_quote_asset: market.quote_asset.to_ascii_uppercase(),
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
                    order_types: market.order_types,
                    time_in_forces: market.time_in_forces,
                }),
            }
        })
        .collect();
    let snapshot = MarketSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        provider: "bulk".to_string(),
        provider_type: ProviderType::Standalone,
        source_url: BULK_MARKETS_URL.to_string(),
        fetched_at: fetched_at(),
        exchanges: vec![ExchangeMarkets {
            exchange: "bulk".to_string(),
            name: "BULK".to_string(),
            market_type: MarketType::Futures,
            markets,
        }],
    };
    snapshot.validate()?;
    Ok(snapshot)
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
                let quote_asset = internal_quote(&market.normalised_quote);
                let symbol = format!("{base_asset}/{quote_asset}");
                markets.entry(symbol.clone()).or_insert_with(|| Market {
                    symbol,
                    provider_symbol: market.symbol,
                    venue_symbol: market.exchange_ticker,
                    aliases: vec![format!(
                        "{}/{}",
                        market.base.to_ascii_uppercase(),
                        market.quote.to_ascii_uppercase()
                    )],
                    base_asset,
                    quote_asset,
                    venue_base_asset: market.base.to_ascii_uppercase(),
                    venue_quote_asset: market.quote.to_ascii_uppercase(),
                    status: "AVAILABLE".to_string(),
                    price_increment: Some(market.tick_size),
                    size_increment: Some(market.step_size),
                    execution: None,
                });
            }
            ExchangeMarkets {
                market_type: classify_mmt_exchange(&exchange.id),
                exchange: exchange.id,
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

fn canonical_symbol(symbol: &str) -> Result<String> {
    let normalized = symbol.trim().to_ascii_uppercase().replace('-', "/");
    let mut parts = normalized.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(base), Some(quote), None) if !base.is_empty() && !quote.is_empty() => {
            Ok(format!("{base}/{quote}"))
        }
        _ => bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT"),
    }
}

fn internal_quote(quote: &str) -> String {
    match quote.trim().to_ascii_uppercase().as_str() {
        "USD" => "USDT".to_string(),
        quote => quote.to_string(),
    }
}

fn fetched_at() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn symbol_key(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase().replace('-', "/")
}

fn key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn classify_mmt_exchange(exchange: &str) -> MarketType {
    classify_exchange_name(exchange)
}

fn classify_exchange_name(exchange: &str) -> MarketType {
    let exchange = key(exchange);
    let family = exchange.split('-').next().unwrap_or(exchange.as_str());
    if family.ends_with('f') || matches!(family, "bulk" | "hyperliquid") {
        MarketType::Futures
    } else {
        MarketType::Spot
    }
}

#[cfg(test)]
fn test_snapshots() -> Vec<MarketSnapshot> {
    let bulk_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "BTC-USD".to_string(),
        venue_symbol: "BTC-USD".to_string(),
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
            order_types: vec!["LIMIT".to_string(), "MARKET".to_string()],
            time_in_forces: vec!["GTC".to_string(), "IOC".to_string(), "ALO".to_string()],
        }),
    };
    let mmt_market = Market {
        symbol: "BTC/USDT".to_string(),
        provider_symbol: "btc/usd".to_string(),
        venue_symbol: "btc/usd".to_string(),
        aliases: vec!["BTC/USDT".to_string()],
        base_asset: "BTC".to_string(),
        quote_asset: "USDT".to_string(),
        venue_base_asset: "BTC".to_string(),
        venue_quote_asset: "USDT".to_string(),
        status: "AVAILABLE".to_string(),
        price_increment: Some(0.1),
        size_increment: Some(0.001),
        execution: None,
    };
    vec![
        MarketSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            provider: "bulk".to_string(),
            provider_type: ProviderType::Standalone,
            source_url: BULK_MARKETS_URL.to_string(),
            fetched_at: "2026-07-19T00:00:00Z".to_string(),
            exchanges: vec![ExchangeMarkets {
                exchange: "bulk".to_string(),
                name: "BULK".to_string(),
                market_type: MarketType::Futures,
                markets: vec![bulk_market],
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
                    name: "binancef".to_string(),
                    market_type: MarketType::Futures,
                    markets: vec![mmt_market.clone()],
                },
                ExchangeMarkets {
                    exchange: "binance".to_string(),
                    name: "binance".to_string(),
                    market_type: MarketType::Spot,
                    markets: vec![mmt_market.clone()],
                },
                ExchangeMarkets {
                    exchange: "hyperliquid".to_string(),
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
        assert_eq!(registry.snapshots.len(), 2);

        let bulk = exchange_market("bulk", "btc/usdt").expect("BULK market resolves");
        assert_eq!(bulk.symbol, "BTC/USDT");
        assert_eq!(bulk.venue_symbol, "BTC-USD");
        assert_eq!(
            bulk.execution_rules().expect("execution rules").lot_size,
            0.000001
        );

        let mmt = provider_market("mmt", "binancef", "btc/usdt").expect("MMT market resolves");
        assert_eq!(mmt.provider_symbol, "btc/usd");
        assert!(mmt.execution.is_none());
    }

    #[test]
    fn provider_and_direct_routes_are_distinct() {
        assert!(provider_market("mmt", "hyperliquid", "BTC/USDT").is_ok());
        assert!(exchange_market("hyperliquid", "BTC/USDT").is_err());
        assert!(provider_market("mmt", "missing", "BTC/USDT").is_err());
    }

    #[test]
    fn bulk_native_and_usd_aliases_resolve() {
        let native = exchange_market("bulk", "BTC-USD").expect("native symbol resolves");
        let usd = exchange_market("bulk", "BTC/USD").expect("USD alias resolves");
        assert_eq!(native.symbol, "BTC/USDT");
        assert_eq!(native.symbol, usd.symbol);
    }

    #[test]
    fn exchange_market_type_is_available_in_constant_time() {
        assert!(is_futures_exchange("bulk").expect("BULK type resolves"));
        assert!(is_futures_exchange("binancef").expect("Binance Futures type resolves"));
        assert!(is_futures_exchange("hyperliquid").expect("Hyperliquid type resolves"));
        assert!(!is_futures_exchange("binance").expect("Binance spot type resolves"));
        assert!(is_futures_exchange("missing").is_err());
    }

    #[test]
    fn snapshots_serialize_market_type_and_classify_legacy_exchange_entries() {
        let exchange = ExchangeMarkets {
            exchange: "bybitf".to_string(),
            name: "Bybit Futures".to_string(),
            market_type: MarketType::Futures,
            markets: Vec::new(),
        };
        let encoded = serde_json::to_value(&exchange).expect("exchange serializes");
        assert_eq!(encoded["marketType"], "futures");

        let legacy: ExchangeMarkets = serde_json::from_value(serde_json::json!({
            "exchange": "bybitf",
            "name": "Bybit Futures",
            "markets": []
        }))
        .expect("legacy exchange entry parses");
        assert_eq!(legacy.market_type, MarketType::Futures);
    }

    #[test]
    fn mmt_exchange_families_distinguish_spot_and_futures() {
        assert_eq!(classify_mmt_exchange("binance"), MarketType::Spot);
        assert_eq!(classify_mmt_exchange("bybit"), MarketType::Spot);
        assert_eq!(classify_mmt_exchange("okx"), MarketType::Spot);
        assert_eq!(classify_mmt_exchange("binancef"), MarketType::Futures);
        assert_eq!(classify_mmt_exchange("bybitf-inverse"), MarketType::Futures);
        assert_eq!(
            classify_mmt_exchange("hyperliquid-xyz"),
            MarketType::Futures
        );
    }
}
