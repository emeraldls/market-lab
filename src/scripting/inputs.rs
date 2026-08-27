use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde_json::{Map, Value, json};

use crate::domain::enums::ProviderKind;
use crate::providers::market_data::MarketDataAdapter;

use super::manifest::{InputType, ScriptManifest, ScriptParamSchema, ScriptSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConfig {
    pub selector: String,
    pub symbol: String,
    pub source: ScriptSource,
    pub provider: ProviderKind,
    pub exchange: String,
    pub position: usize,
    pub timeframe: Option<u32>,
    pub depth: Option<u16>,
    pub bucket: Option<u8>,
}

impl SourceConfig {
    fn new(
        selector: String,
        symbol: String,
        source: ScriptSource,
        provider: ProviderKind,
        exchange: String,
        position: usize,
    ) -> Self {
        Self {
            selector,
            symbol,
            source,
            provider,
            exchange,
            position,
            timeframe: None,
            depth: None,
            bucket: None,
        }
    }

    pub fn market_symbol(&self) -> String {
        script_symbol_to_market(&self.symbol)
    }

    pub fn require_timeframe(&self, source: &ScriptSource) -> Result<u32> {
        self.timeframe.ok_or_else(|| {
            anyhow::anyhow!("source {}:timeframe=<seconds> is required", source.as_str())
        })
    }

    pub fn depth_or_default(&self) -> u16 {
        self.depth.unwrap_or(100)
    }

    pub fn require_bucket(&self, source: &ScriptSource) -> Result<u8> {
        self.bucket.ok_or_else(|| {
            anyhow::anyhow!("source {}:bucket=<1..=11> is required", source.as_str())
        })
    }
}

pub type SourceConfigs = BTreeMap<String, SourceConfig>;
pub type RawParamValues = BTreeMap<String, String>;

pub fn parse_source_configs(values: &[String]) -> Result<SourceConfigs> {
    let mut configs = SourceConfigs::new();

    for (position, value) in values.iter().enumerate() {
        let (binding, options) = split_source_options(value);
        let (selector, symbol, source, provider, exchange) = parse_source_selector(binding)?;
        validate_source_market(provider, &exchange, &script_symbol_to_market(&symbol))?;
        let config = configs.entry(selector.clone()).or_insert_with(|| {
            SourceConfig::new(
                selector.clone(),
                symbol.clone(),
                source.clone(),
                provider,
                exchange.clone(),
                position,
            )
        });
        if config.provider != provider || config.exchange != exchange {
            bail!(
                "source `{selector}` cannot bind both {} and {exchange}",
                config.exchange
            );
        }
        if options.is_empty() {
            continue;
        }
        for option in options.split(',') {
            let Some((key, raw_value)) = option.split_once('=') else {
                bail!("source option must use key=value, got `{option}` in `{value}`");
            };
            if key.trim().is_empty() {
                bail!("source key cannot be empty");
            }
            let duplicate = match (source.clone(), key) {
                (ScriptSource::Candles, "timeframe")
                | (ScriptSource::Orderbook, "timeframe")
                | (ScriptSource::Vd, "timeframe")
                | (ScriptSource::Oi, "timeframe")
                | (ScriptSource::Volumes, "timeframe") => config
                    .timeframe
                    .replace(parse_positive_u32(raw_value, "timeframe")?)
                    .is_some(),
                (ScriptSource::Orderbook, "depth") => config
                    .depth
                    .replace(parse_positive_u16(raw_value, "depth")?)
                    .is_some(),
                (ScriptSource::Vd, "bucket") => {
                    config.bucket.replace(parse_bucket(raw_value)?).is_some()
                }
                _ => bail!("unknown source {selector}:{key}"),
            };
            if duplicate {
                bail!("duplicate source {selector}:{key}");
            }
        }
    }

    reject_duplicate_resolved_sources(&configs)?;

    Ok(configs)
}

fn split_source_options(value: &str) -> (&str, &str) {
    let separator = value.rfind('@').and_then(|last_at| {
        value[last_at..]
            .find(':')
            .map(|relative| last_at + relative)
    });
    separator.map_or((value, ""), |index| (&value[..index], &value[index + 1..]))
}

pub fn parse_param_values(values: &[String]) -> Result<RawParamValues> {
    let mut parsed = RawParamValues::new();
    for value in values {
        let Some((key, raw)) = value.split_once('=') else {
            bail!("--param must use key=value, got `{value}`");
        };
        if key.trim().is_empty() || key.contains(':') {
            bail!("--param must use key=value, got `{value}`");
        }
        if parsed.insert(key.to_string(), raw.to_string()).is_some() {
            bail!("duplicate --param {key}");
        }
    }
    Ok(parsed)
}

pub fn source_config<'a>(
    configs: &'a SourceConfigs,
    source: &ScriptSource,
) -> Result<&'a SourceConfig> {
    let mut matching = configs.values().filter(|config| &config.source == source);
    let config = matching
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing source config for {}", source.as_str()))?;
    if matching.next().is_some() {
        bail!(
            "multiple {} source configs require a selector",
            source.as_str()
        );
    }
    Ok(config)
}

pub fn source_exchange_label(configs: &SourceConfigs) -> String {
    let mut exchanges = configs
        .values()
        .map(|config| config.exchange.as_str())
        .collect::<Vec<_>>();
    exchanges.sort_unstable();
    exchanges.dedup();
    exchanges.join(",")
}

pub fn source_provider_label(configs: &SourceConfigs) -> String {
    let mut providers = configs
        .values()
        .map(source_config_provider_name)
        .collect::<Vec<_>>();
    providers.sort_unstable();
    providers.dedup();
    providers.join(",")
}

pub fn source_type_names(configs: &SourceConfigs) -> Vec<String> {
    let mut configs = configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    let mut names = Vec::new();
    for config in configs {
        let name = config.source.as_str().to_string();
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

pub fn configured_source_selectors(configs: &SourceConfigs) -> Vec<String> {
    let mut configs = configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    configs
        .into_iter()
        .map(|config| config.selector.clone())
        .collect()
}

fn source_config_provider_name(config: &SourceConfig) -> String {
    match config.provider {
        ProviderKind::Direct => config.exchange.clone(),
        provider => source_provider_name(provider).to_string(),
    }
}

pub fn source_provider_name(provider: ProviderKind) -> &'static str {
    provider.as_str()
}

pub fn source_configs_payload(configs: &SourceConfigs) -> Value {
    let mut payload = Map::new();
    let mut configs = configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    for config in configs {
        payload.insert(
            config.selector.clone(),
            json!({
                "symbol": config.symbol,
                "market_symbol": config.market_symbol(),
                "type": config.source.as_str(),
                "provider": source_config_provider_name(config),
                "exchange": config.exchange,
                "timeframe_sec": config.timeframe,
                "depth": config.depth,
                "bucket": config.bucket,
            }),
        );
    }
    Value::Object(payload)
}

fn validate_source_requirements(manifest: &ScriptManifest, configs: &SourceConfigs) -> Result<()> {
    if manifest.version.trim() == "2" {
        if configs.is_empty() {
            bail!("Python Scripting V2 requires at least one literal history.source selector");
        }
        return Ok(());
    }

    for config in configs.values() {
        if !manifest.sources.contains(&config.source) {
            bail!("source {} is not listed in script.sources", config.selector);
        }
    }

    for source in &manifest.sources {
        if !configs.values().any(|config| &config.source == source) {
            bail!("missing source config for {}", source.as_str());
        }
    }
    Ok(())
}

pub fn validate_source_configs(manifest: &ScriptManifest, configs: &SourceConfigs) -> Result<()> {
    validate_source_requirements(manifest, configs)?;

    for config in configs.values() {
        validate_source_config(config, true)?;
    }

    Ok(())
}

pub fn validate_historical_source_config(config: &SourceConfig) -> Result<()> {
    validate_source_config(config, true)
}

pub fn validate_source_configs_for_run(
    manifest: &ScriptManifest,
    configs: &SourceConfigs,
) -> Result<()> {
    validate_source_requirements(manifest, configs)?;

    for config in configs.values() {
        validate_source_config(config, false)?;
    }

    Ok(())
}

fn validate_source_config(config: &SourceConfig, historical: bool) -> Result<()> {
    if config.source == ScriptSource::Oi && !crate::markets::is_futures_exchange(&config.exchange)?
    {
        bail!(
            "source {} requires a futures exchange; `{}` is spot",
            config.selector,
            config.exchange
        );
    }
    match config.provider {
        ProviderKind::Mmt => match &config.source {
            ScriptSource::Candles => {
                let timeframe = config.require_timeframe(&config.source)?;
                if historical {
                    crate::cli::mmt_timeframe_from_seconds(timeframe)?;
                }
            }
            ScriptSource::Trades => {
                if historical {
                    bail!("MMT raw trades are live-only and cannot be backtested");
                }
            }
            ScriptSource::Orderbook => {
                if historical {
                    config.require_timeframe(&config.source)?;
                }
                if config.depth_or_default() == 0 {
                    bail!("source {}:depth must be >= 1", config.selector);
                }
            }
            ScriptSource::Vd => {
                config.require_timeframe(&config.source)?;
                config.require_bucket(&config.source)?;
            }
            ScriptSource::Oi | ScriptSource::Volumes => {
                config.require_timeframe(&config.source)?;
            }
        },
        ProviderKind::Direct => {
            let adapter = MarketDataAdapter::for_exchange(&config.exchange, false)?;
            let capabilities = adapter.capabilities();
            match &config.source {
                ScriptSource::Candles => {
                    let timeframe = config.require_timeframe(&config.source)?;
                    if historical {
                        direct_timeframe_from_seconds(&config.exchange, timeframe)?;
                        if !capabilities.historical_candles {
                            bail!("{} does not provide historical candles", config.exchange);
                        }
                    } else if !capabilities.live_trades {
                        bail!(
                            "{} does not provide live trades for candle aggregation",
                            config.exchange
                        );
                    }
                }
                ScriptSource::Trades if historical => {
                    bail!(
                        "{} raw trades are live-only and cannot be backtested",
                        config.exchange
                    );
                }
                ScriptSource::Trades => {
                    if config.timeframe.is_some() {
                        bail!("standalone live trades do not use a timeframe");
                    }
                    if !capabilities.live_trades {
                        bail!("{} does not provide live trades", config.exchange);
                    }
                }
                ScriptSource::Volumes => {
                    let timeframe = config.require_timeframe(&config.source)?;
                    direct_timeframe_from_seconds(&config.exchange, timeframe)?;
                    let supported = if historical {
                        capabilities.historical_volume_bars
                    } else {
                        capabilities.live_candles
                    };
                    if !supported {
                        bail!(
                            "{} does not provide {} volume bars",
                            config.exchange,
                            if historical { "historical" } else { "live" }
                        );
                    }
                }
                ScriptSource::Orderbook if historical => {
                    bail!(
                        "{} does not provide historical orderbooks for script backtests",
                        config.exchange
                    );
                }
                ScriptSource::Vd if historical => {
                    bail!(
                        "{} does not provide historical volume delta for script backtests",
                        config.exchange
                    );
                }
                ScriptSource::Oi if historical => {
                    bail!(
                        "{} does not provide historical open interest for script backtests",
                        config.exchange
                    );
                }
                ScriptSource::Orderbook => {
                    if config.timeframe.is_some() {
                        bail!("standalone live orderbook does not use a timeframe");
                    }
                    if !capabilities.live_orderbook {
                        bail!("{} does not provide live orderbooks", config.exchange);
                    }
                }
                ScriptSource::Vd => {
                    if config.timeframe.is_some() || config.bucket.is_some() {
                        bail!(
                            "standalone live volume delta is trade-derived; omit timeframe and bucket"
                        );
                    }
                    if !capabilities.live_trades {
                        bail!(
                            "{} does not provide live trades for volume delta",
                            config.exchange
                        );
                    }
                }
                ScriptSource::Oi => {
                    if config.timeframe.is_some() {
                        bail!("standalone live open interest is snapshot-based; omit timeframe");
                    }
                    if !capabilities.live_ticker {
                        bail!("{} does not provide live open interest", config.exchange);
                    }
                }
            }
        }
        ProviderKind::MarketLab => bail!("marketlab is not a script source provider"),
    }
    Ok(())
}

pub fn resolve_params(manifest: &ScriptManifest, raw_params: &RawParamValues) -> Result<Value> {
    for key in raw_params.keys() {
        if !manifest.params.contains_key(key) {
            bail!("unknown script param `{key}`");
        }
    }

    let mut out = Map::new();
    for (key, schema) in &manifest.params {
        if let Some(raw) = raw_params.get(key) {
            out.insert(key.clone(), coerce_value(raw, schema)?);
            continue;
        }
        if let Some(default) = &schema.default {
            out.insert(key.clone(), default.clone());
            continue;
        }
        if schema.required {
            bail!("missing required script param `{key}`");
        }
    }

    Ok(Value::Object(out))
}

fn parse_source(source: &str) -> Result<ScriptSource> {
    match source {
        "candles" => Ok(ScriptSource::Candles),
        "orderbook" => Ok(ScriptSource::Orderbook),
        "trades" => Ok(ScriptSource::Trades),
        "vd" => Ok(ScriptSource::Vd),
        "oi" => Ok(ScriptSource::Oi),
        "volumes" => Ok(ScriptSource::Volumes),
        _ => bail!("unknown script source `{source}`"),
    }
}

fn parse_source_selector(
    raw: &str,
) -> Result<(String, String, ScriptSource, ProviderKind, String)> {
    let parts = raw.split('@').collect::<Vec<_>>();
    let (symbol_raw, source_raw, exchange, provider) = match parts.as_slice() {
        [symbol, source, provider_name] => {
            let provider = parse_source_provider(provider_name)?;
            if provider == ProviderKind::Mmt {
                bail!(
                    "MMT sources require symbol@source@exchange@mmt, for example `{symbol}@{source}@binancef@mmt`"
                );
            }
            (*symbol, *source, *provider_name, provider)
        }
        [symbol, source, exchange, provider] => {
            let provider = parse_source_provider(provider)?;
            if provider != ProviderKind::Mmt {
                bail!(
                    "{} sources must use `symbol@source@{}`",
                    source_provider_name(provider),
                    source_provider_name(provider)
                );
            }
            (*symbol, *source, *exchange, provider)
        }
        _ => bail!(
            "source `{raw}` must use symbol@source@provider or symbol@source@exchange@provider"
        ),
    };
    let symbol = normalize_script_symbol(symbol_raw)?;
    let source = parse_source(source_raw)?;
    let exchange = exchange.trim().to_ascii_lowercase();
    validate_exchange_name(&exchange)?;
    let selector = match provider {
        ProviderKind::Mmt => format!("{symbol}@{}@{exchange}@mmt", source.as_str()),
        ProviderKind::Direct => format!("{symbol}@{}@{exchange}", source.as_str()),
        ProviderKind::MarketLab => unreachable!(),
    };
    Ok((selector, symbol, source, provider, exchange))
}

pub fn normalize_script_symbol(raw: &str) -> Result<String> {
    let symbol = raw.trim().to_ascii_lowercase();
    if symbol.is_empty()
        || !symbol
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, '-' | '_' | '/' | ':'))
        || symbol.starts_with('/')
        || symbol.ends_with('/')
        || symbol.matches('/').count() > 1
    {
        bail!(
            "script symbol `{raw}` must be a base asset, spot pair, or outcome side such as `btc`, `hype/usdc`, or `1001:0`"
        );
    }
    Ok(symbol)
}

pub fn script_symbol_to_market(symbol: &str) -> String {
    symbol.trim().to_ascii_uppercase()
}

fn validate_source_market(_provider: ProviderKind, exchange: &str, symbol: &str) -> Result<()> {
    if exchange.eq_ignore_ascii_case("hyperliquid-outcomes") {
        return crate::providers::hyperliquid::outcomes::parse_symbol(symbol).map(|_| ());
    }
    let market_type = if crate::markets::is_futures_exchange(exchange)? {
        crate::markets::MarketType::Futures
    } else {
        crate::markets::MarketType::Spot
    };
    crate::markets::canonical_market_symbol(symbol, market_type).map(|_| ())
}

fn parse_source_provider(raw: &str) -> Result<ProviderKind> {
    let raw = raw.trim().to_ascii_lowercase();
    match raw.as_str() {
        "mmt" => Ok(ProviderKind::Mmt),
        _ if MarketDataAdapter::for_exchange(&raw, false).is_ok() => Ok(ProviderKind::Direct),
        _ => bail!("unsupported script source provider `{raw}`"),
    }
}

fn direct_timeframe_from_seconds(exchange: &str, seconds: u32) -> Result<&'static str> {
    MarketDataAdapter::for_exchange(exchange, false)?.timeframe_from_seconds(seconds)
}

fn validate_exchange_name(exchange: &str) -> Result<()> {
    if exchange.is_empty()
        || !exchange
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        bail!("script source exchange `{exchange}` must use letters, numbers, `-`, or `_`");
    }
    Ok(())
}

fn reject_duplicate_resolved_sources(configs: &SourceConfigs) -> Result<()> {
    let values = configs.values().collect::<Vec<_>>();
    for (idx, left) in values.iter().enumerate() {
        for right in values.iter().skip(idx + 1) {
            if left.source == right.source
                && left.symbol == right.symbol
                && left.provider == right.provider
                && left.exchange == right.exchange
            {
                bail!(
                    "duplicate script source {} for exchange {}",
                    left.source.as_str(),
                    left.exchange
                );
            }
        }
    }
    Ok(())
}

fn parse_positive_u32(raw: &str, key: &str) -> Result<u32> {
    let parsed: u32 = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("expected positive integer for {key}, got `{raw}`"))?;
    if parsed == 0 {
        bail!("{key} must be >= 1");
    }
    Ok(parsed)
}

fn parse_positive_u16(raw: &str, key: &str) -> Result<u16> {
    let parsed: u16 = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("expected positive integer for {key}, got `{raw}`"))?;
    if parsed == 0 {
        bail!("{key} must be >= 1");
    }
    Ok(parsed)
}

fn parse_bucket(raw: &str) -> Result<u8> {
    let parsed: u8 = raw
        .parse()
        .map_err(|_| anyhow::anyhow!("expected integer bucket 1..=11, got `{raw}`"))?;
    if !(1..=11).contains(&parsed) {
        bail!("bucket must be in range 1..=11");
    }
    Ok(parsed)
}

fn coerce_value(raw: &str, schema: &ScriptParamSchema) -> Result<Value> {
    match schema.input_type {
        InputType::String => Ok(Value::String(raw.to_string())),
        InputType::Number => {
            let parsed: f64 = raw
                .parse()
                .map_err(|_| anyhow::anyhow!("expected number, got `{raw}`"))?;
            let number = serde_json::Number::from_f64(parsed)
                .ok_or_else(|| anyhow::anyhow!("invalid number `{raw}`"))?;
            Ok(Value::Number(number))
        }
        InputType::Boolean => match raw {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => bail!("expected boolean true|false, got `{raw}`"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripting::manifest::{ScriptManifest, ScriptParamSchema};

    fn manifest(sources: Vec<ScriptSource>) -> ScriptManifest {
        ScriptManifest {
            name: "test-script".to_string(),
            version: "1".to_string(),
            sources,
            description: None,
            lookback: None,
            params: BTreeMap::new(),
        }
    }

    #[test]
    fn resolves_required_and_default_params() {
        let manifest = ScriptManifest {
            name: "buy-pressure".to_string(),
            version: "1".to_string(),
            sources: vec![ScriptSource::Candles],
            description: None,
            lookback: None,
            params: BTreeMap::from([
                (
                    "min_vbuy".to_string(),
                    ScriptParamSchema {
                        input_type: InputType::Number,
                        required: true,
                        default: None,
                        description: None,
                    },
                ),
                (
                    "enabled".to_string(),
                    ScriptParamSchema {
                        input_type: InputType::Boolean,
                        required: false,
                        default: Some(Value::Bool(true)),
                        description: None,
                    },
                ),
            ]),
        };

        let raw = parse_param_values(&["min_vbuy=50000".to_string()]).unwrap();
        let value = resolve_params(&manifest, &raw).expect("params resolve");

        assert_eq!(value["min_vbuy"], 50000.0);
        assert_eq!(value["enabled"], true);
    }

    #[test]
    fn rejects_source_scoped_param_syntax() {
        let error = parse_param_values(&["candles:min_vbuy=50000".to_string()])
            .expect_err("source-scoped params must be rejected");

        assert!(error.to_string().contains("key=value"));
    }

    #[test]
    fn parses_exchange_qualified_source_configs() {
        let configs = parse_source_configs(&[
            "btc/usdt@candles@okx@mmt:timeframe=60".to_string(),
            "btc@orderbook@binancef@mmt:timeframe=60,depth=50".to_string(),
            "btc@trades@hyperliquidf@mmt".to_string(),
            "btc@vd@hyperliquidf@mmt:timeframe=60,bucket=1".to_string(),
            "btc@oi@binancef@mmt:timeframe=60".to_string(),
            "btc/usdt@volumes@okx@mmt:timeframe=60".to_string(),
        ])
        .unwrap();
        assert_eq!(configs["btc/usdt@candles@okx@mmt"].exchange, "okx");
        assert_eq!(configs["btc/usdt@candles@okx@mmt"].symbol, "btc/usdt");
        assert_eq!(
            configs["btc/usdt@candles@okx@mmt"].provider,
            ProviderKind::Mmt
        );
        assert_eq!(configs["btc@orderbook@binancef@mmt"].depth, Some(50));
        assert_eq!(configs["btc@trades@hyperliquidf@mmt"].timeframe, None);
        assert_eq!(configs["btc@vd@hyperliquidf@mmt"].bucket, Some(1));
        assert_eq!(configs["btc@oi@binancef@mmt"].timeframe, Some(60));
        assert_eq!(configs["btc/usdt@volumes@okx@mmt"].timeframe, Some(60));
    }

    #[test]
    fn python_v2_uses_configured_sources_without_manifest_duplication() {
        let python_manifest = ScriptManifest {
            name: "python-script".to_string(),
            version: "2".to_string(),
            sources: Vec::new(),
            description: None,
            lookback: None,
            params: BTreeMap::new(),
        };
        let configs = parse_source_configs(&[
            "btc@oi@binancef@mmt:timeframe=60".to_string(),
            "eth@candles@hyperliquidf@mmt:timeframe=60".to_string(),
            "eth@orderbook@binancef@mmt:timeframe=60,depth=20".to_string(),
        ])
        .expect("parse Python source configs");

        validate_source_configs(&python_manifest, &configs)
            .expect("configured Python sources are authoritative");
        assert_eq!(
            source_type_names(&configs),
            vec!["oi", "candles", "orderbook"]
        );
        assert_eq!(
            configured_source_selectors(&configs),
            vec![
                "btc@oi@binancef@mmt",
                "eth@candles@hyperliquidf@mmt",
                "eth@orderbook@binancef@mmt",
            ]
        );
    }

    #[test]
    fn python_v2_requires_a_configured_source() {
        let python_manifest = ScriptManifest {
            name: "python-script".to_string(),
            version: "2".to_string(),
            sources: Vec::new(),
            description: None,
            lookback: None,
            params: BTreeMap::new(),
        };

        let error = validate_source_configs(&python_manifest, &SourceConfigs::new())
            .expect_err("Python must receive at least one source");
        assert!(format!("{error:#}").contains("literal history.source selector"));
    }

    #[test]
    fn validates_two_candle_exchanges() {
        let manifest = manifest(vec![ScriptSource::Candles]);
        let configs = parse_source_configs(&[
            "btc@candles@binancef@mmt:timeframe=60".to_string(),
            "btc/usdt@candles@okx@mmt:timeframe=300".to_string(),
        ])
        .expect("qualified bindings should parse");

        validate_source_configs(&manifest, &configs).expect("backtest configs should validate");
        validate_source_configs_for_run(&manifest, &configs).expect("live configs should validate");
        assert_eq!(source_exchange_label(&configs), "binancef,okx");
        assert_eq!(configs["btc/usdt@candles@okx@mmt"].timeframe, Some(300));
    }

    #[test]
    fn keeps_the_same_feed_for_different_symbols_distinct() {
        let manifest = manifest(vec![ScriptSource::Candles]);
        let configs = parse_source_configs(&[
            "btc@candles@binancef@mmt:timeframe=60".to_string(),
            "zec@candles@binancef@mmt:timeframe=60".to_string(),
        ])
        .expect("multi-symbol bindings should parse");

        validate_source_configs(&manifest, &configs).expect("multi-symbol configs should validate");
        assert_eq!(configs.len(), 2);
        assert_eq!(configs["btc@candles@binancef@mmt"].market_symbol(), "BTC");
        assert_eq!(configs["zec@candles@binancef@mmt"].market_symbol(), "ZEC");
    }

    #[test]
    fn live_candles_accept_custom_second_timeframes() {
        let manifest = manifest(vec![ScriptSource::Candles]);
        for selector in [
            "btc@candles@binancef@mmt:timeframe=1",
            "btc@candles@bulkf:timeframe=1",
        ] {
            let configs = parse_source_configs(&[selector.to_string()]).unwrap();
            validate_source_configs_for_run(&manifest, &configs)
                .expect("trade-derived live candles should accept one second");
            validate_source_configs(&manifest, &configs)
                .expect_err("historical providers do not store one-second candles");
        }
    }

    #[test]
    fn rejects_source_without_exchange() {
        let error = parse_source_configs(&["candles:timeframe=60".to_string()])
            .expect_err("unqualified source must fail");
        assert!(error.to_string().contains("symbol@source@provider"));
    }

    #[test]
    fn validates_bulkf_bindings_for_snapshot_sources() {
        let live_manifest = manifest(vec![
            ScriptSource::Candles,
            ScriptSource::Orderbook,
            ScriptSource::Trades,
            ScriptSource::Vd,
            ScriptSource::Oi,
        ]);
        let configs = parse_source_configs(&[
            "btc@candles@bulkf:timeframe=60".to_string(),
            "btc@orderbook@bulkf:depth=50".to_string(),
            "btc@trades@bulkf".to_string(),
            "btc@vd@bulkf".to_string(),
            "btc@oi@bulkf".to_string(),
        ])
        .unwrap();

        validate_source_configs_for_run(&live_manifest, &configs)
            .expect("BULK live configs should validate");
        assert!(configs.contains_key("btc@vd@bulkf"));
        assert!(configs.contains_key("btc@oi@bulkf"));
        assert!(configs.contains_key("btc@trades@bulkf"));
    }

    #[test]
    fn rejects_bare_bulk_source_bindings() {
        let error = parse_source_configs(&["btc@orderbook@bulk:depth=50".to_string()])
            .expect_err("bare bulk must not be accepted as a script source");
        assert!(
            error
                .to_string()
                .contains("unsupported script source provider `bulk`")
        );
    }

    #[test]
    fn validates_standalone_hyperliquid_bindings() {
        let live_manifest = manifest(vec![
            ScriptSource::Candles,
            ScriptSource::Orderbook,
            ScriptSource::Trades,
            ScriptSource::Vd,
            ScriptSource::Oi,
            ScriptSource::Volumes,
        ]);
        let configs = parse_source_configs(&[
            "btc@candles@hyperliquidf:timeframe=60".to_string(),
            "btc@orderbook@hyperliquidf:depth=20".to_string(),
            "btc@trades@hyperliquidf".to_string(),
            "btc@vd@hyperliquidf".to_string(),
            "btc@oi@hyperliquidf".to_string(),
            "btc@volumes@hyperliquidf:timeframe=60".to_string(),
        ])
        .expect("standalone Hyperliquid selectors should parse");

        validate_source_configs_for_run(&live_manifest, &configs)
            .expect("standalone Hyperliquid live configs should validate");
        assert_eq!(
            configs["btc@candles@hyperliquidf"].provider,
            ProviderKind::Direct
        );
        assert_eq!(configs["btc@orderbook@hyperliquidf"].depth, Some(20));
        assert_eq!(configs["btc@trades@hyperliquidf"].timeframe, None);

        let historical_manifest = manifest(vec![ScriptSource::Candles, ScriptSource::Volumes]);
        let historical_configs = parse_source_configs(&[
            "btc@candles@hyperliquidf:timeframe=60".to_string(),
            "btc@volumes@hyperliquidf:timeframe=60".to_string(),
        ])
        .expect("historical Hyperliquid selectors should parse");
        validate_source_configs(&historical_manifest, &historical_configs)
            .expect("Hyperliquid candles and volume should support backtests");
    }

    #[test]
    fn validates_standalone_hyperliquid_xyz_bindings() {
        let live_manifest = manifest(vec![
            ScriptSource::Candles,
            ScriptSource::Orderbook,
            ScriptSource::Trades,
            ScriptSource::Vd,
            ScriptSource::Oi,
            ScriptSource::Volumes,
        ]);
        let configs = parse_source_configs(&[
            "tsla@candles@hyperliquidf-xyz:timeframe=60".to_string(),
            "tsla@orderbook@hyperliquidf-xyz:depth=20".to_string(),
            "tsla@trades@hyperliquidf-xyz".to_string(),
            "tsla@vd@hyperliquidf-xyz".to_string(),
            "tsla@oi@hyperliquidf-xyz".to_string(),
            "tsla@volumes@hyperliquidf-xyz:timeframe=60".to_string(),
        ])
        .expect("standalone XYZ selectors should parse");

        validate_source_configs_for_run(&live_manifest, &configs)
            .expect("standalone XYZ live configs should validate");
        assert_eq!(
            configs["tsla@candles@hyperliquidf-xyz"].provider,
            ProviderKind::Direct
        );
        assert_eq!(
            configs["tsla@candles@hyperliquidf-xyz"].exchange,
            "hyperliquidf-xyz"
        );

        let historical_manifest = manifest(vec![ScriptSource::Candles, ScriptSource::Volumes]);
        validate_source_configs(
            &historical_manifest,
            &parse_source_configs(&[
                "tsla@candles@hyperliquidf-xyz:timeframe=60".to_string(),
                "tsla@volumes@hyperliquidf-xyz:timeframe=60".to_string(),
            ])
            .expect("historical XYZ selectors should parse"),
        )
        .expect("XYZ candles and volume should support backtests");
    }

    #[test]
    fn validates_standalone_hyperliquid_spot_bindings() {
        let live_manifest = manifest(vec![
            ScriptSource::Candles,
            ScriptSource::Orderbook,
            ScriptSource::Trades,
            ScriptSource::Vd,
            ScriptSource::Volumes,
        ]);
        let configs = parse_source_configs(&[
            "btc/usdc@candles@hyperliquid:timeframe=60".to_string(),
            "btc/usdc@orderbook@hyperliquid:depth=20".to_string(),
            "btc/usdc@trades@hyperliquid".to_string(),
            "btc/usdc@vd@hyperliquid".to_string(),
            "btc/usdc@volumes@hyperliquid:timeframe=60".to_string(),
        ])
        .expect("standalone Hyperliquid spot selectors should parse");

        validate_source_configs_for_run(&live_manifest, &configs)
            .expect("standalone Hyperliquid spot live configs should validate");
        assert_eq!(
            configs["btc/usdc@candles@hyperliquid"].exchange,
            "hyperliquid"
        );
        assert_eq!(
            configs["btc/usdc@candles@hyperliquid"].provider,
            ProviderKind::Direct
        );

        let oi_manifest = manifest(vec![ScriptSource::Oi]);
        let oi = parse_source_configs(&["btc/usdc@oi@hyperliquid".to_string()])
            .expect("spot OI selector parses before capability validation");
        let error = validate_source_configs_for_run(&oi_manifest, &oi)
            .expect_err("spot must not expose open interest");
        assert!(error.to_string().contains("requires a futures exchange"));
    }

    #[test]
    fn validates_dynamic_hyperliquid_outcome_bindings() {
        let live_manifest = manifest(vec![
            ScriptSource::Candles,
            ScriptSource::Orderbook,
            ScriptSource::Trades,
            ScriptSource::Vd,
            ScriptSource::Volumes,
        ]);
        let configs = parse_source_configs(&[
            "1001:0@candles@hyperliquid-outcomes:timeframe=60".to_string(),
            "1001:0@orderbook@hyperliquid-outcomes:depth=20".to_string(),
            "1001:0@trades@hyperliquid-outcomes".to_string(),
            "1001:0@vd@hyperliquid-outcomes".to_string(),
            "1001:0@volumes@hyperliquid-outcomes:timeframe=60".to_string(),
        ])
        .expect("outcome selectors should preserve the side separator");

        validate_source_configs_for_run(&live_manifest, &configs)
            .expect("outcome live configs should validate");
        assert_eq!(
            configs["1001:0@candles@hyperliquid-outcomes"].market_symbol(),
            "1001:0"
        );
        assert_eq!(
            configs["1001:0@orderbook@hyperliquid-outcomes"].depth,
            Some(20)
        );

        let historical_manifest = manifest(vec![ScriptSource::Candles, ScriptSource::Volumes]);
        validate_source_configs(
            &historical_manifest,
            &parse_source_configs(&[
                "1001:1@candles@hyperliquid-outcomes:timeframe=60".to_string(),
                "1001:1@volumes@hyperliquid-outcomes:timeframe=60".to_string(),
            ])
            .expect("historical outcome selectors should parse"),
        )
        .expect("outcome candles and volume should support backtests");

        assert!(parse_source_configs(&["1001:2@trades@hyperliquid-outcomes".to_string()]).is_err());
    }

    #[test]
    fn validates_historical_binance_spot_and_futures_bindings() {
        let manifest = manifest(vec![ScriptSource::Candles, ScriptSource::Volumes]);
        let configs = parse_source_configs(&[
            "btc/usdt@candles@binance:timeframe=60".to_string(),
            "btc@volumes@binancef:timeframe=300".to_string(),
        ])
        .expect("standalone Binance selectors should parse");

        validate_source_configs(&manifest, &configs)
            .expect("historical Binance configs should validate");
        assert_eq!(
            configs["btc/usdt@candles@binance"].provider,
            ProviderKind::Direct
        );
        assert_eq!(
            configs["btc@volumes@binancef"].provider,
            ProviderKind::Direct
        );
        validate_source_configs_for_run(&manifest, &configs)
            .expect_err("live Binance streams are not implemented");
    }

    #[test]
    fn rejects_open_interest_on_a_spot_exchange() {
        let manifest = manifest(vec![ScriptSource::Oi]);
        let configs = parse_source_configs(&["btc/usdt@oi@binance@mmt:timeframe=60".to_string()])
            .expect("spot OI binding parses before capability validation");

        let error = validate_source_configs_for_run(&manifest, &configs)
            .expect_err("spot exchange must not provide open interest");
        assert!(error.to_string().contains("requires a futures exchange"));
    }

    #[test]
    fn rejects_bulk_historical_sources_that_do_not_exist() {
        let manifest = manifest(vec![ScriptSource::Orderbook]);
        let configs = parse_source_configs(&["btc@orderbook@bulkf:depth=50".to_string()])
            .expect("parse book binding");

        let error = validate_source_configs(&manifest, &configs)
            .expect_err("historical BULK orderbook should fail");
        assert!(error.to_string().contains("historical orderbooks"));
    }

    #[test]
    fn rejects_raw_trades_for_backtests() {
        for selector in [
            "btc@trades@binancef@mmt",
            "btc@trades@bulkf",
            "btc@trades@hyperliquidf",
        ] {
            let manifest = manifest(vec![ScriptSource::Trades]);
            let configs = parse_source_configs(&[selector.to_string()]).expect("parse trades");
            let error = validate_source_configs(&manifest, &configs)
                .expect_err("raw trades must remain live-only");
            assert!(error.to_string().contains("live-only"));
        }
    }
}
