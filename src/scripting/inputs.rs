use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde_json::{Map, Value, json};

use crate::domain::enums::ProviderKind;
use crate::providers::bulk::market_data::timeframe_from_seconds as bulk_timeframe_from_seconds;

use super::manifest::{InputType, ScriptManifest, ScriptParamSchema, ScriptSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConfig {
    pub selector: String,
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
        source: ScriptSource,
        provider: ProviderKind,
        exchange: String,
        position: usize,
    ) -> Self {
        Self {
            selector,
            source,
            provider,
            exchange,
            position,
            timeframe: None,
            depth: None,
            bucket: None,
        }
    }

    pub fn require_timeframe(&self, source: &ScriptSource) -> Result<u32> {
        self.timeframe.ok_or_else(|| {
            anyhow::anyhow!(
                "--source {}:timeframe=<seconds> is required",
                source.as_str()
            )
        })
    }

    pub fn depth_or_default(&self) -> u16 {
        self.depth.unwrap_or(100)
    }

    pub fn require_bucket(&self, source: &ScriptSource) -> Result<u8> {
        self.bucket.ok_or_else(|| {
            anyhow::anyhow!("--source {}:bucket=<1..=11> is required", source.as_str())
        })
    }
}

pub type SourceConfigs = BTreeMap<String, SourceConfig>;
pub type RawParamValues = BTreeMap<String, String>;

pub fn parse_source_configs(values: &[String]) -> Result<SourceConfigs> {
    let mut configs = SourceConfigs::new();

    for (position, value) in values.iter().enumerate() {
        let (binding, options) = value
            .split_once(':')
            .map_or((value.as_str(), ""), |(binding, options)| {
                (binding, options)
            });
        let (selector, source, provider, exchange) = parse_source_selector(binding)?;
        let config = configs.entry(selector.clone()).or_insert_with(|| {
            SourceConfig::new(
                selector.clone(),
                source.clone(),
                provider,
                exchange.clone(),
                position,
            )
        });
        if config.provider != provider || config.exchange != exchange {
            bail!(
                "--source `{selector}` cannot bind both {} and {exchange}",
                config.exchange
            );
        }
        if options.is_empty() {
            continue;
        }
        for option in options.split(',') {
            let Some((key, raw_value)) = option.split_once('=') else {
                bail!("--source option must use key=value, got `{option}` in `{value}`");
            };
            if key.trim().is_empty() {
                bail!("--source key cannot be empty");
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
                _ => bail!("unknown --source {selector}:{key}"),
            };
            if duplicate {
                bail!("duplicate --source {selector}:{key}");
            }
        }
    }

    reject_duplicate_resolved_sources(&configs)?;

    Ok(configs)
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
        .map(|config| source_provider_name(config.provider))
        .collect::<Vec<_>>();
    providers.sort_unstable();
    providers.dedup();
    providers.join(",")
}

pub fn source_provider_name(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Mmt => "mmt",
        ProviderKind::Bulk => "bulk",
        ProviderKind::MarketLab => "marketlab",
    }
}

pub fn source_configs_payload(configs: &SourceConfigs) -> Value {
    let mut payload = Map::new();
    let mut configs = configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    for config in configs {
        payload.insert(
            config.selector.clone(),
            json!({
                "type": config.source.as_str(),
                "provider": source_provider_name(config.provider),
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
    for config in configs.values() {
        if !manifest.sources.contains(&config.source) {
            bail!(
                "--source {} is not listed in script.sources",
                config.selector
            );
        }
    }

    for source in &manifest.sources {
        if !configs.values().any(|config| &config.source == source) {
            bail!("missing --source config for {}", source.as_str());
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
            "--source {} requires a futures exchange; `{}` is spot",
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
            ScriptSource::Orderbook => {
                if historical {
                    config.require_timeframe(&config.source)?;
                }
                if config.depth_or_default() == 0 {
                    bail!("--source {}:depth must be >= 1", config.selector);
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
        ProviderKind::Bulk => match &config.source {
            ScriptSource::Candles => {
                let timeframe = config.require_timeframe(&config.source)?;
                if historical {
                    bulk_timeframe_from_seconds(timeframe)?;
                }
            }
            ScriptSource::Volumes => {
                let timeframe = config.require_timeframe(&config.source)?;
                bulk_timeframe_from_seconds(timeframe)?;
            }
            ScriptSource::Orderbook if historical => {
                bail!("BULK does not provide historical orderbooks for script backtests");
            }
            ScriptSource::Vd if historical => {
                bail!("BULK does not provide historical volume delta for script backtests");
            }
            ScriptSource::Oi if historical => {
                bail!("BULK does not provide historical open interest for script backtests");
            }
            ScriptSource::Orderbook => {
                if config.timeframe.is_some() {
                    bail!("BULK live orderbook does not use a timeframe");
                }
            }
            ScriptSource::Vd => {
                if config.timeframe.is_some() || config.bucket.is_some() {
                    bail!("BULK live volume delta is trade-derived; omit timeframe and bucket");
                }
            }
            ScriptSource::Oi => {
                if config.timeframe.is_some() {
                    bail!("BULK live open interest is snapshot-based; omit timeframe");
                }
            }
        },
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
        "vd" => Ok(ScriptSource::Vd),
        "oi" => Ok(ScriptSource::Oi),
        "volumes" => Ok(ScriptSource::Volumes),
        _ => bail!("unknown script source `{source}`"),
    }
}

fn parse_source_selector(raw: &str) -> Result<(String, ScriptSource, ProviderKind, String)> {
    let parts = raw.split('@').collect::<Vec<_>>();
    let (source_raw, exchange, provider) = match parts.as_slice() {
        [source, provider] => {
            let provider = parse_source_provider(provider)?;
            if provider == ProviderKind::Mmt {
                bail!(
                    "MMT sources require source@exchange@mmt, for example `{source}@binancef@mmt`"
                );
            }
            (*source, provider_name_for_exchange(provider), provider)
        }
        [source, exchange, provider] => {
            let provider = parse_source_provider(provider)?;
            if provider != ProviderKind::Mmt {
                bail!(
                    "{} sources must use `source@{}`",
                    source_provider_name(provider),
                    source_provider_name(provider)
                );
            }
            (*source, *exchange, provider)
        }
        _ => bail!("--source `{raw}` must use source@provider or source@exchange@provider"),
    };
    let source = parse_source(source_raw)?;
    let exchange = exchange.trim().to_ascii_lowercase();
    validate_exchange_name(&exchange)?;
    let selector = match provider {
        ProviderKind::Mmt => format!("{}@{exchange}@mmt", source.as_str()),
        ProviderKind::Bulk => format!("{}@bulk", source.as_str()),
        ProviderKind::MarketLab => unreachable!(),
    };
    Ok((selector, source, provider, exchange))
}

fn parse_source_provider(raw: &str) -> Result<ProviderKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "mmt" => Ok(ProviderKind::Mmt),
        "bulk" => Ok(ProviderKind::Bulk),
        other => bail!("unsupported script source provider `{other}`"),
    }
}

fn provider_name_for_exchange(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Bulk => "bulk",
        ProviderKind::Mmt => "mmt",
        ProviderKind::MarketLab => "marketlab",
    }
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
            "candles@okx@mmt:timeframe=60".to_string(),
            "orderbook@binancef@mmt:timeframe=60,depth=50".to_string(),
            "vd@hyperliquid@mmt:timeframe=60,bucket=1".to_string(),
            "oi@binancef@mmt:timeframe=60".to_string(),
            "volumes@okx@mmt:timeframe=60".to_string(),
        ])
        .unwrap();
        assert_eq!(configs["candles@okx@mmt"].exchange, "okx");
        assert_eq!(configs["candles@okx@mmt"].provider, ProviderKind::Mmt);
        assert_eq!(configs["orderbook@binancef@mmt"].depth, Some(50));
        assert_eq!(configs["vd@hyperliquid@mmt"].bucket, Some(1));
        assert_eq!(configs["oi@binancef@mmt"].timeframe, Some(60));
        assert_eq!(configs["volumes@okx@mmt"].timeframe, Some(60));
    }

    #[test]
    fn validates_two_candle_exchanges() {
        let manifest = manifest(vec![ScriptSource::Candles]);
        let configs = parse_source_configs(&[
            "candles@binancef@mmt:timeframe=60".to_string(),
            "candles@okx@mmt:timeframe=300".to_string(),
        ])
        .expect("qualified bindings should parse");

        validate_source_configs(&manifest, &configs).expect("backtest configs should validate");
        validate_source_configs_for_run(&manifest, &configs).expect("live configs should validate");
        assert_eq!(source_exchange_label(&configs), "binancef,okx");
        assert_eq!(configs["candles@okx@mmt"].timeframe, Some(300));
    }

    #[test]
    fn live_candles_accept_custom_second_timeframes() {
        let manifest = manifest(vec![ScriptSource::Candles]);
        for selector in [
            "candles@binancef@mmt:timeframe=1",
            "candles@bulk:timeframe=1",
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
        assert!(error.to_string().contains("source@provider"));
    }

    #[test]
    fn validates_bare_bulk_bindings_for_snapshot_sources() {
        let manifest = manifest(vec![
            ScriptSource::Candles,
            ScriptSource::Orderbook,
            ScriptSource::Vd,
            ScriptSource::Oi,
        ]);
        let configs = parse_source_configs(&[
            "candles@bulk:timeframe=60".to_string(),
            "orderbook@bulk:depth=50".to_string(),
            "vd@bulk".to_string(),
            "oi@bulk".to_string(),
        ])
        .unwrap();

        validate_source_configs_for_run(&manifest, &configs)
            .expect("BULK live configs should validate");
        assert!(configs.contains_key("vd@bulk"));
        assert!(configs.contains_key("oi@bulk"));
    }

    #[test]
    fn rejects_open_interest_on_a_spot_exchange() {
        let manifest = manifest(vec![ScriptSource::Oi]);
        let configs = parse_source_configs(&["oi@binance@mmt:timeframe=60".to_string()])
            .expect("spot OI binding parses before capability validation");

        let error = validate_source_configs_for_run(&manifest, &configs)
            .expect_err("spot exchange must not provide open interest");
        assert!(error.to_string().contains("requires a futures exchange"));
    }

    #[test]
    fn rejects_bulk_historical_sources_that_do_not_exist() {
        let manifest = manifest(vec![ScriptSource::Orderbook]);
        let configs = parse_source_configs(&["orderbook@bulk:depth=50".to_string()])
            .expect("parse book binding");

        let error = validate_source_configs(&manifest, &configs)
            .expect_err("historical BULK orderbook should fail");
        assert!(error.to_string().contains("historical orderbooks"));
    }
}
