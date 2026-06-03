use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde_json::{Map, Value};

use super::manifest::{InputType, ScriptManifest, ScriptParamSchema, ScriptSource};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceConfig {
    pub timeframe: Option<u32>,
    pub depth: Option<u16>,
}

impl SourceConfig {
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
}

pub type SourceConfigs = BTreeMap<ScriptSource, SourceConfig>;
pub type RawScopedValues = BTreeMap<ScriptSource, BTreeMap<String, String>>;

pub fn parse_source_configs(values: &[String]) -> Result<SourceConfigs> {
    let raw = parse_scoped_values(values, "--source")?;
    let mut configs = SourceConfigs::new();

    for (source, entries) in raw {
        let mut config = SourceConfig::default();
        for (key, raw_value) in entries {
            match (source.clone(), key.as_str()) {
                (ScriptSource::Candles, "timeframe") => {
                    config.timeframe = Some(parse_positive_u32(&raw_value, "timeframe")?);
                }
                (ScriptSource::Orderbook, "timeframe") => {
                    config.timeframe = Some(parse_positive_u32(&raw_value, "timeframe")?);
                }
                (ScriptSource::Orderbook, "depth") => {
                    config.depth = Some(parse_positive_u16(&raw_value, "depth")?);
                }
                _ => bail!("unknown --source {}:{}", source.as_str(), key),
            }
        }
        configs.insert(source, config);
    }

    Ok(configs)
}

pub fn parse_param_values(values: &[String]) -> Result<RawScopedValues> {
    parse_scoped_values(values, "--param")
}

pub fn validate_source_configs(manifest: &ScriptManifest, configs: &SourceConfigs) -> Result<()> {
    for source in configs.keys() {
        if !manifest.sources.contains(source) {
            bail!(
                "--source {} is not listed in script.sources",
                source.as_str()
            );
        }
    }

    for source in &manifest.sources {
        let config = configs
            .get(source)
            .ok_or_else(|| anyhow::anyhow!("missing --source config for {}", source.as_str()))?;
        match source {
            ScriptSource::Candles => {
                config.require_timeframe(source)?;
            }
            ScriptSource::Orderbook => {
                config.require_timeframe(source)?;
                if config.depth_or_default() == 0 {
                    bail!("--source orderbook:depth must be >= 1");
                }
            }
        }
    }

    Ok(())
}

pub fn resolve_params(manifest: &ScriptManifest, raw_params: &RawScopedValues) -> Result<Value> {
    for source in raw_params.keys() {
        if !manifest.sources.contains(source) {
            bail!(
                "--param {} is not listed in script.sources",
                source.as_str()
            );
        }
    }

    let mut out = Map::new();
    for source in &manifest.sources {
        let schema = manifest.params.get(source);
        let raw_for_source = raw_params.get(source);
        if let Some(raw_entries) = raw_for_source {
            for key in raw_entries.keys() {
                if !schema.is_some_and(|schema| schema.contains_key(key)) {
                    bail!("unknown script param `{}:{key}`", source.as_str());
                }
            }
        }

        let mut source_out = Map::new();
        if let Some(schema) = schema {
            for (key, param_schema) in schema {
                if let Some(raw) = raw_for_source.and_then(|values| values.get(key)) {
                    source_out.insert(key.clone(), coerce_value(raw, param_schema)?);
                    continue;
                }
                if let Some(default) = &param_schema.default {
                    source_out.insert(key.clone(), default.clone());
                    continue;
                }
                if param_schema.required {
                    bail!("missing required script param `{}:{key}`", source.as_str());
                }
            }
        }
        out.insert(source.as_str().to_string(), Value::Object(source_out));
    }

    Ok(Value::Object(out))
}

fn parse_scoped_values(values: &[String], flag: &str) -> Result<RawScopedValues> {
    let mut parsed = RawScopedValues::new();
    for value in values {
        let Some((scope_key, raw)) = value.split_once('=') else {
            bail!("{flag} must use source:key=value, got `{value}`");
        };
        let Some((source_raw, key)) = scope_key.split_once(':') else {
            bail!("{flag} must use source:key=value, got `{value}`");
        };
        if key.trim().is_empty() {
            bail!("{flag} key cannot be empty");
        }
        let source = parse_source(source_raw)?;
        let source_values = parsed.entry(source.clone()).or_default();
        if source_values
            .insert(key.to_string(), raw.to_string())
            .is_some()
        {
            bail!("duplicate {flag} {}:{key}", source.as_str());
        }
    }
    Ok(parsed)
}

fn parse_source(source: &str) -> Result<ScriptSource> {
    match source {
        "candles" => Ok(ScriptSource::Candles),
        "orderbook" => Ok(ScriptSource::Orderbook),
        _ => bail!("unknown script source `{source}`"),
    }
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
    use crate::scripting::manifest::{ScriptManifest, ScriptMode, ScriptParamSchema};

    #[test]
    fn resolves_required_and_default_params() {
        let manifest = ScriptManifest {
            name: "buy-pressure".to_string(),
            version: "1".to_string(),
            sources: vec![ScriptSource::Candles],
            modes: vec![ScriptMode::Window],
            clock: None,
            description: None,
            lookback: None,
            params: BTreeMap::from([(
                ScriptSource::Candles,
                BTreeMap::from([
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
            )]),
        };

        let raw = parse_param_values(&["candles:min_vbuy=50000".to_string()]).unwrap();
        let value = resolve_params(&manifest, &raw).expect("params resolve");

        assert_eq!(value["candles"]["min_vbuy"], 50000.0);
        assert_eq!(value["candles"]["enabled"], true);
    }

    #[test]
    fn parses_source_config() {
        let configs = parse_source_configs(&[
            "candles:timeframe=60".to_string(),
            "orderbook:timeframe=60".to_string(),
            "orderbook:depth=50".to_string(),
        ])
        .unwrap();
        assert_eq!(configs[&ScriptSource::Candles].timeframe, Some(60));
        assert_eq!(configs[&ScriptSource::Orderbook].depth, Some(50));
    }
}
