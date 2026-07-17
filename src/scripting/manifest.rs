use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::limits::SCRIPT_MAX_LOOKBACK_CANDLES;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum ScriptSource {
    Candles,
    Orderbook,
    Vd,
    Oi,
    Volumes,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    String,
    Number,
    Boolean,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScriptParamSchema {
    #[serde(rename = "type")]
    pub input_type: InputType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptManifest {
    pub name: String,
    pub version: String,
    pub sources: Vec<ScriptSource>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub lookback: Option<usize>,
    #[serde(default)]
    pub params: BTreeMap<String, ScriptParamSchema>,
}

impl ScriptManifest {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("script.name is required");
        }
        if self.version.trim() != "1" {
            bail!("script.version must be \"1\"");
        }
        if self.sources.is_empty() {
            bail!("script.sources must not be empty");
        }
        if matches!(self.lookback, Some(0)) {
            bail!("script.lookback must be >= 1");
        }
        if let Some(lookback) = self.lookback
            && lookback > SCRIPT_MAX_LOOKBACK_CANDLES
        {
            bail!("script.lookback must be <= {SCRIPT_MAX_LOOKBACK_CANDLES}");
        }
        for (key, schema) in &self.params {
            if !is_valid_param_name(key) {
                bail!("script.params key `{key}` must be snake_case");
            }
            if is_reserved_param_name(key) {
                bail!("script.params key `{key}` is reserved by Market Lab");
            }
            if schema.required && schema.default.is_some() {
                bail!("script.params.{key} cannot be required and also have a default");
            }
            if let Some(default) = &schema.default {
                validate_default_value(key, &schema.input_type, default)?;
            }
        }
        Ok(())
    }

    pub fn source_names(&self) -> String {
        self.sources
            .iter()
            .map(ScriptSource::as_str)
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl ScriptSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScriptSource::Candles => "candles",
            ScriptSource::Orderbook => "orderbook",
            ScriptSource::Vd => "vd",
            ScriptSource::Oi => "oi",
            ScriptSource::Volumes => "volumes",
        }
    }
}

const RESERVED_PARAM_NAMES: &[&str] = &[
    "at",
    "bucket",
    "buffer_size",
    "depth",
    "exchange",
    "from",
    "interval_ms",
    "output",
    "provider",
    "source",
    "stream",
    "symbol",
    "timeframe",
    "to",
    "verbose",
];

fn is_reserved_param_name(name: &str) -> bool {
    RESERVED_PARAM_NAMES.contains(&name)
}

fn is_valid_param_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}

fn validate_default_value(
    key: &str,
    input_type: &InputType,
    value: &serde_json::Value,
) -> Result<()> {
    let ok = match input_type {
        InputType::String => value.is_string(),
        InputType::Number => value.is_number(),
        InputType::Boolean => value.is_boolean(),
    };
    if !ok {
        bail!("script.params.{key}.default does not match declared type");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle_sources() -> Vec<ScriptSource> {
        vec![ScriptSource::Candles]
    }

    #[test]
    fn manifest_requires_name() {
        let manifest = ScriptManifest {
            name: String::new(),
            version: "1".to_string(),
            sources: candle_sources(),
            description: None,
            lookback: None,
            params: BTreeMap::new(),
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_bad_param_name() {
        let params = BTreeMap::from([(
            "min-vbuy".to_string(),
            ScriptParamSchema {
                input_type: InputType::Number,
                required: true,
                default: None,
                description: None,
            },
        )]);
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            sources: candle_sources(),
            description: None,
            lookback: None,
            params,
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_reserved_param_name() {
        let params = BTreeMap::from([(
            "timeframe".to_string(),
            ScriptParamSchema {
                input_type: InputType::Number,
                required: true,
                default: None,
                description: None,
            },
        )]);
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            sources: candle_sources(),
            description: None,
            lookback: None,
            params,
        };
        let err = manifest.validate().expect_err("reserved param should fail");
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn manifest_rejects_unknown_source() {
        let err = serde_json::from_value::<ScriptManifest>(serde_json::json!({
            "name": "x",
            "version": "1",
            "sources": ["xyz"]
        }))
        .expect_err("unknown source should fail");

        assert!(err.to_string().contains("unknown variant"));
    }

    #[test]
    fn manifest_rejects_unknown_fields() {
        let err = serde_json::from_value::<ScriptManifest>(serde_json::json!({
            "name": "x",
            "version": "1",
            "sources": ["candles"],
            "anything": true
        }))
        .expect_err("unknown field should fail");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn manifest_rejects_zero_lookback() {
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            sources: candle_sources(),
            description: None,
            lookback: Some(0),
            params: BTreeMap::new(),
        };

        let err = manifest.validate().expect_err("zero lookback should fail");
        assert!(err.to_string().contains("lookback"));
    }

    #[test]
    fn manifest_rejects_lookback_above_max() {
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            sources: candle_sources(),
            description: None,
            lookback: Some(SCRIPT_MAX_LOOKBACK_CANDLES + 1),
            params: BTreeMap::new(),
        };

        let err = manifest.validate().expect_err("large lookback should fail");
        assert!(err.to_string().contains("lookback"));
    }
}
