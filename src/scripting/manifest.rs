use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::limits::SCRIPT_MAX_LOOKBACK_CANDLES;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScriptSource {
    Candles,
    Orderbook,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScriptMode {
    Window,
    Stream,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    String,
    Number,
    Boolean,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScriptInputSchema {
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
pub struct ScriptManifest {
    pub name: String,
    pub version: String,
    pub source: ScriptSource,
    pub modes: Vec<ScriptMode>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub lookback: Option<usize>,
    #[serde(default)]
    pub inputs: BTreeMap<String, ScriptInputSchema>,
}

impl ScriptManifest {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("script.name is required");
        }
        if self.version.trim() != "1" {
            bail!("script.version must be \"1\"");
        }
        if self.modes.is_empty() {
            bail!("script.modes must not be empty");
        }
        if matches!(self.lookback, Some(0)) {
            bail!("script.lookback must be >= 1");
        }
        if let Some(lookback) = self.lookback
            && lookback > SCRIPT_MAX_LOOKBACK_CANDLES
        {
            bail!("script.lookback must be <= {SCRIPT_MAX_LOOKBACK_CANDLES}");
        }
        for key in self.inputs.keys() {
            if !is_valid_input_name(key) {
                bail!("script.inputs key `{key}` must be snake_case");
            }
            if is_reserved_input_name(key) {
                bail!("script.inputs key `{key}` is reserved by Market Lab");
            }
        }
        for (key, schema) in &self.inputs {
            if schema.required && schema.default.is_some() {
                bail!("script.inputs.{key} cannot be required and also have a default");
            }
            if let Some(default) = &schema.default {
                validate_default_value(key, &schema.input_type, default)?;
            }
        }
        Ok(())
    }

    pub fn supports_mode(&self, mode: ScriptMode) -> bool {
        self.modes.contains(&mode)
    }
}

const RESERVED_INPUT_NAMES: &[&str] = &[
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

fn is_reserved_input_name(name: &str) -> bool {
    RESERVED_INPUT_NAMES.contains(&name)
}

fn is_valid_input_name(name: &str) -> bool {
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
        bail!("script.inputs.{key}.default does not match declared type");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_requires_name() {
        let manifest = ScriptManifest {
            name: String::new(),
            version: "1".to_string(),
            source: ScriptSource::Candles,
            modes: vec![ScriptMode::Window],
            description: None,
            lookback: None,
            inputs: BTreeMap::new(),
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_bad_input_name() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "min-vbuy".to_string(),
            ScriptInputSchema {
                input_type: InputType::Number,
                required: true,
                default: None,
                description: None,
            },
        );
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            source: ScriptSource::Candles,
            modes: vec![ScriptMode::Window],
            description: None,
            lookback: None,
            inputs,
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_reserved_input_name() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "timeframe".to_string(),
            ScriptInputSchema {
                input_type: InputType::Number,
                required: true,
                default: None,
                description: None,
            },
        );
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            source: ScriptSource::Candles,
            modes: vec![ScriptMode::Window],
            description: None,
            lookback: None,
            inputs,
        };
        let err = manifest.validate().expect_err("reserved input should fail");
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn manifest_rejects_unknown_source() {
        let err = serde_json::from_value::<ScriptManifest>(serde_json::json!({
            "name": "x",
            "version": "1",
            "source": "xyz",
            "modes": ["window"]
        }))
        .expect_err("unknown source should fail");

        assert!(err.to_string().contains("unknown variant"));
    }

    #[test]
    fn manifest_rejects_zero_lookback() {
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            source: ScriptSource::Candles,
            modes: vec![ScriptMode::Window],
            description: None,
            lookback: Some(0),
            inputs: BTreeMap::new(),
        };

        let err = manifest.validate().expect_err("zero lookback should fail");
        assert!(err.to_string().contains("lookback"));
    }

    #[test]
    fn manifest_rejects_lookback_above_max() {
        let manifest = ScriptManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            source: ScriptSource::Candles,
            modes: vec![ScriptMode::Window],
            description: None,
            lookback: Some(SCRIPT_MAX_LOOKBACK_CANDLES + 1),
            inputs: BTreeMap::new(),
        };

        let err = manifest.validate().expect_err("large lookback should fail");
        assert!(err.to_string().contains("lookback"));
    }
}
