use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StudySource {
    Candles,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StudyMode {
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
pub struct StudyInputSchema {
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
pub struct StudyManifest {
    pub name: String,
    pub version: String,
    pub source: StudySource,
    pub modes: Vec<StudyMode>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub inputs: BTreeMap<String, StudyInputSchema>,
}

impl StudyManifest {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("study.name is required");
        }
        if self.version.trim() != "1" {
            bail!("study.version must be \"1\"");
        }
        if self.modes.is_empty() {
            bail!("study.modes must not be empty");
        }
        for key in self.inputs.keys() {
            if !is_valid_input_name(key) {
                bail!("study.inputs key `{key}` must be snake_case");
            }
            if is_reserved_input_name(key) {
                bail!("study.inputs key `{key}` is reserved by Market Lab");
            }
        }
        for (key, schema) in &self.inputs {
            if schema.required && schema.default.is_some() {
                bail!("study.inputs.{key} cannot be required and also have a default");
            }
            if let Some(default) = &schema.default {
                validate_default_value(key, &schema.input_type, default)?;
            }
        }
        Ok(())
    }

    pub fn supports_mode(&self, mode: StudyMode) -> bool {
        self.modes.iter().any(|candidate| *candidate == mode)
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
        bail!("study.inputs.{key}.default does not match declared type");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_requires_name() {
        let manifest = StudyManifest {
            name: String::new(),
            version: "1".to_string(),
            source: StudySource::Candles,
            modes: vec![StudyMode::Window],
            description: None,
            inputs: BTreeMap::new(),
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_bad_input_name() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "min-vbuy".to_string(),
            StudyInputSchema {
                input_type: InputType::Number,
                required: true,
                default: None,
                description: None,
            },
        );
        let manifest = StudyManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            source: StudySource::Candles,
            modes: vec![StudyMode::Window],
            description: None,
            inputs,
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_reserved_input_name() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "timeframe".to_string(),
            StudyInputSchema {
                input_type: InputType::Number,
                required: true,
                default: None,
                description: None,
            },
        );
        let manifest = StudyManifest {
            name: "x".to_string(),
            version: "1".to_string(),
            source: StudySource::Candles,
            modes: vec![StudyMode::Window],
            description: None,
            inputs,
        };
        let err = manifest.validate().expect_err("reserved input should fail");
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn manifest_rejects_unknown_source() {
        let err = serde_json::from_value::<StudyManifest>(serde_json::json!({
            "name": "x",
            "version": "1",
            "source": "xyz",
            "modes": ["window"]
        }))
        .expect_err("unknown source should fail");

        assert!(err.to_string().contains("unknown variant"));
    }
}
