use std::collections::BTreeMap;

use anyhow::{Result, bail};
use serde_json::{Map, Value};

use super::manifest::{InputType, ScriptInputSchema, ScriptManifest};

pub fn parse_kv_inputs(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for value in values {
        let Some((key, raw)) = value.split_once('=') else {
            bail!("--input must use key=value, got `{value}`");
        };
        if key.trim().is_empty() {
            bail!("--input key cannot be empty");
        }
        if parsed.insert(key.to_string(), raw.to_string()).is_some() {
            bail!("duplicate --input key `{key}`");
        }
    }
    Ok(parsed)
}

pub fn resolve_inputs(
    manifest: &ScriptManifest,
    raw_inputs: &BTreeMap<String, String>,
) -> Result<Value> {
    for key in raw_inputs.keys() {
        if !manifest.inputs.contains_key(key) {
            bail!("unknown script input `{key}`");
        }
    }

    let mut out = Map::new();
    for (key, schema) in &manifest.inputs {
        if let Some(raw) = raw_inputs.get(key) {
            out.insert(key.clone(), coerce_value(raw, schema)?);
            continue;
        }
        if let Some(default) = &schema.default {
            out.insert(key.clone(), default.clone());
            continue;
        }
        if schema.required {
            bail!("missing required script input `{key}`");
        }
    }

    Ok(Value::Object(out))
}

fn coerce_value(raw: &str, schema: &ScriptInputSchema) -> Result<Value> {
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
    use crate::scripting::manifest::{ScriptInputSchema, ScriptManifest, ScriptMode, ScriptSource};

    #[test]
    fn resolves_required_and_default_inputs() {
        let manifest = ScriptManifest {
            name: "buy-pressure".to_string(),
            version: "1".to_string(),
            source: ScriptSource::Candles,
            modes: vec![ScriptMode::Window],
            description: None,
            lookback: None,
            inputs: BTreeMap::from([
                (
                    "min_vbuy".to_string(),
                    ScriptInputSchema {
                        input_type: InputType::Number,
                        required: true,
                        default: None,
                        description: None,
                    },
                ),
                (
                    "enabled".to_string(),
                    ScriptInputSchema {
                        input_type: InputType::Boolean,
                        required: false,
                        default: Some(Value::Bool(true)),
                        description: None,
                    },
                ),
            ]),
        };

        let raw = BTreeMap::from([("min_vbuy".to_string(), "50000".to_string())]);
        let value = resolve_inputs(&manifest, &raw).expect("inputs resolve");

        assert_eq!(value["min_vbuy"], 50000.0);
        assert_eq!(value["enabled"], true);
    }
}
