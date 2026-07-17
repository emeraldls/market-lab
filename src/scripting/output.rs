use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptOutput {
    #[serde(default)]
    pub metrics: Value,
    #[serde(default)]
    pub meta: Value,
}

impl ScriptOutput {
    pub fn from_json(value: Value) -> Result<Self> {
        if value.is_null() {
            return Ok(Self::empty());
        }
        let output: Self =
            serde_json::from_value(value).map_err(|err| anyhow::anyhow!(err.to_string()))?;
        if !output.metrics.is_null() && !output.metrics.is_object() {
            bail!("script return `metrics` must be an object when provided");
        }
        if !output.meta.is_null() && !output.meta.is_object() {
            bail!("script return `meta` must be an object when provided");
        }
        Ok(Self {
            metrics: normalize_object(output.metrics),
            meta: normalize_meta(output.meta),
        })
    }

    pub fn empty() -> Self {
        Self {
            metrics: Value::Object(Default::default()),
            meta: Value::Object(Default::default()),
        }
    }

    pub fn is_empty(&self) -> bool {
        is_empty_object(&self.metrics) && is_empty_object(&self.meta)
    }
}

fn normalize_object(value: Value) -> Value {
    if value.is_null() {
        Value::Object(Default::default())
    } else {
        value
    }
}

fn normalize_meta(meta: Value) -> Value {
    normalize_object(meta)
}

fn is_empty_object(value: &Value) -> bool {
    matches!(value, Value::Object(map) if map.is_empty())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::ScriptOutput;

    #[test]
    fn accepts_optional_diagnostics() {
        let output = ScriptOutput::from_json(json!({
            "metrics": { "close": 100.0 },
            "meta": { "note": "crossed" }
        }))
        .expect("diagnostics should decode");

        assert_eq!(output.metrics["close"], 100.0);
        assert_eq!(output.meta["note"], "crossed");
        assert!(!output.is_empty());
    }

    #[test]
    fn accepts_no_return_value() {
        assert!(
            ScriptOutput::from_json(Value::Null)
                .expect("no return should be valid")
                .is_empty()
        );
        assert!(
            ScriptOutput::from_json(json!({}))
                .expect("empty return should be valid")
                .is_empty()
        );
    }

    #[test]
    fn rejects_removed_signal_output() {
        let err = ScriptOutput::from_json(json!({
            "signal": { "side": "buy" }
        }))
        .expect_err("signal output should fail");

        assert!(err.to_string().contains("signal"));
    }
}
