use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptOutput {
    pub metrics: Value,
    #[serde(default)]
    pub signal: Value,
    #[serde(default)]
    pub intent: Value,
    #[serde(default)]
    pub meta: Value,
}

impl ScriptOutput {
    pub fn from_json(value: Value) -> Result<Self> {
        let output: Self =
            serde_json::from_value(value).map_err(|err| anyhow::anyhow!(err.to_string()))?;
        if !output.metrics.is_object() {
            bail!("script return `metrics` must be an object");
        }
        if !output.signal.is_null() && !output.signal.is_object() {
            bail!("script return `signal` must be an object when provided");
        }
        if !output.intent.is_null() && !output.intent.is_object() {
            bail!("script return `intent` must be an object when provided");
        }
        if !output.meta.is_null() && !output.meta.is_object() {
            bail!("script return `meta` must be an object when provided");
        }
        Ok(Self {
            metrics: output.metrics,
            signal: normalize_object(output.signal),
            intent: normalize_object(output.intent),
            meta: normalize_meta(output.meta),
        })
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ScriptOutput;

    #[test]
    fn accepts_strategy_like_output() {
        let output = ScriptOutput::from_json(json!({
            "metrics": { "close": 100.0 },
            "signal": {
                "event": "cross_up",
                "side": "buy",
                "triggered": true
            },
            "intent": {
                "type": "order",
                "side": "buy",
                "order_type": "market",
                "notional": 1000
            }
        }))
        .expect("strategy-like output should decode");

        assert_eq!(output.signal["side"], "buy");
        assert_eq!(output.intent["type"], "order");
    }

    #[test]
    fn rejects_non_object_signal() {
        let err = ScriptOutput::from_json(json!({
            "metrics": {},
            "signal": "buy"
        }))
        .expect_err("string signal should fail");

        assert!(err.to_string().contains("signal"));
    }
}
