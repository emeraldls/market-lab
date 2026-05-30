use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptStudyOutput {
    pub metrics: Value,
    #[serde(default)]
    pub meta: Value,
}

impl ScriptStudyOutput {
    pub fn from_json(value: Value) -> Result<Self> {
        let output: Self =
            serde_json::from_value(value).map_err(|err| anyhow::anyhow!(err.to_string()))?;
        if !output.metrics.is_object() {
            bail!("study return `metrics` must be an object");
        }
        if !output.meta.is_null() && !output.meta.is_object() {
            bail!("study return `meta` must be an object when provided");
        }
        Ok(Self {
            metrics: output.metrics,
            meta: normalize_meta(output.meta),
        })
    }
}

fn normalize_meta(meta: Value) -> Value {
    if meta.is_null() {
        Value::Object(Default::default())
    } else {
        meta
    }
}
