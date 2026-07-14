use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::cli::OutputFormat;
use crate::domain::enums::ProviderKind;

#[derive(Debug, Clone, Serialize)]
pub struct StudyDescriptor {
    pub name: String,
    pub kind: &'static str,
    pub source: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct StudyEnvelope<I, M>
where
    I: Serialize,
    M: Serialize,
{
    pub r#type: String,
    pub version: &'static str,
    pub provider: &'static str,
    pub exchange: String,
    pub symbol: String,
    pub ts_ms: u64,
    pub stream: bool,
    pub study: StudyDescriptor,
    pub inputs: I,
    pub metrics: M,
    pub meta: Value,
}

#[derive(Debug, Serialize)]
struct CompactStudyEnvelope<'a, I, M>
where
    I: Serialize,
    M: Serialize,
{
    r#type: &'a str,
    version: &'static str,
    provider: &'static str,
    exchange: &'a str,
    symbol: &'a str,
    ts_ms: u64,
    stream: bool,
    study: &'a StudyDescriptor,
    #[serde(skip_serializing_if = "is_empty_object")]
    inputs: &'a I,
    metrics: &'a M,
}

pub fn provider_name(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::Mmt => "mmt",
        ProviderKind::MarketLab => "marketlab",
        ProviderKind::Bulk => "bulk",
    }
}

pub fn print_study_json<I, M>(
    env: &StudyEnvelope<I, M>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()>
where
    I: Serialize,
    M: Serialize,
{
    if verbose {
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(env)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(env)?),
            _ => unreachable!(),
        }
    } else {
        let compact = CompactStudyEnvelope {
            r#type: &env.r#type,
            version: env.version,
            provider: env.provider,
            exchange: &env.exchange,
            symbol: &env.symbol,
            ts_ms: env.ts_ms,
            stream: env.stream,
            study: &env.study,
            inputs: &env.inputs,
            metrics: &env.metrics,
        };
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&compact)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(&compact)?),
            _ => unreachable!(),
        }
    }

    Ok(())
}

pub fn empty_meta() -> Value {
    Value::Object(Default::default())
}

pub fn is_empty_object<T: Serialize>(value: &T) -> bool {
    match serde_json::to_value(value) {
        Ok(Value::Object(map)) => map.is_empty(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn study_envelope_serializes_contract_keys() {
        let env = StudyEnvelope {
            r#type: "study.spread.result".to_string(),
            version: "1",
            provider: "mmt",
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            stream: false,
            study: StudyDescriptor {
                name: "spread".to_string(),
                kind: "snapshot",
                source: "builtin",
            },
            inputs: serde_json::json!({"depth": 20}),
            metrics: serde_json::json!({"spread_bps": 1.2}),
            meta: empty_meta(),
        };
        let v = serde_json::to_value(env).expect("serialize study envelope");
        assert_eq!(v["type"], "study.spread.result");
        assert_eq!(v["version"], "1");
        assert_eq!(v["study"]["name"], "spread");
        assert!(v.get("inputs").is_some());
        assert!(v.get("metrics").is_some());
    }

    #[test]
    fn compact_study_json_omits_inputs_and_meta() {
        let env = StudyEnvelope {
            r#type: "study.spread.result".to_string(),
            version: "1",
            provider: "mmt",
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            stream: false,
            study: StudyDescriptor {
                name: "spread".to_string(),
                kind: "snapshot",
                source: "builtin",
            },
            inputs: serde_json::json!({}),
            metrics: serde_json::json!({"spread_bps": 1.2}),
            meta: serde_json::json!({"debug": true}),
        };
        let compact = CompactStudyEnvelope {
            r#type: &env.r#type,
            version: env.version,
            provider: env.provider,
            exchange: &env.exchange,
            symbol: &env.symbol,
            ts_ms: env.ts_ms,
            stream: env.stream,
            study: &env.study,
            inputs: &env.inputs,
            metrics: &env.metrics,
        };
        let v = serde_json::to_value(compact).expect("serialize compact study envelope");
        assert!(v.get("inputs").is_none());
        assert!(v.get("meta").is_none());
        assert!(v.get("metrics").is_some());
        assert!(v.get("study").is_some());
    }
}
