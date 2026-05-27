use anyhow::Result;
use serde::Serialize;

use crate::cli::OutputFormat;
use crate::domain::enums::ProviderKind;

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
    pub inputs: I,
    pub metrics: M,
    pub meta: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct CompactStudyEnvelope<'a, M>
where
    M: Serialize,
{
    r#type: &'a str,
    version: &'static str,
    provider: &'static str,
    exchange: &'a str,
    symbol: &'a str,
    ts_ms: u64,
    stream: bool,
    metrics: &'a M,
}

pub fn provider_name(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::Mmt => "mmt",
        ProviderKind::MarketLab => "marketlab",
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
            inputs: serde_json::json!({"depth": 20}),
            metrics: serde_json::json!({"spread_bps": 1.2}),
            meta: serde_json::json!({}),
        };
        let v = serde_json::to_value(env).expect("serialize study envelope");
        assert_eq!(v["type"], "study.spread.result");
        assert_eq!(v["version"], "1");
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
            inputs: serde_json::json!({"depth": 20}),
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
            metrics: &env.metrics,
        };
        let v = serde_json::to_value(compact).expect("serialize compact study envelope");
        assert!(v.get("inputs").is_none());
        assert!(v.get("meta").is_none());
        assert!(v.get("metrics").is_some());
    }
}
