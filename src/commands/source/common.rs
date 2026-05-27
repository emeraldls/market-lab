use std::collections::VecDeque;
use std::io::{self, Write};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::cli::OutputFormat;

#[derive(Debug, Clone, Serialize)]
pub struct SourceMeta {
    pub depth: Option<u16>,
    pub min_size: Option<f64>,
    pub max_size: Option<f64>,
    pub price_group: Option<f64>,
    pub interval_ms: Option<u64>,
    pub timeframe: Option<String>,
    pub bucket: Option<u8>,
    pub from: Option<u64>,
    pub to: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceEnvelope<T> {
    pub r#type: String,
    pub version: &'static str,
    pub provider: &'static str,
    pub exchange: String,
    pub symbol: String,
    pub ts_ms: u64,
    pub stream: bool,
    pub data: T,
    pub meta: SourceMeta,
}

pub fn render_terminal(title: &str, buf: &VecDeque<String>) -> Result<()> {
    print!("\x1B[2J\x1B[H");
    println!("{} (latest {} updates)", title, buf.len());
    println!("--------------------------------------------------------");
    for line in buf {
        println!("{}", line);
    }
    io::stdout().flush().context("flush failed")?;
    Ok(())
}

pub fn render_json_or_terminal<T, F>(
    env: &SourceEnvelope<T>,
    output: &OutputFormat,
    terminal_line: F,
    export_todo: &str,
) -> Result<()>
where
    T: Serialize,
    F: FnOnce(&SourceEnvelope<T>) -> String,
{
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(env)?),
        OutputFormat::Terminal => println!("{}", terminal_line(env)),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO {} export: {:?}", export_todo, output);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_envelope_serializes_contract_keys() {
        let env = SourceEnvelope {
            r#type: "source.orderbook.snapshot".to_string(),
            version: "1",
            provider: "mmt",
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            stream: false,
            data: vec![(1.0_f64, 2.0_f64)],
            meta: SourceMeta {
                depth: Some(20),
                min_size: None,
                max_size: None,
                price_group: None,
                interval_ms: None,
                timeframe: None,
                bucket: None,
                from: None,
                to: None,
            },
        };
        let v = serde_json::to_value(env).expect("serialize source envelope");
        assert_eq!(v["type"], "source.orderbook.snapshot");
        assert_eq!(v["version"], "1");
        assert!(v.get("data").is_some());
        assert!(v.get("meta").is_some());
    }
}
