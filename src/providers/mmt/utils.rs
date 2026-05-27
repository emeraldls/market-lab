use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::domain::types::OrderBookLevel;

pub fn normalize_symbol_for_mmt(symbol: &str) -> Result<String> {
    let normalized = symbol.trim().to_lowercase();
    let mut parts = normalized.split('/');
    let base = parts.next().unwrap_or_default();
    let quote = parts.next().unwrap_or_default();
    if base.is_empty() || quote.is_empty() || parts.next().is_some() {
        bail!("invalid symbol format: expected BASE/QUOTE, got {symbol}");
    }

    let quote = if quote == "usdt" { "usd" } else { quote };
    Ok(format!("{base}/{quote}"))
}

pub fn normalize_to_ms(ts: u64) -> u64 {
    if ts < 10_000_000_000 { ts * 1000 } else { ts }
}

pub fn normalize_to_seconds(ts: u64) -> u64 {
    if ts >= 10_000_000_000 { ts / 1000 } else { ts }
}

pub fn parse_levels(maybe_levels: Option<&Value>) -> Result<Vec<OrderBookLevel>> {
    let Some(levels) = maybe_levels else {
        return Ok(Vec::new());
    };
    let arr = levels
        .as_array()
        .context("orderbook levels must be an array")?;

    let mut out = Vec::with_capacity(arr.len());
    for lvl in arr {
        if let Some(tuple) = lvl.as_array() {
            if tuple.len() < 2 {
                continue;
            }
            let price = value_to_f64(&tuple[0]).context("invalid bid/ask price")?;
            let qty = value_to_f64(&tuple[1]).context("invalid bid/ask quantity")?;
            out.push(OrderBookLevel {
                price,
                quantity: qty,
            });
            continue;
        }

        let price = lvl
            .get("price")
            .or_else(|| lvl.get("p"))
            .and_then(value_to_f64_from_value)
            .context("missing level price")?;

        let qty = lvl
            .get("quantity")
            .or_else(|| lvl.get("qty"))
            .or_else(|| lvl.get("size"))
            .or_else(|| lvl.get("q"))
            .and_then(value_to_f64_from_value)
            .context("missing level quantity")?;

        out.push(OrderBookLevel {
            price,
            quantity: qty,
        });
    }

    Ok(out)
}

fn value_to_f64_from_value(v: &Value) -> Option<f64> {
    value_to_f64(v).ok()
}

fn value_to_f64(v: &Value) -> Result<f64> {
    if let Some(n) = v.as_f64() {
        return Ok(n);
    }
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().context("failed to parse float string");
    }
    bail!("value is not numeric")
}
