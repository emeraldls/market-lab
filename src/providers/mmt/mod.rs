use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

use crate::domain::enums::BookMode;
use crate::domain::requests::{InspectRequest, ReplayRequest};
use crate::domain::types::{OrderBookLevel, OrderBookSnapshot, ProviderHealth, TopOfBook};

pub mod utils;
pub mod ws;

use utils::{normalize_symbol_for_mmt, normalize_to_ms, parse_levels};

const MMT_BASE_URL: &str = "https://eu-central-1.mmt.gg/api/v1";
const MMT_API_KEY_ENV: &str = "MMT_API_KEY";
const MMT_HTTP_TIMEOUT_SECS: u64 = 8;

pub struct MmtProvider;

impl MmtProvider {
    pub async fn inspect(req: &InspectRequest) -> Result<OrderBookSnapshot> {
        if matches!(req.book_mode, BookMode::Raw) {
            bail!("MMT supports only binned book mode in current inspect integration");
        }
        let at_seconds = req.at / 1000;
        fetch_flat_heatmap_hd_snapshot(&req.exchange, &req.symbol, at_seconds, req.depth).await
    }

    pub async fn live_orderbook(
        exchange: &str,
        symbol: &str,
        depth: u16,
    ) -> Result<OrderBookSnapshot> {
        fetch_orderbook_snapshot(exchange, symbol, depth).await
    }

    pub async fn replay(_req: &ReplayRequest) -> Result<Vec<TopOfBook>> {
        bail!("MMT replay is not implemented yet")
    }

    pub async fn health() -> Result<ProviderHealth> {
        let api_key = mmt_api_key()?;
        let url = format!("{MMT_BASE_URL}/usage");

        let resp = Client::new()
            .get(url)
            .timeout(std::time::Duration::from_secs(MMT_HTTP_TIMEOUT_SECS))
            .header("X-API-Key", api_key)
            .send()
            .await
            .context("failed to call MMT /usage")?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .context("failed to decode MMT /usage response")?;

        if !status.is_success() {
            bail!("MMT /usage returned HTTP {} body={}", status, body);
        }

        Ok(ProviderHealth {
            provider: "mmt".to_string(),
            status: "ok".to_string(),
            details: body,
        })
    }
}

#[derive(Debug, Deserialize)]
struct FlatHeatmapHdResponse {
    exchange: String,
    symbol: String,
    data: Vec<HeatmapPoint>,
}

#[derive(Debug, Deserialize)]
struct HeatmapPoint {
    t: u64,
    pg: f64,
    s: Vec<f64>,
    si: usize,
    minp: f64,
}

fn mmt_api_key() -> Result<String> {
    let api_key = std::env::var(MMT_API_KEY_ENV)
        .with_context(|| format!("{MMT_API_KEY_ENV} is required when --provider mmt"))?;
    if api_key.trim().is_empty() {
        bail!("{MMT_API_KEY_ENV} is set but empty");
    }
    Ok(api_key)
}

async fn fetch_orderbook_snapshot(
    exchange: &str,
    symbol: &str,
    depth: u16,
) -> Result<OrderBookSnapshot> {
    let api_key = mmt_api_key()?;
    let normalized_symbol = normalize_symbol_for_mmt(symbol)?;
    let exchange = exchange.trim().to_lowercase();

    let levels = levels_param(depth);
    let url = format!("{MMT_BASE_URL}/orderbook");
    let resp = Client::new()
        .get(url)
        .timeout(std::time::Duration::from_secs(MMT_HTTP_TIMEOUT_SECS))
        .header("X-API-Key", api_key)
        .query(&[
            ("exchange", exchange.as_str()),
            ("symbol", normalized_symbol.as_str()),
            ("levels", levels.as_str()),
        ])
        .send()
        .await
        .context("failed to call MMT /orderbook")?;

    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .context("failed to decode MMT /orderbook response")?;

    if !status.is_success() {
        bail!("MMT /orderbook returned HTTP {} body={}", status, body);
    }

    let mut snapshot = parse_orderbook_body(&body)?;
    let max_depth = depth as usize;
    snapshot.bids.truncate(max_depth);
    snapshot.asks.truncate(max_depth);
    Ok(snapshot)
}

fn levels_param(depth: u16) -> String {
    if depth <= 100 {
        "100".to_string()
    } else if depth <= 1000 {
        "1000".to_string()
    } else if depth <= 5000 {
        "5000".to_string()
    } else {
        "full".to_string()
    }
}

fn parse_orderbook_body(body: &Value) -> Result<OrderBookSnapshot> {
    let exchange = body
        .get("exchange")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let symbol = body
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();

    let data = body.get("data").unwrap_or(body);

    let timestamp_ms = data
        .get("t")
        .and_then(Value::as_u64)
        .map(normalize_to_ms)
        .unwrap_or(0);
    let bids = parse_levels(
        data.get("b")
            .or_else(|| data.get("bids"))
            .or_else(|| body.get("b"))
            .or_else(|| body.get("bids")),
    )?;
    let asks = parse_levels(
        data.get("a")
            .or_else(|| data.get("asks"))
            .or_else(|| body.get("a"))
            .or_else(|| body.get("asks")),
    )?;

    Ok(OrderBookSnapshot {
        exchange,
        symbol,
        timestamp_ms,
        bids,
        asks,
    })
}

async fn fetch_flat_heatmap_hd_snapshot(
    exchange: &str,
    symbol: &str,
    at_seconds: u64,
    depth: u16,
) -> Result<OrderBookSnapshot> {
    let api_key = mmt_api_key()?;
    let normalized_symbol = normalize_symbol_for_mmt(symbol)?;
    let exchange = exchange.trim().to_lowercase();

    let tf = "1m";
    let from = at_seconds.saturating_sub(3600);
    let to = at_seconds.saturating_add(60);

    let url = format!("{MMT_BASE_URL}/flat_heatmap_hd");
    let resp = Client::new()
        .get(url)
        .timeout(std::time::Duration::from_secs(MMT_HTTP_TIMEOUT_SECS))
        .header("X-API-Key", api_key)
        .query(&[
            ("exchange", exchange.as_str()),
            ("symbol", normalized_symbol.as_str()),
            ("tf", tf),
            ("from", &from.to_string()),
            ("to", &to.to_string()),
        ])
        .send()
        .await
        .context("failed to call MMT /flat_heatmap_hd")?;

    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .context("failed to decode MMT /flat_heatmap_hd response")?;

    if !status.is_success() {
        bail!(
            "MMT /flat_heatmap_hd returned HTTP {} body={}",
            status,
            body
        );
    }

    let parsed: FlatHeatmapHdResponse =
        serde_json::from_value(body).context("invalid /flat_heatmap_hd payload shape")?;
    let point = pick_point_at_or_before(&parsed.data, at_seconds)
        .or_else(|| parsed.data.last())
        .context("/flat_heatmap_hd returned no data points")?;

    Ok(heatmap_point_to_snapshot(
        &parsed.exchange,
        &parsed.symbol,
        point,
        depth,
    ))
}

fn pick_point_at_or_before(points: &[HeatmapPoint], at_seconds: u64) -> Option<&HeatmapPoint> {
    points
        .iter()
        .filter(|p| p.t <= at_seconds)
        .max_by_key(|p| p.t)
}

fn heatmap_point_to_snapshot(
    exchange: &str,
    symbol: &str,
    point: &HeatmapPoint,
    depth: u16,
) -> OrderBookSnapshot {
    let depth = depth as usize;
    let total = point.s.len();
    let split = point.si.min(total);

    let bids_start = split.saturating_sub(depth);
    let asks_end = (split + depth).min(total);

    let mut bids = Vec::with_capacity(split.saturating_sub(bids_start));
    for idx in bids_start..split {
        let price = point.minp + (idx as f64 * point.pg);
        bids.push(OrderBookLevel {
            price,
            quantity: point.s[idx],
        });
    }
    bids.reverse();

    let mut asks = Vec::with_capacity(asks_end.saturating_sub(split));
    for idx in split..asks_end {
        let price = point.minp + (idx as f64 * point.pg);
        asks.push(OrderBookLevel {
            price,
            quantity: point.s[idx],
        });
    }

    OrderBookSnapshot {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        timestamp_ms: point.t * 1000,
        bids,
        asks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_symbol_maps_usdt_to_usd() {
        let got = normalize_symbol_for_mmt("BTC/USDT").expect("must normalize");
        assert_eq!(got, "btc/usd");
    }

    #[test]
    fn heatmap_point_conversion_respects_split() {
        let p = HeatmapPoint {
            t: 100,
            pg: 5.0,
            s: vec![10.0, 20.0, 30.0, 40.0, 50.0],
            si: 2,
            minp: 100.0,
        };
        let snap = heatmap_point_to_snapshot("binancef", "btc/usd", &p, 2);
        assert_eq!(snap.bids.len(), 2);
        assert_eq!(snap.asks.len(), 2);
        assert!(snap.bids[0].price > snap.bids[1].price);
    }

    #[test]
    fn levels_param_selects_expected_bucket() {
        assert_eq!(levels_param(20), "100");
        assert_eq!(levels_param(500), "1000");
        assert_eq!(levels_param(2500), "5000");
        assert_eq!(levels_param(7000), "full");
    }
}
