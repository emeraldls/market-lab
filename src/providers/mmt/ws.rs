use crate::core::orderbook::OrderBookState;
use crate::domain::types::OrderBookSnapshot;
use anyhow::{Context, Result, bail};

use super::utils::{normalize_symbol_for_mmt, normalize_to_ms, parse_levels};
use super::ws_client::MmtWsClient;

pub struct MmtDepthStream {
    client: MmtWsClient,
    state: OrderBookState,
    depth: u16,
}

impl MmtDepthStream {
    pub async fn connect(exchange: &str, symbol: &str, depth: u16) -> Result<Self> {
        let provider_symbol = normalize_symbol_for_mmt(exchange, symbol)?;
        let client = MmtWsClient::shared().await?;

        let subscribe = serde_json::json!({
            "type": "subscribe",
            "channel": "depth",
            "exchange": exchange.to_lowercase(),
            "symbol": provider_symbol,
        });

        client
            .subscribe(subscribe)
            .await
            .context("failed to subscribe to depth channel")?;

        Ok(Self {
            client,
            state: OrderBookState::default(),
            depth,
        })
    }

    pub async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        loop {
            let Some(v) = self.client.next_json().await? else {
                bail!("websocket closed by server");
            };
            if v.is_null() {
                continue;
            }
            if let Some(snap) = handle_ws_value(v, &mut self.state, self.depth)? {
                return Ok(snap);
            }
        }
    }
}

fn handle_ws_value(
    v: serde_json::Value,
    state: &mut OrderBookState,
    depth: u16,
) -> Result<Option<OrderBookSnapshot>> {
    if v.get("type").and_then(|x| x.as_str()) == Some("subscribed") {
        return Ok(None);
    }
    if v.get("type").and_then(|x| x.as_str()) != Some("data") {
        return Ok(None);
    }
    if v.get("channel").and_then(|x| x.as_str()) != Some("depth") {
        return Ok(None);
    }

    let exchange = v
        .get("exchange")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let symbol = v
        .get("symbol")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown")
        .to_string();
    let payload = v.get("data").unwrap_or(&v);

    let ts_ms = payload
        .get("t")
        .and_then(|x| x.as_u64())
        .map(normalize_to_ms)
        .context("depth payload missing t timestamp")?;
    let seq = payload.get("seq").and_then(|x| x.as_u64());
    let is_snapshot = payload
        .get("snapshot")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let bids = parse_levels(payload.get("b").or_else(|| payload.get("bids")))?;
    let asks = parse_levels(payload.get("a").or_else(|| payload.get("asks")))?;

    if is_snapshot {
        state.apply_snapshot(exchange, symbol, ts_ms, bids, asks, seq);
    } else {
        state.apply_delta(ts_ms, bids, asks, seq);
    }

    Ok(state.snapshot(depth))
}
