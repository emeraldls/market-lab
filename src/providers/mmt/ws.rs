use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::core::orderbook::OrderBookState;
use crate::domain::types::OrderBookSnapshot;

use super::utils::{normalize_symbol_for_mmt, normalize_to_ms, parse_levels};

const MMT_WS_URL: &str = "wss://eu-central-1.mmt.gg/api/v1/ws";

pub struct MmtDepthStream {
    read: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    state: OrderBookState,
    depth: u16,
}

impl MmtDepthStream {
    pub async fn connect(
        exchange: &str,
        symbol: &str,
        depth: u16,
        state_cap: usize,
    ) -> Result<Self> {
        let api_key = std::env::var("MMT_API_KEY").context("MMT_API_KEY is required for stream")?;
        let ws_url = format!("{MMT_WS_URL}?api_key={}", api_key);
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .context("failed to connect websocket")?;
        let (mut write, read) = ws_stream.split();

        let subscribe = serde_json::json!({
            "type": "subscribe",
            "channel": "depth",
            "exchange": exchange.to_lowercase(),
            "symbol": normalize_symbol_for_mmt(symbol)?,
        });

        write
            .send(Message::Text(subscribe.to_string().into()))
            .await
            .context("failed to subscribe to depth channel")?;

        Ok(Self {
            read,
            state: OrderBookState::with_max_levels_per_side(state_cap),
            depth,
        })
    }

    pub async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        loop {
            let Some(msg) = self.read.next().await else {
                bail!("websocket closed by server");
            };
            let msg = msg.context("websocket read error")?;
            if let Some(snap) = handle_ws_message(msg, &mut self.state, self.depth)? {
                return Ok(snap);
            }
        }
    }
}

fn handle_ws_message(
    msg: Message,
    state: &mut OrderBookState,
    depth: u16,
) -> Result<Option<OrderBookSnapshot>> {
    let text = match msg {
        Message::Text(t) => t,
        Message::Binary(_)
        | Message::Ping(_)
        | Message::Pong(_)
        | Message::Close(_)
        | Message::Frame(_) => return Ok(None),
    };

    let v: serde_json::Value = serde_json::from_str(&text).context("invalid websocket JSON")?;

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
