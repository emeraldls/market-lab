use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::core::orderbook::OrderBookState;
use crate::domain::types::{
    MarketTicker, OhlcvCandle, OrderBookLevel, OrderBookSnapshot, TradeTick,
};

use super::catalog;
use super::market_data::{BulkKline, BulkTicker, normalize_timestamp_ms};

const BULK_WS_URL: &str = "wss://exchange-ws1.bulk.trade";

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct BulkWsClient {
    stream: WsStream,
}

impl BulkWsClient {
    async fn subscribe(subscription: Value) -> Result<Self> {
        let (mut stream, _) = connect_async(BULK_WS_URL)
            .await
            .context("failed to connect to BULK WebSocket")?;
        let request = serde_json::json!({
            "method": "subscribe",
            "subscription": [subscription],
        });
        stream
            .send(Message::Text(request.to_string().into()))
            .await
            .context("failed to send BULK WebSocket subscription")?;
        Ok(Self { stream })
    }

    async fn next_json(&mut self) -> Result<Value> {
        loop {
            let Some(message) = self.stream.next().await else {
                bail!("BULK WebSocket closed by server");
            };
            match message.context("BULK WebSocket read failed")? {
                Message::Text(text) => {
                    let value = serde_json::from_str(&text)
                        .context("BULK WebSocket returned invalid JSON")?;
                    return validate_ws_message(value);
                }
                Message::Binary(bytes) => {
                    let value = serde_json::from_slice(&bytes)
                        .context("BULK WebSocket returned invalid binary JSON")?;
                    return validate_ws_message(value);
                }
                Message::Ping(payload) => {
                    self.stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed to answer BULK WebSocket ping")?;
                }
                Message::Close(frame) => {
                    bail!("BULK WebSocket closed: {frame:?}");
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

fn validate_ws_message(value: Value) -> Result<Value> {
    if value.get("type").and_then(Value::as_str) == Some("error")
        || value.get("error").is_some_and(|error| !error.is_null())
    {
        bail!("BULK WebSocket error: {value}");
    }
    if value.get("type").and_then(Value::as_str) == Some("subscriptionResponse")
        && value.get("topics").and_then(Value::as_array).is_none()
    {
        bail!("BULK WebSocket rejected subscription: {value}");
    }
    Ok(value)
}

pub struct BulkCandleStream {
    client: BulkWsClient,
}

impl BulkCandleStream {
    pub async fn connect(symbol: &str, interval: &str) -> Result<Self> {
        let market = catalog::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "candle",
            "symbol": market.symbol,
            "interval": interval,
        }))
        .await?;
        Ok(Self { client })
    }

    pub async fn next_candle(&mut self) -> Result<OhlcvCandle> {
        loop {
            let message = self.client.next_json().await?;
            if message.get("type").and_then(Value::as_str) == Some("subscriptionResponse") {
                continue;
            }
            if message.get("type").and_then(Value::as_str) != Some("candle") {
                continue;
            }
            let candles = message
                .pointer("/data/candles")
                .and_then(Value::as_array)
                .context("BULK candle stream omitted data.candles")?;
            let Some(latest) = candles.last() else {
                continue;
            };
            let raw: BulkKline = serde_json::from_value(latest.clone())
                .context("BULK candle stream returned an invalid candle")?;
            return Ok(OhlcvCandle::from(raw));
        }
    }
}

pub struct BulkTickerStream {
    client: BulkWsClient,
    internal_symbol: String,
}

impl BulkTickerStream {
    pub async fn connect(symbol: &str) -> Result<Self> {
        let market = catalog::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "ticker",
            "symbol": market.symbol,
        }))
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.internal_symbol.clone(),
        })
    }
    pub async fn next_ticker(&mut self) -> Result<MarketTicker> {
        loop {
            let message = self.client.next_json().await?;
            if message.get("type").and_then(Value::as_str) == Some("subscriptionResponse") {
                continue;
            }
            if message.get("type").and_then(Value::as_str) != Some("ticker") {
                continue;
            }
            let raw: BulkTicker = serde_json::from_value(
                message
                    .pointer("/data/ticker")
                    .context("BULK ticker stream omitted data.ticker")?
                    .clone(),
            )
            .context("BULK ticker stream returned an invalid ticker")?;
            return raw.into_ticker(&self.internal_symbol);
        }
    }
}

pub struct BulkOrderBookStream {
    client: BulkWsClient,
    state: OrderBookState,
    internal_symbol: String,
    venue_symbol: String,
    depth: u16,
}

impl BulkOrderBookStream {
    pub async fn connect(symbol: &str, depth: u16, state_cap: usize) -> Result<Self> {
        let market = catalog::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "l2Delta",
            "symbol": market.symbol,
        }))
        .await?;
        Ok(Self {
            client,
            state: OrderBookState::with_max_levels_per_side(state_cap),
            internal_symbol: market.internal_symbol.clone(),
            venue_symbol: market.symbol.clone(),
            depth,
        })
    }

    pub async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        loop {
            let message = self.client.next_json().await?;
            if message.get("type").and_then(Value::as_str) == Some("subscriptionResponse") {
                continue;
            }
            if message.get("type").and_then(Value::as_str) != Some("l2Delta") {
                continue;
            }
            let raw: WsBook = serde_json::from_value(
                message
                    .pointer("/data/book")
                    .context("BULK orderbook stream omitted data.book")?
                    .clone(),
            )
            .context("BULK orderbook stream returned an invalid book")?;
            if raw.symbol != self.venue_symbol {
                bail!(
                    "BULK orderbook stream returned `{}`; expected `{}`",
                    raw.symbol,
                    self.venue_symbol
                );
            }
            if raw.levels.len() != 2 {
                bail!("BULK orderbook update must contain bid and ask arrays");
            }

            let mut sides = raw.levels.into_iter();
            let bids = sides
                .next()
                .expect("length checked")
                .into_iter()
                .map(OrderBookLevel::from)
                .collect();
            let asks = sides
                .next()
                .expect("length checked")
                .into_iter()
                .map(OrderBookLevel::from)
                .collect();
            let timestamp_ms = normalize_timestamp_ms(raw.timestamp);
            match raw.update_type.as_str() {
                "snapshot" => self.state.apply_snapshot(
                    "bulk".to_string(),
                    self.internal_symbol.clone(),
                    timestamp_ms,
                    bids,
                    asks,
                    None,
                ),
                "delta" => self.state.apply_delta(timestamp_ms, bids, asks, None),
                kind => bail!("unknown BULK orderbook update type `{kind}`"),
            }

            if let Some(snapshot) = self.state.snapshot(self.depth) {
                return Ok(snapshot);
            }
        }
    }
}

pub struct BulkTradesStream {
    client: BulkWsClient,
    internal_symbol: String,
    venue_symbol: String,
}

/// Dedicated account-event connection used by `mlabd`. It receives the complete
/// account snapshot on subscription followed by order, position, fill, margin,
/// liquidation, and ADL deltas. Reconnection is deliberately owned by the
/// daemon supervisor rather than this low-level stream.
pub struct BulkAccountStream {
    client: BulkWsClient,
    topic: String,
}

impl BulkAccountStream {
    pub async fn connect(account: &str) -> Result<Self> {
        if account.trim().is_empty() {
            bail!("BULK account WebSocket requires an account public key");
        }
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "account",
            "user": account,
        }))
        .await?;
        Ok(Self {
            client,
            topic: format!("account.{account}"),
        })
    }

    pub async fn next_event(&mut self) -> Result<Value> {
        loop {
            let message = self.client.next_json().await?;
            if message.get("type").and_then(Value::as_str) == Some("subscriptionResponse") {
                let subscribed =
                    message
                        .get("topics")
                        .and_then(Value::as_array)
                        .is_some_and(|topics| {
                            topics
                                .iter()
                                .any(|topic| topic.as_str() == Some(self.topic.as_str()))
                        });
                if !subscribed {
                    bail!("BULK account WebSocket did not subscribe to {}", self.topic);
                }
                continue;
            }
            if message.get("type").and_then(Value::as_str) != Some("account") {
                continue;
            }
            if message.get("topic").and_then(Value::as_str) != Some(self.topic.as_str()) {
                continue;
            }
            return message
                .get("data")
                .cloned()
                .context("BULK account event omitted data");
        }
    }
}

impl BulkTradesStream {
    pub async fn connect(symbol: &str) -> Result<Self> {
        let market = catalog::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "trades",
            "symbol": market.symbol,
        }))
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.internal_symbol.clone(),
            venue_symbol: market.symbol.clone(),
        })
    }

    pub async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        loop {
            let message = self.client.next_json().await?;
            if message.get("type").and_then(Value::as_str) == Some("subscriptionResponse") {
                continue;
            }
            if message.get("type").and_then(Value::as_str) != Some("trades") {
                continue;
            }
            let raw: Vec<WsTrade> = serde_json::from_value(
                message
                    .pointer("/data/trades")
                    .context("BULK trades stream omitted data.trades")?
                    .clone(),
            )
            .context("BULK trades stream returned invalid trades")?;
            return raw
                .into_iter()
                .map(|trade| {
                    if trade.symbol != self.venue_symbol {
                        bail!(
                            "BULK trade returned `{}`; expected `{}`",
                            trade.symbol,
                            self.venue_symbol
                        );
                    }
                    Ok(TradeTick {
                        exchange: "bulk".to_string(),
                        symbol: self.internal_symbol.clone(),
                        timestamp_ms: normalize_timestamp_ms(trade.time),
                        price: trade.price,
                        size: trade.size,
                        taker_buy: trade.side,
                    })
                })
                .collect();
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsBook {
    update_type: String,
    symbol: String,
    levels: Vec<Vec<WsBookLevel>>,
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct WsBookLevel {
    px: f64,
    sz: f64,
    #[serde(rename = "n")]
    _orders: u64,
}

impl From<WsBookLevel> for OrderBookLevel {
    fn from(value: WsBookLevel) -> Self {
        Self {
            price: value.px,
            quantity: value.sz,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WsTrade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "px")]
    price: f64,
    #[serde(rename = "sz")]
    size: f64,
    time: u64,
    side: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_documented_orderbook_and_trade_updates() {
        let book: WsBook =
            serde_json::from_str(include_str!("fixtures/ws_book.json")).expect("book parses");
        assert_eq!(book.update_type, "delta");

        let trade: WsTrade =
            serde_json::from_str(include_str!("fixtures/ws_trade.json")).expect("trade parses");
        assert!(trade.side);
    }

    #[test]
    fn surfaces_websocket_error_messages() {
        assert!(
            validate_ws_message(serde_json::json!({"type": "error", "message": "bad"})).is_err()
        );
        assert!(
            validate_ws_message(serde_json::json!({
                "type": "subscriptionResponse",
                "topics": ["trades.BTC-USD"]
            }))
            .is_ok()
        );
    }
}
