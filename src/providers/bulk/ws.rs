use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::core::orderbook::OrderBookState;
use crate::domain::types::{
    MarketTicker, OhlcvCandle, OrderBookLevel, OrderBookSnapshot, TopOfBook, TradeTick,
};

use super::market_data::{BulkKline, BulkTicker, normalize_timestamp_ms};
use super::markets;

const BULK_WS_URL: &str = "wss://exchange-ws1.bulk.trade";
const BULK_TRADING_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct BulkWsClient {
    stream: WsStream,
}

#[derive(Default)]
pub struct BulkTradingClient {
    connection: Arc<Mutex<Option<mpsc::Sender<TradingCommand>>>>,
}

struct TradingCommand {
    payload: Value,
    response: oneshot::Sender<Result<Value>>,
}

impl BulkTradingClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn post(&self, payload: &impl Serialize) -> Result<Value> {
        let payload = serde_json::to_value(payload)
            .context("failed to encode BULK WebSocket trading payload")?;
        let (response_tx, response_rx) = oneshot::channel();
        let mut command = TradingCommand {
            payload,
            response: response_tx,
        };

        for _ in 0..2 {
            let sender = self.sender().await?;
            match sender.send(command).await {
                Ok(()) => {
                    return tokio::time::timeout(BULK_TRADING_RESPONSE_TIMEOUT, response_rx)
                        .await
                        .context("BULK WebSocket trading response timed out")?
                        .context("BULK WebSocket trading connection closed before responding")?;
                }
                Err(error) => {
                    command = error.0;
                    *self.connection.lock().await = None;
                }
            }
        }
        bail!("BULK WebSocket trading connection is unavailable")
    }

    async fn sender(&self) -> Result<mpsc::Sender<TradingCommand>> {
        let mut connection = self.connection.lock().await;
        if let Some(sender) = connection.as_ref()
            && !sender.is_closed()
        {
            return Ok(sender.clone());
        }

        let (stream, _) = connect_async(BULK_WS_URL)
            .await
            .context("failed to connect to BULK trading WebSocket")?;
        let (sender, receiver) = mpsc::channel(1024);
        tokio::spawn(run_trading_connection(stream, receiver));
        *connection = Some(sender.clone());
        Ok(sender)
    }
}

async fn run_trading_connection(
    stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    mut commands: mpsc::Receiver<TradingCommand>,
) {
    let (mut sink, mut source) = stream.split();
    let mut next_id = 1_u64;
    let mut pending = HashMap::<u64, oneshot::Sender<Result<Value>>>::new();
    let failure = loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break "trading request channel closed".to_string();
                };
                let id = next_id;
                next_id = next_id.saturating_add(1);
                let request = trading_request(id, command.payload);
                if let Err(error) = sink.send(Message::Text(request.to_string().into())).await {
                    let message = format!("failed to send BULK WebSocket trading request: {error}");
                    let _ = command.response.send(Err(anyhow::anyhow!(message.clone())));
                    break message;
                }
                pending.insert(id, command.response);
            }
            message = source.next() => {
                let Some(message) = message else {
                    break "BULK trading WebSocket closed by server".to_string();
                };
                match message {
                    Ok(Message::Text(text)) => match serde_json::from_str::<Value>(&text) {
                        Ok(value) => route_trading_response(value, &mut pending),
                        Err(error) => break format!("BULK trading WebSocket returned invalid JSON: {error}"),
                    },
                    Ok(Message::Binary(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
                        Ok(value) => route_trading_response(value, &mut pending),
                        Err(error) => break format!("BULK trading WebSocket returned invalid binary JSON: {error}"),
                    },
                    Ok(Message::Ping(payload)) => {
                        if let Err(error) = sink.send(Message::Pong(payload)).await {
                            break format!("failed to answer BULK trading WebSocket ping: {error}");
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        break format!("BULK trading WebSocket closed: {frame:?}");
                    }
                    Ok(Message::Pong(_) | Message::Frame(_)) => {}
                    Err(error) => break format!("BULK trading WebSocket read failed: {error}"),
                }
            }
        }
    };

    for (_, response) in pending {
        let _ = response.send(Err(anyhow::anyhow!(failure.clone())));
    }
}

fn trading_request(id: u64, payload: Value) -> Value {
    serde_json::json!({
        "method": "post",
        "request": {
            "type": "action",
            "payload": payload,
        },
        "id": id,
    })
}

fn route_trading_response(
    value: Value,
    pending: &mut HashMap<u64, oneshot::Sender<Result<Value>>>,
) {
    let Some(id) = value.get("id").and_then(Value::as_u64) else {
        if value.get("type").and_then(Value::as_str) == Some("error") {
            let message = format!("BULK WebSocket trading error: {value}");
            for (_, response) in pending.drain() {
                let _ = response.send(Err(anyhow::anyhow!(message.clone())));
            }
        }
        return;
    };
    let Some(response) = pending.remove(&id) else {
        return;
    };
    let result = if value.get("type").and_then(Value::as_str) == Some("error") {
        Err(anyhow::anyhow!("BULK WebSocket trading error: {value}"))
    } else if value.get("type").and_then(Value::as_str) != Some("post") {
        Err(anyhow::anyhow!(
            "BULK WebSocket trading request returned an unexpected response: {value}"
        ))
    } else if is_trading_acknowledgement(&value) {
        Ok(value)
    } else {
        decode_trading_response(&value)
    };
    let _ = response.send(result);
}

pub(super) fn is_trading_acknowledgement(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("post")
        && value.pointer("/data/type").and_then(Value::as_str) == Some("ack")
        && value.pointer("/data/ok").and_then(Value::as_bool) == Some(true)
}

fn decode_trading_response(value: &Value) -> Result<Value> {
    if let Some(payload) = value.pointer("/data/payload") {
        return Ok(payload.clone());
    }
    if let Some(data) = value.get("data")
        && (data.get("status").is_some() || data.get("response").is_some())
    {
        return Ok(data.clone());
    }
    if let Some(payload) = value.get("payload") {
        return Ok(payload.clone());
    }
    bail!("BULK WebSocket trading response omitted its action payload: {value}")
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

    async fn next_typed<T: DeserializeOwned>(&mut self) -> Result<T> {
        loop {
            let Some(message) = self.stream.next().await else {
                bail!("BULK WebSocket closed by server");
            };
            match message.context("BULK WebSocket read failed")? {
                Message::Text(text) => {
                    return serde_json::from_str(&text)
                        .context("BULK WebSocket returned invalid JSON");
                }
                Message::Binary(bytes) => {
                    return serde_json::from_slice(&bytes)
                        .context("BULK WebSocket returned invalid binary JSON");
                }
                Message::Ping(payload) => {
                    self.stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed to answer BULK WebSocket ping")?;
                }
                Message::Close(frame) => bail!("BULK WebSocket closed: {frame:?}"),
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
        let market = markets::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "candle",
            "symbol": market.provider_symbol,
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
        let market = markets::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "ticker",
            "symbol": market.provider_symbol,
        }))
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.symbol.clone(),
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
    last_touch: Option<(f64, f64)>,
}

impl BulkOrderBookStream {
    pub async fn connect(symbol: &str, depth: u16) -> Result<Self> {
        let market = markets::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "l2Delta",
            "symbol": market.provider_symbol,
        }))
        .await?;
        Ok(Self {
            client,
            state: OrderBookState::default(),
            internal_symbol: market.symbol.clone(),
            venue_symbol: market.venue_symbol.clone(),
            depth,
            last_touch: None,
        })
    }

    pub async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        loop {
            self.apply_next_update().await?;
            if let Some(snapshot) = self.state.snapshot(self.depth) {
                return Ok(snapshot);
            }
        }
    }

    /// Receives the next book mutation and returns only the current touch.
    /// This is the native strategy/bot path: it performs no depth snapshot or
    /// exchange/symbol cloning for every delta.
    pub async fn next_top(&mut self) -> Result<TopOfBook> {
        loop {
            self.apply_next_update().await?;
            if let Some((best_bid, best_ask)) = self.state.best_bid_ask() {
                // BULK can move the two sides of the touch in consecutive
                // deltas. Do not expose the transient crossed state between
                // those messages to an execution controller.
                if best_bid.price >= best_ask.price {
                    continue;
                }
                let touch = (best_bid.price, best_ask.price);
                if self.last_touch == Some(touch) {
                    continue;
                }
                self.last_touch = Some(touch);
                return Ok(TopOfBook {
                    timestamp_ms: self.state.timestamp_ms(),
                    best_bid: Some(best_bid),
                    best_ask: Some(best_ask),
                });
            }
        }
    }

    async fn apply_next_update(&mut self) -> Result<()> {
        loop {
            let message: WsBookEnvelope = self.client.next_typed().await?;
            if message.kind == "error" || message.error.is_some() {
                bail!(
                    "BULK orderbook WebSocket error: {}",
                    message
                        .error
                        .map_or_else(|| "unknown error".to_string(), |error| error.to_string())
                );
            }
            if message.kind == "subscriptionResponse" {
                if message.topics.is_none() {
                    bail!("BULK WebSocket rejected the orderbook subscription");
                }
                continue;
            }
            if message.kind != "l2Delta" {
                continue;
            }
            let raw = message
                .data
                .context("BULK orderbook stream omitted data")?
                .book;
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
                .map(OrderBookLevel::from);
            let asks = sides
                .next()
                .expect("length checked")
                .into_iter()
                .map(OrderBookLevel::from);
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
            return Ok(());
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
        let market = markets::market(symbol)?;
        let client = BulkWsClient::subscribe(serde_json::json!({
            "type": "trades",
            "symbol": market.provider_symbol,
        }))
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.symbol.clone(),
            venue_symbol: market.venue_symbol.clone(),
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
struct WsBookEnvelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    data: Option<WsBookData>,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct WsBookData {
    book: WsBook,
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
    fn parses_typed_orderbook_envelopes_without_a_value_round_trip() {
        let subscription: WsBookEnvelope = serde_json::from_value(serde_json::json!({
            "type": "subscriptionResponse",
            "topics": ["l2Delta.BTC-USD"]
        }))
        .expect("subscription response parses");
        assert_eq!(subscription.kind, "subscriptionResponse");
        assert!(subscription.topics.is_some());

        let update: WsBookEnvelope = serde_json::from_value(serde_json::json!({
            "type": "l2Delta",
            "data": {
                "book": {
                    "updateType": "delta",
                    "symbol": "BTC-USD",
                    "levels": [
                        [{"px": 65000.0, "sz": 1.5, "n": 2}],
                        [{"px": 65000.25, "sz": 0.0, "n": 0}]
                    ],
                    "timestamp": 1784600000000000000_u64
                }
            }
        }))
        .expect("orderbook envelope parses");
        let book = update.data.expect("update includes data").book;
        assert_eq!(book.update_type, "delta");
        assert_eq!(book.levels[0][0].px, 65_000.0);
        assert_eq!(book.levels[1][0].sz, 0.0);
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

    #[test]
    fn wraps_and_correlates_websocket_trading_requests() {
        let request = trading_request(7, serde_json::json!({ "actions": [] }));
        assert_eq!(request["method"], "post");
        assert_eq!(request["request"]["type"], "action");
        assert_eq!(
            request["request"]["payload"]["actions"],
            serde_json::json!([])
        );
        assert_eq!(request["id"], 7);

        let (response_tx, mut response_rx) = oneshot::channel();
        let mut pending = HashMap::from([(7, response_tx)]);
        route_trading_response(
            serde_json::json!({
                "type": "post",
                "id": 7,
                "data": {
                    "type": "action",
                    "payload": { "status": "ok", "response": { "data": { "statuses": [] } } }
                }
            }),
            &mut pending,
        );
        let response = response_rx
            .try_recv()
            .expect("response should be routed")
            .expect("response should be valid");
        assert_eq!(response["status"], "ok");
        assert!(pending.is_empty());
    }

    #[test]
    fn accepts_flat_websocket_trading_response_data() {
        let response = decode_trading_response(&serde_json::json!({
            "type": "post",
            "id": 9,
            "data": {
                "status": "ok",
                "response": { "data": { "statuses": [] } }
            }
        }))
        .expect("flat response data should decode");

        assert_eq!(response["status"], "ok");
    }

    #[test]
    fn routes_ack_as_the_completed_transport_response() {
        let (response_tx, mut response_rx) = oneshot::channel();
        let mut pending = HashMap::from([(11, response_tx)]);

        route_trading_response(
            serde_json::json!({
                "type": "post",
                "id": 11,
                "data": { "type": "ack", "ok": true }
            }),
            &mut pending,
        );

        let response = response_rx
            .try_recv()
            .expect("acknowledgement should be routed")
            .expect("acknowledgement should be valid");
        assert!(is_trading_acknowledgement(&response));
        assert!(pending.is_empty());
    }

    #[test]
    fn uncorrelated_websocket_errors_fail_pending_requests() {
        let (response_tx, mut response_rx) = oneshot::channel();
        let mut pending = HashMap::from([(3, response_tx)]);

        route_trading_response(
            serde_json::json!({
                "type": "error",
                "error": { "message": "invalid transaction", "code": 400 }
            }),
            &mut pending,
        );

        let error = response_rx
            .try_recv()
            .expect("response should be routed")
            .expect_err("request should fail");
        assert!(error.to_string().contains("invalid transaction"));
        assert!(pending.is_empty());
    }
}
