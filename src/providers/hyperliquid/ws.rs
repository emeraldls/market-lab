use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::domain::types::{
    MarketTicker, OhlcvCandle, OrderBookLevel, OrderBookSnapshot, TopOfBook, TradeTick,
};

use super::WS_URL;
use super::markets;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const TRADING_RESPONSE_TIMEOUT: Duration = Duration::from_secs(20);
const TRADING_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const SUBSCRIPTION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const MAX_INFLIGHT_POSTS: usize = 100;

struct HyperliquidWsClient {
    stream: WsStream,
    heartbeat: tokio::time::Interval,
}

#[derive(Clone, Default)]
pub struct HyperliquidTradingClient {
    connection: Arc<Mutex<Option<mpsc::Sender<TradingCommand>>>>,
}

struct TradingCommand {
    payload: Value,
    response: oneshot::Sender<Result<Value>>,
}

impl HyperliquidTradingClient {
    pub fn shared() -> Self {
        static CLIENT: OnceLock<HyperliquidTradingClient> = OnceLock::new();
        CLIENT.get_or_init(Self::default).clone()
    }

    pub async fn post_action(&self, payload: &impl Serialize) -> Result<Value> {
        let payload = serde_json::to_value(payload)
            .context("failed to encode Hyperliquid WebSocket action payload")?;
        let (response_tx, response_rx) = oneshot::channel();
        let mut command = TradingCommand {
            payload,
            response: response_tx,
        };

        for _ in 0..2 {
            let sender = self.sender().await?;
            match sender.send(command).await {
                Ok(()) => {
                    return tokio::time::timeout(TRADING_RESPONSE_TIMEOUT, response_rx)
                        .await
                        .context("Hyperliquid WebSocket trading response timed out")?
                        .context(
                            "Hyperliquid WebSocket trading connection closed before responding",
                        )?;
                }
                Err(error) => {
                    command = error.0;
                    *self.connection.lock().await = None;
                }
            }
        }
        bail!("Hyperliquid WebSocket trading connection is unavailable")
    }

    async fn sender(&self) -> Result<mpsc::Sender<TradingCommand>> {
        let mut connection = self.connection.lock().await;
        if let Some(sender) = connection.as_ref()
            && !sender.is_closed()
        {
            return Ok(sender.clone());
        }

        let (stream, _) = connect_async(super::WS_URL)
            .await
            .context("failed to connect to Hyperliquid trading WebSocket")?;
        let (sender, receiver) = mpsc::channel(MAX_INFLIGHT_POSTS);
        tokio::spawn(run_trading_connection(stream, receiver));
        *connection = Some(sender.clone());
        Ok(sender)
    }
}

async fn run_trading_connection(stream: WsStream, mut commands: mpsc::Receiver<TradingCommand>) {
    let (mut sink, mut source) = stream.split();
    let mut next_id = 1_u64;
    let mut pending = HashMap::<u64, oneshot::Sender<Result<Value>>>::new();
    let start = tokio::time::Instant::now() + TRADING_HEARTBEAT_INTERVAL;
    let mut heartbeat = tokio::time::interval_at(start, TRADING_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let failure = loop {
        tokio::select! {
            command = commands.recv(), if pending.len() < MAX_INFLIGHT_POSTS => {
                let Some(command) = command else {
                    break "Hyperliquid trading request channel closed".to_string();
                };
                let id = next_id;
                next_id = next_id.saturating_add(1);
                let request = trading_request(id, command.payload);
                if let Err(error) = sink.send(Message::Text(request.to_string().into())).await {
                    let message = format!("failed to send Hyperliquid WebSocket trading request: {error}");
                    let _ = command.response.send(Err(anyhow::anyhow!(message.clone())));
                    break message;
                }
                pending.insert(id, command.response);
            }
            message = source.next() => {
                let Some(message) = message else {
                    break "Hyperliquid trading WebSocket closed by server".to_string();
                };
                match message {
                    Ok(Message::Text(text)) => match serde_json::from_str::<Value>(&text) {
                        Ok(value) => route_trading_response(value, &mut pending),
                        Err(error) => break format!("Hyperliquid trading WebSocket returned invalid JSON: {error}"),
                    },
                    Ok(Message::Binary(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
                        Ok(value) => route_trading_response(value, &mut pending),
                        Err(error) => break format!("Hyperliquid trading WebSocket returned invalid binary JSON: {error}"),
                    },
                    Ok(Message::Ping(payload)) => {
                        if let Err(error) = sink.send(Message::Pong(payload)).await {
                            break format!("failed to answer Hyperliquid trading WebSocket ping: {error}");
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        break format!("Hyperliquid trading WebSocket closed: {frame:?}");
                    }
                    Ok(Message::Pong(_) | Message::Frame(_)) => {}
                    Err(error) => break format!("Hyperliquid trading WebSocket read failed: {error}"),
                }
            }
            _ = heartbeat.tick() => {
                let ping = serde_json::json!({ "method": "ping" });
                if let Err(error) = sink.send(Message::Text(ping.to_string().into())).await {
                    break format!("failed to heartbeat Hyperliquid trading WebSocket: {error}");
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
        "id": id,
        "request": {
            "type": "action",
            "payload": payload,
        },
    })
}

fn route_trading_response(
    value: Value,
    pending: &mut HashMap<u64, oneshot::Sender<Result<Value>>>,
) {
    if value.get("channel").and_then(Value::as_str) == Some("pong") {
        return;
    }
    if value.get("channel").and_then(Value::as_str) == Some("error") {
        let message = format!("Hyperliquid WebSocket trading error: {value}");
        for (_, response) in pending.drain() {
            let _ = response.send(Err(anyhow::anyhow!(message.clone())));
        }
        return;
    }
    if value.get("channel").and_then(Value::as_str) != Some("post") {
        return;
    }
    let Some(id) = value.pointer("/data/id").and_then(Value::as_u64) else {
        let message = format!("Hyperliquid WebSocket post response omitted its id: {value}");
        for (_, response) in pending.drain() {
            let _ = response.send(Err(anyhow::anyhow!(message.clone())));
        }
        return;
    };
    let Some(response) = pending.remove(&id) else {
        return;
    };
    let response_type = value.pointer("/data/response/type").and_then(Value::as_str);
    let payload = value.pointer("/data/response/payload");
    let result = match (response_type, payload) {
        (Some("action"), Some(payload)) => Ok(payload.clone()),
        (Some("error"), Some(payload)) => Err(anyhow::anyhow!(
            "Hyperliquid WebSocket trading request failed: {payload}"
        )),
        _ => Err(anyhow::anyhow!(
            "Hyperliquid WebSocket trading request returned an unexpected response: {value}"
        )),
    };
    let _ = response.send(result);
}

impl HyperliquidWsClient {
    async fn subscribe(subscriptions: impl IntoIterator<Item = Value>) -> Result<Self> {
        let (mut stream, _) = connect_async(WS_URL)
            .await
            .context("failed to connect to Hyperliquid testnet WebSocket")?;
        for subscription in subscriptions {
            stream
                .send(Message::Text(
                    serde_json::json!({
                        "method": "subscribe",
                        "subscription": subscription,
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .context("failed to subscribe to Hyperliquid testnet WebSocket")?;
        }
        let start = tokio::time::Instant::now() + SUBSCRIPTION_HEARTBEAT_INTERVAL;
        let mut heartbeat = tokio::time::interval_at(start, SUBSCRIPTION_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(Self { stream, heartbeat })
    }

    async fn next_json(&mut self) -> Result<Value> {
        loop {
            let message = tokio::select! {
                biased;
                _ = self.heartbeat.tick() => {
                    self.stream
                        .send(Message::Text(
                            serde_json::json!({ "method": "ping" }).to_string().into(),
                        ))
                        .await
                        .context("failed to heartbeat Hyperliquid WebSocket")?;
                    continue;
                }
                message = self.stream.next() => match message {
                    Some(message) => message.context("Hyperliquid WebSocket read failed")?,
                    None => bail!("Hyperliquid WebSocket closed by server"),
                },
            };
            let value: Value = match message {
                Message::Text(text) => serde_json::from_str(&text)
                    .context("Hyperliquid WebSocket returned invalid JSON")?,
                Message::Binary(bytes) => serde_json::from_slice(&bytes)
                    .context("Hyperliquid WebSocket returned invalid binary JSON")?,
                Message::Ping(payload) => {
                    self.stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed to answer Hyperliquid WebSocket ping")?;
                    continue;
                }
                Message::Close(frame) => {
                    bail!("Hyperliquid WebSocket closed: {frame:?}")
                }
                Message::Pong(_) | Message::Frame(_) => continue,
            };
            if value.get("channel").and_then(Value::as_str) == Some("error") {
                bail!("Hyperliquid WebSocket error: {value}");
            }
            return Ok(value);
        }
    }
}

pub struct HyperliquidOrderBookStream {
    client: HyperliquidWsClient,
    internal_symbol: String,
    venue_symbol: String,
    depth: u16,
    last_touch: Option<(f64, f64)>,
}

impl HyperliquidOrderBookStream {
    pub async fn connect(symbol: &str, depth: u16) -> Result<Self> {
        if depth == 0 || depth > 20 {
            bail!("Hyperliquid orderbook depth must be between 1 and 20");
        }
        let market = markets::market(symbol)?;
        let client = HyperliquidWsClient::subscribe([serde_json::json!({
            "type": "l2Book",
            "coin": market.venue_symbol,
        })])
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.symbol.clone(),
            venue_symbol: market.venue_symbol.clone(),
            depth,
            last_touch: None,
        })
    }

    pub async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        loop {
            let value = self.client.next_json().await?;
            if value.get("channel").and_then(Value::as_str) != Some("l2Book") {
                continue;
            }
            let book: WsBook = serde_json::from_value(
                value
                    .get("data")
                    .cloned()
                    .context("Hyperliquid book update omitted data")?,
            )
            .context("invalid Hyperliquid book update")?;
            if book.coin != self.venue_symbol || book.levels.len() != 2 {
                bail!("Hyperliquid book update did not match the subscribed native perpetual");
            }
            let mut sides = book.levels.into_iter();
            let parse = |levels: Vec<WsLevel>| {
                levels
                    .into_iter()
                    .take(self.depth as usize)
                    .map(WsLevel::into_level)
                    .collect::<Result<Vec<_>>>()
            };
            return Ok(OrderBookSnapshot {
                exchange: "hyperliquid".to_string(),
                symbol: self.internal_symbol.clone(),
                timestamp_ms: book.time,
                bids: parse(sides.next().expect("book length checked"))?,
                asks: parse(sides.next().expect("book length checked"))?,
            });
        }
    }

    pub async fn next_top(&mut self) -> Result<TopOfBook> {
        loop {
            let snapshot = self.next_snapshot().await?;
            let (Some(bid), Some(ask)) = (snapshot.bids.first(), snapshot.asks.first()) else {
                continue;
            };
            if bid.price >= ask.price || self.last_touch == Some((bid.price, ask.price)) {
                continue;
            }
            self.last_touch = Some((bid.price, ask.price));
            return Ok(TopOfBook {
                timestamp_ms: snapshot.timestamp_ms,
                best_bid: Some(*bid),
                best_ask: Some(*ask),
            });
        }
    }
}

pub struct HyperliquidTradesStream {
    client: HyperliquidWsClient,
    internal_symbol: String,
    venue_symbol: String,
}

impl HyperliquidTradesStream {
    pub async fn connect(symbol: &str) -> Result<Self> {
        let market = markets::market(symbol)?;
        let client = HyperliquidWsClient::subscribe([serde_json::json!({
            "type": "trades",
            "coin": market.venue_symbol,
        })])
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.symbol.clone(),
            venue_symbol: market.venue_symbol.clone(),
        })
    }

    pub async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        loop {
            let value = self.client.next_json().await?;
            if value.get("channel").and_then(Value::as_str) != Some("trades") {
                continue;
            }
            let trades: Vec<WsTrade> = serde_json::from_value(
                value
                    .get("data")
                    .cloned()
                    .context("Hyperliquid trades update omitted data")?,
            )
            .context("invalid Hyperliquid trades update")?;
            return trades
                .into_iter()
                .map(|trade| {
                    if trade.coin != self.venue_symbol {
                        bail!("Hyperliquid trade did not match the subscribed native perpetual");
                    }
                    Ok(TradeTick {
                        exchange: "hyperliquid".to_string(),
                        symbol: self.internal_symbol.clone(),
                        timestamp_ms: trade.time,
                        price: parse(&trade.px, "trade price")?,
                        size: parse(&trade.sz, "trade size")?,
                        taker_buy: trade.side.eq_ignore_ascii_case("B"),
                    })
                })
                .collect();
        }
    }
}

pub struct HyperliquidCandleStream {
    client: HyperliquidWsClient,
    venue_symbol: String,
}

impl HyperliquidCandleStream {
    pub async fn connect(symbol: &str, interval: &str) -> Result<Self> {
        let market = markets::market(symbol)?;
        let client = HyperliquidWsClient::subscribe([serde_json::json!({
            "type": "candle",
            "coin": market.venue_symbol,
            "interval": interval,
        })])
        .await?;
        Ok(Self {
            client,
            venue_symbol: market.venue_symbol.clone(),
        })
    }

    pub async fn next_candle(&mut self) -> Result<OhlcvCandle> {
        loop {
            let value = self.client.next_json().await?;
            if value.get("channel").and_then(Value::as_str) != Some("candle") {
                continue;
            }
            let candle: WsCandle = serde_json::from_value(
                value
                    .get("data")
                    .cloned()
                    .context("Hyperliquid candle update omitted data")?,
            )
            .context("invalid Hyperliquid candle update")?;
            if candle.symbol != self.venue_symbol {
                bail!("Hyperliquid candle did not match the subscribed native perpetual");
            }
            return candle.into_candle();
        }
    }
}

pub struct HyperliquidAssetContextStream {
    client: HyperliquidWsClient,
    internal_symbol: String,
    venue_symbol: String,
}

impl HyperliquidAssetContextStream {
    pub async fn connect(symbol: &str) -> Result<Self> {
        let market = markets::market(symbol)?;
        let client = HyperliquidWsClient::subscribe([serde_json::json!({
            "type": "activeAssetCtx",
            "coin": market.venue_symbol,
        })])
        .await?;
        Ok(Self {
            client,
            internal_symbol: market.symbol.clone(),
            venue_symbol: market.venue_symbol.clone(),
        })
    }

    pub async fn next_ticker(&mut self) -> Result<MarketTicker> {
        loop {
            let value = self.client.next_json().await?;
            if value.get("channel").and_then(Value::as_str) != Some("activeAssetCtx") {
                continue;
            }
            let update: WsAssetContext = serde_json::from_value(
                value
                    .get("data")
                    .cloned()
                    .context("Hyperliquid asset context omitted data")?,
            )
            .context("invalid Hyperliquid asset context")?;
            if update.coin != self.venue_symbol {
                bail!("Hyperliquid asset context did not match the subscribed native perpetual");
            }
            return update.ctx.into_ticker(&self.internal_symbol);
        }
    }
}

/// Raw authenticated account stream. Runtime reconciliation owns the mapping
/// into job/order events so public market streams stay allocation-light.
pub struct HyperliquidAccountStream {
    client: HyperliquidWsClient,
}

impl HyperliquidAccountStream {
    pub async fn connect(account: &str) -> Result<Self> {
        let client = HyperliquidWsClient::subscribe([
            serde_json::json!({ "type": "orderUpdates", "user": account }),
            serde_json::json!({ "type": "userEvents", "user": account }),
        ])
        .await?;
        Ok(Self { client })
    }

    pub async fn next_event(&mut self) -> Result<Value> {
        loop {
            let value = self.client.next_json().await?;
            if matches!(
                value.get("channel").and_then(Value::as_str),
                Some("orderUpdates" | "user")
            ) {
                return Ok(value);
            }
        }
    }
}

#[derive(Deserialize)]
struct WsBook {
    coin: String,
    levels: Vec<Vec<WsLevel>>,
    time: u64,
}

#[derive(Deserialize)]
struct WsLevel {
    px: String,
    sz: String,
}

impl WsLevel {
    fn into_level(self) -> Result<OrderBookLevel> {
        Ok(OrderBookLevel {
            price: parse(&self.px, "book price")?,
            quantity: parse(&self.sz, "book size")?,
        })
    }
}

#[derive(Deserialize)]
struct WsTrade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
}

#[derive(Deserialize)]
struct WsCandle {
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "t")]
    open_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    trades: u64,
}

impl WsCandle {
    fn into_candle(self) -> Result<OhlcvCandle> {
        Ok(OhlcvCandle {
            t: self.open_time,
            close_time: self.close_time,
            o: parse(&self.open, "candle open")?,
            h: parse(&self.high, "candle high")?,
            l: parse(&self.low, "candle low")?,
            c: parse(&self.close, "candle close")?,
            volume: parse(&self.volume, "candle volume")?,
            trades: self.trades,
        })
    }
}

#[derive(Deserialize)]
struct WsAssetContext {
    coin: String,
    ctx: WsContext,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsContext {
    funding: String,
    open_interest: String,
    prev_day_px: String,
    day_ntl_vlm: String,
    oracle_px: String,
    mark_px: String,
    mid_px: Option<String>,
}

impl WsContext {
    fn into_ticker(self, symbol: &str) -> Result<MarketTicker> {
        let mark = parse(&self.mark_px, "mark price")?;
        let previous = parse(&self.prev_day_px, "previous-day price")?;
        let last = self
            .mid_px
            .as_deref()
            .map_or(Ok(mark), |value| parse(value, "mid price"))?;
        let quote_volume = parse(&self.day_ntl_vlm, "day notional volume")?;
        Ok(MarketTicker {
            exchange: "hyperliquid".to_string(),
            symbol: symbol.to_string(),
            timestamp_ms: now_ms(),
            price_change: last - previous,
            price_change_percent: if previous == 0.0 {
                0.0
            } else {
                (last - previous) / previous * 100.0
            },
            last_price: last,
            high_price: 0.0,
            low_price: 0.0,
            volume: if mark == 0.0 {
                0.0
            } else {
                quote_volume / mark
            },
            quote_volume,
            mark_price: mark,
            oracle_price: parse(&self.oracle_px, "oracle price")?,
            open_interest: parse(&self.open_interest, "open interest")?,
            funding_rate: parse(&self.funding, "funding rate")?,
            regime: 0,
            regime_dt: 0,
            regime_vol: 0.0,
            regime_mv: 0.0,
            fair_book_price: last,
            fair_vol: 0.0,
            fair_bias: 0.0,
        })
    }
}

fn parse(value: &str, name: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .with_context(|| format!("invalid Hyperliquid {name} `{value}`"))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trading_action_request_matches_official_websocket_shape() {
        let request = trading_request(
            256,
            serde_json::json!({
                "action": { "type": "cancel", "cancels": [{ "a": 3, "o": 42 }] },
                "nonce": 1_713_825_891_591_u64,
                "signature": { "r": "0x1", "s": "0x2", "v": 28 },
                "vaultAddress": null,
            }),
        );

        assert_eq!(request["method"], "post");
        assert_eq!(request["id"], 256);
        assert_eq!(request["request"]["type"], "action");
        assert_eq!(request["request"]["payload"]["action"]["type"], "cancel");
    }

    #[test]
    fn correlated_trading_response_returns_only_the_action_payload() {
        let (response_tx, mut response_rx) = oneshot::channel();
        let mut pending = HashMap::from([(256, response_tx)]);
        let payload = serde_json::json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": { "statuses": [{ "resting": { "oid": 88383 } }] }
            }
        });

        route_trading_response(
            serde_json::json!({
                "channel": "post",
                "data": {
                    "id": 256,
                    "response": { "type": "action", "payload": payload }
                }
            }),
            &mut pending,
        );

        assert!(pending.is_empty());
        assert_eq!(
            response_rx
                .try_recv()
                .expect("response routed")
                .expect("action accepted"),
            payload
        );
    }

    #[test]
    fn correlated_trading_error_fails_only_its_request() {
        let (response_tx, mut response_rx) = oneshot::channel();
        let (other_tx, _other_rx) = oneshot::channel();
        let mut pending = HashMap::from([(7, response_tx), (8, other_tx)]);

        route_trading_response(
            serde_json::json!({
                "channel": "post",
                "data": {
                    "id": 7,
                    "response": { "type": "error", "payload": "429 Too Many Requests" }
                }
            }),
            &mut pending,
        );

        assert!(pending.contains_key(&8));
        assert!(
            response_rx
                .try_recv()
                .expect("response routed")
                .expect_err("request should fail")
                .to_string()
                .contains("429 Too Many Requests")
        );
    }

    #[test]
    fn parses_native_perp_trade() {
        let trade: WsTrade = serde_json::from_value(serde_json::json!({
            "coin": "BTC", "side": "B", "px": "65000", "sz": "0.1", "time": 1
        }))
        .expect("trade parses");
        assert_eq!(trade.coin, "BTC");
        assert_eq!(parse(&trade.sz, "size").expect("size"), 0.1);
    }

    #[test]
    fn parses_full_book_snapshot_levels() {
        let book: WsBook = serde_json::from_value(serde_json::json!({
            "coin": "BTC", "time": 10,
            "levels": [[{"px":"100","sz":"2","n":1}], [{"px":"101","sz":"3","n":1}]]
        }))
        .expect("book parses");
        assert_eq!(book.levels[0][0].px, "100");
    }
}
