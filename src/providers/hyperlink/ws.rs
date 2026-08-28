use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::domain::execution::ExecutionVenue;
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::HyperliquidProduct;
use crate::providers::hyperliquid::exchange::next_nonce;
use crate::providers::hyperliquid::signing::HyperliquidWallet;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

pub struct HyperlinkAccountStream {
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    heartbeat: tokio::time::Interval,
}

impl HyperlinkAccountStream {
    pub async fn connect(
        venue: ExecutionVenue,
        account: &str,
        wallet: &HyperliquidWallet,
    ) -> Result<Self> {
        let (mut stream, _) = connect_async(super::WS_URL)
            .await
            .context("failed to connect to HyperLink account WebSocket")?;
        let product = HyperliquidProduct::from_venue(venue.market_data_id())?;
        let mut subscriptions = vec![
            serde_json::json!({ "type": "orderUpdates", "user": account }),
            serde_json::json!({ "type": "userFills", "user": account }),
        ];
        match product {
            HyperliquidProduct::Perpetual => subscriptions
                .push(serde_json::json!({ "type": "allDexsClearinghouseState", "user": account })),
            HyperliquidProduct::Spot => {
                subscriptions.push(serde_json::json!({ "type": "spotState", "user": account }))
            }
            HyperliquidProduct::Outcome => {
                bail!("HyperLink does not support outcome-market account streams")
            }
        }
        for subscription in subscriptions {
            let nonce = next_nonce()?;
            let signature =
                wallet.sign_l1_action(&subscription, nonce, HyperliquidNetwork::Mainnet)?;
            stream
                .send(Message::Text(
                    serde_json::json!({
                        "method": "subscribe",
                        "subscription": subscription,
                        "signature": signature,
                        "nonce": nonce,
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .context("failed to subscribe to HyperLink account updates")?;
        }
        let start = tokio::time::Instant::now() + HEARTBEAT_INTERVAL;
        let mut heartbeat = tokio::time::interval_at(start, HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(Self { stream, heartbeat })
    }

    pub async fn next_event(&mut self) -> Result<Value> {
        loop {
            let message = tokio::select! {
                biased;
                _ = self.heartbeat.tick() => {
                    self.stream
                        .send(Message::Text(serde_json::json!({ "method": "ping" }).to_string().into()))
                        .await
                        .context("failed to heartbeat HyperLink account WebSocket")?;
                    continue;
                }
                message = self.stream.next() => message,
            };
            let message = message.context("HyperLink account WebSocket closed")??;
            let value = match message {
                Message::Text(text) => serde_json::from_str::<Value>(&text)
                    .context("HyperLink account WebSocket returned invalid JSON")?,
                Message::Binary(bytes) => serde_json::from_slice::<Value>(&bytes)
                    .context("HyperLink account WebSocket returned invalid binary JSON")?,
                Message::Ping(payload) => {
                    self.stream
                        .send(Message::Pong(payload))
                        .await
                        .context("failed to answer HyperLink account WebSocket ping")?;
                    continue;
                }
                Message::Pong(_) | Message::Frame(_) => continue,
                Message::Close(frame) => {
                    bail!("HyperLink account WebSocket closed: {frame:?}")
                }
            };
            if let Some(value) = normalize_account_event(value)? {
                return Ok(value);
            }
        }
    }
}

fn normalize_account_event(value: Value) -> Result<Option<Value>> {
    match value.get("channel").and_then(Value::as_str) {
        Some("orderUpdates") => Ok(Some(value)),
        Some("userFills") => {
            let fills = value
                .pointer("/data/fills")
                .cloned()
                .or_else(|| value.get("data").filter(|data| data.is_array()).cloned())
                .context("HyperLink userFills update omitted fills")?;
            Ok(Some(serde_json::json!({
                "channel": "user",
                "data": { "fills": fills },
            })))
        }
        Some("allDexsClearinghouseState" | "spotState") => Ok(Some(value)),
        Some("error") => bail!("HyperLink account WebSocket error: {value}"),
        Some("pong" | "subscriptionResponse") | None => Ok(None),
        Some(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_hyperlink_fills_for_the_shared_account_runtime() {
        let value = normalize_account_event(serde_json::json!({
            "channel": "userFills",
            "data": {
                "user": "0xabc",
                "fills": [{ "coin": "BTC", "oid": 42 }]
            }
        }))
        .expect("valid fill update")
        .expect("runtime event");

        assert_eq!(value["channel"], "user");
        assert_eq!(value["data"]["fills"][0]["coin"], "BTC");
        assert_eq!(value["data"]["fills"][0]["oid"], 42);
    }

    #[test]
    fn preserves_hyperlink_order_updates() {
        let input = serde_json::json!({
            "channel": "orderUpdates",
            "data": [{ "status": "open" }]
        });
        assert_eq!(
            normalize_account_event(input.clone())
                .expect("valid order update")
                .expect("runtime event"),
            input
        );
    }

    #[test]
    fn preserves_hyperlink_product_state_updates() {
        for channel in ["allDexsClearinghouseState", "spotState"] {
            let input = serde_json::json!({
                "channel": channel,
                "data": { "user": "0xabc" }
            });
            assert_eq!(
                normalize_account_event(input.clone())
                    .expect("valid state update")
                    .expect("runtime event"),
                input
            );
        }
    }
}
