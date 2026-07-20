use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::credentials::mmt_api_key;

const MMT_WS_URL: &str = "wss://eu-central-1.mmt.gg/api/v1/ws";

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct MmtWsClient {
    inner: Arc<Mutex<WsStream>>,
}

static SHARED_WS: OnceLock<Arc<Mutex<WsStream>>> = OnceLock::new();

impl MmtWsClient {
    pub async fn connect() -> Result<Self> {
        let api_key = mmt_api_key()?;
        let ws_url = format!("{MMT_WS_URL}?api_key={}", api_key);
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .context("failed to connect websocket")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(ws_stream)),
        })
    }

    pub async fn shared() -> Result<Self> {
        if let Some(inner) = SHARED_WS.get() {
            return Ok(Self {
                inner: Arc::clone(inner),
            });
        }

        let client = Self::connect().await?;
        let arc = Arc::clone(&client.inner);
        let _ = SHARED_WS.set(Arc::clone(&arc));

        let inner = SHARED_WS
            .get()
            .expect("shared websocket should be initialized")
            .clone();
        Ok(Self { inner })
    }

    pub async fn subscribe(&self, payload: serde_json::Value) -> Result<()> {
        let mut ws = self.inner.lock().await;
        ws.send(Message::Text(payload.to_string().into()))
            .await
            .context("failed to send websocket subscribe message")?;
        Ok(())
    }

    pub async fn next_json(&self) -> Result<Option<serde_json::Value>> {
        let mut ws = self.inner.lock().await;
        loop {
            let Some(msg) = ws.next().await else {
                return Ok(None);
            };
            let msg = msg.context("websocket read error")?;
            match msg {
                Message::Text(text) => {
                    return serde_json::from_str(&text)
                        .context("invalid websocket JSON")
                        .map(Some);
                }
                Message::Binary(bytes) => {
                    return serde_json::from_slice(&bytes)
                        .context("invalid websocket binary JSON")
                        .map(Some);
                }
                Message::Ping(payload) => {
                    ws.send(Message::Pong(payload))
                        .await
                        .context("failed to answer websocket ping")?;
                }
                Message::Close(_) => return Ok(None),
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}
