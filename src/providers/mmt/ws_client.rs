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
    pub async fn shared() -> Result<Self> {
        if let Some(inner) = SHARED_WS.get() {
            return Ok(Self {
                inner: Arc::clone(inner),
            });
        }

        let api_key = mmt_api_key()?;
        let ws_url = format!("{MMT_WS_URL}?api_key={}", api_key);
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .context("failed to connect websocket")?;
        let arc = Arc::new(Mutex::new(ws_stream));
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
        let Some(msg) = ws.next().await else {
            return Ok(None);
        };
        let msg = msg.context("websocket read error")?;
        let text = match msg {
            Message::Text(t) => t,
            Message::Binary(_)
            | Message::Ping(_)
            | Message::Pong(_)
            | Message::Close(_)
            | Message::Frame(_) => return Ok(Some(serde_json::Value::Null)),
        };
        let v: serde_json::Value = serde_json::from_str(&text).context("invalid websocket JSON")?;
        Ok(Some(v))
    }
}
