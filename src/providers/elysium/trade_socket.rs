//! Filtered JSON-RPC subscriptions. Reconnection and replay use the trade stream checkpoint.
use std::collections::VecDeque;
use std::time::Duration;

use alloy_primitives::{Address, B256, U64};
use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use url::Url;

use super::Block;
use super::market_data::TradeLog;

const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_EARLY_MESSAGES: usize = 1024;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) enum Notification {
    Log(TradeLog),
    Head(Block),
}

pub(super) struct TradeSocket {
    socket: Socket,
    logs: String,
    heads: String,
    early: VecDeque<Value>,
}

pub(super) fn websocket_url(rpc: &Url, configured: Option<&str>) -> Result<Url> {
    if let Some(configured) = configured {
        let url = Url::parse(configured).context("invalid MLAB_ELYSIUM_WS_URL")?;
        ensure!(
            matches!(url.scheme(), "ws" | "wss"),
            "MLAB_ELYSIUM_WS_URL must use ws or wss"
        );
        return Ok(url);
    }
    let mut url = rpc.clone();
    let scheme = match rpc.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => bail!("Elysium RPC URL must use http or https"),
    };
    url.set_scheme(scheme)
        .map_err(|_| anyhow::anyhow!("invalid Elysium WebSocket URL"))?;
    Ok(url)
}

impl TradeSocket {
    pub(super) async fn connect(
        url: &Url,
        chain: u64,
        address: Address,
        topics: Vec<B256>,
    ) -> Result<Self> {
        tokio::time::timeout(TIMEOUT, Self::subscribe(url, chain, address, topics))
            .await
            .context("Elysium WebSocket subscription timed out")?
    }

    async fn subscribe(url: &Url, chain: u64, address: Address, topics: Vec<B256>) -> Result<Self> {
        let config = WebSocketConfig::default()
            .max_message_size(Some(1024 * 1024))
            .max_frame_size(Some(1024 * 1024));
        // Do not include a potentially authenticated RPC URL in errors.
        let (socket, _) = connect_async_with_config(url.as_str(), Some(config), false)
            .await
            .map_err(|error| match error {
                tokio_tungstenite::tungstenite::Error::Http(response) => anyhow::anyhow!(
                    "Elysium WebSocket handshake rejected (HTTP {}); check MLAB_ELYSIUM_WS_URL and provider access",
                    response.status()
                ),
                _ => anyhow::anyhow!("cannot connect to Elysium WebSocket; configure MLAB_ELYSIUM_WS_URL"),
            })?;
        let mut client = Self {
            socket,
            logs: String::new(),
            heads: String::new(),
            early: VecDeque::new(),
        };
        let actual: U64 =
            serde_json::from_value(client.request(1, "eth_chainId", json!([])).await?)?;
        ensure!(
            actual.to::<u64>() == chain,
            "Elysium HTTP and WebSocket endpoints use different chains"
        );
        client.logs = serde_json::from_value(
            client
                .request(
                    2,
                    "eth_subscribe",
                    json!(["logs", {
                        "address": address, "topics": [topics],
                    }]),
                )
                .await?,
        )
        .context("invalid log subscription id")?;
        client.heads = serde_json::from_value(
            client
                .request(3, "eth_subscribe", json!(["newHeads"]))
                .await?,
        )
        .context("invalid head subscription id")?;
        ensure!(
            !client.logs.is_empty() && !client.heads.is_empty() && client.logs != client.heads,
            "invalid Elysium subscription ids"
        );
        Ok(client)
    }

    async fn request(&mut self, id: u64, method: &str, params: Value) -> Result<Value> {
        self.socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
                    .to_string()
                    .into(),
            ))
            .await?;
        loop {
            let value = receive(&mut self.socket).await?;
            if value.get("id") == Some(&json!(id)) {
                if let Some(error) = value.get("error") {
                    bail!("Elysium WebSocket {method} failed: {error}");
                }
                return value
                    .get("result")
                    .cloned()
                    .context("missing WebSocket RPC result");
            }
            ensure!(
                self.early.len() < MAX_EARLY_MESSAGES,
                "Elysium subscription buffer overflow; reconnect to recover"
            );
            self.early.push_back(value);
        }
    }

    pub(super) async fn next(&mut self) -> Result<Notification> {
        let value = match self.early.pop_front() {
            Some(value) => value,
            None => tokio::time::timeout(TIMEOUT, receive(&mut self.socket))
                .await
                .context("Elysium WebSocket is idle; reconnect to recover")??,
        };
        ensure!(
            value["method"] == "eth_subscription",
            "unexpected Elysium WebSocket message"
        );
        let params = &value["params"];
        if params["subscription"] == self.logs {
            return Ok(Notification::Log(serde_json::from_value(
                params["result"].clone(),
            )?));
        }
        if params["subscription"] == self.heads {
            return Ok(Notification::Head(serde_json::from_value(
                params["result"].clone(),
            )?));
        }
        bail!("unknown Elysium WebSocket subscription");
    }
}

async fn receive(socket: &mut Socket) -> Result<Value> {
    loop {
        match socket
            .next()
            .await
            .context("Elysium WebSocket closed; reconnect to recover")??
        {
            Message::Text(text) => return Ok(serde_json::from_str(&text)?),
            Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
            Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await?,
            Message::Pong(_) => {}
            Message::Close(_) => bail!("Elysium WebSocket closed; reconnect to recover"),
            Message::Frame(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_public_rpc_and_preserves_http_override() {
        let client = super::super::PoolClient::new(None).unwrap();
        assert_eq!(
            client.rpc_url.as_str(),
            "https://elysium-testnet-rpc.hypedexer.com/"
        );
        assert_eq!(
            websocket_url(&client.rpc_url, None).unwrap().as_str(),
            "wss://elysium-testnet-rpc.hypedexer.com/"
        );

        let custom = Url::parse("http://localhost:8545").unwrap();
        let client = super::super::PoolClient::new(Some(custom.clone())).unwrap();
        assert_eq!(client.rpc_url, custom);
    }

    #[test]
    fn derives_ws_url_and_accepts_separate_endpoint() {
        assert_eq!(
            websocket_url(
                &Url::parse("https://rpc.example/path?key=abc").unwrap(),
                None
            )
            .unwrap()
            .as_str(),
            "wss://rpc.example/path?key=abc"
        );
        assert_eq!(
            websocket_url(&Url::parse("http://localhost:8545").unwrap(), None)
                .unwrap()
                .scheme(),
            "ws"
        );
        assert_eq!(
            websocket_url(
                &Url::parse("https://rpc.example").unwrap(),
                Some("wss://ws.example")
            )
            .unwrap()
            .host_str(),
            Some("ws.example")
        );
        assert!(
            websocket_url(
                &Url::parse("https://rpc.example").unwrap(),
                Some("https://ws.example")
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod socket_tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn filtered_subscriptions_keep_notifications_received_during_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let address = Address::repeat_byte(1);
        let topic = B256::repeat_byte(2);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            for id in 1..=3 {
                let request: Value =
                    serde_json::from_slice(&ws.next().await.unwrap().unwrap().into_data()).unwrap();
                assert_eq!(request["id"], id);
                if id == 2 {
                    assert_eq!(request["method"], "eth_subscribe");
                    assert_eq!(
                        request["params"],
                        json!(["logs", {"address": address, "topics":[[topic]]}])
                    );
                }
                if id == 3 {
                    assert_eq!(request["params"], json!(["newHeads"]));
                    let notification = json!({"method":"eth_subscription", "params":{"subscription":"logs", "result":{
                        "address":address, "topics":[topic], "data":"0x", "blockNumber":"0x10", "blockHash":B256::repeat_byte(3),
                        "transactionHash":B256::repeat_byte(4), "transactionIndex":"0x0", "logIndex":"0x1", "removed":false
                    }}});
                    ws.send(Message::Text(notification.to_string().into()))
                        .await
                        .unwrap();
                }
                let result = match id {
                    1 => "0x185d9",
                    2 => "logs",
                    _ => "heads",
                };
                ws.send(Message::Text(
                    json!({"id":id, "result":result}).to_string().into(),
                ))
                .await
                .unwrap();
            }
            ws.send(Message::Text(
                json!({"method":"eth_subscription", "params":{"subscription":"heads", "result":{
                    "number":"0x11", "hash":B256::repeat_byte(5), "timestamp":"0x123"
                }}})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
            ws.close(None).await.unwrap();
        });
        let mut client = TradeSocket::connect(&url, 99801, address, vec![topic])
            .await
            .unwrap();
        assert!(matches!(client.next().await.unwrap(), Notification::Log(_)));
        assert!(matches!(
            client.next().await.unwrap(),
            Notification::Head(_)
        ));
        assert!(client.next().await.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_error_reports_status_without_endpoint_credentials() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "ws://{}/private-api-key",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).await.unwrap() > 0);
            socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let error = TradeSocket::connect(&url, 99801, Address::repeat_byte(1), vec![])
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("401"));
        assert!(!error.contains("private-api-key"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_a_websocket_on_another_chain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(
                json!({"id":1,"result":"0x1"}).to_string().into(),
            ))
            .await
            .unwrap();
        });
        assert!(
            TradeSocket::connect(&url, 99801, Address::repeat_byte(1), vec![])
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("different chains")
        );
        server.await.unwrap();
    }
}
