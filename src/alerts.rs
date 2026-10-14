use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

use crate::credentials::telegram::{TelegramCredential, validate_token};

fn default_cooldown() -> u64 {
    60
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlertRequest {
    pub source: String,
    pub key: String,
    pub message: String,
    #[serde(default = "default_cooldown")]
    pub cooldown_seconds: u64,
}

impl AlertRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.source == "telegram",
            "ctx.alert supports only source='telegram'"
        );
        ensure!(
            !self.key.trim().is_empty()
                && self.key.len() <= 128
                && !self.key.chars().any(char::is_control),
            "alert key must be 1-128 bytes without control characters"
        );
        ensure!(
            !self.message.trim().is_empty() && self.message.len() <= 4096,
            "alert message must be 1-4096 UTF-8 bytes"
        );
        ensure!(
            (1..=86400).contains(&self.cooldown_seconds),
            "alert cooldown_seconds must be 1-86400"
        );
        Ok(())
    }
}

pub struct TelegramClient {
    client: reqwest::Client,
    base: String,
}

impl TelegramClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| anyhow::anyhow!("could not initialize Telegram client"))?,
            base: "https://api.telegram.org".into(),
        })
    }

    async fn call(&self, token: &str, method: &str, payload: Value) -> Result<Value> {
        validate_token(token)?;
        let url = zeroize::Zeroizing::new(format!("{}/bot{token}/{method}", self.base));
        // Never propagate reqwest errors or Telegram descriptions: either may echo secrets.
        let mut response = self
            .client
            .post(url.as_str())
            .json(&payload)
            .send()
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Telegram request failed; delivery may be unknown. No automatic retry was made"
                )
            })?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            anyhow::anyhow!("Telegram response interrupted; delivery may be unknown")
        })? {
            ensure!(
                bytes.len() + chunk.len()
                    <= if method == "getUpdates" {
                        1024 * 1024
                    } else {
                        65536
                    },
                "Telegram response exceeded its size limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            anyhow::anyhow!("Telegram returned an invalid response; delivery may be unknown")
        })?;
        if !status.is_success() || value["ok"] != true {
            let code = value["error_code"]
                .as_u64()
                .unwrap_or(status.as_u16() as u64);
            match code {
                401 => bail!("Telegram rejected the bot token; run `mlab alert setup`"),
                400 | 403 => bail!(
                    "Telegram could not reach that chat. Start the bot and check its chat ID and permissions"
                ),
                429 => bail!(
                    "Telegram rate limit reached; wait {} seconds",
                    value["parameters"]["retry_after"].as_u64().unwrap_or(60)
                ),
                409 if method == "getUpdates" => bail!(
                    "Another bot connection is receiving messages. Use a dedicated alert bot or pass --chat-id"
                ),
                _ => bail!("Telegram request failed (code {code}); delivery may be unknown"),
            }
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Telegram response omitted its result"))
    }

    pub async fn verify(&self, credential: &TelegramCredential) -> Result<()> {
        credential.validate()?;
        let result = self.call(&credential.token, "getMe", json!({})).await?;
        ensure!(
            result["is_bot"] == true,
            "Telegram did not return a bot account"
        );
        Ok(())
    }

    pub async fn bot_username(&self, token: &str) -> Result<String> {
        let result = self.call(token, "getMe", json!({})).await?;
        ensure!(
            result["is_bot"] == true,
            "Telegram did not return a bot account"
        );
        let username = result["username"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Telegram bot has no username"))?;
        ensure!(
            (5..=64).contains(&username.len())
                && username
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "Telegram returned an invalid bot username"
        );
        Ok(username.into())
    }

    pub async fn wait_for_private_chat(
        &self,
        token: &str,
        code: &str,
        since: u64,
        wait: Duration,
    ) -> Result<i64> {
        let discovery = async {
            let webhook = self.call(token, "getWebhookInfo", json!({})).await?;
            ensure!(
                webhook["url"].as_str() == Some(""),
                "This bot already uses a webhook. Use a dedicated alert bot or pass --chat-id; the existing webhook was not changed"
            );
            let mut offset = 0_i64;
            loop {
                let result = self
                    .call(
                        token,
                        "getUpdates",
                        json!({"offset":offset, "limit":100, "timeout":5}),
                    )
                    .await?;
                let updates = result
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("Telegram returned invalid message updates"))?;
                for update in updates {
                    let id = update["update_id"]
                        .as_i64()
                        .filter(|id| *id >= 0)
                        .ok_or_else(|| anyhow::anyhow!("Telegram update has no valid ID"))?;
                    if id < offset {
                        continue;
                    }
                    offset = id
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("Telegram update ID exceeded its limit"))?;
                    if let Some(chat_id) = pairing_chat(update, code, since) {
                        return Ok(chat_id);
                    }
                }
                // Empty mocked/short responses must not create a busy polling loop.
                if updates.is_empty() {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        };
        tokio::time::timeout(wait, discovery).await
            .map_err(|_| anyhow::anyhow!("Telegram setup timed out. Run `mlab alert setup` again and send the new code. Existing credentials are unchanged"))?
    }

    pub(crate) async fn prepare_pairing(&self, token: &str) -> Result<()> {
        let webhook = self.call(token, "getWebhookInfo", json!({})).await?;
        ensure!(
            webhook["url"].as_str() == Some(""),
            "Use a dedicated Telegram bot for this connection. Its existing webhook was not changed"
        );
        Ok(())
    }

    pub(crate) async fn pairing_update(
        &self,
        token: &str,
        code: &str,
        since: u64,
        offset: &mut i64,
    ) -> Result<Option<i64>> {
        let result = self
            .call(
                token,
                "getUpdates",
                json!({"offset":*offset,"limit":100,"timeout":1}),
            )
            .await?;
        for update in result
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Telegram returned invalid updates"))?
        {
            let id = update["update_id"]
                .as_i64()
                .filter(|id| *id >= 0)
                .ok_or_else(|| anyhow::anyhow!("Invalid Telegram update ID"))?;
            if id < *offset {
                continue;
            }
            *offset = id
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("Invalid Telegram update ID"))?;
            if let Some(chat) = pairing_chat(update, code, since) {
                return Ok(Some(chat));
            }
        }
        Ok(None)
    }

    pub async fn send(&self, credential: &TelegramCredential, message: &str) -> Result<i64> {
        credential.validate()?;
        ensure!(
            !message.trim().is_empty() && message.len() <= 4096,
            "Telegram message must be 1-4096 UTF-8 bytes"
        );
        let result = self
            .call(
                &credential.token,
                "sendMessage",
                json!({
                    "chat_id": credential.chat_id,
                    "text": message,
                    "link_preview_options": {"is_disabled": true},
                }),
            )
            .await?;
        result["message_id"].as_i64().ok_or_else(|| {
            anyhow::anyhow!("Telegram response omitted its message ID; delivery may be unknown")
        })
    }
}

fn pairing_chat(update: &Value, code: &str, since: u64) -> Option<i64> {
    let message = update.get("message")?;
    let chat = message.get("chat")?;
    let from = message.get("from")?;
    let chat_id = chat["id"].as_i64()?;
    if chat["type"] != "private"
        || chat_id <= 0
        || from["id"].as_i64() != Some(chat_id)
        || from["is_bot"] != false
        || message["date"].as_u64()? < since
        || message.get("forward_origin").is_some()
        || message.get("forward_from").is_some()
    {
        return None;
    }
    let text = message["text"].as_str()?.trim();
    if text == code
        || text
            .strip_prefix("/start ")
            .is_some_and(|value| value.trim() == code)
    {
        Some(chat_id)
    } else {
        None
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn alert_request_is_bounded_and_disallows_credentials() {
        let good = json!({"source":"telegram", "key":"breakout", "message":"BTC crossed", "cooldown_seconds":60});
        serde_json::from_value::<AlertRequest>(good.clone())
            .unwrap()
            .validate()
            .unwrap();
        let mut bad = good.clone();
        bad["token"] = json!("secret");
        assert!(serde_json::from_value::<AlertRequest>(bad).is_err());
        for (field, value) in [
            ("source", json!("webhook")),
            ("cooldown_seconds", json!(0)),
            ("message", json!("x".repeat(4097))),
            ("key", json!("")),
        ] {
            let mut bad = good.clone();
            bad[field] = value;
            assert!(
                serde_json::from_value::<AlertRequest>(bad)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }

    pub(crate) async fn mock(
        body: &'static str,
        status: u16,
    ) -> (TelegramClient, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            loop {
                let mut buf = [0; 1024];
                let n = socket.read(&mut buf).await.unwrap();
                data.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&data);
                if let Some((header, payload)) = text.split_once("\r\n\r\n") {
                    let len: usize = header
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .parse()
                        .unwrap();
                    if payload.len() >= len {
                        break;
                    }
                }
                assert!(n > 0);
            }
            socket.write_all(format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            String::from_utf8(data).unwrap()
        });
        let mut client = TelegramClient::new().unwrap();
        client.base = base;
        (client, task)
    }

    fn credential() -> TelegramCredential {
        TelegramCredential {
            token: "123:abcdefghijklmnopqrstuvwxyz_123456789".into(),
            chat_id: -1001,
        }
    }

    #[test]
    fn pairing_requires_a_fresh_private_message_from_its_owner() {
        let valid = json!({"update_id":1,"message":{"date":100,"text":"mlab_code","chat":{"id":42,"type":"private"},"from":{"id":42,"is_bot":false}}});
        assert_eq!(pairing_chat(&valid, "mlab_code", 100), Some(42));
        let mut start = valid.clone();
        start["message"]["text"] = json!("/start mlab_code");
        assert_eq!(pairing_chat(&start, "mlab_code", 100), Some(42));
        for (pointer, value) in [
            ("/message/text", json!("wrong-code")),
            ("/message/text", json!("/start mlab_code extra")),
            ("/message/date", json!(99)),
            ("/message/chat/type", json!("group")),
            ("/message/chat/id", json!(-1001)),
            ("/message/from/id", json!(7)),
            ("/message/from/is_bot", json!(true)),
        ] {
            let mut invalid = valid.clone();
            *invalid.pointer_mut(pointer).unwrap() = value;
            assert_eq!(pairing_chat(&invalid, "mlab_code", 100), None);
        }
        let mut forwarded = valid.clone();
        forwarded["message"]["forward_origin"] = json!({"type":"user"});
        assert_eq!(pairing_chat(&forwarded, "mlab_code", 100), None);
        assert_eq!(
            pairing_chat(
                &json!({"edited_message":valid["message"]}),
                "mlab_code",
                100
            ),
            None
        );
    }

    async fn mock_sequence(
        responses: Vec<(Value, u16)>,
    ) -> (TelegramClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (body, status) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                loop {
                    let mut buf = [0; 2048];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&data);
                    if let Some((header, payload)) = text.split_once("\r\n\r\n") {
                        let length: usize = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(str::to_owned)
                            })
                            .unwrap()
                            .parse()
                            .unwrap();
                        if payload.len() >= length {
                            break;
                        }
                    }
                }
                let body = body.to_string();
                socket.write_all(format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                requests.push(String::from_utf8(data).unwrap());
            }
            requests
        });
        let mut client = TelegramClient::new().unwrap();
        client.base = base;
        (client, task)
    }

    #[tokio::test]
    async fn telegram_pairing_identifies_bot_advances_updates_and_tests_discovered_dm() {
        let (client, task) = mock_sequence(vec![
            (json!({"ok":true,"result":{"is_bot":true,"username":"marketlab_test_bot"}}), 200),
            (json!({"ok":true,"result":{"url":""}}), 200),
            (json!({"ok":true,"result":[{"update_id":7,"message":{"date":100,"text":"wrong-code","chat":{"id":42,"type":"private"},"from":{"id":42,"is_bot":false}}}]}), 200),
            (json!({"ok":true,"result":[{"update_id":8,"message":{"date":101,"text":"/start mlab_code","chat":{"id":42,"type":"private"},"from":{"id":42,"is_bot":false}}}]}), 200),
            (json!({"ok":true,"result":{"message_id":9}}), 200),
        ]).await;
        let mut credential = credential();
        assert_eq!(
            client.bot_username(&credential.token).await.unwrap(),
            "marketlab_test_bot"
        );
        credential.chat_id = client
            .wait_for_private_chat(&credential.token, "mlab_code", 100, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(credential.chat_id, 42);
        assert_eq!(client.send(&credential, "Test connected").await.unwrap(), 9);
        let requests = task.await.unwrap();
        assert!(requests[2].contains("\"offset\":0"));
        assert!(requests[3].contains("\"offset\":8"));
        assert!(requests[4].contains("\"chat_id\":42"));
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("deleteWebhook")
                    && !request.contains("allowed_updates"))
        );
    }

    #[tokio::test]
    async fn telegram_pairing_keeps_webhooks_and_rejects_conflicting_consumers() {
        for responses in [
            vec![(
                json!({"ok":true,"result":{"url":"https://existing.example/hook"}}),
                200,
            )],
            vec![
                (json!({"ok":true,"result":{"url":""}}), 200),
                (
                    json!({"ok":false,"error_code":409,"description":"TOKEN SECRET"}),
                    409,
                ),
            ],
        ] {
            let (client, task) = mock_sequence(responses).await;
            let error = client
                .wait_for_private_chat(
                    &credential().token,
                    "mlab_code",
                    100,
                    Duration::from_secs(2),
                )
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("--chat-id"));
            assert!(!error.contains("TOKEN SECRET"));
            assert!(
                task.await
                    .unwrap()
                    .iter()
                    .all(|request| !request.contains("deleteWebhook"))
            );
        }
    }

    #[tokio::test]
    async fn telegram_pairing_deadline_is_bounded() {
        // A local server that never responds isolates the outer setup deadline.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TelegramClient::new().unwrap();
        client.base = format!("http://{}", listener.local_addr().unwrap());
        let error = client
            .wait_for_private_chat(
                &credential().token,
                "mlab_code",
                100,
                Duration::from_millis(100),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("timed out"));
        drop(listener);
    }

    #[tokio::test]
    async fn telegram_sends_plain_text_to_saved_chat() {
        let (client, task) = mock(r#"{"ok":true,"result":{"message_id":42}}"#, 200).await;
        assert_eq!(
            client.send(&credential(), "BTC <crossed>").await.unwrap(),
            42
        );
        let request = task.await.unwrap();
        assert!(request.starts_with("POST /bot123:"));
        assert!(request.contains("\"chat_id\":-1001"));
        assert!(!request.contains("parse_mode"));
    }

    #[tokio::test]
    async fn telegram_errors_never_echo_response_or_token() {
        for (body, status) in [
            (
                r#"{"ok":false,"error_code":401,"description":"123:abcdefghijklmnopqrstuvwxyz_123456789"}"#,
                401,
            ),
            ("NOT JSON SECRET", 200),
            (
                r#"{"ok":false,"error_code":429,"parameters":{"retry_after":7}}"#,
                429,
            ),
        ] {
            let (client, task) = mock(body, status).await;
            let error = client
                .send(&credential(), "hello")
                .await
                .unwrap_err()
                .to_string();
            assert!(!error.contains("abcdefghijklmnopqrstuvwxyz"));
            assert!(!error.contains("SECRET"));
            task.await.unwrap();
        }
    }
}
