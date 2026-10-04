use super::{PoolClient, RpcResponse};
use alloy_primitives::{Address, Bytes};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{StatusCode, header::RETRY_AFTER};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::time::Duration;

impl PoolClient {
    async fn post_rpc(&self, payload: &Value, retry_reads: bool) -> Result<Value> {
        let mut payload = payload.clone();
        let mut completed = Vec::new();
        for attempt in 0..4 {
            let response = self
                .http
                .post(self.rpc_url.clone())
                .json(&payload)
                .send()
                .await
                .map_err(reqwest::Error::without_url)
                .context("Elysium RPC request failed")?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                if !retry_reads || attempt == 3 {
                    bail!("Elysium RPC rate limited (HTTP 429 Too Many Requests)");
                }
                let delay = retry_delay(
                    response
                        .headers()
                        .get(RETRY_AFTER)
                        .and_then(|value| value.to_str().ok()),
                    attempt,
                );
                // Do not retry earlier than a provider's long cooldown permits.
                ensure!(
                    delay <= Duration::from_secs(30),
                    "Elysium RPC rate limited (HTTP 429 Too Many Requests); retry later"
                );
                drop(response);
                tokio::time::sleep(delay).await;
                continue;
            }
            let value: Value = response
                .error_for_status()
                .map_err(reqwest::Error::without_url)?
                .json()
                .await
                .context("invalid Elysium RPC response")?;
            let rate_limited = |value: &Value| {
                matches!(
                    value.pointer("/error/code").and_then(Value::as_i64),
                    Some(429 | -32005)
                ) || (value.pointer("/error/code").and_then(Value::as_i64) == Some(-32017)
                    && value
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .is_some_and(|message| message.to_ascii_lowercase().contains("rate limit")))
            };
            let limited =
                if let (Some(requests), Some(responses)) = (payload.as_array(), value.as_array()) {
                    ensure!(
                        requests.len() == responses.len(),
                        "incomplete Elysium RPC batch response"
                    );
                    let mut pending = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    for response in responses {
                        let id = response
                            .get("id")
                            .and_then(Value::as_u64)
                            .context("invalid Elysium RPC batch response identity")?;
                        ensure!(
                            response.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
                                && seen.insert(id),
                            "invalid Elysium RPC batch response identity"
                        );
                        let request = requests
                            .iter()
                            .find(|request| request["id"].as_u64() == Some(id))
                            .context("unknown Elysium RPC batch response identity")?;
                        if rate_limited(response) {
                            pending.push(request.clone());
                        } else {
                            completed.push(response.clone());
                        }
                    }
                    if pending.is_empty() {
                        return Ok(json!(completed));
                    }
                    // Retain successful reads at the pinned block; only retry rejected items.
                    payload = json!(pending);
                    true
                } else {
                    rate_limited(&value)
                };
            if !limited {
                return Ok(value);
            }
            if !retry_reads || attempt == 3 {
                bail!("Elysium RPC rate limited (429 Too Many Requests)");
            }
            tokio::time::sleep(retry_delay(None, attempt)).await;
        }
        unreachable!("all RPC attempts return or retry within the bounded loop")
    }

    pub(super) async fn rpc<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        // Never replay broadcasts or unknown methods after an ambiguous response.
        let retry_reads = matches!(
            method,
            "eth_call"
                | "eth_chainId"
                | "eth_blockNumber"
                | "eth_getBlockByNumber"
                | "eth_getBalance"
                | "eth_getCode"
                | "eth_getTransactionCount"
                | "eth_getTransactionReceipt"
                | "eth_getTransactionByHash"
                | "eth_estimateGas"
                | "eth_gasPrice"
                | "eth_maxPriorityFeePerGas"
        );
        let response: RpcResponse = serde_json::from_value(
            self.post_rpc(
                &json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }),
                retry_reads,
            )
            .await?,
        )
        .context("invalid Elysium RPC response")?;
        ensure!(
            response.id == 1 && response.jsonrpc == "2.0",
            "invalid Elysium RPC response identity"
        );
        if let Some(error) = response.error {
            bail!(
                "Elysium {method} failed ({}): {}",
                error.code,
                error.message
            );
        }
        serde_json::from_value(response.result)
            .with_context(|| format!("Elysium {method} returned an invalid result"))
    }

    pub(super) async fn calls(
        &self,
        calls: &[(Address, Vec<u8>)],
        at: &Value,
    ) -> Result<Vec<Bytes>> {
        let mut values = Vec::with_capacity(calls.len());
        // A bounded batch also works with providers that cap requests per payload.
        for chunk in calls.chunks(20) {
            let payload: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(id, (address, data))| {
                    json!({
                        "jsonrpc": "2.0", "id": id + 1, "method": "eth_call",
                        "params": [{ "to": address, "data": Bytes::copy_from_slice(data) }, at],
                    })
                })
                .collect();
            let responses: Vec<RpcResponse> =
                serde_json::from_value(self.post_rpc(&json!(payload), true).await?)
                    .context("invalid Elysium RPC batch response")?;
            values.extend(decode_batch(responses, chunk.len())?);
        }
        Ok(values)
    }
}

fn decode_batch(responses: Vec<RpcResponse>, count: usize) -> Result<Vec<Bytes>> {
    ensure!(
        responses.len() == count,
        "incomplete Elysium RPC batch response"
    );
    let mut ordered = vec![None; count];
    for response in responses {
        ensure!(
            response.jsonrpc == "2.0" && response.id > 0 && response.id <= count as u64,
            "invalid Elysium RPC batch response identity"
        );
        let slot = &mut ordered[(response.id - 1) as usize];
        ensure!(
            slot.is_none(),
            "duplicate Elysium RPC batch response identity"
        );
        if let Some(error) = response.error {
            bail!(
                "Elysium eth_call failed ({}): {}",
                error.code,
                error.message
            );
        }
        *slot = Some(
            serde_json::from_value(response.result).context("invalid Elysium eth_call result")?,
        );
    }
    ordered
        .into_iter()
        .map(|value| value.context("missing Elysium RPC batch response"))
        .collect()
}

fn retry_delay(header: Option<&str>, attempt: u32) -> Duration {
    let backoff = Duration::from_secs(1 << attempt);
    let requested = header.and_then(|value| {
        value
            .parse::<u64>()
            .ok()
            .map(Duration::from_secs)
            .or_else(|| {
                chrono::DateTime::parse_from_rfc2822(value)
                    .ok()
                    .map(|time| {
                        (time.with_timezone(&chrono::Utc) - chrono::Utc::now())
                            .to_std()
                            .unwrap_or_default()
                    })
            })
    });
    backoff.max(requested.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn response(id: u64, result: &str) -> RpcResponse {
        serde_json::from_value(json!({"jsonrpc": "2.0", "id": id, "result": result})).unwrap()
    }

    #[test]
    fn batch_matches_ids_not_response_order() {
        let values = decode_batch(vec![response(2, "0x02"), response(1, "0x01")], 2).unwrap();
        assert_eq!(values, vec![Bytes::from(vec![1]), Bytes::from(vec![2])]);
    }

    #[test]
    fn batch_rejects_missing_duplicate_and_unknown_ids() {
        assert!(decode_batch(vec![response(1, "0x01")], 2).is_err());
        assert!(decode_batch(vec![response(1, "0x01"), response(1, "0x02")], 2).is_err());
        assert!(decode_batch(vec![response(0, "0x01")], 1).is_err());
        assert!(decode_batch(vec![response(2, "0x01")], 1).is_err());
        let error = serde_json::from_value(json!({
            "jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "execution reverted"}
        }))
        .unwrap();
        assert!(
            decode_batch(vec![error], 1)
                .unwrap_err()
                .to_string()
                .contains("execution reverted")
        );
    }

    #[test]
    fn respects_retry_after_and_backs_off() {
        assert_eq!(retry_delay(None, 0), Duration::from_secs(1));
        assert_eq!(retry_delay(None, 2), Duration::from_secs(4));
        assert_eq!(retry_delay(Some("12"), 0), Duration::from_secs(12));
        assert_eq!(retry_delay(Some("0"), 1), Duration::from_secs(2));
        let future = (chrono::Utc::now() + chrono::Duration::seconds(20)).to_rfc2822();
        assert!(retry_delay(Some(&future), 0) >= Duration::from_secs(18));
    }

    async fn mock(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (PoolClient, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = Vec::new();
                loop {
                    let mut bytes = [0u8; 4096];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    buffer.extend_from_slice(&bytes[..n]);
                    if let Some(end) = buffer.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buffer[..end]).to_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if buffer.len() >= end + 4 + length {
                            requests.push(
                                serde_json::from_slice(&buffer[end + 4..end + 4 + length]).unwrap(),
                            );
                            break;
                        }
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (PoolClient::new(Some(url)).unwrap(), server)
    }

    #[tokio::test]
    async fn read_retries_http_429() {
        let (client, server) = mock(vec![
            ("429 Too Many Requests", "{}"),
            ("200 OK", r#"{"jsonrpc":"2.0","id":1,"result":"0x185d9"}"#),
        ])
        .await;
        let start = std::time::Instant::now();
        let value: String = client.rpc("eth_chainId", json!([])).await.unwrap();
        assert_eq!(value, "0x185d9");
        assert!(start.elapsed() >= Duration::from_secs(1));
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }

    #[tokio::test]
    async fn broadcast_does_not_retry_http_429() {
        let (client, server) = mock(vec![("429 Too Many Requests", "{}")]).await;
        let err = client
            .rpc::<Value>("eth_sendRawTransaction", json!(["0x1234"]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("429 Too Many Requests"));
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn calls_retry_rpc_rate_limit_and_keep_block_hash() {
        let (client, server) = mock(vec![
            (
                "200 OK",
                r#"[{"jsonrpc":"2.0","id":1,"error":{"code":-32017,"message":"Rate Limit Exceeded"}}]"#,
            ),
            ("200 OK", r#"[{"jsonrpc":"2.0","id":1,"result":"0x01"}]"#),
        ])
        .await;
        let at = json!({"blockHash": "0xabc", "requireCanonical": true});
        let values = client
            .calls(&[(Address::ZERO, vec![1, 2])], &at)
            .await
            .unwrap();
        assert_eq!(values, vec![Bytes::from(vec![1])]);
        let requests = server.await.unwrap();
        assert_eq!(requests[0], requests[1]);
        assert_eq!(requests[0][0]["params"][1], at);
    }

    #[tokio::test]
    async fn partial_batch_retries_only_rejected_calls() {
        let (client, server) = mock(vec![
            ("200 OK", r#"[{"jsonrpc":"2.0","id":1,"result":"0x01"},{"jsonrpc":"2.0","id":2,"error":{"code":-32017,"message":"Rate Limit Exceeded"}}]"#),
            ("200 OK", r#"[{"jsonrpc":"2.0","id":2,"result":"0x02"}]"#),
        ]).await;
        let values = client
            .calls(
                &[(Address::ZERO, vec![1]), (Address::ZERO, vec![2])],
                &json!("latest"),
            )
            .await
            .unwrap();
        assert_eq!(values, vec![Bytes::from(vec![1]), Bytes::from(vec![2])]);
        let requests = server.await.unwrap();
        assert_eq!(requests[1].as_array().unwrap().len(), 1);
        assert_eq!(requests[1][0], requests[0][1]);
    }

    #[tokio::test]
    async fn ordinary_rpc_error_is_not_retried() {
        let (client, server) = mock(vec![(
            "200 OK",
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"reverted"}}"#,
        )])
        .await;
        let err = client
            .rpc::<Value>("eth_call", json!([]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("reverted"));
        assert_eq!(server.await.unwrap().len(), 1);
    }
}
