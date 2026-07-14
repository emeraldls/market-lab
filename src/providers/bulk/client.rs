use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, Method};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

const BULK_API_URL: &str = "https://exchange-api.bulk.trade/api/v1";
const BULK_HTTP_TIMEOUT_SECS: u64 = 10;

#[derive(Clone)]
pub struct BulkClient {
    http: Client,
    base_url: String,
}

impl BulkClient {
    pub fn new() -> Result<Self> {
        Self::with_base_url(BULK_API_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        if base_url.is_empty() {
            bail!("BULK API base URL cannot be empty");
        }

        let http = Client::builder()
            .timeout(Duration::from_secs(BULK_HTTP_TIMEOUT_SECS))
            .build()
            .context("failed to build BULK HTTP client")?;
        Ok(Self { http, base_url })
    }

    pub async fn get<T, Q>(&self, path: &str, query: &Q) -> Result<T>
    where
        T: DeserializeOwned,
        Q: Serialize + ?Sized,
    {
        let url = self.url(path);
        let response = self
            .http
            .request(Method::GET, &url)
            .query(query)
            .send()
            .await
            .with_context(|| format!("failed to call BULK {path}"))?;
        decode_response(response, path).await
    }

    pub async fn get_without_query<T>(&self, path: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.get(path, &[] as &[(&str, &str)]).await
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }
}

async fn decode_response<T>(response: reqwest::Response, operation: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .with_context(|| format!("failed to decode BULK {operation} response"))?;
    if !status.is_success() {
        bail!(
            "BULK {operation} returned HTTP {status}: {}",
            response_message(&body)
        );
    }

    serde_json::from_value(body)
        .with_context(|| format!("BULK {operation} returned an unexpected payload"))
}

fn response_message(body: &Value) -> String {
    body.get("message")
        .and_then(Value::as_str)
        .or_else(|| body.pointer("/error/message").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_base_url_and_paths() {
        let client =
            BulkClient::with_base_url("https://example.test/api/v1/").expect("client should build");
        assert_eq!(client.url("/klines"), "https://example.test/api/v1/klines");
    }
}
