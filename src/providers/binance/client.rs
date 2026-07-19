use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, Method};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

const BINANCE_API_URL: &str = "https://api.binance.com/api/v3";
const BINANCE_FUTURES_API_URL: &str = "https://fapi.binance.com/fapi/v1";
const BINANCE_HTTP_TIMEOUT_SECS: u64 = 15;

#[derive(Clone)]
pub struct BinanceClient {
    http: Client,
    base_url: String,
}

impl BinanceClient {
    pub fn new() -> Result<Self> {
        Self::with_base_url(BINANCE_API_URL)
    }

    pub fn new_futures() -> Result<Self> {
        Self::with_base_url(BINANCE_FUTURES_API_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        if base_url.is_empty() {
            bail!("Binance API base URL cannot be empty");
        }

        let http = Client::builder()
            .timeout(Duration::from_secs(BINANCE_HTTP_TIMEOUT_SECS))
            .build()
            .context("failed to build Binance HTTP client")?;
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
            .header("User-Agent", "mlab/0.0.4")
            .send()
            .await
            .with_context(|| format!("failed to call Binance {path}"))?;
        decode_response(response, path).await
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    /// Direct access to the inner HTTP client for endpoints that don't use the standard query pattern.
    pub fn http(&self) -> &Client {
        &self.http
    }
}

async fn decode_response<T>(response: reqwest::Response, operation: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read Binance {operation} response body"))?;

    if !status.is_success() {
        // Binance error responses are usually JSON with a `msg`/`message` field,
        // but gateways may return HTML/text on 502/503. Fall back to raw text.
        let body: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        bail!(
            "Binance {operation} returned HTTP {status}: {}",
            response_message(&body)
        );
    }

    serde_json::from_slice(&bytes)
        .with_context(|| format!("Binance {operation} returned an unexpected payload"))
}

fn response_message(body: &Value) -> String {
    body.get("msg")
        .and_then(Value::as_str)
        .or_else(|| body.get("message").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| body.to_string())
}
