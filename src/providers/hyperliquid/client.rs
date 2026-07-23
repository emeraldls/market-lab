use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::HyperliquidNetwork;

const HTTP_TIMEOUT_SECS: u64 = 20;

#[derive(Clone)]
pub struct HyperliquidClient {
    http: Client,
    base_url: String,
}

impl HyperliquidClient {
    pub fn new() -> Result<Self> {
        Self::for_network(HyperliquidNetwork::Mainnet)
    }

    pub fn for_network(network: HyperliquidNetwork) -> Result<Self> {
        Self::with_base_url(network.http_url())
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        if base_url.is_empty() {
            bail!("Hyperliquid API base URL cannot be empty");
        }
        let http = Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
            .context("failed to build Hyperliquid HTTP client")?;
        Ok(Self { http, base_url })
    }

    pub async fn info<T, B>(&self, body: &B) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let response = self
            .http
            .post(format!("{}/info", self.base_url))
            .json(body)
            .send()
            .await
            .context("failed to call Hyperliquid info API")?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .context("failed to decode Hyperliquid info response")?;
        if !status.is_success() {
            bail!("Hyperliquid info returned HTTP {status}: {body}");
        }
        serde_json::from_value(body).context("Hyperliquid info returned an unexpected payload")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_base_url() {
        let client =
            HyperliquidClient::with_base_url("https://example.test/").expect("client should build");
        assert_eq!(client.base_url, "https://example.test");
    }
}
