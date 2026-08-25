use anyhow::{Context, Result, bail};

use crate::volume::FillVolumeBatch;

const VOLUME_INGEST_KEY_HEADER: &str = "x-marketlab-ingest-key";

#[derive(Clone)]
pub struct MarketLabCloudClient {
    http: reqwest::Client,
    volume_ingest_url: reqwest::Url,
    volume_ingest_key: String,
}

impl MarketLabCloudClient {
    pub fn configured() -> Result<Option<Self>> {
        if environment_flag("MLAB_VOLUME_TELEMETRY_DISABLED") {
            return Ok(None);
        }

        let url = std::env::var("MLAB_VOLUME_INGEST_URL")
            .ok()
            .or_else(|| option_env!("MLAB_VOLUME_INGEST_URL").map(str::to_string));
        let Some(url) = url.filter(|url| !url.trim().is_empty()) else {
            return Ok(None);
        };
        let volume_ingest_url =
            reqwest::Url::parse(&url).context("MLAB_VOLUME_INGEST_URL is not a valid URL")?;
        let volume_ingest_key = std::env::var("MLAB_VOLUME_INGEST_KEY")
            .ok()
            .or_else(|| option_env!("MLAB_VOLUME_INGEST_KEY").map(str::to_string))
            .filter(|key| !key.trim().is_empty())
            .context("MLAB_VOLUME_INGEST_KEY is required when volume ingestion is configured")?;
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .context("failed to create Market Lab Cloud HTTP client")?;

        Ok(Some(Self {
            http,
            volume_ingest_url,
            volume_ingest_key,
        }))
    }

    pub async fn ingest_volume(&self, batch: &FillVolumeBatch) -> Result<()> {
        let response = self
            .http
            .post(self.volume_ingest_url.clone())
            .header(VOLUME_INGEST_KEY_HEADER, &self.volume_ingest_key)
            .json(batch)
            .send()
            .await
            .context("Market Lab Cloud volume ingestion request failed")?;
        if !response.status().is_success() {
            bail!(
                "Market Lab Cloud volume ingestion returned HTTP {}",
                response.status()
            );
        }
        Ok(())
    }
}

fn environment_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}
