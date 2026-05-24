use anyhow::{Context, Result, bail};

use crate::domain::types::VdCandle;

use super::utils::normalize_symbol_for_mmt;
use super::ws_client::MmtWsClient;

pub struct MmtVdStream {
    client: MmtWsClient,
}

impl MmtVdStream {
    pub async fn connect(exchange: &str, symbol: &str, tf: &str, bucket: u8) -> Result<Self> {
        let client = MmtWsClient::shared().await?;

        let subscribe = serde_json::json!({
            "type": "subscribe",
            "channel": "vd",
            "exchange": exchange.to_lowercase(),
            "symbol": normalize_symbol_for_mmt(symbol)?,
            "tf": tf,
            "bucket": bucket,
        });

        client
            .subscribe(subscribe)
            .await
            .context("failed to subscribe to vd channel")?;

        Ok(Self { client })
    }

    pub async fn next_candle(&mut self) -> Result<VdCandle> {
        loop {
            let Some(v) = self.client.next_json().await? else {
                bail!("websocket closed by server");
            };
            if v.is_null() {
                continue;
            }
            if let Some(c) = handle_ws_value(v)? {
                return Ok(c);
            }
        }
    }
}

fn handle_ws_value(v: serde_json::Value) -> Result<Option<VdCandle>> {
    if v.get("type").and_then(|x| x.as_str()) == Some("subscribed") {
        return Ok(None);
    }
    if v.get("type").and_then(|x| x.as_str()) != Some("data") {
        return Ok(None);
    }
    if v.get("channel").and_then(|x| x.as_str()) != Some("vd") {
        return Ok(None);
    }

    let payload = v.get("data").context("vd payload missing data")?;
    let candle: VdCandle =
        serde_json::from_value(payload.clone()).context("invalid vd candle shape")?;
    Ok(Some(candle))
}
