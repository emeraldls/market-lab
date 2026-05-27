use anyhow::{Context, Result, bail};

use crate::domain::types::OhlcvtCandle;

use super::utils::normalize_symbol_for_mmt;
use super::ws_client::MmtWsClient;

pub struct MmtCandlesStream {
    client: MmtWsClient,
}

impl MmtCandlesStream {
    pub async fn connect(exchange: &str, symbol: &str, tf: &str) -> Result<Self> {
        let client = MmtWsClient::shared().await?;

        let subscribe = serde_json::json!({
            "type": "subscribe",
            "channel": "candles",
            "exchange": exchange.to_lowercase(),
            "symbol": normalize_symbol_for_mmt(symbol)?,
            "tf": tf,
        });

        client
            .subscribe(subscribe)
            .await
            .context("failed to subscribe to candles channel")?;

        Ok(Self { client })
    }

    pub async fn next_candle(&mut self) -> Result<OhlcvtCandle> {
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

fn handle_ws_value(v: serde_json::Value) -> Result<Option<OhlcvtCandle>> {
    if v.get("type").and_then(|x| x.as_str()) == Some("subscribed") {
        return Ok(None);
    }
    if v.get("type").and_then(|x| x.as_str()) != Some("data") {
        return Ok(None);
    }
    if v.get("channel").and_then(|x| x.as_str()) != Some("candles") {
        return Ok(None);
    }

    let payload = v.get("data").context("candles payload missing data")?;
    let candle: OhlcvtCandle =
        serde_json::from_value(payload.clone()).context("invalid candles shape")?;
    Ok(Some(candle))
}
