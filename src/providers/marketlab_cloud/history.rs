use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::domain::types::{HistoricalCandle, OhlcvtCandle};

pub const CANDLES_URL: &str = "https://lab.int8.me/v1/market-data/candles";
const PAGE_SIZE: usize = 5000;

#[derive(Deserialize)]
struct Page {
    exchange: String,
    symbol: String,
    tf: String,
    from: u64,
    to: u64,
    coverage_from: u64,
    coverage_to: u64,
    #[serde(deserialize_with = "required_cursor")]
    next_cursor: Option<u64>,
    data: Vec<Candle>,
}

fn required_cursor<'de, D: serde::Deserializer<'de>>(
    value: D,
) -> std::result::Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(value)
}

#[derive(Deserialize)]
struct Candle {
    unix: u64,
    close_time: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    v_buy: f64,
    v_sell: f64,
    t_buy: u64,
    t_sell: u64,
}

pub async fn candles(symbol: &str, tf: &str, from: u64, to: u64) -> Result<Vec<HistoricalCandle>> {
    if from < 1_000_000_000_000 || from >= to {
        bail!("Cloud history requires from < to in Unix milliseconds");
    }
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut result = Vec::new();
    let mut after: Option<u64> = None;
    loop {
        let mut query = vec![
            ("exchange", "hyperliquidf".to_string()),
            ("symbol", symbol.to_string()),
            ("tf", tf.to_string()),
            ("from", from.to_string()),
            ("to", to.to_string()),
            ("limit", PAGE_SIZE.to_string()),
        ];
        if let Some(cursor) = after {
            query.push(("after", cursor.to_string()));
        }
        let mut attempt = 0;
        let response = loop {
            let response = http.get(CANDLES_URL).query(&query).send().await;
            let retry = match &response {
                Ok(response) => {
                    response.status().as_u16() == 429 || response.status().is_server_error()
                }
                Err(error) => error.is_timeout() || error.is_connect(),
            };
            if retry && attempt < 3 {
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                attempt += 1;
                continue;
            }
            break response.context("Marketlab Cloud candle request failed")?;
        };
        let response = response
            .error_for_status()
            .context("Marketlab Cloud history endpoint rejected the request")?;
        let page: Page = response
            .json()
            .await
            .context("Invalid Marketlab Cloud candle response")?;
        if page.exchange != "hyperliquidf"
            || page.symbol != symbol
            || page.tf != tf
            || page.from != from
            || page.to != to
            || page.data.len() > PAGE_SIZE
        {
            bail!("Marketlab Cloud returned a different market/range or an oversized page");
        }
        if page.coverage_from > from || page.coverage_to < to {
            bail!(
                "Marketlab Cloud history for {symbol} covers {}..{} ms; requested {from}..{to} ms. Choose an available range",
                page.coverage_from,
                page.coverage_to
            );
        }
        let mut last = after;
        for row in page.data {
            let prices = [row.open, row.high, row.low, row.close];
            if row.unix < from
                || row.unix >= to
                || row.close_time <= row.unix
                || row.close_time > to
                || last.is_some_and(|last| row.unix <= last)
                || prices.iter().any(|v| !v.is_finite() || *v <= 0.0)
                || row.low > row.open.min(row.close)
                || row.high < row.open.max(row.close)
                || !row.v_buy.is_finite()
                || !row.v_sell.is_finite()
                || row.v_buy < 0.0
                || row.v_sell < 0.0
                || !(row.v_buy + row.v_sell).is_finite()
                || row.t_buy.checked_add(row.t_sell).is_none()
            {
                bail!("Invalid or out-of-order Marketlab Cloud candle for {symbol}");
            }
            last = Some(row.unix);
            result.push(HistoricalCandle::Split(
                OhlcvtCandle {
                    t: row.unix,
                    o: row.open,
                    h: row.high,
                    l: row.low,
                    c: row.close,
                    vb: row.v_buy,
                    vs: row.v_sell,
                    tb: row.t_buy,
                    ts: row.t_sell,
                },
                row.close_time,
            ));
        }
        eprintln!(
            "Marketlab Cloud: fetched {} candles for {symbol}",
            result.len()
        );
        match page.next_cursor {
            Some(cursor) if Some(cursor) == last && after != last => after = Some(cursor),
            Some(_) => bail!("Marketlab Cloud returned a non-advancing history cursor"),
            None => break,
        }
    }
    if result.is_empty() {
        bail!("Marketlab Cloud returned no completed candles for {symbol} in {from}..{to} ms");
    }
    Ok(result)
}
