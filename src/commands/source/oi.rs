use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cli::{OutputFormat, SourceOiArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::{OiCandle, OpenInterestSnapshot};
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::ws::BulkTickerStream;
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::utils::normalize_symbol_for_mmt;
use crate::providers::mmt::ws_client::MmtWsClient;

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceOiArgs) -> Result<()> {
    args.validate()?;
    match args.provider_kind()?.into() {
        ProviderKind::Mmt => handle_mmt(args).await,
        ProviderKind::Bulk => handle_bulk(args).await,
        ProviderKind::Binance | ProviderKind::BinanceFutures => bail!("Binance provider does not support this source"),
        ProviderKind::MarketLab => bail!("source oi does not support --provider market-lab"),
    }
}

async fn handle_mmt(args: SourceOiArgs) -> Result<()> {
    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        return stream_oi(args).await;
    }

    let series = MmtProvider::oi(
        args.exchange_name()?,
        &args.symbol,
        args.mmt_tf()?,
        args.from
            .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?,
        args.to
            .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?,
    )
    .await?;

    let ts_ms = series.data.last().map(|c| c.t * 1000).unwrap_or(0);
    let env = SourceEnvelope {
        r#type: "source.oi.series".to_string(),
        version: "1",
        provider: "mmt",
        exchange: series.exchange.clone(),
        symbol: series.symbol.clone(),
        ts_ms,
        stream: false,
        data: series,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: Some(args.mmt_tf()?.to_string()),
            bucket: None,
            from: args.from,
            to: args.to,
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
        OutputFormat::Terminal => {
            println!(
                "{} OI tf={} points={} from={} to={}",
                env.symbol,
                env.meta.timeframe.clone().unwrap_or_default(),
                env.data.points,
                env.meta.from.unwrap_or(0),
                env.meta.to.unwrap_or(0)
            );
        }
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source oi export: {:?}", args.output)
        }
    }

    Ok(())
}

async fn handle_bulk(args: SourceOiArgs) -> Result<()> {
    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        return stream_bulk_oi(args).await;
    }

    let snapshot = BulkProvider::open_interest(&args.symbol).await?;
    let env = SourceEnvelope {
        r#type: "source.oi.snapshot".to_string(),
        version: "1",
        provider: "bulk",
        exchange: snapshot.exchange.clone(),
        symbol: snapshot.symbol.clone(),
        ts_ms: snapshot.timestamp_ms,
        stream: false,
        data: snapshot,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: None,
            bucket: None,
            from: None,
            to: None,
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
        OutputFormat::Terminal => println!(
            "{} open_interest={} mark={} notional={} ts={}",
            env.symbol, env.data.open_interest, env.data.mark_price, env.data.notional, env.ts_ms
        ),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source oi export: {:?}", args.output)
        }
    }
    Ok(())
}

async fn stream_oi(args: SourceOiArgs) -> Result<()> {
    let exchange = args.exchange_name()?.to_string();
    let ws = MmtWsClient::shared().await?;
    ws.subscribe(serde_json::json!({
        "type": "subscribe",
        "channel": "oi",
        "exchange": exchange.to_lowercase(),
        "symbol": normalize_symbol_for_mmt(&args.symbol)?,
        "tf": args.mmt_tf()?,
    }))
    .await
    .context("failed to subscribe to oi channel")?;

    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<OiCandle> = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            msg = ws.next_json() => {
                let Some(value) = msg? else { bail!("websocket closed by server"); };
                if let Some(candle) = parse_oi_message(value)? {
                    latest = Some(candle);
                }
            }
            _ = ticker.tick() => {
                let Some(c) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.oi.stream".to_string(),
                    version: "1",
                    provider: "mmt",
                    exchange: exchange.to_lowercase(),
                    symbol: args.symbol.to_uppercase(),
                    ts_ms: c.t * 1000,
                    stream: true,
                    data: c.clone(),
                    meta: SourceMeta {
                        depth: None,
                        min_size: None,
                        max_size: None,
                        price_group: None,
                        interval_ms: Some(args.interval_ms),
                        timeframe: Some(args.mmt_tf()?.to_string()),
                        bucket: None,
                        from: None,
                        to: None,
                    },
                };

                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!(
                            "t={} o={} h={} l={} c={} samples={}",
                            c.t, c.o, c.h, c.l, c.c, c.n
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source oi stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }

    Ok(())
}

fn parse_oi_message(value: serde_json::Value) -> Result<Option<OiCandle>> {
    if value.is_null() || value.get("type").and_then(|x| x.as_str()) == Some("subscribed") {
        return Ok(None);
    }
    if value.get("type").and_then(|x| x.as_str()) != Some("data") {
        return Ok(None);
    }
    if value.get("channel").and_then(|x| x.as_str()) != Some("oi") {
        return Ok(None);
    }
    let payload = value.get("data").context("oi payload missing data")?;
    let candle: OiCandle =
        serde_json::from_value(payload.clone()).context("invalid oi candle shape")?;
    Ok(Some(candle))
}

async fn stream_bulk_oi(args: SourceOiArgs) -> Result<()> {
    let mut stream = BulkTickerStream::connect(&args.symbol).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            update = stream.next_ticker() => {
                let update = update?;
                latest = Some(OpenInterestSnapshot {
                    exchange: update.exchange,
                    symbol: update.symbol,
                    timestamp_ms: update.timestamp_ms,
                    open_interest: update.open_interest,
                    mark_price: update.mark_price,
                    notional: update.open_interest * update.mark_price,
                });
            }
            _ = ticker.tick() => {
                let Some(snapshot) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.oi.stream".to_string(),
                    version: "1",
                    provider: "bulk",
                    exchange: snapshot.exchange.clone(),
                    symbol: snapshot.symbol.clone(),
                    ts_ms: snapshot.timestamp_ms,
                    stream: true,
                    data: snapshot.clone(),
                    meta: SourceMeta {
                        depth: None,
                        min_size: None,
                        max_size: None,
                        price_group: None,
                        interval_ms: Some(args.interval_ms),
                        timeframe: None,
                        bucket: None,
                        from: None,
                        to: None,
                    },
                };
                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!(
                            "ts={} oi={} mark={} notional={}",
                            snapshot.timestamp_ms, snapshot.open_interest, snapshot.mark_price, snapshot.notional
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source BULK open-interest stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}
