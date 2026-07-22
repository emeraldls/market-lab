use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Result;

use crate::cli::{OutputFormat, SourceFundingArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::FundingRateSnapshot;
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::ws::BulkTickerStream;
use crate::providers::hyperliquid::market_data::HyperliquidProvider;
use crate::providers::hyperliquid::ws::HyperliquidAssetContextStream;

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceFundingArgs) -> Result<()> {
    args.validate()?;
    match args.provider_kind()?.into() {
        ProviderKind::Bulk => handle_bulk(args).await,
        ProviderKind::Hyperliquid => handle_hyperliquid(args).await,
        ProviderKind::Mmt | ProviderKind::MarketLab => {
            unreachable!("funding source is standalone-only")
        }
    }
}

async fn handle_bulk(args: SourceFundingArgs) -> Result<()> {
    if args.stream {
        return stream_bulk_funding(args).await;
    }

    let funding = BulkProvider::funding(&args.symbol).await?;
    render_funding(funding, &args, "bulk")
}

async fn handle_hyperliquid(args: SourceFundingArgs) -> Result<()> {
    if args.stream {
        return stream_hyperliquid_funding(args).await;
    }
    let funding = HyperliquidProvider::funding(&args.symbol).await?;
    render_funding(funding, &args, "hyperliquid")
}

fn render_funding(
    funding: FundingRateSnapshot,
    args: &SourceFundingArgs,
    provider: &'static str,
) -> Result<()> {
    let env = SourceEnvelope {
        r#type: "source.funding.snapshot".to_string(),
        version: "1",
        provider,
        exchange: funding.exchange.clone(),
        symbol: funding.symbol.clone(),
        ts_ms: funding.timestamp_ms,
        stream: false,
        data: funding,
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
            "{} funding_rate={} annualized={} ts={}",
            env.symbol,
            env.data.current,
            env.data
                .annualized
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_string()),
            env.ts_ms
        ),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source funding export: {:?}", args.output)
        }
    }
    Ok(())
}

async fn stream_bulk_funding(args: SourceFundingArgs) -> Result<()> {
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
                latest = Some(FundingRateSnapshot {
                    exchange: update.exchange,
                    symbol: update.symbol,
                    timestamp_ms: update.timestamp_ms,
                    current: update.funding_rate,
                    annualized: None,
                });
            }
            _ = ticker.tick() => {
                let Some(snapshot) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.funding.stream".to_string(),
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
                        let line = format!("ts={} funding_rate={}", snapshot.timestamp_ms, snapshot.current);
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source BULK funding stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}

async fn stream_hyperliquid_funding(args: SourceFundingArgs) -> Result<()> {
    let mut stream = HyperliquidAssetContextStream::connect(&args.symbol).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest = None;
    let mut buf = VecDeque::with_capacity(args.buffer_size as usize);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            update = stream.next_ticker() => {
                let update = update?;
                latest = Some(FundingRateSnapshot {
                    exchange: update.exchange, symbol: update.symbol,
                    timestamp_ms: update.timestamp_ms, current: update.funding_rate,
                    annualized: Some(update.funding_rate * 24.0 * 365.0),
                });
            }
            _ = ticker.tick() => {
                let Some(snapshot) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.funding.stream".to_string(), version: "1",
                    provider: "hyperliquid", exchange: snapshot.exchange.clone(),
                    symbol: snapshot.symbol.clone(), ts_ms: snapshot.timestamp_ms,
                    stream: true, data: snapshot.clone(),
                    meta: SourceMeta { depth: None, min_size: None, max_size: None, price_group: None,
                        interval_ms: Some(args.interval_ms), timeframe: None, bucket: None, from: None, to: None },
                };
                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!("ts={} funding_rate={}", snapshot.timestamp_ms, snapshot.current);
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source Hyperliquid funding stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}
