use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Result, bail};

use crate::cli::{OutputFormat, SourceStatsArgs};
use crate::domain::enums::ProviderKind;
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::ws::BulkTickerStream;
use crate::providers::hyperliquid::market_data::HyperliquidProvider;
use crate::providers::hyperliquid::ws::HyperliquidAssetContextStream;

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceStatsArgs) -> Result<()> {
    args.validate()?;
    match args.provider_kind()?.into() {
        ProviderKind::Bulk => handle_bulk(args).await,
        ProviderKind::Hyperliquid => handle_hyperliquid(args).await,
        ProviderKind::Binance | ProviderKind::BinanceFutures => {
            bail!("Binance statistics are not implemented")
        }
        ProviderKind::Mmt | ProviderKind::MarketLab => {
            unreachable!("statistics source is standalone-only")
        }
    }
}

async fn handle_bulk(args: SourceStatsArgs) -> Result<()> {
    if args.stream {
        return stream_bulk_stats(args).await;
    }

    let stats = BulkProvider::statistics(&args.period, args.symbol.as_deref()).await?;
    render_stats(stats, &args, "bulk")
}

async fn handle_hyperliquid(args: SourceStatsArgs) -> Result<()> {
    if args.stream {
        return stream_hyperliquid_stats(args).await;
    }
    let stats = HyperliquidProvider::statistics(&args.period, args.symbol.as_deref()).await?;
    render_stats(stats, &args, "hyperliquid")
}

fn render_stats(
    stats: crate::domain::types::ExchangeStatistics,
    args: &SourceStatsArgs,
    provider: &'static str,
) -> Result<()> {
    let env = SourceEnvelope {
        r#type: "source.stats.snapshot".to_string(),
        version: "1",
        provider,
        exchange: stats.exchange.clone(),
        symbol: args
            .symbol
            .clone()
            .unwrap_or_else(|| "ALL/USDT".to_string()),
        ts_ms: stats.timestamp_ms,
        stream: false,
        data: stats,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: Some(args.period.clone()),
            bucket: None,
            from: None,
            to: None,
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
        OutputFormat::Terminal => println!(
            "{} stats period={} markets={} volume_usd={} oi_usd={} ts={}",
            provider,
            env.data.period,
            env.data.markets.len(),
            env.data.total_volume_usd,
            env.data.total_open_interest_usd,
            env.ts_ms
        ),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source stats export: {:?}", args.output)
        }
    }
    Ok(())
}

async fn stream_bulk_stats(args: SourceStatsArgs) -> Result<()> {
    let symbol = args
        .symbol
        .as_deref()
        .expect("validation requires a symbol when streaming");
    let mut stream = BulkTickerStream::connect(symbol).await?;
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
                latest = Some(update?);
            }
            _ = ticker.tick() => {
                let Some(snapshot) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.stats.stream".to_string(),
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
                        timeframe: Some("24h".to_string()),
                        bucket: None,
                        from: None,
                        to: None,
                    },
                };
                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!(
                            "ts={} last={} mark={} volume={} oi={} funding={}",
                            snapshot.timestamp_ms,
                            snapshot.last_price,
                            snapshot.mark_price,
                            snapshot.volume,
                            snapshot.open_interest,
                            snapshot.funding_rate
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source BULK stats stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}

async fn stream_hyperliquid_stats(args: SourceStatsArgs) -> Result<()> {
    let symbol = args
        .symbol
        .as_deref()
        .expect("validation requires a symbol when streaming");
    let mut stream = HyperliquidAssetContextStream::connect(symbol).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest = None;
    let mut buf = VecDeque::with_capacity(args.buffer_size as usize);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            update = stream.next_ticker() => latest = Some(update?),
            _ = ticker.tick() => {
                let Some(snapshot) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.stats.stream".to_string(), version: "1",
                    provider: "hyperliquid", exchange: snapshot.exchange.clone(),
                    symbol: snapshot.symbol.clone(), ts_ms: snapshot.timestamp_ms,
                    stream: true, data: snapshot.clone(),
                    meta: SourceMeta { depth: None, min_size: None, max_size: None, price_group: None,
                        interval_ms: Some(args.interval_ms), timeframe: Some("24h".to_string()),
                        bucket: None, from: None, to: None },
                };
                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!("ts={} last={} mark={} volume={} oi={} funding={}", snapshot.timestamp_ms, snapshot.last_price, snapshot.mark_price, snapshot.volume, snapshot.open_interest, snapshot.funding_rate);
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source Hyperliquid stats stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}
