use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Result;

use crate::cli::{OutputFormat, SourceStatsArgs};
use crate::providers::market_data::{MarketDataAdapter, VenueTickerStream};

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceStatsArgs) -> Result<()> {
    args.validate()?;
    handle_direct(args).await
}

async fn handle_direct(args: SourceStatsArgs) -> Result<()> {
    let adapter = match args.symbol.as_deref() {
        Some(symbol) => MarketDataAdapter::for_exchange_market(&args.exchange, false, symbol)?,
        None => MarketDataAdapter::for_exchange(&args.exchange, false)?,
    };
    if args.stream {
        return stream_stats(args).await;
    }

    let stats = adapter
        .statistics(&args.period, args.symbol.as_deref())
        .await?;
    render_stats(stats, &args, adapter.exchange())
}

fn render_stats(
    stats: crate::domain::types::ExchangeStatistics,
    args: &SourceStatsArgs,
    provider: &str,
) -> Result<()> {
    let env = SourceEnvelope {
        r#type: "source.stats.snapshot".to_string(),
        version: "1",
        provider: provider.to_string(),
        exchange: stats.exchange.clone(),
        symbol: args.symbol.clone().unwrap_or_else(|| "ALL".to_string()),
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

async fn stream_stats(args: SourceStatsArgs) -> Result<()> {
    let symbol = args
        .symbol
        .as_deref()
        .expect("validation requires a symbol when streaming");
    let adapter = MarketDataAdapter::for_exchange_market(&args.exchange, false, symbol)?;
    let mut stream = VenueTickerStream::connect(&args.exchange, symbol, false).await?;
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
                    provider: adapter.exchange().to_string(),
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
                        render_terminal(&format!("market-lab source {} stats stream", adapter.label()), &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}
