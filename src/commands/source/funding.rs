use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Result;

use crate::cli::{OutputFormat, SourceFundingArgs};
use crate::domain::types::FundingRateSnapshot;
use crate::providers::market_data::{MarketDataAdapter, VenueTickerStream};

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceFundingArgs) -> Result<()> {
    args.validate()?;
    handle_direct(args).await
}

async fn handle_direct(args: SourceFundingArgs) -> Result<()> {
    let adapter = MarketDataAdapter::for_exchange_market(&args.exchange, false, &args.symbol)?;
    if args.stream {
        return stream_funding(args).await;
    }

    let funding = adapter.funding(&args.symbol).await?;
    render_funding(funding, &args, adapter.exchange())
}

fn render_funding(
    funding: FundingRateSnapshot,
    args: &SourceFundingArgs,
    provider: &str,
) -> Result<()> {
    let env = SourceEnvelope {
        r#type: "source.funding.snapshot".to_string(),
        version: "1",
        provider: provider.to_string(),
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

async fn stream_funding(args: SourceFundingArgs) -> Result<()> {
    let adapter = MarketDataAdapter::for_exchange_market(&args.exchange, false, &args.symbol)?;
    let mut stream = VenueTickerStream::connect(&args.exchange, &args.symbol, false).await?;
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
                latest = Some(adapter.funding_from_ticker(update));
            }
            _ = ticker.tick() => {
                let Some(snapshot) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.funding.stream".to_string(),
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
                        render_terminal(&format!("market-lab source {} funding stream", adapter.label()), &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}
