use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Result, bail};

use crate::cli::{OutputFormat, SourceCandlesArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::{OhlcvSeries, OhlcvtCandle};
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::ws::BulkCandleStream;
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws_candles::MmtCandlesStream;

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

pub async fn handle(args: SourceCandlesArgs) -> Result<()> {
    args.validate()?;
    match args.provider_kind()?.into() {
        ProviderKind::Mmt => handle_mmt(args).await,
        ProviderKind::Bulk => handle_bulk(args).await,
        ProviderKind::Binance | ProviderKind::BinanceFutures => handle_binance(args).await,
        ProviderKind::MarketLab => {
            bail!("source candles does not support --provider market-lab")
        }
    }
}

async fn handle_mmt(args: SourceCandlesArgs) -> Result<()> {
    if args.stream {
        ensure_stream_output(args.output)?;
        return stream_mmt_candles(args).await;
    }

    let series = MmtProvider::candles(
        args.exchange_name()?,
        &args.symbol,
        args.timeframe_name()?,
        args.from
            .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?,
        args.to
            .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?,
    )
    .await?;

    let ts_ms = series.data.last().map(|c| c.t * 1000).unwrap_or(0);
    let env = SourceEnvelope {
        r#type: "source.candles.series".to_string(),
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
            timeframe: Some(args.timeframe_name()?.to_string()),
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
                "{} candles tf={} points={} from={} to={}",
                env.symbol,
                env.meta.timeframe.clone().unwrap_or_default(),
                env.data.points,
                env.meta.from.unwrap_or(0),
                env.meta.to.unwrap_or(0)
            );
        }
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source candles export: {:?}", args.output)
        }
    }

    Ok(())
}

async fn handle_bulk(args: SourceCandlesArgs) -> Result<()> {
    if args.stream {
        ensure_stream_output(args.output)?;
        return stream_bulk_candles(args).await;
    }

    let series = BulkProvider::candles(
        &args.symbol,
        args.timeframe_name()?,
        args.from
            .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?,
        args.to
            .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?,
    )
    .await?;
    render_bulk_series(&series, &args)
}

fn render_bulk_series(series: &OhlcvSeries, args: &SourceCandlesArgs) -> Result<()> {
    let env = SourceEnvelope {
        r#type: "source.candles.series".to_string(),
        version: "1",
        provider: "bulk",
        exchange: series.exchange.clone(),
        symbol: series.symbol.clone(),
        ts_ms: series.data.last().map(|candle| candle.t).unwrap_or(0),
        stream: false,
        data: series,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: Some(series.tf.clone()),
            bucket: None,
            from: Some(series.from),
            to: Some(series.to),
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
        OutputFormat::Terminal => println!(
            "{} BULK candles tf={} points={} from={} to={}",
            env.symbol, series.tf, series.points, series.from, series.to
        ),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source candles export: {:?}", args.output)
        }
    }
    Ok(())
}

fn ensure_stream_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("stream mode currently supports only --output terminal|json|jsonl");
    }
    Ok(())
}

async fn stream_mmt_candles(args: SourceCandlesArgs) -> Result<()> {
    let exchange = args.exchange_name()?.to_string();
    let mut stream =
        MmtCandlesStream::connect(&exchange, &args.symbol, args.timeframe_name()?).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<OhlcvtCandle> = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            c = stream.next_candle() => {
                latest = Some(c?);
            }
            _ = ticker.tick() => {
                let Some(c) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.candles.stream".to_string(),
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
                        timeframe: Some(args.timeframe_name()?.to_string()),
                        bucket: None,
                        from: None,
                        to: None,
                    },
                };

                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!(
                            "t={} o={} h={} l={} c={} vb={} vs={} tb={} ts={}",
                            c.t, c.o, c.h, c.l, c.c, c.vb, c.vs, c.tb, c.ts
                        );
                        if buf.len() >= args.buffer_size as usize {
                            buf.pop_front();
                        }
                        buf.push_back(line);
                        render_terminal("market-lab source candles stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }

    Ok(())
}

async fn stream_bulk_candles(args: SourceCandlesArgs) -> Result<()> {
    let mut stream = BulkCandleStream::connect(&args.symbol, args.timeframe_name()?).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            candle = stream.next_candle() => {
                latest = Some(candle?);
            }
            _ = ticker.tick() => {
                let Some(candle) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.candles.stream".to_string(),
                    version: "1",
                    provider: "bulk",
                    exchange: "bulk".to_string(),
                    symbol: crate::providers::bulk::catalog::market(&args.symbol)?.internal_symbol.clone(),
                    ts_ms: candle.t,
                    stream: true,
                    data: candle.clone(),
                    meta: SourceMeta {
                        depth: None,
                        min_size: None,
                        max_size: None,
                        price_group: None,
                        interval_ms: Some(args.interval_ms),
                        timeframe: Some(args.timeframe_name()?.to_string()),
                        bucket: None,
                        from: None,
                        to: None,
                    },
                };

                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!(
                            "t={} o={} h={} l={} c={} volume={} trades={}",
                            candle.t, candle.o, candle.h, candle.l, candle.c, candle.volume, candle.trades
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source BULK candles stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }
    Ok(())
}

async fn handle_binance(args: SourceCandlesArgs) -> Result<()> {
    use crate::providers::binance::market_data::BinanceProvider;
    use crate::domain::enums::ProviderKind;

    let from = args.from
        .ok_or_else(|| anyhow::anyhow!("--from is required for Binance"))?;
    let to = args.to
        .ok_or_else(|| anyhow::anyhow!("--to is required for Binance"))?;
    let tf = args.timeframe_name()?;

    let provider_kind: ProviderKind = args.provider_kind()?.into();
    let series = match provider_kind {
        ProviderKind::BinanceFutures => {
            BinanceProvider::candles_paginated_futures(&args.symbol, &tf, from, to).await?
        }
        _ => {
            BinanceProvider::candles_paginated(&args.symbol, &tf, from, to).await?
        }
    };
    
    let env = SourceEnvelope {
        r#type: "source.candles.series".to_string(),
        version: "1",
        provider: "binance",
        exchange: series.exchange.clone(),
        symbol: series.symbol.clone(),
        ts_ms: series.data.last().map(|candle| candle.t).unwrap_or(0),
        stream: false,
        data: &series,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: Some(series.tf.clone()),
            bucket: None,
            from: Some(series.from),
            to: Some(series.to),
        },
    };

    match args.output {
        OutputFormat::Terminal => {
            println!(
                "{} {} Binance candles tf={} points={} from={} to={}",
                series.symbol, series.exchange, series.tf, series.points, series.from, series.to
            );
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Jsonl => {
            let json = serde_json::to_string(&env)?;
            println!("{}", json);
            Ok(())
        }
        _ => bail!("unsupported output format for Binance candles"),
    }
}
