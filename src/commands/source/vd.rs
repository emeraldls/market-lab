use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, SourceVdArgs};
use crate::domain::enums::ProviderKind;
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws_vd::MmtVdStream;

use super::common::{SourceEnvelope, SourceMeta, render_terminal};

#[derive(Debug, Clone, Serialize)]
struct VdStreamItem {
    t: u64,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    n: u64,
    delta_step: f64,
    cvd_since_start: f64,
}

pub async fn handle(args: SourceVdArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("source vd currently supports only --provider mmt");
    }

    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        return stream_vd(args).await;
    }

    let series = MmtProvider::vd(
        &args.exchange,
        &args.symbol,
        args.mmt_tf()?,
        args.from
            .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?,
        args.to
            .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?,
        args.bucket,
    )
    .await?;

    let ts_ms = series.data.last().map(|c| c.t * 1000).unwrap_or(0);
    let env = SourceEnvelope {
        r#type: "source.vd.series".to_string(),
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
            bucket: Some(args.bucket),
            from: args.from,
            to: args.to,
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
        OutputFormat::Terminal => {
            println!(
                "{} VD tf={} bucket={} points={} from={} to={}",
                env.symbol,
                env.meta.timeframe.clone().unwrap_or_default(),
                env.meta.bucket.unwrap_or(0),
                env.data.points,
                env.meta.from.unwrap_or(0),
                env.meta.to.unwrap_or(0)
            );
        }
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source vd export: {:?}", args.output)
        }
    }

    Ok(())
}

async fn stream_vd(args: SourceVdArgs) -> Result<()> {
    let mut stream =
        MmtVdStream::connect(&args.exchange, &args.symbol, args.mmt_tf()?, args.bucket).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<crate::domain::types::VdCandle> = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);
    let mut prev_close: Option<f64> = None;
    let mut start_close: Option<f64> = None;

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
                if start_close.is_none() {
                    start_close = Some(c.c);
                }
                let delta_step = prev_close.map(|p| c.c - p).unwrap_or(0.0);
                let cvd_since_start = c.c - start_close.unwrap_or(c.c);
                let item = VdStreamItem {
                    t: c.t,
                    o: c.o,
                    h: c.h,
                    l: c.l,
                    c: c.c,
                    n: c.n,
                    delta_step,
                    cvd_since_start,
                };
                let env = SourceEnvelope {
                    r#type: "source.vd.stream".to_string(),
                    version: "1",
                    provider: "mmt",
                    exchange: args.exchange.to_lowercase(),
                    symbol: args.symbol.to_lowercase().replace("usdt","usd"),
                    ts_ms: c.t * 1000,
                    stream: true,
                    data: item,
                    meta: SourceMeta {
                        depth: None,
                        min_size: None,
                        max_size: None,
                        price_group: None,
                        interval_ms: Some(args.interval_ms),
                        timeframe: Some(args.mmt_tf()?.to_string()),
                        bucket: Some(args.bucket),
                        from: None,
                        to: None,
                    },
                };

                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format!(
                            "t={} tf={} bucket={} c={} step={} cvd={} trades={}",
                            c.t, args.mmt_tf()?, args.bucket, c.c, delta_step, cvd_since_start, c.n
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source vd stream", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
                prev_close = Some(c.c);
            }
        }
    }

    Ok(())
}
