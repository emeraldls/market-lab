use std::collections::VecDeque;
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{CvdArgs, OutputFormat};
use crate::domain::types::{CvdStudyResult, VdCandle, VdSeries};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws_vd::MmtVdStream;

use super::common::{StudyDescriptor, StudyEnvelope, empty_meta, print_study_json};

#[derive(Debug, Clone, Serialize)]
struct CvdStreamPoint {
    ts_s: u64,
    candle_o: f64,
    candle_h: f64,
    candle_l: f64,
    candle_c: f64,
    trades: u64,
    delta_step: f64,
    cvd_since_start: f64,
}

#[derive(Debug, Clone, Serialize)]
struct CvdInputs {
    timeframe_sec: u32,
    bucket: u8,
    from: Option<u64>,
    to: Option<u64>,
}

pub async fn handle(args: CvdArgs) -> Result<()> {
    args.validate()?;

    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        return stream_cvd(args).await;
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

    let result = to_cvd_result(series);
    render(&args, &result, args.output, args.verbose)
}

async fn stream_cvd(args: CvdArgs) -> Result<()> {
    let mut stream =
        MmtVdStream::connect(&args.exchange, &args.symbol, args.mmt_tf()?, args.bucket).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<VdCandle> = None;
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
                let point = CvdStreamPoint {
                    ts_s: c.t,
                    candle_o: c.o,
                    candle_h: c.h,
                    candle_l: c.l,
                    candle_c: c.c,
                    trades: c.n,
                    delta_step,
                    cvd_since_start,
                };
                let env = StudyEnvelope {
                    r#type: "study.cvd.result".to_string(),
                    version: "1",
                    provider: "mmt",
                    exchange: args.exchange.clone(),
                    symbol: args.symbol.clone(),
                    ts_ms: c.t * 1000,
                    stream: true,
                    study: StudyDescriptor {
                        name: "cvd".to_string(),
                        kind: "series",
                        source: "builtin",
                    },
                    inputs: CvdInputs {
                        timeframe_sec: args.timeframe,
                        bucket: args.bucket,
                        from: None,
                        to: None,
                    },
                    metrics: point,
                    meta: empty_meta(),
                };
                match args.output {
                    OutputFormat::Json | OutputFormat::Jsonl => {
                        print_study_json(&env, args.output, args.verbose)?
                    }
                    OutputFormat::Terminal => {
                        let line = format!(
                            "t={} tf={} bucket={} c={} step={} cvd={} trades={}",
                            c.t, args.mmt_tf()?, args.bucket, c.c, delta_step, cvd_since_start, c.n
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal(&buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
                prev_close = Some(c.c);
            }
        }
    }

    Ok(())
}

fn to_cvd_result(series: VdSeries) -> CvdStudyResult {
    let first_close = series.data.first().map(|x| x.c).unwrap_or(0.0);
    let last_close = series.data.last().map(|x| x.c).unwrap_or(0.0);
    CvdStudyResult {
        points: series.points,
        first_close,
        last_close,
        delta: last_close - first_close,
        candles: series.data,
    }
}

fn render(
    args: &CvdArgs,
    result: &CvdStudyResult,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    let env = StudyEnvelope {
        r#type: "study.cvd.result".to_string(),
        version: "1",
        provider: "mmt",
        exchange: args.exchange.clone(),
        symbol: args.symbol.clone(),
        ts_ms: args.to.unwrap_or_else(|| args.from.unwrap_or_default()),
        stream: false,
        study: StudyDescriptor {
            name: "cvd".to_string(),
            kind: "window",
            source: "builtin",
        },
        inputs: CvdInputs {
            timeframe_sec: args.timeframe,
            bucket: args.bucket,
            from: args.from,
            to: args.to,
        },
        metrics: result.clone(),
        meta: empty_meta(),
    };
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} CVD {} [{}-{}] bucket={} points={} first={} last={} delta={}",
                env.symbol,
                args.mmt_tf()?,
                args.from.unwrap_or_default(),
                args.to.unwrap_or_default(),
                args.bucket,
                result.points,
                result.first_close,
                result.last_close,
                result.delta
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(&env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO cvd export: {:?}", output);
        }
    }
    Ok(())
}

fn render_terminal(buf: &VecDeque<String>) -> Result<()> {
    print!("\x1B[2J\x1B[H");
    println!("market-lab study cvd stream (latest {} updates)", buf.len());
    println!("-----------------------------------------------");
    for line in buf {
        println!("{}", line);
    }
    io::stdout().flush().context("flush failed")?;
    Ok(())
}
