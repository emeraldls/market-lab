use std::collections::VecDeque;
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cli::{CvdArgs, OutputFormat};
use crate::domain::types::{CvdStudyResult, VdCandle, VdSeries};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws_vd::MmtVdStream;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
struct CvdStreamPoint {
    exchange: String,
    symbol: String,
    tf: String,
    bucket: u8,
    ts_s: u64,
    ts_ms: u64,
    candle_o: f64,
    candle_h: f64,
    candle_l: f64,
    candle_c: f64,
    n: u64,
    delta_step: f64,
    cvd_since_start: f64,
}

pub async fn handle(args: CvdArgs) -> Result<()> {
    args.validate()?;

    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json");
        }
        return stream_cvd(args).await;
    }

    let series = MmtProvider::vd(
        &args.exchange,
        &args.symbol,
        args.mmt_tf()?,
        args.from,
        args.to,
        args.bucket,
    )
    .await?;

    let result = to_cvd_result(series);
    render(&result, args.output)
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
                    exchange: args.exchange.to_lowercase(),
                    symbol: args.symbol.to_lowercase().replace("usdt","usd"),
                    tf: args.mmt_tf()?.to_string(),
                    bucket: args.bucket,
                    ts_s: c.t,
                    ts_ms: c.t * 1000,
                    candle_o: c.o,
                    candle_h: c.h,
                    candle_l: c.l,
                    candle_c: c.c,
                    n: c.n,
                    delta_step,
                    cvd_since_start,
                };
                match args.output {
                    OutputFormat::Json => println!("{}", serde_json::to_string(&point)?),
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
        exchange: series.exchange,
        symbol: series.symbol,
        tf: series.tf,
        from: series.from,
        to: series.to,
        bucket: series.bucket,
        points: series.points,
        first_close,
        last_close,
        delta: last_close - first_close,
        candles: series.data,
    }
}

fn render(result: &CvdStudyResult, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} CVD {} [{}-{}] bucket={} points={} first={} last={} delta={}",
                result.symbol,
                result.tf,
                result.from,
                result.to,
                result.bucket,
                result.points,
                result.first_close,
                result.last_close,
                result.delta
            );
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(result)?),
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
