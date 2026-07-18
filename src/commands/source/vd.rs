use std::collections::VecDeque;
use std::time::Duration;

use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, SourceVdArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::VolumeDeltaTick;
use crate::providers::bulk::catalog;
use crate::providers::bulk::ws::BulkTradesStream;
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
    match args.provider_kind()?.into() {
        ProviderKind::Mmt => handle_mmt(args).await,
        ProviderKind::Bulk => stream_bulk_vd(args).await,
        ProviderKind::Binance | ProviderKind::BinanceFutures => bail!("Binance provider does not support this source"),
        ProviderKind::MarketLab => {
            bail!("source vd does not support --provider market-lab")
        }
    }
}

async fn handle_mmt(args: SourceVdArgs) -> Result<()> {
    if args.stream {
        ensure_stream_output(args.output)?;
        return stream_mmt_vd(args).await;
    }

    let series = MmtProvider::vd(
        args.exchange_name()?,
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

fn ensure_stream_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("stream mode currently supports only --output terminal|json|jsonl");
    }
    Ok(())
}

async fn stream_mmt_vd(args: SourceVdArgs) -> Result<()> {
    let exchange = args.exchange_name()?.to_string();
    let mut stream =
        MmtVdStream::connect(&exchange, &args.symbol, args.mmt_tf()?, args.bucket).await?;
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
                    exchange: exchange.to_lowercase(),
                    symbol: args.symbol.to_uppercase(),
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

async fn stream_bulk_vd(args: SourceVdArgs) -> Result<()> {
    ensure_stream_output(args.output)?;
    let market = catalog::market(&args.symbol)?;
    let internal_symbol = market.internal_symbol.clone();
    let mut stream = BulkTradesStream::connect(&args.symbol).await?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<VolumeDeltaTick> = None;
    let mut cumulative_delta = 0.0;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            trades = stream.next_trades() => {
                let trades = trades?;
                if let Some(delta) = volume_delta_from_trades(&trades, &mut cumulative_delta, &internal_symbol) {
                    latest = Some(delta);
                }
            }
            _ = ticker.tick() => {
                let Some(delta) = latest.as_ref() else { continue; };
                let env = SourceEnvelope {
                    r#type: "source.vd.trades.stream".to_string(),
                    version: "1",
                    provider: "bulk",
                    exchange: "bulk".to_string(),
                    symbol: internal_symbol.clone(),
                    ts_ms: delta.timestamp_ms,
                    stream: true,
                    data: delta.clone(),
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
                    OutputFormat::Json | OutputFormat::Jsonl => {
                        println!("{}", serde_json::to_string(&env)?)
                    }
                    OutputFormat::Terminal => {
                        let line = format!(
                            "ts_ms={} delta={} cumulative_delta={}",
                            delta.timestamp_ms, delta.delta, delta.cumulative_delta
                        );
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source BULK live volume delta", &buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }

    Ok(())
}

fn volume_delta_from_trades(
    trades: &[crate::domain::types::TradeTick],
    cumulative_delta: &mut f64,
    internal_symbol: &str,
) -> Option<VolumeDeltaTick> {
    if trades.is_empty() {
        return None;
    }
    let delta = trades
        .iter()
        .map(|trade| {
            if trade.taker_buy {
                trade.size
            } else {
                -trade.size
            }
        })
        .sum::<f64>();
    *cumulative_delta += delta;
    Some(VolumeDeltaTick {
        exchange: "bulk".to_string(),
        symbol: internal_symbol.to_string(),
        timestamp_ms: trades
            .iter()
            .map(|trade| trade.timestamp_ms)
            .max()
            .unwrap_or(0),
        delta,
        cumulative_delta: *cumulative_delta,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::types::TradeTick;

    #[test]
    fn derives_side_signed_live_volume_delta() {
        let trades = vec![
            TradeTick {
                exchange: "bulk".to_string(),
                symbol: "BTC/USDT".to_string(),
                timestamp_ms: 1_700_000_000_000,
                price: 100_000.0,
                size: 0.15,
                taker_buy: true,
            },
            TradeTick {
                exchange: "bulk".to_string(),
                symbol: "BTC/USDT".to_string(),
                timestamp_ms: 1_700_000_000_001,
                price: 100_001.0,
                size: 0.05,
                taker_buy: false,
            },
        ];
        let mut cumulative = 1.0;
        let delta = volume_delta_from_trades(&trades, &mut cumulative, "BTC/USDT")
            .expect("non-empty trades yield a delta");

        assert!((delta.delta - 0.10).abs() < f64::EPSILON);
        assert!((delta.cumulative_delta - 1.10).abs() < f64::EPSILON);
        assert_eq!(delta.timestamp_ms, 1_700_000_000_001);
        assert_eq!(delta.symbol, "BTC/USDT");
    }
}
