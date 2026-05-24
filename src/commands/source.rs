use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, SourceOrderbookArgs, SourceVdArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::{OrderBookLevel, OrderBookSnapshot};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws::MmtDepthStream;
use crate::providers::mmt::ws_vd::MmtVdStream;

#[derive(Debug, Clone, Serialize)]
struct SourceMeta {
    depth: Option<u16>,
    min_size: Option<f64>,
    max_size: Option<f64>,
    price_group: Option<f64>,
    interval_ms: Option<u64>,
    timeframe: Option<String>,
    bucket: Option<u8>,
    from: Option<u64>,
    to: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct OrderbookItem {
    side: &'static str,
    price: f64,
    size: f64,
}

#[derive(Debug, Clone, Serialize)]
struct SourceEnvelope<T> {
    source: &'static str,
    provider: &'static str,
    exchange: String,
    symbol: String,
    ts_ms: u64,
    stream: bool,
    items: T,
    meta: SourceMeta,
}

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

pub async fn handle_orderbook(args: SourceOrderbookArgs) -> Result<()> {
    args.validate()?;

    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("source orderbook currently supports only --provider mmt");
    }

    if args.stream {
        return stream_orderbook(args).await;
    }

    let snap = MmtProvider::live_orderbook(&args.exchange, &args.symbol, args.depth).await?;
    let env = build_orderbook_envelope(&snap, &args, false)?;
    render_envelope(&env, &args.output)
}

pub async fn handle_vd(args: SourceVdArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("source vd currently supports only --provider mmt");
    }

    if args.stream {
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json");
        }
        return stream_vd(args).await;
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

    let ts_ms = series.data.last().map(|c| c.t * 1000).unwrap_or(0);
    let env = SourceEnvelope {
        source: "vd",
        provider: "mmt",
        exchange: series.exchange.clone(),
        symbol: series.symbol.clone(),
        ts_ms,
        stream: false,
        items: series,
        meta: SourceMeta {
            depth: None,
            min_size: None,
            max_size: None,
            price_group: None,
            interval_ms: None,
            timeframe: Some(args.mmt_tf()?.to_string()),
            bucket: Some(args.bucket),
            from: Some(args.from),
            to: Some(args.to),
        },
    };

    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&env)?),
        OutputFormat::Terminal => {
            println!(
                "{} VD tf={} bucket={} points={} from={} to={}",
                env.symbol,
                env.meta.timeframe.clone().unwrap_or_default(),
                env.meta.bucket.unwrap_or(0),
                env.items.points,
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
                    source: "vd",
                    provider: "mmt",
                    exchange: args.exchange.to_lowercase(),
                    symbol: args.symbol.to_lowercase().replace("usdt","usd"),
                    ts_ms: c.t * 1000,
                    stream: true,
                    items: item,
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
                    OutputFormat::Json => println!("{}", serde_json::to_string(&env)?),
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

async fn stream_orderbook(args: SourceOrderbookArgs) -> Result<()> {
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("stream mode currently supports only --output terminal|json");
    }

    let state_cap = (args.depth as usize).saturating_mul(10).clamp(100, 10_000);
    let mut stream =
        MmtDepthStream::connect(&args.exchange, &args.symbol, args.depth, state_cap).await?;

    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms));
    let mut latest: Option<OrderBookSnapshot> = None;
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            snap = stream.next_snapshot() => {
                latest = Some(snap?);
            }
            _ = ticker.tick() => {
                let Some(snap) = latest.as_ref() else { continue; };
                let env = build_orderbook_envelope(snap, &args, true)?;
                match args.output {
                    OutputFormat::Json => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format_terminal_summary(&env);
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal(&buf)?;
                    }
                    OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
                }
            }
        }
    }

    Ok(())
}

fn build_orderbook_envelope(
    snap: &OrderBookSnapshot,
    args: &SourceOrderbookArgs,
    stream: bool,
) -> Result<SourceEnvelope<Vec<OrderbookItem>>> {
    let bids = filter_levels(&snap.bids, args.min_size, args.max_size)?;
    let asks = filter_levels(&snap.asks, args.min_size, args.max_size)?;

    let bids = if let Some(step) = args.price_group {
        group_levels(&bids, step, true)?
    } else {
        bids
    };
    let asks = if let Some(step) = args.price_group {
        group_levels(&asks, step, false)?
    } else {
        asks
    };

    let mut items = Vec::with_capacity(bids.len() + asks.len());
    for b in bids {
        items.push(OrderbookItem {
            side: "bid",
            price: b.price,
            size: b.quantity,
        });
    }
    for a in asks {
        items.push(OrderbookItem {
            side: "ask",
            price: a.price,
            size: a.quantity,
        });
    }

    Ok(SourceEnvelope {
        source: "orderbook",
        provider: "mmt",
        exchange: snap.exchange.clone(),
        symbol: snap.symbol.clone(),
        ts_ms: snap.timestamp_ms,
        stream,
        items,
        meta: SourceMeta {
            depth: Some(args.depth),
            min_size: args.min_size,
            max_size: args.max_size,
            price_group: args.price_group,
            interval_ms: if stream { Some(args.interval_ms) } else { None },
            timeframe: None,
            bucket: None,
            from: None,
            to: None,
        },
    })
}

fn filter_levels(
    levels: &[OrderBookLevel],
    min_size: Option<f64>,
    max_size: Option<f64>,
) -> Result<Vec<OrderBookLevel>> {
    if let (Some(min), Some(max)) = (min_size, max_size)
        && min > max
    {
        bail!("--min-size cannot be greater than --max-size");
    }

    Ok(levels
        .iter()
        .filter(|l| min_size.is_none_or(|m| l.quantity >= m))
        .filter(|l| max_size.is_none_or(|m| l.quantity <= m))
        .cloned()
        .collect())
}

fn group_levels(
    levels: &[OrderBookLevel],
    step: f64,
    bids_desc: bool,
) -> Result<Vec<OrderBookLevel>> {
    if step <= 0.0 {
        bail!("--price-group must be > 0");
    }

    let mut m: BTreeMap<i64, f64> = BTreeMap::new();
    for l in levels {
        let bucket = (l.price / step).round() as i64;
        *m.entry(bucket).or_insert(0.0) += l.quantity;
    }

    let mut out: Vec<OrderBookLevel> = m
        .into_iter()
        .map(|(k, qty)| OrderBookLevel {
            price: k as f64 * step,
            quantity: qty,
        })
        .collect();

    if bids_desc {
        out.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    } else {
        out.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    Ok(out)
}

fn format_terminal_summary(env: &SourceEnvelope<Vec<OrderbookItem>>) -> String {
    let best_bid = env.items.iter().filter(|x| x.side == "bid").max_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let best_ask = env.items.iter().filter(|x| x.side == "ask").min_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    match (best_bid, best_ask) {
        (Some(b), Some(a)) => format!(
            "ts={} bid={:.2}x{:.4} ask={:.2}x{:.4} spread={:.4} items={}",
            env.ts_ms,
            b.price,
            b.size,
            a.price,
            a.size,
            a.price - b.price,
            env.items.len()
        ),
        _ => format!("ts={} items={}", env.ts_ms, env.items.len()),
    }
}

fn render_terminal(buf: &VecDeque<String>) -> Result<()> {
    print!("\x1B[2J\x1B[H");
    println!(
        "market-lab source orderbook stream (latest {} updates)",
        buf.len()
    );
    println!("--------------------------------------------------------");
    for line in buf {
        println!("{}", line);
    }
    io::stdout().flush().context("flush failed")?;
    Ok(())
}

fn render_envelope(env: &SourceEnvelope<Vec<OrderbookItem>>, output: &OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(env)?),
        OutputFormat::Terminal => {
            println!("{}", format_terminal_summary(env));
        }
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO source orderbook export: {:?}", output);
        }
    }
    Ok(())
}
