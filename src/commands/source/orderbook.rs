use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, SourceOrderbookArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::types::{OrderBookLevel, OrderBookSnapshot};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws::MmtDepthStream;

use super::common::{SourceEnvelope, SourceMeta, render_json_or_terminal, render_terminal};

#[derive(Debug, Clone, Serialize)]
struct OrderbookItem {
    side: &'static str,
    price: f64,
    size: f64,
}

pub async fn handle(args: SourceOrderbookArgs) -> Result<()> {
    args.validate()?;

    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("source orderbook currently supports only --provider mmt");
    }

    if args.stream {
        return stream_orderbook(args).await;
    }

    let snap = MmtProvider::live_orderbook(&args.exchange, &args.symbol, args.depth).await?;
    let env = build_orderbook_envelope(&snap, &args, false)?;
    render_json_or_terminal(
        &env,
        &args.output,
        format_terminal_summary,
        "source orderbook",
    )
}

async fn stream_orderbook(args: SourceOrderbookArgs) -> Result<()> {
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("stream mode currently supports only --output terminal|json|jsonl");
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
                    OutputFormat::Json | OutputFormat::Jsonl => println!("{}", serde_json::to_string(&env)?),
                    OutputFormat::Terminal => {
                        let line = format_terminal_summary(&env);
                        if buf.len() >= args.buffer_size as usize { buf.pop_front(); }
                        buf.push_back(line);
                        render_terminal("market-lab source orderbook stream", &buf)?;
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
        r#type: if stream {
            "source.orderbook.stream".to_string()
        } else {
            "source.orderbook.snapshot".to_string()
        },
        version: "1",
        provider: "mmt",
        exchange: snap.exchange.clone(),
        symbol: snap.symbol.clone(),
        ts_ms: snap.timestamp_ms,
        stream,
        data: items,
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
    let best_bid = env.data.iter().filter(|x| x.side == "bid").max_by(|a, b| {
        a.price
            .partial_cmp(&b.price)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let best_ask = env.data.iter().filter(|x| x.side == "ask").min_by(|a, b| {
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
            env.data.len()
        ),
        _ => format!("ts={} items={}", env.ts_ms, env.data.len()),
    }
}
