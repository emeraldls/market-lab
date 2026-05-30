use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, VampArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::VampEstimate;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::common::{StudyDescriptor, StudyEnvelope, empty_meta, print_study_json, provider_name};
use super::realtime::{StreamRunConfig, run_mmt_realtime};

#[derive(Clone, Debug, Serialize)]
struct VampInputs {
    depth: u16,
    dollar_depth: f64,
}

pub async fn handle(args: VampArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }

        eprintln!("note: MMT vamp stream uses live depth websocket");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| {
                let metrics = estimate_vamp(snap, req.dollar_depth)?;
                Ok(to_envelope(
                    provider_name(req.provider),
                    &req.exchange,
                    &req.symbol,
                    snap.timestamp_ms,
                    true,
                    VampInputs {
                        depth: req.depth,
                        dollar_depth: req.dollar_depth,
                    },
                    metrics,
                ))
            },
            |out| {
                format!(
                    "{} @ {} depth=${}: vamp={} (bid_vwap={}, ask_vwap={}) complete={} max_bid_quote={} max_ask_quote={}",
                    out.symbol,
                    out.ts_ms,
                    out.inputs.dollar_depth,
                    out.metrics.vamp,
                    out.metrics.bid_vwap,
                    out.metrics.ask_vwap,
                    out.metrics.complete,
                    out.metrics.max_reachable_quote_bid,
                    out.metrics.max_reachable_quote_ask,
                )
            },
            |out, output| Ok(match output {
                OutputFormat::Json => serde_json::to_string_pretty(out)?,
                OutputFormat::Jsonl => serde_json::to_string(out)?,
                _ => unreachable!(),
            }),
        )
        .await;
    }

    let snapshot = match req.provider {
        ProviderKind::Mmt => {
            eprintln!("note: MMT vamp uses live /orderbook snapshot");
            MmtProvider::live_orderbook(&req.exchange, &req.symbol, req.depth).await?
        }
        _ => {
            let client = ProviderClient::from_kind(req.provider);
            let inspect_req = InspectRequest {
                provider: req.provider,
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                at: 0,
                depth: req.depth,
                book_mode: req.book_mode,
            };
            client.inspect(&inspect_req).await?
        }
    };

    let out = estimate_vamp(&snapshot, req.dollar_depth)?;
    let env = to_envelope(
        provider_name(req.provider),
        &req.exchange,
        &req.symbol,
        snapshot.timestamp_ms,
        false,
        VampInputs {
            depth: req.depth,
            dollar_depth: req.dollar_depth,
        },
        out,
    );
    render(&env, args.output, args.verbose)
}

fn render(
    env: &StudyEnvelope<VampInputs, VampEstimate>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            let out = &env.metrics;
            println!(
                "{} @ {} depth=${}: vamp={} (bid_vwap={}, ask_vwap={}) complete={} max_bid_quote={} max_ask_quote={}",
                env.symbol,
                env.ts_ms,
                env.inputs.dollar_depth,
                out.vamp,
                out.bid_vwap,
                out.ask_vwap,
                out.complete,
                out.max_reachable_quote_bid,
                out.max_reachable_quote_ask,
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO vamp export: {:?}", output);
        }
    }

    Ok(())
}

fn to_envelope(
    provider: &'static str,
    exchange: &str,
    symbol: &str,
    at: u64,
    stream: bool,
    inputs: VampInputs,
    metrics: VampEstimate,
) -> StudyEnvelope<VampInputs, VampEstimate> {
    StudyEnvelope {
        r#type: "study.vamp.result".to_string(),
        version: "1",
        provider,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        ts_ms: at,
        stream,
        study: StudyDescriptor {
            name: "vamp".to_string(),
            kind: "snapshot",
            source: "builtin",
        },
        inputs,
        metrics,
        meta: empty_meta(),
    }
}

fn estimate_vamp(
    book: &crate::domain::types::OrderBookSnapshot,
    dollar_depth: f64,
) -> Result<VampEstimate> {
    if dollar_depth <= 0.0 {
        bail!("dollar_depth must be > 0");
    }

    let max_reachable_quote_ask = total_quote_capacity(&book.asks);
    let max_reachable_quote_bid = total_quote_capacity(&book.bids);

    let ask_fill = side_vwap_for_quote_notional(&book.asks, dollar_depth)?;
    let bid_fill = side_vwap_for_quote_notional(&book.bids, dollar_depth)?;

    let ask_vwap = ask_fill.vwap;
    let bid_vwap = bid_fill.vwap;
    let vamp = (ask_vwap + bid_vwap) / 2.0;

    let complete = ask_fill.filled_quote >= dollar_depth && bid_fill.filled_quote >= dollar_depth;

    Ok(VampEstimate {
        ask_vwap,
        bid_vwap,
        vamp,
        ask_levels_consumed: ask_fill.levels_consumed,
        bid_levels_consumed: bid_fill.levels_consumed,
        max_reachable_quote_ask,
        max_reachable_quote_bid,
        complete,
    })
}

fn total_quote_capacity(levels: &[crate::domain::types::OrderBookLevel]) -> f64 {
    levels.iter().map(|l| l.price * l.quantity).sum()
}

struct SideFill {
    vwap: f64,
    levels_consumed: u16,
    filled_quote: f64,
}

fn side_vwap_for_quote_notional(
    levels: &[crate::domain::types::OrderBookLevel],
    target_quote: f64,
) -> Result<SideFill> {
    if levels.is_empty() {
        bail!("orderbook side is empty");
    }

    let mut remaining_quote = target_quote;
    let mut total_quote = 0.0_f64;
    let mut total_base = 0.0_f64;
    let mut levels_consumed = 0_u16;

    for level in levels {
        if remaining_quote <= 0.0 {
            break;
        }

        let level_quote_capacity = level.price * level.quantity;
        let take_quote = remaining_quote.min(level_quote_capacity);
        let take_base = take_quote / level.price;

        total_quote += take_quote;
        total_base += take_base;
        remaining_quote -= take_quote;
        levels_consumed += 1;
    }

    if total_base <= 0.0 {
        bail!("computed base fill is zero");
    }

    Ok(SideFill {
        vwap: total_quote / total_base,
        levels_consumed,
        filled_quote: total_quote,
    })
}
