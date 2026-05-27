use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, SlippageArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::SlippageEstimate;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::common::{StudyEnvelope, print_study_json, provider_name};
use super::realtime::{StreamRunConfig, run_mmt_realtime};

#[derive(Clone, Debug, Serialize)]
struct SlippageInputs {
    side: String,
    notional: f64,
    depth: u16,
    at: u64,
}

pub async fn handle(args: SlippageArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }

        eprintln!("note: MMT slippage stream uses live depth websocket");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| {
                let metrics =
                    estimate_slippage(snap, req.notional, snap.timestamp_ms, req.side)?;
                Ok(to_envelope(
                    provider_name(req.provider),
                    &req.exchange,
                    &req.symbol,
                    snap.timestamp_ms,
                    true,
                    SlippageInputs {
                        side: metrics.side.clone(),
                        notional: req.notional,
                        depth: req.depth,
                        at: snap.timestamp_ms,
                    },
                    metrics,
                ))
            },
            |out| {
                format!(
                    "{} {} @ {}: avg_fill={} slippage_bps={}",
                    out.metrics.symbol, out.metrics.side, out.metrics.at, out.metrics.avg_fill_price, out.metrics.slippage_bps
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
            eprintln!(
                "note: MMT slippage uses live /orderbook snapshot"
            );
            MmtProvider::live_orderbook(&req.exchange, &req.symbol, req.depth).await?
        }
        _ => {
            let client = ProviderClient::from_kind(req.provider);
            let snapshot_req = InspectRequest {
                provider: req.provider,
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                at: 0,
                depth: req.depth,
                book_mode: req.book_mode,
            };
            client.inspect(&snapshot_req).await?
        }
    };

    let estimate = estimate_slippage(&snapshot, req.notional, snapshot.timestamp_ms, req.side)?;
    let env = to_envelope(
        provider_name(req.provider),
        &req.exchange,
        &req.symbol,
        snapshot.timestamp_ms,
        false,
        SlippageInputs {
            side: estimate.side.clone(),
            notional: req.notional,
            depth: req.depth,
            at: 0,
        },
        estimate,
    );

    render(&env, args.output, args.verbose)
}

fn render(
    env: &StudyEnvelope<SlippageInputs, SlippageEstimate>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            let estimate = &env.metrics;
            println!(
                "{} {} @ {}: avg_fill={} slippage_bps={}",
                estimate.symbol,
                estimate.side,
                estimate.at,
                estimate.avg_fill_price,
                estimate.slippage_bps
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO slippage export: {:?}", output);
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
    inputs: SlippageInputs,
    metrics: SlippageEstimate,
) -> StudyEnvelope<SlippageInputs, SlippageEstimate> {
    StudyEnvelope {
        r#type: "study.slippage.result".to_string(),
        version: "1",
        provider,
        exchange: exchange.to_lowercase(),
        symbol: symbol.to_uppercase(),
        ts_ms: at,
        stream,
        inputs,
        metrics,
        meta: serde_json::json!({}),
    }
}

fn estimate_slippage(
    book: &crate::domain::types::OrderBookSnapshot,
    notional: f64,
    at: u64,
    side: crate::domain::enums::Side,
) -> Result<SlippageEstimate> {
    let levels = match side {
        crate::domain::enums::Side::Buy => &book.asks,
        crate::domain::enums::Side::Sell => &book.bids,
    };

    if levels.is_empty() {
        bail!("orderbook side is empty");
    }

    let best_price = levels[0].price;
    let mut remaining_quote = notional;
    let mut total_base = 0.0_f64;
    let mut total_quote = 0.0_f64;
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

    if remaining_quote > 0.0 {
        bail!("insufficient depth to fill notional={notional}");
    }

    if total_base <= 0.0 {
        bail!("computed base fill is zero");
    }

    let avg_fill_price = total_quote / total_base;
    let slippage_abs = match side {
        crate::domain::enums::Side::Buy => avg_fill_price - best_price,
        crate::domain::enums::Side::Sell => best_price - avg_fill_price,
    };
    let slippage_bps = if best_price > 0.0 {
        (slippage_abs / best_price) * 10_000.0
    } else {
        0.0
    };

    Ok(SlippageEstimate {
        exchange: book.exchange.clone(),
        symbol: book.symbol.clone(),
        side: match side {
            crate::domain::enums::Side::Buy => "buy".to_string(),
            crate::domain::enums::Side::Sell => "sell".to_string(),
        },
        notional,
        at,
        avg_fill_price,
        best_price,
        slippage_abs,
        slippage_bps,
        levels_consumed,
    })
}
