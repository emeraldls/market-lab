use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{ImbalanceArgs, OutputFormat};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::ImbalanceEstimate;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::common::{StudyEnvelope, print_study_json, provider_name};
use super::realtime::{StreamRunConfig, run_mmt_realtime};

#[derive(Clone, Debug, Serialize)]
struct ImbalanceInputs {
    depth: u16,
    at: u64,
}

pub async fn handle(args: ImbalanceArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }

        eprintln!("note: MMT imbalance stream uses live depth websocket");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| {
                let metrics = estimate_imbalance(snap, snap.timestamp_ms, req.depth)?;
                Ok(to_envelope(
                    provider_name(req.provider),
                    &req.exchange,
                    &req.symbol,
                    snap.timestamp_ms,
                    true,
                    ImbalanceInputs {
                        depth: req.depth,
                        at: snap.timestamp_ms,
                    },
                    metrics,
                ))
            },
            |out| {
                format!(
                    "{} @ {} depth={} imbalance={:.6} bid_vol={} ask_vol={}",
                    out.metrics.symbol,
                    out.metrics.at,
                    out.metrics.depth,
                    out.metrics.imbalance,
                    out.metrics.bid_volume,
                    out.metrics.ask_volume
                )
            },
            |out, output| {
                Ok(match output {
                    OutputFormat::Json => serde_json::to_string_pretty(out)?,
                    OutputFormat::Jsonl => serde_json::to_string(out)?,
                    _ => unreachable!(),
                })
            },
        )
        .await;
    }

    let snapshot = match req.provider {
        ProviderKind::Mmt => {
            eprintln!("note: MMT imbalance uses live /orderbook snapshot");
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

    let estimate = estimate_imbalance(&snapshot, snapshot.timestamp_ms, req.depth)?;
    let env = to_envelope(
        provider_name(req.provider),
        &req.exchange,
        &req.symbol,
        snapshot.timestamp_ms,
        false,
        ImbalanceInputs {
            depth: req.depth,
            at: 0,
        },
        estimate,
    );
    render(&env, args.output, args.verbose)
}

fn render(
    env: &StudyEnvelope<ImbalanceInputs, ImbalanceEstimate>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            let estimate = &env.metrics;
            println!(
                "{} @ {} depth={} imbalance={:.6} bid_vol={} ask_vol={}",
                estimate.symbol,
                estimate.at,
                estimate.depth,
                estimate.imbalance,
                estimate.bid_volume,
                estimate.ask_volume
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO imbalance export: {:?}", output);
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
    inputs: ImbalanceInputs,
    metrics: ImbalanceEstimate,
) -> StudyEnvelope<ImbalanceInputs, ImbalanceEstimate> {
    StudyEnvelope {
        r#type: "study.imbalance.result".to_string(),
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

fn estimate_imbalance(
    book: &crate::domain::types::OrderBookSnapshot,
    at: u64,
    depth: u16,
) -> Result<ImbalanceEstimate> {
    if depth == 0 {
        bail!("depth must be >= 1");
    }

    let n = depth as usize;
    let bid_volume: f64 = book.bids.iter().take(n).map(|l| l.quantity).sum();
    let ask_volume: f64 = book.asks.iter().take(n).map(|l| l.quantity).sum();
    let denom = bid_volume + ask_volume;

    if denom <= 0.0 {
        bail!("empty book volumes at requested depth");
    }

    let imbalance = (bid_volume - ask_volume) / denom;

    Ok(ImbalanceEstimate {
        exchange: book.exchange.clone(),
        symbol: book.symbol.clone(),
        at,
        depth,
        bid_volume,
        ask_volume,
        imbalance,
    })
}
