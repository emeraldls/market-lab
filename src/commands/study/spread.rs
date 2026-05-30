use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, SpreadArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::SpreadEstimate;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::common::{StudyDescriptor, StudyEnvelope, empty_meta, print_study_json, provider_name};
use super::realtime::{StreamRunConfig, run_mmt_realtime};

#[derive(Clone, Debug, Serialize)]
struct SpreadInputs {
    depth: u16,
}

pub async fn handle(args: SpreadArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }

        eprintln!("note: MMT spread stream uses live depth websocket");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| {
                let metrics = estimate_spread(snap)?;
                Ok(to_envelope(
                    provider_name(req.provider),
                    &req.exchange,
                    &req.symbol,
                    snap.timestamp_ms,
                    true,
                    SpreadInputs { depth: req.depth },
                    metrics,
                ))
            },
            |out| {
                format!(
                    "{} @ {} bid={} ask={} spread={} spread_bps={}",
                    out.symbol,
                    out.ts_ms,
                    out.metrics.best_bid,
                    out.metrics.best_ask,
                    out.metrics.spread_abs,
                    out.metrics.spread_bps
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
            eprintln!("note: MMT spread uses live /orderbook snapshot");
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

    let out = estimate_spread(&snapshot)?;
    let env = to_envelope(
        provider_name(req.provider),
        &req.exchange,
        &req.symbol,
        snapshot.timestamp_ms,
        false,
        SpreadInputs { depth: req.depth },
        out,
    );
    render(&env, args.output, args.verbose)
}

fn estimate_spread(book: &crate::domain::types::OrderBookSnapshot) -> Result<SpreadEstimate> {
    let best_bid = book
        .bids
        .first()
        .map(|x| x.price)
        .ok_or_else(|| anyhow::anyhow!("bids are empty"))?;
    let best_ask = book
        .asks
        .first()
        .map(|x| x.price)
        .ok_or_else(|| anyhow::anyhow!("asks are empty"))?;

    let spread_abs = best_ask - best_bid;
    let mid = (best_ask + best_bid) / 2.0;
    let spread_bps = if mid > 0.0 {
        (spread_abs / mid) * 10_000.0
    } else {
        0.0
    };

    Ok(SpreadEstimate {
        best_bid,
        best_ask,
        spread_abs,
        spread_bps,
        mid,
    })
}

fn render(
    env: &StudyEnvelope<SpreadInputs, SpreadEstimate>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            let out = &env.metrics;
            println!(
                "{} @ {} bid={} ask={} spread={} spread_bps={} mid={}",
                env.symbol,
                env.ts_ms,
                out.best_bid,
                out.best_ask,
                out.spread_abs,
                out.spread_bps,
                out.mid
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO spread export: {:?}", output);
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
    inputs: SpreadInputs,
    metrics: SpreadEstimate,
) -> StudyEnvelope<SpreadInputs, SpreadEstimate> {
    StudyEnvelope {
        r#type: "study.spread.result".to_string(),
        version: "1",
        provider,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        ts_ms: at,
        stream,
        study: StudyDescriptor {
            name: "spread".to_string(),
            kind: "snapshot",
            source: "builtin",
        },
        inputs,
        metrics,
        meta: empty_meta(),
    }
}
