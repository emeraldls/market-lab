use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{OutputFormat, VampArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::VampEstimate;
use crate::functions;
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
                let metrics = functions::vamp(snap, req.dollar_depth)?;
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

    let out = functions::vamp(&snapshot, req.dollar_depth)?;
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
