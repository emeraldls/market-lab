use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::{DepthArgs, OutputFormat};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::DepthEstimate;
use crate::functions;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::common::{StudyDescriptor, StudyEnvelope, empty_meta, print_study_json, provider_name};
use super::realtime::{StreamRunConfig, run_mmt_realtime};

#[derive(Clone, Debug, Serialize)]
struct DepthInputs {
    levels: u16,
}

pub async fn handle(args: DepthArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }

        eprintln!("note: MMT depth stream uses live depth websocket");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.levels,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| {
                let metrics = functions::depth(snap, req.levels)?;
                Ok(to_envelope(
                    provider_name(req.provider),
                    &req.exchange,
                    &req.symbol,
                    snap.timestamp_ms,
                    true,
                    DepthInputs { levels: req.levels },
                    metrics,
                ))
            },
            |out| {
                format!(
                    "{} @ {} levels={} bid_quote={} ask_quote={} total_quote={}",
                    out.symbol,
                    out.ts_ms,
                    out.inputs.levels,
                    out.metrics.bid_quote,
                    out.metrics.ask_quote,
                    out.metrics.total_quote
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
            eprintln!("note: MMT depth uses live /orderbook snapshot");
            MmtProvider::live_orderbook(&req.exchange, &req.symbol, req.levels).await?
        }
        _ => {
            let client = ProviderClient::from_kind(req.provider);
            let inspect_req = InspectRequest {
                provider: req.provider,
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                at: 0,
                depth: req.levels,
                book_mode: req.book_mode,
            };
            client.inspect(&inspect_req).await?
        }
    };

    let out = functions::depth(&snapshot, req.levels)?;
    let env = to_envelope(
        provider_name(req.provider),
        &req.exchange,
        &req.symbol,
        snapshot.timestamp_ms,
        false,
        DepthInputs { levels: req.levels },
        out,
    );
    render(&env, args.output, args.verbose)
}

fn render(
    env: &StudyEnvelope<DepthInputs, DepthEstimate>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            let out = &env.metrics;
            println!(
                "{} @ {} levels={} bid_quote={} ask_quote={} total_quote={} bid_base={} ask_base={}",
                env.symbol,
                env.ts_ms,
                env.inputs.levels,
                out.bid_quote,
                out.ask_quote,
                out.total_quote,
                out.bid_base,
                out.ask_base
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO depth export: {:?}", output);
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
    inputs: DepthInputs,
    metrics: DepthEstimate,
) -> StudyEnvelope<DepthInputs, DepthEstimate> {
    StudyEnvelope {
        r#type: "study.depth.result".to_string(),
        version: "1",
        provider,
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        ts_ms: at,
        stream,
        study: StudyDescriptor {
            name: "depth".to_string(),
            kind: "snapshot",
            source: "builtin",
        },
        inputs,
        metrics,
        meta: empty_meta(),
    }
}
