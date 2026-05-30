use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;

use crate::cli::{CustomStudyRunArgs, OutputFormat};
use crate::domain::enums::ProviderKind;
use crate::providers::mmt::MmtProvider;
use crate::scripting::engine::StudyScript;
use crate::scripting::inputs::{parse_kv_inputs, resolve_inputs};
use crate::scripting::manifest::{StudyMode, StudySource};

use super::common::{StudyDescriptor, StudyEnvelope, print_study_json, provider_name};

#[derive(Debug, Clone, Serialize)]
struct CustomStudyInputs {
    #[serde(flatten)]
    values: Value,
}

pub async fn handle_run(args: CustomStudyRunArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("custom studies currently support only --provider mmt");
    }
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("custom studies currently support only --output terminal|json|jsonl");
    }

    let script = StudyScript::load(&args.script)?;
    if args.stream && !script.manifest.supports_mode(StudyMode::Stream) {
        bail!("study does not support stream mode");
    }
    if !args.stream && !script.manifest.supports_mode(StudyMode::Window) {
        bail!("study does not support window mode");
    }
    if args.stream {
        bail!("custom study stream execution is not implemented yet");
    }

    let raw_inputs = parse_kv_inputs(&args.input)?;
    let resolved_inputs = resolve_inputs(&script.manifest, &raw_inputs)?;

    match script.manifest.source {
        StudySource::Candles => run_candles_window(args, script, resolved_inputs).await,
    }
}

async fn run_candles_window(
    args: CustomStudyRunArgs,
    script: StudyScript,
    resolved_inputs: Value,
) -> Result<()> {
    let series = MmtProvider::candles(
        &args.exchange,
        &args.symbol,
        args.mmt_tf()?,
        args.from,
        args.to,
    )
    .await?;

    let candles_json = serde_json::to_value(&series.data).context("failed to encode candles")?;
    let output = script.run_candles_window(&resolved_inputs, &candles_json)?;

    let env = StudyEnvelope {
        r#type: format!("study.{}.result", script.manifest.name),
        version: "1",
        provider: provider_name(args.provider.into()),
        exchange: args.exchange.clone(),
        symbol: args.symbol.clone(),
        ts_ms: series.to,
        stream: false,
        study: StudyDescriptor {
            name: script.manifest.name.clone(),
            kind: "window",
            source: "custom",
        },
        inputs: CustomStudyInputs {
            values: resolved_inputs,
        },
        metrics: output.metrics,
        meta: output.meta,
    };

    render(&env, args.output, args.verbose)
}

fn render(
    env: &StudyEnvelope<CustomStudyInputs, Value>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            let metrics = serde_json::to_string_pretty(&env.metrics)?;
            println!(
                "{} study={} @ {}\n{}",
                env.symbol, env.study.name, env.ts_ms, metrics
            );
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_study_json(env, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}
