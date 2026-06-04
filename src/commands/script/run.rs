use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::Ordering;

use crate::cli::{OutputFormat, ScriptRunArgs, mmt_timeframe_from_seconds};
use crate::commands::script::{
    ScriptDescriptor, ScriptInputs, report_builder, write_report_best_effort,
    write_running_report_best_effort,
};
use crate::commands::source::common::render_terminal;
use crate::domain::enums::ProviderKind;
use crate::domain::types::OhlcvtCandle;
use crate::providers::mmt::ws_candles::MmtCandlesStream;
use crate::scripting::engine::Script;
use crate::scripting::inputs::{
    SourceConfigs, parse_param_values, parse_source_configs, resolve_params,
    validate_source_configs,
};
use crate::scripting::manifest::{ScriptMode, ScriptSource};

#[derive(Debug, Clone, Serialize)]
struct ScriptRunResult<I>
where
    I: Serialize,
{
    r#type: &'static str,
    version: &'static str,
    provider: &'static str,
    exchange: String,
    symbol: String,
    ts_ms: u64,
    stream: bool,
    script: ScriptDescriptor,
    params: I,
    output: ScriptRunOutput,
}

#[derive(Debug, Clone, Serialize)]
struct CompactScriptRunResult<'a, I>
where
    I: Serialize,
{
    r#type: &'static str,
    version: &'static str,
    provider: &'static str,
    exchange: &'a str,
    symbol: &'a str,
    ts_ms: u64,
    stream: bool,
    script: &'a ScriptDescriptor,
    output: &'a ScriptRunOutput,
    #[serde(skip_serializing_if = "is_empty_object")]
    params: &'a I,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptRunOutput {
    metrics: Value,
    signal: Value,
    intent: Value,
    meta: Value,
}

pub async fn handle(args: ScriptRunArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("scripts currently support only --provider mmt");
    }
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("scripts currently support only --output terminal|json|jsonl");
    }

    let script = Script::load(&args.script)?;
    let mut report = report_builder(
        "script.run",
        &script,
        Some("mmt".to_string()),
        args.exchange.clone(),
        args.symbol.clone(),
    );
    let result = run(args, script, &mut report).await;
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(err) if err.is::<ScriptCancelled>() => report.finish_cancelled(),
        Err(err) => report.finish_error(err),
    };
    write_report_best_effort(&runtime_report);
    result
}

async fn run(
    args: ScriptRunArgs,
    script: Script,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<()> {
    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical data"
        );
    }
    if !script.manifest.supports_mode(ScriptMode::Stream) {
        bail!("script does not support stream mode");
    }

    let exchange = require_non_empty(args.exchange.as_deref(), "--exchange")?.to_string();
    let symbol = require_symbol(args.symbol.as_deref())?.to_string();

    let source_configs = parse_source_configs(&args.source)?;
    validate_source_configs(&script.manifest, &source_configs)?;
    let raw_params = parse_param_values(&args.param)?;
    let resolved_params = resolve_params(&script.manifest, &raw_params)?;

    match script.manifest.sources.as_slice() {
        [ScriptSource::Candles] => {
            stream_candles(
                args,
                script,
                source_configs,
                resolved_params,
                exchange,
                symbol,
                report,
            )
            .await
        }
        _ => bail!("script run currently supports only sources=[\"candles\"]"),
    }
}

async fn stream_candles(
    args: ScriptRunArgs,
    script: Script,
    source_configs: SourceConfigs,
    resolved_params: Value,
    exchange: String,
    symbol: String,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<()> {
    let config = source_configs
        .get(&ScriptSource::Candles)
        .context("missing source config for candles")?;
    let timeframe = config.require_timeframe(&ScriptSource::Candles)?;
    let tf = mmt_timeframe_from_seconds(timeframe)?;

    report.set_phase("connecting_candles_stream");
    write_running_report_best_effort(report);
    let mut stream = MmtCandlesStream::connect(&exchange, &symbol, tf).await?;
    let session = script.start_session(&resolved_params)?;
    let cancel_handle = session.cancel_handle();
    let _cancel_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_handle.store(true, Ordering::Relaxed);
        }
    });
    let mut rendered = VecDeque::with_capacity(50);
    let mut hooks = 0_u64;

    report.set_phase("streaming_candles");
    write_running_report_best_effort(report);

    loop {
        if session.is_cancelled() {
            report.set_phase("cancelled");
            return Err(ScriptCancelled.into());
        }

        let candle = tokio::select! {
            result = stream.next_candle() => result?,
            _ = tokio::signal::ctrl_c() => {
                report.set_phase("cancelled");
                return Err(ScriptCancelled.into());
            }
        };
        let payload = candle_stream_payload(&candle)?;
        let execution = match session.run_stream(payload) {
            Ok(execution) => execution,
            Err(err) => {
                report.record_hook_failure();
                if session.is_cancelled() {
                    report.set_phase("cancelled");
                    return Err(ScriptCancelled.into());
                }
                return Err(err);
            }
        };
        hooks += 1;
        report.record_hook(&execution.stats);
        report.set_progress("streaming_candles", hooks, hooks);
        write_running_report_best_effort(report);

        let result = ScriptRunResult {
            r#type: "script.run.result",
            version: "1",
            provider: "mmt",
            exchange: exchange.clone(),
            symbol: symbol.clone(),
            ts_ms: candle.t * 1000,
            stream: true,
            script: ScriptDescriptor {
                name: script.manifest.name.clone(),
                sources: script
                    .manifest
                    .sources
                    .iter()
                    .map(ScriptSource::as_str)
                    .collect(),
            },
            params: ScriptInputs {
                values: resolved_params.clone(),
            },
            output: ScriptRunOutput {
                metrics: execution.output.metrics,
                signal: execution.output.signal,
                intent: execution.output.intent,
                meta: execution.output.meta,
            },
        };
        render_stream_result(&result, args.output, args.verbose, &mut rendered)?;
    }
}

fn candle_stream_payload(candle: &OhlcvtCandle) -> Result<Value> {
    Ok(json!({
        "mode": "stream",
        "candles": {
            "candle": candle,
        }
    }))
}

fn render_stream_result(
    result: &ScriptRunResult<ScriptInputs>,
    output: OutputFormat,
    verbose: bool,
    rendered: &mut VecDeque<String>,
) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(result)?),
        OutputFormat::Jsonl => {
            if verbose {
                println!("{}", serde_json::to_string(result)?);
            } else {
                let compact = compact_result(result);
                println!("{}", serde_json::to_string(&compact)?);
            }
        }
        OutputFormat::Terminal => {
            let signal = result
                .output
                .signal
                .get("event")
                .or_else(|| result.output.signal.get("side"))
                .and_then(Value::as_str)
                .unwrap_or("-");
            let triggered = result
                .output
                .signal
                .get("triggered")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let line = format!(
                "ts={} script={} signal={} triggered={} metrics={}",
                result.ts_ms,
                result.script.name,
                signal,
                triggered,
                serde_json::to_string(&result.output.metrics)?
            );
            if rendered.len() >= 50 {
                rendered.pop_front();
            }
            rendered.push_back(line);
            render_terminal("market-lab script run stream", rendered)?;
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn compact_result<I>(result: &ScriptRunResult<I>) -> CompactScriptRunResult<'_, I>
where
    I: Serialize,
{
    CompactScriptRunResult {
        r#type: result.r#type,
        version: result.version,
        provider: result.provider,
        exchange: &result.exchange,
        symbol: &result.symbol,
        ts_ms: result.ts_ms,
        stream: result.stream,
        script: &result.script,
        output: &result.output,
        params: &result.params,
    }
}

fn is_empty_object<I>(value: &I) -> bool
where
    I: Serialize,
{
    serde_json::to_value(value)
        .map(|value| matches!(value, Value::Object(map) if map.is_empty()))
        .unwrap_or(false)
}

#[derive(Debug)]
struct ScriptCancelled;

impl fmt::Display for ScriptCancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("script run cancelled by user")
    }
}

impl std::error::Error for ScriptCancelled {}

fn require_non_empty<'a>(value: Option<&'a str>, flag: &str) -> Result<&'a str> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{flag} is required"))?;
    if value.trim().is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(value)
}

fn require_symbol(value: Option<&str>) -> Result<&str> {
    let symbol = require_non_empty(value, "--symbol")?;
    if !symbol.contains('/') {
        bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
    }
    Ok(symbol)
}
