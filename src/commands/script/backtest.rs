use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;

use crate::cli::{OutputFormat, ScriptBacktestArgs};
use crate::commands::script::{
    ScriptDescriptor, ScriptInputs, report_builder, write_report_best_effort,
};
use crate::commands::study::common::{is_empty_object, provider_name};
use crate::domain::enums::ProviderKind;
use crate::providers::mmt::MmtProvider;
use crate::scripting::engine::Script;
use crate::scripting::inputs::{parse_kv_inputs, resolve_inputs};
use crate::scripting::manifest::{ScriptMode, ScriptSource};

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestResult<I>
where
    I: Serialize,
{
    r#type: &'static str,
    version: &'static str,
    provider: &'static str,
    exchange: String,
    symbol: String,
    ts_ms: u64,
    script: ScriptDescriptor,
    window: ScriptWindow,
    inputs: I,
    performance: ScriptBacktestPerformance,
    latest_output: ScriptBacktestLatestOutput,
    meta: Value,
}

#[derive(Debug, Serialize)]
struct CompactScriptBacktestResult<'a, I>
where
    I: Serialize,
{
    r#type: &'static str,
    version: &'static str,
    provider: &'static str,
    exchange: &'a str,
    symbol: &'a str,
    ts_ms: u64,
    script: &'a ScriptDescriptor,
    performance: &'a ScriptBacktestPerformance,
    #[serde(skip_serializing_if = "is_empty_object")]
    inputs: &'a I,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptWindow {
    from: u64,
    to: u64,
    timeframe_sec: u32,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ScriptBacktestPerformance {
    trades: usize,
    sharpe: Option<f64>,
    max_drawdown: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestLatestOutput {
    metrics: Value,
    signal: Value,
    intent: Value,
}

pub async fn handle(args: ScriptBacktestArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("scripts currently support only --provider mmt");
    }
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("script backtest currently supports only --output terminal|json|jsonl");
    }

    let script = Script::load(&args.script)?;
    let mut report = report_builder(
        "script.backtest",
        &script,
        Some(provider_name(args.provider.into()).to_string()),
        Some(args.exchange.clone()),
        Some(args.symbol.clone()),
    );
    if !script.manifest.supports_mode(ScriptMode::Window) {
        let err = anyhow::anyhow!("script does not support window mode");
        let runtime_report = report.finish_error(&err);
        write_report_best_effort(&runtime_report);
        return Err(err);
    }

    let raw_inputs = match parse_kv_inputs(&args.input) {
        Ok(raw_inputs) => raw_inputs,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };
    let resolved_inputs = match resolve_inputs(&script.manifest, &raw_inputs) {
        Ok(resolved_inputs) => resolved_inputs,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };

    let result = match script.manifest.source {
        ScriptSource::Candles => {
            backtest_candles_window(args, script, resolved_inputs, &mut report).await
        }
    };
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(err) => report.finish_error(err),
    };
    write_report_best_effort(&runtime_report);
    result
}

async fn backtest_candles_window(
    args: ScriptBacktestArgs,
    script: Script,
    resolved_inputs: Value,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<()> {
    let series = MmtProvider::candles(
        &args.exchange,
        &args.symbol,
        args.mmt_tf()?,
        args.from,
        args.to,
    )
    .await?;

    if series.data.len() < 2 {
        bail!("script backtest requires at least 2 candles");
    }

    let mut returns = Vec::new();
    let mut trades = 0_usize;
    let mut position = 0.0_f64;
    let mut saw_strategy_like_output = false;
    let mut latest_output = None;

    for idx in 0..(series.data.len() - 1) {
        let candles_json =
            serde_json::to_value(&series.data[..=idx]).context("failed to encode candles")?;
        let execution = match script.run_candles_window(&resolved_inputs, &candles_json) {
            Ok(execution) => execution,
            Err(err) => {
                report.record_hook_failure();
                return Err(err);
            }
        };
        report.record_hook(&execution.stats);
        let output = execution.output;

        if !is_empty_json_object(&output.signal) || !is_empty_json_object(&output.intent) {
            saw_strategy_like_output = true;
        }

        if let Some(next_position) = position_from_output(&output.signal, &output.intent) {
            trades += 1;
            position = next_position;
        }

        let curr = series.data[idx].c;
        let next = series.data[idx + 1].c;
        let denom = curr.abs().max(1.0);
        returns.push(position * ((next - curr) / denom));
        latest_output = Some(ScriptBacktestLatestOutput {
            metrics: output.metrics,
            signal: output.signal,
            intent: output.intent,
        });
    }

    if !saw_strategy_like_output {
        bail!("script backtest requires strategy-like output: return `signal` or `intent`");
    }

    let performance = ScriptBacktestPerformance {
        trades,
        sharpe: sharpe(&returns),
        max_drawdown: max_drawdown(&returns),
    };
    let result = ScriptBacktestResult {
        r#type: "script.backtest.result",
        version: "1",
        provider: provider_name(args.provider.into()),
        exchange: args.exchange.clone(),
        symbol: args.symbol.clone(),
        ts_ms: series.to,
        script: ScriptDescriptor {
            name: script.manifest.name.clone(),
            source: "candles",
        },
        window: ScriptWindow {
            from: args.from,
            to: args.to,
            timeframe_sec: args.timeframe,
        },
        inputs: ScriptInputs {
            values: resolved_inputs,
        },
        performance,
        latest_output: latest_output.unwrap_or(ScriptBacktestLatestOutput {
            metrics: serde_json::json!({}),
            signal: serde_json::json!({}),
            intent: serde_json::json!({}),
        }),
        meta: serde_json::json!({}),
    };

    render_backtest(&result, args.output, args.verbose)
}

fn position_from_output(signal: &Value, intent: &Value) -> Option<f64> {
    let triggered = signal
        .get("triggered")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| !is_empty_json_object(intent));
    if !triggered {
        return None;
    }

    let side = intent
        .get("side")
        .or_else(|| signal.get("side"))
        .and_then(Value::as_str)?;
    match side {
        "buy" | "long" => Some(1.0),
        "sell" | "short" => Some(-1.0),
        _ => None,
    }
}

fn is_empty_json_object(value: &Value) -> bool {
    matches!(value, Value::Object(map) if map.is_empty())
}

fn render_backtest(
    result: &ScriptBacktestResult<ScriptInputs>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} tf={} [{}-{}]",
                result.symbol, result.window.timeframe_sec, result.window.from, result.window.to
            );
            println!("script: {}", result.script.name);
            println!(
                "trades={} sharpe={:.4?} max_drawdown={:.4?}",
                result.performance.trades,
                result.performance.sharpe,
                result.performance.max_drawdown
            );
            if verbose {
                println!(
                    "latest_output: {}",
                    serde_json::to_string_pretty(&result.latest_output)?
                );
            }
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_backtest_json(result, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn print_backtest_json<I>(
    result: &ScriptBacktestResult<I>,
    output: OutputFormat,
    verbose: bool,
) -> Result<()>
where
    I: Serialize,
{
    if verbose {
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(result)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(result)?),
            _ => unreachable!(),
        }
    } else {
        let compact = CompactScriptBacktestResult {
            r#type: result.r#type,
            version: result.version,
            provider: result.provider,
            exchange: &result.exchange,
            symbol: &result.symbol,
            ts_ms: result.ts_ms,
            script: &result.script,
            performance: &result.performance,
            inputs: &result.inputs,
        };
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&compact)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(&compact)?),
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn sharpe(returns: &[f64]) -> Option<f64> {
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let var = returns
        .iter()
        .map(|r| {
            let d = r - mean;
            d * d
        })
        .sum::<f64>()
        / (returns.len() as f64 - 1.0);
    let std = var.sqrt();
    if std <= f64::EPSILON {
        None
    } else {
        Some((mean / std) * (returns.len() as f64).sqrt())
    }
}

fn max_drawdown(returns: &[f64]) -> Option<f64> {
    if returns.is_empty() {
        return None;
    }
    let mut equity = 1.0_f64;
    let mut peak = 1.0_f64;
    let mut max_dd = 0.0_f64;

    for r in returns {
        equity *= 1.0 + r;
        if equity > peak {
            peak = equity;
        }
        let dd = if peak > 0.0 {
            (peak - equity) / peak
        } else {
            0.0
        };
        if dd > max_dd {
            max_dd = dd;
        }
    }

    Some(max_dd)
}
