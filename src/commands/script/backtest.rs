use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::fmt;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::cli::{OutputFormat, ScriptBacktestArgs, mmt_timeframe_from_seconds};
use crate::commands::script::{
    ScriptDescriptor, ScriptInputs, report_builder, write_report_best_effort,
    write_running_report_best_effort,
};
use crate::commands::study::common::{is_empty_object, provider_name};
use crate::domain::enums::ProviderKind;
use crate::domain::types::{OhlcvtCandle, OrderBookSnapshot};
use crate::providers::mmt::MmtProvider;
use crate::scripting::engine::Script;
use crate::scripting::inputs::{
    SourceConfigs, parse_param_values, parse_source_configs, resolve_params,
    validate_source_configs,
};
use crate::scripting::limits::SCRIPT_DEFAULT_LOOKBACK_CANDLES;
use crate::scripting::manifest::{ScriptMode, ScriptSource};
use tokio::task::JoinHandle;

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
    params: I,
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
    params: &'a I,
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

#[derive(Default)]
struct BacktestData {
    candles: Option<Vec<OhlcvtCandle>>,
    orderbooks: Option<Vec<OrderBookSnapshot>>,
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

    let source_configs = match parse_source_configs(&args.source) {
        Ok(configs) => configs,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };
    if let Err(err) = validate_source_configs(&script.manifest, &source_configs) {
        let runtime_report = report.finish_error(&err);
        write_report_best_effort(&runtime_report);
        return Err(err);
    }

    let raw_params = match parse_param_values(&args.param) {
        Ok(raw_params) => raw_params,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };
    let resolved_params = match resolve_params(&script.manifest, &raw_params) {
        Ok(resolved_params) => resolved_params,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };

    let result = backtest_window(args, script, source_configs, resolved_params, &mut report).await;
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(err) if err.is::<ScriptCancelled>() => report.finish_cancelled(),
        Err(err) => report.finish_error(err),
    };
    write_report_best_effort(&runtime_report);
    result
}

async fn backtest_window(
    args: ScriptBacktestArgs,
    script: Script,
    source_configs: SourceConfigs,
    resolved_params: Value,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<()> {
    let data = fetch_sources(&args, &script, &source_configs, report).await?;
    let clock = script.manifest.clock_source().clone();
    let clock_len = clock_len(&clock, &data)?;
    if clock_len < 2 {
        bail!(
            "script backtest requires at least 2 {} records",
            clock.as_str()
        );
    }

    let mut returns = Vec::new();
    let mut trades = 0_usize;
    let mut position = 0.0_f64;
    let mut saw_strategy_like_output = false;
    let mut latest_output = None;
    let session = script.start_session(&resolved_params)?;
    let cancel_handle = session.cancel_handle();
    let _cancel_task = AbortOnDrop(tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_handle.store(true, Ordering::Relaxed);
        }
    }));

    let lookback = effective_lookback(&script, &resolved_params);
    eprintln!(
        "running script={} sources={} clock={} records={} lookback={}",
        script.manifest.name,
        script.manifest.source_names(),
        clock.as_str(),
        clock_len,
        lookback
    );
    report.set_progress("executing_hooks", 0, (clock_len - 1) as u64);
    write_running_report_best_effort(report);

    for idx in 0..(clock_len - 1) {
        if session.is_cancelled() {
            report.set_progress("cancelled", idx as u64, (clock_len - 1) as u64);
            return Err(ScriptCancelled.into());
        }

        let cutoff = clock_ts_ms(&clock, &data, idx)?;
        let payload = build_window_payload(&script, &data, &clock, idx, cutoff, lookback)?;
        let execution = match session.run_window(payload) {
            Ok(execution) => execution,
            Err(err) => {
                report.record_hook_failure();
                if session.is_cancelled() {
                    report.set_progress("cancelled", idx as u64, (clock_len - 1) as u64);
                    return Err(ScriptCancelled.into());
                }
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

        let curr = clock_price(&clock, &data, idx)?;
        let next = clock_price(&clock, &data, idx + 1)?;
        returns.push(position * ((next - curr) / curr.abs().max(1.0)));
        latest_output = Some(ScriptBacktestLatestOutput {
            metrics: output.metrics,
            signal: output.signal,
            intent: output.intent,
        });

        if (idx + 1) % 500 == 0 || idx + 2 == clock_len {
            eprintln!(
                "processed {}/{} {} records",
                idx + 1,
                clock_len - 1,
                clock.as_str()
            );
            report.set_progress("executing_hooks", (idx + 1) as u64, (clock_len - 1) as u64);
            write_running_report_best_effort(report);
        }
    }

    if !saw_strategy_like_output {
        bail!("script backtest requires strategy-like output: return `signal` or `intent`");
    }

    let timeframe_sec = source_configs
        .get(&clock)
        .and_then(|config| config.timeframe)
        .unwrap_or_default();
    let result = ScriptBacktestResult {
        r#type: "script.backtest.result",
        version: "1",
        provider: provider_name(args.provider.into()),
        exchange: args.exchange.clone(),
        symbol: args.symbol.clone(),
        ts_ms: clock_ts_ms(&clock, &data, clock_len - 1).unwrap_or(args.to),
        script: ScriptDescriptor {
            name: script.manifest.name.clone(),
            sources: script
                .manifest
                .sources
                .iter()
                .map(ScriptSource::as_str)
                .collect(),
        },
        window: ScriptWindow {
            from: args.from,
            to: args.to,
            timeframe_sec,
        },
        params: ScriptInputs {
            values: resolved_params,
        },
        performance: ScriptBacktestPerformance {
            trades,
            sharpe: sharpe(&returns),
            max_drawdown: max_drawdown(&returns),
        },
        latest_output: latest_output.unwrap_or(ScriptBacktestLatestOutput {
            metrics: json!({}),
            signal: json!({}),
            intent: json!({}),
        }),
        meta: json!({
            "clock": clock.as_str(),
            "source_data": {
                "orderbook": "flat_heatmap_hd"
            }
        }),
    };

    render_backtest(&result, args.output, args.verbose)
}

async fn fetch_sources(
    args: &ScriptBacktestArgs,
    script: &Script,
    source_configs: &SourceConfigs,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<BacktestData> {
    let mut data = BacktestData::default();
    let mut cancel = Box::pin(tokio::signal::ctrl_c());

    for source in &script.manifest.sources {
        let config = source_configs
            .get(source)
            .ok_or_else(|| anyhow::anyhow!("missing source config for {}", source.as_str()))?;
        match source {
            ScriptSource::Candles => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let started = Instant::now();
                report.set_phase("fetching_candles");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching candles exchange={} symbol={} tf={} from={} to={}",
                    args.exchange, args.symbol, timeframe, args.from, args.to
                );
                let future =
                    MmtProvider::candles(&args.exchange, &args.symbol, tf, args.from, args.to);
                let series = tokio::select! {
                    result = future => result?,
                    _ = &mut cancel => {
                        report.set_phase("cancelled");
                        return Err(ScriptCancelled.into());
                    }
                };
                eprintln!(
                    "fetched {} candles in {}ms",
                    series.data.len(),
                    started.elapsed().as_millis()
                );
                report.set_progress(
                    "candles_fetched",
                    series.data.len() as u64,
                    series.data.len() as u64,
                );
                write_running_report_best_effort(report);
                data.candles = Some(series.data);
            }
            ScriptSource::Orderbook => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let depth = config.depth_or_default();
                let started = Instant::now();
                report.set_phase("fetching_orderbooks");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching orderbooks exchange={} symbol={} tf={} from={} to={} depth={}",
                    args.exchange, args.symbol, timeframe, args.from, args.to, depth
                );
                let future = MmtProvider::historical_orderbooks(
                    &args.exchange,
                    &args.symbol,
                    tf,
                    args.from,
                    args.to,
                    depth,
                );
                let series = tokio::select! {
                    result = future => result?,
                    _ = &mut cancel => {
                        report.set_phase("cancelled");
                        return Err(ScriptCancelled.into());
                    }
                };
                eprintln!(
                    "fetched {} orderbooks in {}ms",
                    series.len(),
                    started.elapsed().as_millis()
                );
                report.set_progress(
                    "orderbooks_fetched",
                    series.len() as u64,
                    series.len() as u64,
                );
                write_running_report_best_effort(report);
                data.orderbooks = Some(series);
            }
        }
    }

    Ok(data)
}

fn build_window_payload(
    script: &Script,
    data: &BacktestData,
    clock: &ScriptSource,
    clock_idx: usize,
    cutoff_ms: u64,
    lookback: usize,
) -> Result<Value> {
    let mut root = Map::new();
    root.insert("mode".to_string(), Value::String("window".to_string()));

    for source in &script.manifest.sources {
        match source {
            ScriptSource::Candles => {
                let candles = data.candles.as_ref().context("candles data not loaded")?;
                let end = if source == clock {
                    clock_idx + 1
                } else {
                    upper_bound_by_ts(candles, cutoff_ms, candle_ts_ms)
                };
                let start = end.saturating_sub(lookback);
                let slice = serde_json::to_value(&candles[start..end])?;
                root.insert("candles".to_string(), json!({ "candles": slice }));
            }
            ScriptSource::Orderbook => {
                let books = data
                    .orderbooks
                    .as_ref()
                    .context("orderbook data not loaded")?;
                let end = if source == clock {
                    clock_idx + 1
                } else {
                    upper_bound_by_ts(books, cutoff_ms, |book| book.timestamp_ms)
                };
                let start = end.saturating_sub(lookback);
                let slice = serde_json::to_value(&books[start..end])?;
                root.insert("orderbook".to_string(), json!({ "books": slice }));
            }
        }
    }

    Ok(Value::Object(root))
}

fn upper_bound_by_ts<T>(items: &[T], cutoff_ms: u64, ts: impl Fn(&T) -> u64) -> usize {
    items
        .iter()
        .position(|item| ts(item) > cutoff_ms)
        .unwrap_or(items.len())
}

fn clock_len(clock: &ScriptSource, data: &BacktestData) -> Result<usize> {
    match clock {
        ScriptSource::Candles => Ok(data
            .candles
            .as_ref()
            .context("candles data not loaded")?
            .len()),
        ScriptSource::Orderbook => Ok(data
            .orderbooks
            .as_ref()
            .context("orderbook data not loaded")?
            .len()),
    }
}

fn clock_ts_ms(clock: &ScriptSource, data: &BacktestData, idx: usize) -> Result<u64> {
    match clock {
        ScriptSource::Candles => Ok(candle_ts_ms(
            &data.candles.as_ref().context("candles data not loaded")?[idx],
        )),
        ScriptSource::Orderbook => Ok(data
            .orderbooks
            .as_ref()
            .context("orderbook data not loaded")?[idx]
            .timestamp_ms),
    }
}

fn clock_price(clock: &ScriptSource, data: &BacktestData, idx: usize) -> Result<f64> {
    match clock {
        ScriptSource::Candles => {
            Ok(data.candles.as_ref().context("candles data not loaded")?[idx].c)
        }
        ScriptSource::Orderbook => book_mid(
            &data
                .orderbooks
                .as_ref()
                .context("orderbook data not loaded")?[idx],
        ),
    }
}

fn candle_ts_ms(candle: &OhlcvtCandle) -> u64 {
    if candle.t < 10_000_000_000 {
        candle.t * 1000
    } else {
        candle.t
    }
}

fn book_mid(book: &OrderBookSnapshot) -> Result<f64> {
    let bid = book
        .bids
        .first()
        .map(|level| level.price)
        .context("orderbook snapshot has no bids")?;
    let ask = book
        .asks
        .first()
        .map(|level| level.price)
        .context("orderbook snapshot has no asks")?;
    Ok((bid + ask) / 2.0)
}

fn effective_lookback(script: &Script, resolved_params: &Value) -> usize {
    if let Some(lookback) = script.manifest.lookback {
        return lookback;
    }

    resolved_params
        .as_object()
        .and_then(|params| {
            params
                .values()
                .find_map(|source| source.get("lookback").and_then(Value::as_f64))
        })
        .filter(|value| value.is_finite() && *value >= 1.0)
        .map(|value| value.floor() as usize)
        .unwrap_or(SCRIPT_DEFAULT_LOOKBACK_CANDLES)
        .min(SCRIPT_DEFAULT_LOOKBACK_CANDLES)
}

#[derive(Debug)]
struct ScriptCancelled;

impl fmt::Display for ScriptCancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("script run cancelled by user")
    }
}

impl std::error::Error for ScriptCancelled {}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
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
            params: &result.params,
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
