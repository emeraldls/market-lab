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
use crate::domain::types::{OhlcvtCandle, OiCandle, OrderBookSnapshot, VdCandle, VolumeProfile};
use crate::providers::mmt::MmtProvider;
use crate::scripting::engine::Script;
use crate::scripting::inputs::{
    SourceConfigs, parse_param_values, parse_source_configs, resolve_params,
    validate_source_configs,
};
use crate::scripting::limits::SCRIPT_DEFAULT_LOOKBACK_CANDLES;
use crate::scripting::manifest::ScriptSource;
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
    summary: ScriptBacktestSummary,
    performance: ScriptBacktestPerformance,
    closed_trades: Vec<ScriptBacktestTrade>,
    open_positions: Vec<ScriptBacktestOpenPosition>,
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
    summary: &'a ScriptBacktestSummary,
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
struct ScriptBacktestSummary {
    signals: usize,
    orders: usize,
    closed_trades: usize,
    open_positions: usize,
    wins: usize,
    losses: usize,
    win_rate: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ScriptBacktestPerformance {
    leverage: f64,
    capital: f64,
    gross_pnl: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    total_pnl: f64,
    net_pnl: f64,
    realized_return: f64,
    total_return: f64,
    net_return: f64,
    profit_factor: Option<f64>,
    best_trade_pnl: Option<f64>,
    worst_trade_pnl: Option<f64>,
    avg_trade_pnl: Option<f64>,
    sharpe: Option<f64>,
    max_drawdown: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestTrade {
    id: String,
    position_id: String,
    side: TradeSide,
    entry: ScriptBacktestTradeLeg,
    exit: ScriptBacktestTradeLeg,
    notional: f64,
    margin: f64,
    leverage: f64,
    qty: f64,
    gross_pnl: f64,
    fees: f64,
    slippage: f64,
    net_pnl: f64,
    net_return: f64,
    bars_held: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestTradeLeg {
    ts_ms: u64,
    price: f64,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestOpenPosition {
    id: String,
    side: TradeSide,
    entry_ts_ms: u64,
    entry_price: f64,
    mark_ts_ms: u64,
    mark_price: f64,
    notional: f64,
    margin: f64,
    leverage: f64,
    qty: f64,
    unrealized_pnl: f64,
    bars_held: usize,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TradeSide {
    Long,
    Short,
}

#[derive(Debug, Clone)]
struct OpenTrade {
    id: String,
    side: TradeSide,
    entry_idx: usize,
    entry_ts_ms: u64,
    entry_price: f64,
    notional: f64,
    qty: f64,
    reason: String,
}

#[derive(Debug, Clone)]
struct TradeEvent {
    idx: usize,
    ts_ms: u64,
    price: f64,
    reason: String,
    notional: Option<f64>,
    position_id: Option<String>,
}

#[derive(Debug, Clone)]
enum TradeAction {
    OpenLong,
    OpenShort,
    Close,
    CloseAll,
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
    vd: Option<Vec<VdCandle>>,
    oi: Option<Vec<OiCandle>>,
    volumes: Option<Vec<VolumeProfile>>,
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
    let mut signals = 0_usize;
    let mut orders = 0_usize;
    let mut closed_trades = Vec::new();
    let mut open_trades = Vec::new();
    let mut next_position_id = 1_usize;
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
        let payload = build_window_payload(WindowPayloadContext {
            script: &script,
            data: &data,
            source_configs: &source_configs,
            clock: &clock,
            clock_idx: idx,
            cutoff_ms: cutoff,
            lookback,
            open_trades: &open_trades,
            leverage: args.leverage,
        })?;
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

        let has_signal = !is_empty_json_object(&output.signal);
        let has_intent = !is_empty_json_object(&output.intent);
        if has_signal || has_intent {
            saw_strategy_like_output = true;
        }
        if triggered_output(&output.signal, &output.intent) {
            signals += usize::from(has_signal);
            orders += usize::from(has_intent);
            if let Some(action) = action_from_output(&output.signal, &output.intent) {
                let event = TradeEvent {
                    idx,
                    ts_ms: clock_ts_ms(&clock, &data, idx)?,
                    price: clock_price(&clock, &data, idx)?,
                    reason: reason_from_output(&output.signal, &output.intent),
                    notional: notional_from_output(&output.intent),
                    position_id: position_id_from_output(&output.intent),
                };
                apply_trade_action(
                    action,
                    &mut open_trades,
                    &mut closed_trades,
                    &mut next_position_id,
                    event,
                    args.leverage,
                )?;
            }
        }

        let curr = clock_price(&clock, &data, idx)?;
        let next = clock_price(&clock, &data, idx + 1)?;
        returns.push(position_return(&open_trades, curr, next, args.leverage));
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
    let mark_ts_ms = clock_ts_ms(&clock, &data, clock_len - 1).unwrap_or(args.to);
    let mark_price = clock_price(&clock, &data, clock_len - 1).unwrap_or_default();
    let open_positions = open_trades_to_positions(
        &open_trades,
        clock_len - 1,
        mark_ts_ms,
        mark_price,
        args.leverage,
    )
    .into_iter()
    .collect::<Vec<_>>();
    let summary = backtest_summary(signals, orders, &closed_trades, &open_positions);
    let performance =
        backtest_performance(&returns, &closed_trades, &open_positions, args.leverage);
    let result = ScriptBacktestResult {
        r#type: "script.backtest.result",
        version: "1",
        provider: provider_name(args.provider.into()),
        exchange: args.exchange.clone(),
        symbol: args.symbol.clone(),
        ts_ms: mark_ts_ms,
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
        summary,
        performance,
        closed_trades,
        open_positions,
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
            ScriptSource::Vd => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let bucket = config.require_bucket(source)?;
                let started = Instant::now();
                report.set_phase("fetching_vd");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching vd exchange={} symbol={} tf={} from={} to={} bucket={}",
                    args.exchange, args.symbol, timeframe, args.from, args.to, bucket
                );
                let future =
                    MmtProvider::vd(&args.exchange, &args.symbol, tf, args.from, args.to, bucket);
                let series = tokio::select! {
                    result = future => result?,
                    _ = &mut cancel => {
                        report.set_phase("cancelled");
                        return Err(ScriptCancelled.into());
                    }
                };
                eprintln!(
                    "fetched {} vd candles in {}ms",
                    series.data.len(),
                    started.elapsed().as_millis()
                );
                report.set_progress(
                    "vd_fetched",
                    series.data.len() as u64,
                    series.data.len() as u64,
                );
                write_running_report_best_effort(report);
                data.vd = Some(series.data);
            }
            ScriptSource::Oi => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let started = Instant::now();
                report.set_phase("fetching_oi");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching oi exchange={} symbol={} tf={} from={} to={}",
                    args.exchange, args.symbol, timeframe, args.from, args.to
                );
                let future = MmtProvider::oi(&args.exchange, &args.symbol, tf, args.from, args.to);
                let series = tokio::select! {
                    result = future => result?,
                    _ = &mut cancel => {
                        report.set_phase("cancelled");
                        return Err(ScriptCancelled.into());
                    }
                };
                eprintln!(
                    "fetched {} oi candles in {}ms",
                    series.data.len(),
                    started.elapsed().as_millis()
                );
                report.set_progress(
                    "oi_fetched",
                    series.data.len() as u64,
                    series.data.len() as u64,
                );
                write_running_report_best_effort(report);
                data.oi = Some(series.data);
            }
            ScriptSource::Volumes => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let started = Instant::now();
                report.set_phase("fetching_volumes");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching volumes exchange={} symbol={} tf={} from={} to={}",
                    args.exchange, args.symbol, timeframe, args.from, args.to
                );
                let future =
                    MmtProvider::volumes(&args.exchange, &args.symbol, tf, args.from, args.to);
                let series = tokio::select! {
                    result = future => result?,
                    _ = &mut cancel => {
                        report.set_phase("cancelled");
                        return Err(ScriptCancelled.into());
                    }
                };
                eprintln!(
                    "fetched {} volume profiles in {}ms",
                    series.data.len(),
                    started.elapsed().as_millis()
                );
                report.set_progress(
                    "volumes_fetched",
                    series.data.len() as u64,
                    series.data.len() as u64,
                );
                write_running_report_best_effort(report);
                data.volumes = Some(series.data);
            }
        }
    }

    Ok(data)
}

struct WindowPayloadContext<'a> {
    script: &'a Script,
    data: &'a BacktestData,
    source_configs: &'a SourceConfigs,
    clock: &'a ScriptSource,
    clock_idx: usize,
    cutoff_ms: u64,
    lookback: usize,
    open_trades: &'a [OpenTrade],
    leverage: f64,
}

fn build_window_payload(ctx: WindowPayloadContext<'_>) -> Result<Value> {
    let mut root = Map::new();
    root.insert("mode".to_string(), Value::String("window".to_string()));

    for source in &ctx.script.manifest.sources {
        match source {
            ScriptSource::Candles => {
                let candles = ctx
                    .data
                    .candles
                    .as_ref()
                    .context("candles data not loaded")?;
                let end = if source == ctx.clock {
                    ctx.clock_idx + 1
                } else {
                    upper_bound_by_ts(candles, ctx.cutoff_ms, candle_ts_ms)
                };
                let start = end.saturating_sub(ctx.lookback);
                let slice = serde_json::to_value(&candles[start..end])?;
                root.insert("candles".to_string(), json!({ "candles": slice }));
            }
            ScriptSource::Orderbook => {
                let books = ctx
                    .data
                    .orderbooks
                    .as_ref()
                    .context("orderbook data not loaded")?;
                let end = if source == ctx.clock {
                    ctx.clock_idx + 1
                } else {
                    upper_bound_by_ts(books, ctx.cutoff_ms, |book| book.timestamp_ms)
                };
                let start = end.saturating_sub(ctx.lookback);
                let slice = serde_json::to_value(&books[start..end])?;
                root.insert("orderbook".to_string(), json!({ "books": slice }));
            }
            ScriptSource::Vd => {
                let config = ctx
                    .source_configs
                    .get(source)
                    .context("missing source config for vd")?;
                let candles = ctx.data.vd.as_ref().context("vd data not loaded")?;
                let end = if source == ctx.clock {
                    ctx.clock_idx + 1
                } else {
                    upper_bound_by_ts(candles, ctx.cutoff_ms, vd_ts_ms)
                };
                let start = end.saturating_sub(ctx.lookback);
                let slice = serde_json::to_value(&candles[start..end])?;
                root.insert(
                    "vd".to_string(),
                    json!({
                        "candles": slice,
                        "bucket": config.require_bucket(source)?,
                        "timeframe_sec": config.require_timeframe(source)?,
                    }),
                );
            }
            ScriptSource::Oi => {
                let config = ctx
                    .source_configs
                    .get(source)
                    .context("missing source config for oi")?;
                let candles = ctx.data.oi.as_ref().context("oi data not loaded")?;
                let end = if source == ctx.clock {
                    ctx.clock_idx + 1
                } else {
                    upper_bound_by_ts(candles, ctx.cutoff_ms, oi_ts_ms)
                };
                let start = end.saturating_sub(ctx.lookback);
                let slice = serde_json::to_value(&candles[start..end])?;
                root.insert(
                    "oi".to_string(),
                    json!({
                        "candles": slice,
                        "timeframe_sec": config.require_timeframe(source)?,
                    }),
                );
            }
            ScriptSource::Volumes => {
                let config = ctx
                    .source_configs
                    .get(source)
                    .context("missing source config for volumes")?;
                let profiles = ctx
                    .data
                    .volumes
                    .as_ref()
                    .context("volumes data not loaded")?;
                let end = if source == ctx.clock {
                    ctx.clock_idx + 1
                } else {
                    upper_bound_by_ts(profiles, ctx.cutoff_ms, volume_ts_ms)
                };
                let start = end.saturating_sub(ctx.lookback);
                let slice = serde_json::to_value(&profiles[start..end])?;
                root.insert(
                    "volumes".to_string(),
                    json!({
                        "profiles": slice,
                        "timeframe_sec": config.require_timeframe(source)?,
                    }),
                );
            }
        }
    }

    let mark_price = clock_price(ctx.clock, ctx.data, ctx.clock_idx)?;
    let open_positions = open_trades_to_positions(
        ctx.open_trades,
        ctx.clock_idx,
        ctx.cutoff_ms,
        mark_price,
        ctx.leverage,
    );
    root.insert("positions".to_string(), json!({ "open": open_positions }));

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
        ScriptSource::Vd => Ok(data.vd.as_ref().context("vd data not loaded")?.len()),
        ScriptSource::Oi => Ok(data.oi.as_ref().context("oi data not loaded")?.len()),
        ScriptSource::Volumes => Ok(data
            .volumes
            .as_ref()
            .context("volumes data not loaded")?
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
        ScriptSource::Vd => Ok(vd_ts_ms(
            &data.vd.as_ref().context("vd data not loaded")?[idx],
        )),
        ScriptSource::Oi => Ok(oi_ts_ms(
            &data.oi.as_ref().context("oi data not loaded")?[idx],
        )),
        ScriptSource::Volumes => Ok(volume_ts_ms(
            &data.volumes.as_ref().context("volumes data not loaded")?[idx],
        )),
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
        ScriptSource::Vd => Ok(data.vd.as_ref().context("vd data not loaded")?[idx].c),
        ScriptSource::Oi => Ok(data.oi.as_ref().context("oi data not loaded")?[idx].c),
        ScriptSource::Volumes => {
            volume_profile_price(&data.volumes.as_ref().context("volumes data not loaded")?[idx])
        }
    }
}

fn candle_ts_ms(candle: &OhlcvtCandle) -> u64 {
    if candle.t < 10_000_000_000 {
        candle.t * 1000
    } else {
        candle.t
    }
}

fn vd_ts_ms(candle: &VdCandle) -> u64 {
    if candle.t < 10_000_000_000 {
        candle.t * 1000
    } else {
        candle.t
    }
}

fn oi_ts_ms(candle: &OiCandle) -> u64 {
    if candle.t < 10_000_000_000 {
        candle.t * 1000
    } else {
        candle.t
    }
}

fn volume_ts_ms(profile: &VolumeProfile) -> u64 {
    if profile.t < 10_000_000_000 {
        profile.t * 1000
    } else {
        profile.t
    }
}

fn volume_profile_price(profile: &VolumeProfile) -> Result<f64> {
    let mut best_price = None;
    let mut best_volume = f64::NEG_INFINITY;
    for (idx, price) in profile.p.iter().enumerate() {
        let total =
            profile.b.get(idx).copied().unwrap_or(0.0) + profile.s.get(idx).copied().unwrap_or(0.0);
        if total > best_volume {
            best_volume = total;
            best_price = Some(*price);
        }
    }
    best_price.context("volume profile has no price levels")
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

fn triggered_output(signal: &Value, intent: &Value) -> bool {
    signal
        .get("triggered")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| !is_empty_json_object(intent))
}

fn action_from_output(signal: &Value, intent: &Value) -> Option<TradeAction> {
    let action = signal
        .get("action")
        .or_else(|| signal.get("event"))
        .or_else(|| intent.get("action"))
        .and_then(Value::as_str);
    match action {
        Some("open_long" | "enter_long" | "long" | "buy") => return Some(TradeAction::OpenLong),
        Some("open_short" | "enter_short" | "short" | "sell") => {
            return Some(TradeAction::OpenShort);
        }
        Some("open") => return side_to_open_action(output_side(signal, intent)?),
        Some("close") => return Some(TradeAction::Close),
        Some("close_all") => return Some(TradeAction::CloseAll),
        _ => {}
    }

    let side = output_side(signal, intent)?;
    side_to_open_action(side)
}

fn output_side<'a>(signal: &'a Value, intent: &'a Value) -> Option<&'a str> {
    intent
        .get("side")
        .or_else(|| signal.get("side"))
        .and_then(Value::as_str)
}

fn side_to_open_action(side: &str) -> Option<TradeAction> {
    match side {
        "buy" | "long" => Some(TradeAction::OpenLong),
        "sell" | "short" => Some(TradeAction::OpenShort),
        _ => None,
    }
}

fn reason_from_output(signal: &Value, intent: &Value) -> String {
    signal
        .get("reason")
        .or_else(|| intent.get("reason"))
        .or_else(|| signal.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("script signal")
        .to_string()
}

fn notional_from_output(intent: &Value) -> Option<f64> {
    intent
        .get("notional")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn position_id_from_output(intent: &Value) -> Option<String> {
    intent
        .get("position_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
}

fn apply_trade_action(
    action: TradeAction,
    open_trades: &mut Vec<OpenTrade>,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
    next_position_id: &mut usize,
    event: TradeEvent,
    leverage: f64,
) -> Result<()> {
    match action {
        TradeAction::OpenLong => {
            open_trades.push(open_trade_from_entry(
                next_position_id,
                TradeSide::Long,
                event.idx,
                event.ts_ms,
                event.price,
                event.reason,
                event.notional,
            ));
        }
        TradeAction::OpenShort => {
            open_trades.push(open_trade_from_entry(
                next_position_id,
                TradeSide::Short,
                event.idx,
                event.ts_ms,
                event.price,
                event.reason,
                event.notional,
            ));
        }
        TradeAction::Close => {
            let position_id = event
                .position_id
                .as_deref()
                .context("close intent requires `position_id`")?;
            close_position_by_id(open_trades, closed_trades, position_id, &event, leverage)?;
        }
        TradeAction::CloseAll => {
            close_all_positions(open_trades, closed_trades, &event, leverage);
        }
    }
    Ok(())
}

fn open_trade_from_entry(
    next_position_id: &mut usize,
    side: TradeSide,
    idx: usize,
    ts_ms: u64,
    price: f64,
    reason: String,
    notional: Option<f64>,
) -> OpenTrade {
    let id = format_position_id(*next_position_id);
    *next_position_id += 1;
    let notional = notional.unwrap_or(1_000.0);
    let qty = if price.abs() > f64::EPSILON {
        notional / price
    } else {
        0.0
    };
    OpenTrade {
        id,
        side,
        entry_idx: idx,
        entry_ts_ms: ts_ms,
        entry_price: price,
        notional,
        qty,
        reason,
    }
}

fn close_position_by_id(
    open_trades: &mut Vec<OpenTrade>,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
    position_id: &str,
    event: &TradeEvent,
    leverage: f64,
) -> Result<()> {
    let Some(index) = open_trades.iter().position(|trade| trade.id == position_id) else {
        bail!("cannot close unknown position_id `{position_id}`");
    };
    let open = open_trades.remove(index);
    close_open_trade(open, closed_trades, event, leverage);
    Ok(())
}

fn close_all_positions(
    open_trades: &mut Vec<OpenTrade>,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
    event: &TradeEvent,
    leverage: f64,
) {
    let trades = std::mem::take(open_trades);
    for open in trades {
        close_open_trade(open, closed_trades, event, leverage);
    }
}

fn close_open_trade(
    open: OpenTrade,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
    event: &TradeEvent,
    leverage: f64,
) {
    let gross_pnl = trade_pnl(open.side, open.entry_price, event.price, open.qty);
    let fees = 0.0;
    let slippage = 0.0;
    let net_pnl = gross_pnl - fees - slippage;
    let margin = margin_for_notional(open.notional, leverage);
    let net_return = if margin.abs() > f64::EPSILON {
        net_pnl / margin
    } else {
        0.0
    };

    closed_trades.push(ScriptBacktestTrade {
        id: format_trade_id(closed_trades.len() + 1),
        position_id: open.id,
        side: open.side,
        entry: ScriptBacktestTradeLeg {
            ts_ms: open.entry_ts_ms,
            price: open.entry_price,
            reason: open.reason,
        },
        exit: ScriptBacktestTradeLeg {
            ts_ms: event.ts_ms,
            price: event.price,
            reason: event.reason.clone(),
        },
        notional: open.notional,
        margin,
        leverage,
        qty: open.qty,
        gross_pnl,
        fees,
        slippage,
        net_pnl,
        net_return,
        bars_held: event.idx.saturating_sub(open.entry_idx),
    });
}

fn trade_pnl(side: TradeSide, entry_price: f64, exit_price: f64, qty: f64) -> f64 {
    match side {
        TradeSide::Long => (exit_price - entry_price) * qty,
        TradeSide::Short => (entry_price - exit_price) * qty,
    }
}

fn position_return(open_trades: &[OpenTrade], curr: f64, next: f64, leverage: f64) -> f64 {
    if open_trades.is_empty() {
        return 0.0;
    }
    let pnl = open_trades
        .iter()
        .map(|open| trade_pnl(open.side, curr, next, open.qty))
        .sum::<f64>();
    let margin = open_trades
        .iter()
        .map(|open| margin_for_notional(open.notional, leverage))
        .sum::<f64>();
    if margin.abs() > f64::EPSILON {
        pnl / margin
    } else {
        0.0
    }
}

fn open_trades_to_positions(
    open_trades: &[OpenTrade],
    mark_idx: usize,
    mark_ts_ms: u64,
    mark_price: f64,
    leverage: f64,
) -> Vec<ScriptBacktestOpenPosition> {
    open_trades
        .iter()
        .map(|open| ScriptBacktestOpenPosition {
            id: open.id.clone(),
            side: open.side,
            entry_ts_ms: open.entry_ts_ms,
            entry_price: open.entry_price,
            mark_ts_ms,
            mark_price,
            notional: open.notional,
            margin: margin_for_notional(open.notional, leverage),
            leverage,
            qty: open.qty,
            unrealized_pnl: trade_pnl(open.side, open.entry_price, mark_price, open.qty),
            bars_held: mark_idx.saturating_sub(open.entry_idx),
            reason: "backtest ended before exit signal".to_string(),
        })
        .collect()
}

fn margin_for_notional(notional: f64, leverage: f64) -> f64 {
    notional / leverage.max(f64::EPSILON)
}

fn format_position_id(id: usize) -> String {
    format!("pos_{id:06}")
}

fn format_trade_id(id: usize) -> String {
    format!("trade_{id:06}")
}

fn backtest_summary(
    signals: usize,
    orders: usize,
    trades: &[ScriptBacktestTrade],
    open_positions: &[ScriptBacktestOpenPosition],
) -> ScriptBacktestSummary {
    let wins = trades.iter().filter(|trade| trade.net_pnl > 0.0).count();
    let losses = trades.iter().filter(|trade| trade.net_pnl < 0.0).count();
    let win_rate = if trades.is_empty() {
        None
    } else {
        Some(wins as f64 / trades.len() as f64)
    };
    ScriptBacktestSummary {
        signals,
        orders,
        closed_trades: trades.len(),
        open_positions: open_positions.len(),
        wins,
        losses,
        win_rate,
    }
}

fn backtest_performance(
    returns: &[f64],
    trades: &[ScriptBacktestTrade],
    open_positions: &[ScriptBacktestOpenPosition],
    leverage: f64,
) -> ScriptBacktestPerformance {
    let gross_pnl = trades.iter().map(|trade| trade.gross_pnl).sum::<f64>();
    let realized_pnl = trades.iter().map(|trade| trade.net_pnl).sum::<f64>();
    let unrealized_pnl = open_positions
        .iter()
        .map(|position| position.unrealized_pnl)
        .sum::<f64>();
    let total_pnl = realized_pnl + unrealized_pnl;
    let capital = trades
        .first()
        .map(|trade| trade.margin)
        .or_else(|| open_positions.first().map(|position| position.margin))
        .unwrap_or_else(|| margin_for_notional(1_000.0, leverage))
        .max(1.0);
    let gross_profit = trades
        .iter()
        .filter(|trade| trade.net_pnl > 0.0)
        .map(|trade| trade.net_pnl)
        .sum::<f64>();
    let gross_loss = trades
        .iter()
        .filter(|trade| trade.net_pnl < 0.0)
        .map(|trade| trade.net_pnl.abs())
        .sum::<f64>();
    let profit_factor = (gross_loss > f64::EPSILON).then_some(gross_profit / gross_loss);
    let best_trade_pnl = trades
        .iter()
        .map(|trade| trade.net_pnl)
        .max_by(f64::total_cmp);
    let worst_trade_pnl = trades
        .iter()
        .map(|trade| trade.net_pnl)
        .min_by(f64::total_cmp);
    let avg_trade_pnl = if trades.is_empty() {
        None
    } else {
        Some(realized_pnl / trades.len() as f64)
    };

    ScriptBacktestPerformance {
        leverage,
        capital,
        gross_pnl,
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        net_pnl: total_pnl,
        realized_return: realized_pnl / capital,
        total_return: total_pnl / capital,
        net_return: total_pnl / capital,
        profit_factor,
        best_trade_pnl,
        worst_trade_pnl,
        avg_trade_pnl,
        sharpe: sharpe(returns),
        max_drawdown: max_drawdown(returns),
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
            println!("script backtest");
            println!("---------------");
            println!(
                "market: {}:{} tf={} [{}-{}]",
                result.exchange,
                result.symbol,
                result.window.timeframe_sec,
                result.window.from,
                result.window.to
            );
            println!("script: {}", result.script.name);
            println!();
            println!("summary");
            println!(
                "  signals: {}\n  orders: {}\n  closed trades: {}\n  open positions: {}\n  wins/losses: {}/{}\n  win rate: {}",
                result.summary.signals,
                result.summary.orders,
                result.summary.closed_trades,
                result.summary.open_positions,
                result.summary.wins,
                result.summary.losses,
                format_percent(result.summary.win_rate)
            );
            println!();
            println!("performance");
            println!(
                "  leverage: {:.2}x\n  capital: {}\n  realized pnl: {}\n  unrealized pnl: {}\n  total pnl: {}\n  total return: {}\n  gross pnl: {}\n  profit factor: {}\n  avg trade: {}\n  best trade: {}\n  worst trade: {}\n  sharpe: {}\n  max drawdown: {}",
                result.performance.leverage,
                format_money(result.performance.capital),
                format_money(result.performance.realized_pnl),
                format_money(result.performance.unrealized_pnl),
                format_money(result.performance.total_pnl),
                format_percent(Some(result.performance.total_return)),
                format_money(result.performance.gross_pnl),
                format_number(result.performance.profit_factor),
                format_money_opt(result.performance.avg_trade_pnl),
                format_money_opt(result.performance.best_trade_pnl),
                format_money_opt(result.performance.worst_trade_pnl),
                format_number(result.performance.sharpe),
                format_percent(result.performance.max_drawdown.map(|value| -value))
            );
            if !result.closed_trades.is_empty() {
                println!();
                println!("closed trades");
                let shown = if verbose {
                    result.closed_trades.len()
                } else {
                    result.closed_trades.len().min(10)
                };
                for trade in result.closed_trades.iter().take(shown) {
                    println!(
                        "  {} pos={} {} entry={} exit={} notional={} margin={} pnl={} bars={} reason={}",
                        trade.id,
                        trade.position_id,
                        format_side(trade.side),
                        format_price(trade.entry.price),
                        format_price(trade.exit.price),
                        format_money(trade.notional),
                        format_money(trade.margin),
                        format_money(trade.net_pnl),
                        trade.bars_held,
                        trade.exit.reason
                    );
                }
                if !verbose && result.closed_trades.len() > shown {
                    println!(
                        "  ... {} more trades, rerun with --verbose to show all",
                        result.closed_trades.len() - shown
                    );
                }
            }
            if !result.open_positions.is_empty() {
                println!();
                println!("open positions");
                for open in &result.open_positions {
                    println!(
                        "  {} {} entry={} mark={} notional={} margin={} unrealized={} bars={}",
                        open.id,
                        format_side(open.side),
                        format_price(open.entry_price),
                        format_price(open.mark_price),
                        format_money(open.notional),
                        format_money(open.margin),
                        format_money(open.unrealized_pnl),
                        open.bars_held
                    );
                }
            }
            if verbose {
                println!();
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
            summary: &result.summary,
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

fn format_side(side: TradeSide) -> &'static str {
    match side {
        TradeSide::Long => "long",
        TradeSide::Short => "short",
    }
}

fn format_money(value: f64) -> String {
    let value = if value.abs() < 0.00005 { 0.0 } else { value };
    if value >= 0.0 {
        format!("+{value:.4}")
    } else {
        format!("{value:.4}")
    }
}

fn format_money_opt(value: Option<f64>) -> String {
    value.map(format_money).unwrap_or_else(|| "-".to_string())
}

fn format_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "-".to_string())
}

fn format_number(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

fn format_price(value: f64) -> String {
    format!("{value:.6}")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event(idx: usize, price: f64, position_id: Option<&str>) -> TradeEvent {
        TradeEvent {
            idx,
            ts_ms: 1_780_000_000_000 + idx as u64,
            price,
            reason: "test".to_string(),
            notional: Some(1_000.0),
            position_id: position_id.map(ToString::to_string),
        }
    }

    #[test]
    fn closes_only_requested_position_id() {
        let mut open_trades = Vec::new();
        let mut closed_trades = Vec::new();
        let mut next_position_id = 1;

        apply_trade_action(
            TradeAction::OpenLong,
            &mut open_trades,
            &mut closed_trades,
            &mut next_position_id,
            event(0, 100.0, None),
            5.0,
        )
        .expect("open first position");
        apply_trade_action(
            TradeAction::OpenLong,
            &mut open_trades,
            &mut closed_trades,
            &mut next_position_id,
            event(1, 110.0, None),
            5.0,
        )
        .expect("open second position");

        apply_trade_action(
            TradeAction::Close,
            &mut open_trades,
            &mut closed_trades,
            &mut next_position_id,
            event(2, 120.0, Some("pos_000001")),
            5.0,
        )
        .expect("close exact position");

        assert_eq!(closed_trades.len(), 1);
        assert_eq!(closed_trades[0].id, "trade_000001");
        assert_eq!(closed_trades[0].position_id, "pos_000001");
        assert_eq!(open_trades.len(), 1);
        assert_eq!(open_trades[0].id, "pos_000002");
    }

    #[test]
    fn rejects_close_without_position_id() {
        let mut open_trades = Vec::new();
        let mut closed_trades = Vec::new();
        let mut next_position_id = 1;

        apply_trade_action(
            TradeAction::OpenLong,
            &mut open_trades,
            &mut closed_trades,
            &mut next_position_id,
            event(0, 100.0, None),
            1.0,
        )
        .expect("open position");

        let err = apply_trade_action(
            TradeAction::Close,
            &mut open_trades,
            &mut closed_trades,
            &mut next_position_id,
            event(1, 101.0, None),
            1.0,
        )
        .expect_err("close without id must fail");

        assert!(err.to_string().contains("position_id"));
        assert_eq!(open_trades.len(), 1);
        assert!(closed_trades.is_empty());
    }
}
