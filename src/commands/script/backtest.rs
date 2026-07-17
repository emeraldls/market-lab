use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::cli::{OutputFormat, ScriptBacktestArgs, mmt_timeframe_from_seconds};
use crate::commands::script::{
    ScriptDescriptor, ScriptInputs, report_builder, write_report_best_effort,
    write_running_report_best_effort,
};
use crate::commands::study::common::is_empty_object;
use crate::domain::enums::ProviderKind;
use crate::domain::types::OrderBookSnapshot;
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::mmt::MmtProvider;
use crate::scripting::engine::{Script, ScriptSession};
use crate::scripting::execution::{
    ScriptCancelRequest, ScriptExecutionCommand, ScriptExecutionContext, ScriptOrderKind,
    ScriptOrderRef, ScriptTradeRequest,
};
use crate::scripting::inputs::{
    SourceConfig, SourceConfigs, first_source_config, parse_param_values, parse_source_configs,
    resolve_params, source_configs_payload, source_exchange_label, source_provider_label,
    validate_source_configs,
};
use crate::scripting::manifest::ScriptSource;
use crate::scripting::market_data::{
    ScriptCandle, ScriptOpenInterest, ScriptVolume, ScriptVolumeDelta,
};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestResult<I>
where
    I: Serialize,
{
    r#type: &'static str,
    version: &'static str,
    provider: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    latest_output: Option<ScriptBacktestLatestOutput>,
    meta: Value,
}

#[derive(Debug, Serialize)]
struct CompactScriptBacktestResult<'a, I>
where
    I: Serialize,
{
    r#type: &'static str,
    version: &'static str,
    provider: &'a str,
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
    orders: usize,
    pending_orders: usize,
    cancelled_orders: usize,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
    side: TradeSide,
    entry_ts_ms: u64,
    entry_price: f64,
    mark_ts_ms: u64,
    mark_price: f64,
    notional: f64,
    margin: f64,
    leverage: f64,
    qty: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_loss_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    take_profit_price: Option<f64>,
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
    order_id: Option<String>,
    side: TradeSide,
    entry_idx: usize,
    entry_ts_ms: u64,
    entry_price: f64,
    notional: f64,
    qty: f64,
    leverage: f64,
    stop_loss_price: Option<f64>,
    take_profit_price: Option<f64>,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulatedOrderStatus {
    Pending,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone)]
struct SimulatedScriptOrder {
    order: ScriptOrderRef,
    request: ScriptTradeRequest,
    submitted_idx: usize,
    status: SimulatedOrderStatus,
}

#[derive(Debug, Clone)]
struct TradeEvent {
    idx: usize,
    ts_ms: u64,
    price: f64,
    reason: String,
}

struct TradeEntry {
    side: TradeSide,
    idx: usize,
    ts_ms: u64,
    price: f64,
    reason: String,
    notional: Option<f64>,
    leverage: f64,
    order_id: Option<String>,
    stop_loss_price: Option<f64>,
    take_profit_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct ScriptBacktestLatestOutput {
    metrics: Value,
    meta: Value,
}

#[derive(Default)]
struct BacktestData {
    series: BTreeMap<String, BacktestSeries>,
}

enum BacktestSeries {
    Candles(Vec<ScriptCandle>),
    Orderbooks(Vec<OrderBookSnapshot>),
    Vd(Vec<ScriptVolumeDelta>),
    Oi(Vec<ScriptOpenInterest>),
    Volumes(Vec<ScriptVolume>),
}

#[derive(Default)]
struct BacktestHistoryCursors {
    next: BTreeMap<String, usize>,
}

impl BacktestHistoryCursors {
    fn sync(
        &mut self,
        session: &ScriptSession,
        data: &BacktestData,
        source_configs: &SourceConfigs,
        clock: &SourceConfig,
        clock_idx: usize,
        cutoff_ms: u64,
    ) -> Result<()> {
        let mut configs = source_configs.values().collect::<Vec<_>>();
        configs.sort_by_key(|config| config.position);
        for config in configs {
            let series = data
                .series
                .get(&config.selector)
                .with_context(|| format!("{} data not loaded", config.selector))?;
            let next = self.next.entry(config.selector.clone()).or_default();
            let end = if config.selector == clock.selector {
                clock_idx + 1
            } else {
                let mut end = *next;
                while end < backtest_series_len(series)
                    && backtest_series_ts_ms(series, end)? <= cutoff_ms
                {
                    end += 1;
                }
                end
            };
            while *next < end {
                let (record, identity) = backtest_history_record(series, *next)?;
                session.record_source(&config.selector, record, identity)?;
                *next += 1;
            }
        }
        Ok(())
    }
}

pub async fn handle(args: ScriptBacktestArgs) -> Result<()> {
    args.validate()?;
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("script backtest currently supports only --output terminal|json|jsonl");
    }

    let script = Script::load(&args.script)?;
    let mut report = report_builder(
        "script.backtest",
        &script,
        None,
        None,
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
    let source_validation = validate_source_configs(&script.manifest, &source_configs);
    if let Err(err) = source_validation {
        let runtime_report = report.finish_error(&err);
        write_report_best_effort(&runtime_report);
        return Err(err);
    }
    report.set_provider(Some(source_provider_label(&source_configs)));
    report.set_exchange(Some(source_exchange_label(&source_configs)));

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
    let clock = first_source_config(&source_configs, script.manifest.clock_source())?.clone();
    let clock_len = clock_len(&clock, &data)?;
    if clock_len < 2 {
        bail!(
            "script backtest requires at least 2 {} records",
            clock.selector
        );
    }

    let mut returns = Vec::new();
    let mut orders = 0_usize;
    let mut closed_trades = Vec::new();
    let mut open_trades = Vec::new();
    let mut script_orders = HashMap::<String, SimulatedScriptOrder>::new();
    let mut next_position_id = 1_usize;
    let mut saw_execution_action = false;
    let mut latest_output = None;
    let session = script.start_session_with_execution(
        &resolved_params,
        ScriptExecutionContext {
            job_id: "backtest".to_string(),
            enabled: true,
        },
    )?;
    let cancel_handle = session.cancel_handle();
    let _cancel_task = AbortOnDrop(tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_handle.store(true, Ordering::Relaxed);
        }
    }));

    let lookback = script.history_capacity(&resolved_params);
    let provider_label = source_provider_label(&source_configs);
    let mut history_cursors = BacktestHistoryCursors::default();
    eprintln!(
        "running script={} sources={} clock={} records={} lookback={}",
        script.manifest.name,
        script.manifest.source_names(),
        clock.selector,
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
        history_cursors.sync(&session, &data, &source_configs, &clock, idx, cutoff)?;
        apply_protective_triggers(&clock, &data, idx, &mut open_trades, &mut closed_trades)?;
        fill_pending_script_orders(
            &clock,
            &data,
            idx,
            &mut script_orders,
            &mut open_trades,
            &mut next_position_id,
        )?;
        let payload = build_window_payload(WindowPayloadContext {
            source_configs: &source_configs,
            provider: &provider_label,
            symbol: &args.symbol,
            clock: &clock,
            clock_idx: idx,
            cutoff_ms: cutoff,
            mark_price: clock_price(&clock, &data, idx)?,
            open_trades: &open_trades,
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
        let commands = execution.commands;
        let output = execution.output;

        let script_order_count = apply_script_execution_commands(
            commands,
            idx,
            cutoff,
            clock_price(&clock, &data, idx)?,
            &mut script_orders,
            &mut open_trades,
            &mut next_position_id,
        )?;
        if script_order_count > 0 {
            saw_execution_action = true;
            orders += script_order_count;
        }

        let curr = clock_price(&clock, &data, idx)?;
        let next = clock_price(&clock, &data, idx + 1)?;
        returns.push(position_return(&open_trades, curr, next));
        if !output.is_empty() {
            latest_output = Some(ScriptBacktestLatestOutput {
                metrics: output.metrics,
                meta: output.meta,
            });
        }

        if (idx + 1) % 500 == 0 || idx + 2 == clock_len {
            eprintln!(
                "processed {}/{} {} records",
                idx + 1,
                clock_len - 1,
                clock.selector
            );
            report.set_progress("executing_hooks", (idx + 1) as u64, (clock_len - 1) as u64);
            write_running_report_best_effort(report);
        }
    }

    if !saw_execution_action {
        bail!("script backtest requires an execution action: call `ctx.trade()`");
    }

    let timeframe_sec = source_configs
        .get(&clock.selector)
        .and_then(|config| config.timeframe)
        .unwrap_or_default();
    let mark_ts_ms = clock_ts_ms(&clock, &data, clock_len - 1).unwrap_or(args.to);
    let mark_price = clock_price(&clock, &data, clock_len - 1).unwrap_or_default();
    let open_positions =
        open_trades_to_positions(&open_trades, clock_len - 1, mark_ts_ms, mark_price)
            .into_iter()
            .collect::<Vec<_>>();
    let summary = backtest_summary(orders, &script_orders, &closed_trades, &open_positions);
    let performance =
        backtest_performance(&returns, &closed_trades, &open_positions, args.leverage);
    let result = ScriptBacktestResult {
        r#type: "script.backtest.result",
        version: "1",
        provider: provider_label,
        exchange: clock.exchange.clone(),
        symbol: args.symbol.clone(),
        ts_ms: mark_ts_ms,
        script: ScriptDescriptor {
            name: script.manifest.name.clone(),
            sources: script
                .manifest
                .sources
                .iter()
                .map(|source| source.as_str().to_string())
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
        latest_output,
        meta: json!({
            "clock": clock.selector,
            "source_data": {
                "orderbook": "flat_heatmap_hd"
            }
        }),
    };

    render_backtest(&result, args.output, args.verbose)
}

async fn fetch_sources(
    args: &ScriptBacktestArgs,
    _script: &Script,
    source_configs: &SourceConfigs,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<BacktestData> {
    let mut data = BacktestData::default();
    if source_configs
        .values()
        .any(|config| config.provider == ProviderKind::Mmt)
    {
        data.series.extend(
            fetch_mmt_sources(args, source_configs, report)
                .await?
                .series,
        );
    }
    if source_configs
        .values()
        .any(|config| config.provider == ProviderKind::Bulk)
    {
        data.series.extend(
            fetch_bulk_sources(args, source_configs, report)
                .await?
                .series,
        );
    }
    Ok(data)
}

async fn fetch_mmt_sources(
    args: &ScriptBacktestArgs,
    source_configs: &SourceConfigs,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<BacktestData> {
    let mut data = BacktestData::default();
    let mut cancel = Box::pin(tokio::signal::ctrl_c());
    let mut configs = source_configs.values().collect::<Vec<_>>();
    configs.retain(|config| config.provider == ProviderKind::Mmt);
    configs.sort_by_key(|config| config.position);

    for config in configs {
        let source = &config.source;
        let exchange = config.exchange.as_str();
        match source {
            ScriptSource::Candles => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let started = Instant::now();
                report.set_phase("fetching_candles");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching candles exchange={} symbol={} tf={} from={} to={}",
                    exchange, args.symbol, timeframe, args.from, args.to
                );
                let future = MmtProvider::candles(exchange, &args.symbol, tf, args.from, args.to);
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
                data.series.insert(
                    config.selector.clone(),
                    BacktestSeries::Candles(
                        series
                            .data
                            .into_iter()
                            .map(ScriptCandle::from_mmt)
                            .collect(),
                    ),
                );
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
                    exchange, args.symbol, timeframe, args.from, args.to, depth
                );
                let future = MmtProvider::historical_orderbooks(
                    exchange,
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
                data.series
                    .insert(config.selector.clone(), BacktestSeries::Orderbooks(series));
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
                    exchange, args.symbol, timeframe, args.from, args.to, bucket
                );
                let future =
                    MmtProvider::vd(exchange, &args.symbol, tf, args.from, args.to, bucket);
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
                data.series.insert(
                    config.selector.clone(),
                    BacktestSeries::Vd(
                        series
                            .data
                            .into_iter()
                            .map(ScriptVolumeDelta::from_mmt)
                            .collect(),
                    ),
                );
            }
            ScriptSource::Oi => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let started = Instant::now();
                report.set_phase("fetching_oi");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching oi exchange={} symbol={} tf={} from={} to={}",
                    exchange, args.symbol, timeframe, args.from, args.to
                );
                let future = MmtProvider::oi(exchange, &args.symbol, tf, args.from, args.to);
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
                data.series.insert(
                    config.selector.clone(),
                    BacktestSeries::Oi(
                        series
                            .data
                            .into_iter()
                            .map(ScriptOpenInterest::from_mmt)
                            .collect(),
                    ),
                );
            }
            ScriptSource::Volumes => {
                let timeframe = config.require_timeframe(source)?;
                let tf = mmt_timeframe_from_seconds(timeframe)?;
                let started = Instant::now();
                report.set_phase("fetching_volumes");
                write_running_report_best_effort(report);
                eprintln!(
                    "fetching volumes exchange={} symbol={} tf={} from={} to={}",
                    exchange, args.symbol, timeframe, args.from, args.to
                );
                let future = MmtProvider::volumes(exchange, &args.symbol, tf, args.from, args.to);
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
                data.series.insert(
                    config.selector.clone(),
                    BacktestSeries::Volumes(
                        series
                            .data
                            .into_iter()
                            .map(ScriptVolume::from_mmt)
                            .collect(),
                    ),
                );
            }
        }
    }

    Ok(data)
}

async fn fetch_bulk_sources(
    args: &ScriptBacktestArgs,
    source_configs: &SourceConfigs,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<BacktestData> {
    let mut data = BacktestData::default();
    let mut cancel = Box::pin(tokio::signal::ctrl_c());

    let mut configs = source_configs.values().collect::<Vec<_>>();
    configs.retain(|config| config.provider == ProviderKind::Bulk);
    configs.sort_by_key(|config| config.position);
    for config in configs {
        let source = &config.source;
        let timeframe = config.require_timeframe(source)?;
        let interval = crate::providers::bulk::market_data::timeframe_from_seconds(timeframe)?;
        let phase = match source {
            ScriptSource::Candles => "fetching_candles",
            ScriptSource::Volumes => "fetching_volumes",
            ScriptSource::Orderbook | ScriptSource::Vd | ScriptSource::Oi => {
                bail!(
                    "BULK does not provide historical {} for script backtests",
                    source.as_str()
                );
            }
        };
        report.set_phase(phase);
        write_running_report_best_effort(report);
        let started = Instant::now();
        eprintln!(
            "fetching BULK {} symbol={} tf={} from={} to={}",
            source.as_str(),
            args.symbol,
            timeframe,
            args.from,
            args.to
        );
        let future = BulkProvider::candles(&args.symbol, interval, args.from, args.to);
        let series = tokio::select! {
            result = future => result?,
            _ = &mut cancel => {
                report.set_phase("cancelled");
                return Err(ScriptCancelled.into());
            }
        };
        let points = series.data.len();
        match source {
            ScriptSource::Candles => {
                data.series.insert(
                    config.selector.clone(),
                    BacktestSeries::Candles(
                        series
                            .data
                            .into_iter()
                            .map(ScriptCandle::from_bulk)
                            .collect(),
                    ),
                );
            }
            ScriptSource::Volumes => {
                data.series.insert(
                    config.selector.clone(),
                    BacktestSeries::Volumes(
                        series
                            .data
                            .into_iter()
                            .map(ScriptVolume::from_bulk_candle)
                            .collect(),
                    ),
                );
            }
            ScriptSource::Orderbook | ScriptSource::Vd | ScriptSource::Oi => unreachable!(),
        }
        eprintln!(
            "fetched {points} BULK {} records in {}ms",
            source.as_str(),
            started.elapsed().as_millis()
        );
        report.set_progress(
            format!("{}_fetched", source.as_str()),
            points as u64,
            points as u64,
        );
        write_running_report_best_effort(report);
    }

    Ok(data)
}

struct WindowPayloadContext<'a> {
    source_configs: &'a SourceConfigs,
    provider: &'a str,
    symbol: &'a str,
    clock: &'a SourceConfig,
    clock_idx: usize,
    cutoff_ms: u64,
    mark_price: f64,
    open_trades: &'a [OpenTrade],
}

fn build_window_payload(ctx: WindowPayloadContext<'_>) -> Result<Value> {
    let mut root = Map::new();
    root.insert("mode".to_string(), Value::String("window".to_string()));
    root.insert(
        "provider".to_string(),
        Value::String(ctx.provider.to_string()),
    );
    root.insert(
        "exchange".to_string(),
        Value::String(ctx.clock.exchange.clone()),
    );
    root.insert("symbol".to_string(), Value::String(ctx.symbol.to_string()));
    root.insert(
        "clock".to_string(),
        Value::String(ctx.clock.selector.clone()),
    );
    root.insert(
        "source_configs".to_string(),
        source_configs_payload(ctx.source_configs),
    );
    let open_positions = open_trades_to_positions(
        ctx.open_trades,
        ctx.clock_idx,
        ctx.cutoff_ms,
        ctx.mark_price,
    );
    root.insert("positions".to_string(), json!({ "open": open_positions }));

    Ok(Value::Object(root))
}

fn backtest_series_len(series: &BacktestSeries) -> usize {
    match series {
        BacktestSeries::Candles(items) => items.len(),
        BacktestSeries::Orderbooks(items) => items.len(),
        BacktestSeries::Vd(items) => items.len(),
        BacktestSeries::Oi(items) => items.len(),
        BacktestSeries::Volumes(items) => items.len(),
    }
}

fn backtest_series_ts_ms(series: &BacktestSeries, idx: usize) -> Result<u64> {
    match series {
        BacktestSeries::Candles(items) => items
            .get(idx)
            .map(candle_ts_ms)
            .context("candle history index is out of range"),
        BacktestSeries::Orderbooks(items) => items
            .get(idx)
            .map(|item| item.timestamp_ms)
            .context("orderbook history index is out of range"),
        BacktestSeries::Vd(items) => items
            .get(idx)
            .map(vd_ts_ms)
            .context("vd history index is out of range"),
        BacktestSeries::Oi(items) => items
            .get(idx)
            .map(oi_ts_ms)
            .context("oi history index is out of range"),
        BacktestSeries::Volumes(items) => items
            .get(idx)
            .map(volume_ts_ms)
            .context("volumes history index is out of range"),
    }
}

fn backtest_history_record(series: &BacktestSeries, idx: usize) -> Result<(Value, Option<u64>)> {
    let ts_ms = backtest_series_ts_ms(series, idx)?;
    let record = match series {
        BacktestSeries::Candles(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Orderbooks(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Vd(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Oi(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Volumes(items) => serde_json::to_value(&items[idx]),
    }
    .context("failed to serialize backtest history record")?;
    let identity = (!matches!(series, BacktestSeries::Orderbooks(_))).then_some(ts_ms);
    Ok((record, identity))
}

fn clock_len(clock: &SourceConfig, data: &BacktestData) -> Result<usize> {
    match data.series.get(&clock.selector) {
        Some(BacktestSeries::Candles(items)) => Ok(items.len()),
        Some(BacktestSeries::Orderbooks(items)) => Ok(items.len()),
        Some(BacktestSeries::Vd(items)) => Ok(items.len()),
        Some(BacktestSeries::Oi(items)) => Ok(items.len()),
        Some(BacktestSeries::Volumes(items)) => Ok(items.len()),
        None => bail!("{} data not loaded", clock.selector),
    }
}

fn clock_ts_ms(clock: &SourceConfig, data: &BacktestData, idx: usize) -> Result<u64> {
    match data.series.get(&clock.selector) {
        Some(BacktestSeries::Candles(items)) => Ok(candle_ts_ms(&items[idx])),
        Some(BacktestSeries::Orderbooks(items)) => Ok(items[idx].timestamp_ms),
        Some(BacktestSeries::Vd(items)) => Ok(vd_ts_ms(&items[idx])),
        Some(BacktestSeries::Oi(items)) => Ok(oi_ts_ms(&items[idx])),
        Some(BacktestSeries::Volumes(items)) => Ok(volume_ts_ms(&items[idx])),
        None => bail!("{} data not loaded", clock.selector),
    }
}

fn clock_price(clock: &SourceConfig, data: &BacktestData, idx: usize) -> Result<f64> {
    match data.series.get(&clock.selector) {
        Some(BacktestSeries::Candles(items)) => Ok(items[idx].c),
        Some(BacktestSeries::Orderbooks(items)) => book_mid(&items[idx]),
        Some(BacktestSeries::Vd(items)) => {
            let record = &items[idx];
            Ok(record.c.unwrap_or(record.value))
        }
        Some(BacktestSeries::Oi(items)) => Ok(items[idx].c),
        Some(BacktestSeries::Volumes(items)) => items[idx]
            .reference_price()
            .context("volume record has no reference price"),
        None => bail!("{} data not loaded", clock.selector),
    }
}

fn clock_candle<'a>(
    clock: &SourceConfig,
    data: &'a BacktestData,
    idx: usize,
) -> Result<&'a ScriptCandle> {
    match data.series.get(&clock.selector) {
        Some(BacktestSeries::Candles(items)) => items
            .get(idx)
            .with_context(|| format!("{} record {idx} is out of range", clock.selector)),
        Some(_) => bail!("{} is not candle data", clock.selector),
        None => bail!("{} data not loaded", clock.selector),
    }
}

fn candle_ts_ms(candle: &ScriptCandle) -> u64 {
    candle.t
}

fn vd_ts_ms(candle: &ScriptVolumeDelta) -> u64 {
    candle.t
}

fn oi_ts_ms(candle: &ScriptOpenInterest) -> u64 {
    candle.t
}

fn volume_ts_ms(profile: &ScriptVolume) -> u64 {
    profile.t
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

fn apply_script_execution_commands(
    commands: Vec<ScriptExecutionCommand>,
    idx: usize,
    ts_ms: u64,
    current_price: f64,
    script_orders: &mut HashMap<String, SimulatedScriptOrder>,
    open_trades: &mut Vec<OpenTrade>,
    next_position_id: &mut usize,
) -> Result<usize> {
    let mut submitted = 0;
    for command in commands {
        match command {
            ScriptExecutionCommand::Trade { order, request } => {
                request.validate()?;
                if request.reduce_only {
                    bail!("ctx.trade reduceOnly is not yet supported by the backtest simulator");
                }
                if let Some(existing) = script_orders.get(&order.id) {
                    if existing.request != request {
                        bail!(
                            "ctx.trade key `{}` was reused with different order parameters",
                            request.key
                        );
                    }
                    continue;
                }
                let reference_price = request.order.price.unwrap_or(current_price);
                validate_script_protection(&request, reference_price)?;
                let order_id = order.id.clone();
                script_orders.insert(
                    order_id.clone(),
                    SimulatedScriptOrder {
                        order,
                        request: request.clone(),
                        submitted_idx: idx,
                        status: SimulatedOrderStatus::Pending,
                    },
                );
                submitted += 1;
                if request.order.kind == ScriptOrderKind::Market {
                    fill_script_order(
                        &order_id,
                        idx,
                        ts_ms,
                        current_price,
                        script_orders,
                        open_trades,
                        next_position_id,
                    )?;
                }
            }
            ScriptExecutionCommand::Cancel { request } => {
                cancel_script_order(request, script_orders)?;
            }
        }
    }
    Ok(submitted)
}

fn cancel_script_order(
    request: ScriptCancelRequest,
    script_orders: &mut HashMap<String, SimulatedScriptOrder>,
) -> Result<()> {
    request.validate()?;
    let Some(order_id) = script_orders
        .values()
        .find(|order| order.order.id == request.order || order.order.key == request.order)
        .map(|order| order.order.id.clone())
    else {
        bail!(
            "ctx.cancel could not find simulated order `{}`",
            request.order
        );
    };
    let order = script_orders
        .get_mut(&order_id)
        .context("simulated order disappeared during cancellation")?;
    if order.status == SimulatedOrderStatus::Pending {
        order.status = SimulatedOrderStatus::Cancelled;
    }
    Ok(())
}

fn fill_pending_script_orders(
    clock: &SourceConfig,
    data: &BacktestData,
    idx: usize,
    script_orders: &mut HashMap<String, SimulatedScriptOrder>,
    open_trades: &mut Vec<OpenTrade>,
    next_position_id: &mut usize,
) -> Result<()> {
    let ts_ms = clock_ts_ms(clock, data, idx)?;
    let mut fillable = Vec::new();
    for order in script_orders.values().filter(|order| {
        order.status == SimulatedOrderStatus::Pending
            && order.submitted_idx < idx
            && order.request.order.kind == ScriptOrderKind::Limit
    }) {
        let price = order
            .request
            .order
            .price
            .context("simulated limit order omitted its price")?;
        if limit_order_touched(clock, data, idx, &order.request, price)? {
            fillable.push((order.order.id.clone(), price));
        }
    }
    for (order_id, price) in fillable {
        fill_script_order(
            &order_id,
            idx,
            ts_ms,
            price,
            script_orders,
            open_trades,
            next_position_id,
        )?;
    }
    Ok(())
}

fn limit_order_touched(
    clock: &SourceConfig,
    data: &BacktestData,
    idx: usize,
    request: &ScriptTradeRequest,
    limit: f64,
) -> Result<bool> {
    use crate::domain::execution::PositionDirection;

    if clock.source == ScriptSource::Candles {
        let candle = clock_candle(clock, data, idx)?;
        return Ok(match request.side {
            PositionDirection::Long => candle.l <= limit,
            PositionDirection::Short => candle.h >= limit,
        });
    }
    let price = clock_price(clock, data, idx)?;
    Ok(match request.side {
        PositionDirection::Long => price <= limit,
        PositionDirection::Short => price >= limit,
    })
}

fn fill_script_order(
    order_id: &str,
    idx: usize,
    ts_ms: u64,
    price: f64,
    script_orders: &mut HashMap<String, SimulatedScriptOrder>,
    open_trades: &mut Vec<OpenTrade>,
    next_position_id: &mut usize,
) -> Result<()> {
    use crate::domain::execution::PositionDirection;

    let order = script_orders
        .get(order_id)
        .cloned()
        .with_context(|| format!("simulated order `{order_id}` was not found"))?;
    if order.status != SimulatedOrderStatus::Pending {
        return Ok(());
    }
    let side = match order.request.side {
        PositionDirection::Long => TradeSide::Long,
        PositionDirection::Short => TradeSide::Short,
    };
    let notional = order
        .request
        .notional
        .or_else(|| order.request.size.map(|size| size * price));
    open_trades.push(open_trade_from_entry(
        next_position_id,
        TradeEntry {
            side,
            idx,
            ts_ms,
            price,
            reason: format!("ctx.trade {}", order.request.key),
            notional,
            leverage: order.request.leverage,
            order_id: Some(order.order.id.clone()),
            stop_loss_price: order.request.sl,
            take_profit_price: order.request.tp,
        },
    ));
    script_orders
        .get_mut(order_id)
        .context("simulated order disappeared after fill")?
        .status = SimulatedOrderStatus::Filled;
    Ok(())
}

fn validate_script_protection(request: &ScriptTradeRequest, entry_price: f64) -> Result<()> {
    use crate::domain::execution::PositionDirection;

    match request.side {
        PositionDirection::Long => {
            if request.sl.is_some_and(|price| price >= entry_price) {
                bail!("long ctx.trade sl must be below the entry price");
            }
            if request.tp.is_some_and(|price| price <= entry_price) {
                bail!("long ctx.trade tp must be above the entry price");
            }
        }
        PositionDirection::Short => {
            if request.sl.is_some_and(|price| price <= entry_price) {
                bail!("short ctx.trade sl must be above the entry price");
            }
            if request.tp.is_some_and(|price| price >= entry_price) {
                bail!("short ctx.trade tp must be below the entry price");
            }
        }
    }
    Ok(())
}

fn apply_protective_triggers(
    clock: &SourceConfig,
    data: &BacktestData,
    idx: usize,
    open_trades: &mut Vec<OpenTrade>,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
) -> Result<()> {
    let ts_ms = clock_ts_ms(clock, data, idx)?;
    let mut open_index = 0;
    while open_index < open_trades.len() {
        let trigger = protective_trigger(clock, data, idx, &open_trades[open_index])?;
        let Some((price, reason)) = trigger else {
            open_index += 1;
            continue;
        };
        let open = open_trades.remove(open_index);
        close_open_trade(
            open,
            closed_trades,
            &TradeEvent {
                idx,
                ts_ms,
                price,
                reason,
            },
        );
    }
    Ok(())
}

fn protective_trigger(
    clock: &SourceConfig,
    data: &BacktestData,
    idx: usize,
    open: &OpenTrade,
) -> Result<Option<(f64, String)>> {
    if idx <= open.entry_idx {
        return Ok(None);
    }
    let (low, high) = if clock.source == ScriptSource::Candles {
        let candle = clock_candle(clock, data, idx)?;
        (candle.l, candle.h)
    } else {
        let price = clock_price(clock, data, idx)?;
        (price, price)
    };

    // With OHLC data the intra-bar path is unknown. If both sides are touched,
    // choose the stop first so the simulation does not assume the favorable path.
    let stop_hit = match (open.side, open.stop_loss_price) {
        (TradeSide::Long, Some(stop)) if low <= stop => Some(stop),
        (TradeSide::Short, Some(stop)) if high >= stop => Some(stop),
        _ => None,
    };
    if let Some(price) = stop_hit {
        return Ok(Some((price, "ctx.trade stop loss".to_string())));
    }
    let take_profit_hit = match (open.side, open.take_profit_price) {
        (TradeSide::Long, Some(target)) if high >= target => Some(target),
        (TradeSide::Short, Some(target)) if low <= target => Some(target),
        _ => None,
    };
    Ok(take_profit_hit.map(|price| (price, "ctx.trade take profit".to_string())))
}

fn open_trade_from_entry(next_position_id: &mut usize, entry: TradeEntry) -> OpenTrade {
    let id = format_position_id(*next_position_id);
    *next_position_id += 1;
    let notional = entry.notional.unwrap_or(1_000.0);
    let qty = if entry.price.abs() > f64::EPSILON {
        notional / entry.price
    } else {
        0.0
    };
    OpenTrade {
        id,
        order_id: entry.order_id,
        side: entry.side,
        entry_idx: entry.idx,
        entry_ts_ms: entry.ts_ms,
        entry_price: entry.price,
        notional,
        qty,
        leverage: entry.leverage,
        stop_loss_price: entry.stop_loss_price,
        take_profit_price: entry.take_profit_price,
        reason: entry.reason,
    }
}

fn close_open_trade(
    open: OpenTrade,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
    event: &TradeEvent,
) {
    let gross_pnl = trade_pnl(open.side, open.entry_price, event.price, open.qty);
    let fees = 0.0;
    let slippage = 0.0;
    let net_pnl = gross_pnl - fees - slippage;
    let margin = margin_for_notional(open.notional, open.leverage);
    let net_return = if margin.abs() > f64::EPSILON {
        net_pnl / margin
    } else {
        0.0
    };

    closed_trades.push(ScriptBacktestTrade {
        id: format_trade_id(closed_trades.len() + 1),
        position_id: open.id,
        order_id: open.order_id,
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
        leverage: open.leverage,
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

fn position_return(open_trades: &[OpenTrade], curr: f64, next: f64) -> f64 {
    if open_trades.is_empty() {
        return 0.0;
    }
    let pnl = open_trades
        .iter()
        .map(|open| trade_pnl(open.side, curr, next, open.qty))
        .sum::<f64>();
    let margin = open_trades
        .iter()
        .map(|open| margin_for_notional(open.notional, open.leverage))
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
) -> Vec<ScriptBacktestOpenPosition> {
    open_trades
        .iter()
        .map(|open| ScriptBacktestOpenPosition {
            id: open.id.clone(),
            order_id: open.order_id.clone(),
            side: open.side,
            entry_ts_ms: open.entry_ts_ms,
            entry_price: open.entry_price,
            mark_ts_ms,
            mark_price,
            notional: open.notional,
            margin: margin_for_notional(open.notional, open.leverage),
            leverage: open.leverage,
            qty: open.qty,
            stop_loss_price: open.stop_loss_price,
            take_profit_price: open.take_profit_price,
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
    orders: usize,
    script_orders: &HashMap<String, SimulatedScriptOrder>,
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
        orders,
        pending_orders: script_orders
            .values()
            .filter(|order| order.status == SimulatedOrderStatus::Pending)
            .count(),
        cancelled_orders: script_orders
            .values()
            .filter(|order| order.status == SimulatedOrderStatus::Cancelled)
            .count(),
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
                "  orders: {}\n  pending/cancelled orders: {}/{}\n  closed trades: {}\n  open positions: {}\n  wins/losses: {}/{}\n  win rate: {}",
                result.summary.orders,
                result.summary.pending_orders,
                result.summary.cancelled_orders,
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
            if verbose && let Some(latest_output) = &result.latest_output {
                println!();
                println!(
                    "latest_output: {}",
                    serde_json::to_string_pretty(latest_output)?
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
            provider: &result.provider,
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

    fn candle_clock() -> SourceConfig {
        SourceConfig {
            selector: "candles@binancef@mmt".to_string(),
            source: ScriptSource::Candles,
            provider: ProviderKind::Mmt,
            exchange: "binancef".to_string(),
            position: 0,
            timeframe: Some(60),
            depth: None,
            bucket: None,
        }
    }

    fn candle_data(candles: Vec<ScriptCandle>) -> BacktestData {
        BacktestData {
            series: BTreeMap::from([(
                "candles@binancef@mmt".to_string(),
                BacktestSeries::Candles(candles),
            )]),
        }
    }

    fn candle(idx: usize, open: f64, high: f64, low: f64, close: f64) -> ScriptCandle {
        ScriptCandle {
            t: 1_780_000_000_000 + idx as u64 * 60_000,
            o: open,
            h: high,
            l: low,
            c: close,
            volume: 1.0,
            trades: 1,
            close_time: None,
            vb: None,
            vs: None,
            tb: None,
            ts: None,
        }
    }

    fn script_trade(value: Value) -> ScriptExecutionCommand {
        let request: ScriptTradeRequest =
            serde_json::from_value(value).expect("valid script trade request");
        let order = ScriptOrderRef {
            id: crate::scripting::execution::local_order_id("backtest", &request.key),
            key: request.key.clone(),
        };
        ScriptExecutionCommand::Trade { order, request }
    }

    #[test]
    fn window_payload_exposes_source_metadata_without_source_records() {
        let configs = parse_source_configs(&[
            "candles@binancef@mmt:timeframe=60".to_string(),
            "candles@okx@mmt:timeframe=60".to_string(),
        ])
        .expect("parse source configs");
        let clock = &configs["candles@binancef@mmt"];
        let data = BacktestData {
            series: BTreeMap::from([
                (
                    "candles@binancef@mmt".to_string(),
                    BacktestSeries::Candles(vec![candle(0, 10.0, 10.0, 10.0, 10.0)]),
                ),
                (
                    "candles@okx@mmt".to_string(),
                    BacktestSeries::Candles(vec![candle(0, 20.0, 20.0, 20.0, 20.0)]),
                ),
            ]),
        };

        let payload = build_window_payload(WindowPayloadContext {
            source_configs: &configs,
            provider: "mmt",
            symbol: "BTC/USDT",
            clock,
            clock_idx: 0,
            cutoff_ms: clock_ts_ms(clock, &data, 0).unwrap(),
            mark_price: clock_price(clock, &data, 0).unwrap(),
            open_trades: &[],
        })
        .expect("build window payload");

        assert_eq!(payload["clock"], "candles@binancef@mmt");
        assert_eq!(payload["exchange"], "binancef");
        assert_eq!(
            payload["source_configs"]["candles@binancef@mmt"]["exchange"],
            "binancef"
        );
        assert_eq!(
            payload["source_configs"]["candles@okx@mmt"]["exchange"],
            "okx"
        );
        assert!(payload.get("sources").is_none());
        assert!(payload.get("candles").is_none());
    }

    #[test]
    fn backtest_history_is_incremental_and_exchange_qualified() {
        let path =
            std::env::temp_dir().join(format!("mlab-backtest-history-{}.js", std::process::id()));
        std::fs::write(
            &path,
            r#"
export const script = {
  name: "backtest-history",
  version: "1",
  sources: ["candles"],
  clock: "candles",
  lookback: 3,
  params: {}
};

export function onData(ctx, input, history) {
  const binance = history.source("candles@binancef@mmt");
  const okx = history.source("candles@okx@mmt");
  return {
    metrics: {
      binance: binance.map((candle) => candle.c),
      okx: okx.map((candle) => candle.c),
      current: history.source("candles@binancef@mmt", 0)?.c ?? null,
      previous: history.source("candles@binancef@mmt", 1)?.c ?? null,
      has_legacy_input: input.candles !== undefined || input.sources !== undefined
    }
  };
}
"#,
        )
        .expect("write history script");

        let script = Script::load(&path).expect("load history script");
        let configs = parse_source_configs(&[
            "candles@binancef@mmt:timeframe=60".to_string(),
            "candles@okx@mmt:timeframe=60".to_string(),
        ])
        .expect("parse source configs");
        let clock = &configs["candles@binancef@mmt"];
        let data = BacktestData {
            series: BTreeMap::from([
                (
                    "candles@binancef@mmt".to_string(),
                    BacktestSeries::Candles(vec![
                        candle(0, 10.0, 10.0, 10.0, 10.0),
                        candle(1, 11.0, 11.0, 11.0, 11.0),
                    ]),
                ),
                (
                    "candles@okx@mmt".to_string(),
                    BacktestSeries::Candles(vec![
                        candle(0, 20.0, 20.0, 20.0, 20.0),
                        candle(1, 21.0, 21.0, 21.0, 21.0),
                    ]),
                ),
            ]),
        };
        let session = script.start_session(&json!({})).expect("start session");
        let mut cursors = BacktestHistoryCursors::default();

        cursors
            .sync(
                &session,
                &data,
                &configs,
                clock,
                0,
                clock_ts_ms(clock, &data, 0).unwrap(),
            )
            .expect("sync first bar");
        let first = session
            .run_window(json!({ "mode": "window" }))
            .expect("run first hook");
        assert_eq!(first.output.metrics["binance"], json!([10]));
        assert_eq!(first.output.metrics["okx"], json!([20]));
        assert!(first.output.metrics["previous"].is_null());

        cursors
            .sync(
                &session,
                &data,
                &configs,
                clock,
                1,
                clock_ts_ms(clock, &data, 1).unwrap(),
            )
            .expect("sync second bar");
        let second = session
            .run_window(json!({ "mode": "window" }))
            .expect("run second hook");
        assert_eq!(second.output.metrics["binance"], json!([10, 11]));
        assert_eq!(second.output.metrics["okx"], json!([20, 21]));
        assert_eq!(second.output.metrics["current"], 11.0);
        assert_eq!(second.output.metrics["previous"], 10.0);
        assert_eq!(second.output.metrics["has_legacy_input"], false);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn script_limit_order_cannot_fill_on_its_submission_bar() {
        let data = candle_data(vec![
            candle(0, 100.0, 105.0, 85.0, 100.0),
            candle(1, 100.0, 101.0, 89.0, 95.0),
        ]);
        let clock = candle_clock();
        let mut orders = HashMap::new();
        let mut open = Vec::new();
        let mut next_position_id = 1;
        let submitted = apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "limit-1",
                "side": "long",
                "notional": 100,
                "leverage": 2,
                "order": { "type": "limit", "price": 90, "tif": "gtc" }
            }))],
            0,
            clock_ts_ms(&clock, &data, 0).unwrap(),
            100.0,
            &mut orders,
            &mut open,
            &mut next_position_id,
        )
        .expect("queue limit order");
        assert_eq!(submitted, 1);

        fill_pending_script_orders(
            &clock,
            &data,
            0,
            &mut orders,
            &mut open,
            &mut next_position_id,
        )
        .expect("same-bar check");
        assert!(open.is_empty());

        fill_pending_script_orders(
            &clock,
            &data,
            1,
            &mut orders,
            &mut open,
            &mut next_position_id,
        )
        .expect("next-bar fill");
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].entry_price, 90.0);
        assert_eq!(open[0].leverage, 2.0);
    }

    #[test]
    fn simulated_oco_uses_stop_when_both_triggers_touch_same_candle() {
        let data = candle_data(vec![
            candle(0, 100.0, 105.0, 95.0, 100.0),
            candle(1, 100.0, 125.0, 85.0, 105.0),
        ]);
        let clock = candle_clock();
        let mut orders = HashMap::new();
        let mut open = Vec::new();
        let mut closed = Vec::new();
        let mut next_position_id = 1;
        apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "protected-1",
                "side": "long",
                "notional": 100,
                "leverage": 2,
                "order": { "type": "market" },
                "sl": 90,
                "tp": 120
            }))],
            0,
            clock_ts_ms(&clock, &data, 0).unwrap(),
            100.0,
            &mut orders,
            &mut open,
            &mut next_position_id,
        )
        .expect("fill market order");

        apply_protective_triggers(&clock, &data, 1, &mut open, &mut closed)
            .expect("apply protection");
        assert!(open.is_empty());
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].exit.price, 90.0);
        assert_eq!(closed[0].exit.reason, "ctx.trade stop loss");
    }
}
