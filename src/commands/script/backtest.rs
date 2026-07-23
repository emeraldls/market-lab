use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};
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
use crate::providers::binance::{BinanceMarket, BinanceProvider};
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::hyperliquid::market_data::HyperliquidProvider;
use crate::providers::mmt::MmtProvider;
use crate::scripting::engine::Script;
use crate::scripting::execution::{
    ScriptCancelRequest, ScriptExecutionCommand, ScriptExecutionContext, ScriptManagedRequest,
    ScriptOrderKind, ScriptOrderRef, ScriptRawOrderRequest, ScriptTradeRequest,
};
use crate::scripting::inputs::{
    SourceConfig, SourceConfigs, parse_param_values, parse_source_configs, resolve_params,
    source_configs_payload, source_exchange_label, source_provider_label, source_provider_name,
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
    capital_required: f64,
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
    events_held: usize,
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
    events_held: usize,
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
    margin: f64,
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum SimulatedFillOutcome {
    Filled(f64),
    Cancelled,
}

#[derive(Debug, Clone)]
struct SimulatedScriptOrder {
    order: ScriptOrderRef,
    request: ScriptManagedRequest,
    submitted_idx: usize,
    status: SimulatedOrderStatus,
}

struct ScriptSimulationState {
    orders: HashMap<String, SimulatedScriptOrder>,
    open_trades: Vec<OpenTrade>,
    closed_trades: Vec<ScriptBacktestTrade>,
    next_position_id: usize,
    execution_events: VecDeque<Value>,
    next_execution_event_seq: u64,
}

impl Default for ScriptSimulationState {
    fn default() -> Self {
        Self {
            orders: HashMap::new(),
            open_trades: Vec::new(),
            closed_trades: Vec::new(),
            next_position_id: 1,
            execution_events: VecDeque::new(),
            next_execution_event_seq: 0,
        }
    }
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
    margin: Option<f64>,
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

#[derive(Debug, Clone)]
struct BacktestEvent {
    selector: String,
    record_idx: usize,
    ts_ms: u64,
    source_position: usize,
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

    let result = backtest_events(args, script, source_configs, resolved_params, &mut report).await;
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(err) if err.is::<ScriptCancelled>() => report.finish_cancelled(),
        Err(err) => report.finish_error(err),
    };
    write_report_best_effort(&runtime_report);
    result
}

async fn backtest_events(
    args: ScriptBacktestArgs,
    script: Script,
    source_configs: SourceConfigs,
    resolved_params: Value,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
) -> Result<()> {
    let data = fetch_sources(&args, &script, &source_configs, report).await?;
    let events = build_event_timeline(&data, &source_configs)?;
    if events.is_empty() {
        bail!("script backtest received no source events in the requested range");
    }
    let reference_source = resolve_reference_source(&data, &source_configs)?;
    let reference_selector = reference_source.selector.clone();

    let mut returns = Vec::new();
    let mut orders = 0_usize;
    let mut simulation = ScriptSimulationState::default();
    let mut peak_margin = 0.0_f64;
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
    let exchange_label = source_exchange_label(&source_configs);
    let mut latest_reference_price = None;
    let mut latest_reference_ts = args.from;
    eprintln!(
        "running script={} sources={} events={} reference={} lookback={}",
        script.manifest.name,
        script.manifest.source_names(),
        events.len(),
        reference_selector,
        lookback
    );
    report.set_progress("executing_hooks", 0, events.len() as u64);
    write_running_report_best_effort(report);

    for (idx, event) in events.iter().enumerate() {
        if session.is_cancelled() {
            report.set_progress("cancelled", idx as u64, events.len() as u64);
            return Err(ScriptCancelled.into());
        }

        let config = source_configs
            .get(&event.selector)
            .with_context(|| format!("missing source config for {}", event.selector))?;
        let series = data
            .series
            .get(&event.selector)
            .with_context(|| format!("{} data not loaded", event.selector))?;
        if event.selector == reference_selector
            && let Some(price) = backtest_series_reference_price(series, event.record_idx)?
        {
            if let Some(previous_price) = latest_reference_price {
                returns.push(position_return(
                    &simulation.open_trades,
                    previous_price,
                    price,
                ));
            }
            latest_reference_price = Some(price);
            latest_reference_ts = event.ts_ms;
            apply_protective_triggers(
                config,
                &data,
                event.record_idx,
                idx,
                &mut simulation.open_trades,
                &mut simulation.closed_trades,
            )?;
            fill_pending_script_orders(config, &data, event.record_idx, idx, &mut simulation)?;
            orders += dispatch_simulated_execution_events(
                &session,
                idx,
                event.ts_ms,
                latest_reference_price,
                &mut simulation,
                report,
                &mut latest_output,
            )?;
            peak_margin = peak_margin.max(open_position_margin(&simulation.open_trades));
        }
        let payload = build_event_payload(EventPayloadContext {
            source_configs: &source_configs,
            symbol: &args.symbol,
            config,
            series,
            record_idx: event.record_idx,
            event_idx: idx,
            mark_ts_ms: latest_reference_ts,
            mark_price: latest_reference_price.unwrap_or_default(),
            open_trades: &simulation.open_trades,
        })?;
        let execution = match session.run_event(payload) {
            Ok(execution) => execution,
            Err(err) => {
                report.record_hook_failure();
                if session.is_cancelled() {
                    report.set_progress("cancelled", idx as u64, events.len() as u64);
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
            event.ts_ms,
            latest_reference_price,
            &mut simulation,
        )?;
        peak_margin = peak_margin.max(open_position_margin(&simulation.open_trades));
        if script_order_count > 0 {
            orders += script_order_count;
        }

        if !output.is_empty() {
            latest_output = Some(ScriptBacktestLatestOutput {
                metrics: output.metrics,
                meta: output.meta,
            });
        }
        orders += dispatch_simulated_execution_events(
            &session,
            idx,
            event.ts_ms,
            latest_reference_price,
            &mut simulation,
            report,
            &mut latest_output,
        )?;
        peak_margin = peak_margin.max(open_position_margin(&simulation.open_trades));

        if (idx + 1) % 500 == 0 || idx + 1 == events.len() {
            eprintln!("processed {}/{} source events", idx + 1, events.len());
            report.set_progress("executing_hooks", (idx + 1) as u64, events.len() as u64);
            write_running_report_best_effort(report);
        }
    }

    let mark_price = latest_reference_price.context("backtest produced no reference price")?;
    let open_positions = open_trades_to_positions(
        &simulation.open_trades,
        events.len().saturating_sub(1),
        latest_reference_ts,
        mark_price,
    );
    let summary = backtest_summary(
        orders,
        &simulation.orders,
        &simulation.closed_trades,
        &open_positions,
    );
    let performance = backtest_performance(
        &returns,
        &simulation.closed_trades,
        &open_positions,
        peak_margin,
    );
    let result = ScriptBacktestResult {
        r#type: "script.backtest.result",
        version: "1",
        provider: provider_label,
        exchange: exchange_label,
        symbol: args.symbol.clone(),
        ts_ms: latest_reference_ts,
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
        },
        params: ScriptInputs {
            values: resolved_params,
        },
        summary,
        performance,
        closed_trades: simulation.closed_trades,
        open_positions,
        latest_output,
        meta: json!({
            "events": events.len(),
            "reference_source": reference_selector,
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
            fetch_direct_sources(args, source_configs, report, ProviderKind::Bulk)
                .await?
                .series,
        );
    }
    if source_configs
        .values()
        .any(|config| config.provider == ProviderKind::Hyperliquid)
    {
        data.series.extend(
            fetch_direct_sources(args, source_configs, report, ProviderKind::Hyperliquid)
                .await?
                .series,
        );
    }
    for provider in [ProviderKind::Binance, ProviderKind::BinanceFutures] {
        if source_configs
            .values()
            .any(|config| config.provider == provider)
        {
            data.series.extend(
                fetch_direct_sources(args, source_configs, report, provider)
                    .await?
                    .series,
            );
        }
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

async fn fetch_direct_sources(
    args: &ScriptBacktestArgs,
    source_configs: &SourceConfigs,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
    provider: ProviderKind,
) -> Result<BacktestData> {
    let mut data = BacktestData::default();
    let mut cancel = Box::pin(tokio::signal::ctrl_c());

    let mut configs = source_configs.values().collect::<Vec<_>>();
    configs.retain(|config| config.provider == provider);
    configs.sort_by_key(|config| config.position);
    for config in configs {
        let source = &config.source;
        let timeframe = config.require_timeframe(source)?;
        let provider_name = match provider {
            ProviderKind::Bulk => "BULK",
            ProviderKind::Hyperliquid => "Hyperliquid",
            ProviderKind::Binance => "Binance Spot",
            ProviderKind::BinanceFutures => "Binance Futures",
            _ => bail!("historical direct source provider is invalid"),
        };
        let interval = match provider {
            ProviderKind::Bulk => {
                crate::providers::bulk::market_data::timeframe_from_seconds(timeframe)?
            }
            ProviderKind::Hyperliquid => {
                crate::providers::hyperliquid::market_data::timeframe_from_seconds(timeframe)?
            }
            ProviderKind::Binance | ProviderKind::BinanceFutures => {
                crate::providers::binance::market_data::timeframe_from_seconds(timeframe)?
            }
            _ => unreachable!(),
        };
        let phase = match source {
            ScriptSource::Candles => "fetching_candles",
            ScriptSource::Volumes => "fetching_volumes",
            ScriptSource::Orderbook | ScriptSource::Vd | ScriptSource::Oi => {
                bail!(
                    "{} does not provide historical {} for script backtests",
                    provider_name,
                    source.as_str(),
                );
            }
        };
        report.set_phase(phase);
        write_running_report_best_effort(report);
        let started = Instant::now();
        eprintln!(
            "fetching {} {} symbol={} tf={} from={} to={}",
            provider_name,
            source.as_str(),
            args.symbol,
            timeframe,
            args.from,
            args.to
        );
        let future = async {
            match provider {
                ProviderKind::Bulk => {
                    BulkProvider::candles(&args.symbol, interval, args.from, args.to).await
                }
                ProviderKind::Hyperliquid => {
                    HyperliquidProvider::candles(&args.symbol, interval, args.from, args.to).await
                }
                ProviderKind::Binance => {
                    BinanceProvider::candles_paginated(
                        BinanceMarket::Spot,
                        &args.symbol,
                        interval,
                        args.from,
                        args.to,
                    )
                    .await
                }
                ProviderKind::BinanceFutures => {
                    BinanceProvider::candles_paginated(
                        BinanceMarket::Futures,
                        &args.symbol,
                        interval,
                        args.from,
                        args.to,
                    )
                    .await
                }
                _ => unreachable!(),
            }
        };
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
            "fetched {points} {} {} records in {}ms",
            provider_name,
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

struct EventPayloadContext<'a> {
    source_configs: &'a SourceConfigs,
    symbol: &'a str,
    config: &'a SourceConfig,
    series: &'a BacktestSeries,
    record_idx: usize,
    event_idx: usize,
    mark_ts_ms: u64,
    mark_price: f64,
    open_trades: &'a [OpenTrade],
}

fn build_event_payload(ctx: EventPayloadContext<'_>) -> Result<Value> {
    let mut root = Map::new();
    root.insert(
        "provider".to_string(),
        Value::String(source_provider_name(ctx.config.provider).to_string()),
    );
    root.insert(
        "exchange".to_string(),
        Value::String(ctx.config.exchange.clone()),
    );
    root.insert("symbol".to_string(), Value::String(ctx.symbol.to_string()));
    root.insert(
        "source".to_string(),
        Value::String(ctx.config.selector.clone()),
    );
    root.insert(
        "source_type".to_string(),
        Value::String(ctx.config.source.as_str().to_string()),
    );
    root.insert(
        "data".to_string(),
        backtest_record_payload(ctx.series, ctx.record_idx, ctx.config)?,
    );
    root.insert(
        "source_configs".to_string(),
        source_configs_payload(ctx.source_configs),
    );
    let open_positions = open_trades_to_positions(
        ctx.open_trades,
        ctx.event_idx,
        ctx.mark_ts_ms,
        ctx.mark_price,
    );
    root.insert("positions".to_string(), json!({ "open": open_positions }));

    Ok(Value::Object(root))
}

fn build_event_timeline(
    data: &BacktestData,
    source_configs: &SourceConfigs,
) -> Result<Vec<BacktestEvent>> {
    let mut events = Vec::new();
    for config in source_configs.values() {
        let series = data
            .series
            .get(&config.selector)
            .with_context(|| format!("{} data not loaded", config.selector))?;
        for record_idx in 0..backtest_series_len(series) {
            events.push(BacktestEvent {
                selector: config.selector.clone(),
                record_idx,
                ts_ms: backtest_series_event_ts_ms(series, record_idx, config)?,
                source_position: config.position,
            });
        }
    }
    events.sort_by_key(|event| (event.ts_ms, event.source_position, event.record_idx));
    Ok(events)
}

fn resolve_reference_source<'a>(
    data: &BacktestData,
    source_configs: &'a SourceConfigs,
) -> Result<&'a SourceConfig> {
    let mut configs = source_configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    for config in configs {
        let series = data
            .series
            .get(&config.selector)
            .with_context(|| format!("{} data not loaded", config.selector))?;
        for idx in 0..backtest_series_len(series) {
            if backtest_series_reference_price(series, idx)?.is_some() {
                return Ok(config);
            }
        }
    }
    bail!("script backtest requires a price-bearing source such as candles, orderbook, or volumes")
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

fn backtest_series_event_ts_ms(
    series: &BacktestSeries,
    idx: usize,
    config: &SourceConfig,
) -> Result<u64> {
    if let BacktestSeries::Orderbooks(_) = series {
        return backtest_series_ts_ms(series, idx);
    }
    if let BacktestSeries::Candles(items) = series
        && let Some(close_time) = items.get(idx).and_then(|item| item.close_time)
    {
        return Ok(close_time);
    }
    if let BacktestSeries::Volumes(items) = series
        && let Some(close_time) = items.get(idx).and_then(|item| item.close_time)
    {
        return Ok(close_time);
    }
    let timeframe_ms = u64::from(config.require_timeframe(&config.source)?) * 1_000;
    Ok(backtest_series_ts_ms(series, idx)?.saturating_add(timeframe_ms))
}

fn backtest_record_payload(
    series: &BacktestSeries,
    idx: usize,
    config: &SourceConfig,
) -> Result<Value> {
    let record = match series {
        BacktestSeries::Candles(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Orderbooks(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Vd(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Oi(items) => serde_json::to_value(&items[idx]),
        BacktestSeries::Volumes(items) => serde_json::to_value(&items[idx]),
    }
    .context("failed to serialize backtest source event")?;
    Ok(match &config.source {
        ScriptSource::Candles => json!({ "candle": record }),
        ScriptSource::Orderbook => json!({ "snapshot": record }),
        ScriptSource::Vd => json!({
            "candle": record,
            "record": record,
            "bucket": config.bucket,
            "timeframe_sec": config.timeframe,
        }),
        ScriptSource::Oi => json!({
            "candle": record,
            "record": record,
            "timeframe_sec": config.timeframe,
        }),
        ScriptSource::Volumes => json!({
            "profile": record,
            "record": record,
            "timeframe_sec": config.timeframe,
        }),
    })
}

fn backtest_series_reference_price(series: &BacktestSeries, idx: usize) -> Result<Option<f64>> {
    let price = match series {
        BacktestSeries::Candles(items) => items.get(idx).map(|item| item.c),
        BacktestSeries::Orderbooks(items) => items.get(idx).map(book_mid).transpose()?,
        BacktestSeries::Vd(_) => None,
        BacktestSeries::Oi(items) => items.get(idx).and_then(|item| item.mark_price),
        BacktestSeries::Volumes(items) => items.get(idx).and_then(ScriptVolume::reference_price),
    };
    Ok(price.filter(|price| price.is_finite() && *price > 0.0))
}

fn backtest_candle<'a>(
    config: &SourceConfig,
    data: &'a BacktestData,
    idx: usize,
) -> Result<&'a ScriptCandle> {
    match data.series.get(&config.selector) {
        Some(BacktestSeries::Candles(items)) => items
            .get(idx)
            .with_context(|| format!("{} record {idx} is out of range", config.selector)),
        Some(_) => bail!("{} is not candle data", config.selector),
        None => bail!("{} data not loaded", config.selector),
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
    current_price: Option<f64>,
    simulation: &mut ScriptSimulationState,
) -> Result<usize> {
    let mut submitted = 0;
    for command in commands {
        match command {
            ScriptExecutionCommand::Trade { order, request } => {
                request.validate()?;
                validate_position_transition(&request, &simulation.open_trades)?;
                let reference_price = request.order.price.or(current_price).context(
                    "ctx.trade requires a price-bearing source before submitting this order",
                )?;
                validate_script_protection(&request, reference_price)?;
                submitted += usize::from(submit_simulated_script_order(
                    order,
                    ScriptManagedRequest::Trade(request),
                    "ctx.trade",
                    idx,
                    ts_ms,
                    current_price,
                    simulation,
                )?);
            }
            ScriptExecutionCommand::Order { order, request } => {
                request.validate()?;
                submitted += usize::from(submit_simulated_script_order(
                    order,
                    ScriptManagedRequest::Order(request),
                    "ctx.order",
                    idx,
                    ts_ms,
                    current_price,
                    simulation,
                )?);
            }
            ScriptExecutionCommand::Cancel { request } => {
                cancel_script_order(request, ts_ms, simulation)?;
            }
        }
    }
    Ok(submitted)
}

fn dispatch_simulated_execution_events(
    session: &crate::scripting::engine::ScriptSession,
    idx: usize,
    ts_ms: u64,
    current_price: Option<f64>,
    simulation: &mut ScriptSimulationState,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
    latest_output: &mut Option<ScriptBacktestLatestOutput>,
) -> Result<usize> {
    let mut submitted = 0;
    let mut dispatched = 0_usize;
    while let Some(event) = simulation.execution_events.pop_front() {
        dispatched += 1;
        if dispatched > 10_000 {
            bail!("simulated onExecution hooks produced too many recursive events");
        }
        let execution = match session.run_execution_event(event) {
            Ok(execution) => execution,
            Err(error) => {
                report.record_hook_failure();
                return Err(error);
            }
        };
        let Some(execution) = execution else {
            continue;
        };
        report.record_hook(&execution.stats);
        submitted += apply_script_execution_commands(
            execution.commands,
            idx,
            ts_ms,
            current_price,
            simulation,
        )?;
        if !execution.output.is_empty() {
            *latest_output = Some(ScriptBacktestLatestOutput {
                metrics: execution.output.metrics,
                meta: execution.output.meta,
            });
        }
    }
    Ok(submitted)
}

fn submit_simulated_script_order(
    order: ScriptOrderRef,
    request: ScriptManagedRequest,
    operation: &str,
    idx: usize,
    ts_ms: u64,
    current_price: Option<f64>,
    simulation: &mut ScriptSimulationState,
) -> Result<bool> {
    if let Some(existing) = simulation.orders.get(&order.id) {
        if existing.request != request {
            bail!(
                "{operation} key `{}` was reused with different order parameters",
                request.key()
            );
        }
        return Ok(false);
    }
    request.order().price.or(current_price).with_context(|| {
        format!("{operation} requires a price-bearing source before submitting this order")
    })?;
    let order_id = order.id.clone();
    let is_market = request.order().kind == ScriptOrderKind::Market;
    simulation.orders.insert(
        order_id.clone(),
        SimulatedScriptOrder {
            order,
            request,
            submitted_idx: idx,
            status: SimulatedOrderStatus::Pending,
        },
    );
    let submitted = simulation
        .orders
        .get(&order_id)
        .cloned()
        .context("simulated order disappeared after submission")?;
    queue_simulated_order_event(
        simulation,
        &submitted,
        ts_ms,
        "order.pending",
        "pending",
        false,
        serde_json::to_value(&submitted.request)?,
    );
    if !is_market {
        queue_simulated_order_event(
            simulation,
            &submitted,
            ts_ms,
            "order.accepted",
            "resting",
            false,
            Value::Null,
        );
    }
    if is_market {
        let fill_price = current_price.with_context(|| {
            format!("{operation} market order requires a price-bearing source event first")
        })?;
        fill_script_order(&order_id, idx, ts_ms, fill_price, simulation)?;
    }
    Ok(true)
}

fn cancel_script_order(
    request: ScriptCancelRequest,
    ts_ms: u64,
    simulation: &mut ScriptSimulationState,
) -> Result<()> {
    request.validate()?;
    let Some(order_id) = simulation
        .orders
        .values()
        .find(|order| order.order.id == request.order || order.order.key == request.order)
        .map(|order| order.order.id.clone())
    else {
        bail!(
            "ctx.cancel could not find simulated order `{}`",
            request.order
        );
    };
    let order = simulation
        .orders
        .get_mut(&order_id)
        .context("simulated order disappeared during cancellation")?;
    if order.status == SimulatedOrderStatus::Pending {
        order.status = SimulatedOrderStatus::Cancelled;
        let order = order.clone();
        queue_simulated_order_event(
            simulation,
            &order,
            ts_ms,
            "order.cancelled",
            "cancelled",
            true,
            Value::Null,
        );
    }
    Ok(())
}

fn queue_simulated_order_event(
    simulation: &mut ScriptSimulationState,
    order: &SimulatedScriptOrder,
    ts_ms: u64,
    event_type: &str,
    status: &str,
    terminal: bool,
    data: Value,
) {
    simulation.next_execution_event_seq = simulation.next_execution_event_seq.saturating_add(1);
    simulation.execution_events.push_back(json!({
        "seq": simulation.next_execution_event_seq,
        "jobId": "backtest",
        "tsMs": ts_ms,
        "type": event_type,
        "orderId": order.order.id,
        "key": order.order.key,
        "venue": "bulk",
        "status": status,
        "terminal": terminal,
        "data": data,
    }));
}

fn fill_pending_script_orders(
    config: &SourceConfig,
    data: &BacktestData,
    record_idx: usize,
    event_idx: usize,
    simulation: &mut ScriptSimulationState,
) -> Result<()> {
    let series = data
        .series
        .get(&config.selector)
        .with_context(|| format!("{} data not loaded", config.selector))?;
    let ts_ms = backtest_series_event_ts_ms(series, record_idx, config)?;
    let mut fillable = Vec::new();
    for order in simulation.orders.values().filter(|order| {
        order.status == SimulatedOrderStatus::Pending
            && order.submitted_idx < event_idx
            && order.request.order().kind == ScriptOrderKind::Limit
    }) {
        let price = order
            .request
            .order()
            .price
            .context("simulated limit order omitted its price")?;
        if limit_order_touched(config, data, record_idx, &order.request, price)? {
            fillable.push((order.submitted_idx, order.order.id.clone(), price));
        }
    }
    // OHLC cannot reveal the path between two touched limits. Keep the result reproducible by
    // applying older orders first, then stable local order id within the same submission event.
    fillable.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
    for (_, order_id, price) in fillable {
        fill_script_order(&order_id, event_idx, ts_ms, price, simulation)?;
    }
    Ok(())
}

fn limit_order_touched(
    config: &SourceConfig,
    data: &BacktestData,
    record_idx: usize,
    request: &ScriptManagedRequest,
    limit: f64,
) -> Result<bool> {
    if config.source == ScriptSource::Candles {
        let candle = backtest_candle(config, data, record_idx)?;
        return Ok(match request.order_direction() {
            crate::domain::execution::PositionDirection::Long => candle.l <= limit,
            crate::domain::execution::PositionDirection::Short => candle.h >= limit,
        });
    }
    let series = data
        .series
        .get(&config.selector)
        .with_context(|| format!("{} data not loaded", config.selector))?;
    let price = backtest_series_reference_price(series, record_idx)?
        .context("limit order evaluation requires a price-bearing source event")?;
    Ok(match request.order_direction() {
        crate::domain::execution::PositionDirection::Long => price <= limit,
        crate::domain::execution::PositionDirection::Short => price >= limit,
    })
}

fn fill_script_order(
    order_id: &str,
    idx: usize,
    ts_ms: u64,
    price: f64,
    simulation: &mut ScriptSimulationState,
) -> Result<()> {
    let order = simulation
        .orders
        .get(order_id)
        .cloned()
        .with_context(|| format!("simulated order `{order_id}` was not found"))?;
    if order.status != SimulatedOrderStatus::Pending {
        return Ok(());
    }
    let outcome =
        match &order.request {
            ScriptManagedRequest::Trade(request) => SimulatedFillOutcome::Filled(
                fill_simulated_trade(request, &order.order, idx, ts_ms, price, simulation)?,
            ),
            ScriptManagedRequest::Order(request) => {
                fill_simulated_raw_order(request, &order.order, idx, ts_ms, price, simulation)?
            }
        };
    let status = match outcome {
        SimulatedFillOutcome::Filled(_) => SimulatedOrderStatus::Filled,
        SimulatedFillOutcome::Cancelled => SimulatedOrderStatus::Cancelled,
    };
    let managed = {
        let managed = simulation
            .orders
            .get_mut(order_id)
            .context("simulated order disappeared after fill")?;
        managed.status = status;
        managed.clone()
    };
    match outcome {
        SimulatedFillOutcome::Filled(size) => {
            let data = json!({
                "price": price,
                "size": size,
                "side": match managed.request.order_direction() {
                    crate::domain::execution::PositionDirection::Long => "buy",
                    crate::domain::execution::PositionDirection::Short => "sell",
                }
            });
            queue_simulated_order_event(
                simulation,
                &managed,
                ts_ms,
                "order.fill",
                "filled",
                false,
                data.clone(),
            );
            queue_simulated_order_event(
                simulation,
                &managed,
                ts_ms,
                "order.filled",
                "filled",
                true,
                data,
            );
        }
        SimulatedFillOutcome::Cancelled => queue_simulated_order_event(
            simulation,
            &managed,
            ts_ms,
            "order.cancelled",
            "cancelledReduceOnly",
            true,
            json!({ "reason": "reduce-only order would not reduce the net position" }),
        ),
    }
    Ok(())
}

fn fill_simulated_trade(
    request: &ScriptTradeRequest,
    order: &ScriptOrderRef,
    idx: usize,
    ts_ms: u64,
    price: f64,
    simulation: &mut ScriptSimulationState,
) -> Result<f64> {
    validate_position_transition(request, &simulation.open_trades)?;
    if request.position.is_open() {
        let side = trade_side(request.position.position_direction());
        let leverage = request.leverage_or_default();
        let notional = request
            .margin
            .map(|margin| margin * leverage)
            .or_else(|| request.size.map(|size| size * price));
        let margin = request
            .margin
            .or_else(|| notional.map(|notional| margin_for_notional(notional, leverage)));
        let opened = open_trade_from_entry(
            &mut simulation.next_position_id,
            TradeEntry {
                side,
                idx,
                ts_ms,
                price,
                reason: format!("ctx.trade {}", request.key),
                notional,
                margin,
                leverage,
                order_id: Some(order.id.clone()),
                stop_loss_price: request.sl,
                take_profit_price: request.tp,
            },
        );
        let filled_qty = opened.qty;
        if let Some(existing) = simulation.open_trades.first_mut() {
            add_to_open_position(existing, opened);
        } else {
            simulation.open_trades.push(opened);
        }
        Ok(filled_qty)
    } else {
        let side = trade_side(request.position.position_direction());
        let open_index = simulation
            .open_trades
            .iter()
            .position(|open| open.side == side)
            .context("matching simulated position disappeared before the close filled")?;
        let close_qty = request
            .size
            .unwrap_or(simulation.open_trades[open_index].qty);
        close_position_quantity(
            open_index,
            close_qty,
            &mut simulation.open_trades,
            &mut simulation.closed_trades,
            &TradeEvent {
                idx,
                ts_ms,
                price,
                reason: format!("ctx.trade {}", request.key),
            },
        )?;
        Ok(close_qty)
    }
}

fn fill_simulated_raw_order(
    request: &ScriptRawOrderRequest,
    order: &ScriptOrderRef,
    idx: usize,
    ts_ms: u64,
    price: f64,
    simulation: &mut ScriptSimulationState,
) -> Result<SimulatedFillOutcome> {
    let side = trade_side(request.side.order_direction());
    let leverage = request.leverage_or_default();
    let notional = request
        .margin
        .map(|margin| margin * leverage)
        .or_else(|| request.size.map(|size| size * price))
        .context("ctx.order simulation could not determine order notional")?;
    let quantity = notional / price;
    let existing = simulation.open_trades.first().map(|open| open.side);

    if (existing.is_none() || existing == Some(side)) && request.reduce_only {
        return Ok(SimulatedFillOutcome::Cancelled);
    }

    let event = TradeEvent {
        idx,
        ts_ms,
        price,
        reason: format!("ctx.order {}", request.key),
    };
    let mut remaining = quantity;
    if existing.is_some_and(|existing_side| existing_side != side) {
        let closing = remaining.min(simulation.open_trades[0].qty);
        close_position_quantity(
            0,
            closing,
            &mut simulation.open_trades,
            &mut simulation.closed_trades,
            &event,
        )?;
        remaining -= closing;
        if request.reduce_only {
            return Ok(SimulatedFillOutcome::Filled(closing));
        }
    }

    if remaining > f64::EPSILON {
        let opened = open_trade_from_entry(
            &mut simulation.next_position_id,
            TradeEntry {
                side,
                idx,
                ts_ms,
                price,
                reason: event.reason,
                notional: Some(remaining * price),
                margin: Some(margin_for_notional(remaining * price, leverage)),
                leverage,
                order_id: Some(order.id.clone()),
                stop_loss_price: None,
                take_profit_price: None,
            },
        );
        if let Some(existing) = simulation.open_trades.first_mut() {
            add_to_open_position(existing, opened);
        } else {
            simulation.open_trades.push(opened);
        }
    }
    Ok(SimulatedFillOutcome::Filled(quantity))
}

fn validate_position_transition(
    request: &ScriptTradeRequest,
    open_trades: &[OpenTrade],
) -> Result<()> {
    let target = trade_side(request.position.position_direction());
    let Some(open) = open_trades.first() else {
        if request.position.is_close() {
            bail!(
                "ctx.trade {} requires an open {} position",
                request.position.as_str(),
                format_side(target)
            );
        }
        return Ok(());
    };

    if request.position.is_open() && open.side != target {
        let required_close = match open.side {
            TradeSide::Long => "close-long",
            TradeSide::Short => "close-short",
        };
        bail!(
            "ctx.trade {} cannot reverse an open {} position; submit {required_close} first",
            request.position.as_str(),
            format_side(open.side)
        );
    }
    if request.position.is_close() && open.side != target {
        bail!(
            "ctx.trade {} requires an open {} position",
            request.position.as_str(),
            format_side(target)
        );
    }
    Ok(())
}

fn trade_side(direction: crate::domain::execution::PositionDirection) -> TradeSide {
    match direction {
        crate::domain::execution::PositionDirection::Long => TradeSide::Long,
        crate::domain::execution::PositionDirection::Short => TradeSide::Short,
    }
}

fn add_to_open_position(existing: &mut OpenTrade, added: OpenTrade) {
    debug_assert_eq!(existing.side, added.side);
    let qty = existing.qty + added.qty;
    if qty > f64::EPSILON {
        existing.entry_price =
            ((existing.entry_price * existing.qty) + (added.entry_price * added.qty)) / qty;
    }
    existing.qty = qty;
    existing.notional += added.notional;
    existing.leverage = added.leverage;
    existing.margin = margin_for_notional(existing.notional, existing.leverage);
    if added.stop_loss_price.is_some() {
        existing.stop_loss_price = added.stop_loss_price;
    }
    if added.take_profit_price.is_some() {
        existing.take_profit_price = added.take_profit_price;
    }
}

fn close_position_quantity(
    open_index: usize,
    close_qty: f64,
    open_trades: &mut Vec<OpenTrade>,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
    event: &TradeEvent,
) -> Result<()> {
    let open_qty = open_trades[open_index].qty;
    let tolerance = (open_qty.abs() * 1e-9).max(f64::EPSILON);
    if close_qty > open_qty + tolerance {
        bail!("ctx.trade close size {close_qty} exceeds the open position size {open_qty}");
    }

    let closed = if close_qty >= open_qty - tolerance {
        open_trades.remove(open_index)
    } else {
        let fraction = close_qty / open_qty;
        let mut closed = open_trades[open_index].clone();
        closed.qty = close_qty;
        closed.notional *= fraction;
        closed.margin *= fraction;

        let open = &mut open_trades[open_index];
        open.qty -= close_qty;
        open.notional -= closed.notional;
        open.margin -= closed.margin;
        closed
    };
    close_open_trade(closed, closed_trades, event);
    Ok(())
}

fn validate_script_protection(request: &ScriptTradeRequest, entry_price: f64) -> Result<()> {
    if request.position.is_close() {
        return Ok(());
    }
    match request.position.position_direction() {
        crate::domain::execution::PositionDirection::Long => {
            if request.sl.is_some_and(|price| price >= entry_price) {
                bail!("long ctx.trade sl must be below the entry price");
            }
            if request.tp.is_some_and(|price| price <= entry_price) {
                bail!("long ctx.trade tp must be above the entry price");
            }
        }
        crate::domain::execution::PositionDirection::Short => {
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
    config: &SourceConfig,
    data: &BacktestData,
    record_idx: usize,
    event_idx: usize,
    open_trades: &mut Vec<OpenTrade>,
    closed_trades: &mut Vec<ScriptBacktestTrade>,
) -> Result<()> {
    let series = data
        .series
        .get(&config.selector)
        .with_context(|| format!("{} data not loaded", config.selector))?;
    let ts_ms = backtest_series_event_ts_ms(series, record_idx, config)?;
    let mut open_index = 0;
    while open_index < open_trades.len() {
        let trigger = protective_trigger(
            config,
            data,
            record_idx,
            event_idx,
            &open_trades[open_index],
        )?;
        let Some((price, reason)) = trigger else {
            open_index += 1;
            continue;
        };
        let open = open_trades.remove(open_index);
        close_open_trade(
            open,
            closed_trades,
            &TradeEvent {
                idx: event_idx,
                ts_ms,
                price,
                reason,
            },
        );
    }
    Ok(())
}

fn protective_trigger(
    config: &SourceConfig,
    data: &BacktestData,
    record_idx: usize,
    event_idx: usize,
    open: &OpenTrade,
) -> Result<Option<(f64, String)>> {
    if event_idx <= open.entry_idx {
        return Ok(None);
    }
    let (low, high) = if config.source == ScriptSource::Candles {
        let candle = backtest_candle(config, data, record_idx)?;
        (candle.l, candle.h)
    } else {
        let series = data
            .series
            .get(&config.selector)
            .with_context(|| format!("{} data not loaded", config.selector))?;
        let price = backtest_series_reference_price(series, record_idx)?
            .context("protective order evaluation requires a price-bearing source event")?;
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
    let margin = entry
        .margin
        .unwrap_or_else(|| margin_for_notional(notional, entry.leverage));
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
        margin,
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
    let margin = open.margin;
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
        events_held: event.idx.saturating_sub(open.entry_idx),
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
    let margin = open_trades.iter().map(|open| open.margin).sum::<f64>();
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
            margin: open.margin,
            leverage: open.leverage,
            qty: open.qty,
            stop_loss_price: open.stop_loss_price,
            take_profit_price: open.take_profit_price,
            unrealized_pnl: trade_pnl(open.side, open.entry_price, mark_price, open.qty),
            events_held: mark_idx.saturating_sub(open.entry_idx),
            reason: "backtest ended before exit signal".to_string(),
        })
        .collect()
}

fn margin_for_notional(notional: f64, leverage: f64) -> f64 {
    notional / leverage.max(f64::EPSILON)
}

fn open_position_margin(open_trades: &[OpenTrade]) -> f64 {
    open_trades.iter().map(|open| open.margin).sum()
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
    peak_margin: f64,
) -> ScriptBacktestPerformance {
    let gross_pnl = trades.iter().map(|trade| trade.gross_pnl).sum::<f64>();
    let realized_pnl = trades.iter().map(|trade| trade.net_pnl).sum::<f64>();
    let unrealized_pnl = open_positions
        .iter()
        .map(|position| position.unrealized_pnl)
        .sum::<f64>();
    let total_pnl = realized_pnl + unrealized_pnl;
    let capital_required = peak_margin
        .max(
            trades
                .iter()
                .map(|trade| trade.margin)
                .chain(open_positions.iter().map(|position| position.margin))
                .fold(0.0_f64, f64::max),
        )
        .max(0.0);
    let return_basis = capital_required.max(1.0);
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
        capital_required,
        gross_pnl,
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        net_pnl: total_pnl,
        realized_return: realized_pnl / return_basis,
        total_return: total_pnl / return_basis,
        net_return: total_pnl / return_basis,
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
                "market: {}:{} [{}-{}]",
                result.exchange, result.symbol, result.window.from, result.window.to
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
                "  capital required: {}\n  realized pnl: {}\n  unrealized pnl: {}\n  total pnl: {}\n  total return: {}\n  gross pnl: {}\n  profit factor: {}\n  avg trade: {}\n  best trade: {}\n  worst trade: {}\n  sharpe: {}\n  max drawdown: {}",
                format_money(result.performance.capital_required),
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
                        "  {} pos={} {} entry={} exit={} notional={} margin={} pnl={} events={} reason={}",
                        trade.id,
                        trade.position_id,
                        format_side(trade.side),
                        format_price(trade.entry.price),
                        format_price(trade.exit.price),
                        format_money(trade.notional),
                        format_money(trade.margin),
                        format_money(trade.net_pnl),
                        trade.events_held,
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
                        "  {} {} entry={} mark={} notional={} margin={} unrealized={} events={}",
                        open.id,
                        format_side(open.side),
                        format_price(open.entry_price),
                        format_price(open.mark_price),
                        format_money(open.notional),
                        format_money(open.margin),
                        format_money(open.unrealized_pnl),
                        open.events_held
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

    fn candle_source() -> SourceConfig {
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

    fn script_order(value: Value) -> ScriptExecutionCommand {
        let request: ScriptRawOrderRequest =
            serde_json::from_value(value).expect("valid raw script order request");
        let order = ScriptOrderRef {
            id: crate::scripting::execution::local_order_id("backtest", &request.key),
            key: request.key.clone(),
        };
        ScriptExecutionCommand::Order { order, request }
    }

    #[test]
    fn event_payload_matches_live_source_metadata() {
        let configs = parse_source_configs(&[
            "candles@binancef@mmt:timeframe=60".to_string(),
            "candles@okx@mmt:timeframe=60".to_string(),
        ])
        .expect("parse source configs");
        let config = &configs["candles@binancef@mmt"];
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

        let series = &data.series["candles@binancef@mmt"];
        let payload = build_event_payload(EventPayloadContext {
            source_configs: &configs,
            symbol: "BTC/USDT",
            config,
            series,
            record_idx: 0,
            event_idx: 0,
            mark_ts_ms: backtest_series_event_ts_ms(series, 0, config).unwrap(),
            mark_price: backtest_series_reference_price(series, 0).unwrap().unwrap(),
            open_trades: &[],
        })
        .expect("build event payload");

        assert_eq!(payload["source"], "candles@binancef@mmt");
        assert_eq!(payload["source_type"], "candles");
        assert_eq!(payload["exchange"], "binancef");
        assert_eq!(payload["provider"], "mmt");
        assert_eq!(payload["data"]["candle"]["c"], 10.0);
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
    fn reference_source_skips_non_price_series() {
        let configs = parse_source_configs(&[
            "oi@binancef@mmt:timeframe=60".to_string(),
            "candles@binancef@mmt:timeframe=60".to_string(),
        ])
        .expect("parse source configs");
        let data = BacktestData {
            series: BTreeMap::from([
                (
                    "oi@binancef@mmt".to_string(),
                    BacktestSeries::Oi(vec![ScriptOpenInterest {
                        t: candle(0, 0.0, 0.0, 0.0, 0.0).t,
                        value: 1_000.0,
                        o: 1_000.0,
                        h: 1_000.0,
                        l: 1_000.0,
                        c: 1_000.0,
                        n: 1,
                        mark_price: None,
                        notional: None,
                    }]),
                ),
                (
                    "candles@binancef@mmt".to_string(),
                    BacktestSeries::Candles(vec![candle(0, 10.0, 10.0, 10.0, 10.0)]),
                ),
            ]),
        };

        assert_eq!(
            resolve_reference_source(&data, &configs)
                .expect("resolve reference source")
                .selector,
            "candles@binancef@mmt"
        );
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
      trigger: input.source,
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
        let events = build_event_timeline(&data, &configs).expect("build event timeline");
        assert_eq!(
            events
                .iter()
                .map(|event| event.selector.as_str())
                .collect::<Vec<_>>(),
            vec![
                "candles@binancef@mmt",
                "candles@okx@mmt",
                "candles@binancef@mmt",
                "candles@okx@mmt",
            ]
        );
        assert_eq!(events[0].ts_ms, candle(0, 0.0, 0.0, 0.0, 0.0).t + 60_000);
        let mut outputs = Vec::new();
        for (event_idx, event) in events.iter().enumerate() {
            let config = &configs[&event.selector];
            let series = &data.series[&event.selector];
            let payload = build_event_payload(EventPayloadContext {
                source_configs: &configs,
                symbol: "BTC/USDT",
                config,
                series,
                record_idx: event.record_idx,
                event_idx,
                mark_ts_ms: event.ts_ms,
                mark_price: backtest_series_reference_price(series, event.record_idx)
                    .unwrap()
                    .unwrap(),
                open_trades: &[],
            })
            .expect("build event payload");
            outputs.push(
                session
                    .run_event(payload)
                    .expect("run source event")
                    .output
                    .metrics,
            );
        }

        let first = &outputs[0];
        assert_eq!(first["binance"], json!([10]));
        assert_eq!(first["okx"], json!([]));
        assert!(first["previous"].is_null());
        assert_eq!(first["trigger"], "candles@binancef@mmt");

        let second = &outputs[1];
        assert_eq!(second["binance"], json!([10]));
        assert_eq!(second["okx"], json!([20]));
        assert_eq!(second["trigger"], "candles@okx@mmt");

        let last = outputs.last().unwrap();
        assert_eq!(last["binance"], json!([10, 11]));
        assert_eq!(last["okx"], json!([20, 21]));
        assert_eq!(last["current"], 11.0);
        assert_eq!(last["previous"], 10.0);
        assert_eq!(last["has_legacy_input"], false);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn script_limit_order_cannot_fill_on_its_submission_event() {
        let data = candle_data(vec![
            candle(0, 100.0, 105.0, 85.0, 100.0),
            candle(1, 100.0, 101.0, 89.0, 95.0),
        ]);
        let source = candle_source();
        let mut simulation = ScriptSimulationState::default();
        let submitted = apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "limit-1",
                "position": "open-long",
                "margin": 100,
                "leverage": 2,
                "order": { "type": "limit", "price": 90, "tif": "gtc" }
            }))],
            0,
            candle_ts_ms(&candle(0, 100.0, 105.0, 85.0, 100.0)),
            Some(100.0),
            &mut simulation,
        )
        .expect("queue limit order");
        assert_eq!(submitted, 1);

        fill_pending_script_orders(&source, &data, 0, 0, &mut simulation)
            .expect("same-event check");
        assert!(simulation.open_trades.is_empty());

        fill_pending_script_orders(&source, &data, 1, 1, &mut simulation)
            .expect("later-event fill");
        assert_eq!(simulation.open_trades.len(), 1);
        assert_eq!(simulation.open_trades[0].entry_price, 90.0);
        assert_eq!(simulation.open_trades[0].leverage, 2.0);
        assert_eq!(simulation.open_trades[0].notional, 200.0);
    }

    #[test]
    fn simulated_oco_uses_stop_when_both_triggers_touch_same_candle() {
        let data = candle_data(vec![
            candle(0, 100.0, 105.0, 95.0, 100.0),
            candle(1, 100.0, 125.0, 85.0, 105.0),
        ]);
        let source = candle_source();
        let mut simulation = ScriptSimulationState::default();
        apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "protected-1",
                "position": "open-long",
                "margin": 100,
                "leverage": 2,
                "order": { "type": "market" },
                "sl": 90,
                "tp": 120
            }))],
            0,
            candle_ts_ms(&candle(0, 100.0, 105.0, 95.0, 100.0)),
            Some(100.0),
            &mut simulation,
        )
        .expect("fill market order");

        apply_protective_triggers(
            &source,
            &data,
            1,
            1,
            &mut simulation.open_trades,
            &mut simulation.closed_trades,
        )
        .expect("apply protection");
        assert!(simulation.open_trades.is_empty());
        assert_eq!(simulation.closed_trades.len(), 1);
        assert_eq!(simulation.closed_trades[0].exit.price, 90.0);
        assert_eq!(
            simulation.closed_trades[0].exit.reason,
            "ctx.trade stop loss"
        );
    }

    #[test]
    fn close_long_is_reduce_only_and_defaults_to_the_full_position() {
        let mut simulation = ScriptSimulationState::default();

        apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "open-1",
                "position": "open-long",
                "margin": 100,
                "leverage": 2
            }))],
            0,
            1_000,
            Some(100.0),
            &mut simulation,
        )
        .expect("open long");
        assert_eq!(simulation.open_trades.len(), 1);

        apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "close-1",
                "position": "close-long"
            }))],
            1,
            2_000,
            Some(110.0),
            &mut simulation,
        )
        .expect("close long");

        assert!(simulation.open_trades.is_empty());
        assert_eq!(simulation.closed_trades.len(), 1);
        assert_eq!(simulation.closed_trades[0].qty, 2.0);
        assert_eq!(simulation.closed_trades[0].net_pnl, 20.0);
    }

    #[test]
    fn opposite_open_requires_an_explicit_close_first() {
        let mut simulation = ScriptSimulationState::default();

        apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "open-long-1",
                "position": "open-long",
                "size": 1
            }))],
            0,
            1_000,
            Some(100.0),
            &mut simulation,
        )
        .expect("open long");

        let error = apply_script_execution_commands(
            vec![script_trade(json!({
                "key": "open-short-1",
                "position": "open-short",
                "size": 1
            }))],
            1,
            2_000,
            Some(99.0),
            &mut simulation,
        )
        .expect_err("opposite open must fail");

        assert!(error.to_string().contains("submit close-long first"));
    }

    #[test]
    fn raw_order_reduces_and_flips_the_net_position() {
        let mut simulation = ScriptSimulationState::default();

        apply_script_execution_commands(
            vec![script_order(json!({
                "key": "raw-buy",
                "side": "long",
                "size": 10
            }))],
            0,
            1_000,
            Some(100.0),
            &mut simulation,
        )
        .expect("raw buy fills");
        assert_eq!(simulation.open_trades.len(), 1);
        assert_eq!(simulation.open_trades[0].side, TradeSide::Long);
        assert_eq!(simulation.open_trades[0].qty, 10.0);

        apply_script_execution_commands(
            vec![script_order(json!({
                "key": "raw-sell",
                "side": "short",
                "size": 14
            }))],
            1,
            2_000,
            Some(110.0),
            &mut simulation,
        )
        .expect("raw sell closes and flips");

        assert_eq!(simulation.closed_trades.len(), 1);
        assert_eq!(simulation.closed_trades[0].qty, 10.0);
        assert_eq!(simulation.closed_trades[0].net_pnl, 100.0);
        assert_eq!(simulation.open_trades.len(), 1);
        assert_eq!(simulation.open_trades[0].side, TradeSide::Short);
        assert_eq!(simulation.open_trades[0].qty, 4.0);
        assert_eq!(simulation.open_trades[0].entry_price, 110.0);
    }

    #[test]
    fn raw_reduce_only_order_closes_without_flipping() {
        let mut simulation = ScriptSimulationState::default();
        apply_script_execution_commands(
            vec![script_order(json!({
                "key": "raw-buy",
                "side": "buy",
                "size": 10
            }))],
            0,
            1_000,
            Some(100.0),
            &mut simulation,
        )
        .expect("raw buy fills");

        apply_script_execution_commands(
            vec![script_order(json!({
                "key": "reduce-sell",
                "side": "sell",
                "size": 14,
                "reduceOnly": true
            }))],
            1,
            2_000,
            Some(110.0),
            &mut simulation,
        )
        .expect("reduce-only sell fills only the open long");

        assert!(simulation.open_trades.is_empty());
        assert_eq!(simulation.closed_trades.len(), 1);
        assert_eq!(simulation.closed_trades[0].qty, 10.0);
    }

    #[test]
    fn backtest_delivers_simulated_fills_to_on_execution() {
        let path = std::env::temp_dir().join(format!(
            "marketlab-backtest-execution-{}-{}.js",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(
            &path,
            r#"
export const script = {
  name: "backtest-execution",
  version: "1",
  sources: ["candles"],
  params: {}
};

export function onData() {}

export function onExecution(ctx, event) {
  if (event.type === "order.filled" && event.key === "raw-buy") {
    ctx.order({ key: "raw-sell", side: "sell", size: 1 });
  }
  return { metrics: { last_event: event.type } };
}
"#,
        )
        .expect("write test script");
        let script = Script::load(&path).expect("load test script");
        let session = script
            .start_session_with_execution(
                &json!({}),
                ScriptExecutionContext {
                    job_id: "backtest".to_string(),
                    enabled: true,
                },
            )
            .expect("start test session");
        let mut simulation = ScriptSimulationState::default();
        apply_script_execution_commands(
            vec![script_order(json!({
                "key": "raw-buy",
                "side": "buy",
                "size": 1
            }))],
            0,
            1_000,
            Some(100.0),
            &mut simulation,
        )
        .expect("initial raw order fills");
        let mut report = crate::scripting::telemetry::ScriptRuntimeReportBuilder::start(
            "test",
            crate::scripting::telemetry::ScriptReportScript {
                name: "backtest-execution".to_string(),
                path: path.display().to_string(),
                source: "test".to_string(),
            },
            None,
            None,
            Some("BTC/USDT".to_string()),
        );
        let mut latest_output = None;

        let submitted = dispatch_simulated_execution_events(
            &session,
            0,
            1_000,
            Some(100.0),
            &mut simulation,
            &mut report,
            &mut latest_output,
        )
        .expect("dispatch simulated events");

        assert_eq!(submitted, 1);
        assert!(simulation.open_trades.is_empty());
        assert_eq!(simulation.closed_trades.len(), 1);
        assert_eq!(latest_output.unwrap().metrics["last_event"], "order.filled");
        let _ = std::fs::remove_file(path);
    }
}
