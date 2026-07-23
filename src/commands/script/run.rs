use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::cli::{ExecutionVenueArg, OutputFormat, ScriptRunArgs, mmt_timeframe_from_seconds};
use crate::commands::script::{
    ScriptDescriptor, ScriptInputs, report_builder, write_report_best_effort,
    write_running_report_best_effort,
};
use crate::commands::source::common::render_terminal;
use crate::core::orderbook::OrderBookState;
use crate::domain::enums::ProviderKind;
use crate::domain::execution::Position;
use crate::domain::types::{
    OiCandle, OpenInterestSnapshot, OrderBookSnapshot, TradeTick, VdCandle, VolumeDeltaTick,
    VolumeProfile,
};
use crate::providers::bulk::markets as bulk_markets;
use crate::providers::bulk::ws::{
    BulkCandleStream, BulkOrderBookStream, BulkTickerStream, BulkTradesStream,
};
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::markets as hyperliquid_markets;
use crate::providers::hyperliquid::ws::{
    HyperliquidAssetContextStream, HyperliquidCandleStream, HyperliquidOrderBookStream,
    HyperliquidTradesStream,
};
use crate::providers::mmt::utils::{normalize_symbol_for_mmt, normalize_to_ms, parse_levels};
use crate::providers::mmt::ws_client::MmtWsClient;
use crate::scripting::engine::Script;
use crate::scripting::execution::{ScriptExecutionCommand, ScriptExecutionContext};
use crate::scripting::inputs::{
    SourceConfig, SourceConfigs, parse_param_values, parse_source_configs, resolve_params,
    source_config, source_configs_payload, source_exchange_label, source_provider_label,
    source_provider_name, validate_source_configs_for_run,
};
use crate::scripting::jobs::ScriptJobSubmission;
use crate::scripting::manifest::ScriptSource;
use crate::scripting::market_data::{
    ScriptCandle, ScriptOpenInterest, ScriptVolume, ScriptVolumeDelta, TradeCandleAggregator,
};

const SCRIPT_STREAM_RECONNECT_MAX_SECS: u64 = 30;
const SCRIPT_STREAM_EVENT_CAPACITY: usize = 256;

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
    meta: Value,
}

#[derive(Debug, Clone)]
enum LiveRecord {
    Candles(ScriptCandle),
    Orderbook(OrderBookSnapshot),
    Vd(ScriptVolumeDelta),
    Oi(ScriptOpenInterest),
    Volumes(ScriptVolume),
}

#[derive(Debug, Clone)]
struct LiveUpdate {
    selector: String,
    source: ScriptSource,
    provider: ProviderKind,
    exchange: String,
    record: LiveRecord,
}

enum ScriptStreamEvent {
    Update(LiveUpdate),
    Disconnected { error: String, retry_seconds: u64 },
    Reconnected,
}

struct ScriptRunMarket {
    symbol: String,
}

struct ScriptWorkerState<'a> {
    job_id: &'a str,
    initial_event_cursor: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ScriptRunSummary {
    updates: u64,
    outputs: u64,
    hook_failures: u64,
    last_ts_ms: Option<u64>,
    latest_output: Option<ScriptRunOutput>,
}

pub async fn handle(args: ScriptRunArgs) -> Result<()> {
    args.validate()?;
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("scripts currently support only --output terminal|json|jsonl");
    }

    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical data"
        );
    }
    let script = Script::load(&args.script)?;
    let symbol = require_symbol(args.symbol.as_deref())?.to_string();
    let source_configs = parse_source_configs(&args.source)?;
    validate_source_configs_for_run(&script.manifest, &source_configs)?;
    let raw_params = parse_param_values(&args.param)?;
    resolve_params(&script.manifest, &raw_params)?;
    let providers = source_provider_label(&source_configs)
        .split(',')
        .map(str::to_string)
        .collect();
    let exchanges = source_exchange_label(&source_configs)
        .split(',')
        .map(str::to_string)
        .collect();

    let submission = ScriptJobSubmission {
        script_name: script.manifest.name.clone(),
        original_path: script.path.display().to_string(),
        source: script.source().to_string(),
        providers,
        exchanges,
        symbol,
        sources: args.source,
        params: args.param,
        venue: args.venue.map(Into::into),
        testnet: args.testnet,
        duration_seconds: args.duration,
        verbose: args.verbose,
    };
    let job = crate::runtime::submit_script_job(submission).await?;
    match args.output {
        OutputFormat::Terminal => {
            println!("script deployed");
            println!("  job:       {}", job.id);
            println!("  status:    starting");
            println!("  providers: {}", job.definition.providers.join(","));
            println!("  symbol:    {}", job.definition.symbol);
            println!(
                "  venue:     {}",
                job.definition.venue.map_or_else(
                    || "disabled".to_string(),
                    |venue| format!("{venue:?}").to_ascii_lowercase()
                )
            );
            println!(
                "  duration:  {}",
                job.definition
                    .duration_seconds
                    .map_or_else(|| "forever".to_string(), |seconds| format!("{seconds}s"))
            );
            println!("  logs:      mlab script logs {} --follow", job.id);
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&job)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&job)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub async fn handle_worker(job_id: &str) -> Result<()> {
    let job = crate::runtime::get_script_job_from_running_daemon(job_id).await?;
    let script = Script::load(&job.definition.snapshot_path)?;
    let venue = job.definition.venue.map(|venue| match venue {
        crate::domain::execution::ExecutionVenue::Bulk => ExecutionVenueArg::Bulk,
        crate::domain::execution::ExecutionVenue::Hyperliquid => ExecutionVenueArg::Hyperliquid,
    });
    let args = ScriptRunArgs {
        script: job.definition.snapshot_path.display().to_string(),
        config: None,
        symbol: Some(job.definition.symbol.clone()),
        venue,
        testnet: job.definition.testnet,
        from: None,
        to: None,
        source: job.definition.sources.clone(),
        param: job.definition.params.clone(),
        duration: job.definition.duration_seconds,
        output: OutputFormat::Jsonl,
        verbose: job.definition.verbose,
    };
    let mut report = report_builder(
        "script.worker",
        &script,
        Some(job.definition.providers.join(",")),
        Some(job.definition.exchanges.join(",")),
        Some(job.definition.symbol.clone()),
    );
    let pid = std::process::id();
    crate::runtime::script_worker_started(job_id, pid).await?;
    let result = if let Some(duration_seconds) = job.definition.duration_seconds {
        match tokio::time::timeout(
            Duration::from_secs(duration_seconds),
            run(args, script, &mut report, job_id, job.worker_event_cursor),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                report.set_phase("duration_elapsed");
                Ok(())
            }
        }
    } else {
        run(args, script, &mut report, job_id, job.worker_event_cursor).await
    };
    let error = result
        .as_ref()
        .err()
        .and_then(|error| (!error.is::<ScriptCancelled>()).then(|| format!("{error:#}")));
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(error) if error.is::<ScriptCancelled>() => report.finish_cancelled(),
        Err(error) => report.finish_error(error),
    };
    write_report_best_effort(&runtime_report);
    let _ = crate::runtime::script_worker_finished(job_id, pid, error).await;
    match result {
        Err(error) if error.is::<ScriptCancelled>() => Ok(()),
        result => result,
    }
}

async fn run(
    args: ScriptRunArgs,
    script: Script,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
    job_id: &str,
    initial_event_cursor: u64,
) -> Result<()> {
    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical data"
        );
    }
    let symbol = require_symbol(args.symbol.as_deref())?.to_string();

    let source_configs = parse_source_configs(&args.source)?;
    validate_source_configs_for_run(&script.manifest, &source_configs)?;
    let raw_params = parse_param_values(&args.param)?;
    let resolved_params = resolve_params(&script.manifest, &raw_params)?;

    let market = ScriptRunMarket { symbol };
    stream_sources(
        args,
        script,
        source_configs,
        resolved_params,
        market,
        report,
        ScriptWorkerState {
            job_id,
            initial_event_cursor,
        },
    )
    .await
}

async fn stream_sources(
    args: ScriptRunArgs,
    script: Script,
    source_configs: SourceConfigs,
    resolved_params: Value,
    market: ScriptRunMarket,
    report: &mut crate::scripting::telemetry::ScriptRuntimeReportBuilder,
    worker: ScriptWorkerState<'_>,
) -> Result<()> {
    let job_id = worker.job_id;
    report.set_phase("connecting_streams");
    write_running_report_best_effort(report);

    let streams = ScriptLiveStreams::connect(&source_configs, &market.symbol, args.testnet).await?;
    let mut stream_events = spawn_script_stream_supervisor(
        streams,
        source_configs.clone(),
        market.symbol.clone(),
        args.testnet,
    );

    let session = script.start_session_with_execution(
        &resolved_params,
        ScriptExecutionContext {
            job_id: job_id.to_string(),
            enabled: args.venue.is_some(),
        },
    )?;
    let cancel_handle = session.cancel_handle();
    let _cancel_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancel_handle.store(true, Ordering::Relaxed);
        }
    });
    let mut rendered = VecDeque::with_capacity(50);
    let mut hooks = 0_u64;
    let mut summary = ScriptRunSummary::default();
    let mut event_cursor = worker.initial_event_cursor;
    let mut positions = crate::runtime::script_positions(job_id).await?;
    let mut execution_events = tokio::time::interval(std::time::Duration::from_millis(250));
    execution_events.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install script worker termination handler")?;

    report.set_phase("streaming_sources");
    write_running_report_best_effort(report);

    loop {
        if session.is_cancelled() {
            report.set_phase("cancelled");
            render_run_summary(&summary, args.output, args.verbose)?;
            return Err(ScriptCancelled.into());
        }

        let update = tokio::select! {
            event = stream_events.recv() => {
                match event.context("script market-data supervisor stopped unexpectedly")? {
                    ScriptStreamEvent::Update(update) => update,
                    ScriptStreamEvent::Disconnected { error, retry_seconds } => {
                        let cleanup_error = if args.venue.is_some() {
                            crate::runtime::cancel_all_script_orders(job_id)
                                .await
                                .err()
                                .map(|error| format!("{error:#}"))
                        } else {
                            None
                        };
                        report.set_phase("reconnecting_streams");
                        write_running_report_best_effort(report);
                        crate::runtime::append_script_output(job_id, &json!({
                            "type": "script.source.disconnected",
                            "version": "1",
                            "ts_ms": now_ms(),
                            "error": error,
                            "retrySeconds": retry_seconds,
                            "orderCleanupError": cleanup_error,
                        }))?;
                        continue;
                    }
                    ScriptStreamEvent::Reconnected => {
                        report.set_phase("streaming_sources");
                        write_running_report_best_effort(report);
                        crate::runtime::append_script_output(job_id, &json!({
                            "type": "script.source.reconnected",
                            "version": "1",
                            "ts_ms": now_ms(),
                        }))?;
                        continue;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                report.set_phase("cancelled");
                render_run_summary(&summary, args.output, args.verbose)?;
                return Err(ScriptCancelled.into());
            }
            _ = terminate.recv() => {
                report.set_phase("cancelled");
                render_run_summary(&summary, args.output, args.verbose)?;
                return Err(ScriptCancelled.into());
            }
            _ = heartbeat.tick() => {
                crate::runtime::script_worker_heartbeat(job_id, std::process::id()).await?;
                continue;
            }
            _ = execution_events.tick() => {
                dispatch_execution_events(&session, job_id, &mut event_cursor).await?;
                positions = crate::runtime::script_positions(job_id).await?;
                continue;
            }
        };

        let ts_ms = update.ts_ms();
        let payload = live_stream_payload(&update, &source_configs, &market.symbol, &positions)?;
        let execution = match session.run_event(payload) {
            Ok(execution) => execution,
            Err(err) => {
                report.record_hook_failure();
                summary.hook_failures += 1;
                if session.is_cancelled() {
                    report.set_phase("cancelled");
                    render_run_summary(&summary, args.output, args.verbose)?;
                    return Err(ScriptCancelled.into());
                }
                return Err(err);
            }
        };
        dispatch_execution_commands(job_id, execution.commands).await?;
        hooks += 1;
        summary.record_update(ts_ms);
        report.record_hook(&execution.stats);
        report.set_progress("streaming_sources", hooks, hooks);
        write_running_report_best_effort(report);

        if execution.output.is_empty() {
            continue;
        }

        let result = ScriptRunResult {
            r#type: "script.run.result",
            version: "1",
            provider: source_provider_name(update.provider),
            exchange: update.exchange.clone(),
            symbol: market.symbol.clone(),
            ts_ms,
            stream: true,
            script: ScriptDescriptor {
                name: script.manifest.name.clone(),
                sources: script
                    .manifest
                    .sources
                    .iter()
                    .map(|source| source.as_str().to_string())
                    .collect(),
            },
            params: ScriptInputs {
                values: resolved_params.clone(),
            },
            output: ScriptRunOutput {
                metrics: execution.output.metrics,
                meta: execution.output.meta,
            },
        };
        summary.record_output(&result);
        crate::runtime::append_script_output(job_id, &result)?;
        render_stream_result(&result, args.output, args.verbose, &mut rendered)?;
    }
}

async fn dispatch_execution_commands(
    job_id: &str,
    commands: Vec<ScriptExecutionCommand>,
) -> Result<()> {
    for command in commands {
        let result = match command {
            ScriptExecutionCommand::Trade { order, request } => {
                crate::runtime::submit_script_trade(job_id, order, request)
                    .await
                    .map(|_| ())
            }
            ScriptExecutionCommand::Order { order, request } => {
                crate::runtime::submit_script_order(job_id, order, request)
                    .await
                    .map(|_| ())
            }
            ScriptExecutionCommand::Cancel { request } => {
                crate::runtime::submit_script_cancellation(job_id, request)
                    .await
                    .map(|_| ())
            }
        };
        if let Err(error) = result {
            crate::runtime::append_script_output(
                job_id,
                &serde_json::json!({
                    "type": "script.execution.error",
                    "version": "1",
                    "ts_ms": now_ms(),
                    "error": format!("{error:#}")
                }),
            )?;
        }
    }
    Ok(())
}

async fn dispatch_execution_events(
    session: &crate::scripting::engine::ScriptSession,
    job_id: &str,
    cursor: &mut u64,
) -> Result<()> {
    let events = crate::runtime::script_execution_events(job_id, *cursor, 100).await?;
    for event in events {
        let seq = event.seq;
        let event_value = serde_json::to_value(&event)?;
        let execution = session.run_execution_event(event_value.clone())?;
        let mut record = serde_json::json!({
            "type": "script.execution.event",
            "version": "1",
            "ts_ms": now_ms(),
            "event": event_value,
        });
        if let Some(execution) = execution {
            dispatch_execution_commands(job_id, execution.commands).await?;
            if !execution.output.is_empty() {
                record["output"] = serde_json::to_value(execution.output)?;
            }
        }
        crate::runtime::append_script_output(job_id, &record)?;
        crate::runtime::acknowledge_script_events(job_id, seq).await?;
        *cursor = (*cursor).max(seq);
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

struct ScriptLiveStreams {
    mmt: Option<MmtScriptStreams>,
    bulk: Option<Box<DirectScriptStreams>>,
    hyperliquid: Option<Box<DirectScriptStreams>>,
}

struct MmtScriptStreams {
    ws: MmtWsClient,
    source_configs: SourceConfigs,
    orderbook_states: BTreeMap<String, OrderBookState>,
    candle_aggregators: BTreeMap<String, TradeCandleAggregator>,
}

impl ScriptLiveStreams {
    async fn connect(source_configs: &SourceConfigs, symbol: &str, testnet: bool) -> Result<Self> {
        let mmt_configs = configs_for_provider(source_configs, ProviderKind::Mmt);
        let bulk_configs = configs_for_provider(source_configs, ProviderKind::Bulk);
        let hyperliquid_configs = configs_for_provider(source_configs, ProviderKind::Hyperliquid);
        let mmt = if mmt_configs.is_empty() {
            None
        } else {
            for config in mmt_configs.values() {
                normalize_symbol_for_mmt(&config.exchange, symbol)?;
            }
            let ws = MmtWsClient::connect().await?;
            subscribe_mmt_sources(&ws, &mmt_configs, symbol).await?;
            let orderbook_states = orderbook_states(&mmt_configs);
            let candle_aggregators = trade_candle_aggregators(&mmt_configs, now_ms())?;
            Some(MmtScriptStreams {
                ws,
                source_configs: mmt_configs,
                orderbook_states,
                candle_aggregators,
            })
        };
        let bulk = if bulk_configs.is_empty() {
            None
        } else {
            let symbol = bulk_markets::market(symbol)?.symbol.clone();
            Some(Box::new(
                DirectScriptStreams::connect(ProviderKind::Bulk, &bulk_configs, &symbol, false)
                    .await?,
            ))
        };
        let hyperliquid = if hyperliquid_configs.is_empty() {
            None
        } else {
            let symbol = hyperliquid_markets::market(symbol)?.symbol.clone();
            Some(Box::new(
                DirectScriptStreams::connect(
                    ProviderKind::Hyperliquid,
                    &hyperliquid_configs,
                    &symbol,
                    testnet,
                )
                .await?,
            ))
        };
        if mmt.is_none() && bulk.is_none() && hyperliquid.is_none() {
            bail!("script has no supported live source providers");
        }
        Ok(Self {
            mmt,
            bulk,
            hyperliquid,
        })
    }

    async fn next_update(&mut self) -> Result<Option<LiveUpdate>> {
        let has_mmt = self.mmt.is_some();
        let has_bulk = self.bulk.is_some();
        let has_hyperliquid = self.hyperliquid.is_some();
        tokio::select! {
            update = async { self.mmt.as_mut().expect("guarded MMT streams").next_update().await }, if has_mmt => update,
            update = async { self.bulk.as_mut().expect("guarded BULK streams").next_update().await }, if has_bulk => update.map(Some),
            update = async { self.hyperliquid.as_mut().expect("guarded Hyperliquid streams").next_update().await }, if has_hyperliquid => update.map(Some),
            else => bail!("script has no active live source streams"),
        }
    }

    fn carry_runtime_state_from(&mut self, previous: &Self) {
        if let (Some(current), Some(previous)) = (self.bulk.as_mut(), previous.bulk.as_ref()) {
            current.cumulative_delta = previous.cumulative_delta;
        }
        if let (Some(current), Some(previous)) =
            (self.hyperliquid.as_mut(), previous.hyperliquid.as_ref())
        {
            current.cumulative_delta = previous.cumulative_delta;
        }
    }
}

fn spawn_script_stream_supervisor(
    streams: ScriptLiveStreams,
    source_configs: SourceConfigs,
    symbol: String,
    testnet: bool,
) -> mpsc::Receiver<ScriptStreamEvent> {
    let (sender, receiver) = mpsc::channel(SCRIPT_STREAM_EVENT_CAPACITY);
    tokio::spawn(supervise_script_streams(
        streams,
        source_configs,
        symbol,
        testnet,
        sender,
    ));
    receiver
}

async fn supervise_script_streams(
    mut streams: ScriptLiveStreams,
    source_configs: SourceConfigs,
    symbol: String,
    testnet: bool,
    sender: mpsc::Sender<ScriptStreamEvent>,
) {
    let mut retry_seconds = 1_u64;
    loop {
        match streams.next_update().await {
            Ok(Some(update)) => {
                retry_seconds = 1;
                if sender
                    .send(ScriptStreamEvent::Update(update))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                if sender
                    .send(ScriptStreamEvent::Disconnected {
                        error: format!("{error:#}"),
                        retry_seconds,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                loop {
                    tokio::time::sleep(Duration::from_secs(retry_seconds)).await;
                    match ScriptLiveStreams::connect(&source_configs, &symbol, testnet).await {
                        Ok(mut reconnected) => {
                            reconnected.carry_runtime_state_from(&streams);
                            streams = reconnected;
                            retry_seconds = 1;
                            if sender.send(ScriptStreamEvent::Reconnected).await.is_err() {
                                return;
                            }
                            break;
                        }
                        Err(error) => {
                            retry_seconds = next_stream_reconnect_delay(retry_seconds);
                            if sender
                                .send(ScriptStreamEvent::Disconnected {
                                    error: format!("{error:#}"),
                                    retry_seconds,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

fn next_stream_reconnect_delay(current: u64) -> u64 {
    current
        .saturating_mul(2)
        .min(SCRIPT_STREAM_RECONNECT_MAX_SECS)
}

impl MmtScriptStreams {
    async fn next_update(&mut self) -> Result<Option<LiveUpdate>> {
        next_mmt_update(
            &self.ws,
            &self.source_configs,
            &mut self.orderbook_states,
            &mut self.candle_aggregators,
        )
        .await
    }
}

fn configs_for_provider(configs: &SourceConfigs, provider: ProviderKind) -> SourceConfigs {
    configs
        .iter()
        .filter(|(_, config)| config.provider == provider)
        .map(|(selector, config)| (selector.clone(), config.clone()))
        .collect()
}

fn trade_candle_aggregators(
    source_configs: &SourceConfigs,
    started_at_ms: u64,
) -> Result<BTreeMap<String, TradeCandleAggregator>> {
    source_configs
        .values()
        .filter(|config| config.source == ScriptSource::Candles)
        .map(|config| {
            let timeframe = config.require_timeframe(&config.source)?;
            Ok((
                config.selector.clone(),
                TradeCandleAggregator::new(timeframe, started_at_ms),
            ))
        })
        .collect()
}

enum DirectTradesStream {
    Bulk(BulkTradesStream),
    Hyperliquid(HyperliquidTradesStream),
}

impl DirectTradesStream {
    async fn connect(provider: ProviderKind, symbol: &str, testnet: bool) -> Result<Self> {
        match provider {
            ProviderKind::Bulk => Ok(Self::Bulk(BulkTradesStream::connect(symbol).await?)),
            ProviderKind::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidTradesStream::connect_on(
                    symbol,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ProviderKind::Mmt | ProviderKind::MarketLab => {
                bail!("provider does not use the direct trade stream")
            }
        }
    }

    async fn next_trades(&mut self) -> Result<Vec<TradeTick>> {
        match self {
            Self::Bulk(stream) => stream.next_trades().await,
            Self::Hyperliquid(stream) => stream.next_trades().await,
        }
    }
}

enum DirectOrderBookStream {
    Bulk(BulkOrderBookStream),
    Hyperliquid(HyperliquidOrderBookStream),
}

impl DirectOrderBookStream {
    async fn connect(
        provider: ProviderKind,
        symbol: &str,
        depth: u16,
        testnet: bool,
    ) -> Result<Self> {
        match provider {
            ProviderKind::Bulk => Ok(Self::Bulk(
                BulkOrderBookStream::connect(symbol, depth).await?,
            )),
            ProviderKind::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidOrderBookStream::connect_on(
                    symbol,
                    depth,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ProviderKind::Mmt | ProviderKind::MarketLab => {
                bail!("provider does not use the direct orderbook stream")
            }
        }
    }

    async fn next_snapshot(&mut self) -> Result<OrderBookSnapshot> {
        match self {
            Self::Bulk(stream) => stream.next_snapshot().await,
            Self::Hyperliquid(stream) => stream.next_snapshot().await,
        }
    }
}

enum DirectTickerStream {
    Bulk(BulkTickerStream),
    Hyperliquid(HyperliquidAssetContextStream),
}

impl DirectTickerStream {
    async fn connect(provider: ProviderKind, symbol: &str, testnet: bool) -> Result<Self> {
        match provider {
            ProviderKind::Bulk => Ok(Self::Bulk(BulkTickerStream::connect(symbol).await?)),
            ProviderKind::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidAssetContextStream::connect_on(
                    symbol,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ProviderKind::Mmt | ProviderKind::MarketLab => {
                bail!("provider does not use the direct ticker stream")
            }
        }
    }

    async fn next_ticker(&mut self) -> Result<crate::domain::types::MarketTicker> {
        match self {
            Self::Bulk(stream) => stream.next_ticker().await,
            Self::Hyperliquid(stream) => stream.next_ticker().await,
        }
    }
}

enum DirectCandleStream {
    Bulk(BulkCandleStream),
    Hyperliquid(HyperliquidCandleStream),
}

impl DirectCandleStream {
    async fn connect(
        provider: ProviderKind,
        symbol: &str,
        interval: &str,
        testnet: bool,
    ) -> Result<Self> {
        match provider {
            ProviderKind::Bulk => Ok(Self::Bulk(
                BulkCandleStream::connect(symbol, interval).await?,
            )),
            ProviderKind::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidCandleStream::connect_on(
                    symbol,
                    interval,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ProviderKind::Mmt | ProviderKind::MarketLab => {
                bail!("provider does not use the direct candle stream")
            }
        }
    }

    async fn next_candle(&mut self) -> Result<crate::domain::types::OhlcvCandle> {
        match self {
            Self::Bulk(stream) => stream.next_candle().await,
            Self::Hyperliquid(stream) => stream.next_candle().await,
        }
    }
}

fn direct_provider_name(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Bulk => "bulk",
        ProviderKind::Hyperliquid => "hyperliquid",
        ProviderKind::Mmt => "mmt",
        ProviderKind::MarketLab => "marketlab",
    }
}

fn direct_timeframe(provider: ProviderKind, seconds: u32) -> Result<&'static str> {
    match provider {
        ProviderKind::Bulk => crate::providers::bulk::market_data::timeframe_from_seconds(seconds),
        ProviderKind::Hyperliquid => {
            crate::providers::hyperliquid::market_data::timeframe_from_seconds(seconds)
        }
        ProviderKind::Mmt | ProviderKind::MarketLab => {
            bail!("provider does not use a direct timeframe")
        }
    }
}

struct DirectScriptStreams {
    provider: ProviderKind,
    source_configs: SourceConfigs,
    trades: Option<DirectTradesStream>,
    candle_aggregator: Option<TradeCandleAggregator>,
    orderbook: Option<DirectOrderBookStream>,
    oi: Option<DirectTickerStream>,
    volumes: Option<DirectCandleStream>,
    cumulative_delta: f64,
    pending: VecDeque<LiveUpdate>,
}

impl DirectScriptStreams {
    async fn connect(
        provider: ProviderKind,
        source_configs: &SourceConfigs,
        symbol: &str,
        testnet: bool,
    ) -> Result<Self> {
        let candle_timeframe = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Candles)
        {
            Some(
                source_config(source_configs, &ScriptSource::Candles)?
                    .require_timeframe(&ScriptSource::Candles)?,
            )
        } else {
            None
        };
        let trades = if candle_timeframe.is_some()
            || source_configs
                .values()
                .any(|config| config.source == ScriptSource::Vd)
        {
            Some(DirectTradesStream::connect(provider, symbol, testnet).await?)
        } else {
            None
        };
        let candle_aggregator =
            candle_timeframe.map(|timeframe| TradeCandleAggregator::new(timeframe, now_ms()));
        let orderbook = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Orderbook)
        {
            let depth = source_config(source_configs, &ScriptSource::Orderbook)?.depth_or_default();
            Some(DirectOrderBookStream::connect(provider, symbol, depth, testnet).await?)
        } else {
            None
        };
        let oi = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Oi)
        {
            Some(DirectTickerStream::connect(provider, symbol, testnet).await?)
        } else {
            None
        };
        let volumes = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Volumes)
        {
            let seconds = source_config(source_configs, &ScriptSource::Volumes)?
                .require_timeframe(&ScriptSource::Volumes)?;
            let interval = direct_timeframe(provider, seconds)?;
            Some(DirectCandleStream::connect(provider, symbol, interval, testnet).await?)
        } else {
            None
        };
        Ok(Self {
            provider,
            source_configs: source_configs.clone(),
            trades,
            candle_aggregator,
            orderbook,
            oi,
            volumes,
            cumulative_delta: 0.0,
            pending: VecDeque::new(),
        })
    }

    async fn next_update(&mut self) -> Result<LiveUpdate> {
        loop {
            if let Some(update) = self.pending.pop_front() {
                return Ok(update);
            }

            let has_trades = self.trades.is_some();
            let has_orderbook = self.orderbook.is_some();
            let has_oi = self.oi.is_some();
            let has_volumes = self.volumes.is_some();
            let trades = &mut self.trades;
            let candle_aggregator = &mut self.candle_aggregator;
            let orderbook = &mut self.orderbook;
            let oi = &mut self.oi;
            let volumes = &mut self.volumes;
            let cumulative_delta = &mut self.cumulative_delta;
            let pending = &mut self.pending;
            let candles_config = source_config(&self.source_configs, &ScriptSource::Candles)
                .ok()
                .cloned();
            let orderbook_config = source_config(&self.source_configs, &ScriptSource::Orderbook)
                .ok()
                .cloned();
            let vd_config = source_config(&self.source_configs, &ScriptSource::Vd)
                .ok()
                .cloned();
            let oi_config = source_config(&self.source_configs, &ScriptSource::Oi)
                .ok()
                .cloned();
            let volumes_config = source_config(&self.source_configs, &ScriptSource::Volumes)
                .ok()
                .cloned();

            tokio::select! {
                snapshot = async { orderbook.as_mut().expect("guarded orderbook stream").next_snapshot().await }, if has_orderbook => {
                    return Ok(LiveUpdate::new(orderbook_config.as_ref().expect("configured orderbook source"), LiveRecord::Orderbook(snapshot?)));
                }
                batch = async { trades.as_mut().expect("guarded trades stream").next_trades().await }, if has_trades => {
                    let batch = batch?;
                    if let (Some(aggregator), Some(config)) = (candle_aggregator.as_mut(), candles_config.as_ref()) {
                        pending.extend(
                            aggregator
                                .push_batch(&batch)
                                .into_iter()
                                .map(|candle| LiveUpdate::new(config, LiveRecord::Candles(candle))),
                        );
                    }
                    if let Some(config) = vd_config.as_ref()
                        && let Some(update) = direct_vd_update(self.provider, &batch, cumulative_delta)
                    {
                        pending.push_back(LiveUpdate::new(config, LiveRecord::Vd(update)));
                    }
                    if let Some(update) = pending.pop_front() {
                        return Ok(update);
                    }
                }
                ticker = async { oi.as_mut().expect("guarded ticker stream").next_ticker().await }, if has_oi => {
                    let ticker = ticker?;
                    return Ok(LiveUpdate::new(oi_config.as_ref().expect("configured oi source"), LiveRecord::Oi(ScriptOpenInterest::from_bulk(OpenInterestSnapshot {
                        exchange: ticker.exchange,
                        symbol: ticker.symbol,
                        timestamp_ms: ticker.timestamp_ms,
                        open_interest: ticker.open_interest,
                        mark_price: ticker.mark_price,
                        notional: ticker.open_interest * ticker.mark_price,
                    }))));
                }
                candle = async { volumes.as_mut().expect("guarded volume stream").next_candle().await }, if has_volumes => {
                    return Ok(LiveUpdate::new(volumes_config.as_ref().expect("configured volumes source"), LiveRecord::Volumes(ScriptVolume::from_bulk_candle(candle?))));
                }
                else => bail!("{} script has no live source streams", direct_provider_name(self.provider)),
            }
        }
    }
}

fn direct_vd_update(
    provider: ProviderKind,
    trades: &[crate::domain::types::TradeTick],
    cumulative_delta: &mut f64,
) -> Option<ScriptVolumeDelta> {
    if trades.is_empty() {
        return None;
    }
    let delta = trades
        .iter()
        .map(|trade| {
            if trade.taker_buy {
                trade.size
            } else {
                -trade.size
            }
        })
        .sum::<f64>();
    *cumulative_delta += delta;
    Some(ScriptVolumeDelta::from_bulk(VolumeDeltaTick {
        exchange: direct_provider_name(provider).to_string(),
        symbol: trades[0].symbol.clone(),
        timestamp_ms: trades
            .iter()
            .map(|trade| trade.timestamp_ms)
            .max()
            .unwrap_or_default(),
        delta,
        cumulative_delta: *cumulative_delta,
    }))
}

async fn subscribe_mmt_sources(
    ws: &MmtWsClient,
    source_configs: &SourceConfigs,
    symbol: &str,
) -> Result<()> {
    let mut configs = source_configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    for config in configs {
        let exchange = config.exchange.as_str();
        let provider_symbol = normalize_symbol_for_mmt(exchange, symbol)?;
        match &config.source {
            ScriptSource::Candles => {
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "trades",
                    "exchange": exchange,
                    "symbol": provider_symbol.as_str(),
                }))
                .await
                .with_context(|| format!("failed to subscribe {}", config.selector))?;
            }
            ScriptSource::Orderbook => {
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "depth",
                    "exchange": exchange,
                    "symbol": provider_symbol.as_str(),
                }))
                .await
                .with_context(|| format!("failed to subscribe {}", config.selector))?;
            }
            ScriptSource::Vd => {
                let tf = config
                    .require_timeframe(&config.source)
                    .and_then(mmt_timeframe_from_seconds)?;
                let bucket = config.require_bucket(&config.source)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "vd",
                    "exchange": exchange,
                    "symbol": provider_symbol.as_str(),
                    "tf": tf,
                    "bucket": bucket,
                }))
                .await
                .with_context(|| format!("failed to subscribe {}", config.selector))?;
            }
            ScriptSource::Oi => {
                let tf = config
                    .require_timeframe(&config.source)
                    .and_then(mmt_timeframe_from_seconds)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "oi",
                    "exchange": exchange,
                    "symbol": provider_symbol.as_str(),
                    "tf": tf,
                }))
                .await
                .with_context(|| format!("failed to subscribe {}", config.selector))?;
            }
            ScriptSource::Volumes => {
                let tf = config
                    .require_timeframe(&config.source)
                    .and_then(mmt_timeframe_from_seconds)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "volumes",
                    "exchange": exchange,
                    "symbol": provider_symbol.as_str(),
                    "tf": tf,
                }))
                .await
                .with_context(|| format!("failed to subscribe {}", config.selector))?;
            }
        }
    }
    Ok(())
}

fn orderbook_states(source_configs: &SourceConfigs) -> BTreeMap<String, OrderBookState> {
    source_configs
        .values()
        .filter(|config| config.source == ScriptSource::Orderbook)
        .map(|config| (config.selector.clone(), OrderBookState::default()))
        .collect()
}

async fn next_mmt_update(
    ws: &MmtWsClient,
    source_configs: &SourceConfigs,
    orderbook_states: &mut BTreeMap<String, OrderBookState>,
    candle_aggregators: &mut BTreeMap<String, TradeCandleAggregator>,
) -> Result<Option<LiveUpdate>> {
    let Some(value) = ws.next_json().await? else {
        bail!("websocket closed by server");
    };
    if value.is_null() {
        return Ok(None);
    }
    if value.get("type").and_then(Value::as_str) == Some("subscribed") {
        return Ok(None);
    }
    if value.get("type").and_then(Value::as_str) != Some("data") {
        return Ok(None);
    }

    let source = match value.get("channel").and_then(Value::as_str) {
        Some("trades") => ScriptSource::Candles,
        Some("depth") => ScriptSource::Orderbook,
        Some("vd") => ScriptSource::Vd,
        Some("oi") => ScriptSource::Oi,
        Some("volumes") => ScriptSource::Volumes,
        _ => return Ok(None),
    };
    let config = mmt_update_config(&value, source_configs, &source)?;
    let Some(config) = config else {
        return Ok(None);
    };

    match source {
        ScriptSource::Vd => {
            let payload = value.get("data").context("vd payload missing data")?;
            let candle: VdCandle =
                serde_json::from_value(payload.clone()).context("invalid vd candle shape")?;
            Ok(Some(LiveUpdate::new(
                config,
                LiveRecord::Vd(ScriptVolumeDelta::from_mmt(candle)),
            )))
        }
        ScriptSource::Oi => {
            let payload = value.get("data").context("oi payload missing data")?;
            let candle: OiCandle =
                serde_json::from_value(payload.clone()).context("invalid oi candle shape")?;
            Ok(Some(LiveUpdate::new(
                config,
                LiveRecord::Oi(ScriptOpenInterest::from_mmt(candle)),
            )))
        }
        ScriptSource::Volumes => {
            let payload = value.get("data").context("volumes payload missing data")?;
            let profile: VolumeProfile =
                serde_json::from_value(payload.clone()).context("invalid volumes profile shape")?;
            Ok(Some(LiveUpdate::new(
                config,
                LiveRecord::Volumes(ScriptVolume::from_mmt(profile)),
            )))
        }
        ScriptSource::Orderbook => {
            let Some(state) = orderbook_states.get_mut(&config.selector) else {
                return Ok(None);
            };
            let depth = config.depth_or_default();
            Ok(parse_depth_update(value, state, depth)?
                .map(|snapshot| LiveUpdate::new(config, LiveRecord::Orderbook(snapshot))))
        }
        ScriptSource::Candles => {
            let payload = value.get("data").context("trade payload missing data")?;
            let trade: MmtTrade =
                serde_json::from_value(payload.clone()).context("invalid MMT trade shape")?;
            let trade = TradeTick {
                exchange: config.exchange.clone(),
                symbol: value
                    .get("symbol")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                timestamp_ms: normalize_to_ms(trade.t),
                price: trade.p,
                size: trade.q,
                taker_buy: trade.b,
            };
            let aggregator = candle_aggregators
                .get_mut(&config.selector)
                .context("missing MMT candle aggregator")?;
            Ok(aggregator
                .push(&trade)
                .map(|candle| LiveUpdate::new(config, LiveRecord::Candles(candle))))
        }
    }
}

#[derive(Debug, Deserialize)]
struct MmtTrade {
    t: u64,
    p: f64,
    q: f64,
    b: bool,
}

fn mmt_update_config<'a>(
    value: &Value,
    source_configs: &'a SourceConfigs,
    source: &ScriptSource,
) -> Result<Option<&'a SourceConfig>> {
    let exchange = value
        .get("exchange")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let mut matching = source_configs
        .values()
        .filter(|config| &config.source == source)
        .filter(|config| {
            exchange
                .as_ref()
                .is_none_or(|value| &config.exchange == value)
        });
    let config = matching.next();
    if config.is_some() && matching.next().is_some() {
        bail!(
            "MMT {} update did not identify a unique exchange",
            source.as_str()
        );
    }
    Ok(config)
}

fn parse_depth_update(
    value: Value,
    state: &mut OrderBookState,
    depth: u16,
) -> Result<Option<OrderBookSnapshot>> {
    let exchange = value
        .get("exchange")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let payload = value.get("data").unwrap_or(&value);
    let ts_ms = payload
        .get("t")
        .and_then(Value::as_u64)
        .map(normalize_to_ms)
        .context("depth payload missing t timestamp")?;
    let seq = payload.get("seq").and_then(Value::as_u64);
    let is_snapshot = payload
        .get("snapshot")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let bids = parse_levels(payload.get("b").or_else(|| payload.get("bids")))?;
    let asks = parse_levels(payload.get("a").or_else(|| payload.get("asks")))?;

    if is_snapshot {
        state.apply_snapshot(exchange, symbol, ts_ms, bids, asks, seq);
    } else {
        state.apply_delta(ts_ms, bids, asks, seq);
    }

    Ok(state.snapshot(depth))
}

impl LiveUpdate {
    fn new(config: &SourceConfig, record: LiveRecord) -> Self {
        Self {
            selector: config.selector.clone(),
            source: config.source.clone(),
            provider: config.provider,
            exchange: config.exchange.clone(),
            record,
        }
    }

    fn ts_ms(&self) -> u64 {
        match &self.record {
            LiveRecord::Candles(candle) => candle.t,
            LiveRecord::Orderbook(snapshot) => snapshot.timestamp_ms,
            LiveRecord::Vd(candle) => candle.t,
            LiveRecord::Oi(candle) => candle.t,
            LiveRecord::Volumes(profile) => profile.t,
        }
    }
}

impl ScriptRunSummary {
    fn record_update(&mut self, ts_ms: u64) {
        self.updates += 1;
        self.last_ts_ms = Some(ts_ms);
    }

    fn record_output(&mut self, result: &ScriptRunResult<ScriptInputs>) {
        self.outputs += 1;
        self.latest_output = Some(result.output.clone());
    }
}

fn render_run_summary(
    summary: &ScriptRunSummary,
    output: OutputFormat,
    verbose: bool,
) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!();
            println!("script run summary");
            println!("------------------");
            println!("status: cancelled");
            println!("updates: {}", summary.updates);
            println!("outputs: {}", summary.outputs);
            println!("hook failures: {}", summary.hook_failures);
            if let Some(ts_ms) = summary.last_ts_ms {
                println!("last ts: {ts_ms}");
            }
            if verbose && let Some(output) = &summary.latest_output {
                println!("latest_output: {}", serde_json::to_string_pretty(output)?);
            }
        }
        OutputFormat::Json | OutputFormat::Jsonl => {
            let value = json!({
                "type": "script.run.summary",
                "version": "1",
                "status": "cancelled",
                "summary": summary
            });
            if matches!(output, OutputFormat::Json) {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!("{}", serde_json::to_string(&value)?);
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn live_stream_payload(
    update: &LiveUpdate,
    source_configs: &SourceConfigs,
    symbol: &str,
    positions: &[Position],
) -> Result<Value> {
    let mut payload = serde_json::Map::new();
    payload.insert("source".to_string(), Value::String(update.selector.clone()));
    payload.insert(
        "source_type".to_string(),
        Value::String(update.source.as_str().to_string()),
    );
    payload.insert(
        "provider".to_string(),
        Value::String(source_provider_name(update.provider).to_string()),
    );
    payload.insert(
        "exchange".to_string(),
        Value::String(update.exchange.clone()),
    );
    payload.insert("symbol".to_string(), Value::String(symbol.to_string()));

    let current_config = source_configs
        .get(&update.selector)
        .context("missing current source config")?;
    payload.insert(
        "data".to_string(),
        live_record_payload(&update.record, current_config),
    );
    payload.insert(
        "source_configs".to_string(),
        source_configs_payload(source_configs),
    );
    payload.insert(
        "positions".to_string(),
        json!({
            "open": positions.iter().map(live_position_payload).collect::<Vec<_>>()
        }),
    );

    Ok(Value::Object(payload))
}

fn live_position_payload(position: &Position) -> Value {
    let margin = position.notional / position.leverage.max(f64::EPSILON);
    json!({
        "id": format!("{}:{:?}", position.venue_symbol, position.direction).to_lowercase(),
        "side": position.direction,
        "entry_price": position.entry_price,
        "mark_price": position.mark_price,
        "notional": position.notional,
        "margin": margin,
        "leverage": position.leverage,
        "qty": position.size,
        "realized_pnl": position.realized_pnl,
        "unrealized_pnl": position.unrealized_pnl,
        "liquidation_price": position.liquidation_price,
        "fees": position.fees,
        "funding": position.funding,
    })
}

fn live_record_payload(record: &LiveRecord, config: &SourceConfig) -> Value {
    match record {
        LiveRecord::Candles(candle) => json!({ "candle": candle }),
        LiveRecord::Orderbook(snapshot) => json!({ "snapshot": snapshot }),
        LiveRecord::Vd(candle) => json!({
            "candle": candle,
            "record": candle,
            "bucket": config.bucket,
            "timeframe_sec": config.timeframe,
        }),
        LiveRecord::Oi(candle) => json!({
            "candle": candle,
            "record": candle,
            "timeframe_sec": config.timeframe,
        }),
        LiveRecord::Volumes(profile) => json!({
            "profile": profile,
            "record": profile,
            "timeframe_sec": config.timeframe,
        }),
    }
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
            let line = format!(
                "ts={} script={} metrics={} meta={}",
                result.ts_ms,
                result.script.name,
                serde_json::to_string(&result.output.metrics)?,
                serde_json::to_string(&result.output.meta)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(close: f64) -> ScriptCandle {
        ScriptCandle {
            t: 1_780_000_000_000,
            o: close,
            h: close,
            l: close,
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

    fn cross_exchange_configs() -> SourceConfigs {
        parse_source_configs(&[
            "candles@binancef@mmt:timeframe=60".to_string(),
            "candles@okx@mmt:timeframe=60".to_string(),
        ])
        .expect("parse source configs")
    }

    #[test]
    fn mmt_updates_route_to_the_exchange_qualified_selector() {
        let configs = cross_exchange_configs();
        let update = json!({
            "type": "data",
            "channel": "trades",
            "exchange": "OKX",
            "data": {}
        });

        let config = mmt_update_config(&update, &configs, &ScriptSource::Candles)
            .expect("route update")
            .expect("matching source config");

        assert_eq!(config.selector, "candles@okx@mmt");
    }

    #[test]
    fn parses_mmt_trade_shape_used_for_live_candles() {
        let trade: MmtTrade = serde_json::from_value(json!({
            "id": "3065401760",
            "t": 1_704_067_200_123_u64,
            "p": 42_050.0,
            "q": 0.5,
            "b": true
        }))
        .expect("MMT trade should parse");

        assert_eq!(trade.t, 1_704_067_200_123);
        assert_eq!(trade.p, 42_050.0);
        assert_eq!(trade.q, 0.5);
        assert!(trade.b);
    }

    #[test]
    fn live_payload_contains_current_internal_record_and_source_metadata() {
        let configs = cross_exchange_configs();
        let okx = LiveUpdate::new(
            &configs["candles@okx@mmt"],
            LiveRecord::Candles(candle(20.0)),
        );

        let payload =
            live_stream_payload(&okx, &configs, "BTC/USDT", &[]).expect("build live payload");

        assert_eq!(payload["source"], "candles@okx@mmt");
        assert_eq!(payload["source_type"], "candles");
        assert_eq!(payload["exchange"], "okx");
        assert_eq!(payload["data"]["candle"]["c"], 20.0);
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
    fn script_stream_reconnect_delay_is_bounded() {
        assert_eq!(next_stream_reconnect_delay(1), 2);
        assert_eq!(next_stream_reconnect_delay(16), 30);
        assert_eq!(next_stream_reconnect_delay(30), 30);
        assert_eq!(next_stream_reconnect_delay(u64::MAX), 30);
    }
}
