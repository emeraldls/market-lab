use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::{
    CliProviderKind, ExecutionVenueArg, OutputFormat, ScriptRunArgs, mmt_timeframe_from_seconds,
};
use crate::commands::script::{
    ScriptDescriptor, ScriptInputs, report_builder, write_report_best_effort,
    write_running_report_best_effort,
};
use crate::commands::source::common::render_terminal;
use crate::commands::study::common::provider_name;
use crate::core::orderbook::OrderBookState;
use crate::domain::enums::ProviderKind;
use crate::domain::types::{
    OhlcvtCandle, OiCandle, OpenInterestSnapshot, OrderBookSnapshot, VdCandle, VolumeDeltaTick,
    VolumeProfile,
};
use crate::providers::bulk::catalog;
use crate::providers::bulk::ws::{
    BulkCandleStream, BulkOrderBookStream, BulkTickerStream, BulkTradesStream,
};
use crate::providers::mmt::utils::{normalize_symbol_for_mmt, normalize_to_ms, parse_levels};
use crate::providers::mmt::ws_client::MmtWsClient;
use crate::scripting::engine::Script;
use crate::scripting::execution::{ScriptExecutionCommand, ScriptExecutionContext};
use crate::scripting::inputs::{
    SourceConfig, SourceConfigs, parse_param_values, parse_source_configs,
    populate_source_defaults, resolve_params, source_config, source_exchange_label,
    validate_bulk_source_configs, validate_source_configs_for_run,
};
use crate::scripting::jobs::ScriptJobSubmission;
use crate::scripting::manifest::ScriptSource;
use crate::scripting::market_data::{
    ScriptCandle, ScriptOpenInterest, ScriptVolume, ScriptVolumeDelta,
};

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

#[derive(Debug, Clone, Default)]
struct LiveState {
    records: BTreeMap<String, LiveRecord>,
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
    exchange: String,
    record: LiveRecord,
}

struct ScriptRunMarket {
    provider: ProviderKind,
    symbol: String,
}

struct ScriptWorkerState<'a> {
    job_id: &'a str,
    initial_event_cursor: u64,
}

pub(super) fn default_script_exchange(
    provider: ProviderKind,
    exchange: Option<&str>,
) -> Result<Option<String>> {
    match provider {
        ProviderKind::Mmt => exchange
            .map(|exchange| {
                let exchange = exchange.trim().to_ascii_lowercase();
                if exchange.is_empty() {
                    bail!("--exchange cannot be empty");
                }
                Ok(exchange)
            })
            .transpose(),
        ProviderKind::Bulk => {
            if exchange.is_some_and(|exchange| !exchange.eq_ignore_ascii_case("bulk")) {
                bail!("--exchange must be omitted or set to `bulk` with --provider bulk");
            }
            Ok(Some("bulk".to_string()))
        }
        ProviderKind::MarketLab => bail!("script run supports --provider mmt|bulk"),
    }
}

fn uses_unqualified_source(sources: &[String]) -> bool {
    sources.iter().any(|value| {
        value
            .split_once(':')
            .map(|(selector, _)| !selector.contains('@'))
            .unwrap_or(true)
    })
}

#[derive(Debug, Clone, Default, Serialize)]
struct ScriptRunSummary {
    updates: u64,
    signals: u64,
    intents: u64,
    hook_failures: u64,
    last_ts_ms: Option<u64>,
    latest_output: Option<ScriptRunOutput>,
}

pub async fn handle(args: ScriptRunArgs) -> Result<()> {
    args.validate()?;
    if matches!(args.provider.into(), ProviderKind::MarketLab) {
        bail!("script run supports --provider mmt|bulk");
    }
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("scripts currently support only --output terminal|json|jsonl");
    }

    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical data"
        );
    }
    let script = Script::load(&args.script)?;
    let provider: ProviderKind = args.provider.into();
    let default_exchange = default_script_exchange(provider, args.exchange.as_deref())?;
    let symbol = require_symbol(args.symbol.as_deref())?;
    let symbol = match provider {
        ProviderKind::Bulk => catalog::market(symbol)?.internal_symbol.clone(),
        ProviderKind::Mmt => symbol.to_string(),
        ProviderKind::MarketLab => unreachable!(),
    };
    let mut source_configs = parse_source_configs(&args.source, default_exchange.as_deref())?;
    match provider {
        ProviderKind::Mmt => validate_source_configs_for_run(&script.manifest, &source_configs)?,
        ProviderKind::Bulk => {
            populate_source_defaults(&script.manifest, &mut source_configs, "bulk");
            validate_bulk_source_configs(&script.manifest, &source_configs, false)?;
        }
        ProviderKind::MarketLab => unreachable!(),
    };
    let raw_params = parse_param_values(&args.param)?;
    resolve_params(&script.manifest, &raw_params)?;
    let exchange = if uses_unqualified_source(&args.source) {
        default_exchange.context("unqualified script sources require --exchange")?
    } else {
        source_exchange_label(&source_configs)
    };

    let submission = ScriptJobSubmission {
        script_name: script.manifest.name.clone(),
        original_path: script.path.display().to_string(),
        source: script.source().to_string(),
        provider,
        exchange,
        symbol,
        sources: args.source,
        params: args.param,
        venue: args.venue.map(Into::into),
        verbose: args.verbose,
    };
    let job = crate::runtime::submit_script_job(submission).await?;
    match args.output {
        OutputFormat::Terminal => {
            println!("script deployed");
            println!("  job:       {}", job.id);
            println!("  status:    starting");
            println!("  provider:  {}", provider_name(job.definition.provider));
            println!("  symbol:    {}", job.definition.symbol);
            println!(
                "  venue:     {}",
                job.definition.venue.map_or_else(
                    || "disabled".to_string(),
                    |venue| format!("{venue:?}").to_ascii_lowercase()
                )
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
    let provider = match job.definition.provider {
        ProviderKind::Mmt => CliProviderKind::Mmt,
        ProviderKind::Bulk => CliProviderKind::Bulk,
        ProviderKind::MarketLab => bail!("script worker does not support marketlab provider"),
    };
    let venue = job.definition.venue.map(|venue| match venue {
        crate::domain::execution::ExecutionVenue::Bulk => ExecutionVenueArg::Bulk,
    });
    let args = ScriptRunArgs {
        script: job.definition.snapshot_path.display().to_string(),
        config: None,
        provider,
        exchange: (job.definition.provider == ProviderKind::Mmt
            && uses_unqualified_source(&job.definition.sources))
        .then(|| job.definition.exchange.clone()),
        symbol: Some(job.definition.symbol.clone()),
        venue,
        from: None,
        to: None,
        source: job.definition.sources.clone(),
        param: job.definition.params.clone(),
        output: OutputFormat::Jsonl,
        verbose: job.definition.verbose,
    };
    let mut report = report_builder(
        "script.worker",
        &script,
        Some(provider_name(job.definition.provider).to_string()),
        Some(job.definition.exchange.clone()),
        Some(job.definition.symbol.clone()),
    );
    let pid = std::process::id();
    crate::runtime::script_worker_started(job_id, pid).await?;
    let result = run(args, script, &mut report, job_id, job.worker_event_cursor).await;
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
    let provider: ProviderKind = args.provider.into();
    let default_exchange = default_script_exchange(provider, args.exchange.as_deref())?;
    let symbol = require_symbol(args.symbol.as_deref())?;
    let symbol = match provider {
        ProviderKind::Bulk => catalog::market(symbol)?.internal_symbol.clone(),
        ProviderKind::Mmt => symbol.to_string(),
        ProviderKind::MarketLab => unreachable!(),
    };

    let mut source_configs = parse_source_configs(&args.source, default_exchange.as_deref())?;
    match provider {
        ProviderKind::Mmt => validate_source_configs_for_run(&script.manifest, &source_configs)?,
        ProviderKind::Bulk => {
            populate_source_defaults(&script.manifest, &mut source_configs, "bulk");
            validate_bulk_source_configs(&script.manifest, &source_configs, false)?;
        }
        ProviderKind::MarketLab => unreachable!(),
    }
    let raw_params = parse_param_values(&args.param)?;
    let resolved_params = resolve_params(&script.manifest, &raw_params)?;

    let market = ScriptRunMarket { provider, symbol };
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

    let mut streams =
        ScriptLiveStreams::connect(market.provider, &source_configs, &market.symbol).await?;

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
    let mut live_state = LiveState::default();
    let mut event_cursor = worker.initial_event_cursor;
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
            update = streams.next_update() => {
                let Some(update) = update? else { continue; };
                update
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
                continue;
            }
        };

        let ts_ms = update.ts_ms();
        live_state.apply(&update);
        let payload = live_stream_payload(
            &live_state,
            &update,
            &source_configs,
            provider_name(market.provider),
            &market.symbol,
        )?;
        let execution = match session.run_stream(payload) {
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
        report.record_hook(&execution.stats);
        report.set_progress("streaming_sources", hooks, hooks);
        write_running_report_best_effort(report);

        let result = ScriptRunResult {
            r#type: "script.run.result",
            version: "1",
            provider: provider_name(market.provider),
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
        summary.record(&result);
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
        if let Some(execution) = session.run_execution_event(serde_json::to_value(&event)?)? {
            dispatch_execution_commands(job_id, execution.commands).await?;
            crate::runtime::append_script_output(
                job_id,
                &serde_json::json!({
                    "type": "script.execution.result",
                    "version": "1",
                    "ts_ms": now_ms(),
                    "event": event,
                    "output": execution.output,
                }),
            )?;
        }
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

enum ScriptLiveStreams {
    Mmt {
        ws: MmtWsClient,
        source_configs: SourceConfigs,
        orderbook_states: BTreeMap<String, OrderBookState>,
    },
    Bulk(Box<BulkScriptStreams>),
}

impl ScriptLiveStreams {
    async fn connect(
        provider: ProviderKind,
        source_configs: &SourceConfigs,
        symbol: &str,
    ) -> Result<Self> {
        match provider {
            ProviderKind::Mmt => {
                let ws = MmtWsClient::shared().await?;
                subscribe_mmt_sources(&ws, source_configs, symbol).await?;
                Ok(Self::Mmt {
                    ws,
                    source_configs: source_configs.clone(),
                    orderbook_states: orderbook_states(source_configs),
                })
            }
            ProviderKind::Bulk => Ok(Self::Bulk(Box::new(
                BulkScriptStreams::connect(source_configs, symbol).await?,
            ))),
            ProviderKind::MarketLab => bail!("script run supports --provider mmt|bulk"),
        }
    }

    async fn next_update(&mut self) -> Result<Option<LiveUpdate>> {
        match self {
            Self::Mmt {
                ws,
                source_configs,
                orderbook_states,
            } => next_mmt_update(ws, source_configs, orderbook_states).await,
            Self::Bulk(streams) => streams.next_update().await.map(Some),
        }
    }
}

struct BulkScriptStreams {
    source_configs: SourceConfigs,
    candles: Option<BulkCandleStream>,
    orderbook: Option<BulkOrderBookStream>,
    vd: Option<BulkTradesStream>,
    oi: Option<BulkTickerStream>,
    volumes: Option<BulkCandleStream>,
    cumulative_delta: f64,
}

impl BulkScriptStreams {
    async fn connect(source_configs: &SourceConfigs, symbol: &str) -> Result<Self> {
        let candles = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Candles)
        {
            let seconds = source_config(source_configs, &ScriptSource::Candles)?
                .require_timeframe(&ScriptSource::Candles)?;
            let interval = crate::providers::bulk::market_data::timeframe_from_seconds(seconds)?;
            Some(BulkCandleStream::connect(symbol, interval).await?)
        } else {
            None
        };
        let orderbook = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Orderbook)
        {
            let depth = source_config(source_configs, &ScriptSource::Orderbook)?.depth_or_default();
            let state_cap = (depth as usize).saturating_mul(10).clamp(100, 10_000);
            Some(BulkOrderBookStream::connect(symbol, depth, state_cap).await?)
        } else {
            None
        };
        let vd = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Vd)
        {
            Some(BulkTradesStream::connect(symbol).await?)
        } else {
            None
        };
        let oi = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Oi)
        {
            Some(BulkTickerStream::connect(symbol).await?)
        } else {
            None
        };
        let volumes = if source_configs
            .values()
            .any(|config| config.source == ScriptSource::Volumes)
        {
            let seconds = source_config(source_configs, &ScriptSource::Volumes)?
                .require_timeframe(&ScriptSource::Volumes)?;
            let interval = crate::providers::bulk::market_data::timeframe_from_seconds(seconds)?;
            Some(BulkCandleStream::connect(symbol, interval).await?)
        } else {
            None
        };
        Ok(Self {
            source_configs: source_configs.clone(),
            candles,
            orderbook,
            vd,
            oi,
            volumes,
            cumulative_delta: 0.0,
        })
    }

    async fn next_update(&mut self) -> Result<LiveUpdate> {
        loop {
            let has_candles = self.candles.is_some();
            let has_orderbook = self.orderbook.is_some();
            let has_vd = self.vd.is_some();
            let has_oi = self.oi.is_some();
            let has_volumes = self.volumes.is_some();
            let candles = &mut self.candles;
            let orderbook = &mut self.orderbook;
            let vd = &mut self.vd;
            let oi = &mut self.oi;
            let volumes = &mut self.volumes;
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
                candle = async { candles.as_mut().expect("guarded candle stream").next_candle().await }, if has_candles => {
                    return Ok(LiveUpdate::new(candles_config.as_ref().expect("configured candle source"), LiveRecord::Candles(ScriptCandle::from_bulk(candle?))));
                }
                snapshot = async { orderbook.as_mut().expect("guarded orderbook stream").next_snapshot().await }, if has_orderbook => {
                    return Ok(LiveUpdate::new(orderbook_config.as_ref().expect("configured orderbook source"), LiveRecord::Orderbook(snapshot?)));
                }
                trades = async { vd.as_mut().expect("guarded trades stream").next_trades().await }, if has_vd => {
                    let trades = trades?;
                    if let Some(update) = bulk_vd_update(&trades, &mut self.cumulative_delta) {
                        return Ok(LiveUpdate::new(vd_config.as_ref().expect("configured vd source"), LiveRecord::Vd(update)));
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
                else => bail!("BULK script has no live source streams"),
            }
        }
    }
}

fn bulk_vd_update(
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
        exchange: "bulk".to_string(),
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
    let symbol = normalize_symbol_for_mmt(symbol)?;
    let mut configs = source_configs.values().collect::<Vec<_>>();
    configs.sort_by_key(|config| config.position);
    for config in configs {
        let exchange = config.exchange.as_str();
        match &config.source {
            ScriptSource::Candles => {
                let tf = config
                    .require_timeframe(&config.source)
                    .and_then(mmt_timeframe_from_seconds)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "candles",
                    "exchange": exchange,
                    "symbol": symbol.as_str(),
                    "tf": tf,
                }))
                .await
                .with_context(|| format!("failed to subscribe {}", config.selector))?;
            }
            ScriptSource::Orderbook => {
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "depth",
                    "exchange": exchange,
                    "symbol": symbol.as_str(),
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
                    "symbol": symbol.as_str(),
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
                    "symbol": symbol.as_str(),
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
                    "symbol": symbol.as_str(),
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
        .map(|config| {
            let state_cap = (config.depth_or_default() as usize)
                .saturating_mul(10)
                .clamp(100, 10_000);
            (
                config.selector.clone(),
                OrderBookState::with_max_levels_per_side(state_cap),
            )
        })
        .collect()
}

async fn next_mmt_update(
    ws: &MmtWsClient,
    source_configs: &SourceConfigs,
    orderbook_states: &mut BTreeMap<String, OrderBookState>,
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
        Some("candles") => ScriptSource::Candles,
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
            let payload = value.get("data").context("candles payload missing data")?;
            let candle: OhlcvtCandle =
                serde_json::from_value(payload.clone()).context("invalid candles candle shape")?;
            Ok(Some(LiveUpdate::new(
                config,
                LiveRecord::Candles(ScriptCandle::from_mmt(candle)),
            )))
        }
    }
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

impl LiveState {
    fn apply(&mut self, update: &LiveUpdate) {
        self.records
            .insert(update.selector.clone(), update.record.clone());
    }
}

impl ScriptRunSummary {
    fn record(&mut self, result: &ScriptRunResult<ScriptInputs>) {
        self.updates += 1;
        self.last_ts_ms = Some(result.ts_ms);
        if !is_empty_json_value_object(&result.output.signal) {
            self.signals += 1;
        }
        if !is_empty_json_value_object(&result.output.intent) {
            self.intents += 1;
        }
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
            println!("signals: {}", summary.signals);
            println!("intents: {}", summary.intents);
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
    state: &LiveState,
    update: &LiveUpdate,
    source_configs: &SourceConfigs,
    provider: &str,
    symbol: &str,
) -> Result<Value> {
    let mut payload = serde_json::Map::new();
    payload.insert("mode".to_string(), Value::String("stream".to_string()));
    payload.insert("source".to_string(), Value::String(update.selector.clone()));
    payload.insert(
        "source_type".to_string(),
        Value::String(update.source.as_str().to_string()),
    );
    payload.insert("provider".to_string(), Value::String(provider.to_string()));
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

    let mut sources = serde_json::Map::new();
    for (selector, record) in &state.records {
        let config = source_configs
            .get(selector)
            .with_context(|| format!("missing source config for {selector}"))?;
        let envelope = live_record_payload(record, config);
        sources.insert(selector.clone(), envelope.clone());
        let same_kind = source_configs
            .values()
            .filter(|candidate| candidate.source == config.source)
            .count();
        if same_kind == 1 {
            payload.insert(config.source.as_str().to_string(), envelope);
        }
    }
    payload.insert("sources".to_string(), Value::Object(sources));

    Ok(Value::Object(payload))
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

fn is_empty_json_value_object(value: &Value) -> bool {
    matches!(value, Value::Object(map) if map.is_empty())
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
        parse_source_configs(
            &[
                "candles@binancef:timeframe=60".to_string(),
                "candles@okx:timeframe=60".to_string(),
            ],
            None,
        )
        .expect("parse source configs")
    }

    #[test]
    fn mmt_updates_route_to_the_exchange_qualified_selector() {
        let configs = cross_exchange_configs();
        let update = json!({
            "type": "data",
            "channel": "candles",
            "exchange": "OKX",
            "data": {}
        });

        let config = mmt_update_config(&update, &configs, &ScriptSource::Candles)
            .expect("route update")
            .expect("matching source config");

        assert_eq!(config.selector, "candles@okx");
    }

    #[test]
    fn live_payload_keeps_same_kind_exchanges_separate() {
        let configs = cross_exchange_configs();
        let binance = LiveUpdate::new(
            &configs["candles@binancef"],
            LiveRecord::Candles(candle(10.0)),
        );
        let okx = LiveUpdate::new(&configs["candles@okx"], LiveRecord::Candles(candle(20.0)));
        let mut state = LiveState::default();
        state.apply(&binance);
        state.apply(&okx);

        let payload = live_stream_payload(&state, &okx, &configs, "mmt", "BTC/USDT")
            .expect("build live payload");

        assert_eq!(payload["source"], "candles@okx");
        assert_eq!(payload["source_type"], "candles");
        assert_eq!(payload["exchange"], "okx");
        assert_eq!(payload["data"]["candle"]["c"], 20.0);
        assert_eq!(payload["sources"]["candles@binancef"]["candle"]["c"], 10.0);
        assert_eq!(payload["sources"]["candles@okx"]["candle"]["c"], 20.0);
        assert!(payload.get("candles").is_none());
    }
}
