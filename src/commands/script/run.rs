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
use crate::scripting::inputs::{
    SourceConfigs, parse_param_values, parse_source_configs, populate_source_defaults,
    resolve_params, validate_bulk_source_configs, validate_source_configs_for_run,
};
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
    candles: Option<ScriptCandle>,
    orderbook: Option<OrderBookSnapshot>,
    vd: Option<ScriptVolumeDelta>,
    oi: Option<ScriptOpenInterest>,
    volumes: Option<ScriptVolume>,
}

#[derive(Debug, Clone)]
enum LiveUpdate {
    Candles(ScriptCandle),
    Orderbook(OrderBookSnapshot),
    Vd(ScriptVolumeDelta),
    Oi(ScriptOpenInterest),
    Volumes(ScriptVolume),
}

struct ScriptRunMarket {
    provider: ProviderKind,
    exchange: String,
    symbol: String,
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

    let script = Script::load(&args.script)?;
    let provider = provider_name(args.provider.into()).to_string();
    let exchange = args.exchange_name()?.to_string();
    let mut report = report_builder(
        "script.run",
        &script,
        Some(provider),
        Some(exchange),
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
    let provider: ProviderKind = args.provider.into();
    let exchange = args.exchange_name()?.to_string();
    let symbol = require_symbol(args.symbol.as_deref())?;
    let symbol = match provider {
        ProviderKind::Bulk => catalog::market(symbol)?.internal_symbol.clone(),
        ProviderKind::Mmt => symbol.to_string(),
        ProviderKind::MarketLab => unreachable!(),
    };

    let mut source_configs = parse_source_configs(&args.source)?;
    match provider {
        ProviderKind::Mmt => validate_source_configs_for_run(&script.manifest, &source_configs)?,
        ProviderKind::Bulk => {
            populate_source_defaults(&script.manifest, &mut source_configs);
            validate_bulk_source_configs(&script.manifest, &source_configs, false)?;
        }
        ProviderKind::MarketLab => unreachable!(),
    }
    let raw_params = parse_param_values(&args.param)?;
    let resolved_params = resolve_params(&script.manifest, &raw_params)?;

    let market = ScriptRunMarket {
        provider,
        exchange,
        symbol,
    };
    stream_sources(
        args,
        script,
        source_configs,
        resolved_params,
        market,
        report,
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
) -> Result<()> {
    report.set_phase("connecting_streams");
    write_running_report_best_effort(report);

    let mut streams = ScriptLiveStreams::connect(
        market.provider,
        &script,
        &source_configs,
        &market.exchange,
        &market.symbol,
    )
    .await?;

    let session = script.start_session(&resolved_params)?;
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
        };

        let source = update.source();
        let ts_ms = update.ts_ms();
        live_state.apply(update);
        let payload = live_stream_payload(
            source,
            &live_state,
            &source_configs,
            provider_name(market.provider),
            &market.exchange,
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
        hooks += 1;
        report.record_hook(&execution.stats);
        report.set_progress("streaming_sources", hooks, hooks);
        write_running_report_best_effort(report);

        let result = ScriptRunResult {
            r#type: "script.run.result",
            version: "1",
            provider: provider_name(market.provider),
            exchange: market.exchange.clone(),
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
        render_stream_result(&result, args.output, args.verbose, &mut rendered)?;
    }
}

enum ScriptLiveStreams {
    Mmt {
        ws: MmtWsClient,
        source_configs: SourceConfigs,
        orderbook_state: Option<OrderBookState>,
    },
    Bulk(Box<BulkScriptStreams>),
}

impl ScriptLiveStreams {
    async fn connect(
        provider: ProviderKind,
        script: &Script,
        source_configs: &SourceConfigs,
        exchange: &str,
        symbol: &str,
    ) -> Result<Self> {
        match provider {
            ProviderKind::Mmt => {
                let ws = MmtWsClient::shared().await?;
                subscribe_mmt_sources(&ws, script, source_configs, exchange, symbol).await?;
                Ok(Self::Mmt {
                    ws,
                    source_configs: source_configs.clone(),
                    orderbook_state: orderbook_state(source_configs),
                })
            }
            ProviderKind::Bulk => Ok(Self::Bulk(Box::new(
                BulkScriptStreams::connect(script, source_configs, symbol).await?,
            ))),
            ProviderKind::MarketLab => bail!("script run supports --provider mmt|bulk"),
        }
    }

    async fn next_update(&mut self) -> Result<Option<LiveUpdate>> {
        match self {
            Self::Mmt {
                ws,
                source_configs,
                orderbook_state,
            } => next_mmt_update(ws, source_configs, orderbook_state).await,
            Self::Bulk(streams) => streams.next_update().await.map(Some),
        }
    }
}

struct BulkScriptStreams {
    candles: Option<BulkCandleStream>,
    orderbook: Option<BulkOrderBookStream>,
    vd: Option<BulkTradesStream>,
    oi: Option<BulkTickerStream>,
    volumes: Option<BulkCandleStream>,
    cumulative_delta: f64,
}

impl BulkScriptStreams {
    async fn connect(
        script: &Script,
        source_configs: &SourceConfigs,
        symbol: &str,
    ) -> Result<Self> {
        let candles = if script.manifest.sources.contains(&ScriptSource::Candles) {
            let seconds = source_configs
                .get(&ScriptSource::Candles)
                .context("missing source config for candles")?
                .require_timeframe(&ScriptSource::Candles)?;
            let interval = crate::providers::bulk::market_data::timeframe_from_seconds(seconds)?;
            Some(BulkCandleStream::connect(symbol, interval).await?)
        } else {
            None
        };
        let orderbook = if script.manifest.sources.contains(&ScriptSource::Orderbook) {
            let depth = source_configs
                .get(&ScriptSource::Orderbook)
                .context("missing source config for orderbook")?
                .depth_or_default();
            let state_cap = (depth as usize).saturating_mul(10).clamp(100, 10_000);
            Some(BulkOrderBookStream::connect(symbol, depth, state_cap).await?)
        } else {
            None
        };
        let vd = if script.manifest.sources.contains(&ScriptSource::Vd) {
            Some(BulkTradesStream::connect(symbol).await?)
        } else {
            None
        };
        let oi = if script.manifest.sources.contains(&ScriptSource::Oi) {
            Some(BulkTickerStream::connect(symbol).await?)
        } else {
            None
        };
        let volumes = if script.manifest.sources.contains(&ScriptSource::Volumes) {
            let seconds = source_configs
                .get(&ScriptSource::Volumes)
                .context("missing source config for volumes")?
                .require_timeframe(&ScriptSource::Volumes)?;
            let interval = crate::providers::bulk::market_data::timeframe_from_seconds(seconds)?;
            Some(BulkCandleStream::connect(symbol, interval).await?)
        } else {
            None
        };
        Ok(Self {
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

            tokio::select! {
                candle = async { candles.as_mut().expect("guarded candle stream").next_candle().await }, if has_candles => {
                    return Ok(LiveUpdate::Candles(ScriptCandle::from_bulk(candle?)));
                }
                snapshot = async { orderbook.as_mut().expect("guarded orderbook stream").next_snapshot().await }, if has_orderbook => {
                    return Ok(LiveUpdate::Orderbook(snapshot?));
                }
                trades = async { vd.as_mut().expect("guarded trades stream").next_trades().await }, if has_vd => {
                    let trades = trades?;
                    if let Some(update) = bulk_vd_update(&trades, &mut self.cumulative_delta) {
                        return Ok(LiveUpdate::Vd(update));
                    }
                }
                ticker = async { oi.as_mut().expect("guarded ticker stream").next_ticker().await }, if has_oi => {
                    let ticker = ticker?;
                    return Ok(LiveUpdate::Oi(ScriptOpenInterest::from_bulk(OpenInterestSnapshot {
                        exchange: ticker.exchange,
                        symbol: ticker.symbol,
                        timestamp_ms: ticker.timestamp_ms,
                        open_interest: ticker.open_interest,
                        mark_price: ticker.mark_price,
                        notional: ticker.open_interest * ticker.mark_price,
                    })));
                }
                candle = async { volumes.as_mut().expect("guarded volume stream").next_candle().await }, if has_volumes => {
                    return Ok(LiveUpdate::Volumes(ScriptVolume::from_bulk_candle(candle?)));
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
    script: &Script,
    source_configs: &SourceConfigs,
    exchange: &str,
    symbol: &str,
) -> Result<()> {
    let exchange = exchange.to_lowercase();
    let symbol = normalize_symbol_for_mmt(symbol)?;
    for source in &script.manifest.sources {
        match source {
            ScriptSource::Candles => {
                let tf = source_configs
                    .get(source)
                    .context("missing source config for candles")?
                    .require_timeframe(source)
                    .and_then(mmt_timeframe_from_seconds)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "candles",
                    "exchange": exchange.as_str(),
                    "symbol": symbol.as_str(),
                    "tf": tf,
                }))
                .await
                .context("failed to subscribe to candles channel")?;
            }
            ScriptSource::Orderbook => {
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "depth",
                    "exchange": exchange.as_str(),
                    "symbol": symbol.as_str(),
                }))
                .await
                .context("failed to subscribe to depth channel")?;
            }
            ScriptSource::Vd => {
                let config = source_configs
                    .get(source)
                    .context("missing source config for vd")?;
                let tf = config
                    .require_timeframe(source)
                    .and_then(mmt_timeframe_from_seconds)?;
                let bucket = config.require_bucket(source)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "vd",
                    "exchange": exchange.as_str(),
                    "symbol": symbol.as_str(),
                    "tf": tf,
                    "bucket": bucket,
                }))
                .await
                .context("failed to subscribe to vd channel")?;
            }
            ScriptSource::Oi => {
                let tf = source_configs
                    .get(source)
                    .context("missing source config for oi")?
                    .require_timeframe(source)
                    .and_then(mmt_timeframe_from_seconds)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "oi",
                    "exchange": exchange.as_str(),
                    "symbol": symbol.as_str(),
                    "tf": tf,
                }))
                .await
                .context("failed to subscribe to oi channel")?;
            }
            ScriptSource::Volumes => {
                let tf = source_configs
                    .get(source)
                    .context("missing source config for volumes")?
                    .require_timeframe(source)
                    .and_then(mmt_timeframe_from_seconds)?;
                ws.subscribe(json!({
                    "type": "subscribe",
                    "channel": "volumes",
                    "exchange": exchange.as_str(),
                    "symbol": symbol.as_str(),
                    "tf": tf,
                }))
                .await
                .context("failed to subscribe to volumes channel")?;
            }
        }
    }
    Ok(())
}

fn orderbook_state(source_configs: &SourceConfigs) -> Option<OrderBookState> {
    source_configs.get(&ScriptSource::Orderbook).map(|config| {
        let state_cap = (config.depth_or_default() as usize)
            .saturating_mul(10)
            .clamp(100, 10_000);
        OrderBookState::with_max_levels_per_side(state_cap)
    })
}

async fn next_mmt_update(
    ws: &MmtWsClient,
    source_configs: &SourceConfigs,
    orderbook_state: &mut Option<OrderBookState>,
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

    match value.get("channel").and_then(Value::as_str) {
        Some("candles") => {
            let payload = value.get("data").context("candles payload missing data")?;
            let candle: OhlcvtCandle =
                serde_json::from_value(payload.clone()).context("invalid candles candle shape")?;
            Ok(Some(LiveUpdate::Candles(ScriptCandle::from_mmt(candle))))
        }
        Some("vd") => {
            let payload = value.get("data").context("vd payload missing data")?;
            let candle: VdCandle =
                serde_json::from_value(payload.clone()).context("invalid vd candle shape")?;
            Ok(Some(LiveUpdate::Vd(ScriptVolumeDelta::from_mmt(candle))))
        }
        Some("oi") => {
            let payload = value.get("data").context("oi payload missing data")?;
            let candle: OiCandle =
                serde_json::from_value(payload.clone()).context("invalid oi candle shape")?;
            Ok(Some(LiveUpdate::Oi(ScriptOpenInterest::from_mmt(candle))))
        }
        Some("volumes") => {
            let payload = value.get("data").context("volumes payload missing data")?;
            let profile: VolumeProfile =
                serde_json::from_value(payload.clone()).context("invalid volumes profile shape")?;
            Ok(Some(LiveUpdate::Volumes(ScriptVolume::from_mmt(profile))))
        }
        Some("depth") => {
            let Some(state) = orderbook_state.as_mut() else {
                return Ok(None);
            };
            let depth = source_configs
                .get(&ScriptSource::Orderbook)
                .map(|config| config.depth_or_default())
                .unwrap_or(100);
            Ok(parse_depth_update(value, state, depth)?.map(LiveUpdate::Orderbook))
        }
        _ => Ok(None),
    }
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
    fn source(&self) -> ScriptSource {
        match self {
            LiveUpdate::Candles(_) => ScriptSource::Candles,
            LiveUpdate::Orderbook(_) => ScriptSource::Orderbook,
            LiveUpdate::Vd(_) => ScriptSource::Vd,
            LiveUpdate::Oi(_) => ScriptSource::Oi,
            LiveUpdate::Volumes(_) => ScriptSource::Volumes,
        }
    }

    fn ts_ms(&self) -> u64 {
        match self {
            LiveUpdate::Candles(candle) => candle.t,
            LiveUpdate::Orderbook(snapshot) => snapshot.timestamp_ms,
            LiveUpdate::Vd(candle) => candle.t,
            LiveUpdate::Oi(candle) => candle.t,
            LiveUpdate::Volumes(profile) => profile.t,
        }
    }
}

impl LiveState {
    fn apply(&mut self, update: LiveUpdate) {
        match update {
            LiveUpdate::Candles(candle) => self.candles = Some(candle),
            LiveUpdate::Orderbook(snapshot) => self.orderbook = Some(snapshot),
            LiveUpdate::Vd(candle) => self.vd = Some(candle),
            LiveUpdate::Oi(candle) => self.oi = Some(candle),
            LiveUpdate::Volumes(profile) => self.volumes = Some(profile),
        }
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
    source: ScriptSource,
    state: &LiveState,
    source_configs: &SourceConfigs,
    provider: &str,
    exchange: &str,
    symbol: &str,
) -> Result<Value> {
    let mut payload = serde_json::Map::new();
    payload.insert("mode".to_string(), Value::String("stream".to_string()));
    payload.insert(
        "source".to_string(),
        Value::String(source.as_str().to_string()),
    );
    payload.insert("provider".to_string(), Value::String(provider.to_string()));
    payload.insert("exchange".to_string(), Value::String(exchange.to_string()));
    payload.insert("symbol".to_string(), Value::String(symbol.to_string()));

    if let Some(candle) = &state.candles {
        payload.insert(
            "candles".to_string(),
            json!({
                "candle": candle,
            }),
        );
    }
    if let Some(snapshot) = &state.orderbook {
        payload.insert(
            "orderbook".to_string(),
            json!({
                "snapshot": snapshot,
            }),
        );
    }
    if let Some(candle) = &state.vd {
        let config = source_configs
            .get(&ScriptSource::Vd)
            .context("missing source config for vd")?;
        payload.insert(
            "vd".to_string(),
            json!({
                "candle": candle,
                "record": candle,
                "bucket": config.bucket,
                "timeframe_sec": config.timeframe,
            }),
        );
    }
    if let Some(candle) = &state.oi {
        let config = source_configs
            .get(&ScriptSource::Oi)
            .context("missing source config for oi")?;
        payload.insert(
            "oi".to_string(),
            json!({
                "candle": candle,
                "record": candle,
                "timeframe_sec": config.timeframe,
            }),
        );
    }
    if let Some(profile) = &state.volumes {
        let config = source_configs
            .get(&ScriptSource::Volumes)
            .context("missing source config for volumes")?;
        payload.insert(
            "volumes".to_string(),
            json!({
                "profile": profile,
                "record": profile,
                "timeframe_sec": config.require_timeframe(&ScriptSource::Volumes)?,
            }),
        );
    }

    Ok(Value::Object(payload))
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
