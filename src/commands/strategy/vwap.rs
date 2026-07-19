use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use tokio::sync::mpsc;

use crate::cli::{
    CliSide, ExecutionVenueArg, OutputFormat, RunVwapArgs, TradeArgs, TradeOrderKind,
    TradeTimeInForce,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::{ExecutionVenue, PositionDirection};
use crate::domain::types::{OrderBookLevel, OrderBookSnapshot, VolumeProfile};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::markets;
use crate::providers::bulk::ws::{BulkCandleStream, BulkOrderBookStream};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::utils::{normalize_symbol_for_mmt, normalize_to_ms};
use crate::providers::mmt::ws_client::MmtWsClient;
use crate::strategies::execution::{FillProgress, StrategyOrderManager};
use crate::strategies::jobs::{
    StrategyJob, StrategyJobDefinition, StrategyJobSubmission, StrategySide, VwapJobDefinition,
};
use crate::strategies::vwap::{
    HistoricalVolume, VolumeCurve, VolumeProvider, VolumeSource, VolumeSourceSelector,
};

const HISTORY_DAYS: u64 = 7;
const MINUTE_MS: u64 = 60_000;
const ORDERBOOK_DEPTH: u16 = 100;
const ORDERBOOK_STATE_CAP: usize = 2_000;
const CONTROL_INTERVAL_MS: u64 = 1_000;
const MAKER_STALE_SECS: u64 = 15;
const TRAJECTORY_BAND_FRACTION: f64 = 0.005;
const MAX_TAKER_SLIPPAGE_BPS: f64 = 20.0;
const FINAL_FILL_WAIT_SECS: u64 = 10;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VwapPlanView<'a> {
    r#type: &'static str,
    strategy: &'static str,
    venue: &'static str,
    symbol: &'a str,
    side: &'static str,
    total_size: f64,
    requested_margin: Option<f64>,
    estimated_margin: f64,
    estimated_exposure: f64,
    reference_price: f64,
    duration_secs: u64,
    volume_sources: Vec<String>,
    volume_timeframe: &'static str,
    history_days: u64,
    forecast_volume: f64,
    execution_policy: &'static str,
    max_taker_slippage_bps: f64,
    leverage: f64,
    reduce_only: bool,
    dry_run: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VwapOrderEvent<'a> {
    r#type: &'static str,
    strategy: &'static str,
    job_id: &'a str,
    action: &'static str,
    sequence: u64,
    order_id: Option<&'a str>,
    size: f64,
    price: Option<f64>,
    target_size: f64,
    filled_size: f64,
    actual_volume: f64,
    degraded_market_data: bool,
    status: &'a str,
}

struct OrderEventDetails<'a> {
    action: &'static str,
    sequence: u64,
    receipt: &'a crate::domain::execution::ExecutionReceipt,
    size: f64,
    price: Option<f64>,
    target_size: f64,
    filled_size: f64,
    actual_volume: f64,
    degraded_market_data: bool,
}

struct PlanInput<'a> {
    symbol: &'a str,
    side: &'static str,
    parent: &'a crate::domain::execution::TradePlan,
    duration_secs: u64,
    sources: &'a [VolumeSource],
    curve: &'a VolumeCurve,
    reduce_only: bool,
    dry_run: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VwapRunSummary<'a> {
    r#type: &'static str,
    strategy: &'static str,
    job_id: &'a str,
    venue: &'static str,
    symbol: &'a str,
    side: &'static str,
    status: &'static str,
    target_size: f64,
    filled_size: f64,
    fill_vwap: Option<f64>,
    arrival_price: f64,
    slippage_bps: Option<f64>,
    submitted_orders: u64,
    maker_orders: u64,
    taker_orders: u64,
    degraded_market_data: bool,
    elapsed_ms: u128,
}

#[derive(Debug)]
struct StrategyStopped;

impl fmt::Display for StrategyStopped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("strategy worker stopped")
    }
}

impl Error for StrategyStopped {}

#[derive(Debug)]
enum LiveVolumeEvent {
    Bucket {
        source: String,
        ts_ms: u64,
        volume: f64,
    },
    Degraded {
        source: String,
        error: String,
    },
}

#[derive(Debug)]
struct LiveVolumeTracker {
    start_bucket_ms: u64,
    baselines: HashMap<String, f64>,
    buckets: BTreeMap<(String, u64), f64>,
    degraded: HashSet<String>,
}

impl LiveVolumeTracker {
    fn new(start_ms: u64) -> Self {
        Self {
            start_bucket_ms: start_ms / MINUTE_MS * MINUTE_MS,
            baselines: HashMap::new(),
            buckets: BTreeMap::new(),
            degraded: HashSet::new(),
        }
    }

    fn apply(&mut self, event: LiveVolumeEvent) -> Option<(String, String)> {
        match event {
            LiveVolumeEvent::Bucket {
                source,
                ts_ms,
                volume,
            } => {
                if !volume.is_finite() || volume < 0.0 {
                    self.degraded.insert(source.clone());
                    return Some((source, "received invalid live volume".to_string()));
                }
                let bucket_ms = ts_ms / MINUTE_MS * MINUTE_MS;
                if bucket_ms < self.start_bucket_ms {
                    return None;
                }
                let adjusted = if bucket_ms == self.start_bucket_ms {
                    let baseline = *self.baselines.entry(source.clone()).or_insert(volume);
                    (volume - baseline).max(0.0)
                } else {
                    volume
                };
                self.buckets.insert((source.clone(), bucket_ms), adjusted);
                self.degraded.remove(&source);
                None
            }
            LiveVolumeEvent::Degraded { source, error } => {
                self.degraded.insert(source.clone());
                Some((source, error))
            }
        }
    }

    fn total(&self) -> f64 {
        self.buckets.values().sum()
    }

    fn is_degraded(&self) -> bool {
        !self.degraded.is_empty()
    }
}

pub async fn handle(args: RunVwapArgs) -> Result<()> {
    args.validate()?;
    let selector = VolumeSourceSelector::parse(&args.volume_sources, "bulk", &args.symbol)?;
    let direction = direction(args.side);
    let parent = build_trade_plan(&trade_args(&args, args.size, args.margin), direction).await?;
    let start_ms = now_ms()?;
    let curve = build_curve(
        start_ms,
        args.duration,
        selector.sources(),
        &parent.internal_symbol,
    )
    .await?;
    let view = plan_view(PlanInput {
        symbol: &args.symbol,
        side: side_name(args.side),
        parent: &parent,
        duration_secs: args.duration,
        sources: selector.sources(),
        curve: &curve,
        reduce_only: args.reduce_only,
        dry_run: args.dry_run,
    });

    if args.dry_run {
        render_plan(&view, args.output)?;
        return Ok(());
    }
    if !args.yes && !matches!(args.output, OutputFormat::Terminal) {
        bail!("live VWAP execution with structured output requires --yes");
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_plan(&view, args.output)?;
        if !args.yes && !confirm_live_execution()? {
            println!("cancelled; no strategy job was submitted");
            return Ok(());
        }
    }

    let submission = StrategyJobSubmission {
        definition: StrategyJobDefinition::Vwap(VwapJobDefinition {
            venue: parent.venue,
            symbol: parent.internal_symbol,
            side: strategy_side(args.side),
            total_size: parent.size,
            requested_margin: parent.requested_margin,
            target_margin: parent.estimated_margin,
            target_exposure: parent.estimated_exposure,
            duration_seconds: args.duration,
            volume_sources: selector.sources().to_vec(),
            leverage: args.leverage,
            reduce_only: args.reduce_only,
        }),
    };
    let job = crate::runtime::submit_strategy_job(submission).await?;
    render_submission(&job, args.output)
}

pub async fn handle_worker_job(job_id: &str, job: StrategyJob) -> Result<()> {
    let StrategyJobDefinition::Vwap(definition) = job.definition else {
        bail!("strategy worker received a non-VWAP job");
    };
    let pid = std::process::id();
    crate::runtime::strategy_worker_started(job_id, pid).await?;
    let result = run_worker(job_id, &definition).await;
    let error = result
        .as_ref()
        .err()
        .and_then(|error| (!error.is::<StrategyStopped>()).then(|| format!("{error:#}")));
    if let Some(message) = &error {
        let _ = crate::runtime::append_strategy_output(
            job_id,
            &serde_json::json!({
                "type": "strategy.run.failed",
                "strategy": "vwap",
                "jobId": job_id,
                "error": message,
            }),
        );
    }
    let _ = crate::runtime::strategy_worker_finished(job_id, pid, error).await;
    match result {
        Err(error) if error.is::<StrategyStopped>() => Ok(()),
        result => result,
    }
}

async fn run_worker(job_id: &str, definition: &VwapJobDefinition) -> Result<()> {
    let start_ms = now_ms()?;
    let curve = build_curve(
        start_ms,
        definition.duration_seconds,
        &definition.volume_sources,
        &definition.symbol,
    )
    .await?;
    let direction = strategy_direction(definition.side);
    let parent = build_trade_plan(
        &worker_trade_args(definition, definition.total_size, None),
        direction,
    )
    .await?;
    let market = markets::market(&definition.symbol)?;
    let rules = market.execution_rules()?;
    let plan = plan_view(PlanInput {
        symbol: &definition.symbol,
        side: strategy_side_name(definition.side),
        parent: &parent,
        duration_secs: definition.duration_seconds,
        sources: &definition.volume_sources,
        curve: &curve,
        reduce_only: definition.reduce_only,
        dry_run: false,
    });
    crate::runtime::append_strategy_output(job_id, &plan)?;

    let arrival_price = parent.reference_price;
    let adapter = BulkExecutionAdapter::new()?;
    let mut orders = StrategyOrderManager::new(job_id, &parent);
    let started = Instant::now();
    let mut book_stream =
        BulkOrderBookStream::connect(&definition.symbol, ORDERBOOK_DEPTH, ORDERBOOK_STATE_CAP)
            .await?;
    let (volume_tx, mut volume_rx) = mpsc::channel(256);
    spawn_live_volume_feeds(&definition.volume_sources, &definition.symbol, volume_tx)?;
    let mut volume = LiveVolumeTracker::new(start_ms);
    let mut latest_book = None;
    let mut maker_orders = 0_u64;
    let mut taker_orders = 0_u64;
    let mut control = tokio::time::interval(Duration::from_millis(CONTROL_INTERVAL_MS));
    control.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install strategy worker termination handler")?;

    let result: Result<(&'static str, FillProgress)> = loop {
        tokio::select! {
            snapshot = book_stream.next_snapshot() => latest_book = Some(snapshot?),
            event = volume_rx.recv() => {
                if let Some(event) = event
                    && let Some((source, error)) = volume.apply(event)
                {
                    crate::runtime::append_strategy_output(job_id, &serde_json::json!({
                        "type": "strategy.market_data.degraded",
                        "strategy": "vwap",
                        "jobId": job_id,
                        "source": source,
                        "error": error,
                    }))?;
                }
            }
            _ = heartbeat.tick() => {
                crate::runtime::strategy_worker_heartbeat(job_id, std::process::id()).await?;
            }
            _ = control.tick() => {
                let Some(book) = latest_book.as_ref() else { continue; };
                let now = now_ms()?;
                let progress = orders.reconcile(&adapter).await?;
                if progress.filled_size + rules.lot_size / 2.0 >= definition.total_size {
                    break Ok(("completed", progress));
                }
                if now >= curve.end_ms() {
                    orders.cancel_working().await?;
                    let progress = orders.reconcile(&adapter).await?;
                    let remainder = (definition.total_size - progress.filled_size).max(0.0);
                    if remainder >= rules.lot_size / 2.0 {
                        let size = taker_size_within_guard(
                            definition.side,
                            remainder,
                            book,
                            rules.lot_size,
                            rules.size_precision,
                            rules.min_notional,
                        )?;
                        if size < remainder - rules.lot_size / 2.0 {
                            bail!(
                                "VWAP deadline reached but only {size} of {remainder} can execute within the {MAX_TAKER_SLIPPAGE_BPS} bps guard"
                            );
                        }
                        let receipt = submit_child(
                            &mut orders,
                            definition,
                            direction,
                            size,
                            None,
                        ).await?;
                        taker_orders += 1;
                        append_order_event(job_id, OrderEventDetails {
                            action: "deadline_taker",
                            sequence: orders.submitted_orders(),
                            receipt: &receipt,
                            size,
                            price: None,
                            target_size: definition.total_size,
                            filled_size: progress.filled_size,
                            actual_volume: volume.total(),
                            degraded_market_data: volume.is_degraded(),
                        })?;
                    }
                    let progress = orders.wait_for_target(
                        &adapter,
                        definition.total_size,
                        rules.lot_size,
                        Duration::from_secs(FINAL_FILL_WAIT_SECS),
                    ).await?;
                    if progress.filled_size + rules.lot_size / 2.0 < definition.total_size {
                        bail!(
                            "VWAP ended with {} filled of {} after final reconciliation",
                            progress.filled_size,
                            definition.total_size
                        );
                    }
                    break Ok(("completed", progress));
                }

                let target_fraction = curve.target_fraction(now, volume.total(), volume.is_degraded());
                let target_size = definition.total_size * target_fraction;
                let band = (definition.total_size * TRAJECTORY_BAND_FRACTION)
                    .max(rules.lot_size * 2.0);
                let lower_target = (target_size - band).max(0.0);
                let upper_target = (target_size + band).min(definition.total_size);
                let remaining_ms = curve.end_ms().saturating_sub(now);
                let urgent = remaining_ms <= (definition.duration_seconds * 100).max(60_000);

                if progress.filled_size + rules.lot_size / 2.0 < lower_target || (urgent && progress.filled_size < target_size) {
                    orders.cancel_working().await?;
                    let progress = orders.reconcile(&adapter).await?;
                    let deficit = (target_size - progress.filled_size)
                        .min(definition.total_size - progress.filled_size)
                        .max(0.0);
                    let size = taker_size_within_guard(
                        definition.side,
                        deficit,
                        book,
                        rules.lot_size,
                        rules.size_precision,
                        rules.min_notional,
                    )?;
                    if size >= rules.lot_size / 2.0 {
                        let receipt = submit_child(
                            &mut orders,
                            definition,
                            direction,
                            size,
                            None,
                        ).await?;
                        taker_orders += 1;
                        append_order_event(job_id, OrderEventDetails {
                            action: "catch_up_taker",
                            sequence: orders.submitted_orders(),
                            receipt: &receipt,
                            size,
                            price: None,
                            target_size,
                            filled_size: progress.filled_size,
                            actual_volume: volume.total(),
                            degraded_market_data: volume.is_degraded(),
                        })?;
                    }
                    continue;
                }

                if progress.filled_size > upper_target + rules.lot_size / 2.0 {
                    orders.cancel_working().await?;
                    continue;
                }

                let maker_price = passive_price(definition.side, book)?;
                let desired = (upper_target - progress.filled_size).max(0.0);
                let maker_size = executable_size(
                    desired,
                    definition.total_size - progress.filled_size,
                    rules.lot_size,
                    rules.size_precision,
                    maker_price,
                    rules.min_notional,
                );
                let should_replace = orders.working_needs_replace(
                    maker_price,
                    maker_size,
                    rules.tick_size,
                    rules.lot_size,
                    Duration::from_secs(MAKER_STALE_SECS),
                );
                if should_replace || maker_size < rules.lot_size / 2.0 {
                    orders.cancel_working().await?;
                }
                if !orders.has_working_order() && maker_size >= rules.lot_size / 2.0 {
                    let receipt = submit_child(
                        &mut orders,
                        definition,
                        direction,
                        maker_size,
                        Some(maker_price),
                    ).await?;
                    maker_orders += 1;
                    append_order_event(job_id, OrderEventDetails {
                        action: "maker",
                        sequence: orders.submitted_orders(),
                        receipt: &receipt,
                        size: maker_size,
                        price: Some(maker_price),
                        target_size,
                        filled_size: progress.filled_size,
                        actual_volume: volume.total(),
                        degraded_market_data: volume.is_degraded(),
                    })?;
                }
            }
            _ = terminate.recv() => break Err(StrategyStopped.into()),
            _ = tokio::signal::ctrl_c() => break Err(StrategyStopped.into()),
        }
    };

    if result.is_err() {
        let _ = orders.cancel_working().await;
    }
    match result {
        Ok((status, progress)) => {
            append_summary(
                job_id,
                definition,
                status,
                progress,
                arrival_price,
                maker_orders,
                taker_orders,
                volume.is_degraded(),
                started.elapsed(),
            )?;
            Ok(())
        }
        Err(error) if error.is::<StrategyStopped>() => {
            let progress = orders.reconcile(&adapter).await.unwrap_or_default();
            append_summary(
                job_id,
                definition,
                "stopped",
                progress,
                arrival_price,
                maker_orders,
                taker_orders,
                volume.is_degraded(),
                started.elapsed(),
            )?;
            Err(error)
        }
        Err(error) => Err(error),
    }
}

async fn build_curve(
    start_ms: u64,
    duration_secs: u64,
    sources: &[VolumeSource],
    symbol: &str,
) -> Result<VolumeCurve> {
    let history_to = start_ms / MINUTE_MS * MINUTE_MS;
    let history_from = history_to.saturating_sub(HISTORY_DAYS * 86_400_000);
    let mut history = Vec::new();
    let mmt_exchanges = sources
        .iter()
        .filter(|source| source.provider == VolumeProvider::Mmt)
        .map(|source| source.exchange.clone())
        .collect::<Vec<_>>();
    if !mmt_exchanges.is_empty() {
        let series =
            MmtProvider::aggregated_volumes(&mmt_exchanges, symbol, "1m", history_from, history_to)
                .await?;
        history.extend(series.data.into_iter().map(|profile| HistoricalVolume {
            ts_ms: normalize_to_ms(profile.t),
            volume: profile.b.iter().sum::<f64>() + profile.s.iter().sum::<f64>(),
        }));
    }
    for source in sources
        .iter()
        .filter(|source| source.provider == VolumeProvider::Direct)
    {
        match source.exchange.as_str() {
            "bulk" => {
                history.extend(fetch_bulk_volume_history(symbol, history_from, history_to).await?);
            }
            exchange => bail!(
                "standalone volume adapter for `{exchange}` is not implemented; use `{exchange}@mmt`"
            ),
        }
    }
    VolumeCurve::build(start_ms, duration_secs, &history)
}

async fn fetch_bulk_volume_history(
    symbol: &str,
    from_ms: u64,
    to_ms: u64,
) -> Result<Vec<HistoricalVolume>> {
    const BULK_CHUNK_MINUTES: u64 = 2_000;
    let chunk_ms = BULK_CHUNK_MINUTES * MINUTE_MS;
    let mut cursor = from_ms;
    let mut points = BTreeMap::new();
    while cursor < to_ms {
        let chunk_to = cursor.saturating_add(chunk_ms).min(to_ms);
        let series = BulkProvider::volume_bars(symbol, "1m", cursor, chunk_to).await?;
        for bar in series.data {
            if bar.t >= from_ms && bar.t < to_ms {
                points.insert(bar.t, bar.volume);
            }
        }
        cursor = chunk_to;
    }
    Ok(points
        .into_iter()
        .map(|(ts_ms, volume)| HistoricalVolume { ts_ms, volume })
        .collect())
}

fn spawn_live_volume_feeds(
    sources: &[VolumeSource],
    symbol: &str,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    let mmt = sources
        .iter()
        .filter(|source| source.provider == VolumeProvider::Mmt)
        .map(|source| source.exchange.clone())
        .collect::<Vec<_>>();
    if !mmt.is_empty() {
        let symbol = symbol.to_string();
        let sender = sender.clone();
        tokio::spawn(async move {
            if let Err(error) = stream_mmt_volumes(&mmt, &symbol, sender.clone()).await {
                let _ = sender
                    .send(LiveVolumeEvent::Degraded {
                        source: "mmt".to_string(),
                        error: format!("{error:#}"),
                    })
                    .await;
            }
        });
    }
    for source in sources
        .iter()
        .filter(|source| source.provider == VolumeProvider::Direct)
    {
        match source.exchange.as_str() {
            "bulk" => {
                let symbol = symbol.to_string();
                let sender = sender.clone();
                tokio::spawn(async move {
                    if let Err(error) = stream_bulk_volumes(&symbol, sender.clone()).await {
                        let _ = sender
                            .send(LiveVolumeEvent::Degraded {
                                source: "bulk".to_string(),
                                error: format!("{error:#}"),
                            })
                            .await;
                    }
                });
            }
            exchange => bail!("standalone live volume adapter for `{exchange}` is not implemented"),
        }
    }
    Ok(())
}

async fn stream_mmt_volumes(
    exchanges: &[String],
    symbol: &str,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    let mut normalized_symbol = None;
    for exchange in exchanges {
        let candidate = normalize_symbol_for_mmt(exchange, symbol)?;
        if normalized_symbol
            .as_ref()
            .is_some_and(|expected| expected != &candidate)
        {
            bail!("MMT live volume sources do not share one provider symbol");
        }
        normalized_symbol.get_or_insert(candidate);
    }
    let mut exchanges = exchanges.to_vec();
    exchanges.sort();
    let source = exchanges
        .iter()
        .map(|exchange| format!("{exchange}@mmt"))
        .collect::<Vec<_>>()
        .join(",");
    let ws = MmtWsClient::shared().await?;
    ws.subscribe(serde_json::json!({
        "type": "subscribe",
        "channel": "volumes",
        "exchange": exchanges.join(":"),
        "symbol": normalized_symbol.expect("non-empty exchanges"),
        "tf": "1m",
    }))
    .await?;
    loop {
        let Some(value) = ws.next_json().await? else {
            bail!("MMT WebSocket closed");
        };
        let Some(profile) = parse_mmt_volume(value)? else {
            continue;
        };
        sender
            .send(LiveVolumeEvent::Bucket {
                source: source.clone(),
                ts_ms: normalize_to_ms(profile.t),
                volume: profile.b.iter().sum::<f64>() + profile.s.iter().sum::<f64>(),
            })
            .await
            .context("VWAP worker stopped receiving MMT volume")?;
    }
}

async fn stream_bulk_volumes(symbol: &str, sender: mpsc::Sender<LiveVolumeEvent>) -> Result<()> {
    let mut stream = BulkCandleStream::connect(symbol, "1m").await?;
    loop {
        let candle = stream.next_candle().await?;
        sender
            .send(LiveVolumeEvent::Bucket {
                source: "bulk".to_string(),
                ts_ms: candle.t,
                volume: candle.volume,
            })
            .await
            .context("VWAP worker stopped receiving BULK volume")?;
    }
}

fn parse_mmt_volume(value: serde_json::Value) -> Result<Option<VolumeProfile>> {
    if value.is_null()
        || value.get("type").and_then(serde_json::Value::as_str) == Some("subscribed")
    {
        return Ok(None);
    }
    if value.get("type").and_then(serde_json::Value::as_str) != Some("data")
        || value.get("channel").and_then(serde_json::Value::as_str) != Some("volumes")
    {
        return Ok(None);
    }
    let profile = value
        .get("data")
        .context("MMT volumes message omitted data")?;
    Ok(Some(
        serde_json::from_value(profile.clone()).context("invalid MMT volume profile")?,
    ))
}

async fn submit_child(
    orders: &mut StrategyOrderManager,
    definition: &VwapJobDefinition,
    direction: PositionDirection,
    size: f64,
    maker_price: Option<f64>,
) -> Result<crate::domain::execution::ExecutionReceipt> {
    let plan =
        build_trade_plan(&worker_trade_args(definition, size, maker_price), direction).await?;
    orders.submit(&plan, maker_price).await
}

fn passive_price(side: StrategySide, book: &OrderBookSnapshot) -> Result<f64> {
    let level = match side {
        StrategySide::Buy => book.bids.first(),
        StrategySide::Sell => book.asks.first(),
    }
    .context("BULK order book has no passive-side level")?;
    Ok(level.price)
}

fn taker_size_within_guard(
    side: StrategySide,
    requested: f64,
    book: &OrderBookSnapshot,
    lot_size: f64,
    precision: u8,
    min_notional: f64,
) -> Result<f64> {
    if requested < lot_size / 2.0 {
        return Ok(0.0);
    }
    let levels = match side {
        StrategySide::Buy => &book.asks,
        StrategySide::Sell => &book.bids,
    };
    let best = levels
        .first()
        .context("BULK order book has no taker-side liquidity")?
        .price;
    let guarded_depth = levels
        .iter()
        .take_while(|level| within_slippage(side, best, level))
        .map(|level| level.quantity)
        .sum::<f64>();
    Ok(executable_size(
        requested.min(guarded_depth),
        requested,
        lot_size,
        precision,
        best,
        min_notional,
    ))
}

fn within_slippage(side: StrategySide, best: f64, level: &&OrderBookLevel) -> bool {
    let bps = match side {
        StrategySide::Buy => (level.price - best) / best * 10_000.0,
        StrategySide::Sell => (best - level.price) / best * 10_000.0,
    };
    bps <= MAX_TAKER_SLIPPAGE_BPS + f64::EPSILON
}

fn executable_size(
    requested: f64,
    remaining: f64,
    lot_size: f64,
    precision: u8,
    price: f64,
    min_notional: f64,
) -> f64 {
    let mut size = floor_to_step(requested.min(remaining), lot_size, precision);
    let remainder = floor_to_step((remaining - size).max(0.0), lot_size, precision);
    if remainder > 0.0 && remainder * price < min_notional {
        size = floor_to_step(remaining, lot_size, precision);
    }
    if size * price + f64::EPSILON < min_notional {
        0.0
    } else {
        size
    }
}

fn floor_to_step(value: f64, step: f64, precision: u8) -> f64 {
    let units = (value / step + 1e-10).floor();
    let factor = 10_f64.powi(i32::from(precision));
    (units * step * factor).round() / factor
}

fn append_order_event(job_id: &str, details: OrderEventDetails<'_>) -> Result<()> {
    crate::runtime::append_strategy_output(
        job_id,
        &VwapOrderEvent {
            r#type: "strategy.child_order",
            strategy: "vwap",
            job_id,
            action: details.action,
            sequence: details.sequence,
            order_id: details.receipt.order_id.as_deref(),
            size: details.size,
            price: details.price,
            target_size: details.target_size,
            filled_size: details.filled_size,
            actual_volume: details.actual_volume,
            degraded_market_data: details.degraded_market_data,
            status: &details.receipt.status,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn append_summary(
    job_id: &str,
    definition: &VwapJobDefinition,
    status: &'static str,
    progress: FillProgress,
    arrival_price: f64,
    maker_orders: u64,
    taker_orders: u64,
    degraded_market_data: bool,
    elapsed: Duration,
) -> Result<()> {
    let fill_vwap = progress.vwap();
    let slippage_bps = fill_vwap.map(|price| match definition.side {
        StrategySide::Buy => (price - arrival_price) / arrival_price * 10_000.0,
        StrategySide::Sell => (arrival_price - price) / arrival_price * 10_000.0,
    });
    crate::runtime::append_strategy_output(
        job_id,
        &VwapRunSummary {
            r#type: "strategy.run.finished",
            strategy: "vwap",
            job_id,
            venue: "bulk",
            symbol: &definition.symbol,
            side: strategy_side_name(definition.side),
            status,
            target_size: definition.total_size,
            filled_size: progress.filled_size,
            fill_vwap,
            arrival_price,
            slippage_bps,
            submitted_orders: maker_orders + taker_orders,
            maker_orders,
            taker_orders,
            degraded_market_data,
            elapsed_ms: elapsed.as_millis(),
        },
    )
}

fn plan_view(input: PlanInput<'_>) -> VwapPlanView<'_> {
    VwapPlanView {
        r#type: "strategy.plan",
        strategy: "vwap",
        venue: "bulk",
        symbol: input.symbol,
        side: input.side,
        total_size: input.parent.size,
        requested_margin: input.parent.requested_margin,
        estimated_margin: input.parent.estimated_margin,
        estimated_exposure: input.parent.estimated_exposure,
        reference_price: input.parent.reference_price,
        duration_secs: input.duration_secs,
        volume_sources: input.sources.iter().map(VolumeSource::selector).collect(),
        volume_timeframe: "1m",
        history_days: HISTORY_DAYS,
        forecast_volume: input.curve.total_forecast_volume(),
        execution_policy: "maker_first_taker_catch_up",
        max_taker_slippage_bps: MAX_TAKER_SLIPPAGE_BPS,
        leverage: input.parent.leverage,
        reduce_only: input.reduce_only,
        dry_run: input.dry_run,
    }
}

fn render_plan(plan: &VwapPlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "VWAP plan{}",
                if plan.dry_run {
                    " (dry run — nothing will be submitted)"
                } else {
                    ""
                }
            );
            println!("  venue:             {}", plan.venue);
            println!("  symbol / side:     {} / {}", plan.symbol, plan.side);
            println!("  total size:        {}", plan.total_size);
            if let Some(margin) = plan.requested_margin {
                println!("  requested margin:  {margin:.8}");
            }
            println!("  est. margin:       {:.8}", plan.estimated_margin);
            println!("  est. exposure:     {:.8}", plan.estimated_exposure);
            println!("  reference price:   {}", plan.reference_price);
            println!("  duration:          {}s", plan.duration_secs);
            println!("  volume sources:    {}", plan.volume_sources.join(","));
            println!("  volume timeframe:  {}", plan.volume_timeframe);
            println!("  history:           {} days", plan.history_days);
            println!("  forecast volume:   {}", plan.forecast_volume);
            println!("  execution:         maker-first / taker catch-up");
            println!("  taker guard:       {} bps", plan.max_taker_slippage_bps);
            println!("  leverage:          {}x", plan.leverage);
            println!("  reduce only:       {}", plan.reduce_only);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_submission(job: &StrategyJob, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(job)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(job)?),
        OutputFormat::Terminal => {
            println!("strategy deployed");
            println!("  job:       {}", job.id);
            println!("  strategy:  {}", job.definition.name());
            println!("  status:    starting");
            println!("  symbol:    {}", job.definition.symbol());
            println!("  logs:      mlab strategy logs {} --follow", job.id);
            println!("  stop:      mlab strategy stop {}", job.id);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn trade_args(args: &RunVwapArgs, size: Option<f64>, margin: Option<f64>) -> TradeArgs {
    TradeArgs {
        symbol: args.symbol.clone(),
        config: None,
        venue: args.venue,
        size,
        margin,
        order_kind: TradeOrderKind::Market,
        price: None,
        tif: TradeTimeInForce::Gtc,
        leverage: args.leverage,
        reduce_only: args.reduce_only,
        sl: None,
        tp: None,
        dry_run: false,
        yes: true,
        output: args.output,
    }
}

fn worker_trade_args(
    definition: &VwapJobDefinition,
    size: f64,
    maker_price: Option<f64>,
) -> TradeArgs {
    TradeArgs {
        symbol: definition.symbol.clone(),
        config: None,
        venue: match definition.venue {
            ExecutionVenue::Bulk => ExecutionVenueArg::Bulk,
        },
        size: Some(size),
        margin: None,
        order_kind: if maker_price.is_some() {
            TradeOrderKind::Limit
        } else {
            TradeOrderKind::Market
        },
        price: maker_price,
        tif: if maker_price.is_some() {
            TradeTimeInForce::Alo
        } else {
            TradeTimeInForce::Gtc
        },
        leverage: definition.leverage,
        reduce_only: definition.reduce_only,
        sl: None,
        tp: None,
        dry_run: false,
        yes: true,
        output: OutputFormat::Jsonl,
    }
}

fn confirm_live_execution() -> Result<bool> {
    print!("Submit a live maker-first VWAP job on BULK? [y/N]: ");
    io::stdout()
        .flush()
        .context("failed to flush confirmation prompt")?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("failed to read confirmation")?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn direction(side: CliSide) -> PositionDirection {
    match side {
        CliSide::Buy => PositionDirection::Long,
        CliSide::Sell => PositionDirection::Short,
    }
}

fn strategy_direction(side: StrategySide) -> PositionDirection {
    match side {
        StrategySide::Buy => PositionDirection::Long,
        StrategySide::Sell => PositionDirection::Short,
    }
}

fn strategy_side(side: CliSide) -> StrategySide {
    match side {
        CliSide::Buy => StrategySide::Buy,
        CliSide::Sell => StrategySide::Sell,
    }
}

fn side_name(side: CliSide) -> &'static str {
    match side {
        CliSide::Buy => "buy",
        CliSide::Sell => "sell",
    }
}

fn strategy_side_name(side: StrategySide) -> &'static str {
    match side {
        StrategySide::Buy => "buy",
        StrategySide::Sell => "sell",
    }
}

fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("current timestamp does not fit in u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_partial_live_bucket_uses_a_baseline() {
        let mut tracker = LiveVolumeTracker::new(75_000);
        tracker.apply(LiveVolumeEvent::Bucket {
            source: "bulk".to_string(),
            ts_ms: 60_000,
            volume: 10.0,
        });
        tracker.apply(LiveVolumeEvent::Bucket {
            source: "bulk".to_string(),
            ts_ms: 60_000,
            volume: 13.5,
        });
        assert!((tracker.total() - 3.5).abs() < 1e-9);
    }

    #[test]
    fn executable_size_absorbs_untradeable_dust() {
        let size = executable_size(0.6, 1.0, 0.1, 1, 10.0, 5.0);
        assert_eq!(size, 1.0);
    }

    #[test]
    fn slippage_guard_caps_depth() {
        let levels = vec![
            OrderBookLevel {
                price: 100.0,
                quantity: 1.0,
            },
            OrderBookLevel {
                price: 100.1,
                quantity: 2.0,
            },
            OrderBookLevel {
                price: 100.3,
                quantity: 10.0,
            },
        ];
        let book = OrderBookSnapshot {
            exchange: "bulk".to_string(),
            symbol: "BTC/USDT".to_string(),
            timestamp_ms: 0,
            bids: Vec::new(),
            asks: levels,
        };
        let size = taker_size_within_guard(StrategySide::Buy, 10.0, &book, 0.1, 1, 1.0)
            .expect("guarded size");
        assert_eq!(size, 3.0);
    }
}
