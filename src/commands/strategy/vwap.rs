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
use crate::domain::execution::{ExecutionVenue, PositionDirection, TradePlan};
use crate::domain::types::{OiCandle, OrderBookLevel, OrderBookSnapshot};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::bulk::markets;
use crate::providers::bulk::ws::{BulkOrderBookStream, BulkTradesStream};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::utils::{normalize_symbol_for_mmt, normalize_to_ms};
use crate::providers::mmt::ws_client::MmtWsClient;
use crate::strategies::execution::{FillProgress, StrategyOrderManager};
use crate::strategies::jobs::{
    OiwapJobDefinition, StrategyJob, StrategyJobDefinition, StrategyJobSubmission, StrategySide,
    VwapJobDefinition,
};
use crate::strategies::oiwap::{
    LiveOpenInterestActivity, OpenInterestProvider, OpenInterestSource,
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
const MAKER_FORECAST_HORIZON_MS: u64 = MINUTE_MS;
const TRAJECTORY_BAND_FRACTION: f64 = 0.005;
pub(super) const MAX_PARTICIPATION_RATE: f64 = 0.10;
pub(super) const MAX_TAKER_SLIPPAGE_BPS: f64 = 20.0;
const FINAL_FILL_WAIT_SECS: u64 = 10;

pub(super) struct WeightedCurves {
    pub(super) trajectory: VolumeCurve,
    pub(super) execution: VolumeCurve,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct VwapFeasibility {
    pub(super) required_participation_rate: f64,
    pub(super) forecast_execution_capacity: f64,
    pub(super) forecast_shortfall: f64,
}

impl VwapFeasibility {
    pub(super) fn assess(parent_size: f64, execution_curve: &VolumeCurve) -> Self {
        let execution_volume = execution_curve.total_forecast_volume();
        let required_participation_rate = parent_size / execution_volume;
        let forecast_execution_capacity = execution_volume * MAX_PARTICIPATION_RATE;
        Self {
            required_participation_rate,
            forecast_execution_capacity,
            forecast_shortfall: (parent_size - forecast_execution_capacity).max(0.0),
        }
    }

    pub(super) fn feasible(self) -> bool {
        self.forecast_shortfall <= f64::EPSILON
    }
}

#[derive(Clone, Debug)]
pub(super) struct WeightedJobDefinition {
    pub(super) strategy: &'static str,
    pub(super) venue: ExecutionVenue,
    pub(super) symbol: String,
    pub(super) side: StrategySide,
    pub(super) total_size: f64,
    pub(super) duration_seconds: u64,
    pub(super) leverage: f64,
    pub(super) reduce_only: bool,
}

impl From<&VwapJobDefinition> for WeightedJobDefinition {
    fn from(definition: &VwapJobDefinition) -> Self {
        Self {
            strategy: "vwap",
            venue: definition.venue,
            symbol: definition.symbol.clone(),
            side: definition.side,
            total_size: definition.total_size,
            duration_seconds: definition.duration_seconds,
            leverage: definition.leverage,
            reduce_only: definition.reduce_only,
        }
    }
}

impl From<&OiwapJobDefinition> for WeightedJobDefinition {
    fn from(definition: &OiwapJobDefinition) -> Self {
        Self {
            strategy: "oiwap",
            venue: definition.venue,
            symbol: definition.symbol.clone(),
            side: definition.side,
            total_size: definition.total_size,
            duration_seconds: definition.duration_seconds,
            leverage: definition.leverage,
            reduce_only: definition.reduce_only,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum TrajectoryFeed {
    Volume(Vec<VolumeSource>),
    OpenInterest(Vec<OpenInterestSource>),
}

impl TrajectoryFeed {
    fn metric(&self) -> &'static str {
        match self {
            Self::Volume(_) => "volume",
            Self::OpenInterest(_) => "absolute_open_interest_change",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ParticipationLedger {
    observed_volume: f64,
    credit: f64,
}

impl ParticipationLedger {
    fn observe(&mut self, cumulative_volume: f64, participation_rate: f64) {
        let cumulative_volume = cumulative_volume.max(self.observed_volume);
        let new_volume = cumulative_volume - self.observed_volume;
        self.credit += new_volume * participation_rate.clamp(0.0, MAX_PARTICIPATION_RATE);
        self.observed_volume = cumulative_volume;
    }

    fn available(self, filled_size: f64, working_size: f64) -> f64 {
        (self.credit - filled_size - working_size).max(0.0)
    }

    fn maker_ceiling(self, filled_size: f64, forecast_volume: f64, participation_rate: f64) -> f64 {
        (self.credit + forecast_volume * participation_rate - filled_size).max(0.0)
    }
}

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
    execution_venue_forecast_volume: f64,
    required_participation_rate: f64,
    max_participation_rate: f64,
    forecast_execution_capacity: f64,
    forecast_shortfall: f64,
    feasible: bool,
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
    trajectory_metric: &'static str,
    trajectory_activity: f64,
    execution_venue_volume: f64,
    participation_rate: f64,
    participation_credit: f64,
    degraded_market_data: bool,
    status: &'a str,
}

struct OrderEventDetails<'a> {
    strategy: &'static str,
    action: &'static str,
    sequence: u64,
    receipt: &'a crate::domain::execution::ExecutionReceipt,
    size: f64,
    price: Option<f64>,
    target_size: f64,
    filled_size: f64,
    trajectory_metric: &'static str,
    trajectory_activity: f64,
    execution_venue_volume: f64,
    participation_rate: f64,
    participation_credit: f64,
    degraded_market_data: bool,
}

struct PlanInput<'a> {
    symbol: &'a str,
    side: &'static str,
    parent: &'a crate::domain::execution::TradePlan,
    duration_secs: u64,
    sources: &'a [VolumeSource],
    trajectory_curve: &'a VolumeCurve,
    execution_curve: &'a VolumeCurve,
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
    execution_venue_volume: f64,
    participation_credit: f64,
    max_participation_rate: f64,
    degraded_market_data: bool,
    elapsed_ms: u128,
}

#[derive(Debug)]
pub(super) struct StrategyStopped;

impl fmt::Display for StrategyStopped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("strategy worker stopped")
    }
}

impl Error for StrategyStopped {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LiveVolumeRole {
    Trajectory,
    Execution,
}

impl LiveVolumeRole {
    fn name(self) -> &'static str {
        match self {
            Self::Trajectory => "trajectory",
            Self::Execution => "execution_venue",
        }
    }
}

#[derive(Debug)]
enum LiveVolumeEvent {
    Trade {
        role: LiveVolumeRole,
        source: String,
        ts_ms: u64,
        size: f64,
    },
    Adjustment {
        role: LiveVolumeRole,
        source: String,
        ts_ms: u64,
        delta: f64,
    },
    Degraded {
        role: LiveVolumeRole,
        source: String,
        error: String,
    },
}

impl LiveVolumeEvent {
    fn role(&self) -> LiveVolumeRole {
        match self {
            Self::Trade { role, .. }
            | Self::Adjustment { role, .. }
            | Self::Degraded { role, .. } => *role,
        }
    }
}

#[derive(Debug)]
struct LiveVolumeTracker {
    start_ms: u64,
    source_totals: HashMap<String, f64>,
    degraded: HashSet<String>,
}

impl LiveVolumeTracker {
    fn new(start_ms: u64) -> Self {
        Self {
            start_ms,
            source_totals: HashMap::new(),
            degraded: HashSet::new(),
        }
    }

    fn apply(&mut self, event: LiveVolumeEvent) -> Option<(String, String)> {
        match event {
            LiveVolumeEvent::Trade {
                role: _,
                source,
                ts_ms,
                size,
            } => {
                if !size.is_finite() || size < 0.0 {
                    self.degraded.insert(source.clone());
                    return Some((source, "received invalid live trade size".to_string()));
                }
                if ts_ms < self.start_ms {
                    return None;
                }
                *self.source_totals.entry(source.clone()).or_default() += size;
                self.degraded.remove(&source);
                None
            }
            LiveVolumeEvent::Adjustment {
                role: _,
                source,
                ts_ms,
                delta,
            } => {
                if !delta.is_finite() {
                    self.degraded.insert(source.clone());
                    return Some((
                        source,
                        "received invalid live activity adjustment".to_string(),
                    ));
                }
                if ts_ms < self.start_ms {
                    return None;
                }
                let total = self.source_totals.entry(source.clone()).or_default();
                if *total + delta < -f64::EPSILON {
                    self.degraded.insert(source.clone());
                    return Some((
                        source,
                        "live activity revision exceeded accumulated activity".to_string(),
                    ));
                }
                *total = (*total + delta).max(0.0);
                self.degraded.remove(&source);
                None
            }
            LiveVolumeEvent::Degraded {
                role: _,
                source,
                error,
            } => {
                self.degraded.insert(source.clone());
                Some((source, error))
            }
        }
    }

    fn total(&self) -> f64 {
        self.source_totals.values().sum()
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
    let curves = build_curves(
        start_ms,
        args.duration,
        selector.sources(),
        &parent.internal_symbol,
    )
    .await?;
    let feasibility = VwapFeasibility::assess(parent.size, &curves.execution);
    let view = plan_view(PlanInput {
        symbol: &args.symbol,
        side: side_name(args.side),
        parent: &parent,
        duration_secs: args.duration,
        sources: selector.sources(),
        trajectory_curve: &curves.trajectory,
        execution_curve: &curves.execution,
        reduce_only: args.reduce_only,
        dry_run: args.dry_run,
    });

    if args.dry_run {
        render_plan(&view, args.output)?;
        return Ok(());
    }
    if !feasibility.feasible() {
        if matches!(args.output, OutputFormat::Terminal) {
            render_plan(&view, args.output)?;
        }
        bail!(
            "VWAP is not feasible within the {:.2}% BULK participation cap: forecast capacity is {} with an expected shortfall of {}; reduce the amount or increase --duration",
            MAX_PARTICIPATION_RATE * 100.0,
            feasibility.forecast_execution_capacity,
            feasibility.forecast_shortfall,
        );
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
    let weighted = WeightedJobDefinition::from(definition);
    let curves = build_curves(
        start_ms,
        definition.duration_seconds,
        &definition.volume_sources,
        &definition.symbol,
    )
    .await?;
    let direction = strategy_direction(weighted.side);
    let parent = build_trade_plan(
        &worker_trade_args(&weighted, weighted.total_size, None),
        direction,
    )
    .await?;
    let feasibility = VwapFeasibility::assess(parent.size, &curves.execution);
    if !feasibility.feasible() {
        bail!(
            "VWAP became infeasible before worker start: forecast BULK capacity is {} with a shortfall of {}",
            feasibility.forecast_execution_capacity,
            feasibility.forecast_shortfall,
        );
    }
    let plan = plan_view(PlanInput {
        symbol: &definition.symbol,
        side: strategy_side_name(definition.side),
        parent: &parent,
        duration_secs: definition.duration_seconds,
        sources: &definition.volume_sources,
        trajectory_curve: &curves.trajectory,
        execution_curve: &curves.execution,
        reduce_only: definition.reduce_only,
        dry_run: false,
    });
    crate::runtime::append_strategy_output(job_id, &plan)?;

    run_weighted_execution(
        job_id,
        &weighted,
        start_ms,
        curves,
        parent,
        TrajectoryFeed::Volume(definition.volume_sources.clone()),
    )
    .await
}

pub(super) async fn run_weighted_execution(
    job_id: &str,
    definition: &WeightedJobDefinition,
    start_ms: u64,
    curves: WeightedCurves,
    parent: TradePlan,
    feed: TrajectoryFeed,
) -> Result<()> {
    let direction = strategy_direction(definition.side);
    let market = markets::market(&definition.symbol)?;
    let rules = market.execution_rules()?;
    let feasibility = VwapFeasibility::assess(parent.size, &curves.execution);
    if !feasibility.feasible() {
        bail!(
            "{} became infeasible before worker start: forecast BULK capacity is {} with a shortfall of {}",
            definition.strategy.to_ascii_uppercase(),
            feasibility.forecast_execution_capacity,
            feasibility.forecast_shortfall,
        );
    }
    let arrival_price = parent.reference_price;
    let adapter = BulkExecutionAdapter::new()?;
    let mut orders = StrategyOrderManager::new(job_id, &parent);
    let started = Instant::now();
    let mut book_stream =
        BulkOrderBookStream::connect(&definition.symbol, ORDERBOOK_DEPTH, ORDERBOOK_STATE_CAP)
            .await?;
    let (volume_tx, mut volume_rx) = mpsc::channel(256);
    let trajectory_metric = feed.metric();
    spawn_live_feeds(&feed, &definition.symbol, start_ms, volume_tx)?;
    let mut trajectory_volume = LiveVolumeTracker::new(start_ms);
    let mut execution_volume = LiveVolumeTracker::new(start_ms);
    let mut participation = ParticipationLedger::default();
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
                if let Some(event) = event {
                    let role = event.role();
                    let tracker = match role {
                        LiveVolumeRole::Trajectory => &mut trajectory_volume,
                        LiveVolumeRole::Execution => &mut execution_volume,
                    };
                    if let Some((source, error)) = tracker.apply(event) {
                        crate::runtime::append_strategy_output(job_id, &serde_json::json!({
                            "type": "strategy.market_data.degraded",
                            "strategy": definition.strategy,
                            "jobId": job_id,
                            "role": role.name(),
                            "source": source,
                            "error": error,
                        }))?;
                    }
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
                if now >= curves.trajectory.end_ms() {
                    orders.cancel_working().await?;
                    let progress = orders.reconcile(&adapter).await?;
                    participation.observe(execution_volume.total(), MAX_PARTICIPATION_RATE);
                    let remainder = (definition.total_size - progress.filled_size).max(0.0);
                    if remainder >= rules.lot_size / 2.0 {
                        let venue_capacity = participation.available(
                            progress.filled_size,
                            orders.working_remaining_size(),
                        );
                        let size = taker_size_within_guard(
                            definition.side,
                            remainder.min(venue_capacity),
                            book,
                            rules.lot_size,
                            rules.size_precision,
                            rules.min_notional,
                        )?;
                        if size < remainder - rules.lot_size / 2.0 {
                            bail!(
                                "{} deadline reached but only {size} of {remainder} can execute within the {:.2}% BULK participation cap and {MAX_TAKER_SLIPPAGE_BPS} bps depth guard",
                                definition.strategy.to_ascii_uppercase(),
                                MAX_PARTICIPATION_RATE * 100.0,
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
                            strategy: definition.strategy,
                            action: "deadline_taker",
                            sequence: orders.submitted_orders(),
                            receipt: &receipt,
                            size,
                            price: None,
                            target_size: definition.total_size,
                            filled_size: progress.filled_size,
                            trajectory_metric,
                            trajectory_activity: trajectory_volume.total(),
                            execution_venue_volume: execution_volume.total(),
                            participation_rate: MAX_PARTICIPATION_RATE,
                            participation_credit: participation.credit,
                            degraded_market_data: trajectory_volume.is_degraded()
                                || execution_volume.is_degraded(),
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
                            "{} ended with {} filled of {} after final reconciliation",
                            definition.strategy.to_ascii_uppercase(),
                            progress.filled_size,
                            definition.total_size
                        );
                    }
                    break Ok(("completed", progress));
                }

                let target_fraction = curves.trajectory.target_fraction(
                    now,
                    trajectory_volume.total(),
                    trajectory_volume.is_degraded(),
                );
                let target_size = definition.total_size * target_fraction;
                let band = (definition.total_size * TRAJECTORY_BAND_FRACTION)
                    .max(rules.lot_size * 2.0);
                let lower_target = (target_size - band).max(0.0);
                let upper_target = (target_size + band).min(definition.total_size);
                let remaining_ms = curves.trajectory.end_ms().saturating_sub(now);
                let urgent = remaining_ms <= (definition.duration_seconds * 100).max(60_000);
                let participation_rate = active_participation_rate(
                    definition.total_size,
                    progress.filled_size,
                    target_size,
                    now,
                    &curves.execution,
                );
                participation.observe(execution_volume.total(), participation_rate);

                if execution_volume.is_degraded() {
                    orders.cancel_working().await?;
                    continue;
                }

                if progress.filled_size + rules.lot_size / 2.0 < lower_target || (urgent && progress.filled_size < target_size) {
                    orders.cancel_working().await?;
                    let progress = orders.reconcile(&adapter).await?;
                    let deficit = (target_size - progress.filled_size)
                        .min(definition.total_size - progress.filled_size)
                        .max(0.0);
                    let venue_capacity = participation.available(
                        progress.filled_size,
                        orders.working_remaining_size(),
                    );
                    let size = taker_size_within_guard(
                        definition.side,
                        deficit.min(venue_capacity),
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
                            strategy: definition.strategy,
                            action: "catch_up_taker",
                            sequence: orders.submitted_orders(),
                            receipt: &receipt,
                            size,
                            price: None,
                            target_size,
                            filled_size: progress.filled_size,
                            trajectory_metric,
                            trajectory_activity: trajectory_volume.total(),
                            execution_venue_volume: execution_volume.total(),
                            participation_rate,
                            participation_credit: participation.credit,
                            degraded_market_data: trajectory_volume.is_degraded()
                                || execution_volume.is_degraded(),
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
                let forecast_volume = curves.execution.forecast_between(
                    now,
                    now.saturating_add(MAKER_FORECAST_HORIZON_MS)
                        .min(curves.execution.end_ms()),
                );
                let maker_ceiling = participation.maker_ceiling(
                    progress.filled_size,
                    forecast_volume,
                    participation_rate,
                );
                let maker_size = executable_size(
                    desired.min(maker_ceiling),
                    (definition.total_size - progress.filled_size).min(maker_ceiling),
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
                ) || orders.working_remaining_size() > maker_size + rules.lot_size / 2.0;
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
                        strategy: definition.strategy,
                        action: "maker",
                        sequence: orders.submitted_orders(),
                        receipt: &receipt,
                        size: maker_size,
                        price: Some(maker_price),
                        target_size,
                        filled_size: progress.filled_size,
                        trajectory_metric,
                        trajectory_activity: trajectory_volume.total(),
                        execution_venue_volume: execution_volume.total(),
                        participation_rate,
                        participation_credit: participation.credit,
                        degraded_market_data: trajectory_volume.is_degraded()
                            || execution_volume.is_degraded(),
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
                execution_volume.total(),
                participation.credit,
                trajectory_volume.is_degraded() || execution_volume.is_degraded(),
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
                execution_volume.total(),
                participation.credit,
                trajectory_volume.is_degraded() || execution_volume.is_degraded(),
                started.elapsed(),
            )?;
            Err(error)
        }
        Err(error) => Err(error),
    }
}

async fn build_curves(
    start_ms: u64,
    duration_secs: u64,
    sources: &[VolumeSource],
    symbol: &str,
) -> Result<WeightedCurves> {
    let history_to = start_ms / MINUTE_MS * MINUTE_MS;
    let history_from = history_to.saturating_sub(HISTORY_DAYS * 86_400_000);
    let mmt_exchanges = sources
        .iter()
        .filter(|source| source.provider == VolumeProvider::Mmt)
        .map(|source| source.exchange.clone())
        .collect::<Vec<_>>();
    let (bulk_history, mmt_history) = if mmt_exchanges.is_empty() {
        (
            fetch_bulk_volume_history(symbol, history_from, history_to).await?,
            Vec::new(),
        )
    } else {
        tokio::try_join!(
            fetch_bulk_volume_history(symbol, history_from, history_to),
            fetch_mmt_volume_history(&mmt_exchanges, symbol, history_from, history_to),
        )?
    };

    let mut trajectory_history = mmt_history;
    let mut includes_bulk = false;
    for source in sources
        .iter()
        .filter(|source| source.provider == VolumeProvider::Direct)
    {
        match source.exchange.as_str() {
            "bulk" => includes_bulk = true,
            exchange => bail!(
                "standalone volume adapter for `{exchange}` is not implemented; use `{exchange}@mmt`"
            ),
        }
    }
    if includes_bulk {
        trajectory_history.extend(bulk_history.iter().copied());
    }
    Ok(WeightedCurves {
        trajectory: VolumeCurve::build(start_ms, duration_secs, &trajectory_history)?,
        execution: VolumeCurve::build(start_ms, duration_secs, &bulk_history)?,
    })
}

async fn fetch_mmt_volume_history(
    exchanges: &[String],
    symbol: &str,
    from_ms: u64,
    to_ms: u64,
) -> Result<Vec<HistoricalVolume>> {
    let series = MmtProvider::aggregated_volumes(exchanges, symbol, "1m", from_ms, to_ms).await?;
    Ok(series
        .data
        .into_iter()
        .map(|profile| HistoricalVolume {
            ts_ms: normalize_to_ms(profile.t),
            volume: profile.b.iter().sum::<f64>() + profile.s.iter().sum::<f64>(),
        })
        .collect())
}

pub(super) async fn fetch_bulk_volume_history(
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

fn spawn_live_feeds(
    feed: &TrajectoryFeed,
    symbol: &str,
    start_ms: u64,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    match feed {
        TrajectoryFeed::Volume(sources) => spawn_live_volume_feeds(sources, symbol, sender),
        TrajectoryFeed::OpenInterest(sources) => {
            spawn_live_open_interest_feeds(sources, symbol, start_ms, sender)
        }
    }
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
            if let Err(error) = stream_mmt_trades(&mmt, &symbol, sender.clone()).await {
                let _ = sender
                    .send(LiveVolumeEvent::Degraded {
                        role: LiveVolumeRole::Trajectory,
                        source: "mmt".to_string(),
                        error: format!("{error:#}"),
                    })
                    .await;
            }
        });
    }
    let includes_bulk_trajectory = sources.iter().any(|source| {
        source.provider == VolumeProvider::Direct && source.exchange.as_str() == "bulk"
    });
    if let Some(source) = sources.iter().find(|source| {
        source.provider == VolumeProvider::Direct && source.exchange.as_str() != "bulk"
    }) {
        bail!(
            "standalone live volume adapter for `{}` is not implemented",
            source.exchange
        );
    }
    let symbol = symbol.to_string();
    tokio::spawn(async move {
        if let Err(error) =
            stream_bulk_trades(&symbol, includes_bulk_trajectory, sender.clone()).await
        {
            let error = format!("{error:#}");
            let _ = sender
                .send(LiveVolumeEvent::Degraded {
                    role: LiveVolumeRole::Execution,
                    source: "bulk".to_string(),
                    error: error.clone(),
                })
                .await;
            if includes_bulk_trajectory {
                let _ = sender
                    .send(LiveVolumeEvent::Degraded {
                        role: LiveVolumeRole::Trajectory,
                        source: "bulk".to_string(),
                        error,
                    })
                    .await;
            }
        }
    });
    Ok(())
}

fn spawn_live_open_interest_feeds(
    sources: &[OpenInterestSource],
    symbol: &str,
    start_ms: u64,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    if sources
        .iter()
        .any(|source| source.provider != OpenInterestProvider::Mmt)
    {
        bail!("OIWAP currently supports only exchange@mmt OI sources");
    }
    let exchanges = sources
        .iter()
        .map(|source| source.exchange.clone())
        .collect::<Vec<_>>();
    let oi_symbol = symbol.to_string();
    let oi_sender = sender.clone();
    tokio::spawn(async move {
        if let Err(error) =
            stream_mmt_open_interest(&exchanges, &oi_symbol, start_ms, oi_sender.clone()).await
        {
            let _ = oi_sender
                .send(LiveVolumeEvent::Degraded {
                    role: LiveVolumeRole::Trajectory,
                    source: "mmt_oi".to_string(),
                    error: format!("{error:#}"),
                })
                .await;
        }
    });

    let execution_symbol = symbol.to_string();
    tokio::spawn(async move {
        if let Err(error) = stream_bulk_trades(&execution_symbol, false, sender.clone()).await {
            let _ = sender
                .send(LiveVolumeEvent::Degraded {
                    role: LiveVolumeRole::Execution,
                    source: "bulk".to_string(),
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    Ok(())
}

async fn stream_mmt_trades(
    exchanges: &[String],
    symbol: &str,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    let mut selected = HashSet::new();
    let ws = MmtWsClient::connect().await?;
    for exchange in exchanges {
        let exchange = exchange.to_ascii_lowercase();
        let provider_symbol = normalize_symbol_for_mmt(&exchange, symbol)?;
        ws.subscribe(serde_json::json!({
            "type": "subscribe",
            "channel": "trades",
            "exchange": exchange,
            "symbol": provider_symbol,
        }))
        .await
        .with_context(|| format!("failed to subscribe to {exchange}@mmt trades"))?;
        selected.insert(exchange);
    }
    loop {
        let Some(value) = ws.next_json().await? else {
            bail!("MMT WebSocket closed");
        };
        let Some((exchange, trade)) = parse_mmt_trade(value)? else {
            continue;
        };
        if !selected.contains(&exchange) {
            continue;
        }
        sender
            .send(LiveVolumeEvent::Trade {
                role: LiveVolumeRole::Trajectory,
                source: format!("{exchange}@mmt"),
                ts_ms: normalize_to_ms(trade.t),
                size: trade.q.abs(),
            })
            .await
            .context("VWAP worker stopped receiving MMT trades")?;
    }
}

async fn stream_mmt_open_interest(
    exchanges: &[String],
    symbol: &str,
    start_ms: u64,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    let mut selected = HashSet::new();
    let ws = MmtWsClient::connect().await?;
    for exchange in exchanges {
        let exchange = exchange.to_ascii_lowercase();
        let provider_symbol = normalize_symbol_for_mmt(&exchange, symbol)?;
        ws.subscribe(serde_json::json!({
            "type": "subscribe",
            "channel": "oi",
            "exchange": exchange,
            "symbol": provider_symbol,
            "tf": "1m",
        }))
        .await
        .with_context(|| format!("failed to subscribe to {exchange}@mmt OI"))?;
        selected.insert(exchange);
    }

    let mut activity = LiveOpenInterestActivity::new(start_ms);
    loop {
        let Some(value) = ws.next_json().await? else {
            bail!("MMT WebSocket closed");
        };
        let Some((exchange, candle)) = parse_mmt_open_interest(value)? else {
            continue;
        };
        if !selected.contains(&exchange) {
            continue;
        }
        let Some((ts_ms, change)) = activity.apply(&exchange, candle)? else {
            continue;
        };
        sender
            .send(LiveVolumeEvent::Adjustment {
                role: LiveVolumeRole::Trajectory,
                source: format!("{exchange}@mmt"),
                ts_ms,
                delta: change,
            })
            .await
            .context("OIWAP worker stopped receiving MMT OI activity")?;
    }
}

async fn stream_bulk_trades(
    symbol: &str,
    include_trajectory: bool,
    sender: mpsc::Sender<LiveVolumeEvent>,
) -> Result<()> {
    let mut stream = BulkTradesStream::connect(symbol).await?;
    loop {
        for trade in stream.next_trades().await? {
            sender
                .send(LiveVolumeEvent::Trade {
                    role: LiveVolumeRole::Execution,
                    source: "bulk".to_string(),
                    ts_ms: trade.timestamp_ms,
                    size: trade.size.abs(),
                })
                .await
                .context("VWAP worker stopped receiving BULK trades")?;
            if include_trajectory {
                sender
                    .send(LiveVolumeEvent::Trade {
                        role: LiveVolumeRole::Trajectory,
                        source: "bulk".to_string(),
                        ts_ms: trade.timestamp_ms,
                        size: trade.size.abs(),
                    })
                    .await
                    .context("VWAP worker stopped receiving BULK trajectory trades")?;
            }
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct MmtTrade {
    t: u64,
    q: f64,
}

fn parse_mmt_trade(value: serde_json::Value) -> Result<Option<(String, MmtTrade)>> {
    if value.is_null()
        || value.get("type").and_then(serde_json::Value::as_str) == Some("subscribed")
    {
        return Ok(None);
    }
    if value.get("type").and_then(serde_json::Value::as_str) != Some("data")
        || value.get("channel").and_then(serde_json::Value::as_str) != Some("trades")
    {
        return Ok(None);
    }
    let exchange = value
        .get("exchange")
        .and_then(serde_json::Value::as_str)
        .context("MMT trade message omitted exchange")?
        .to_ascii_lowercase();
    let trade = value
        .get("data")
        .context("MMT trade message omitted data")?;
    Ok(Some((
        exchange,
        serde_json::from_value(trade.clone()).context("invalid MMT trade shape")?,
    )))
}

fn parse_mmt_open_interest(value: serde_json::Value) -> Result<Option<(String, OiCandle)>> {
    if value.is_null()
        || value.get("type").and_then(serde_json::Value::as_str) == Some("subscribed")
    {
        return Ok(None);
    }
    if value.get("type").and_then(serde_json::Value::as_str) != Some("data")
        || value.get("channel").and_then(serde_json::Value::as_str) != Some("oi")
    {
        return Ok(None);
    }
    let exchange = value
        .get("exchange")
        .and_then(serde_json::Value::as_str)
        .context("MMT OI message omitted exchange")?
        .to_ascii_lowercase();
    let candle = value.get("data").context("MMT OI message omitted data")?;
    Ok(Some((
        exchange,
        serde_json::from_value(candle.clone()).context("invalid MMT OI candle shape")?,
    )))
}

async fn submit_child(
    orders: &mut StrategyOrderManager,
    definition: &WeightedJobDefinition,
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

fn active_participation_rate(
    parent_size: f64,
    filled_size: f64,
    target_size: f64,
    now_ms: u64,
    execution_curve: &VolumeCurve,
) -> f64 {
    let base_rate = required_rate(parent_size, execution_curve.total_forecast_volume());
    let remaining_rate = required_rate(
        (parent_size - filled_size).max(0.0),
        execution_curve.forecast_remaining(now_ms),
    );
    let target_deficit = (target_size - filled_size).max(0.0);
    let horizon_end = now_ms
        .saturating_add(MAKER_FORECAST_HORIZON_MS)
        .min(execution_curve.end_ms());
    let catch_up_rate = required_rate(
        target_deficit,
        execution_curve.forecast_between(now_ms, horizon_end),
    );
    base_rate
        .max(remaining_rate)
        .max(catch_up_rate)
        .clamp(0.0, MAX_PARTICIPATION_RATE)
}

fn required_rate(quantity: f64, forecast_volume: f64) -> f64 {
    if quantity <= f64::EPSILON {
        0.0
    } else if forecast_volume <= f64::EPSILON {
        MAX_PARTICIPATION_RATE
    } else {
        quantity / forecast_volume
    }
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
    let hard_limit = requested.min(guarded_depth);
    Ok(executable_size(
        hard_limit,
        hard_limit,
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
            strategy: details.strategy,
            job_id,
            action: details.action,
            sequence: details.sequence,
            order_id: details.receipt.order_id.as_deref(),
            size: details.size,
            price: details.price,
            target_size: details.target_size,
            filled_size: details.filled_size,
            trajectory_metric: details.trajectory_metric,
            trajectory_activity: details.trajectory_activity,
            execution_venue_volume: details.execution_venue_volume,
            participation_rate: details.participation_rate,
            participation_credit: details.participation_credit,
            degraded_market_data: details.degraded_market_data,
            status: &details.receipt.status,
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn append_summary(
    job_id: &str,
    definition: &WeightedJobDefinition,
    status: &'static str,
    progress: FillProgress,
    arrival_price: f64,
    maker_orders: u64,
    taker_orders: u64,
    execution_venue_volume: f64,
    participation_credit: f64,
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
            strategy: definition.strategy,
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
            execution_venue_volume,
            participation_credit,
            max_participation_rate: MAX_PARTICIPATION_RATE,
            degraded_market_data,
            elapsed_ms: elapsed.as_millis(),
        },
    )
}

fn plan_view(input: PlanInput<'_>) -> VwapPlanView<'_> {
    let feasibility = VwapFeasibility::assess(input.parent.size, input.execution_curve);
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
        forecast_volume: input.trajectory_curve.total_forecast_volume(),
        execution_venue_forecast_volume: input.execution_curve.total_forecast_volume(),
        required_participation_rate: feasibility.required_participation_rate,
        max_participation_rate: MAX_PARTICIPATION_RATE,
        forecast_execution_capacity: feasibility.forecast_execution_capacity,
        forecast_shortfall: feasibility.forecast_shortfall,
        feasible: feasibility.feasible(),
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
            println!("  trajectory volume: {}", plan.forecast_volume);
            println!(
                "  BULK volume:       {}",
                plan.execution_venue_forecast_volume
            );
            println!(
                "  participation:     {:.2}% required / {:.2}% maximum",
                plan.required_participation_rate * 100.0,
                plan.max_participation_rate * 100.0,
            );
            println!("  forecast capacity: {}", plan.forecast_execution_capacity);
            println!("  forecast shortfall: {}", plan.forecast_shortfall);
            println!(
                "  feasibility:       {}",
                if plan.feasible {
                    "feasible"
                } else {
                    "infeasible"
                }
            );
            println!("  execution:         maker-first / taker catch-up");
            println!("  taker guard:       {} bps", plan.max_taker_slippage_bps);
            println!("  leverage:          {}x", plan.leverage);
            println!("  reduce only:       {}", plan.reduce_only);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub(super) fn render_submission(job: &StrategyJob, output: OutputFormat) -> Result<()> {
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

pub(super) fn worker_trade_args(
    definition: &WeightedJobDefinition,
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

pub(super) fn strategy_direction(side: StrategySide) -> PositionDirection {
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
    fn live_trade_volume_ignores_pre_start_trades_and_sums_both_sides() {
        let mut tracker = LiveVolumeTracker::new(75_000);
        tracker.apply(LiveVolumeEvent::Trade {
            role: LiveVolumeRole::Execution,
            source: "bulk".to_string(),
            ts_ms: 74_999,
            size: 10.0,
        });
        tracker.apply(LiveVolumeEvent::Trade {
            role: LiveVolumeRole::Execution,
            source: "bulk".to_string(),
            ts_ms: 75_000,
            size: 2.0,
        });
        tracker.apply(LiveVolumeEvent::Trade {
            role: LiveVolumeRole::Execution,
            source: "bulk".to_string(),
            ts_ms: 75_001,
            size: 1.5,
        });
        assert!((tracker.total() - 3.5).abs() < 1e-9);
    }

    #[test]
    fn live_activity_revisions_do_not_double_count_a_forming_oi_candle() {
        let mut tracker = LiveVolumeTracker::new(120_000);
        tracker.apply(LiveVolumeEvent::Adjustment {
            role: LiveVolumeRole::Trajectory,
            source: "binancef@mmt".to_string(),
            ts_ms: 120_000,
            delta: 5.0,
        });
        tracker.apply(LiveVolumeEvent::Adjustment {
            role: LiveVolumeRole::Trajectory,
            source: "binancef@mmt".to_string(),
            ts_ms: 120_000,
            delta: -2.0,
        });
        assert!((tracker.total() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn parses_mmt_trade_for_live_volume() {
        let (exchange, trade) = parse_mmt_trade(serde_json::json!({
            "type": "data",
            "channel": "trades",
            "exchange": "BinanceF",
            "data": {
                "id": "3065401760",
                "t": 1_704_067_200_123_u64,
                "p": 42_050.0,
                "q": 0.5,
                "b": true
            }
        }))
        .expect("MMT trade message parses")
        .expect("trade event returned");

        assert_eq!(exchange, "binancef");
        assert_eq!(trade.t, 1_704_067_200_123);
        assert_eq!(trade.q, 0.5);
    }

    #[test]
    fn participation_credit_only_uses_new_execution_volume() {
        let mut ledger = ParticipationLedger::default();
        ledger.observe(100.0, 0.05);
        ledger.observe(100.0, MAX_PARTICIPATION_RATE);
        ledger.observe(110.0, MAX_PARTICIPATION_RATE);

        assert!((ledger.credit - 6.0).abs() < 1e-9);
        assert!((ledger.available(4.0, 1.0) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn feasibility_uses_execution_venue_volume_not_trajectory_volume() {
        let execution_curve = VolumeCurve::build(
            0,
            60,
            &[HistoricalVolume {
                ts_ms: 0,
                volume: 100.0,
            }],
        )
        .expect("execution curve");
        let feasibility = VwapFeasibility::assess(20.0, &execution_curve);

        assert!(!feasibility.feasible());
        assert!((feasibility.required_participation_rate - 0.20).abs() < 1e-9);
        assert!((feasibility.forecast_execution_capacity - 10.0).abs() < 1e-9);
        assert!((feasibility.forecast_shortfall - 10.0).abs() < 1e-9);
    }

    #[test]
    fn urgency_never_exceeds_the_participation_cap() {
        let execution_curve = VolumeCurve::build(
            0,
            60,
            &[HistoricalVolume {
                ts_ms: 0,
                volume: 100.0,
            }],
        )
        .expect("execution curve");

        let rate = active_participation_rate(10.0, 0.0, 10.0, 59_000, &execution_curve);
        assert_eq!(rate, MAX_PARTICIPATION_RATE);
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

    #[test]
    fn dust_absorption_never_exceeds_guarded_depth() {
        let book = OrderBookSnapshot {
            exchange: "bulk".to_string(),
            symbol: "BTC/USDT".to_string(),
            timestamp_ms: 0,
            bids: Vec::new(),
            asks: vec![OrderBookLevel {
                price: 10.0,
                quantity: 0.6,
            }],
        };

        let size = taker_size_within_guard(StrategySide::Buy, 1.0, &book, 0.1, 1, 5.0)
            .expect("guarded size");
        assert_eq!(size, 0.6);
    }
}
