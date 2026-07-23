use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{
    CliSide, ExecutionVenueArg, OutputFormat, RunTwapArgs, TradeArgs, TradeOrderKind,
    TradeTimeInForce,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::{ExecutionVenue, PositionDirection};
use crate::strategies::jobs::{
    StrategyJob, StrategyJobDefinition, StrategyJobSubmission, StrategySide, TwapJobDefinition,
};
use crate::strategies::twap::TwapSchedule;

#[derive(Debug, Serialize)]
struct TwapPlanView<'a> {
    r#type: &'static str,
    strategy: &'static str,
    venue: &'static str,
    symbol: &'a str,
    side: &'static str,
    total_size: f64,
    requested_margin: Option<f64>,
    estimated_margin: f64,
    estimated_exposure: f64,
    projected_liquidation_price: Option<f64>,
    reference_price: f64,
    duration_secs: u64,
    interval_secs: u64,
    child_orders: usize,
    smallest_child_size: f64,
    largest_child_size: f64,
    leverage: f64,
    reduce_only: bool,
    dry_run: bool,
}

#[derive(Debug, Serialize)]
struct TwapChildEvent<'a> {
    r#type: &'static str,
    strategy: &'static str,
    job_id: &'a str,
    symbol: &'a str,
    side: &'static str,
    sequence: u64,
    child_orders: usize,
    size: f64,
    reference_price: f64,
    estimated_margin: f64,
    estimated_exposure: f64,
    order_id: Option<&'a str>,
    status: &'a str,
    reconciled: bool,
}

#[derive(Debug, Serialize)]
struct TwapRunSummary<'a> {
    r#type: &'static str,
    strategy: &'static str,
    job_id: &'a str,
    venue: &'static str,
    symbol: &'a str,
    side: &'static str,
    status: &'static str,
    target_size: f64,
    submitted_size: f64,
    child_orders: usize,
    submitted_orders: usize,
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

pub async fn handle(args: RunTwapArgs) -> Result<()> {
    args.validate()?;
    let direction = direction(args.side);
    let parent = build_trade_plan(&trade_args(&args, args.size, args.margin), direction).await?;
    let market =
        crate::markets::exchange_market(venue_name(parent.venue), &parent.internal_symbol)?;
    let rules = market.execution_rules()?;
    let schedule = TwapSchedule::build(
        parent.size,
        rules.lot_size,
        parent.reference_price,
        rules.min_notional,
        args.duration,
        args.interval,
    )?;
    let view = plan_view(
        &args.symbol,
        parent.venue,
        side_name(args.side),
        &schedule,
        parent.reference_price,
        parent.requested_margin,
        args.leverage,
        args.reduce_only,
        args.dry_run,
    );

    if args.dry_run {
        render_plan(&view, args.output)?;
        return Ok(());
    }
    if !args.yes && !matches!(args.output, OutputFormat::Terminal) {
        bail!("live TWAP execution with structured output requires --yes");
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_plan(&view, args.output)?;
        if !args.yes
            && !confirm_live_execution(parent.venue, parent.testnet, schedule.children.len())?
        {
            println!("cancelled; no strategy job was submitted");
            return Ok(());
        }
    }

    let submission = StrategyJobSubmission {
        definition: StrategyJobDefinition::Twap(TwapJobDefinition {
            venue: parent.venue,
            testnet: parent.testnet,
            symbol: parent.internal_symbol,
            side: strategy_side(args.side),
            total_size: parent.size,
            requested_margin: parent.requested_margin,
            target_margin: parent.estimated_margin,
            target_exposure: parent.estimated_exposure,
            duration_seconds: args.duration,
            interval_seconds: args.interval,
            leverage: args.leverage,
            reduce_only: args.reduce_only,
        }),
    };
    let job = crate::runtime::submit_strategy_job(submission).await?;
    render_submission(&job, args.output)
}

pub async fn handle_worker(job_id: &str) -> Result<()> {
    let job = crate::runtime::get_strategy_job_from_running_daemon(job_id).await?;
    handle_worker_job(job_id, job).await
}

pub async fn handle_worker_job(job_id: &str, job: StrategyJob) -> Result<()> {
    let StrategyJobDefinition::Twap(definition) = job.definition else {
        bail!("strategy worker received a non-TWAP job");
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
                "strategy": "twap",
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

async fn run_worker(job_id: &str, definition: &TwapJobDefinition) -> Result<()> {
    let direction = strategy_direction(definition.side);
    let parent = build_trade_plan(
        &worker_trade_args(definition, definition.total_size),
        direction,
    )
    .await?;
    let market =
        crate::markets::exchange_market(venue_name(parent.venue), &parent.internal_symbol)?;
    let rules = market.execution_rules()?;
    let schedule = TwapSchedule::build(
        parent.size,
        rules.lot_size,
        parent.reference_price,
        rules.min_notional,
        definition.duration_seconds,
        definition.interval_seconds,
    )?;
    let view = plan_view(
        &definition.symbol,
        definition.venue,
        strategy_side_name(definition.side),
        &schedule,
        parent.reference_price,
        definition.requested_margin,
        definition.leverage,
        definition.reduce_only,
        false,
    );
    crate::runtime::append_strategy_output(job_id, &view)?;

    let started = Instant::now();
    let mut submitted_size = 0.0;
    let mut submitted_orders = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install strategy worker termination handler")?;

    for child in &schedule.children {
        let deadline = started + Duration::from_secs(child.offset_secs);
        while Instant::now() < deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => break,
                _ = heartbeat.tick() => {
                    crate::runtime::strategy_worker_heartbeat(job_id, std::process::id()).await?;
                }
                _ = terminate.recv() => {
                    append_summary(
                        job_id,
                        definition,
                        &schedule,
                        "stopped",
                        submitted_size,
                        submitted_orders,
                        started.elapsed(),
                    )?;
                    return Err(StrategyStopped.into());
                }
                _ = tokio::signal::ctrl_c() => {
                    append_summary(
                        job_id,
                        definition,
                        &schedule,
                        "stopped",
                        submitted_size,
                        submitted_orders,
                        started.elapsed(),
                    )?;
                    return Err(StrategyStopped.into());
                }
            }
        }

        let plan = build_trade_plan(&worker_trade_args(definition, child.size), direction)
            .await
            .with_context(|| format!("failed to prepare TWAP child order {}", child.sequence))?;
        let receipt = crate::runtime::submit_strategy_trade(job_id, child.sequence, &plan)
            .await
            .with_context(|| format!("TWAP child order {} failed", child.sequence))?;
        submitted_size += plan.size;
        submitted_orders += 1;
        crate::runtime::append_strategy_output(
            job_id,
            &TwapChildEvent {
                r#type: "strategy.child_order",
                strategy: "twap",
                job_id,
                symbol: &plan.internal_symbol,
                side: strategy_side_name(definition.side),
                sequence: child.sequence,
                child_orders: schedule.children.len(),
                size: plan.size,
                reference_price: plan.reference_price,
                estimated_margin: plan.estimated_margin,
                estimated_exposure: plan.estimated_exposure,
                order_id: receipt.order_id.as_deref(),
                status: &receipt.status,
                reconciled: receipt
                    .raw_status
                    .get("reconciled")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            },
        )?;
    }

    append_summary(
        job_id,
        definition,
        &schedule,
        "completed",
        submitted_size,
        submitted_orders,
        started.elapsed(),
    )
}

fn append_summary(
    job_id: &str,
    definition: &TwapJobDefinition,
    schedule: &TwapSchedule,
    status: &'static str,
    submitted_size: f64,
    submitted_orders: usize,
    elapsed: Duration,
) -> Result<()> {
    crate::runtime::append_strategy_output(
        job_id,
        &TwapRunSummary {
            r#type: "strategy.run.finished",
            strategy: "twap",
            job_id,
            venue: venue_name(definition.venue),
            symbol: &definition.symbol,
            side: strategy_side_name(definition.side),
            status,
            target_size: schedule.total_size,
            submitted_size,
            child_orders: schedule.children.len(),
            submitted_orders,
            elapsed_ms: elapsed.as_millis(),
        },
    )
}

fn trade_args(args: &RunTwapArgs, size: Option<f64>, margin: Option<f64>) -> TradeArgs {
    TradeArgs {
        symbol: args.symbol.clone(),
        config: None,
        venue: args.venue,
        testnet: args.testnet,
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

fn worker_trade_args(definition: &TwapJobDefinition, size: f64) -> TradeArgs {
    TradeArgs {
        symbol: definition.symbol.clone(),
        config: None,
        venue: match definition.venue {
            ExecutionVenue::Bulk => ExecutionVenueArg::Bulk,
            ExecutionVenue::Hyperliquid => ExecutionVenueArg::Hyperliquid,
        },
        testnet: definition.testnet,
        size: Some(size),
        margin: None,
        order_kind: TradeOrderKind::Market,
        price: None,
        tif: TradeTimeInForce::Gtc,
        leverage: definition.leverage,
        reduce_only: definition.reduce_only,
        sl: None,
        tp: None,
        dry_run: false,
        yes: true,
        output: OutputFormat::Jsonl,
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_view<'a>(
    symbol: &'a str,
    venue: ExecutionVenue,
    side: &'static str,
    schedule: &TwapSchedule,
    reference_price: f64,
    requested_margin: Option<f64>,
    leverage: f64,
    reduce_only: bool,
    dry_run: bool,
) -> TwapPlanView<'a> {
    let smallest_child_size = schedule
        .children
        .iter()
        .map(|child| child.size)
        .fold(f64::INFINITY, f64::min);
    let largest_child_size = schedule
        .children
        .iter()
        .map(|child| child.size)
        .fold(0.0, f64::max);
    TwapPlanView {
        r#type: "strategy.plan",
        strategy: "twap",
        venue: venue_name(venue),
        symbol,
        side,
        total_size: schedule.total_size,
        requested_margin,
        estimated_margin: schedule.total_size * reference_price / leverage,
        estimated_exposure: schedule.total_size * reference_price,
        projected_liquidation_price: None,
        reference_price,
        duration_secs: schedule.duration_secs,
        interval_secs: schedule.interval_secs,
        child_orders: schedule.children.len(),
        smallest_child_size,
        largest_child_size,
        leverage,
        reduce_only,
        dry_run,
    }
}

fn render_plan(plan: &TwapPlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "TWAP plan{}",
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
            println!("  child interval:    {}s", plan.interval_secs);
            println!("  child orders:      {}", plan.child_orders);
            println!(
                "  child size range:  {}..{}",
                plan.smallest_child_size, plan.largest_child_size
            );
            println!("  leverage:          {}x", plan.leverage);
            println!(
                "  liquidation price: determined by {} after fills",
                plan.venue
            );
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

fn confirm_live_execution(venue: ExecutionVenue, testnet: bool, children: usize) -> Result<bool> {
    print!(
        "Submit a live TWAP job with {children} {} market orders? [y/N]: ",
        execution_venue_name(venue, testnet)
    );
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

fn venue_name(venue: ExecutionVenue) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "bulk",
        ExecutionVenue::Hyperliquid => "hyperliquid",
    }
}

fn execution_venue_name(venue: ExecutionVenue, testnet: bool) -> &'static str {
    match (venue, testnet) {
        (ExecutionVenue::Bulk, _) => "BULK testnet",
        (ExecutionVenue::Hyperliquid, true) => "Hyperliquid testnet",
        (ExecutionVenue::Hyperliquid, false) => "Hyperliquid mainnet",
    }
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
