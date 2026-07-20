use std::collections::BTreeMap;
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::Serialize;

use crate::cli::{
    CliSide, OutputFormat, RunOiwapArgs, TradeArgs, TradeOrderKind, TradeTimeInForce,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::PositionDirection;
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::utils::normalize_to_ms;
use crate::strategies::jobs::{
    OiwapJobDefinition, StrategyJob, StrategyJobDefinition, StrategyJobSubmission, StrategySide,
};
use crate::strategies::oiwap::{
    OpenInterestSource, OpenInterestSourceSelector, open_interest_activity,
};
use crate::strategies::vwap::{HistoricalVolume, VolumeCurve};

use super::vwap::{
    MAX_PARTICIPATION_RATE, MAX_TAKER_SLIPPAGE_BPS, StrategyStopped, TrajectoryFeed,
    VwapFeasibility, WeightedCurves, WeightedJobDefinition, fetch_bulk_volume_history,
    render_submission, run_weighted_execution, strategy_direction, worker_trade_args,
};

const HISTORY_DAYS: u64 = 28;
const EXECUTION_HISTORY_DAYS: u64 = 7;
const MINUTE_MS: u64 = 60_000;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OiwapPlanView<'a> {
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
    oi_sources: Vec<String>,
    oi_timeframe: &'static str,
    oi_activity: &'static str,
    history_days: u64,
    forecast_oi_activity: f64,
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

struct PlanInput<'a> {
    symbol: &'a str,
    side: &'static str,
    parent: &'a crate::domain::execution::TradePlan,
    duration_secs: u64,
    sources: &'a [OpenInterestSource],
    curves: &'a WeightedCurves,
    reduce_only: bool,
    dry_run: bool,
}

pub async fn handle(args: RunOiwapArgs) -> Result<()> {
    args.validate()?;
    let selector = OpenInterestSourceSelector::parse(&args.oi_sources, &args.symbol)?;
    let parent = build_trade_plan(
        &trade_args(&args, args.size, args.margin),
        direction(args.side),
    )
    .await?;
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
        curves: &curves,
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
            "OIWAP is not feasible within the {:.2}% execution-venue participation cap: forecast capacity is {} with an expected shortfall of {}; reduce the amount or increase --duration",
            MAX_PARTICIPATION_RATE * 100.0,
            feasibility.forecast_execution_capacity,
            feasibility.forecast_shortfall,
        );
    }
    if !args.yes && !matches!(args.output, OutputFormat::Terminal) {
        bail!("live OIWAP execution with structured output requires --yes");
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_plan(&view, args.output)?;
        if !args.yes && !confirm_live_execution()? {
            println!("cancelled; no strategy job was submitted");
            return Ok(());
        }
    }

    let submission = StrategyJobSubmission {
        definition: StrategyJobDefinition::Oiwap(OiwapJobDefinition {
            venue: parent.venue,
            symbol: parent.internal_symbol,
            side: strategy_side(args.side),
            total_size: parent.size,
            requested_margin: parent.requested_margin,
            target_margin: parent.estimated_margin,
            target_exposure: parent.estimated_exposure,
            duration_seconds: args.duration,
            oi_sources: selector.sources().to_vec(),
            leverage: args.leverage,
            reduce_only: args.reduce_only,
        }),
    };
    let job = crate::runtime::submit_strategy_job(submission).await?;
    render_submission(&job, args.output)
}

pub async fn handle_worker_job(job_id: &str, job: StrategyJob) -> Result<()> {
    let StrategyJobDefinition::Oiwap(definition) = job.definition else {
        bail!("strategy worker received a non-OIWAP job");
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
                "strategy": "oiwap",
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

async fn run_worker(job_id: &str, definition: &OiwapJobDefinition) -> Result<()> {
    let start_ms = now_ms()?;
    let weighted = WeightedJobDefinition::from(definition);
    let curves = build_curves(
        start_ms,
        definition.duration_seconds,
        &definition.oi_sources,
        &definition.symbol,
    )
    .await?;
    let parent = build_trade_plan(
        &worker_trade_args(&weighted, weighted.total_size, None),
        strategy_direction(weighted.side),
    )
    .await?;
    let feasibility = VwapFeasibility::assess(parent.size, &curves.execution);
    if !feasibility.feasible() {
        bail!(
            "OIWAP became infeasible before worker start: forecast execution capacity is {} with a shortfall of {}",
            feasibility.forecast_execution_capacity,
            feasibility.forecast_shortfall,
        );
    }
    let plan = plan_view(PlanInput {
        symbol: &definition.symbol,
        side: strategy_side_name(definition.side),
        parent: &parent,
        duration_secs: definition.duration_seconds,
        sources: &definition.oi_sources,
        curves: &curves,
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
        TrajectoryFeed::OpenInterest(definition.oi_sources.clone()),
    )
    .await
}

async fn build_curves(
    start_ms: u64,
    duration_secs: u64,
    sources: &[OpenInterestSource],
    symbol: &str,
) -> Result<WeightedCurves> {
    let history_to = start_ms / MINUTE_MS * MINUTE_MS;
    let history_from = history_to.saturating_sub(HISTORY_DAYS * 86_400_000);
    let execution_history_from = history_to.saturating_sub(EXECUTION_HISTORY_DAYS * 86_400_000);
    let (oi_history, execution_history) = tokio::try_join!(
        fetch_open_interest_activity(sources, symbol, history_from, history_to),
        fetch_bulk_volume_history(symbol, execution_history_from, history_to),
    )?;

    Ok(WeightedCurves {
        trajectory: VolumeCurve::build_for(
            "OIWAP",
            "open-interest activity",
            start_ms,
            duration_secs,
            &oi_history,
        )?,
        execution: VolumeCurve::build(start_ms, duration_secs, &execution_history)?,
    })
}

async fn fetch_open_interest_activity(
    sources: &[OpenInterestSource],
    symbol: &str,
    from_ms: u64,
    to_ms: u64,
) -> Result<Vec<HistoricalVolume>> {
    let series = stream::iter(sources.iter())
        .map(|source| async move {
            MmtProvider::oi(&source.exchange, symbol, "1m", from_ms, to_ms)
                .await
                .with_context(|| format!("failed to fetch {} OI history", source.selector()))
        })
        .buffer_unordered(sources.len().max(1))
        .try_collect::<Vec<_>>()
        .await?;

    let mut activity_by_minute = BTreeMap::<u64, f64>::new();
    for source_series in series {
        for candle in source_series.data {
            let ts_ms = normalize_to_ms(candle.t);
            if ts_ms >= from_ms && ts_ms < to_ms {
                *activity_by_minute.entry(ts_ms).or_default() +=
                    open_interest_activity(&source_series.exchange, &candle)?;
            }
        }
    }

    Ok(activity_by_minute
        .into_iter()
        .map(|(ts_ms, volume)| HistoricalVolume { ts_ms, volume })
        .collect())
}

fn plan_view(input: PlanInput<'_>) -> OiwapPlanView<'_> {
    let feasibility = VwapFeasibility::assess(input.parent.size, &input.curves.execution);
    OiwapPlanView {
        r#type: "strategy.plan",
        strategy: "oiwap",
        venue: "bulk",
        symbol: input.symbol,
        side: input.side,
        total_size: input.parent.size,
        requested_margin: input.parent.requested_margin,
        estimated_margin: input.parent.estimated_margin,
        estimated_exposure: input.parent.estimated_exposure,
        reference_price: input.parent.reference_price,
        duration_secs: input.duration_secs,
        oi_sources: input
            .sources
            .iter()
            .map(OpenInterestSource::selector)
            .collect(),
        oi_timeframe: "1m",
        oi_activity: "absolute_open_to_close_change",
        history_days: HISTORY_DAYS,
        forecast_oi_activity: input.curves.trajectory.total_forecast_volume(),
        execution_venue_forecast_volume: input.curves.execution.total_forecast_volume(),
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

fn render_plan(plan: &OiwapPlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "OIWAP plan{}",
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
            println!("  OI sources:        {}", plan.oi_sources.join(","));
            println!("  OI timeframe:      {}", plan.oi_timeframe);
            println!("  OI activity:       absolute one-minute open-to-close change");
            println!("  history:           {} days", plan.history_days);
            println!("  forecast activity: {}", plan.forecast_oi_activity);
            println!(
                "  venue volume:      {}",
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

fn trade_args(args: &RunOiwapArgs, size: Option<f64>, margin: Option<f64>) -> TradeArgs {
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

fn confirm_live_execution() -> Result<bool> {
    print!("Submit a live maker-first OIWAP job on BULK? [y/N]: ");
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
    use crate::domain::types::OiCandle;

    use super::*;

    #[test]
    fn historical_activity_sums_absolute_changes_without_cross_venue_cancellation() {
        let rising = OiCandle {
            t: 60,
            o: 100.0,
            h: 110.0,
            l: 100.0,
            c: 110.0,
            n: 2,
        };
        let falling = OiCandle {
            t: 60,
            o: 100.0,
            h: 100.0,
            l: 90.0,
            c: 90.0,
            n: 2,
        };
        let total = open_interest_activity("binancef", &rising).unwrap()
            + open_interest_activity("bybitf", &falling).unwrap();
        assert_eq!(total, 20.0);
    }

    #[test]
    fn oi_candle_shape_remains_compatible_with_mmt() {
        let candle: OiCandle = serde_json::from_value(serde_json::json!({
            "t": 1_704_067_200,
            "o": 100.0,
            "h": 105.0,
            "l": 98.0,
            "c": 103.0,
            "n": 10
        }))
        .expect("MMT OI candle");
        assert_eq!(candle.c, 103.0);
    }
}
