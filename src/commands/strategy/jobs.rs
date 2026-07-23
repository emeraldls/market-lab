use anyhow::{Result, bail};
use chrono::{Local, Utc};

use crate::cli::{OutputFormat, StrategyJobArgs, StrategyJobsArgs, StrategyLogsArgs};
use crate::runtime;
use crate::strategies::jobs::{StrategyJob, StrategyJobDefinition, StrategyJobStatus};

pub async fn handle_list(args: StrategyJobsArgs) -> Result<()> {
    validate_output(args.output)?;
    let jobs = runtime::list_strategy_jobs().await?;
    render_jobs(&jobs, args.output)
}

pub async fn handle_status(args: StrategyJobArgs) -> Result<()> {
    args.validate()?;
    let job = runtime::get_strategy_job(&args.job).await?;
    render_job(&job, args.output)
}

pub async fn handle_stop(args: StrategyJobArgs) -> Result<()> {
    args.validate()?;
    let job = runtime::stop_strategy_job(&args.job).await?;
    render_job(&job, args.output)
}

pub async fn handle_logs(args: StrategyLogsArgs) -> Result<()> {
    args.validate()?;
    let (mut cursor, values) = runtime::strategy_output_after(&args.job, 0)?;
    let start = values.len().saturating_sub(args.limit);
    render_log_values(&values[start..], args.output)?;
    if !args.follow {
        return Ok(());
    }

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = interval.tick() => {
                let (total, values) = runtime::strategy_output_after(&args.job, cursor)?;
                cursor = total;
                render_log_values(&values, args.output)?;
            }
        }
    }
}

fn render_jobs(jobs: &[StrategyJob], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(jobs)?),
        OutputFormat::Jsonl => {
            for job in jobs {
                println!("{}", serde_json::to_string(job)?);
            }
        }
        OutputFormat::Terminal => {
            if jobs.is_empty() {
                println!("no strategy jobs");
                return Ok(());
            }
            println!(
                "{:<36} {:<11} {:>8} {:<14} {:<13} {:<14}",
                "JOB", "STATUS", "PID", "STRATEGY", "EXCHANGE", "SYMBOL"
            );
            for job in jobs {
                println!(
                    "{:<36} {:<11} {:>8} {:<14} {:<13} {:<14}",
                    job.id,
                    status_name(job.status),
                    job.pid
                        .map_or_else(|| "-".to_string(), |pid| pid.to_string()),
                    job.definition.name(),
                    venue_name(job.definition.venue()),
                    job.definition.symbol(),
                );
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_job(job: &StrategyJob, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(job)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(job)?),
        OutputFormat::Terminal => {
            println!("strategy job: {}", job.id);
            println!("  status:           {}", status_name(job.status));
            println!(
                "  pid:              {}",
                job.pid
                    .map_or_else(|| "-".to_string(), |pid| pid.to_string())
            );
            println!("  strategy:         {}", job.definition.name());
            println!("  symbol:           {}", job.definition.symbol());
            match &job.definition {
                StrategyJobDefinition::Twap(definition) => {
                    println!("  venue:            {}", venue_name(definition.venue));
                    println!("  side:             {:?}", definition.side);
                    println!("  total size:       {}", definition.total_size);
                    if let Some(margin) = definition.requested_margin {
                        println!("  requested margin: {margin}");
                    }
                    println!("  target margin:    {}", definition.target_margin);
                    println!("  target exposure:  {}", definition.target_exposure);
                    println!("  duration:         {}s", definition.duration_seconds);
                    println!("  interval:         {}s", definition.interval_seconds);
                    println!("  leverage:         {}x", definition.leverage);
                    println!("  reduce only:      {}", definition.reduce_only);
                }
                StrategyJobDefinition::Vwap(definition) => {
                    println!("  venue:            {}", venue_name(definition.venue));
                    println!("  side:             {:?}", definition.side);
                    println!("  total size:       {}", definition.total_size);
                    if let Some(margin) = definition.requested_margin {
                        println!("  requested margin: {margin}");
                    }
                    println!("  target margin:    {}", definition.target_margin);
                    println!("  target exposure:  {}", definition.target_exposure);
                    println!("  duration:         {}s", definition.duration_seconds);
                    println!(
                        "  volume sources:   {}",
                        definition
                            .volume_sources
                            .iter()
                            .map(crate::strategies::vwap::VolumeSource::selector)
                            .collect::<Vec<_>>()
                            .join(",")
                    );
                    println!("  leverage:         {}x", definition.leverage);
                    println!("  reduce only:      {}", definition.reduce_only);
                }
                StrategyJobDefinition::Oiwap(definition) => {
                    println!("  venue:            {}", venue_name(definition.venue));
                    println!("  side:             {:?}", definition.side);
                    println!("  total size:       {}", definition.total_size);
                    if let Some(margin) = definition.requested_margin {
                        println!("  requested margin: {margin}");
                    }
                    println!("  target margin:    {}", definition.target_margin);
                    println!("  target exposure:  {}", definition.target_exposure);
                    println!("  duration:         {}s", definition.duration_seconds);
                    println!(
                        "  OI sources:       {}",
                        definition
                            .oi_sources
                            .iter()
                            .map(crate::strategies::oiwap::OpenInterestSource::selector)
                            .collect::<Vec<_>>()
                            .join(",")
                    );
                    println!("  leverage:         {}x", definition.leverage);
                    println!("  reduce only:      {}", definition.reduce_only);
                }
            }
            println!("  created:          {}", format_ts(job.created_at_ms));
            println!(
                "  started:          {}",
                job.started_at_ms
                    .map_or_else(|| "not yet".to_string(), format_ts)
            );
            println!(
                "  last heartbeat:   {}",
                job.last_heartbeat_ms
                    .map_or_else(|| "not yet".to_string(), format_ts)
            );
            if let Some(error) = &job.last_error {
                println!("  error:            {error}");
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn venue_name(venue: crate::domain::execution::ExecutionVenue) -> &'static str {
    match venue {
        crate::domain::execution::ExecutionVenue::Bulk => "bulk",
        crate::domain::execution::ExecutionVenue::Hyperliquid => "hyperliquid",
    }
}

fn render_log_values(values: &[serde_json::Value], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(values)?),
        OutputFormat::Jsonl | OutputFormat::Terminal => {
            for value in values {
                println!("{}", serde_json::to_string(value)?);
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn status_name(status: StrategyJobStatus) -> &'static str {
    match status {
        StrategyJobStatus::Starting => "starting",
        StrategyJobStatus::Running => "running",
        StrategyJobStatus::Stopping => "stopping",
        StrategyJobStatus::Stopped => "stopped",
        StrategyJobStatus::Completed => "completed",
        StrategyJobStatus::Failed => "failed",
    }
}

fn format_ts(ts_ms: u64) -> String {
    chrono::DateTime::<Utc>::from_timestamp_millis(ts_ms as i64).map_or_else(
        || format!("{ts_ms} (invalid-time)"),
        |date_time| {
            format!(
                "{ts_ms} ({})",
                date_time
                    .with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S%.3f %Z")
            )
        },
    )
}

fn validate_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("strategy job commands support only --output terminal|json|jsonl");
    }
    Ok(())
}
