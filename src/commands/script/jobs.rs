use anyhow::{Result, bail};
use chrono::{Local, Utc};

use crate::cli::{OutputFormat, ScriptJobArgs, ScriptJobsArgs, ScriptLogsArgs};
use crate::runtime;
use crate::scripting::jobs::{ScriptJob, ScriptJobStatus};

pub async fn handle_list(args: ScriptJobsArgs) -> Result<()> {
    validate_output(args.output)?;
    let jobs = runtime::list_script_jobs().await?;
    render_jobs(&jobs, args.output)
}

pub async fn handle_status(args: ScriptJobArgs) -> Result<()> {
    args.validate()?;
    let job = runtime::get_script_job(&args.job).await?;
    render_job(&job, args.output)
}

pub async fn handle_stop(args: ScriptJobArgs) -> Result<()> {
    args.validate()?;
    let job = runtime::stop_script_job(&args.job).await?;
    render_job(&job, args.output)
}

pub async fn handle_restart(args: ScriptJobArgs) -> Result<()> {
    args.validate()?;
    let job = runtime::restart_script_job(&args.job).await?;
    render_job(&job, args.output)
}

pub async fn handle_logs(args: ScriptLogsArgs) -> Result<()> {
    args.validate()?;
    let (mut cursor, values) = runtime::script_output_after(&args.job, 0)?;
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
                let (total, values) = runtime::script_output_after(&args.job, cursor)?;
                cursor = total;
                render_log_values(&values, args.output)?;
            }
        }
    }
}

fn render_jobs(jobs: &[ScriptJob], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(jobs)?),
        OutputFormat::Jsonl => {
            for job in jobs {
                println!("{}", serde_json::to_string(job)?);
            }
        }
        OutputFormat::Terminal => {
            if jobs.is_empty() {
                println!("no script jobs");
                return Ok(());
            }
            println!(
                "{:<31} {:<11} {:>8} {:<22} {:<12} {:<14}",
                "JOB", "STATUS", "PID", "SCRIPT", "PROVIDER", "SYMBOL"
            );
            for job in jobs {
                println!(
                    "{:<31} {:<11} {:>8} {:<22} {:<12} {:<14}",
                    job.id,
                    status_name(job.status),
                    job.pid
                        .map_or_else(|| "-".to_string(), |pid| pid.to_string()),
                    truncate(&job.definition.script_name, 22),
                    job.definition.providers.join(","),
                    job.definition.symbol,
                );
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_job(job: &ScriptJob, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(job)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(job)?),
        OutputFormat::Terminal => {
            println!("script job: {}", job.id);
            println!("  status:           {}", status_name(job.status));
            println!(
                "  pid:              {}",
                job.pid
                    .map_or_else(|| "-".to_string(), |pid| pid.to_string())
            );
            println!("  script:           {}", job.definition.script_name);
            println!(
                "  snapshot:         {}",
                job.definition.snapshot_path.display()
            );
            println!("  providers:        {}", job.definition.providers.join(","));
            println!("  exchanges:        {}", job.definition.exchanges.join(","));
            println!("  symbol:           {}", job.definition.symbol);
            println!(
                "  venue:            {}",
                job.definition.venue.map_or_else(
                    || "disabled".to_string(),
                    |venue| format!("{venue:?}").to_ascii_lowercase()
                )
            );
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

fn status_name(status: ScriptJobStatus) -> &'static str {
    match status {
        ScriptJobStatus::Starting => "starting",
        ScriptJobStatus::Running => "running",
        ScriptJobStatus::Stopping => "stopping",
        ScriptJobStatus::Stopped => "stopped",
        ScriptJobStatus::Completed => "completed",
        ScriptJobStatus::Failed => "failed",
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

fn truncate(value: &str, width: usize) -> String {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(width).collect::<String>();
    if chars.next().is_some() && width > 1 {
        format!("{}…", prefix.chars().take(width - 1).collect::<String>())
    } else {
        prefix
    }
}

fn validate_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("script job commands support only --output terminal|json|jsonl");
    }
    Ok(())
}
