use anyhow::{Result, bail};
use chrono::{Local, Utc};

use crate::cli::{DaemonEventsArgs, DaemonOutputArgs, OutputFormat};
use crate::runtime::{self, RuntimeStatus};

pub async fn handle_start(args: DaemonOutputArgs) -> Result<()> {
    validate_output(args.output)?;
    let status = runtime::ensure_running().await?;
    render_status(&status, args.output)
}

pub async fn handle_status(args: DaemonOutputArgs) -> Result<()> {
    validate_output(args.output)?;
    let status = runtime::status().await?;
    render_status(&status, args.output)
}

pub async fn handle_stop(args: DaemonOutputArgs) -> Result<()> {
    validate_output(args.output)?;
    let stopped = runtime::stop().await?;
    match args.output {
        OutputFormat::Terminal => println!(
            "{}",
            if stopped {
                "mlabd: stopping"
            } else {
                "mlabd: not running"
            }
        ),
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "stopped": stopped }))?
        ),
        OutputFormat::Jsonl => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({ "stopped": stopped }))?
        ),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub fn handle_events(args: DaemonEventsArgs) -> Result<()> {
    validate_output(args.output)?;
    let events = runtime::recent_events(args.limit)?;
    match args.output {
        OutputFormat::Terminal => {
            if events.is_empty() {
                println!("no execution events");
            } else {
                for event in events {
                    println!("{}", serde_json::to_string(&event)?);
                }
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&events)?),
        OutputFormat::Jsonl => {
            for event in events {
                println!("{}", serde_json::to_string(&event)?);
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_status(status: &RuntimeStatus, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            if !status.running {
                println!("mlabd: stopped");
                return Ok(());
            }
            println!("mlabd: running");
            println!("  runtime version:  {}", status.version);
            println!("  pid:              {}", status.pid.unwrap_or_default());
            println!(
                "  started (ms):     {}",
                format_optional_ts(status.started_at_ms)
            );
            println!(
                "  account stream:   {}",
                if status.account_stream_connected {
                    "connected"
                } else {
                    "disconnected"
                }
            );
            println!(
                "  last account event: {}",
                format_optional_ts(status.last_account_event_ms)
            );
            println!(
                "  last gap recovery: {}",
                format_optional_ts(status.last_recovery_ms)
            );
            println!("  tracked open orders: {}", status.tracked_orders.len());
            println!(
                "  active script jobs: {}",
                status
                    .script_jobs
                    .iter()
                    .filter(|job| job.status.is_active())
                    .count()
            );
            println!(
                "  active strategy jobs: {}",
                status
                    .strategy_jobs
                    .iter()
                    .filter(|job| job.status.is_active())
                    .count()
            );
            if let Some(error) = &status.last_error {
                println!("  last error:       {error}");
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(status)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(status)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn format_optional_ts(ts_ms: Option<u64>) -> String {
    ts_ms.map_or_else(
        || "not yet".to_string(),
        |ts_ms| {
            let readable = chrono::DateTime::<Utc>::from_timestamp_millis(ts_ms as i64)
                .map(|date_time| {
                    date_time
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S%.3f %Z")
                        .to_string()
                })
                .unwrap_or_else(|| "invalid-time".to_string());
            format!("{ts_ms} ({readable})")
        },
    )
}

fn validate_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("daemon commands support only --output terminal|json|jsonl");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_runtime_milliseconds_for_humans() {
        let formatted = format_optional_ts(Some(0));
        assert!(formatted.starts_with("0 (1970-01-01"));
        assert_eq!(format_optional_ts(None), "not yet");
    }
}
