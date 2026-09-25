use anyhow::{Result, bail};
use chrono::{Local, Utc};

use crate::cli::{DaemonBackendArgs, DaemonEventsArgs, DaemonOutputArgs, OutputFormat};
use crate::daemon;
use crate::runtime::{self, RuntimeStatus};

pub async fn handle_start(args: DaemonOutputArgs) -> Result<()> {
    validate_output(args.output)?;
    let status = runtime::ensure_running().await?;
    render_status(&status, args.output)
}

pub async fn handle_backend(args: DaemonBackendArgs) -> Result<()> {
    validate_output(args.output)?;
    let previous = daemon::load()?;
    let target = backend_config(&args, previous)?;
    let config = if args.backend.is_some() {
        runtime::configure_backend(target).await?
    } else {
        target
    };
    match args.output {
        OutputFormat::Terminal => {
            println!("mlabd backend: {}", config.backend.as_str());
            if config.backend == daemon::DaemonBackend::Docker {
                println!("  container: {}", config.docker.container);
                println!("  image:     {}", config.docker.image);
                println!("  endpoint:  {}", config.docker.endpoint());
                if let Some(cpus) = config.docker.cpus {
                    println!("  cpus:      {cpus}");
                }
                if let Some(memory) = config.docker.memory_mib {
                    println!("  memory:    {memory} MiB (swap disabled)");
                }
                if let Some(pids) = config.docker.pids_limit {
                    println!("  pids:      {pids}");
                }
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&config)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&config)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn backend_config(
    args: &DaemonBackendArgs,
    previous: daemon::DaemonConfig,
) -> Result<daemon::DaemonConfig> {
    let docker_options = args.image.is_some()
        || args.container.is_some()
        || args.port.is_some()
        || args.cpus.is_some()
        || args.memory_mib.is_some()
        || args.pids_limit.is_some();
    if docker_options && !matches!(args.backend, Some(crate::cli::DaemonBackendArg::Docker)) {
        bail!(
            "--image, --container, --port, --cpus, --memory-mib and --pids-limit require `mlab daemon backend docker`"
        );
    }
    let Some(backend) = args.backend else {
        return Ok(previous);
    };
    if matches!(backend, crate::cli::DaemonBackendArg::Native) {
        return Ok(daemon::DaemonConfig::default());
    }
    let mut config = if previous.backend == daemon::DaemonBackend::Docker {
        previous
    } else {
        daemon::DaemonConfig::default()
    };
    config.backend = daemon::DaemonBackend::Docker;
    if let Some(image) = &args.image {
        config.docker.image = daemon::validate_docker_image_reference(image)?.to_string();
    }
    if let Some(container) = &args.container {
        config.docker.container = container.clone();
    }
    if let Some(port) = args.port {
        config.docker.port = port;
    }
    if let Some(cpus) = args.cpus {
        config.docker.cpus = Some(cpus);
    }
    if let Some(memory) = args.memory_mib {
        config.docker.memory_mib = Some(memory);
    }
    if let Some(pids) = args.pids_limit {
        config.docker.pids_limit = Some(pids);
    }
    config.validate()?;
    Ok(config)
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
            println!("mlabd backend: {}", daemon::load()?.backend.as_str());
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
            println!(
                "  active bot jobs: {}",
                status
                    .bot_jobs
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
    use crate::cli::{Cli, Commands, DaemonCommands};
    use clap::Parser;

    fn backend_args(flags: &[&str]) -> DaemonBackendArgs {
        let cli = Cli::try_parse_from(
            ["mlab", "daemon", "backend"]
                .into_iter()
                .chain(flags.iter().copied()),
        )
        .unwrap();
        let Commands::Daemon {
            command: DaemonCommands::Backend(args),
        } = cli.command
        else {
            panic!("not backend args")
        };
        args
    }

    #[test]
    fn configures_independent_runtime_and_preserves_omitted_settings() {
        let args = backend_args(&[
            "docker",
            "--container",
            "mlab-alice",
            "--port",
            "48001",
            "--cpus",
            "0.5",
            "--memory-mib",
            "512",
            "--pids-limit",
            "128",
        ]);
        let config = backend_config(&args, daemon::DaemonConfig::default()).unwrap();
        assert_eq!(config.docker.container, "mlab-alice");
        assert_eq!(config.docker.port, 48001);
        assert_eq!(config.docker.cpus, Some(0.5));
        assert_eq!(config.docker.memory_mib, Some(512));
        assert_eq!(config.docker.pids_limit, Some(128));
        let updated =
            backend_config(&backend_args(&["docker", "--cpus", "1"]), config.clone()).unwrap();
        assert_eq!(updated.docker.cpus, Some(1.0));
        assert_eq!(updated.docker.container, config.docker.container);
        assert_eq!(updated.docker.port, config.docker.port);
        assert_eq!(updated.docker.memory_mib, config.docker.memory_mib);
        assert_eq!(
            backend_config(&backend_args(&[]), config.clone()).unwrap(),
            config
        );
    }

    #[test]
    fn formats_runtime_milliseconds_for_humans() {
        let formatted = format_optional_ts(Some(0));
        assert!(formatted.starts_with("0 (1970-01-01"));
        assert_eq!(format_optional_ts(None), "not yet");
    }
}
