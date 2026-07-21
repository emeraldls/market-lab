use anyhow::{Result, bail};
use chrono::{Local, Utc};

use crate::bots::jobs::{BotJob, BotJobDefinition, BotJobStatus};
use crate::cli::{BotJobArgs, BotJobsArgs, BotLogsArgs, OutputFormat};
use crate::runtime;

pub async fn handle_list(args: BotJobsArgs) -> Result<()> {
    validate_output(args.output)?;
    render_jobs(&runtime::list_bot_jobs().await?, args.output)
}

pub async fn handle_status(args: BotJobArgs) -> Result<()> {
    args.validate()?;
    render_job(&runtime::get_bot_job(&args.job).await?, args.output)
}

pub async fn handle_stop(args: BotJobArgs) -> Result<()> {
    args.validate()?;
    render_job(&runtime::stop_bot_job(&args.job).await?, args.output)
}

pub async fn handle_logs(args: BotLogsArgs) -> Result<()> {
    args.validate()?;
    let (mut cursor, values) = runtime::bot_output_after(&args.job, 0)?;
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
                let (total, values) = runtime::bot_output_after(&args.job, cursor)?;
                cursor = total;
                render_log_values(&values, args.output)?;
            }
        }
    }
}

fn render_jobs(jobs: &[BotJob], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(jobs)?),
        OutputFormat::Jsonl => {
            for job in jobs {
                println!("{}", serde_json::to_string(job)?);
            }
        }
        OutputFormat::Terminal => {
            if jobs.is_empty() {
                println!("no bot jobs");
                return Ok(());
            }
            println!(
                "{:<36} {:<11} {:>8} {:<14} {:<14}",
                "JOB", "STATUS", "PID", "BOT", "SYMBOL"
            );
            for job in jobs {
                println!(
                    "{:<36} {:<11} {:>8} {:<14} {:<14}",
                    job.id,
                    status_name(job.status),
                    job.pid
                        .map_or_else(|| "-".to_string(), |pid| pid.to_string()),
                    job.definition.name(),
                    job.definition.symbol(),
                );
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_job(job: &BotJob, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(job)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(job)?),
        OutputFormat::Terminal => {
            println!("bot job: {}", job.id);
            println!("  status:           {}", status_name(job.status));
            println!(
                "  pid:              {}",
                job.pid
                    .map_or_else(|| "-".to_string(), |pid| pid.to_string())
            );
            println!("  bot:              {}", job.definition.name());
            println!("  symbol:           {}", job.definition.symbol());
            match &job.definition {
                BotJobDefinition::MidPrice(definition)
                | BotJobDefinition::VolumeMid(definition) => {
                    println!("  venue:            bulk");
                    println!("  max inventory:    {}", definition.max_inventory_size);
                    if let Some(margin) = definition.requested_margin {
                        println!("  requested margin: {margin}");
                    }
                    println!("  max margin:       {}", definition.max_inventory_margin);
                    println!("  max exposure:     {}", definition.max_inventory_exposure);
                    println!("  sizing:           continuous, inventory-skewed");
                    println!("  duration:         {}s", definition.duration_seconds);
                    println!("  spread:           {} bps", definition.spread_bps);
                    println!("  refresh time:     {}s", definition.refresh_seconds);
                    println!(
                        "  refresh tolerance: {} bps",
                        definition.refresh_tolerance_bps
                    );
                    println!(
                        "  directional bias: {}%",
                        definition.directional_bias_percent
                    );
                    println!("  leverage:         {}x", definition.leverage);
                    if let Some(percent) = definition.stop_loss_pct.filter(|value| *value > 0.0) {
                        println!("  stop loss:        {percent}% of allocated margin");
                    }
                }
            }
            if let Some(performance) = &job.performance {
                println!(
                    "  bought / sold:    {:.8} / {:.8}",
                    performance.bought_size, performance.sold_size
                );
                println!(
                    "  avg buy / sell:   {} / {}",
                    optional_f64(performance.average_buy_price),
                    optional_f64(performance.average_sell_price)
                );
                println!("  matched size:     {:.8}", performance.matched_size);
                println!(
                    "  inventory:        {:.8} @ {}",
                    performance.inventory_size,
                    optional_f64(performance.average_entry_price)
                );
                println!("  mark:             {:.8}", performance.mark_price);
                println!("  gross realized:   {:+.8}", performance.gross_realized_pnl);
                println!("  unrealized:       {:+.8}", performance.unrealized_pnl);
                println!(
                    "  fees / rebates:   {:+.8}{}",
                    performance.fees,
                    if performance.fees_complete {
                        ""
                    } else {
                        " (incomplete)"
                    }
                );
                println!(
                    "  trading pnl:      {}",
                    performance
                        .trading_pnl
                        .map_or_else(|| "unavailable".to_string(), |pnl| format!("{pnl:+.8}"))
                );
                println!(
                    "  return on margin: {}",
                    performance.return_on_margin_pct.map_or_else(
                        || "unavailable".to_string(),
                        |value| format!("{value:+.4}%")
                    )
                );
                println!("  pnl scope:        bot-owned fills; funding excluded");
            } else {
                println!("  performance:      awaiting first worker heartbeat");
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

fn render_log_values(values: &[serde_json::Value], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(values)?),
        OutputFormat::Jsonl => {
            for value in values {
                println!("{}", serde_json::to_string(value)?);
            }
        }
        OutputFormat::Terminal => {
            for value in values {
                println!("{}", terminal_log_line(value));
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn terminal_log_line(value: &serde_json::Value) -> String {
    let kind = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("bot.event");
    match kind {
        "bot.quote" => format!(
            "{} {} {} @ {} size={}",
            value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("quote"),
            value
                .get("side")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?"),
            value
                .get("orderId")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("-"),
            number(value, "price"),
            number(value, "size"),
        ),
        "bot.fill" => format!(
            "fill {} {} @ {} buy={} sell={} inventory={} realized={} unrealized={} fees={} pnl={}",
            value
                .get("side")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("?"),
            number(value, "size"),
            number(value, "price"),
            number(value, "boughtSize"),
            number(value, "soldSize"),
            number(value, "inventorySize"),
            performance_number(value, "grossRealizedPnl"),
            performance_number(value, "unrealizedPnl"),
            performance_number(value, "fees"),
            performance_number(value, "tradingPnl"),
        ),
        "bot.market_data" => format!(
            "market data {}{}",
            value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map_or_else(String::new, |error| format!(": {error}")),
        ),
        "bot.stop_loss" => format!(
            "STOP LOSS pnl={} limit=-{} at mark={}; cancelling quotes and flattening inventory",
            performance_number(value, "tradingPnl"),
            number(value, "maxLoss"),
            number(value, "markPrice"),
        ),
        "bot.run.finished" => format!(
            "{} bought={} sold={} residual={} realized={} unrealized={} fees={} pnl={} return={}% elapsed={}ms",
            value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("finished"),
            number(value, "boughtSize"),
            number(value, "soldSize"),
            number(value, "residualSize"),
            performance_number(value, "grossRealizedPnl"),
            performance_number(value, "unrealizedPnl"),
            performance_number(value, "fees"),
            performance_number(value, "tradingPnl"),
            performance_number(value, "returnOnMarginPct"),
            number(value, "elapsedMs"),
        ),
        "bot.run.failed" => format!(
            "ERROR {}",
            value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("bot failed")
        ),
        _ => serde_json::to_string(value).unwrap_or_else(|_| kind.to_string()),
    }
}

fn number(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .map_or_else(|| "-".to_string(), serde_json::Value::to_string)
}

fn performance_number(value: &serde_json::Value, key: &str) -> String {
    value
        .get("performance")
        .and_then(|performance| performance.get(key))
        .filter(|value| !value.is_null())
        .map_or_else(|| "unavailable".to_string(), serde_json::Value::to_string)
}

fn optional_f64(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("{value:.8}"))
}

fn status_name(status: BotJobStatus) -> &'static str {
    match status {
        BotJobStatus::Starting => "starting",
        BotJobStatus::Running => "running",
        BotJobStatus::Stopping => "stopping",
        BotJobStatus::Stopped => "stopped",
        BotJobStatus::Completed => "completed",
        BotJobStatus::Failed => "failed",
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
        bail!("bot job commands support only --output terminal|json|jsonl");
    }
    Ok(())
}
