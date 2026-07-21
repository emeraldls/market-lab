use std::collections::{HashMap, HashSet};

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
    let mut terminal = TerminalLogState::default();
    let (mut cursor, values) = runtime::script_output_after(&args.job, 0)?;
    let start = values.len().saturating_sub(args.limit);
    render_log_values(&values[start..], args.output, &mut terminal)?;
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
                render_log_values(&values, args.output, &mut terminal)?;
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
            println!(
                "  duration:         {}",
                job.definition
                    .duration_seconds
                    .map_or_else(|| "forever".to_string(), |seconds| format!("{seconds}s"))
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

fn render_log_values(
    values: &[serde_json::Value],
    output: OutputFormat,
    terminal: &mut TerminalLogState,
) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(values)?),
        OutputFormat::Jsonl => {
            for value in values {
                println!("{}", serde_json::to_string(value)?);
            }
        }
        OutputFormat::Terminal => {
            for value in values {
                if let Some(line) = format_terminal_log(value, terminal) {
                    println!("{line}");
                }
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

#[derive(Default)]
struct TerminalLogState {
    orders: HashMap<String, TerminalOrder>,
    position_sizes: HashMap<String, u64>,
    terminal_orders: HashSet<String>,
}

#[derive(Default)]
struct TerminalOrder {
    side: String,
    price: Option<f64>,
    margin: Option<f64>,
    size: Option<f64>,
    leverage: Option<f64>,
    saw_fill: bool,
}

fn format_terminal_log(value: &serde_json::Value, state: &mut TerminalLogState) -> Option<String> {
    let record_type = value.get("type")?.as_str()?;
    if record_type == "script.execution.error" {
        let error = value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("script execution failed");
        if error.contains("BULK rejected order:") {
            return None;
        }
        return Some(format!(
            "[{}] ERROR {}",
            log_time(value.get("ts_ms").and_then(serde_json::Value::as_u64)),
            error
        ));
    }
    if record_type == "script.source.disconnected" {
        let error = value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("market-data stream disconnected");
        let retry_seconds = value
            .get("retrySeconds")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1);
        let cleanup = value
            .get("orderCleanupError")
            .and_then(serde_json::Value::as_str)
            .map_or_else(String::new, |error| {
                format!("; managed-order cleanup failed: {error}")
            });
        return Some(format!(
            "[{}] WARN market data disconnected: {error}; retrying in {retry_seconds}s{cleanup}",
            log_time(value.get("ts_ms").and_then(serde_json::Value::as_u64)),
        ));
    }
    if record_type == "script.source.reconnected" {
        return Some(format!(
            "[{}] market data reconnected",
            log_time(value.get("ts_ms").and_then(serde_json::Value::as_u64)),
        ));
    }
    if record_type == "script.run.failed" {
        let error = value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("script worker failed");
        return Some(format!(
            "[{}] ERROR script failed: {error}",
            log_time(value.get("ts_ms").and_then(serde_json::Value::as_u64)),
        ));
    }
    if record_type != "script.execution.event" {
        return Some(format!(
            "[{}] {record_type}",
            log_time(value.get("ts_ms").and_then(serde_json::Value::as_u64))
        ));
    }

    let event = value.get("event")?;
    let event_type = event.get("type")?.as_str()?;
    let order_id = event.get("orderId").and_then(serde_json::Value::as_str);
    let data = event.get("data").unwrap_or(&serde_json::Value::Null);
    let prefix = format!(
        "[{}]",
        log_time(event.get("tsMs").and_then(serde_json::Value::as_u64))
    );
    if let Some(order_id) = order_id {
        if state.terminal_orders.contains(order_id) {
            return None;
        }
        if event.get("terminal").and_then(serde_json::Value::as_bool) == Some(true) {
            state.terminal_orders.insert(order_id.to_string());
        }
    }

    match event_type {
        "order.pending" => {
            if let Some(order_id) = order_id {
                state.orders.insert(
                    order_id.to_string(),
                    TerminalOrder {
                        side: data
                            .get("side")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("order")
                            .to_ascii_uppercase(),
                        price: data
                            .pointer("/order/price")
                            .and_then(serde_json::Value::as_f64),
                        margin: data.get("margin").and_then(serde_json::Value::as_f64),
                        size: data.get("size").and_then(serde_json::Value::as_f64),
                        leverage: data.get("leverage").and_then(serde_json::Value::as_f64),
                        saw_fill: false,
                    },
                );
            }
            None
        }
        "order.submitted" | "order.accepted" => {
            let order = order_id.and_then(|id| state.orders.get(id));
            let lifecycle = if event_type == "order.submitted" {
                "submitted"
            } else {
                "resting"
            };
            Some(format!(
                "{prefix} {lifecycle} {}{}{}",
                order.map_or_else(
                    || event_side(event).unwrap_or("ORDER"),
                    |order| order.side.as_str()
                ),
                order.map_or_else(String::new, order_amount),
                order
                    .and_then(|order| order.price)
                    .map_or_else(String::new, |price| format!(" @ {}", number(price)))
            ))
        }
        "order.fill" => {
            let mut order = order_id.and_then(|id| state.orders.get_mut(id));
            if let Some(order) = order.as_deref_mut() {
                order.saw_fill = true;
            }
            let side = data
                .get("isBuy")
                .and_then(serde_json::Value::as_bool)
                .map_or_else(
                    || {
                        order
                            .as_deref()
                            .map_or("ORDER", |order| order.side.as_str())
                    },
                    |is_buy| if is_buy { "BUY" } else { "SELL" },
                );
            let size = data
                .get("size")
                .and_then(serde_json::Value::as_f64)
                .map_or_else(|| "?".to_string(), number);
            let price = data
                .get("price")
                .and_then(serde_json::Value::as_f64)
                .map_or_else(|| "?".to_string(), number);
            let liquidity = if data.get("maker").and_then(serde_json::Value::as_bool) == Some(true)
            {
                " maker"
            } else {
                " taker"
            };
            let fee = data
                .get("fee")
                .and_then(serde_json::Value::as_f64)
                .map_or_else(String::new, |fee| format!(" fee={}", number(fee)));
            Some(format!(
                "{prefix} fill {side} {size} @ {price}{liquidity}{fee}"
            ))
        }
        "order.filled" => {
            let order = order_id.and_then(|id| state.orders.remove(id));
            if order.as_ref().is_some_and(|order| order.saw_fill) {
                None
            } else {
                Some(format!(
                    "{prefix} filled {}",
                    order.as_ref().map_or_else(
                        || event_side(event).unwrap_or("ORDER"),
                        |order| order.side.as_str()
                    )
                ))
            }
        }
        "order.cancelled" => {
            let order = order_id.and_then(|id| state.orders.remove(id));
            let side = order.as_ref().map_or_else(
                || event_side(event).unwrap_or("ORDER"),
                |order| order.side.as_str(),
            );
            let price = order
                .as_ref()
                .and_then(|order| order.price)
                .map_or_else(String::new, |price| format!(" @ {}", number(price)));
            Some(format!("{prefix} cancelled {side}{price}"))
        }
        "order.rejected" | "order.terminal" => {
            let order = order_id.and_then(|id| state.orders.remove(id));
            let reason = data
                .get("error")
                .and_then(serde_json::Value::as_str)
                .or_else(|| event.get("status").and_then(serde_json::Value::as_str))
                .unwrap_or("rejected");
            Some(format!(
                "{prefix} rejected {}: {reason}",
                order.as_ref().map_or_else(
                    || event_side(event).unwrap_or("ORDER"),
                    |order| order.side.as_str()
                )
            ))
        }
        "order.cancel_failed" | "order.cancel_rejected" => Some(format!(
            "{prefix} cancel failed: {}",
            data.get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("venue rejected cancellation")
        )),
        "position.updated" => {
            let symbol = data
                .get("symbol")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("position");
            let size = data.get("size").and_then(serde_json::Value::as_f64)?;
            if state.position_sizes.get(symbol) == Some(&size.to_bits()) {
                return None;
            }
            state
                .position_sizes
                .insert(symbol.to_string(), size.to_bits());
            let side = if size > 0.0 { "LONG" } else { "SHORT" };
            let price = data
                .get("price")
                .and_then(serde_json::Value::as_f64)
                .map_or_else(String::new, |price| format!(" @ {}", number(price)));
            let pnl = data
                .get("unrealizedPnl")
                .and_then(serde_json::Value::as_f64)
                .map_or_else(String::new, |pnl| format!(" uPnL={}", number(pnl)));
            Some(format!(
                "{prefix} position {side} {} {symbol}{price}{pnl}",
                number(size.abs())
            ))
        }
        "position.closed" => {
            let symbol = data
                .get("symbol")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("position");
            state.position_sizes.remove(symbol);
            let pnl = data
                .get("realizedPnl")
                .and_then(serde_json::Value::as_f64)
                .map_or_else(String::new, |pnl| format!(" realized={}", number(pnl)));
            Some(format!("{prefix} position FLAT {symbol}{pnl}"))
        }
        "account.margin_updated" | "order.cancel_requested" | "order.updated" => None,
        _ => Some(format!("{prefix} {event_type}")),
    }
}

fn order_amount(order: &TerminalOrder) -> String {
    if let Some(size) = order.size {
        format!(" size={}", number(size))
    } else if let Some(margin) = order.margin {
        format!(
            " margin={} x{}",
            number(margin),
            number(order.leverage.unwrap_or(1.0))
        )
    } else {
        String::new()
    }
}

fn event_side(event: &serde_json::Value) -> Option<&'static str> {
    let key = event.get("key").and_then(serde_json::Value::as_str)?;
    if key.contains("buy") || key.contains("bid") {
        Some("BUY")
    } else if key.contains("sell") || key.contains("ask") {
        Some("SELL")
    } else {
        None
    }
}

fn log_time(ts_ms: Option<u64>) -> String {
    ts_ms
        .and_then(|ts| chrono::DateTime::<Utc>::from_timestamp_millis(ts as i64))
        .map_or_else(
            || "--:--:--".to_string(),
            |date_time| {
                date_time
                    .with_timezone(&Local)
                    .format("%H:%M:%S")
                    .to_string()
            },
        )
}

fn number(value: f64) -> String {
    let rendered = format!("{value:.8}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "type": "script.execution.event",
            "ts_ms": 1_780_000_000_000_u64,
            "event": event,
        })
    }

    #[test]
    fn terminal_logs_compact_order_lifecycle_events() {
        let mut state = TerminalLogState::default();
        let pending = event(serde_json::json!({
            "type": "order.pending",
            "tsMs": 1_780_000_000_000_u64,
            "orderId": "ord_1",
            "data": {
                "side": "sell",
                "margin": 100.0,
                "leverage": 5.0,
                "order": { "price": 65134.0 }
            }
        }));
        assert!(format_terminal_log(&pending, &mut state).is_none());

        let submitted = event(serde_json::json!({
            "type": "order.submitted",
            "tsMs": 1_780_000_000_001_u64,
            "orderId": "ord_1",
            "data": {}
        }));
        let line = format_terminal_log(&submitted, &mut state).expect("submitted line");
        assert!(line.contains("submitted SELL margin=100 x5 @ 65134"));

        let accepted = event(serde_json::json!({
            "type": "order.accepted",
            "tsMs": 1_780_000_000_002_u64,
            "orderId": "ord_1",
            "data": {}
        }));
        let line = format_terminal_log(&accepted, &mut state).expect("accepted line");
        assert!(line.contains("resting SELL margin=100 x5 @ 65134"));

        let fill = event(serde_json::json!({
            "type": "order.fill",
            "tsMs": 1_780_000_000_003_u64,
            "orderId": "ord_1",
            "data": {
                "isBuy": false,
                "size": 0.007676,
                "price": 65134.0,
                "maker": true,
                "fee": -0.099994
            }
        }));
        let line = format_terminal_log(&fill, &mut state).expect("fill line");
        assert!(line.contains("fill SELL 0.007676 @ 65134 maker fee=-0.099994"));

        let filled = event(serde_json::json!({
            "type": "order.filled",
            "tsMs": 1_780_000_000_004_u64,
            "orderId": "ord_1",
            "terminal": true,
            "data": {}
        }));
        assert!(format_terminal_log(&filled, &mut state).is_none());

        let late_resting = event(serde_json::json!({
            "type": "order.accepted",
            "tsMs": 1_780_000_000_005_u64,
            "orderId": "ord_1",
            "data": {}
        }));
        assert!(format_terminal_log(&late_resting, &mut state).is_none());
    }

    #[test]
    fn terminal_logs_hide_margin_noise_and_duplicate_positions() {
        let mut state = TerminalLogState::default();
        let margin = event(serde_json::json!({
            "type": "account.margin_updated",
            "tsMs": 1_780_000_000_000_u64,
            "data": { "totalBalance": 1000.0 }
        }));
        assert!(format_terminal_log(&margin, &mut state).is_none());

        let position = event(serde_json::json!({
            "type": "position.updated",
            "tsMs": 1_780_000_000_001_u64,
            "data": {
                "symbol": "BTC-USD",
                "size": -0.007676,
                "price": 65134.0,
                "unrealizedPnl": -0.1084235
            }
        }));
        let line = format_terminal_log(&position, &mut state).expect("position line");
        assert!(line.contains("position SHORT 0.007676 BTC-USD @ 65134"));
        assert!(format_terminal_log(&position, &mut state).is_none());
    }

    #[test]
    fn terminal_logs_show_one_named_rejection_without_duplicate_error() {
        let mut state = TerminalLogState::default();
        let dispatch_error = serde_json::json!({
            "type": "script.execution.error",
            "ts_ms": 1_780_000_000_000_u64,
            "error": "script order failed: BULK rejected order: rejectedCrossing"
        });
        assert!(format_terminal_log(&dispatch_error, &mut state).is_none());

        let rejected = event(serde_json::json!({
            "type": "order.rejected",
            "tsMs": 1_780_000_000_001_u64,
            "orderId": "ord_missing",
            "key": "mm-sell-1",
            "terminal": true,
            "data": {
                "error": "BULK rejected order: rejectedCrossing: {\"oid\":\"id\"}"
            }
        }));
        let line = format_terminal_log(&rejected, &mut state).expect("rejection line");
        assert!(line.contains("rejected SELL: BULK rejected order: rejectedCrossing"));
        assert!(format_terminal_log(&rejected, &mut state).is_none());
    }

    #[test]
    fn terminal_logs_explain_source_reconnects_and_worker_failures() {
        let mut state = TerminalLogState::default();
        let disconnected = serde_json::json!({
            "type": "script.source.disconnected",
            "ts_ms": 1_780_000_000_000_u64,
            "error": "connection reset by peer",
            "retrySeconds": 2
        });
        let line = format_terminal_log(&disconnected, &mut state).expect("disconnect line");
        assert!(line.contains("market data disconnected: connection reset by peer"));
        assert!(line.contains("retrying in 2s"));

        let reconnected = serde_json::json!({
            "type": "script.source.reconnected",
            "ts_ms": 1_780_000_001_000_u64
        });
        let line = format_terminal_log(&reconnected, &mut state).expect("reconnect line");
        assert!(line.contains("market data reconnected"));

        let failed = serde_json::json!({
            "type": "script.run.failed",
            "ts_ms": 1_780_000_002_000_u64,
            "error": "websocket retry exhausted"
        });
        let line = format_terminal_log(&failed, &mut state).expect("failure line");
        assert!(line.contains("ERROR script failed: websocket retry exhausted"));
    }
}
