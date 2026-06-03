use std::cmp::Reverse;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{Local, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::cli::{OutputFormat, ScriptRunsListArgs, ScriptRunsShowArgs};
use crate::scripting::telemetry::report_dir;

#[derive(Debug, Clone, Serialize)]
struct ScriptRunRecord {
    id: String,
    file: String,
    path: String,
    started_at_ms: u64,
    script_name: Option<String>,
    script_source: Option<String>,
    command: Option<String>,
    provider: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
    status: Option<String>,
    phase: Option<String>,
    progress_current: Option<u64>,
    progress_total: Option<u64>,
    duration_ms: Option<u64>,
    hooks_called: Option<u64>,
    hook_failures: Option<u64>,
    max_hook_duration_ms: Option<u64>,
    avg_hook_duration_ms: Option<f64>,
    max_heap_used_bytes: Option<u64>,
    error_message: Option<String>,
}

pub fn handle_list(args: ScriptRunsListArgs) -> Result<()> {
    args.validate()?;
    reject_unsupported_output(args.output)?;

    let mut records = read_records()?;
    records.sort_by_key(|record| Reverse(record.started_at_ms));

    if !args.all {
        records.truncate(args.limit);
    }

    match args.output {
        OutputFormat::Terminal => print_list_terminal(&records, args.all, args.limit),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&records)?),
        OutputFormat::Jsonl => {
            for record in records {
                println!("{}", serde_json::to_string(&record)?);
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }

    Ok(())
}

pub fn handle_show(args: ScriptRunsShowArgs) -> Result<()> {
    args.validate()?;
    reject_unsupported_output(args.output)?;

    let path = resolve_run_path(&args.run)?;
    let json = read_report_json(&path)?;

    match args.output {
        OutputFormat::Terminal => print_show_terminal(&path, &json),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&json)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&json)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }

    Ok(())
}

fn read_records() -> Result<Vec<ScriptRunRecord>> {
    let dir = report_dir()?;
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        match read_report_json(&path).and_then(|json| record_from_json(path.clone(), &json)) {
            Ok(record) => records.push(record),
            Err(err) => eprintln!(
                "warning: skipped script run report {}: {err}",
                path.display()
            ),
        }
    }

    Ok(records)
}

fn record_from_json(path: PathBuf, json: &Value) -> Result<ScriptRunRecord> {
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("report file name is not valid utf-8")?
        .to_string();
    let id = file.trim_end_matches(".json").to_string();
    let started_at_ms = json
        .get("started_at_ms")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| parse_start_ms_from_file(&file).unwrap_or(0));

    Ok(ScriptRunRecord {
        id,
        file,
        path: path.display().to_string(),
        started_at_ms,
        script_name: get_string(json, &["script", "name"]),
        script_source: get_string(json, &["script", "source"]),
        command: get_string(json, &["command"]),
        provider: get_string(json, &["provider"]),
        exchange: get_string(json, &["exchange"]),
        symbol: get_string(json, &["symbol"]),
        status: get_string(json, &["status"]),
        phase: get_string(json, &["phase"]),
        progress_current: get_u64(json, &["progress", "current"]),
        progress_total: get_u64(json, &["progress", "total"]),
        duration_ms: get_u64(json, &["duration_ms"]),
        hooks_called: get_u64(json, &["runtime", "hooks_called"]),
        hook_failures: get_u64(json, &["runtime", "hook_failures"]),
        max_hook_duration_ms: get_u64(json, &["runtime", "max_hook_duration_ms"]),
        avg_hook_duration_ms: get_f64(json, &["runtime", "avg_hook_duration_ms"]),
        max_heap_used_bytes: get_u64(json, &["runtime", "max_heap_used_bytes"]),
        error_message: get_string(json, &["error", "message"]),
    })
}

fn read_report_json(path: &Path) -> Result<Value> {
    let body =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("failed to parse {}", path.display()))
}

fn resolve_run_path(run: &str) -> Result<PathBuf> {
    let raw = PathBuf::from(run);
    if raw.exists() {
        return Ok(raw);
    }

    let dir = report_dir()?;
    let candidates = [dir.join(run), dir.join(format!("{run}.json"))];
    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    let records = read_records()?;
    let matches: Vec<_> = records
        .into_iter()
        .filter(|record| {
            record.id.starts_with(run)
                || record.file.starts_with(run)
                || record.started_at_ms.to_string().starts_with(run)
        })
        .collect();

    match matches.as_slice() {
        [record] => Ok(PathBuf::from(&record.path)),
        [] => bail!("script run not found: {run}"),
        _ => bail!("script run id is ambiguous: {run}"),
    }
}

fn print_list_terminal(records: &[ScriptRunRecord], all: bool, limit: usize) {
    if records.is_empty() {
        println!("no script runs found");
        println!("runs are written to ~/.market-lab/runs after script run/backtest");
        return;
    }

    let title = if all {
        format!("script runs (all {})", records.len())
    } else {
        format!("script runs (latest {})", records.len().min(limit))
    };
    println!("{title}");
    println!("{}", "-".repeat(title.len()));

    for (idx, record) in records.iter().enumerate() {
        println!(
            "{}. {}  {}  {}",
            idx + 1,
            record.id,
            record.status.as_deref().unwrap_or("unknown"),
            record.script_name.as_deref().unwrap_or("unknown-script")
        );
        println!(
            "   time={} duration={} phase={} progress={} hooks={} failures={} heap={}",
            format_ts_ms(record.started_at_ms),
            format_duration(record.duration_ms),
            record.phase.as_deref().unwrap_or("n/a"),
            format_progress(record.progress_current, record.progress_total),
            format_optional_u64(record.hooks_called),
            format_optional_u64(record.hook_failures),
            format_memory(record.max_heap_used_bytes)
        );
        println!(
            "   command={} market={} file={}",
            record.command.as_deref().unwrap_or("unknown"),
            market_label(record),
            record.file
        );
        if let Some(error) = &record.error_message {
            println!("   error={error}");
        }
    }

    if !all {
        println!();
        println!("show more: mlab script runs list --limit {}", limit + 5);
        println!("view run:  mlab script runs show <run-id>");
    }
}

fn print_show_terminal(path: &Path, json: &Value) {
    println!("script run");
    println!("----------");
    println!("file: {}", path.display());
    println!(
        "id: {}",
        path.file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
    );
    println!("status: {}", display_string(json, &["status"]));
    println!("phase: {}", display_string(json, &["phase"]));
    println!(
        "progress: {}",
        format_progress(
            get_u64(json, &["progress", "current"]),
            get_u64(json, &["progress", "total"])
        )
    );
    println!("command: {}", display_string(json, &["command"]));
    println!("script: {}", display_string(json, &["script", "name"]));
    println!("source: {}", display_string(json, &["script", "source"]));
    println!("provider: {}", display_string(json, &["provider"]));
    println!("market: {}", show_market_label(json));
    println!();
    println!("time");
    println!(
        "  started: {}",
        get_u64(json, &["started_at_ms"])
            .map(format_ts_ms)
            .unwrap_or_else(|| "n/a".to_string())
    );
    println!(
        "  ended:   {}",
        get_u64(json, &["ended_at_ms"])
            .map(format_ts_ms)
            .unwrap_or_else(|| "n/a".to_string())
    );
    println!(
        "  runtime: {}",
        format_duration(get_u64(json, &["duration_ms"]))
    );
    println!();
    println!("runtime");
    println!("  engine: {}", display_string(json, &["runtime", "engine"]));
    println!(
        "  hooks called: {}",
        display_u64(json, &["runtime", "hooks_called"])
    );
    println!(
        "  hook failures: {}",
        display_u64(json, &["runtime", "hook_failures"])
    );
    println!(
        "  max hook duration: {}",
        format_duration(get_u64(json, &["runtime", "max_hook_duration_ms"]))
    );
    println!(
        "  avg hook duration: {}",
        format_optional_f64_ms(get_f64(json, &["runtime", "avg_hook_duration_ms"]))
    );
    println!(
        "  max heap used: {}",
        format_memory(get_u64(json, &["runtime", "max_heap_used_bytes"]))
    );
    println!();
    println!("limits");
    println!(
        "  heap: {}",
        format_memory(get_u64(json, &["limits", "heap_bytes"]))
    );
    println!(
        "  stack: {}",
        format_memory(get_u64(json, &["limits", "stack_bytes"]))
    );
    println!(
        "  hook timeout: {}",
        format_duration(get_u64(json, &["limits", "hook_timeout_ms"]))
    );

    if let Some(error) = json.get("error").filter(|value| !value.is_null()) {
        println!();
        println!("error");
        println!(
            "  kind: {}",
            get_string(error, &["kind"]).unwrap_or_else(|| "unknown".to_string())
        );
        println!(
            "  message: {}",
            get_string(error, &["message"]).unwrap_or_else(|| "n/a".to_string())
        );
    }
}

fn reject_unsupported_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("script run history supports only --output terminal|json|jsonl");
    }
    Ok(())
}

fn market_label(record: &ScriptRunRecord) -> String {
    match (&record.exchange, &record.symbol) {
        (Some(exchange), Some(symbol)) => format!("{exchange}:{symbol}"),
        (Some(exchange), None) => exchange.clone(),
        (None, Some(symbol)) => symbol.clone(),
        (None, None) => "n/a".to_string(),
    }
}

fn show_market_label(json: &Value) -> String {
    match (
        get_string(json, &["exchange"]),
        get_string(json, &["symbol"]),
    ) {
        (Some(exchange), Some(symbol)) => format!("{exchange}:{symbol}"),
        (Some(exchange), None) => exchange,
        (None, Some(symbol)) => symbol,
        (None, None) => "n/a".to_string(),
    }
}

fn get_path<'a>(json: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut value = json;
    for key in path {
        value = value.get(*key)?;
    }
    Some(value)
}

fn get_string(json: &Value, path: &[&str]) -> Option<String> {
    get_path(json, path)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn get_u64(json: &Value, path: &[&str]) -> Option<u64> {
    get_path(json, path).and_then(Value::as_u64)
}

fn get_f64(json: &Value, path: &[&str]) -> Option<f64> {
    get_path(json, path).and_then(Value::as_f64)
}

fn display_string(json: &Value, path: &[&str]) -> String {
    get_string(json, path).unwrap_or_else(|| "n/a".to_string())
}

fn display_u64(json: &Value, path: &[&str]) -> String {
    get_u64(json, path)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn parse_start_ms_from_file(file: &str) -> Option<u64> {
    file.split('-').next()?.parse().ok()
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_f64_ms(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}ms"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_duration(value: Option<u64>) -> String {
    match value {
        Some(ms) if ms < 1_000 => format!("{ms}ms"),
        Some(ms) if ms < 60_000 => format!("{:.2}s", ms as f64 / 1_000.0),
        Some(ms) => format!("{:.2}m", ms as f64 / 60_000.0),
        None => "n/a".to_string(),
    }
}

fn format_progress(current: Option<u64>, total: Option<u64>) -> String {
    match (current, total) {
        (Some(current), Some(total)) if total > 0 => {
            let pct = (current as f64 / total as f64) * 100.0;
            format!("{current}/{total} ({pct:.1}%)")
        }
        (Some(current), Some(total)) => format!("{current}/{total}"),
        _ => "n/a".to_string(),
    }
}

fn format_memory(value: Option<u64>) -> String {
    match value {
        Some(bytes) if bytes < 1024 => format!("{bytes} B"),
        Some(bytes) if bytes < 1024 * 1024 => format!("{:.2} KiB", bytes as f64 / 1024.0),
        Some(bytes) => format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0)),
        None => "n/a".to_string(),
    }
}

fn format_ts_ms(ms: u64) -> String {
    chrono::DateTime::<Utc>::from_timestamp_millis(ms as i64)
        .map(|dt| {
            dt.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S%.3f %Z")
                .to_string()
        })
        .unwrap_or_else(|| "invalid-time".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_epoch_timestamp() {
        assert!(format_ts_ms(0).contains("1970-01-01"));
    }

    #[test]
    fn formats_memory_units() {
        assert_eq!(format_memory(Some(512)), "512 B");
        assert_eq!(format_memory(Some(2048)), "2.00 KiB");
        assert_eq!(format_memory(Some(2 * 1024 * 1024)), "2.00 MiB");
    }
}
