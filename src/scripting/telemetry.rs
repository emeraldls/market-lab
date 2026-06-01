use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;

use super::limits::{ScriptRuntimeLimits, default_limits};

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScriptHookTelemetry {
    pub hooks_called: u64,
    pub hook_failures: u64,
    pub total_hook_duration_ms: u64,
    pub max_hook_duration_ms: u64,
    pub max_heap_used_bytes: Option<u64>,
}

impl ScriptHookTelemetry {
    pub fn record(&mut self, stats: &ScriptHookStats) {
        self.hooks_called += 1;
        self.total_hook_duration_ms += stats.duration_ms;
        self.max_hook_duration_ms = self.max_hook_duration_ms.max(stats.duration_ms);
        self.max_heap_used_bytes = max_option(self.max_heap_used_bytes, stats.heap_used_bytes);
    }

    pub fn record_failure(&mut self) {
        self.hook_failures += 1;
    }

    pub fn avg_hook_duration_ms(&self) -> Option<f64> {
        if self.hooks_called == 0 {
            None
        } else {
            Some(self.total_hook_duration_ms as f64 / self.hooks_called as f64)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScriptHookStats {
    pub duration_ms: u64,
    pub heap_used_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScriptRuntimeReport {
    pub r#type: &'static str,
    pub version: &'static str,
    pub script: ScriptReportScript,
    pub command: String,
    pub provider: Option<String>,
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
    pub duration_ms: u64,
    pub status: ScriptRuntimeStatus,
    pub limits: ScriptRuntimeLimits,
    pub runtime: ScriptRuntimeMetrics,
    pub error: Option<ScriptRuntimeError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScriptReportScript {
    pub name: String,
    pub path: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptRuntimeStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScriptRuntimeMetrics {
    pub engine: &'static str,
    pub hooks_called: u64,
    pub hook_failures: u64,
    pub max_hook_duration_ms: u64,
    pub avg_hook_duration_ms: Option<f64>,
    pub max_heap_used_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScriptRuntimeError {
    pub kind: &'static str,
    pub message: String,
}

pub struct ScriptRuntimeReportBuilder {
    command: String,
    script: ScriptReportScript,
    provider: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
    started_at_ms: u64,
    limits: ScriptRuntimeLimits,
    telemetry: ScriptHookTelemetry,
}

impl ScriptRuntimeReportBuilder {
    pub fn start(
        command: impl Into<String>,
        script: ScriptReportScript,
        provider: Option<String>,
        exchange: Option<String>,
        symbol: Option<String>,
    ) -> Self {
        Self {
            command: command.into(),
            script,
            provider,
            exchange,
            symbol,
            started_at_ms: now_ms(),
            limits: default_limits(),
            telemetry: ScriptHookTelemetry::default(),
        }
    }

    pub fn record_hook(&mut self, stats: &ScriptHookStats) {
        self.telemetry.record(stats);
    }

    pub fn record_hook_failure(&mut self) {
        self.telemetry.record_failure();
    }

    pub fn finish_ok(self) -> ScriptRuntimeReport {
        self.finish(ScriptRuntimeStatus::Ok, None)
    }

    pub fn finish_error(self, error: impl ToString) -> ScriptRuntimeReport {
        self.finish(
            ScriptRuntimeStatus::Error,
            Some(ScriptRuntimeError {
                kind: "runtime_error",
                message: error.to_string(),
            }),
        )
    }

    fn finish(
        self,
        status: ScriptRuntimeStatus,
        error: Option<ScriptRuntimeError>,
    ) -> ScriptRuntimeReport {
        let ended_at_ms = now_ms();
        ScriptRuntimeReport {
            r#type: "script.runtime.report",
            version: "1",
            script: self.script,
            command: self.command,
            provider: self.provider,
            exchange: self.exchange,
            symbol: self.symbol,
            started_at_ms: self.started_at_ms,
            ended_at_ms,
            duration_ms: ended_at_ms.saturating_sub(self.started_at_ms),
            status,
            limits: self.limits,
            runtime: ScriptRuntimeMetrics {
                engine: "quickjs",
                hooks_called: self.telemetry.hooks_called,
                hook_failures: self.telemetry.hook_failures,
                max_hook_duration_ms: self.telemetry.max_hook_duration_ms,
                avg_hook_duration_ms: self.telemetry.avg_hook_duration_ms(),
                max_heap_used_bytes: self.telemetry.max_heap_used_bytes,
            },
            error,
        }
    }
}

pub fn write_runtime_report(report: &ScriptRuntimeReport) -> Result<PathBuf> {
    let dir = report_dir()?;
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let file_name = format!(
        "{}-{}-{}.json",
        report.started_at_ms,
        sanitize(&report.command),
        sanitize(&report.script.name)
    );
    let path = dir.join(file_name);
    fs::write(&path, serde_json::to_vec_pretty(report)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

pub fn report_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is required to write script runtime reports")?;
    Ok(home.join(".market-lab").join("runs"))
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn max_option(lhs: Option<u64>, rhs: Option<u64>) -> Option<u64> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs.max(rhs)),
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_aggregates_hook_stats() {
        let mut telemetry = ScriptHookTelemetry::default();
        telemetry.record(&ScriptHookStats {
            duration_ms: 5,
            heap_used_bytes: Some(100),
        });
        telemetry.record(&ScriptHookStats {
            duration_ms: 9,
            heap_used_bytes: Some(80),
        });

        assert_eq!(telemetry.hooks_called, 2);
        assert_eq!(telemetry.max_hook_duration_ms, 9);
        assert_eq!(telemetry.avg_hook_duration_ms(), Some(7.0));
        assert_eq!(telemetry.max_heap_used_bytes, Some(100));
    }

    #[test]
    fn report_serializes_contract() {
        let builder = ScriptRuntimeReportBuilder::start(
            "script.backtest",
            ScriptReportScript {
                name: "x".to_string(),
                path: "x.js".to_string(),
                source: "candles".to_string(),
            },
            Some("mmt".to_string()),
            Some("bybitf".to_string()),
            Some("BTC/USDT".to_string()),
        );
        let report = builder.finish_ok();
        let value = serde_json::to_value(report).expect("serialize report");
        assert_eq!(value["type"], "script.runtime.report");
        assert_eq!(value["runtime"]["engine"], "quickjs");
    }
}
