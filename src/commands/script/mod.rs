use serde::Serialize;
use serde_json::Value;

use crate::scripting::engine::Script;
use crate::scripting::telemetry::{
    ScriptReportScript, ScriptRuntimeReport, ScriptRuntimeReportBuilder, write_runtime_report,
};

pub mod backtest;
pub mod run;
pub mod runs;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ScriptInputs {
    #[serde(flatten)]
    pub(crate) values: Value,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ScriptDescriptor {
    pub(crate) name: String,
    pub(crate) sources: Vec<&'static str>,
}

pub(crate) fn report_script(script: &Script) -> ScriptReportScript {
    ScriptReportScript {
        name: script.manifest.name.clone(),
        path: script.path.display().to_string(),
        source: script.manifest.source_names(),
    }
}

pub(crate) fn write_report_best_effort(report: &ScriptRuntimeReport) {
    if let Err(err) = write_runtime_report(report) {
        eprintln!("warning: failed to write script runtime report: {err}");
    }
}

pub(crate) fn write_running_report_best_effort(report: &ScriptRuntimeReportBuilder) {
    write_report_best_effort(&report.snapshot_running());
}

pub(crate) fn report_builder(
    command: &'static str,
    script: &Script,
    provider: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
) -> ScriptRuntimeReportBuilder {
    ScriptRuntimeReportBuilder::start(command, report_script(script), provider, exchange, symbol)
}
