use std::path::PathBuf;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::domain::execution::ExecutionVenue;

use super::execution::{ScriptOrderRef, ScriptTradeRequest};

pub const MAX_SCRIPT_SOURCE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptJobSubmission {
    pub script_name: String,
    pub original_path: String,
    pub source: String,
    pub providers: Vec<String>,
    pub exchanges: Vec<String>,
    pub symbol: String,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub params: Vec<String>,
    #[serde(default)]
    pub venue: Option<ExecutionVenue>,
    #[serde(default)]
    pub verbose: bool,
}

impl ScriptJobSubmission {
    pub fn validate(&self) -> Result<()> {
        if self.script_name.trim().is_empty() {
            bail!("script job name is required");
        }
        if self.original_path.trim().is_empty() {
            bail!("script job original path is required");
        }
        if self.source.trim().is_empty() {
            bail!("script job source is empty");
        }
        if self.source.len() > MAX_SCRIPT_SOURCE_BYTES {
            bail!("script source exceeds the 1 MiB job limit");
        }
        if self.symbol.trim().is_empty() {
            bail!("script job symbol is required");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptJobDefinition {
    pub script_name: String,
    pub original_path: String,
    pub snapshot_path: PathBuf,
    pub providers: Vec<String>,
    pub exchanges: Vec<String>,
    pub symbol: String,
    pub sources: Vec<String>,
    pub params: Vec<String>,
    pub venue: Option<ExecutionVenue>,
    pub verbose: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptJobStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Completed,
    Failed,
}

impl ScriptJobStatus {
    pub fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Stopping)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptJob {
    pub id: String,
    pub definition: ScriptJobDefinition,
    pub status: ScriptJobStatus,
    pub pid: Option<u32>,
    pub created_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub stopped_at_ms: Option<u64>,
    pub last_heartbeat_ms: Option<u64>,
    pub last_error: Option<String>,
    #[serde(default)]
    pub next_event_seq: u64,
    #[serde(default)]
    pub worker_event_cursor: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptExecutionEvent {
    pub seq: u64,
    pub job_id: String,
    pub ts_ms: u64,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<ExecutionVenue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub terminal: bool,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptManagedOrder {
    pub job_id: String,
    pub order: ScriptOrderRef,
    pub request: ScriptTradeRequest,
    pub symbol: String,
    pub venue: ExecutionVenue,
    pub status: String,
    pub venue_order_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub cancel_requested: bool,
}
