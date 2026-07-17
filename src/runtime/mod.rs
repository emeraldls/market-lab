use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::credentials;
use crate::domain::execution::{CancelPlan, ExecutionReceipt, ExecutionVenue, TradePlan};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::bulk::ws::BulkAccountStream;
use crate::scripting::execution::{
    ScriptCancelRequest, ScriptOrderRef, ScriptTradeRequest, local_order_id,
};
use crate::scripting::jobs::{
    ScriptExecutionEvent, ScriptJob, ScriptJobDefinition, ScriptJobStatus, ScriptJobSubmission,
    ScriptManagedOrder,
};

const RUNTIME_VERSION: u8 = 7;
const ACCOUNT_RECONNECT_MAX_SECS: u64 = 30;
const MAX_RUNTIME_REQUEST_BYTES: usize = 1024 * 1024 + 128 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackedOrder {
    pub venue: ExecutionVenue,
    pub account: String,
    pub internal_symbol: String,
    pub venue_symbol: String,
    pub order_id: String,
    pub status: String,
    pub registered_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub script_order_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuntimeStatus {
    #[serde(default)]
    pub version: u8,
    pub running: bool,
    pub pid: Option<u32>,
    pub started_at_ms: Option<u64>,
    #[serde(default)]
    pub account_stream_connected: bool,
    #[serde(default)]
    pub last_account_event_ms: Option<u64>,
    #[serde(default)]
    pub last_recovery_ms: Option<u64>,
    pub last_error: Option<String>,
    pub tracked_orders: Vec<TrackedOrder>,
    #[serde(default)]
    pub script_jobs: Vec<ScriptJob>,
}

impl RuntimeStatus {
    fn stopped() -> Self {
        Self {
            version: RUNTIME_VERSION,
            running: false,
            pid: None,
            started_at_ms: None,
            account_stream_connected: false,
            last_account_event_ms: None,
            last_recovery_ms: None,
            last_error: None,
            tracked_orders: Vec::new(),
            script_jobs: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RuntimeRequest {
    Ping,
    Status,
    Stop,
    TrackOrder {
        order: TrackedOrder,
    },
    ExecuteTrade {
        plan: TradePlan,
    },
    CancelOrder {
        plan: CancelPlan,
    },
    SubmitScriptJob {
        submission: ScriptJobSubmission,
    },
    ListScriptJobs,
    GetScriptJob {
        job_id: String,
    },
    StopScriptJob {
        job_id: String,
    },
    RestartScriptJob {
        job_id: String,
    },
    ScriptWorkerStarted {
        job_id: String,
        pid: u32,
    },
    ScriptWorkerHeartbeat {
        job_id: String,
        pid: u32,
    },
    ScriptWorkerFinished {
        job_id: String,
        pid: u32,
        error: Option<String>,
    },
    ScriptExecuteTrade {
        job_id: String,
        order: ScriptOrderRef,
        request: ScriptTradeRequest,
    },
    ScriptCancel {
        job_id: String,
        request: ScriptCancelRequest,
    },
    ScriptEvents {
        job_id: String,
        after_seq: u64,
        limit: usize,
    },
    AckScriptEvents {
        job_id: String,
        through_seq: u64,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeResponse {
    ok: bool,
    message: String,
    status: Option<RuntimeStatus>,
    #[serde(default)]
    receipt: Option<ExecutionReceipt>,
    #[serde(default)]
    job: Option<ScriptJob>,
    #[serde(default)]
    jobs: Option<Vec<ScriptJob>>,
    #[serde(default)]
    script_order: Option<ScriptManagedOrder>,
    #[serde(default)]
    script_events: Option<Vec<ScriptExecutionEvent>>,
}

impl RuntimeResponse {
    fn empty() -> Self {
        Self {
            ok: true,
            message: String::new(),
            status: None,
            receipt: None,
            job: None,
            jobs: None,
            script_order: None,
            script_events: None,
        }
    }

    fn error(message: impl Into<String>, state: &RuntimeState) -> Self {
        Self {
            ok: false,
            message: message.into(),
            status: Some(runtime_status(state)),
            ..Self::empty()
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeState {
    version: u8,
    pid: u32,
    started_at_ms: u64,
    #[serde(default)]
    account_stream_connected: bool,
    #[serde(default)]
    last_account_event_ms: Option<u64>,
    #[serde(default)]
    last_recovery_ms: Option<u64>,
    #[serde(default)]
    account_disconnected_at_ms: Option<u64>,
    #[serde(default)]
    last_error: Option<String>,
    tracked_orders: BTreeMap<String, TrackedOrder>,
    #[serde(default)]
    script_jobs: BTreeMap<String, ScriptJob>,
    #[serde(default)]
    script_orders: BTreeMap<String, ScriptManagedOrder>,
    #[serde(default)]
    script_cancel_keys: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct RuntimeEvent<'a> {
    ts_ms: u64,
    event: &'static str,
    order: &'a TrackedOrder,
}

#[derive(Serialize)]
struct TradeSubmissionEvent<'a> {
    ts_ms: u64,
    event: &'static str,
    plan: &'a TradePlan,
    receipt: &'a ExecutionReceipt,
}

#[derive(Serialize)]
struct CancelSubmissionEvent<'a> {
    ts_ms: u64,
    event: &'static str,
    plan: &'a CancelPlan,
    receipt: &'a ExecutionReceipt,
}

#[derive(Serialize)]
struct AccountRuntimeEvent<'a> {
    ts_ms: u64,
    event: &'static str,
    account: &'a str,
    data: &'a serde_json::Value,
}

enum AccountConnectionEvent {
    Connected {
        account: String,
        reconnected: bool,
    },
    Data {
        account: String,
        data: serde_json::Value,
    },
    Disconnected {
        account: String,
        error: String,
    },
}

struct RuntimePaths {
    directory: PathBuf,
    socket: PathBuf,
    state: PathBuf,
    events: PathBuf,
    log: PathBuf,
    jobs: PathBuf,
}

impl RuntimePaths {
    fn load() -> Result<Self> {
        let home =
            std::env::var_os("HOME").context("HOME is required for the Market Lab runtime")?;
        let directory = PathBuf::from(home).join(".market-lab").join("execution");
        Ok(Self {
            socket: directory.join("mlabd.sock"),
            state: directory.join("runtime.json"),
            events: directory.join("events.jsonl"),
            log: directory.join("mlabd.log"),
            jobs: directory.join("jobs"),
            directory,
        })
    }
}

pub async fn serve() -> Result<()> {
    let paths = RuntimePaths::load()?;
    secure_runtime_directory(&paths)?;
    if paths.socket.exists() {
        if UnixStream::connect(&paths.socket).await.is_ok() {
            bail!("mlabd is already running");
        }
        fs::remove_file(&paths.socket)
            .with_context(|| format!("failed to remove stale socket {}", paths.socket.display()))?;
    }

    let listener = UnixListener::bind(&paths.socket)
        .with_context(|| format!("failed to bind {}", paths.socket.display()))?;
    fs::set_permissions(&paths.socket, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", paths.socket.display()))?;
    let mut state = load_state(&paths)?.unwrap_or_else(|| RuntimeState {
        version: RUNTIME_VERSION,
        pid: std::process::id(),
        started_at_ms: now_ms().unwrap_or(0),
        account_stream_connected: false,
        last_account_event_ms: None,
        last_recovery_ms: None,
        account_disconnected_at_ms: None,
        last_error: None,
        tracked_orders: BTreeMap::new(),
        script_jobs: BTreeMap::new(),
        script_orders: BTreeMap::new(),
        script_cancel_keys: BTreeMap::new(),
    });
    state.version = RUNTIME_VERSION;
    state.pid = std::process::id();
    state.started_at_ms = now_ms()?;
    state.account_stream_connected = false;
    persist_state(&paths, &state)?;
    let adapter = BulkExecutionAdapter::new()?;
    let (account_tx, mut account_rx) = mpsc::channel(1024);
    let mut account_supervisors = HashSet::new();
    if let Ok(account) = credentials::bulk_account() {
        ensure_account_supervisor(&account, &account_tx, &mut account_supervisors);
    }
    let mut should_stop = false;
    while !should_stop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => should_stop = true,
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("mlabd failed to accept a local connection")?;
                match handle_connection(
                    stream,
                    &paths,
                    &adapter,
                    &mut state,
                    &account_tx,
                    &mut account_supervisors,
                ).await {
                    Ok(stop) => should_stop = stop,
                    Err(error) => record_runtime_error(
                        &paths,
                        &mut state,
                        format!("local runtime request failed: {error:#}"),
                    ),
                }
            }
            Some(event) = account_rx.recv() => {
                if let Err(error) = handle_account_connection_event(
                    event,
                    &paths,
                    &adapter,
                    &mut state,
                ).await {
                    record_runtime_error(
                        &paths,
                        &mut state,
                        format!("BULK account stream event failed: {error:#}"),
                    );
                }
            }
        }
    }

    let active_jobs = state
        .script_jobs
        .values()
        .filter(|job| job.status.is_active())
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    for job_id in active_jobs {
        let _ = stop_script_job_in_daemon(&paths, &mut state, &job_id);
    }
    drop(listener);
    let _ = fs::remove_file(&paths.socket);
    state.pid = 0;
    state.account_stream_connected = false;
    persist_state(&paths, &state)?;
    Ok(())
}

pub async fn ensure_running() -> Result<RuntimeStatus> {
    if let Some(status) = try_status().await? {
        if status.version == RUNTIME_VERSION {
            return Ok(status);
        }
        let _ = stop().await;
        for _ in 0..20 {
            if try_status().await?.is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    let paths = RuntimePaths::load()?;
    secure_runtime_directory(&paths)?;
    let daemon = daemon_binary()?;
    if !daemon.exists() {
        bail!(
            "mlabd was not found at {}; install/build both mlab and mlabd",
            daemon.display()
        );
    }
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log)
        .with_context(|| format!("failed to open {}", paths.log.display()))?;
    let log_err = log
        .try_clone()
        .context("failed to clone mlabd log handle")?;
    let mut command = Command::new(&daemon);
    command
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    command
        .spawn()
        .with_context(|| format!("failed to start {}", daemon.display()))?;

    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if let Some(status) = try_status().await? {
            return Ok(status);
        }
    }
    bail!(
        "mlabd did not become ready; inspect {}",
        paths.log.display()
    )
}

fn secure_runtime_directory(paths: &RuntimePaths) -> Result<()> {
    fs::create_dir_all(&paths.directory)
        .with_context(|| format!("failed to create {}", paths.directory.display()))?;
    fs::set_permissions(&paths.directory, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", paths.directory.display()))
}

pub async fn status() -> Result<RuntimeStatus> {
    Ok(try_status().await?.unwrap_or_else(RuntimeStatus::stopped))
}

pub async fn stop() -> Result<bool> {
    let Some(response) = try_request(RuntimeRequest::Stop).await? else {
        return Ok(false);
    };
    if !response.ok {
        bail!("mlabd refused to stop: {}", response.message);
    }
    Ok(true)
}

pub async fn track_receipt(plan: &TradePlan, receipt: &ExecutionReceipt) -> Result<()> {
    if receipt.terminal {
        return Ok(());
    }
    let order_id = receipt
        .order_id
        .as_deref()
        .context("non-terminal BULK receipt omitted its order id")?;
    ensure_running().await?;
    let order = TrackedOrder {
        venue: plan.venue,
        account: plan.account.clone(),
        internal_symbol: plan.internal_symbol.clone(),
        venue_symbol: plan.venue_symbol.clone(),
        order_id: order_id.to_string(),
        status: receipt.status.clone(),
        registered_at_ms: receipt.submitted_at_ms,
        updated_at_ms: receipt.submitted_at_ms,
        script_order_id: None,
    };
    let response = request(RuntimeRequest::TrackOrder { order }).await?;
    if !response.ok {
        bail!("mlabd did not accept order tracking: {}", response.message);
    }
    Ok(())
}

pub async fn submit_trade(plan: &TradePlan) -> Result<ExecutionReceipt> {
    ensure_running().await?;
    let response = request(RuntimeRequest::ExecuteTrade { plan: plan.clone() }).await?;
    if !response.ok {
        bail!("mlabd trade submission failed: {}", response.message);
    }
    response
        .receipt
        .context("mlabd trade response omitted its execution receipt")
}

pub async fn submit_cancel(plan: &CancelPlan) -> Result<ExecutionReceipt> {
    ensure_running().await?;
    let response = request(RuntimeRequest::CancelOrder { plan: plan.clone() }).await?;
    if !response.ok {
        bail!("mlabd cancellation failed: {}", response.message);
    }
    response
        .receipt
        .context("mlabd cancel response omitted its execution receipt")
}

pub async fn submit_script_job(submission: ScriptJobSubmission) -> Result<ScriptJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::SubmitScriptJob { submission }).await?;
    if !response.ok {
        bail!("mlabd rejected script job: {}", response.message);
    }
    response
        .job
        .context("mlabd omitted the submitted script job")
}

pub async fn list_script_jobs() -> Result<Vec<ScriptJob>> {
    ensure_running().await?;
    let response = request(RuntimeRequest::ListScriptJobs).await?;
    if !response.ok {
        bail!("mlabd could not list script jobs: {}", response.message);
    }
    response.jobs.context("mlabd omitted script jobs")
}

pub async fn get_script_job(job_id: &str) -> Result<ScriptJob> {
    ensure_running().await?;
    get_script_job_from_running_daemon(job_id).await
}

pub(crate) async fn get_script_job_from_running_daemon(job_id: &str) -> Result<ScriptJob> {
    let response = request(RuntimeRequest::GetScriptJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not get script job: {}", response.message);
    }
    response.job.context("mlabd omitted the script job")
}

pub async fn stop_script_job(job_id: &str) -> Result<ScriptJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::StopScriptJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not stop script job: {}", response.message);
    }
    response.job.context("mlabd omitted the stopped script job")
}

pub async fn restart_script_job(job_id: &str) -> Result<ScriptJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::RestartScriptJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not restart script job: {}", response.message);
    }
    response
        .job
        .context("mlabd omitted the restarted script job")
}

pub async fn script_worker_started(job_id: &str, pid: u32) -> Result<ScriptJob> {
    let response = request(RuntimeRequest::ScriptWorkerStarted {
        job_id: job_id.to_string(),
        pid,
    })
    .await?;
    if !response.ok {
        bail!("mlabd rejected script worker: {}", response.message);
    }
    response.job.context("mlabd omitted the script worker job")
}

pub async fn script_worker_heartbeat(job_id: &str, pid: u32) -> Result<ScriptJob> {
    let response = request(RuntimeRequest::ScriptWorkerHeartbeat {
        job_id: job_id.to_string(),
        pid,
    })
    .await?;
    if !response.ok {
        bail!(
            "mlabd rejected script worker heartbeat: {}",
            response.message
        );
    }
    response.job.context("mlabd omitted the script worker job")
}

pub async fn script_worker_finished(
    job_id: &str,
    pid: u32,
    error: Option<String>,
) -> Result<ScriptJob> {
    let response = request(RuntimeRequest::ScriptWorkerFinished {
        job_id: job_id.to_string(),
        pid,
        error,
    })
    .await?;
    if !response.ok {
        bail!("mlabd rejected script worker finish: {}", response.message);
    }
    response.job.context("mlabd omitted the script worker job")
}

pub async fn submit_script_trade(
    job_id: &str,
    order: ScriptOrderRef,
    request_value: ScriptTradeRequest,
) -> Result<ScriptManagedOrder> {
    let response = request(RuntimeRequest::ScriptExecuteTrade {
        job_id: job_id.to_string(),
        order,
        request: request_value,
    })
    .await?;
    if !response.ok {
        bail!("script trade failed: {}", response.message);
    }
    response
        .script_order
        .context("mlabd omitted the managed script order")
}

pub async fn submit_script_cancellation(
    job_id: &str,
    request_value: ScriptCancelRequest,
) -> Result<ScriptManagedOrder> {
    let response = request(RuntimeRequest::ScriptCancel {
        job_id: job_id.to_string(),
        request: request_value,
    })
    .await?;
    if !response.ok {
        bail!("script cancellation failed: {}", response.message);
    }
    response
        .script_order
        .context("mlabd omitted the managed script order")
}

pub async fn script_execution_events(
    job_id: &str,
    after_seq: u64,
    limit: usize,
) -> Result<Vec<ScriptExecutionEvent>> {
    let response = request(RuntimeRequest::ScriptEvents {
        job_id: job_id.to_string(),
        after_seq,
        limit,
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not read script events: {}", response.message);
    }
    response
        .script_events
        .context("mlabd omitted script execution events")
}

pub async fn acknowledge_script_events(job_id: &str, through_seq: u64) -> Result<()> {
    let response = request(RuntimeRequest::AckScriptEvents {
        job_id: job_id.to_string(),
        through_seq,
    })
    .await?;
    if !response.ok {
        bail!(
            "mlabd rejected script event acknowledgement: {}",
            response.message
        );
    }
    Ok(())
}

pub fn append_script_output(job_id: &str, value: &impl Serialize) -> Result<()> {
    let paths = RuntimePaths::load()?;
    let path = script_job_directory(&paths, job_id)?.join("output.jsonl");
    append_json_line(&path, value)
}

pub fn recent_script_output(job_id: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
    if limit == 0 {
        bail!("script log limit must be at least 1");
    }
    let paths = RuntimePaths::load()?;
    let path = script_job_directory(&paths, job_id)?.join("output.jsonl");
    let source = match fs::read_to_string(&path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let lines = source.lines().rev().take(limit).collect::<Vec<_>>();
    lines
        .into_iter()
        .rev()
        .map(|line| serde_json::from_str(line).context("script output journal is malformed"))
        .collect()
}

pub fn script_output_after(
    job_id: &str,
    after_line: usize,
) -> Result<(usize, Vec<serde_json::Value>)> {
    let paths = RuntimePaths::load()?;
    let path = script_job_directory(&paths, job_id)?.join("output.jsonl");
    let source = match fs::read_to_string(&path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((0, Vec::new()));
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let lines = source.lines().collect::<Vec<_>>();
    let total = lines.len();
    let values = lines
        .into_iter()
        .skip(after_line.min(total))
        .map(|line| serde_json::from_str(line).context("script output journal is malformed"))
        .collect::<Result<Vec<_>>>()?;
    Ok((total, values))
}

pub fn recent_events(limit: usize) -> Result<Vec<serde_json::Value>> {
    if limit == 0 {
        bail!("event limit must be at least 1");
    }
    let path = RuntimePaths::load()?.events;
    let source = match fs::read_to_string(&path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let lines = source.lines().rev().take(limit).collect::<Vec<_>>();
    lines
        .into_iter()
        .rev()
        .map(|line| serde_json::from_str(line).context("execution event journal is malformed"))
        .collect()
}

fn create_script_job(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    submission: ScriptJobSubmission,
) -> Result<ScriptJob> {
    submission.validate()?;
    if submission.venue == Some(ExecutionVenue::Bulk) {
        credentials::bulk_account()
            .context("BULK authentication is required when a script uses --venue bulk")?;
    }

    fs::create_dir_all(&paths.jobs)
        .with_context(|| format!("failed to create {}", paths.jobs.display()))?;
    fs::set_permissions(&paths.jobs, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", paths.jobs.display()))?;
    let job_id = new_script_job_id(state)?;
    let job_directory = paths.jobs.join(&job_id);
    fs::create_dir(&job_directory)
        .with_context(|| format!("failed to create {}", job_directory.display()))?;
    fs::set_permissions(&job_directory, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", job_directory.display()))?;
    let snapshot_path = job_directory.join("strategy.js");
    fs::write(&snapshot_path, submission.source.as_bytes())
        .with_context(|| format!("failed to write {}", snapshot_path.display()))?;
    fs::set_permissions(&snapshot_path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", snapshot_path.display()))?;

    let created_at_ms = now_ms()?;
    let definition = ScriptJobDefinition {
        script_name: submission.script_name,
        original_path: submission.original_path,
        snapshot_path,
        providers: submission.providers,
        exchanges: submission.exchanges,
        symbol: submission.symbol,
        sources: submission.sources,
        params: submission.params,
        venue: submission.venue,
        duration_seconds: submission.duration_seconds,
        verbose: submission.verbose,
    };
    let job = ScriptJob {
        id: job_id.clone(),
        definition,
        status: ScriptJobStatus::Starting,
        pid: None,
        created_at_ms,
        started_at_ms: None,
        stopped_at_ms: None,
        last_heartbeat_ms: None,
        last_error: None,
        next_event_seq: 0,
        worker_event_cursor: 0,
    };
    state.script_jobs.insert(job_id.clone(), job);
    persist_state(paths, state)?;
    if let Err(error) = spawn_script_worker(paths, state, &job_id) {
        if let Some(job) = state.script_jobs.get_mut(&job_id) {
            job.status = ScriptJobStatus::Failed;
            job.stopped_at_ms = Some(now_ms().unwrap_or(created_at_ms));
            job.last_error = Some(format!("{error:#}"));
        }
        persist_state(paths, state)?;
        return Err(error);
    }
    persist_state(paths, state)?;
    state
        .script_jobs
        .get(&job_id)
        .cloned()
        .context("script job disappeared after creation")
}

fn new_script_job_id(state: &RuntimeState) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    let base = format!("job_{:013x}_{:05x}", now.as_millis(), now.subsec_nanos());
    if !state.script_jobs.contains_key(&base) {
        return Ok(base);
    }
    for suffix in 1..=9999_u16 {
        let candidate = format!("{base}_{suffix}");
        if !state.script_jobs.contains_key(&candidate) {
            return Ok(candidate);
        }
    }
    bail!("could not allocate a unique script job id")
}

fn spawn_script_worker(paths: &RuntimePaths, state: &mut RuntimeState, job_id: &str) -> Result<()> {
    if !state.script_jobs.contains_key(job_id) {
        bail!("script job was not found");
    }
    let worker_log = paths.jobs.join(job_id).join("worker.log");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&worker_log)
        .with_context(|| format!("failed to open {}", worker_log.display()))?;
    let stderr = stdout
        .try_clone()
        .context("failed to clone script worker log handle")?;
    let executable = std::env::current_exe().context("failed to locate mlabd")?;
    let child = Command::new(executable)
        .arg("script-worker")
        .arg(job_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("failed to start script worker for {job_id}"))?;
    let job = state
        .script_jobs
        .get_mut(job_id)
        .context("script job disappeared while starting")?;
    job.status = ScriptJobStatus::Starting;
    job.pid = Some(child.id());
    job.stopped_at_ms = None;
    job.last_error = None;
    Ok(())
}

fn stop_script_job_in_daemon(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<ScriptJob> {
    let job = state
        .script_jobs
        .get_mut(job_id)
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if job.status.is_active()
        && let Some(pid) = job.pid
    {
        let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error).context("failed to stop script worker");
            }
        }
    }
    job.status = ScriptJobStatus::Stopped;
    job.pid = None;
    job.stopped_at_ms = Some(now_ms()?);
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn restart_script_job_in_daemon(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<ScriptJob> {
    if state
        .script_jobs
        .get(job_id)
        .is_some_and(|job| job.status.is_active())
    {
        stop_script_job_in_daemon(paths, state, job_id)?;
    }
    spawn_script_worker(paths, state, job_id)?;
    persist_state(paths, state)?;
    state
        .script_jobs
        .get(job_id)
        .cloned()
        .context("script job disappeared after restart")
}

fn mark_script_worker_started(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
) -> Result<ScriptJob> {
    let now = now_ms()?;
    let job = state
        .script_jobs
        .get_mut(job_id)
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if job.status == ScriptJobStatus::Stopped {
        bail!("script job `{job_id}` was stopped before its worker became ready");
    }
    job.status = ScriptJobStatus::Running;
    job.pid = Some(pid);
    job.started_at_ms = Some(now);
    job.last_heartbeat_ms = Some(now);
    job.last_error = None;
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_script_worker_heartbeat(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
) -> Result<ScriptJob> {
    let job = state
        .script_jobs
        .get_mut(job_id)
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if job.pid != Some(pid) || !job.status.is_active() {
        bail!("script worker is no longer active for job `{job_id}`");
    }
    job.last_heartbeat_ms = Some(now_ms()?);
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_script_worker_finished(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
    error: Option<String>,
) -> Result<ScriptJob> {
    let job = state
        .script_jobs
        .get_mut(job_id)
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if job.pid.is_some() && job.pid != Some(pid) {
        bail!("stale script worker attempted to finish job `{job_id}`");
    }
    if job.status != ScriptJobStatus::Stopped {
        job.status = if error.is_some() {
            ScriptJobStatus::Failed
        } else {
            ScriptJobStatus::Completed
        };
    }
    job.pid = None;
    job.stopped_at_ms = Some(now_ms()?);
    job.last_error = error;
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn script_job_directory(paths: &RuntimePaths, job_id: &str) -> Result<PathBuf> {
    if job_id.is_empty()
        || !job_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid script job id");
    }
    Ok(paths.jobs.join(job_id))
}

fn emit_script_event(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    event_type: impl Into<String>,
    order: Option<&ScriptManagedOrder>,
    terminal: bool,
    data: serde_json::Value,
) -> Result<ScriptExecutionEvent> {
    let job = state
        .script_jobs
        .get_mut(job_id)
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    job.next_event_seq = job.next_event_seq.saturating_add(1);
    let event = ScriptExecutionEvent {
        seq: job.next_event_seq,
        job_id: job_id.to_string(),
        ts_ms: now_ms()?,
        event_type: event_type.into(),
        order_id: order.map(|order| order.order.id.clone()),
        key: order.map(|order| order.order.key.clone()),
        venue: order.map(|order| order.venue),
        venue_order_id: order.and_then(|order| order.venue_order_id.clone()),
        status: order.map(|order| order.status.clone()),
        terminal,
        data,
    };
    let path = script_job_directory(paths, job_id)?.join("events.jsonl");
    append_json_line(&path, &event)?;
    Ok(event)
}

fn read_script_events(
    paths: &RuntimePaths,
    job_id: &str,
    after_seq: u64,
    limit: usize,
) -> Result<Vec<ScriptExecutionEvent>> {
    if limit == 0 || limit > 1000 {
        bail!("script event limit must be between 1 and 1000");
    }
    let path = script_job_directory(paths, job_id)?.join("events.jsonl");
    let source = match fs::read_to_string(&path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    source
        .lines()
        .filter_map(
            |line| match serde_json::from_str::<ScriptExecutionEvent>(line) {
                Ok(event) if event.seq > after_seq => Some(Ok(event)),
                Ok(_) => None,
                Err(error) => Some(Err(error).context("script event journal is malformed")),
            },
        )
        .take(limit)
        .collect()
}

fn acknowledge_script_events_in_daemon(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    through_seq: u64,
) -> Result<ScriptJob> {
    let job = state
        .script_jobs
        .get_mut(job_id)
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if through_seq > job.next_event_seq {
        bail!(
            "cannot acknowledge script event {through_seq}; latest is {}",
            job.next_event_seq
        );
    }
    job.worker_event_cursor = job.worker_event_cursor.max(through_seq);
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

struct ScriptTradeOperation<'a> {
    job_id: &'a str,
    order: ScriptOrderRef,
    request: ScriptTradeRequest,
}

async fn execute_script_trade(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    account_tx: &mpsc::Sender<AccountConnectionEvent>,
    account_supervisors: &mut HashSet<String>,
    operation: ScriptTradeOperation<'_>,
) -> Result<ScriptManagedOrder> {
    let ScriptTradeOperation {
        job_id,
        order,
        request,
    } = operation;
    request.validate()?;
    let job = state
        .script_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("script job `{job_id}` is not running");
    }
    let venue = job
        .definition
        .venue
        .context("script execution is disabled; deploy the script with --venue")?;
    if venue != ExecutionVenue::Bulk {
        bail!("unsupported script execution venue");
    }
    let expected_id = local_order_id(job_id, &request.key);
    if order.id != expected_id || order.key != request.key {
        bail!("script order reference does not match its job and idempotency key");
    }
    if let Some(existing) = state.script_orders.get(&order.id) {
        if existing.job_id == job_id && existing.request == request {
            return Ok(existing.clone());
        }
        bail!(
            "ctx.trade key `{}` was already used with different order parameters",
            request.key
        );
    }

    let created_at_ms = now_ms()?;
    let pending = ScriptManagedOrder {
        job_id: job_id.to_string(),
        order: order.clone(),
        request: request.clone(),
        symbol: job.definition.symbol.clone(),
        venue,
        status: "pending".to_string(),
        venue_order_id: None,
        created_at_ms,
        updated_at_ms: created_at_ms,
        cancel_requested: false,
    };
    state
        .script_orders
        .insert(order.id.clone(), pending.clone());
    emit_script_event(
        paths,
        state,
        job_id,
        "order.pending",
        Some(&pending),
        false,
        serde_json::to_value(&request)?,
    )?;
    persist_state(paths, state)?;

    let order_kind = match request.order.kind {
        crate::scripting::execution::ScriptOrderKind::Market => crate::cli::TradeOrderKind::Market,
        crate::scripting::execution::ScriptOrderKind::Limit => crate::cli::TradeOrderKind::Limit,
    };
    let tif = match request.order.tif {
        crate::scripting::execution::ScriptTimeInForce::Gtc => crate::cli::TradeTimeInForce::Gtc,
        crate::scripting::execution::ScriptTimeInForce::Ioc => crate::cli::TradeTimeInForce::Ioc,
        crate::scripting::execution::ScriptTimeInForce::Alo => crate::cli::TradeTimeInForce::Alo,
    };
    let args = crate::cli::TradeArgs {
        symbol: job.definition.symbol.clone(),
        config: None,
        venue: crate::cli::ExecutionVenueArg::Bulk,
        size: request.size,
        notional: request.notional,
        order_kind,
        price: request.order.price,
        tif,
        leverage: request.leverage,
        reduce_only: request.reduce_only,
        sl: request.sl,
        tp: request.tp,
        dry_run: false,
        yes: true,
        output: crate::cli::OutputFormat::Json,
    };
    let plan = match crate::commands::execution::build_trade_plan(&args, request.side).await {
        Ok(plan) => plan,
        Err(error) => {
            fail_script_order(paths, state, job_id, &order.id, &error)?;
            return Err(error);
        }
    };
    ensure_account_supervisor(&plan.account, account_tx, account_supervisors);
    let receipt = match execute_trade(paths, adapter, state, &plan, Some(order.id.clone())).await {
        Ok(receipt) => receipt,
        Err(error) => {
            fail_script_order(paths, state, job_id, &order.id, &error)?;
            return Err(error);
        }
    };
    let managed = {
        let managed = state
            .script_orders
            .get_mut(&order.id)
            .context("script order disappeared during submission")?;
        managed.status = receipt.status.clone();
        managed.venue_order_id = receipt.order_id.clone();
        managed.updated_at_ms = receipt.submitted_at_ms;
        managed.clone()
    };
    emit_script_event(
        paths,
        state,
        job_id,
        if receipt.terminal {
            "order.terminal"
        } else {
            "order.accepted"
        },
        Some(&managed),
        receipt.terminal,
        serde_json::to_value(&receipt)?,
    )?;
    persist_state(paths, state)?;
    Ok(managed)
}

fn fail_script_order(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    order_id: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let managed = {
        let managed = state
            .script_orders
            .get_mut(order_id)
            .context("script order disappeared while recording its failure")?;
        managed.status = "rejected".to_string();
        managed.updated_at_ms = now_ms()?;
        managed.clone()
    };
    emit_script_event(
        paths,
        state,
        job_id,
        "order.rejected",
        Some(&managed),
        true,
        serde_json::json!({ "error": format!("{error:#}") }),
    )?;
    persist_state(paths, state)
}

async fn execute_script_cancel(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    request: ScriptCancelRequest,
) -> Result<ScriptManagedOrder> {
    request.validate()?;
    let job = state
        .script_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("script job `{job_id}` is not running");
    }
    let venue = job
        .definition
        .venue
        .context("script execution is disabled; deploy the script with --venue")?;
    let cancel_key = format!("{job_id}:{}", request.key);
    if let Some(order_id) = state.script_cancel_keys.get(&cancel_key) {
        return state
            .script_orders
            .get(order_id)
            .cloned()
            .context("idempotent cancellation refers to a missing script order");
    }
    let order_id = state
        .script_orders
        .values()
        .find(|managed| {
            managed.job_id == job_id
                && (managed.order.id == request.order || managed.order.key == request.order)
        })
        .map(|managed| managed.order.id.clone())
        .with_context(|| {
            format!(
                "script order `{}` was not found in job `{job_id}`",
                request.order
            )
        })?;
    state
        .script_cancel_keys
        .insert(cancel_key, order_id.clone());

    let current = state
        .script_orders
        .get(&order_id)
        .cloned()
        .context("script order disappeared before cancellation")?;
    if is_terminal_order_status(&current.status) || current.status == "rejected" {
        persist_state(paths, state)?;
        return Ok(current);
    }
    let Some(venue_order_id) = current.venue_order_id.clone() else {
        let managed = {
            let managed = state
                .script_orders
                .get_mut(&order_id)
                .context("script order disappeared before deferred cancellation")?;
            managed.cancel_requested = true;
            managed.updated_at_ms = now_ms()?;
            managed.clone()
        };
        emit_script_event(
            paths,
            state,
            job_id,
            "order.cancel_requested",
            Some(&managed),
            false,
            serde_json::Value::Null,
        )?;
        persist_state(paths, state)?;
        return Ok(managed);
    };
    let market = crate::providers::bulk::catalog::market(&current.symbol)?;
    let plan = CancelPlan {
        created_at_ms: now_ms()?,
        venue,
        account: credentials::bulk_account()?,
        internal_symbol: market.internal_symbol.clone(),
        venue_symbol: market.symbol.clone(),
        order_id: venue_order_id,
    };
    let receipt = match execute_cancel(paths, adapter, state, &plan).await {
        Ok(receipt) => receipt,
        Err(error) => {
            let managed = state
                .script_orders
                .get(&order_id)
                .cloned()
                .context("script order disappeared after failed cancellation")?;
            emit_script_event(
                paths,
                state,
                job_id,
                "order.cancel_failed",
                Some(&managed),
                false,
                serde_json::json!({ "error": format!("{error:#}") }),
            )?;
            persist_state(paths, state)?;
            return Err(error);
        }
    };
    let managed = {
        let managed = state
            .script_orders
            .get_mut(&order_id)
            .context("script order disappeared after cancellation")?;
        managed.status = receipt.status.clone();
        managed.updated_at_ms = receipt.submitted_at_ms;
        managed.clone()
    };
    emit_script_event(
        paths,
        state,
        job_id,
        "order.cancelled",
        Some(&managed),
        true,
        serde_json::to_value(&receipt)?,
    )?;
    persist_state(paths, state)?;
    Ok(managed)
}

async fn handle_connection(
    stream: UnixStream,
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    account_tx: &mpsc::Sender<AccountConnectionEvent>,
    account_supervisors: &mut HashSet<String>,
) -> Result<bool> {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .await
        .context("failed to read mlabd request")?;
    if line.len() > MAX_RUNTIME_REQUEST_BYTES {
        bail!("mlabd request exceeds the runtime request limit");
    }
    let request: RuntimeRequest = serde_json::from_str(&line).context("invalid mlabd request")?;
    let should_stop = matches!(request, RuntimeRequest::Stop);
    let response = match request {
        RuntimeRequest::Ping => RuntimeResponse {
            ok: true,
            message: "pong".to_string(),
            status: None,
            receipt: None,
            ..RuntimeResponse::empty()
        },
        RuntimeRequest::Status => RuntimeResponse {
            ok: true,
            message: "running".to_string(),
            status: Some(runtime_status(state)),
            receipt: None,
            ..RuntimeResponse::empty()
        },
        RuntimeRequest::Stop => RuntimeResponse {
            ok: true,
            message: "stopping".to_string(),
            status: Some(runtime_status(state)),
            receipt: None,
            ..RuntimeResponse::empty()
        },
        RuntimeRequest::TrackOrder { order } => {
            append_runtime_event(paths, "order_tracking_started", &order)?;
            state.tracked_orders.insert(order.order_id.clone(), order);
            persist_state(paths, state)?;
            RuntimeResponse {
                ok: true,
                message: "order tracking registered".to_string(),
                status: Some(runtime_status(state)),
                receipt: None,
                ..RuntimeResponse::empty()
            }
        }
        RuntimeRequest::ExecuteTrade { plan } => {
            ensure_account_supervisor(&plan.account, account_tx, account_supervisors);
            match execute_trade(paths, adapter, state, &plan, None).await {
                Ok(receipt) => RuntimeResponse {
                    ok: true,
                    message: "order submitted".to_string(),
                    status: Some(runtime_status(state)),
                    receipt: Some(receipt),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse {
                    ok: false,
                    message: format!("{error:#}"),
                    status: Some(runtime_status(state)),
                    receipt: None,
                    ..RuntimeResponse::empty()
                },
            }
        }
        RuntimeRequest::CancelOrder { plan } => {
            ensure_account_supervisor(&plan.account, account_tx, account_supervisors);
            match execute_cancel(paths, adapter, state, &plan).await {
                Ok(receipt) => RuntimeResponse {
                    ok: true,
                    message: "cancellation submitted".to_string(),
                    status: Some(runtime_status(state)),
                    receipt: Some(receipt),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse {
                    ok: false,
                    message: format!("{error:#}"),
                    status: Some(runtime_status(state)),
                    receipt: None,
                    ..RuntimeResponse::empty()
                },
            }
        }
        RuntimeRequest::SubmitScriptJob { submission } => {
            if submission.venue == Some(ExecutionVenue::Bulk)
                && let Ok(account) = credentials::bulk_account()
            {
                ensure_account_supervisor(&account, account_tx, account_supervisors);
            }
            match create_script_job(paths, state, submission) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "script job submitted".to_string(),
                    status: Some(runtime_status(state)),
                    job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ListScriptJobs => RuntimeResponse {
            ok: true,
            message: "script jobs".to_string(),
            status: Some(runtime_status(state)),
            jobs: Some(state.script_jobs.values().cloned().collect()),
            ..RuntimeResponse::empty()
        },
        RuntimeRequest::GetScriptJob { job_id } => match state.script_jobs.get(&job_id).cloned() {
            Some(job) => RuntimeResponse {
                ok: true,
                message: "script job".to_string(),
                status: Some(runtime_status(state)),
                job: Some(job),
                ..RuntimeResponse::empty()
            },
            None => RuntimeResponse::error(format!("script job `{job_id}` was not found"), state),
        },
        RuntimeRequest::StopScriptJob { job_id } => {
            match stop_script_job_in_daemon(paths, state, &job_id) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "script job stopped".to_string(),
                    status: Some(runtime_status(state)),
                    job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::RestartScriptJob { job_id } => {
            match restart_script_job_in_daemon(paths, state, &job_id) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "script job restarted".to_string(),
                    status: Some(runtime_status(state)),
                    job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ScriptWorkerStarted { job_id, pid } => {
            match mark_script_worker_started(paths, state, &job_id, pid) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "script worker running".to_string(),
                    status: Some(runtime_status(state)),
                    job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ScriptWorkerHeartbeat { job_id, pid } => {
            match mark_script_worker_heartbeat(paths, state, &job_id, pid) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "script worker heartbeat".to_string(),
                    status: Some(runtime_status(state)),
                    job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ScriptWorkerFinished { job_id, pid, error } => {
            match mark_script_worker_finished(paths, state, &job_id, pid, error) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "script worker finished".to_string(),
                    status: Some(runtime_status(state)),
                    job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ScriptExecuteTrade {
            job_id,
            order,
            request,
        } => match execute_script_trade(
            paths,
            adapter,
            state,
            account_tx,
            account_supervisors,
            ScriptTradeOperation {
                job_id: &job_id,
                order,
                request,
            },
        )
        .await
        {
            Ok(script_order) => RuntimeResponse {
                ok: true,
                message: "script order processed".to_string(),
                status: Some(runtime_status(state)),
                script_order: Some(script_order),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
        },
        RuntimeRequest::ScriptCancel { job_id, request } => {
            match execute_script_cancel(paths, adapter, state, &job_id, request).await {
                Ok(script_order) => RuntimeResponse {
                    ok: true,
                    message: "script cancellation processed".to_string(),
                    status: Some(runtime_status(state)),
                    script_order: Some(script_order),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ScriptEvents {
            job_id,
            after_seq,
            limit,
        } => match read_script_events(paths, &job_id, after_seq, limit) {
            Ok(events) => RuntimeResponse {
                ok: true,
                message: "script execution events".to_string(),
                status: Some(runtime_status(state)),
                script_events: Some(events),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
        },
        RuntimeRequest::AckScriptEvents {
            job_id,
            through_seq,
        } => match acknowledge_script_events_in_daemon(paths, state, &job_id, through_seq) {
            Ok(job) => RuntimeResponse {
                ok: true,
                message: "script events acknowledged".to_string(),
                status: Some(runtime_status(state)),
                job: Some(job),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
        },
    };
    let mut encoded = serde_json::to_vec(&response).context("failed to encode mlabd response")?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .context("failed to write mlabd response")?;
    writer.shutdown().await.ok();
    Ok(should_stop)
}

async fn execute_trade(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    plan: &TradePlan,
    script_order_id: Option<String>,
) -> Result<ExecutionReceipt> {
    let receipt = adapter
        .submit_trade(credentials::active_bulk_credential()?, plan)
        .await?;
    if let Err(error) = append_json_line(
        &paths.events,
        &TradeSubmissionEvent {
            ts_ms: now_ms()?,
            event: "order_submitted",
            plan,
            receipt: &receipt,
        },
    ) {
        eprintln!("execution journal warning: {error:#}");
    }
    if !receipt.terminal {
        let order_id = receipt
            .order_id
            .as_deref()
            .context("non-terminal BULK receipt omitted its order id")?;
        let order = TrackedOrder {
            venue: plan.venue,
            account: plan.account.clone(),
            internal_symbol: plan.internal_symbol.clone(),
            venue_symbol: plan.venue_symbol.clone(),
            order_id: order_id.to_string(),
            status: receipt.status.clone(),
            registered_at_ms: receipt.submitted_at_ms,
            updated_at_ms: receipt.submitted_at_ms,
            script_order_id,
        };
        if let Err(error) = append_runtime_event(paths, "order_tracking_started", &order) {
            eprintln!("execution journal warning: {error:#}");
        }
        state.tracked_orders.insert(order.order_id.clone(), order);
        persist_state(paths, state)?;
    }
    Ok(receipt)
}

async fn execute_cancel(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    plan: &CancelPlan,
) -> Result<ExecutionReceipt> {
    if plan.venue != ExecutionVenue::Bulk {
        bail!("BULK runtime received a cancel plan for another venue");
    }
    let market = crate::providers::bulk::catalog::market(&plan.internal_symbol)?;
    if market.symbol != plan.venue_symbol {
        bail!("cancel plan symbol mapping does not match the embedded BULK catalog");
    }
    let credential = credentials::active_bulk_credential()?;
    if credential.account.to_base58() != plan.account {
        bail!("cancel plan account no longer matches the configured BULK account");
    }
    let receipt = adapter
        .cancel_order(credential, &plan.venue_symbol, &plan.order_id)
        .await?;
    if let Err(error) = append_json_line(
        &paths.events,
        &CancelSubmissionEvent {
            ts_ms: now_ms()?,
            event: "order_cancelled",
            plan,
            receipt: &receipt,
        },
    ) {
        eprintln!("execution journal warning: {error:#}");
    }
    if state.tracked_orders.remove(&plan.order_id).is_some() {
        persist_state(paths, state)?;
    }
    Ok(receipt)
}

fn ensure_account_supervisor(
    account: &str,
    sender: &mpsc::Sender<AccountConnectionEvent>,
    supervisors: &mut HashSet<String>,
) {
    if !supervisors.insert(account.to_string()) {
        return;
    }
    let account = account.to_string();
    let sender = sender.clone();
    tokio::spawn(async move {
        supervise_account_stream(account, sender).await;
    });
}

async fn supervise_account_stream(account: String, sender: mpsc::Sender<AccountConnectionEvent>) {
    let mut connected_once = false;
    let mut reconnect_delay_secs = 1_u64;
    loop {
        match BulkAccountStream::connect(&account).await {
            Ok(mut stream) => {
                if sender
                    .send(AccountConnectionEvent::Connected {
                        account: account.clone(),
                        reconnected: connected_once,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                connected_once = true;
                reconnect_delay_secs = 1;
                loop {
                    match stream.next_event().await {
                        Ok(data) => {
                            if sender
                                .send(AccountConnectionEvent::Data {
                                    account: account.clone(),
                                    data,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(error) => {
                            if sender
                                .send(AccountConnectionEvent::Disconnected {
                                    account: account.clone(),
                                    error: format!("{error:#}"),
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                if sender
                    .send(AccountConnectionEvent::Disconnected {
                        account: account.clone(),
                        error: format!("{error:#}"),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay_secs)).await;
        reconnect_delay_secs = (reconnect_delay_secs * 2).min(ACCOUNT_RECONNECT_MAX_SECS);
    }
}

async fn handle_account_connection_event(
    event: AccountConnectionEvent,
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
) -> Result<()> {
    match event {
        AccountConnectionEvent::Connected {
            account,
            reconnected,
        } => {
            state.account_stream_connected = true;
            state.last_error = None;
            persist_state(paths, state)?;
            if reconnected {
                recover_account_gap(paths, adapter, state, &account).await?;
            }
        }
        AccountConnectionEvent::Disconnected { account, error } => {
            state.account_stream_connected = false;
            state.account_disconnected_at_ms = Some(now_ms()?);
            record_runtime_error(
                paths,
                state,
                format!("BULK account WebSocket disconnected for {account}: {error}"),
            );
        }
        AccountConnectionEvent::Data { account, data } => {
            let received_at_ms = now_ms()?;
            state.account_stream_connected = true;
            state.last_account_event_ms = Some(received_at_ms);
            append_json_line(
                &paths.events,
                &AccountRuntimeEvent {
                    ts_ms: received_at_ms,
                    event: "account_ws",
                    account: &account,
                    data: &data,
                },
            )?;
            apply_account_event(paths, state, &account, &data, received_at_ms)?;
            persist_state(paths, state)?;
        }
    }
    Ok(())
}

fn apply_account_event(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    account: &str,
    data: &serde_json::Value,
    received_at_ms: u64,
) -> Result<()> {
    match data.get("type").and_then(serde_json::Value::as_str) {
        Some("accountSnapshot") => {
            if let Some(open_orders) = data.get("openOrders").and_then(serde_json::Value::as_array)
            {
                for order in open_orders {
                    let Some(order_id) = order.get("orderId").and_then(serde_json::Value::as_str)
                    else {
                        continue;
                    };
                    let status = order
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("resting");
                    apply_tracked_order_status(
                        paths,
                        state,
                        account,
                        order_id,
                        status,
                        received_at_ms,
                    )?;
                    apply_script_order_status(
                        paths,
                        state,
                        order_id,
                        status,
                        received_at_ms,
                        order.clone(),
                    )?;
                }
            }
        }
        Some("orderUpdate") => {
            let order_id = data
                .get("oid")
                .and_then(serde_json::Value::as_str)
                .context("BULK orderUpdate omitted oid")?;
            let status = data
                .get("status")
                .and_then(serde_json::Value::as_str)
                .context("BULK orderUpdate omitted status")?;
            let event_ms = data
                .get("ts")
                .and_then(serde_json::Value::as_u64)
                .map(crate::providers::bulk::market_data::normalize_timestamp_ms)
                .unwrap_or(received_at_ms);
            apply_tracked_order_status(paths, state, account, order_id, status, event_ms)?;
            apply_script_order_status(paths, state, order_id, status, event_ms, data.clone())?;
        }
        _ => {}
    }
    route_account_event_to_scripts(paths, state, data)?;
    Ok(())
}

fn apply_script_order_status(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    venue_order_id: &str,
    status: &str,
    event_ms: u64,
    data: serde_json::Value,
) -> Result<()> {
    let local_ids = state
        .script_orders
        .iter()
        .filter(|(_, order)| order.venue_order_id.as_deref() == Some(venue_order_id))
        .map(|(local_id, _)| local_id.clone())
        .collect::<Vec<_>>();
    for local_id in local_ids {
        let (job_id, managed, changed) = {
            let managed = state
                .script_orders
                .get_mut(&local_id)
                .context("script order disappeared while applying account event")?;
            let changed = managed.status != status;
            managed.status = status.to_string();
            managed.updated_at_ms = event_ms;
            (managed.job_id.clone(), managed.clone(), changed)
        };
        if changed {
            let event_type = if status == "filled" || status == "partiallyFilled" {
                "order.filled"
            } else if status.starts_with("cancelled") || status == "siblingCancelled" {
                "order.cancelled"
            } else if status.starts_with("rejected") || status == "triggerFailed" {
                "order.rejected"
            } else {
                "order.updated"
            };
            emit_script_event(
                paths,
                state,
                &job_id,
                event_type,
                Some(&managed),
                is_terminal_order_status(status),
                data.clone(),
            )?;
        }
    }
    Ok(())
}

fn route_account_event_to_scripts(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    data: &serde_json::Value,
) -> Result<()> {
    let Some(kind) = data.get("type").and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    if matches!(kind, "accountSnapshot" | "orderUpdate") {
        return Ok(());
    }

    let venue_order_id = data
        .get("orderId")
        .or_else(|| data.get("oid"))
        .and_then(serde_json::Value::as_str);
    let venue_symbol = data.get("symbol").and_then(serde_json::Value::as_str);
    let internal_symbol = venue_symbol
        .and_then(|symbol| crate::providers::bulk::catalog::market(symbol).ok())
        .map(|market| market.internal_symbol.as_str());
    let event_type = match kind {
        "fill" => "order.fill",
        "positionUpdate" if data.get("size").and_then(serde_json::Value::as_f64) == Some(0.0) => {
            "position.closed"
        }
        "positionUpdate" => "position.updated",
        "liquidation" => "position.liquidated",
        "adl" => "position.adl",
        "marginUpdate" => "account.margin_updated",
        "cancelOneRejected" | "cancelAllRejected" => "order.cancel_rejected",
        _ => return Ok(()),
    };

    if let Some(venue_order_id) = venue_order_id {
        let orders = state
            .script_orders
            .values()
            .filter(|order| order.venue_order_id.as_deref() == Some(venue_order_id))
            .cloned()
            .collect::<Vec<_>>();
        for order in orders {
            emit_script_event(
                paths,
                state,
                &order.job_id,
                event_type,
                Some(&order),
                matches!(kind, "liquidation" | "adl"),
                data.clone(),
            )?;
        }
        return Ok(());
    }

    let target_jobs = state
        .script_jobs
        .values()
        .filter(|job| {
            job.status.is_active()
                && job.definition.venue == Some(ExecutionVenue::Bulk)
                && internal_symbol.is_none_or(|symbol| job.definition.symbol == symbol)
        })
        .map(|job| job.id.clone())
        .collect::<HashSet<_>>();
    for job_id in target_jobs {
        emit_script_event(
            paths,
            state,
            &job_id,
            event_type,
            None,
            matches!(kind, "liquidation" | "adl"),
            data.clone(),
        )?;
    }
    Ok(())
}

fn apply_tracked_order_status(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    account: &str,
    order_id: &str,
    status: &str,
    event_ms: u64,
) -> Result<()> {
    let Some(order) = state.tracked_orders.get_mut(order_id) else {
        return Ok(());
    };
    if order.account != account {
        return Ok(());
    }
    let changed = order.status != status;
    order.status = status.to_string();
    order.updated_at_ms = event_ms;
    let snapshot = order.clone();
    if changed {
        append_runtime_event(paths, "order_status", &snapshot)?;
    }
    if is_terminal_order_status(status) {
        state.tracked_orders.remove(order_id);
    }
    Ok(())
}

async fn recover_account_gap(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    account: &str,
) -> Result<()> {
    let gap_started_ms = state
        .account_disconnected_at_ms
        .or(state.last_account_event_ms)
        .unwrap_or(0);

    // These are one-shot gap-recovery calls after a proven disconnect. They are
    // never scheduled on a timer while the account WebSocket is healthy.
    let history = adapter.order_history(account).await?;
    for record in history
        .into_iter()
        .filter(|record| record.ts_ms >= gap_started_ms)
    {
        apply_tracked_order_status(
            paths,
            state,
            account,
            &record.order_id,
            &record.status,
            record.ts_ms,
        )?;
        apply_script_order_status(
            paths,
            state,
            &record.order_id,
            &record.status,
            record.ts_ms,
            serde_json::to_value(&record)?,
        )?;
    }

    for fill in adapter
        .fills(account)
        .await?
        .into_iter()
        .filter(|fill| fill.ts_ms >= gap_started_ms)
    {
        let data = serde_json::to_value(&fill)?;
        append_json_line(
            &paths.events,
            &AccountRuntimeEvent {
                ts_ms: fill.ts_ms,
                event: "account_recovery_fill",
                account,
                data: &data,
            },
        )?;
        let mut routed = data.clone();
        if let Some(object) = routed.as_object_mut() {
            object.insert(
                "type".to_string(),
                serde_json::Value::String("fill".to_string()),
            );
            if let Some(order_id) = &fill.order_id {
                object.insert(
                    "orderId".to_string(),
                    serde_json::Value::String(order_id.clone()),
                );
            }
            object.insert(
                "symbol".to_string(),
                serde_json::Value::String(fill.venue_symbol.clone()),
            );
        }
        route_account_event_to_scripts(paths, state, &routed)?;
    }
    state.last_recovery_ms = Some(now_ms()?);
    state.account_disconnected_at_ms = None;
    persist_state(paths, state)
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status,
        "filled"
            | "partiallyFilled"
            | "cancelled"
            | "cancelledRiskLimit"
            | "cancelledSelfCrossing"
            | "cancelledReduceOnly"
            | "cancelledIoc"
            | "rejectedCrossing"
            | "rejectedDuplicate"
            | "rejectedRiskLimit"
            | "rejectedInvalid"
            | "siblingCancelled"
            | "triggerFailed"
    )
}

async fn try_status() -> Result<Option<RuntimeStatus>> {
    let Some(response) = try_request(RuntimeRequest::Status).await? else {
        return Ok(None);
    };
    if !response.ok {
        bail!("mlabd status failed: {}", response.message);
    }
    Ok(response.status)
}

async fn request(request: RuntimeRequest) -> Result<RuntimeResponse> {
    try_request(request).await?.context("mlabd is not running")
}

async fn try_request(request: RuntimeRequest) -> Result<Option<RuntimeResponse>> {
    let paths = RuntimePaths::load()?;
    let stream = match UnixStream::connect(&paths.socket).await {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to connect to {}", paths.socket.display()));
        }
    };
    let (reader, mut writer) = stream.into_split();
    let mut encoded = serde_json::to_vec(&request).context("failed to encode mlabd request")?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .context("failed to write mlabd request")?;
    writer.shutdown().await.ok();
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .await
        .context("failed to read mlabd response")?;
    let response = serde_json::from_str(&line).context("invalid mlabd response")?;
    Ok(Some(response))
}

fn runtime_status(state: &RuntimeState) -> RuntimeStatus {
    RuntimeStatus {
        version: RUNTIME_VERSION,
        running: true,
        pid: Some(state.pid),
        started_at_ms: Some(state.started_at_ms),
        account_stream_connected: state.account_stream_connected,
        last_account_event_ms: state.last_account_event_ms,
        last_recovery_ms: state.last_recovery_ms,
        last_error: state.last_error.clone(),
        tracked_orders: state.tracked_orders.values().cloned().collect(),
        script_jobs: state.script_jobs.values().cloned().collect(),
    }
}

fn load_state(paths: &RuntimePaths) -> Result<Option<RuntimeState>> {
    let source = match fs::read_to_string(&paths.state) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", paths.state.display()));
        }
    };
    let state: RuntimeState = serde_json::from_str(&source)
        .with_context(|| format!("failed to parse {}", paths.state.display()))?;
    if !(1..=RUNTIME_VERSION).contains(&state.version) {
        bail!("unsupported mlabd state version {}", state.version);
    }
    Ok(Some(state))
}

fn persist_state(paths: &RuntimePaths, state: &RuntimeState) -> Result<()> {
    fs::create_dir_all(&paths.directory)
        .with_context(|| format!("failed to create {}", paths.directory.display()))?;
    let temporary = paths.state.with_extension("json.tmp");
    let encoded = serde_json::to_vec_pretty(state).context("failed to encode mlabd state")?;
    fs::write(&temporary, encoded)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    fs::rename(&temporary, &paths.state)
        .with_context(|| format!("failed to replace {}", paths.state.display()))
}

fn append_runtime_event(
    paths: &RuntimePaths,
    event: &'static str,
    order: &TrackedOrder,
) -> Result<()> {
    append_json_line(
        &paths.events,
        &RuntimeEvent {
            ts_ms: now_ms()?,
            event,
            order,
        },
    )
}

fn record_runtime_error(paths: &RuntimePaths, state: &mut RuntimeState, message: String) {
    if state.last_error.as_deref() == Some(message.as_str()) {
        return;
    }
    state.last_error = Some(message);
    let _ = persist_state(paths, state);
}

fn append_json_line(path: &PathBuf, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut encoded = serde_json::to_vec(value).context("failed to encode runtime event")?;
    encoded.push(b'\n');
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?
        .write_all(&encoded)
        .with_context(|| format!("failed to append {}", path.display()))
}

fn daemon_binary() -> Result<PathBuf> {
    let current = std::env::current_exe().context("failed to locate the mlab executable")?;
    Ok(current.with_file_name("mlabd"))
}

fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("current timestamp does not fit in u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stopped_status_has_no_process_or_workloads() {
        let status = RuntimeStatus::stopped();
        assert!(!status.running);
        assert!(status.pid.is_none());
        assert!(status.tracked_orders.is_empty());
        assert!(status.script_jobs.is_empty());
    }

    #[test]
    fn reads_status_from_runtime_before_account_stream_fields_existed() {
        let status: RuntimeStatus = serde_json::from_value(serde_json::json!({
            "running": true,
            "pid": 123,
            "started_at_ms": 1_780_000_000_000_u64,
            "last_error": null,
            "tracked_orders": []
        }))
        .expect("legacy runtime status should deserialize");

        assert_eq!(status.version, 0);
        assert!(!status.account_stream_connected);
        assert!(status.last_account_event_ms.is_none());
        assert!(status.last_recovery_ms.is_none());
        assert!(status.script_jobs.is_empty());
    }
}
