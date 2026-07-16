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

use crate::credentials;
use crate::domain::execution::{CancelPlan, ExecutionReceipt, ExecutionVenue, TradePlan};
use crate::providers::bulk::execution::BulkExecutionAdapter;

const RUNTIME_VERSION: u8 = 3;
const RECONCILE_INTERVAL_SECS: u64 = 2;
const TERMINAL_UNKNOWN_AFTER_MISSING_POLLS: u16 = 120;

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
    pub missing_polls: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuntimeStatus {
    #[serde(default)]
    pub version: u8,
    pub running: bool,
    pub pid: Option<u32>,
    pub started_at_ms: Option<u64>,
    pub last_reconcile_ms: Option<u64>,
    pub last_error: Option<String>,
    pub tracked_orders: Vec<TrackedOrder>,
}

impl RuntimeStatus {
    fn stopped() -> Self {
        Self {
            version: RUNTIME_VERSION,
            running: false,
            pid: None,
            started_at_ms: None,
            last_reconcile_ms: None,
            last_error: None,
            tracked_orders: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RuntimeRequest {
    Ping,
    Status,
    Stop,
    TrackOrder { order: TrackedOrder },
    ExecuteTrade { plan: TradePlan },
    CancelOrder { plan: CancelPlan },
}

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeResponse {
    ok: bool,
    message: String,
    status: Option<RuntimeStatus>,
    #[serde(default)]
    receipt: Option<ExecutionReceipt>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeState {
    version: u8,
    pid: u32,
    started_at_ms: u64,
    last_reconcile_ms: Option<u64>,
    #[serde(default)]
    last_error: Option<String>,
    tracked_orders: BTreeMap<String, TrackedOrder>,
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

struct RuntimePaths {
    directory: PathBuf,
    socket: PathBuf,
    state: PathBuf,
    events: PathBuf,
    log: PathBuf,
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
        last_reconcile_ms: None,
        last_error: None,
        tracked_orders: BTreeMap::new(),
    });
    state.version = RUNTIME_VERSION;
    state.pid = std::process::id();
    state.started_at_ms = now_ms()?;
    if state.tracked_orders.is_empty() {
        state.last_reconcile_ms = None;
    }
    persist_state(&paths, &state)?;
    let adapter = BulkExecutionAdapter::new()?;

    let mut reconcile =
        tokio::time::interval(std::time::Duration::from_secs(RECONCILE_INTERVAL_SECS));
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut should_stop = false;
    while !should_stop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => should_stop = true,
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("mlabd failed to accept a local connection")?;
                match handle_connection(stream, &paths, &adapter, &mut state).await {
                    Ok(stop) => should_stop = stop,
                    Err(error) => record_runtime_error(
                        &paths,
                        &mut state,
                        format!("local runtime request failed: {error:#}"),
                    ),
                }
            }
            _ = reconcile.tick() => {
                reconcile_orders(&paths, &adapter, &mut state).await;
            }
        }
    }

    drop(listener);
    let _ = fs::remove_file(&paths.socket);
    state.pid = 0;
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
        missing_polls: 0,
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

async fn handle_connection(
    stream: UnixStream,
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
) -> Result<bool> {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .await
        .context("failed to read mlabd request")?;
    if line.len() > 64 * 1024 {
        bail!("mlabd request exceeds 64 KiB");
    }
    let request: RuntimeRequest = serde_json::from_str(&line).context("invalid mlabd request")?;
    let should_stop = matches!(request, RuntimeRequest::Stop);
    let response = match request {
        RuntimeRequest::Ping => RuntimeResponse {
            ok: true,
            message: "pong".to_string(),
            status: None,
            receipt: None,
        },
        RuntimeRequest::Status => RuntimeResponse {
            ok: true,
            message: "running".to_string(),
            status: Some(runtime_status(state)),
            receipt: None,
        },
        RuntimeRequest::Stop => RuntimeResponse {
            ok: true,
            message: "stopping".to_string(),
            status: Some(runtime_status(state)),
            receipt: None,
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
            }
        }
        RuntimeRequest::ExecuteTrade { plan } => {
            match execute_trade(paths, adapter, state, &plan).await {
                Ok(receipt) => RuntimeResponse {
                    ok: true,
                    message: "order submitted".to_string(),
                    status: Some(runtime_status(state)),
                    receipt: Some(receipt),
                },
                Err(error) => RuntimeResponse {
                    ok: false,
                    message: format!("{error:#}"),
                    status: Some(runtime_status(state)),
                    receipt: None,
                },
            }
        }
        RuntimeRequest::CancelOrder { plan } => {
            match execute_cancel(paths, adapter, state, &plan).await {
                Ok(receipt) => RuntimeResponse {
                    ok: true,
                    message: "cancellation submitted".to_string(),
                    status: Some(runtime_status(state)),
                    receipt: Some(receipt),
                },
                Err(error) => RuntimeResponse {
                    ok: false,
                    message: format!("{error:#}"),
                    status: Some(runtime_status(state)),
                    receipt: None,
                },
            }
        }
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
            missing_polls: 0,
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

async fn reconcile_orders(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
) {
    if state.tracked_orders.is_empty() {
        return;
    }
    let accounts = state
        .tracked_orders
        .values()
        .map(|order| order.account.clone())
        .collect::<HashSet<_>>();
    for account in accounts {
        let open_orders = match adapter.open_orders(&account).await {
            Ok(orders) => orders,
            Err(error) => {
                record_runtime_error(
                    paths,
                    state,
                    format!("BULK reconciliation failed for {account}: {error:#}"),
                );
                continue;
            }
        };
        let mut changed = state.last_error.take().is_some();
        let open = open_orders
            .into_iter()
            .map(|order| (order.order_id, order.status))
            .collect::<BTreeMap<_, _>>();
        let needs_history = state
            .tracked_orders
            .values()
            .any(|order| order.account == account && !open.contains_key(&order.order_id));
        let history = if needs_history {
            match adapter.order_history(&account).await {
                Ok(records) => records
                    .into_iter()
                    .map(|record| (record.order_id, record.status))
                    .collect::<BTreeMap<_, _>>(),
                Err(error) => {
                    record_runtime_error(
                        paths,
                        state,
                        format!(
                            "BULK order-history reconciliation failed for {account}: {error:#}"
                        ),
                    );
                    BTreeMap::new()
                }
            }
        } else {
            BTreeMap::new()
        };
        let mut terminal = Vec::new();
        for order in state
            .tracked_orders
            .values_mut()
            .filter(|order| order.account == account)
        {
            let previous = order.status.clone();
            if let Some(status) = open.get(&order.order_id) {
                order.status = status.clone();
                order.missing_polls = 0;
            } else if let Some(status) = history.get(&order.order_id) {
                order.status = status.clone();
                terminal.push(order.order_id.clone());
            } else {
                order.missing_polls = order.missing_polls.saturating_add(1);
                order.status = "not_open".to_string();
                if order.missing_polls >= TERMINAL_UNKNOWN_AFTER_MISSING_POLLS {
                    order.status = "terminal_unknown".to_string();
                    terminal.push(order.order_id.clone());
                }
            }
            if order.status != previous {
                changed = true;
                order.updated_at_ms = now_ms().unwrap_or(order.updated_at_ms);
                let _ = append_runtime_event(paths, "order_status", order);
            }
        }
        for order_id in terminal {
            state.tracked_orders.remove(&order_id);
            changed = true;
        }
        if changed {
            let _ = persist_state(paths, state);
        }
    }
    state.last_reconcile_ms = now_ms().ok();
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
        last_reconcile_ms: state.last_reconcile_ms,
        last_error: state.last_error.clone(),
        tracked_orders: state.tracked_orders.values().cloned().collect(),
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
    }
}
