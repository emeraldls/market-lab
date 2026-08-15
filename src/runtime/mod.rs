use std::collections::{BTreeMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::bots::jobs::{BotJob, BotJobDefinition, BotJobStatus, BotJobSubmission, BotPerformance};
use crate::credentials;
use crate::daemon::{self, DaemonBackend, DaemonConfig};
use crate::domain::execution::{
    CancelPlan, ExecutionOutcome, ExecutionReceipt, ExecutionVenue, Position, TradePlan,
};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::bulk::ws::BulkAccountStream;
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::exchange::UserOutcomeAction;
use crate::providers::hyperliquid::ws::HyperliquidAccountStream;
use crate::scripting::execution::{
    ScriptCancelRequest, ScriptManagedRequest, ScriptOrderRef, ScriptRawOrderRequest,
    ScriptTradeRequest, local_order_id,
};
use crate::scripting::jobs::{
    ScriptExecutionEvent, ScriptJob, ScriptJobDefinition, ScriptJobStatus, ScriptJobSubmission,
    ScriptManagedOrder,
};
use crate::scripting::language::{PythonRuntime, ScriptLanguage};
use crate::strategies::jobs::{
    StrategyJob, StrategyJobDefinition, StrategyJobStatus, StrategyJobSubmission, StrategySide,
};

// Bump whenever the IPC/state schema changes or the CLI must replace an older daemon.
const RUNTIME_VERSION: u8 = 37;
const ACCOUNT_RECONNECT_MAX_SECS: u64 = 30;
const MAX_RUNTIME_REQUEST_BYTES: usize = 1024 * 1024 + 128 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackedOrder {
    pub venue: ExecutionVenue,
    #[serde(default = "legacy_hyperliquid_testnet")]
    pub testnet: bool,
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

const fn legacy_hyperliquid_testnet() -> bool {
    true
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
    #[serde(default)]
    pub strategy_jobs: Vec<StrategyJob>,
    #[serde(default)]
    pub bot_jobs: Vec<BotJob>,
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
            strategy_jobs: Vec::new(),
            bot_jobs: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RuntimeRequest {
    Ping,
    Status,
    ReloadMarkets,
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
        exchange: Option<ExecutionVenue>,
        request: ScriptTradeRequest,
    },
    ScriptExecuteOrder {
        job_id: String,
        order: ScriptOrderRef,
        exchange: Option<ExecutionVenue>,
        request: ScriptRawOrderRequest,
    },
    ScriptCancel {
        job_id: String,
        request: ScriptCancelRequest,
    },
    ScriptCancelAllOrders {
        job_id: String,
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
    ScriptPositions {
        job_id: String,
    },
    SubmitStrategyJob {
        submission: StrategyJobSubmission,
    },
    ListStrategyJobs,
    GetStrategyJob {
        job_id: String,
    },
    StopStrategyJob {
        job_id: String,
    },
    StrategyWorkerStarted {
        job_id: String,
        pid: u32,
    },
    StrategyWorkerHeartbeat {
        job_id: String,
        pid: u32,
    },
    StrategyWorkerFinished {
        job_id: String,
        pid: u32,
        error: Option<String>,
    },
    StrategyExecuteTrade {
        job_id: String,
        sequence: u64,
        plan: TradePlan,
    },
    StrategyCancelOrder {
        job_id: String,
        sequence: u64,
        plan: CancelPlan,
    },
    SubmitBotJob {
        submission: BotJobSubmission,
    },
    ListBotJobs,
    GetBotJob {
        job_id: String,
    },
    StopBotJob {
        job_id: String,
    },
    BotWorkerStarted {
        job_id: String,
        pid: u32,
    },
    BotWorkerHeartbeat {
        job_id: String,
        pid: u32,
        performance: Option<BotPerformance>,
    },
    BotWorkerFinished {
        job_id: String,
        pid: u32,
        error: Option<String>,
    },
    BotExecuteTrade {
        job_id: String,
        sequence: u64,
        plan: TradePlan,
    },
    BotCancelOrder {
        job_id: String,
        sequence: u64,
        plan: CancelPlan,
    },
    BotExecuteTrades {
        job_id: String,
        items: Vec<SequencedTradePlan>,
    },
    BotCancelOrders {
        job_id: String,
        items: Vec<SequencedCancelPlan>,
    },
    BotOutcomeAction {
        job_id: String,
        sequence: u64,
        action: UserOutcomeAction,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeWireRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
    request: RuntimeRequest,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum IncomingRuntimeRequest {
    Authenticated(RuntimeWireRequest),
    Native(RuntimeRequest),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SequencedTradePlan {
    sequence: u64,
    plan: TradePlan,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SequencedCancelPlan {
    sequence: u64,
    plan: CancelPlan,
}

#[derive(Debug, Deserialize, Serialize)]
struct RuntimeResponse {
    ok: bool,
    message: String,
    status: Option<RuntimeStatus>,
    #[serde(default)]
    receipt: Option<ExecutionReceipt>,
    #[serde(default)]
    outcomes: Option<Vec<crate::domain::execution::ExecutionOutcome>>,
    #[serde(default)]
    job: Option<ScriptJob>,
    #[serde(default)]
    jobs: Option<Vec<ScriptJob>>,
    #[serde(default)]
    script_order: Option<ScriptManagedOrder>,
    #[serde(default)]
    script_events: Option<Vec<ScriptExecutionEvent>>,
    #[serde(default)]
    script_positions: Option<Vec<Position>>,
    #[serde(default)]
    strategy_job: Option<StrategyJob>,
    #[serde(default)]
    strategy_jobs: Option<Vec<StrategyJob>>,
    #[serde(default)]
    bot_job: Option<BotJob>,
    #[serde(default)]
    bot_jobs: Option<Vec<BotJob>>,
    #[serde(default)]
    action_response: Option<serde_json::Value>,
}

impl RuntimeResponse {
    fn empty() -> Self {
        Self {
            ok: true,
            message: String::new(),
            status: None,
            receipt: None,
            outcomes: None,
            job: None,
            jobs: None,
            script_order: None,
            script_events: None,
            script_positions: None,
            strategy_job: None,
            strategy_jobs: None,
            bot_job: None,
            bot_jobs: None,
            action_response: None,
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
    strategy_jobs: BTreeMap<String, StrategyJob>,
    #[serde(default)]
    strategy_executions: BTreeMap<String, ExecutionReceipt>,
    #[serde(default)]
    strategy_cancellations: BTreeMap<String, ExecutionReceipt>,
    #[serde(default)]
    bot_jobs: BTreeMap<String, BotJob>,
    #[serde(default)]
    bot_executions: BTreeMap<String, ExecutionReceipt>,
    #[serde(default)]
    bot_cancellations: BTreeMap<String, ExecutionReceipt>,
    #[serde(default)]
    bot_outcome_actions: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    script_orders: BTreeMap<String, ScriptManagedOrder>,
    #[serde(default)]
    script_cancel_keys: BTreeMap<String, String>,
    #[serde(default)]
    account_positions: BTreeMap<String, Vec<Position>>,
    #[serde(default)]
    account_positions_refreshed_at_ms: BTreeMap<String, u64>,
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
        venue: ExecutionVenue,
        testnet: bool,
        account: String,
        reconnected: bool,
    },
    Data {
        venue: ExecutionVenue,
        testnet: bool,
        account: String,
        data: serde_json::Value,
    },
    Disconnected {
        venue: ExecutionVenue,
        testnet: bool,
        account: String,
        error: String,
    },
}

trait RuntimeIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> RuntimeIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BoxedRuntimeIo = Box<dyn RuntimeIo>;

enum RuntimeListener {
    Unix(UnixListener),
    Tcp(TcpListener),
}

impl RuntimeListener {
    async fn accept(&self) -> std::io::Result<BoxedRuntimeIo> {
        match self {
            Self::Unix(listener) => {
                let (stream, _) = listener.accept().await?;
                Ok(Box::new(stream))
            }
            Self::Tcp(listener) => {
                let (stream, _) = listener.accept().await?;
                stream.set_nodelay(true)?;
                Ok(Box::new(stream))
            }
        }
    }
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
        let directory = daemon::market_lab_home()?.join("execution");
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
    let tcp_address = std::env::var("MLAB_DAEMON_TCP_ADDR").ok();
    let required_auth = if tcp_address.is_some() {
        Some(daemon::read_token().context("Docker mlabd requires its authentication token")?)
    } else {
        None
    };
    let listener = if let Some(address) = tcp_address {
        RuntimeListener::Tcp(
            TcpListener::bind(&address)
                .await
                .with_context(|| format!("failed to bind Docker mlabd to {address}"))?,
        )
    } else {
        if paths.socket.exists() {
            if UnixStream::connect(&paths.socket).await.is_ok() {
                bail!("mlabd is already running");
            }
            fs::remove_file(&paths.socket).with_context(|| {
                format!("failed to remove stale socket {}", paths.socket.display())
            })?;
        }
        let listener = UnixListener::bind(&paths.socket)
            .with_context(|| format!("failed to bind {}", paths.socket.display()))?;
        fs::set_permissions(&paths.socket, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", paths.socket.display()))?;
        RuntimeListener::Unix(listener)
    };
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
        strategy_jobs: BTreeMap::new(),
        strategy_executions: BTreeMap::new(),
        strategy_cancellations: BTreeMap::new(),
        bot_jobs: BTreeMap::new(),
        bot_executions: BTreeMap::new(),
        bot_cancellations: BTreeMap::new(),
        bot_outcome_actions: BTreeMap::new(),
        script_orders: BTreeMap::new(),
        script_cancel_keys: BTreeMap::new(),
        account_positions: BTreeMap::new(),
        account_positions_refreshed_at_ms: BTreeMap::new(),
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
        ensure_account_supervisor(
            ExecutionVenue::Bulk,
            false,
            &account,
            &account_tx,
            &mut account_supervisors,
        );
    }
    let mut should_stop = false;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install mlabd SIGTERM handler")?;
    while !should_stop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => should_stop = true,
            _ = terminate.recv() => should_stop = true,
            accepted = listener.accept() => {
                let stream = accepted.context("mlabd failed to accept a local connection")?;
                match handle_connection(
                    stream,
                    required_auth.as_deref(),
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
                        format!("execution account stream event failed: {error:#}"),
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
        let _ = stop_script_job_in_daemon(&paths, &adapter, &mut state, &job_id).await;
    }
    let active_strategy_jobs = state
        .strategy_jobs
        .values()
        .filter(|job| job.status.is_active())
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    for job_id in active_strategy_jobs {
        let _ = stop_strategy_job_in_daemon(&paths, &mut state, &job_id);
    }
    let active_bot_jobs = state
        .bot_jobs
        .values()
        .filter(|job| job.status.is_active())
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    for job_id in active_bot_jobs {
        let _ = stop_bot_job_in_daemon(&paths, &mut state, &job_id);
    }
    drop(listener);
    if required_auth.is_none() {
        let _ = fs::remove_file(&paths.socket);
    }
    state.pid = 0;
    state.account_stream_connected = false;
    persist_state(&paths, &state)?;
    Ok(())
}

pub async fn ensure_running() -> Result<RuntimeStatus> {
    let config = daemon::load()?;
    match config.backend {
        DaemonBackend::Native => ensure_native_running().await,
        DaemonBackend::Docker => ensure_docker_running(&config).await,
    }
}

async fn ensure_native_running() -> Result<RuntimeStatus> {
    if let Some(status) = try_status().await? {
        if status.version == RUNTIME_VERSION {
            return Ok(status);
        }
        let _ = stop().await;
        for _ in 0..20 {
            match try_status().await {
                Ok(None) => break,
                Ok(Some(_)) | Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
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

async fn ensure_docker_running(config: &DaemonConfig) -> Result<RuntimeStatus> {
    if let Some(status) = try_status().await? {
        if status.version == RUNTIME_VERSION {
            return Ok(status);
        }
        bail!(
            "Docker mlabd runtime version {} does not match CLI runtime version {}; run `mlab daemon backend docker` to replace it",
            status.version,
            RUNTIME_VERSION
        );
    }
    ensure_docker_available().await?;
    if !docker_container_exists(&config.docker.container).await? {
        pull_docker_image(&config.docker.image).await?;
        create_docker_container(config).await?;
    } else {
        let image = docker_container_image(&config.docker.container).await?;
        if image != config.docker.image {
            bail!(
                "Docker container `{}` uses `{image}`, expected `{}`; run `mlab daemon backend docker` to replace it",
                config.docker.container,
                config.docker.image
            );
        }
    }
    run_docker(&["start", &config.docker.container]).await?;
    wait_for_runtime().await.with_context(|| {
        format!(
            "Docker mlabd did not become ready; inspect `docker logs {}`",
            config.docker.container
        )
    })
}

async fn wait_for_runtime() -> Result<RuntimeStatus> {
    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        if let Some(status) = try_status().await?
            && status.version == RUNTIME_VERSION
        {
            return Ok(status);
        }
    }
    bail!("mlabd did not become ready within 15 seconds")
}

pub async fn configure_backend(backend: DaemonBackend) -> Result<DaemonConfig> {
    let previous = daemon::load()?;
    let target = match backend {
        DaemonBackend::Native => {
            let binary = daemon_binary()?;
            if !binary.exists() {
                bail!(
                    "native mlabd is not installed at {}; rerun the installer and choose Native",
                    binary.display()
                );
            }
            DaemonConfig::default()
        }
        DaemonBackend::Docker => {
            ensure_docker_available().await?;
            let target = DaemonConfig::docker_for_version(env!("CARGO_PKG_VERSION"));
            daemon::ensure_token()?;
            pull_docker_image(&target.docker.image).await?;
            target
        }
    };

    if let Some(status) = try_status().await?
        && runtime_has_active_work(&status)
        && previous != target
    {
        bail!(
            "cannot switch daemon backend while jobs or managed orders are active; stop them first"
        );
    }

    if previous == target {
        ensure_running().await?;
        return Ok(target);
    }

    stop_backend(&previous).await?;
    daemon::save(&target)?;
    let configured = match target.backend {
        DaemonBackend::Native => ensure_native_running().await.map(|_| ()),
        DaemonBackend::Docker => replace_docker_container(&target).await,
    };
    if let Err(error) = configured {
        daemon::save(&previous).context("failed to restore the previous daemon configuration")?;
        let rollback = match previous.backend {
            DaemonBackend::Native => ensure_native_running().await.map(|_| ()),
            DaemonBackend::Docker => ensure_docker_running(&previous).await.map(|_| ()),
        };
        if let Err(rollback_error) = rollback {
            bail!(
                "failed to configure daemon backend: {error:#}; restoring the previous backend also failed: {rollback_error:#}"
            );
        }
        return Err(error).context("failed to configure daemon backend; previous backend restored");
    }
    ensure_running().await?;
    Ok(target)
}

pub fn runtime_has_active_work(status: &RuntimeStatus) -> bool {
    !status.tracked_orders.is_empty()
        || status.script_jobs.iter().any(|job| job.status.is_active())
        || status
            .strategy_jobs
            .iter()
            .any(|job| job.status.is_active())
        || status.bot_jobs.iter().any(|job| job.status.is_active())
}

pub async fn prepare_docker_upgrade(version: &str) -> Result<()> {
    let config = daemon::load()?;
    if config.backend != DaemonBackend::Docker {
        return Ok(());
    }
    if let Some(status) = try_status().await?
        && runtime_has_active_work(&status)
    {
        bail!("cannot upgrade Docker mlabd while jobs or managed orders are active");
    }
    ensure_docker_available().await?;
    pull_docker_image(&daemon::docker_image_for_version(version)).await
}

pub async fn activate_docker_upgrade(version: &str) -> Result<()> {
    let mut config = daemon::load()?;
    if config.backend != DaemonBackend::Docker {
        return Ok(());
    }
    stop_docker_container(&config).await?;
    config.docker.image = daemon::docker_image_for_version(version);
    daemon::save(&config)?;
    replace_docker_container(&config).await
}

async fn stop_backend(config: &DaemonConfig) -> Result<()> {
    match config.backend {
        DaemonBackend::Native => {
            if let Some(response) = try_request(RuntimeRequest::Stop).await?
                && !response.ok
            {
                bail!("mlabd refused to stop: {}", response.message);
            }
            for _ in 0..120 {
                if try_status().await?.is_none() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            bail!("native mlabd did not stop within 30 seconds")
        }
        DaemonBackend::Docker => stop_docker_container(config).await,
    }
}

async fn ensure_docker_available() -> Result<()> {
    let output = tokio::process::Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .await
        .context("Docker is not installed; install Docker Desktop or Docker Engine")?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Docker is installed but unavailable: {}; start Docker and retry",
            error.trim()
        );
    }
    Ok(())
}

async fn pull_docker_image(image: &str) -> Result<()> {
    let status = tokio::process::Command::new("docker")
        .args(["pull", image])
        .status()
        .await
        .with_context(|| format!("failed to download Docker daemon image `{image}`"))?;
    if !status.success() {
        bail!("failed to download Docker daemon image `{image}`");
    }
    Ok(())
}

async fn replace_docker_container(config: &DaemonConfig) -> Result<()> {
    if docker_container_exists(&config.docker.container).await? {
        run_docker(&["rm", "--force", &config.docker.container]).await?;
    }
    create_docker_container(config).await?;
    run_docker(&["start", &config.docker.container]).await
}

async fn create_docker_container(config: &DaemonConfig) -> Result<()> {
    let home = daemon::market_lab_home()?;
    fs::create_dir_all(&home).with_context(|| format!("failed to create {}", home.display()))?;
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", home.display()))?;
    daemon::ensure_token()?;
    let args = docker_create_args(config, &home, unsafe { libc::geteuid() }, unsafe {
        libc::getegid()
    });
    run_docker_owned(&args).await
}

fn docker_create_args(config: &DaemonConfig, home: &Path, uid: u32, gid: u32) -> Vec<String> {
    let mount = format!(
        "type=bind,source={},target=/home/marketlab/.market-lab",
        home.display()
    );
    let publish = format!(
        "{}:{}:{}",
        config.docker.host,
        config.docker.port,
        daemon::DOCKER_CONTAINER_PORT
    );
    let user = format!("{uid}:{gid}");
    let listen = format!("0.0.0.0:{}", daemon::DOCKER_CONTAINER_PORT);
    let endpoint = format!("127.0.0.1:{}", daemon::DOCKER_CONTAINER_PORT);
    vec![
        "create".to_string(),
        "--name".to_string(),
        config.docker.container.clone(),
        "--label".to_string(),
        "io.marketlab.runtime=mlabd".to_string(),
        "--label".to_string(),
        format!("io.marketlab.version={}", env!("CARGO_PKG_VERSION")),
        "--restart".to_string(),
        "unless-stopped".to_string(),
        "--stop-timeout".to_string(),
        "60".to_string(),
        "--read-only".to_string(),
        "--cap-drop".to_string(),
        "ALL".to_string(),
        "--security-opt".to_string(),
        "no-new-privileges:true".to_string(),
        "--tmpfs".to_string(),
        "/tmp:rw,nosuid,nodev,size=268435456".to_string(),
        "--user".to_string(),
        user,
        "--env".to_string(),
        "HOME=/home/marketlab".to_string(),
        "--env".to_string(),
        "MLAB_HOME=/home/marketlab/.market-lab".to_string(),
        "--env".to_string(),
        format!("MLAB_DAEMON_TCP_ADDR={listen}"),
        "--env".to_string(),
        format!("MLAB_DAEMON_ENDPOINT={endpoint}"),
        "--publish".to_string(),
        publish,
        "--mount".to_string(),
        mount,
        config.docker.image.clone(),
        "serve".to_string(),
    ]
}

async fn docker_container_exists(container: &str) -> Result<bool> {
    let status = tokio::process::Command::new("docker")
        .args(["container", "inspect", container])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("failed to inspect Docker daemon container")?;
    Ok(status.success())
}

async fn docker_container_image(container: &str) -> Result<String> {
    let output = tokio::process::Command::new("docker")
        .args([
            "container",
            "inspect",
            "--format",
            "{{.Config.Image}}",
            container,
        ])
        .output()
        .await
        .context("failed to inspect Docker daemon image")?;
    if !output.status.success() {
        bail!(
            "failed to inspect Docker container `{container}`: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn stop_docker_container(config: &DaemonConfig) -> Result<()> {
    if !docker_container_exists(&config.docker.container).await? {
        return Ok(());
    }
    run_docker(&["stop", "--time", "60", &config.docker.container]).await
}

async fn run_docker(args: &[&str]) -> Result<()> {
    let args = args
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    run_docker_owned(&args).await
}

async fn run_docker_owned(args: &[String]) -> Result<()> {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to run `docker {}`", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "`docker {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
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

pub async fn healthcheck() -> Result<()> {
    let status = try_status().await?.context("mlabd is not reachable")?;
    if status.version != RUNTIME_VERSION {
        bail!(
            "mlabd runtime version {} does not match {}",
            status.version,
            RUNTIME_VERSION
        );
    }
    Ok(())
}

pub async fn stop() -> Result<bool> {
    let config = daemon::load()?;
    if config.backend == DaemonBackend::Docker {
        ensure_docker_available().await?;
        if !docker_container_exists(&config.docker.container).await? {
            return Ok(false);
        }
        if try_status().await?.is_none() {
            return Ok(false);
        }
        stop_docker_container(&config).await?;
        return Ok(true);
    }
    let Some(response) = try_request(RuntimeRequest::Stop).await? else {
        return Ok(false);
    };
    if !response.ok {
        bail!("mlabd refused to stop: {}", response.message);
    }
    Ok(true)
}

pub async fn reload_markets_if_running() -> Result<bool> {
    let Some(status) = try_status().await? else {
        return Ok(false);
    };
    if status.version != RUNTIME_VERSION {
        return Ok(false);
    }
    let Some(response) = try_request(RuntimeRequest::ReloadMarkets).await? else {
        return Ok(false);
    };
    if !response.ok {
        bail!("mlabd failed to reload markets: {}", response.message);
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
        testnet: plan.testnet,
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

pub async fn submit_strategy_job(submission: StrategyJobSubmission) -> Result<StrategyJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::SubmitStrategyJob { submission }).await?;
    if !response.ok {
        bail!("mlabd rejected strategy job: {}", response.message);
    }
    response
        .strategy_job
        .context("mlabd omitted the submitted strategy job")
}

pub async fn list_strategy_jobs() -> Result<Vec<StrategyJob>> {
    ensure_running().await?;
    let response = request(RuntimeRequest::ListStrategyJobs).await?;
    if !response.ok {
        bail!("mlabd could not list strategy jobs: {}", response.message);
    }
    response
        .strategy_jobs
        .context("mlabd omitted strategy jobs")
}

pub async fn get_strategy_job(job_id: &str) -> Result<StrategyJob> {
    ensure_running().await?;
    get_strategy_job_from_running_daemon(job_id).await
}

pub(crate) async fn get_strategy_job_from_running_daemon(job_id: &str) -> Result<StrategyJob> {
    let response = request(RuntimeRequest::GetStrategyJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not get strategy job: {}", response.message);
    }
    response.strategy_job.context("mlabd omitted strategy job")
}

pub async fn stop_strategy_job(job_id: &str) -> Result<StrategyJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::StopStrategyJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not stop strategy job: {}", response.message);
    }
    response.strategy_job.context("mlabd omitted strategy job")
}

pub async fn strategy_worker_started(job_id: &str, pid: u32) -> Result<StrategyJob> {
    let response = request(RuntimeRequest::StrategyWorkerStarted {
        job_id: job_id.to_string(),
        pid,
    })
    .await?;
    if !response.ok {
        bail!("mlabd rejected strategy worker: {}", response.message);
    }
    response.strategy_job.context("mlabd omitted strategy job")
}

pub async fn strategy_worker_heartbeat(job_id: &str, pid: u32) -> Result<StrategyJob> {
    let response = request(RuntimeRequest::StrategyWorkerHeartbeat {
        job_id: job_id.to_string(),
        pid,
    })
    .await?;
    if !response.ok {
        bail!(
            "mlabd rejected strategy worker heartbeat: {}",
            response.message
        );
    }
    response.strategy_job.context("mlabd omitted strategy job")
}

pub async fn strategy_worker_finished(
    job_id: &str,
    pid: u32,
    error: Option<String>,
) -> Result<StrategyJob> {
    let response = request(RuntimeRequest::StrategyWorkerFinished {
        job_id: job_id.to_string(),
        pid,
        error,
    })
    .await?;
    if !response.ok {
        bail!(
            "mlabd rejected strategy worker finish: {}",
            response.message
        );
    }
    response.strategy_job.context("mlabd omitted strategy job")
}

pub async fn submit_strategy_trade(
    job_id: &str,
    sequence: u64,
    plan: &TradePlan,
) -> Result<ExecutionReceipt> {
    let response = request(RuntimeRequest::StrategyExecuteTrade {
        job_id: job_id.to_string(),
        sequence,
        plan: plan.clone(),
    })
    .await?;
    if !response.ok {
        bail!("strategy trade failed: {}", response.message);
    }
    response
        .receipt
        .context("mlabd omitted the strategy execution receipt")
}

pub async fn submit_strategy_cancel(
    job_id: &str,
    sequence: u64,
    plan: &CancelPlan,
) -> Result<ExecutionReceipt> {
    let response = request(RuntimeRequest::StrategyCancelOrder {
        job_id: job_id.to_string(),
        sequence,
        plan: plan.clone(),
    })
    .await?;
    if !response.ok {
        bail!("strategy cancellation failed: {}", response.message);
    }
    response
        .receipt
        .context("mlabd omitted the strategy cancellation receipt")
}

pub async fn submit_bot_job(submission: BotJobSubmission) -> Result<BotJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::SubmitBotJob { submission }).await?;
    if !response.ok {
        bail!("mlabd rejected bot job: {}", response.message);
    }
    response
        .bot_job
        .context("mlabd omitted the submitted bot job")
}

pub async fn list_bot_jobs() -> Result<Vec<BotJob>> {
    ensure_running().await?;
    let response = request(RuntimeRequest::ListBotJobs).await?;
    if !response.ok {
        bail!("mlabd could not list bot jobs: {}", response.message);
    }
    response.bot_jobs.context("mlabd omitted bot jobs")
}

pub async fn get_bot_job(job_id: &str) -> Result<BotJob> {
    ensure_running().await?;
    get_bot_job_from_running_daemon(job_id).await
}

pub(crate) async fn get_bot_job_from_running_daemon(job_id: &str) -> Result<BotJob> {
    let response = request(RuntimeRequest::GetBotJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not get bot job: {}", response.message);
    }
    response.bot_job.context("mlabd omitted bot job")
}

pub async fn stop_bot_job(job_id: &str) -> Result<BotJob> {
    ensure_running().await?;
    let response = request(RuntimeRequest::StopBotJob {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd could not stop bot job: {}", response.message);
    }
    response.bot_job.context("mlabd omitted bot job")
}

pub async fn bot_worker_started(job_id: &str, pid: u32) -> Result<BotJob> {
    let response = request(RuntimeRequest::BotWorkerStarted {
        job_id: job_id.to_string(),
        pid,
    })
    .await?;
    if !response.ok {
        bail!("mlabd rejected bot worker: {}", response.message);
    }
    response.bot_job.context("mlabd omitted bot job")
}

pub async fn bot_worker_heartbeat(
    job_id: &str,
    pid: u32,
    performance: Option<&BotPerformance>,
) -> Result<BotJob> {
    let response = request(RuntimeRequest::BotWorkerHeartbeat {
        job_id: job_id.to_string(),
        pid,
        performance: performance.cloned(),
    })
    .await?;
    if !response.ok {
        bail!("mlabd rejected bot worker heartbeat: {}", response.message);
    }
    response.bot_job.context("mlabd omitted bot job")
}

pub async fn bot_worker_finished(job_id: &str, pid: u32, error: Option<String>) -> Result<BotJob> {
    let response = request(RuntimeRequest::BotWorkerFinished {
        job_id: job_id.to_string(),
        pid,
        error,
    })
    .await?;
    if !response.ok {
        bail!("mlabd rejected bot worker finish: {}", response.message);
    }
    response.bot_job.context("mlabd omitted bot job")
}

pub async fn submit_bot_trade(
    job_id: &str,
    sequence: u64,
    plan: &TradePlan,
) -> Result<ExecutionReceipt> {
    let response = request(RuntimeRequest::BotExecuteTrade {
        job_id: job_id.to_string(),
        sequence,
        plan: plan.clone(),
    })
    .await?;
    if !response.ok {
        bail!("bot trade failed: {}", response.message);
    }
    response
        .receipt
        .context("mlabd omitted the bot execution receipt")
}

pub async fn submit_bot_cancel(
    job_id: &str,
    sequence: u64,
    plan: &CancelPlan,
) -> Result<ExecutionReceipt> {
    let response = request(RuntimeRequest::BotCancelOrder {
        job_id: job_id.to_string(),
        sequence,
        plan: plan.clone(),
    })
    .await?;
    if !response.ok {
        bail!("bot cancellation failed: {}", response.message);
    }
    response
        .receipt
        .context("mlabd omitted the bot cancellation receipt")
}

pub async fn submit_bot_trades(
    job_id: &str,
    items: &[(u64, TradePlan)],
) -> Result<Vec<crate::domain::execution::ExecutionOutcome>> {
    let response = request(RuntimeRequest::BotExecuteTrades {
        job_id: job_id.to_string(),
        items: items
            .iter()
            .map(|(sequence, plan)| SequencedTradePlan {
                sequence: *sequence,
                plan: plan.clone(),
            })
            .collect(),
    })
    .await?;
    if !response.ok {
        bail!("bot trade batch failed: {}", response.message);
    }
    response
        .outcomes
        .context("mlabd omitted the bot batch execution outcomes")
}

pub async fn submit_bot_cancels(
    job_id: &str,
    items: &[(u64, CancelPlan)],
) -> Result<Vec<crate::domain::execution::ExecutionOutcome>> {
    let response = request(RuntimeRequest::BotCancelOrders {
        job_id: job_id.to_string(),
        items: items
            .iter()
            .map(|(sequence, plan)| SequencedCancelPlan {
                sequence: *sequence,
                plan: plan.clone(),
            })
            .collect(),
    })
    .await?;
    if !response.ok {
        bail!("bot cancellation batch failed: {}", response.message);
    }
    response
        .outcomes
        .context("mlabd omitted the bot batch cancellation outcomes")
}

pub async fn submit_bot_outcome_action(
    job_id: &str,
    sequence: u64,
    action: &UserOutcomeAction,
) -> Result<serde_json::Value> {
    let response = request(RuntimeRequest::BotOutcomeAction {
        job_id: job_id.to_string(),
        sequence,
        action: action.clone(),
    })
    .await?;
    if !response.ok {
        bail!("bot outcome action failed: {}", response.message);
    }
    response
        .action_response
        .context("mlabd omitted the bot outcome-action response")
}

pub fn append_bot_output(job_id: &str, value: &impl Serialize) -> Result<()> {
    let paths = RuntimePaths::load()?;
    let path = bot_job_directory(&paths, job_id)?.join("output.jsonl");
    let mut value = serde_json::to_value(value).context("failed to encode bot output")?;
    if let Some(object) = value.as_object_mut() {
        object
            .entry("tsMs")
            .or_insert(serde_json::Value::from(now_ms()?));
    }
    append_json_line(&path, &value)
}

pub fn bot_output_after(
    job_id: &str,
    after_line: usize,
) -> Result<(usize, Vec<serde_json::Value>)> {
    let paths = RuntimePaths::load()?;
    let path = bot_job_directory(&paths, job_id)?.join("output.jsonl");
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
        .map(|line| serde_json::from_str(line).context("bot output journal is malformed"))
        .collect::<Result<Vec<_>>>()?;
    Ok((total, values))
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
    exchange: Option<ExecutionVenue>,
    request_value: ScriptTradeRequest,
) -> Result<ScriptManagedOrder> {
    let response = request(RuntimeRequest::ScriptExecuteTrade {
        job_id: job_id.to_string(),
        order,
        exchange,
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

pub async fn submit_script_order(
    job_id: &str,
    order: ScriptOrderRef,
    exchange: Option<ExecutionVenue>,
    request_value: ScriptRawOrderRequest,
) -> Result<ScriptManagedOrder> {
    let response = request(RuntimeRequest::ScriptExecuteOrder {
        job_id: job_id.to_string(),
        order,
        exchange,
        request: request_value,
    })
    .await?;
    if !response.ok {
        bail!("script order failed: {}", response.message);
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

pub async fn cancel_all_script_orders(job_id: &str) -> Result<()> {
    let response = request(RuntimeRequest::ScriptCancelAllOrders {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!("script managed-order cleanup failed: {}", response.message);
    }
    Ok(())
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

pub async fn script_positions(job_id: &str) -> Result<Vec<Position>> {
    let response = request(RuntimeRequest::ScriptPositions {
        job_id: job_id.to_string(),
    })
    .await?;
    if !response.ok {
        bail!(
            "mlabd could not read script positions: {}",
            response.message
        );
    }
    response
        .script_positions
        .context("mlabd omitted script positions")
}

pub fn append_script_output(job_id: &str, value: &impl Serialize) -> Result<()> {
    let paths = RuntimePaths::load()?;
    let path = script_job_directory(&paths, job_id)?.join("output.jsonl");
    append_json_line(&path, value)
}

fn script_failure_record(ts_ms: u64, error: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "script.run.failed",
        "version": "1",
        "ts_ms": ts_ms,
        "error": error,
    })
}

pub fn append_strategy_output(job_id: &str, value: &impl Serialize) -> Result<()> {
    let paths = RuntimePaths::load()?;
    let path = strategy_job_directory(&paths, job_id)?.join("output.jsonl");
    append_json_line(&path, value)
}

pub fn strategy_output_after(
    job_id: &str,
    after_line: usize,
) -> Result<(usize, Vec<serde_json::Value>)> {
    let paths = RuntimePaths::load()?;
    let path = strategy_job_directory(&paths, job_id)?.join("output.jsonl");
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
        .map(|line| serde_json::from_str(line).context("strategy output journal is malformed"))
        .collect::<Result<Vec<_>>>()?;
    Ok((total, values))
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
    mut submission: ScriptJobSubmission,
) -> Result<ScriptJob> {
    if submission.language == ScriptLanguage::PythonV2
        && std::env::var_os("MLAB_DAEMON_TCP_ADDR").is_some()
    {
        let interpreter = std::env::var_os("MLAB_PYTHON")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/usr/bin/python3"));
        submission.python_runtime = Some(PythonRuntime::inspect(interpreter).context(
            "Docker mlabd could not resolve its Python runtime; use a daemon image containing Python 3.9 or newer",
        )?);
    }
    submission.validate()?;
    if let Some(venue) = submission.venue {
        crate::providers::execution::ExecutionAdapter::configured_account(venue).with_context(
            || {
                format!(
                    "{} authentication is required when a script enables execution",
                    execution_exchange(venue)
                )
            },
        )?;
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
    let snapshot_path = job_directory.join(submission.language.snapshot_file_name());
    fs::write(&snapshot_path, submission.source.as_bytes())
        .with_context(|| format!("failed to write {}", snapshot_path.display()))?;
    fs::set_permissions(&snapshot_path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", snapshot_path.display()))?;

    let created_at_ms = now_ms()?;
    let definition = ScriptJobDefinition {
        script_name: submission.script_name,
        original_path: submission.original_path,
        snapshot_path,
        language: submission.language,
        python_runtime: submission.python_runtime,
        providers: submission.providers,
        exchanges: submission.exchanges,
        sources: submission.sources,
        params: submission.params,
        venue: submission.venue,
        testnet: submission.testnet,
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

async fn cancel_script_job_orders(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<()> {
    if !state.script_jobs.contains_key(job_id) {
        bail!("script job `{job_id}` was not found");
    }
    let order_ids = state
        .script_orders
        .values()
        .filter(|order| {
            order.job_id == job_id
                && order.status != "rejected"
                && !is_terminal_order_status(&order.status)
        })
        .map(|order| order.order.id.clone())
        .collect::<Vec<_>>();
    let mut failures = Vec::new();
    for order_id in order_ids {
        let request = ScriptCancelRequest {
            key: format!("system-cleanup-{order_id}"),
            order: order_id.clone(),
        };
        if let Err(error) = execute_script_cancel(paths, adapter, state, job_id, request).await {
            failures.push(format!("{order_id}: {error:#}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "failed to cancel {} managed order(s): {}",
            failures.len(),
            failures.join("; ")
        )
    }
}

async fn stop_script_job_in_daemon(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<ScriptJob> {
    let current = state
        .script_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if current.status.is_active()
        && let Some(pid) = current.pid
    {
        let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error).context("failed to stop script worker");
            }
        }
    }
    let cleanup_error = if current.status.is_active() {
        cancel_script_job_orders(paths, adapter, state, job_id)
            .await
            .err()
    } else {
        None
    };
    let job = state
        .script_jobs
        .get_mut(job_id)
        .context("script job disappeared while stopping")?;
    job.status = ScriptJobStatus::Stopped;
    job.pid = None;
    job.stopped_at_ms = Some(now_ms()?);
    job.last_error = cleanup_error.as_ref().map(|error| format!("{error:#}"));
    let job = job.clone();
    persist_state(paths, state)?;
    if let Some(error) = cleanup_error {
        Err(error).context("script worker stopped, but its managed orders were not fully cancelled")
    } else {
        Ok(job)
    }
}

async fn restart_script_job_in_daemon(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<ScriptJob> {
    if state
        .script_jobs
        .get(job_id)
        .is_some_and(|job| job.status.is_active())
    {
        stop_script_job_in_daemon(paths, adapter, state, job_id).await?;
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

async fn mark_script_worker_finished(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
    error: Option<String>,
) -> Result<ScriptJob> {
    let current = state
        .script_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if current.pid.is_some() && current.pid != Some(pid) {
        bail!("stale script worker attempted to finish job `{job_id}`");
    }
    let cleanup_error = if current.status.is_active() {
        cancel_script_job_orders(paths, adapter, state, job_id)
            .await
            .err()
    } else {
        None
    };
    let job = state
        .script_jobs
        .get_mut(job_id)
        .context("script job disappeared while finishing its worker")?;
    if job.status != ScriptJobStatus::Stopped {
        job.status = if error.is_some() || cleanup_error.is_some() {
            ScriptJobStatus::Failed
        } else {
            ScriptJobStatus::Completed
        };
    }
    job.pid = None;
    job.stopped_at_ms = Some(now_ms()?);
    job.last_error = match (error, cleanup_error) {
        (Some(worker), Some(cleanup)) => Some(format!(
            "{worker}; managed-order cleanup also failed: {cleanup:#}"
        )),
        (Some(worker), None) => Some(worker),
        (None, Some(cleanup)) => Some(format!("managed-order cleanup failed: {cleanup:#}")),
        (None, None) => None,
    };
    if let Some(error) = &job.last_error {
        let _ = append_json_line(
            &paths.jobs.join(job_id).join("output.jsonl"),
            &script_failure_record(now_ms()?, error),
        );
    }
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn create_strategy_job(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    submission: StrategyJobSubmission,
) -> Result<StrategyJob> {
    submission.validate()?;
    crate::providers::execution::ExecutionAdapter::configured_account(
        submission.definition.venue(),
    )
    .with_context(|| {
        format!(
            "{} authentication is required for strategy jobs",
            execution_exchange(submission.definition.venue())
        )
    })?;

    fs::create_dir_all(&paths.jobs)
        .with_context(|| format!("failed to create {}", paths.jobs.display()))?;
    fs::set_permissions(&paths.jobs, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", paths.jobs.display()))?;
    let job_id = new_strategy_job_id(state)?;
    let job_directory = paths.jobs.join(&job_id);
    fs::create_dir(&job_directory)
        .with_context(|| format!("failed to create {}", job_directory.display()))?;
    fs::set_permissions(&job_directory, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", job_directory.display()))?;

    let created_at_ms = now_ms()?;
    let job = StrategyJob {
        id: job_id.clone(),
        definition: submission.definition,
        status: StrategyJobStatus::Starting,
        pid: None,
        created_at_ms,
        started_at_ms: None,
        stopped_at_ms: None,
        last_heartbeat_ms: None,
        last_error: None,
    };
    state.strategy_jobs.insert(job_id.clone(), job);
    persist_state(paths, state)?;
    if let Err(error) = spawn_strategy_worker(paths, state, &job_id) {
        if let Some(job) = state.strategy_jobs.get_mut(&job_id) {
            job.status = StrategyJobStatus::Failed;
            job.stopped_at_ms = Some(now_ms().unwrap_or(created_at_ms));
            job.last_error = Some(format!("{error:#}"));
        }
        persist_state(paths, state)?;
        return Err(error);
    }
    persist_state(paths, state)?;
    state
        .strategy_jobs
        .get(&job_id)
        .cloned()
        .context("strategy job disappeared after creation")
}

fn new_strategy_job_id(state: &RuntimeState) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    let base = format!(
        "strategy_{:013x}_{:05x}",
        now.as_millis(),
        now.subsec_nanos()
    );
    if !state.strategy_jobs.contains_key(&base) && !state.script_jobs.contains_key(&base) {
        return Ok(base);
    }
    for suffix in 1..=9999_u16 {
        let candidate = format!("{base}_{suffix}");
        if !state.strategy_jobs.contains_key(&candidate)
            && !state.script_jobs.contains_key(&candidate)
        {
            return Ok(candidate);
        }
    }
    bail!("could not allocate a unique strategy job id")
}

fn spawn_strategy_worker(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<()> {
    if !state.strategy_jobs.contains_key(job_id) {
        bail!("strategy job was not found");
    }
    let worker_log = paths.jobs.join(job_id).join("worker.log");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&worker_log)
        .with_context(|| format!("failed to open {}", worker_log.display()))?;
    let stderr = stdout
        .try_clone()
        .context("failed to clone strategy worker log handle")?;
    let executable = std::env::current_exe().context("failed to locate mlabd")?;
    let child = Command::new(executable)
        .arg("strategy-worker")
        .arg(job_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("failed to start strategy worker for {job_id}"))?;
    let job = state
        .strategy_jobs
        .get_mut(job_id)
        .context("strategy job disappeared while starting")?;
    job.status = StrategyJobStatus::Starting;
    job.pid = Some(child.id());
    job.stopped_at_ms = None;
    job.last_error = None;
    Ok(())
}

fn stop_strategy_job_in_daemon(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<StrategyJob> {
    let job = state
        .strategy_jobs
        .get_mut(job_id)
        .with_context(|| format!("strategy job `{job_id}` was not found"))?;
    if job.status.is_active()
        && let Some(pid) = job.pid
    {
        let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error).context("failed to stop strategy worker");
            }
        }
    }
    if job.status.is_active() {
        job.status = StrategyJobStatus::Stopping;
    }
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_strategy_worker_started(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
) -> Result<StrategyJob> {
    let now = now_ms()?;
    let job = state
        .strategy_jobs
        .get_mut(job_id)
        .with_context(|| format!("strategy job `{job_id}` was not found"))?;
    if job.status == StrategyJobStatus::Stopped {
        bail!("strategy job `{job_id}` was stopped before its worker became ready");
    }
    job.status = StrategyJobStatus::Running;
    job.pid = Some(pid);
    job.started_at_ms = Some(now);
    job.last_heartbeat_ms = Some(now);
    job.last_error = None;
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_strategy_worker_heartbeat(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
) -> Result<StrategyJob> {
    let job = state
        .strategy_jobs
        .get_mut(job_id)
        .with_context(|| format!("strategy job `{job_id}` was not found"))?;
    if job.pid != Some(pid) || !job.status.is_active() {
        bail!("strategy worker is no longer active for job `{job_id}`");
    }
    job.last_heartbeat_ms = Some(now_ms()?);
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_strategy_worker_finished(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
    error: Option<String>,
) -> Result<StrategyJob> {
    let job = state
        .strategy_jobs
        .get_mut(job_id)
        .with_context(|| format!("strategy job `{job_id}` was not found"))?;
    if job.pid.is_some() && job.pid != Some(pid) {
        bail!("stale strategy worker attempted to finish job `{job_id}`");
    }
    if job.status == StrategyJobStatus::Stopping {
        job.status = StrategyJobStatus::Stopped;
    } else if job.status != StrategyJobStatus::Stopped {
        job.status = if error.is_some() {
            StrategyJobStatus::Failed
        } else {
            StrategyJobStatus::Completed
        };
    }
    job.pid = None;
    job.stopped_at_ms = Some(now_ms()?);
    job.last_error = error;
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

async fn create_bot_job(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    submission: BotJobSubmission,
) -> Result<BotJob> {
    submission.validate()?;
    crate::providers::execution::ExecutionAdapter::configured_account(
        submission.definition.venue(),
    )
    .with_context(|| {
        format!(
            "{} authentication is required for bot jobs",
            execution_exchange(submission.definition.venue())
        )
    })?;

    if matches!(
        submission.definition.venue(),
        ExecutionVenue::Hyperliquid | ExecutionVenue::HyperliquidXyz
    ) {
        crate::providers::hyperliquid::execution::HyperliquidExecutionAdapter::new_for(
            hyperliquid_product(submission.definition.venue()),
            HyperliquidNetwork::from_testnet(submission.definition.testnet()),
        )
        .await?
        .configure_leverage(
            submission.definition.symbol(),
            submission
                .definition
                .leverage()
                .context("perpetual bot leverage is missing")?,
        )
        .await
        .context("failed to configure Hyperliquid leverage before starting the bot")?;
    }

    fs::create_dir_all(&paths.jobs)
        .with_context(|| format!("failed to create {}", paths.jobs.display()))?;
    fs::set_permissions(&paths.jobs, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", paths.jobs.display()))?;
    let job_id = new_bot_job_id(state)?;
    let job_directory = paths.jobs.join(&job_id);
    fs::create_dir(&job_directory)
        .with_context(|| format!("failed to create {}", job_directory.display()))?;
    fs::set_permissions(&job_directory, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", job_directory.display()))?;

    let created_at_ms = now_ms()?;
    let job = BotJob {
        id: job_id.clone(),
        definition: submission.definition,
        status: BotJobStatus::Starting,
        pid: None,
        created_at_ms,
        started_at_ms: None,
        stopped_at_ms: None,
        last_heartbeat_ms: None,
        last_error: None,
        performance: None,
    };
    state.bot_jobs.insert(job_id.clone(), job);
    persist_state(paths, state)?;
    if let Err(error) = spawn_bot_worker(paths, state, &job_id) {
        if let Some(job) = state.bot_jobs.get_mut(&job_id) {
            job.status = BotJobStatus::Failed;
            job.stopped_at_ms = Some(now_ms().unwrap_or(created_at_ms));
            job.last_error = Some(format!("{error:#}"));
        }
        persist_state(paths, state)?;
        return Err(error);
    }
    persist_state(paths, state)?;
    state
        .bot_jobs
        .get(&job_id)
        .cloned()
        .context("bot job disappeared after creation")
}

fn new_bot_job_id(state: &RuntimeState) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    let base = format!("bot_{:013x}_{:05x}", now.as_millis(), now.subsec_nanos());
    if !state.bot_jobs.contains_key(&base)
        && !state.strategy_jobs.contains_key(&base)
        && !state.script_jobs.contains_key(&base)
    {
        return Ok(base);
    }
    for suffix in 1..=9999_u16 {
        let candidate = format!("{base}_{suffix}");
        if !state.bot_jobs.contains_key(&candidate)
            && !state.strategy_jobs.contains_key(&candidate)
            && !state.script_jobs.contains_key(&candidate)
        {
            return Ok(candidate);
        }
    }
    bail!("could not allocate a unique bot job id")
}

fn spawn_bot_worker(paths: &RuntimePaths, state: &mut RuntimeState, job_id: &str) -> Result<()> {
    if !state.bot_jobs.contains_key(job_id) {
        bail!("bot job was not found");
    }
    let worker_log = paths.jobs.join(job_id).join("worker.log");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&worker_log)
        .with_context(|| format!("failed to open {}", worker_log.display()))?;
    let stderr = stdout
        .try_clone()
        .context("failed to clone bot worker log handle")?;
    let executable = std::env::current_exe().context("failed to locate mlabd")?;
    let child = Command::new(executable)
        .arg("bot-worker")
        .arg(job_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("failed to start bot worker for {job_id}"))?;
    let job = state
        .bot_jobs
        .get_mut(job_id)
        .context("bot job disappeared while starting")?;
    job.status = BotJobStatus::Starting;
    job.pid = Some(child.id());
    job.stopped_at_ms = None;
    job.last_error = None;
    Ok(())
}

fn stop_bot_job_in_daemon(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<BotJob> {
    let job = state
        .bot_jobs
        .get_mut(job_id)
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if job.status.is_active()
        && let Some(pid) = job.pid
    {
        let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error).context("failed to stop bot worker");
            }
        }
        job.status = BotJobStatus::Stopping;
    }
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_bot_worker_started(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
) -> Result<BotJob> {
    let now = now_ms()?;
    let job = state
        .bot_jobs
        .get_mut(job_id)
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if job.status == BotJobStatus::Stopped {
        bail!("bot job `{job_id}` was stopped before its worker became ready");
    }
    job.status = BotJobStatus::Running;
    job.pid = Some(pid);
    job.started_at_ms = Some(now);
    job.last_heartbeat_ms = Some(now);
    job.last_error = None;
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_bot_worker_heartbeat(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
    performance: Option<BotPerformance>,
) -> Result<BotJob> {
    let job = state
        .bot_jobs
        .get_mut(job_id)
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if job.pid != Some(pid) || !job.status.is_active() {
        bail!("bot worker is no longer active for job `{job_id}`");
    }
    job.last_heartbeat_ms = Some(now_ms()?);
    if performance.is_some() {
        job.performance = performance;
    }
    let job = job.clone();
    persist_state(paths, state)?;
    Ok(job)
}

fn mark_bot_worker_finished(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    pid: u32,
    error: Option<String>,
) -> Result<BotJob> {
    let job = state
        .bot_jobs
        .get_mut(job_id)
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if job.pid.is_some() && job.pid != Some(pid) {
        bail!("stale bot worker attempted to finish job `{job_id}`");
    }
    if job.status == BotJobStatus::Stopping {
        job.status = if error.is_some() {
            BotJobStatus::Failed
        } else {
            BotJobStatus::Stopped
        };
    } else if job.status != BotJobStatus::Stopped {
        job.status = if error.is_some() {
            BotJobStatus::Failed
        } else {
            BotJobStatus::Completed
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

fn strategy_job_directory(paths: &RuntimePaths, job_id: &str) -> Result<PathBuf> {
    if job_id.is_empty()
        || !job_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid strategy job id");
    }
    Ok(paths.jobs.join(job_id))
}

fn bot_job_directory(paths: &RuntimePaths, job_id: &str) -> Result<PathBuf> {
    if job_id.is_empty()
        || !job_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid bot job id");
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
        symbol: order.map(|order| order.request.symbol().to_string()),
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

struct ScriptOrderOperation<'a> {
    job_id: &'a str,
    order: ScriptOrderRef,
    exchange: Option<ExecutionVenue>,
    request: ScriptManagedRequest,
}

async fn execute_script_order(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    account_tx: &mpsc::Sender<AccountConnectionEvent>,
    account_supervisors: &mut HashSet<String>,
    operation: ScriptOrderOperation<'_>,
) -> Result<ScriptManagedOrder> {
    let ScriptOrderOperation {
        job_id,
        order,
        exchange,
        request,
    } = operation;
    let operation_name = match &request {
        ScriptManagedRequest::Trade(request) => {
            request.validate()?;
            "ctx.trade"
        }
        ScriptManagedRequest::Order(request) => {
            request.validate()?;
            "ctx.order"
        }
    };
    let key = request.key().to_string();
    let job = state
        .script_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("script job `{job_id}` is not running");
    }
    let internal_symbol = crate::scripting::inputs::script_symbol_to_market(request.symbol());
    if !script_job_tracks_symbol(&job, &internal_symbol) {
        bail!(
            "{operation_name} symbol `{}` is not declared by this script job's sources",
            request.symbol()
        );
    }
    let venue = match job.definition.language {
        crate::scripting::language::ScriptLanguage::JavaScriptV1 => {
            if exchange.is_some() {
                bail!("JavaScript Scripting V1 cannot route execution per request");
            }
            job.definition
                .venue
                .context("script execution is disabled; deploy the script with --venue")?
        }
        crate::scripting::language::ScriptLanguage::PythonV2 => {
            if job.definition.venue.is_some() {
                bail!("Python Scripting V2 cannot use a job-wide execution venue");
            }
            exchange.context(
                "Python Scripting V2 ctx.trade/ctx.order request omitted its execution exchange",
            )?
        }
    };
    let expected_id = local_order_id(job_id, &key);
    if order.id != expected_id || order.key != key {
        bail!("script order reference does not match its job and idempotency key");
    }
    if let Some(existing) = state.script_orders.get(&order.id) {
        if existing.job_id == job_id && existing.venue == venue && existing.request == request {
            return Ok(existing.clone());
        }
        bail!("{operation_name} key `{key}` was already used with different order parameters");
    }

    let created_at_ms = now_ms()?;
    let pending = ScriptManagedOrder {
        job_id: job_id.to_string(),
        order: order.clone(),
        request: request.clone(),
        symbol: internal_symbol.clone(),
        venue,
        testnet: job.definition.testnet,
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

    let order_spec = request.order().clone();
    let order_kind = match order_spec.kind {
        crate::scripting::execution::ScriptOrderKind::Market => crate::cli::TradeOrderKind::Market,
        crate::scripting::execution::ScriptOrderKind::Limit => crate::cli::TradeOrderKind::Limit,
    };
    let tif = match order_spec.tif {
        crate::scripting::execution::ScriptTimeInForce::Gtc => crate::cli::TradeTimeInForce::Gtc,
        crate::scripting::execution::ScriptTimeInForce::Ioc => crate::cli::TradeTimeInForce::Ioc,
        crate::scripting::execution::ScriptTimeInForce::Alo => crate::cli::TradeTimeInForce::Alo,
    };
    let account = match crate::providers::execution::ExecutionAdapter::configured_account(venue) {
        Ok(account) => account,
        Err(error) => {
            fail_script_order(paths, state, job_id, &order.id, &error)?;
            return Err(error);
        }
    };
    let venue_adapter =
        match crate::providers::execution::ExecutionAdapter::new(venue, job.definition.testnet)
            .await
        {
            Ok(adapter) => adapter,
            Err(error) => {
                fail_script_order(paths, state, job_id, &order.id, &error)?;
                return Err(error);
            }
        };
    let venue_arg = match venue {
        ExecutionVenue::Bulk => crate::cli::ExecutionVenueArg::Bulk,
        ExecutionVenue::Hyperliquid => crate::cli::ExecutionVenueArg::Hyperliquid,
        ExecutionVenue::HyperliquidSpot => crate::cli::ExecutionVenueArg::HyperliquidSpot,
        ExecutionVenue::HyperliquidXyz => crate::cli::ExecutionVenueArg::HyperliquidXyz,
        ExecutionVenue::HyperliquidOutcomes => crate::cli::ExecutionVenueArg::HyperliquidOutcomes,
    };
    let (args, direction) = match &request {
        ScriptManagedRequest::Trade(request) => {
            let snapshot = match venue_adapter.account_snapshot(&account).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    fail_script_order(paths, state, job_id, &order.id, &error)?;
                    return Err(error);
                }
            };
            let symbol_position = snapshot.positions.iter().find(|position| {
                position
                    .internal_symbol
                    .eq_ignore_ascii_case(&internal_symbol)
                    && position.size > f64::EPSILON
            });
            let target_direction = request.position.position_direction();
            let (size, margin, leverage) = if request.position.is_open() {
                if let Some(position) = symbol_position
                    && position.direction != target_direction
                {
                    let required_close = match position.direction {
                        crate::domain::execution::PositionDirection::Long => "close-long",
                        crate::domain::execution::PositionDirection::Short => "close-short",
                    };
                    let error = anyhow::anyhow!(
                        "ctx.trade {} cannot reverse an open {:?} position; submit {required_close} first",
                        request.position.as_str(),
                        position.direction
                    );
                    fail_script_order(paths, state, job_id, &order.id, &error)?;
                    return Err(error);
                }
                (request.size, request.margin, request.leverage_or_default())
            } else {
                let Some(position) =
                    symbol_position.filter(|position| position.direction == target_direction)
                else {
                    let error = anyhow::anyhow!(
                        "ctx.trade {} requires an open {:?} position for {}",
                        request.position.as_str(),
                        target_direction,
                        internal_symbol
                    );
                    fail_script_order(paths, state, job_id, &order.id, &error)?;
                    return Err(error);
                };
                let close_size = request.size.unwrap_or(position.size);
                if close_size > position.size + f64::EPSILON {
                    let error = anyhow::anyhow!(
                        "ctx.trade {} size {} exceeds the open position size {}",
                        request.position.as_str(),
                        close_size,
                        position.size
                    );
                    fail_script_order(paths, state, job_id, &order.id, &error)?;
                    return Err(error);
                }
                (Some(close_size), None, position.leverage.max(1.0))
            };
            (
                crate::cli::TradeArgs {
                    symbol: internal_symbol.clone(),
                    symbol_flag: None,
                    config: None,
                    venue: venue_arg,
                    testnet: job.definition.testnet,
                    size,
                    margin,
                    order_kind,
                    price: order_spec.price,
                    tif,
                    leverage: Some(leverage),
                    reduce_only: request.position.reduce_only(),
                    sl: request.sl,
                    tp: request.tp,
                    dry_run: false,
                    yes: true,
                    output: crate::cli::OutputFormat::Json,
                },
                request.position.order_direction(),
            )
        }
        ScriptManagedRequest::Order(request) => (
            crate::cli::TradeArgs {
                symbol: internal_symbol.clone(),
                symbol_flag: None,
                config: None,
                venue: venue_arg,
                testnet: job.definition.testnet,
                size: request.size,
                margin: request.margin,
                order_kind,
                price: order_spec.price,
                tif,
                leverage: match venue {
                    ExecutionVenue::HyperliquidSpot | ExecutionVenue::HyperliquidOutcomes => {
                        request.leverage
                    }
                    ExecutionVenue::Bulk
                    | ExecutionVenue::Hyperliquid
                    | ExecutionVenue::HyperliquidXyz => Some(request.leverage_or_default()),
                },
                reduce_only: request.reduce_only,
                sl: None,
                tp: None,
                dry_run: false,
                yes: true,
                output: crate::cli::OutputFormat::Json,
            },
            request.side.order_direction(),
        ),
    };
    let plan = match crate::commands::execution::build_trade_plan(&args, direction).await {
        Ok(plan) => plan,
        Err(error) => {
            fail_script_order(paths, state, job_id, &order.id, &error)?;
            return Err(error);
        }
    };
    ensure_account_supervisor(
        venue,
        plan.testnet,
        &plan.account,
        account_tx,
        account_supervisors,
    );
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
        } else if receipt.status == "submitted" {
            "order.submitted"
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
    let venue = current.venue;
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
    let market = crate::markets::exchange_market(execution_exchange(venue), &current.symbol)?;
    let plan = CancelPlan {
        created_at_ms: now_ms()?,
        venue,
        testnet: current.testnet,
        account: crate::providers::execution::ExecutionAdapter::configured_account(venue)?,
        internal_symbol: market.symbol.clone(),
        venue_symbol: market.venue_symbol.clone(),
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
        if receipt.terminal {
            managed.status = receipt.status.clone();
        } else {
            managed.cancel_requested = true;
        }
        managed.updated_at_ms = receipt.submitted_at_ms;
        managed.clone()
    };
    emit_script_event(
        paths,
        state,
        job_id,
        if receipt.terminal {
            "order.cancelled"
        } else {
            "order.cancel_requested"
        },
        Some(&managed),
        receipt.terminal,
        serde_json::to_value(&receipt)?,
    )?;
    persist_state(paths, state)?;
    Ok(managed)
}

async fn execute_strategy_trade(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    sequence: u64,
    plan: &TradePlan,
) -> Result<ExecutionReceipt> {
    if sequence == 0 {
        bail!("strategy child sequence must start at 1");
    }
    let job = state
        .strategy_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("strategy job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("strategy job `{job_id}` is not running");
    }
    validate_strategy_trade(&job.definition, sequence, plan)?;

    let execution_key = format!("{job_id}:{sequence}");
    if let Some(receipt) = state.strategy_executions.get(&execution_key) {
        return Ok(receipt.clone());
    }
    if sequence > 1 {
        let previous_key = format!("{job_id}:{}", sequence - 1);
        if !state.strategy_executions.contains_key(&previous_key) {
            bail!("strategy child order {sequence} was submitted out of sequence");
        }
    }

    let receipt = execute_trade(paths, adapter, state, plan, None).await?;
    state
        .strategy_executions
        .insert(execution_key, receipt.clone());
    persist_state(paths, state)?;
    Ok(receipt)
}

fn validate_strategy_trade(
    definition: &StrategyJobDefinition,
    sequence: u64,
    plan: &TradePlan,
) -> Result<()> {
    let (venue, symbol, side, total_size, leverage, reduce_only) = match definition {
        StrategyJobDefinition::Twap(definition) => (
            definition.venue,
            definition.symbol.as_str(),
            definition.side,
            definition.total_size,
            definition.leverage,
            definition.reduce_only,
        ),
        StrategyJobDefinition::Vwap(definition) => (
            definition.venue,
            definition.symbol.as_str(),
            definition.side,
            definition.total_size,
            definition.leverage,
            definition.reduce_only,
        ),
        StrategyJobDefinition::Oiwap(definition) => (
            definition.venue,
            definition.symbol.as_str(),
            definition.side,
            definition.total_size,
            definition.leverage,
            definition.reduce_only,
        ),
    };
    let expected_direction = match side {
        StrategySide::Buy => crate::domain::execution::PositionDirection::Long,
        StrategySide::Sell => crate::domain::execution::PositionDirection::Short,
    };
    if plan.venue != venue
        || plan.internal_symbol != symbol
        || plan.direction != expected_direction
        || plan.reduce_only != reduce_only
        || plan
            .leverage
            .is_none_or(|plan_leverage| (plan_leverage - leverage).abs() > f64::EPSILON)
        || plan.stop_loss_price.is_some()
        || plan.take_profit_price.is_some()
        || plan.size > total_size + 1e-12_f64.max(total_size.abs() * 1e-12)
    {
        bail!(
            "strategy child order does not match its persisted {} definition",
            definition.name()
        );
    }

    match definition {
        StrategyJobDefinition::Twap(definition) => {
            let child_orders = definition
                .duration_seconds
                .div_ceil(definition.interval_seconds);
            if sequence > child_orders {
                bail!("TWAP child sequence {sequence} exceeds schedule length {child_orders}");
            }
            let market = crate::markets::exchange_market(
                execution_exchange(definition.venue),
                &definition.symbol,
            )?;
            let rules = market.execution_rules()?;
            let schedule = crate::strategies::twap::TwapSchedule::build(
                definition.total_size,
                rules.lot_size,
                plan.reference_price,
                rules.min_notional,
                definition.duration_seconds,
                definition.interval_seconds,
            )?;
            let expected_size = schedule.children[(sequence - 1) as usize].size;
            if plan.order_kind != crate::domain::execution::OrderKind::Market
                || (plan.size - expected_size).abs() > 1e-12_f64.max(expected_size.abs() * 1e-12)
            {
                bail!("strategy child order does not match its persisted TWAP definition");
            }
        }
        StrategyJobDefinition::Vwap(_) | StrategyJobDefinition::Oiwap(_) => match plan.order_kind {
            crate::domain::execution::OrderKind::Market => {
                if plan.price.is_some() || plan.time_in_force.is_some() {
                    bail!("weighted strategy market child contains limit-order fields");
                }
            }
            crate::domain::execution::OrderKind::Limit => {
                if plan.price.is_none()
                    || plan.time_in_force != Some(crate::domain::execution::TimeInForce::Alo)
                {
                    bail!("weighted strategy maker children must be post-only ALO limit orders");
                }
            }
        },
    }
    Ok(())
}

async fn execute_strategy_cancel(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    sequence: u64,
    plan: &CancelPlan,
) -> Result<ExecutionReceipt> {
    if sequence == 0 {
        bail!("strategy cancellation sequence must start at 1");
    }
    let job = state
        .strategy_jobs
        .get(job_id)
        .with_context(|| format!("strategy job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("strategy job `{job_id}` is not running");
    }
    let order_prefix = format!("{job_id}:");
    if plan.venue != job.definition.venue()
        || plan.internal_symbol != job.definition.symbol()
        || !state.strategy_executions.iter().any(|(key, receipt)| {
            key.starts_with(&order_prefix)
                && receipt.order_id.as_deref() == Some(plan.order_id.as_str())
        })
    {
        bail!("strategy cannot cancel an order it does not own");
    }
    let cancellation_key = format!("{job_id}:{sequence}");
    if let Some(receipt) = state.strategy_cancellations.get(&cancellation_key) {
        return Ok(receipt.clone());
    }
    let receipt = execute_cancel_with_priority(paths, adapter, state, plan, true).await?;
    state
        .strategy_cancellations
        .insert(cancellation_key, receipt.clone());
    persist_state(paths, state)?;
    Ok(receipt)
}

async fn execute_bot_trade(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    sequence: u64,
    plan: &TradePlan,
) -> Result<ExecutionReceipt> {
    if sequence == 0 {
        bail!("bot order sequence must start at 1");
    }
    let job = state
        .bot_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("bot job `{job_id}` is not running");
    }
    validate_bot_trade(&job.definition, plan)?;

    let execution_key = format!("{job_id}:{sequence}");
    if let Some(receipt) = state.bot_executions.get(&execution_key) {
        return Ok(receipt.clone());
    }
    let receipt = execute_trade(paths, adapter, state, plan, None).await?;
    state.bot_executions.insert(execution_key, receipt.clone());
    persist_state(paths, state)?;
    Ok(receipt)
}

fn validate_bot_trade(definition: &BotJobDefinition, plan: &TradePlan) -> Result<()> {
    let (venue, leverage, name) = match definition {
        BotJobDefinition::Grid(definition) => (definition.venue, definition.leverage, "grid"),
        BotJobDefinition::MidPrice(definition) | BotJobDefinition::VolumeMid(definition) => {
            (definition.venue, definition.leverage, "mid-price")
        }
    };
    let maximum_order_size = definition.maximum_order_size();
    if plan.venue != venue
        || plan.testnet != definition.testnet()
        || !definition.accepts_symbol(&plan.internal_symbol)
        || match leverage {
            Some(leverage) => plan
                .leverage
                .is_none_or(|value| (value - leverage).abs() > f64::EPSILON),
            None => plan.leverage.is_some(),
        }
        || plan.stop_loss_price.is_some()
        || plan.take_profit_price.is_some()
        || plan.size > maximum_order_size + 1e-12_f64.max(maximum_order_size.abs() * 1e-12)
        || crate::domain::execution::OrderSide::from(plan.direction) != plan.side
    {
        bail!("bot order does not match its persisted {name} definition");
    }
    if let Some(outcome) = definition.outcome() {
        let expected_fingerprint = if plan.internal_symbol == outcome.primary_symbol {
            &outcome.primary_market_fingerprint
        } else {
            &outcome.complement_market_fingerprint
        };
        if plan.market_fingerprint.as_deref() != Some(expected_fingerprint)
            || plan.side != crate::domain::execution::OrderSide::Sell
        {
            bail!("outcome quote does not match its persisted market side");
        }
    }

    match plan.order_kind {
        crate::domain::execution::OrderKind::Limit => {
            if plan.reduce_only
                || plan.price.is_none()
                || plan.time_in_force != Some(crate::domain::execution::TimeInForce::Alo)
            {
                bail!("bot quotes must be non-reduce-only post-only ALO limit orders");
            }
        }
        crate::domain::execution::OrderKind::Market => {
            if plan.reduce_only || plan.price.is_some() || plan.time_in_force.is_some() {
                bail!("bot inventory exits must be non-reduce-only market orders");
            }
        }
    }
    Ok(())
}

async fn execute_bot_outcome_action(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    job_id: &str,
    sequence: u64,
    action: UserOutcomeAction,
) -> Result<serde_json::Value> {
    if sequence == 0 {
        bail!("bot outcome-action sequence must start at 1");
    }
    let job = state
        .bot_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("bot job `{job_id}` is not running");
    }
    let definition = job
        .definition
        .outcome()
        .context("only an outcome-enabled bot may submit outcome actions")?;

    let (outcome, amount) = match &action {
        UserOutcomeAction::Split { outcome, amount } => (*outcome, amount.as_str()),
        UserOutcomeAction::Merge {
            outcome,
            amount: Some(amount),
        } => (*outcome, amount.as_str()),
        UserOutcomeAction::Merge { amount: None, .. }
        | UserOutcomeAction::MergeQuestion { .. }
        | UserOutcomeAction::Negate { .. } => {
            bail!("outcome market maker permits only bounded split and merge actions")
        }
    };
    if outcome != definition.outcome_id {
        bail!("outcome action does not match its persisted bot definition");
    }
    let amount = amount
        .parse::<f64>()
        .context("outcome action amount is not a number")?;
    let tolerance = 1e-12_f64.max(definition.pair_size.abs() * 1e-12);
    if !amount.is_finite() || amount <= 0.0 || amount > definition.pair_size + tolerance {
        bail!("outcome action amount exceeds its persisted bot allocation");
    }

    let network = HyperliquidNetwork::from_testnet(job.definition.testnet());
    let primary =
        crate::providers::hyperliquid::outcomes::resolve(network, &definition.primary_symbol)
            .await?;
    let complement =
        crate::providers::hyperliquid::outcomes::resolve(network, &definition.complement_symbol)
            .await?;
    if primary.metadata_fingerprint != definition.primary_market_fingerprint
        || complement.metadata_fingerprint != definition.complement_market_fingerprint
    {
        bail!("outcome metadata changed after the bot was planned; refusing to sign");
    }

    let key = format!("{job_id}:{sequence}");
    if let Some(value) = state.bot_outcome_actions.get(&key) {
        return Ok(value.clone());
    }
    let value = crate::providers::execution::ExecutionAdapter::new(
        ExecutionVenue::HyperliquidOutcomes,
        job.definition.testnet(),
    )
    .await?
    .submit_user_outcome(action)
    .await?;
    state.bot_outcome_actions.insert(key, value.clone());
    persist_state(paths, state)?;
    Ok(value)
}

async fn execute_bot_cancel(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    sequence: u64,
    plan: &CancelPlan,
) -> Result<ExecutionReceipt> {
    if sequence == 0 {
        bail!("bot cancellation sequence must start at 1");
    }
    let job = state
        .bot_jobs
        .get(job_id)
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("bot job `{job_id}` is not running");
    }
    let order_prefix = format!("{job_id}:");
    if plan.venue != job.definition.venue()
        || plan.testnet != job.definition.testnet()
        || !job.definition.accepts_symbol(&plan.internal_symbol)
        || !state.bot_executions.iter().any(|(key, receipt)| {
            key.starts_with(&order_prefix)
                && receipt.order_id.as_deref() == Some(plan.order_id.as_str())
        })
    {
        bail!("bot cannot cancel an order it does not own");
    }
    let cancellation_key = format!("{job_id}:{sequence}");
    if let Some(receipt) = state.bot_cancellations.get(&cancellation_key) {
        return Ok(receipt.clone());
    }
    let receipt = execute_cancel_with_priority(paths, adapter, state, plan, true).await?;
    state
        .bot_cancellations
        .insert(cancellation_key, receipt.clone());
    persist_state(paths, state)?;
    Ok(receipt)
}

async fn execute_bot_trades(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    items: &[SequencedTradePlan],
) -> Result<Vec<ExecutionOutcome>> {
    if items.is_empty() {
        bail!("bot order batch cannot be empty");
    }
    let job = state
        .bot_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("bot job `{job_id}` is not running");
    }
    let mut sequences = HashSet::with_capacity(items.len());
    let mut outcomes = vec![None; items.len()];
    let mut pending_indices = Vec::new();
    let mut pending_plans = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if item.sequence == 0 || !sequences.insert(item.sequence) {
            bail!("bot order batch sequences must be unique and start at 1");
        }
        validate_bot_trade(&job.definition, &item.plan)?;
        let execution_key = format!("{job_id}:{}", item.sequence);
        if let Some(receipt) = state.bot_executions.get(&execution_key) {
            outcomes[index] = Some(ExecutionOutcome::success(receipt.clone()));
        } else {
            pending_indices.push(index);
            pending_plans.push(item.plan.clone());
        }
    }
    if !pending_plans.is_empty() {
        let batch = match pending_plans[0].venue {
            ExecutionVenue::Bulk => {
                adapter
                    .submit_trades(credentials::active_bulk_credential()?, &pending_plans)
                    .await?
            }
            ExecutionVenue::Hyperliquid
            | ExecutionVenue::HyperliquidSpot
            | ExecutionVenue::HyperliquidXyz
            | ExecutionVenue::HyperliquidOutcomes => {
                crate::providers::execution::ExecutionAdapter::new(
                    pending_plans[0].venue,
                    pending_plans[0].testnet,
                )
                .await?
                .submit_trades(&pending_plans)
                .await?
            }
        };
        if batch.len() != pending_plans.len() {
            bail!(
                "venue returned {} outcomes for {} bot orders",
                batch.len(),
                pending_plans.len()
            );
        }
        for ((index, plan), outcome) in pending_indices.into_iter().zip(&pending_plans).zip(batch) {
            if let Some(receipt) = outcome.receipt.as_ref() {
                record_trade_receipt(paths, state, plan, None, receipt)?;
                state.bot_executions.insert(
                    format!("{job_id}:{}", items[index].sequence),
                    receipt.clone(),
                );
            }
            outcomes[index] = Some(outcome);
        }
        persist_state(paths, state)?;
    }
    outcomes
        .into_iter()
        .map(|outcome| outcome.context("bot order batch outcome was not populated"))
        .collect()
}

async fn execute_bot_cancels(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
    items: &[SequencedCancelPlan],
) -> Result<Vec<ExecutionOutcome>> {
    if items.is_empty() {
        bail!("bot cancellation batch cannot be empty");
    }
    let job = state
        .bot_jobs
        .get(job_id)
        .with_context(|| format!("bot job `{job_id}` was not found"))?;
    if !job.status.is_active() {
        bail!("bot job `{job_id}` is not running");
    }
    let order_prefix = format!("{job_id}:");
    let mut sequences = HashSet::with_capacity(items.len());
    let mut outcomes = vec![None; items.len()];
    let mut pending_indices = Vec::new();
    let mut pending_plans = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if item.sequence == 0 || !sequences.insert(item.sequence) {
            bail!("bot cancellation batch sequences must be unique and start at 1");
        }
        if item.plan.venue != job.definition.venue()
            || item.plan.testnet != job.definition.testnet()
            || !job.definition.accepts_symbol(&item.plan.internal_symbol)
            || !state.bot_executions.iter().any(|(key, receipt)| {
                key.starts_with(&order_prefix)
                    && receipt.order_id.as_deref() == Some(item.plan.order_id.as_str())
            })
        {
            bail!("bot cannot cancel an order it does not own");
        }
        let cancellation_key = format!("{job_id}:{}", item.sequence);
        if let Some(receipt) = state.bot_cancellations.get(&cancellation_key) {
            outcomes[index] = Some(ExecutionOutcome::success(receipt.clone()));
        } else {
            pending_indices.push(index);
            pending_plans.push(item.plan.clone());
        }
    }
    if !pending_plans.is_empty() {
        let batch = match pending_plans[0].venue {
            ExecutionVenue::Bulk => {
                adapter
                    .cancel_orders(credentials::active_bulk_credential()?, &pending_plans)
                    .await?
            }
            ExecutionVenue::Hyperliquid
            | ExecutionVenue::HyperliquidSpot
            | ExecutionVenue::HyperliquidXyz
            | ExecutionVenue::HyperliquidOutcomes => {
                crate::providers::execution::ExecutionAdapter::new(
                    pending_plans[0].venue,
                    pending_plans[0].testnet,
                )
                .await?
                .cancel_orders_fast(&pending_plans)
                .await?
            }
        };
        if batch.len() != pending_plans.len() {
            bail!(
                "venue returned {} outcomes for {} bot cancellations",
                batch.len(),
                pending_plans.len()
            );
        }
        for ((index, plan), outcome) in pending_indices.into_iter().zip(&pending_plans).zip(batch) {
            if let Some(receipt) = outcome.receipt.as_ref() {
                record_cancel_receipt(paths, state, plan, receipt)?;
                state.bot_cancellations.insert(
                    format!("{job_id}:{}", items[index].sequence),
                    receipt.clone(),
                );
            }
            outcomes[index] = Some(outcome);
        }
        persist_state(paths, state)?;
    }
    outcomes
        .into_iter()
        .map(|outcome| outcome.context("bot cancellation batch outcome was not populated"))
        .collect()
}

async fn handle_connection(
    stream: BoxedRuntimeIo,
    required_auth: Option<&str>,
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    account_tx: &mpsc::Sender<AccountConnectionEvent>,
    account_supervisors: &mut HashSet<String>,
) -> Result<bool> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut line = String::new();
    BufReader::new(reader)
        .read_line(&mut line)
        .await
        .context("failed to read mlabd request")?;
    if line.len() > MAX_RUNTIME_REQUEST_BYTES {
        bail!("mlabd request exceeds the runtime request limit");
    }
    let incoming: IncomingRuntimeRequest = match serde_json::from_str(&line) {
        Ok(request) => request,
        Err(error) => {
            let message = format!("invalid mlabd request: {error}");
            record_runtime_error(paths, state, message.clone());
            let response = RuntimeResponse::error(message, state);
            let mut encoded =
                serde_json::to_vec(&response).context("failed to encode mlabd error response")?;
            encoded.push(b'\n');
            writer
                .write_all(&encoded)
                .await
                .context("failed to write mlabd error response")?;
            writer.shutdown().await.ok();
            return Ok(false);
        }
    };
    let (auth, request) = match incoming {
        IncomingRuntimeRequest::Authenticated(wire) => (wire.auth, wire.request),
        IncomingRuntimeRequest::Native(request) => (None, request),
    };
    if let Some(required_auth) = required_auth
        && !auth
            .as_deref()
            .is_some_and(|provided| tokens_equal(required_auth, provided))
    {
        let response = RuntimeResponse {
            ok: false,
            message: "unauthorized mlabd request".to_string(),
            ..RuntimeResponse::empty()
        };
        let mut encoded =
            serde_json::to_vec(&response).context("failed to encode mlabd error response")?;
        encoded.push(b'\n');
        writer
            .write_all(&encoded)
            .await
            .context("failed to write mlabd error response")?;
        writer.shutdown().await.ok();
        return Ok(false);
    }
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
        RuntimeRequest::ReloadMarkets => match crate::markets::reload() {
            Ok(()) => RuntimeResponse {
                ok: true,
                message: "market snapshots reloaded".to_string(),
                status: Some(runtime_status(state)),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse {
                ok: false,
                message: format!("{error:#}"),
                status: Some(runtime_status(state)),
                ..RuntimeResponse::empty()
            },
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
            state.tracked_orders.insert(
                tracked_order_key(order.venue, order.testnet, &order.order_id),
                order,
            );
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
            ensure_account_supervisor(
                plan.venue,
                plan.testnet,
                &plan.account,
                account_tx,
                account_supervisors,
            );
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
            ensure_account_supervisor(
                plan.venue,
                plan.testnet,
                &plan.account,
                account_tx,
                account_supervisors,
            );
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
            if let Some(venue) = submission.venue
                && let Ok(account) =
                    crate::providers::execution::ExecutionAdapter::configured_account(venue)
            {
                ensure_account_supervisor(
                    venue,
                    submission.testnet,
                    &account,
                    account_tx,
                    account_supervisors,
                );
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
            match stop_script_job_in_daemon(paths, adapter, state, &job_id).await {
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
            match restart_script_job_in_daemon(paths, adapter, state, &job_id).await {
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
            match mark_script_worker_finished(paths, adapter, state, &job_id, pid, error).await {
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
            exchange,
            request,
        } => match execute_script_order(
            paths,
            adapter,
            state,
            account_tx,
            account_supervisors,
            ScriptOrderOperation {
                job_id: &job_id,
                order,
                exchange,
                request: ScriptManagedRequest::Trade(request),
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
        RuntimeRequest::ScriptExecuteOrder {
            job_id,
            order,
            exchange,
            request,
        } => match execute_script_order(
            paths,
            adapter,
            state,
            account_tx,
            account_supervisors,
            ScriptOrderOperation {
                job_id: &job_id,
                order,
                exchange,
                request: ScriptManagedRequest::Order(request),
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
        RuntimeRequest::ScriptCancelAllOrders { job_id } => {
            let active = state
                .script_jobs
                .get(&job_id)
                .is_some_and(|job| job.status.is_active());
            if !active {
                RuntimeResponse::error(format!("active script job `{job_id}` was not found"), state)
            } else {
                match cancel_script_job_orders(paths, adapter, state, &job_id).await {
                    Ok(()) => RuntimeResponse {
                        ok: true,
                        message: "script managed orders cancelled".to_string(),
                        status: Some(runtime_status(state)),
                        ..RuntimeResponse::empty()
                    },
                    Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
                }
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
        RuntimeRequest::ScriptPositions { job_id } => {
            match script_positions_in_daemon(paths, adapter, state, &job_id).await {
                Ok(positions) => RuntimeResponse {
                    ok: true,
                    message: "script positions".to_string(),
                    status: Some(runtime_status(state)),
                    script_positions: Some(positions),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::SubmitStrategyJob { submission } => {
            let venue = submission.definition.venue();
            let testnet = submission.definition.testnet();
            if let Ok(account) =
                crate::providers::execution::ExecutionAdapter::configured_account(venue)
            {
                ensure_account_supervisor(
                    venue,
                    testnet,
                    &account,
                    account_tx,
                    account_supervisors,
                );
            }
            match create_strategy_job(paths, state, submission) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "strategy job submitted".to_string(),
                    status: Some(runtime_status(state)),
                    strategy_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ListStrategyJobs => RuntimeResponse {
            ok: true,
            message: "strategy jobs".to_string(),
            status: Some(runtime_status(state)),
            strategy_jobs: Some(state.strategy_jobs.values().cloned().collect()),
            ..RuntimeResponse::empty()
        },
        RuntimeRequest::GetStrategyJob { job_id } => {
            match state.strategy_jobs.get(&job_id).cloned() {
                Some(job) => RuntimeResponse {
                    ok: true,
                    message: "strategy job".to_string(),
                    status: Some(runtime_status(state)),
                    strategy_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                None => {
                    RuntimeResponse::error(format!("strategy job `{job_id}` was not found"), state)
                }
            }
        }
        RuntimeRequest::StopStrategyJob { job_id } => {
            match stop_strategy_job_in_daemon(paths, state, &job_id) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "strategy job stopped".to_string(),
                    status: Some(runtime_status(state)),
                    strategy_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::StrategyWorkerStarted { job_id, pid } => {
            match mark_strategy_worker_started(paths, state, &job_id, pid) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "strategy worker running".to_string(),
                    status: Some(runtime_status(state)),
                    strategy_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::StrategyWorkerHeartbeat { job_id, pid } => {
            match mark_strategy_worker_heartbeat(paths, state, &job_id, pid) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "strategy worker heartbeat".to_string(),
                    status: Some(runtime_status(state)),
                    strategy_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::StrategyWorkerFinished { job_id, pid, error } => {
            match mark_strategy_worker_finished(paths, state, &job_id, pid, error) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "strategy worker finished".to_string(),
                    status: Some(runtime_status(state)),
                    strategy_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::StrategyExecuteTrade {
            job_id,
            sequence,
            plan,
        } => {
            ensure_account_supervisor(
                plan.venue,
                plan.testnet,
                &plan.account,
                account_tx,
                account_supervisors,
            );
            match execute_strategy_trade(paths, adapter, state, &job_id, sequence, &plan).await {
                Ok(receipt) => RuntimeResponse {
                    ok: true,
                    message: "strategy child order processed".to_string(),
                    status: Some(runtime_status(state)),
                    receipt: Some(receipt),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::StrategyCancelOrder {
            job_id,
            sequence,
            plan,
        } => match execute_strategy_cancel(paths, adapter, state, &job_id, sequence, &plan).await {
            Ok(receipt) => RuntimeResponse {
                ok: true,
                message: "strategy order cancelled".to_string(),
                status: Some(runtime_status(state)),
                receipt: Some(receipt),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
        },
        RuntimeRequest::SubmitBotJob { submission } => {
            let venue = submission.definition.venue();
            let testnet = submission.definition.testnet();
            if let Ok(account) =
                crate::providers::execution::ExecutionAdapter::configured_account(venue)
            {
                ensure_account_supervisor(
                    venue,
                    testnet,
                    &account,
                    account_tx,
                    account_supervisors,
                );
            }
            match create_bot_job(paths, state, submission).await {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "bot job submitted".to_string(),
                    status: Some(runtime_status(state)),
                    bot_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::ListBotJobs => RuntimeResponse {
            ok: true,
            message: "bot jobs".to_string(),
            status: Some(runtime_status(state)),
            bot_jobs: Some(state.bot_jobs.values().cloned().collect()),
            ..RuntimeResponse::empty()
        },
        RuntimeRequest::GetBotJob { job_id } => match state.bot_jobs.get(&job_id).cloned() {
            Some(job) => RuntimeResponse {
                ok: true,
                message: "bot job".to_string(),
                status: Some(runtime_status(state)),
                bot_job: Some(job),
                ..RuntimeResponse::empty()
            },
            None => RuntimeResponse::error(format!("bot job `{job_id}` was not found"), state),
        },
        RuntimeRequest::StopBotJob { job_id } => {
            match stop_bot_job_in_daemon(paths, state, &job_id) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "bot job stopping".to_string(),
                    status: Some(runtime_status(state)),
                    bot_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::BotWorkerStarted { job_id, pid } => {
            match mark_bot_worker_started(paths, state, &job_id, pid) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "bot worker running".to_string(),
                    status: Some(runtime_status(state)),
                    bot_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::BotWorkerHeartbeat {
            job_id,
            pid,
            performance,
        } => match mark_bot_worker_heartbeat(paths, state, &job_id, pid, performance) {
            Ok(job) => RuntimeResponse {
                ok: true,
                message: "bot worker heartbeat".to_string(),
                status: Some(runtime_status(state)),
                bot_job: Some(job),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
        },
        RuntimeRequest::BotWorkerFinished { job_id, pid, error } => {
            match mark_bot_worker_finished(paths, state, &job_id, pid, error) {
                Ok(job) => RuntimeResponse {
                    ok: true,
                    message: "bot worker finished".to_string(),
                    status: Some(runtime_status(state)),
                    bot_job: Some(job),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::BotExecuteTrade {
            job_id,
            sequence,
            plan,
        } => {
            ensure_account_supervisor(
                plan.venue,
                plan.testnet,
                &plan.account,
                account_tx,
                account_supervisors,
            );
            match execute_bot_trade(paths, adapter, state, &job_id, sequence, &plan).await {
                Ok(receipt) => RuntimeResponse {
                    ok: true,
                    message: "bot order processed".to_string(),
                    status: Some(runtime_status(state)),
                    receipt: Some(receipt),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::BotCancelOrder {
            job_id,
            sequence,
            plan,
        } => match execute_bot_cancel(paths, adapter, state, &job_id, sequence, &plan).await {
            Ok(receipt) => RuntimeResponse {
                ok: true,
                message: "bot order cancellation processed".to_string(),
                status: Some(runtime_status(state)),
                receipt: Some(receipt),
                ..RuntimeResponse::empty()
            },
            Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
        },
        RuntimeRequest::BotExecuteTrades { job_id, items } => {
            for item in &items {
                ensure_account_supervisor(
                    item.plan.venue,
                    item.plan.testnet,
                    &item.plan.account,
                    account_tx,
                    account_supervisors,
                );
            }
            match execute_bot_trades(paths, adapter, state, &job_id, &items).await {
                Ok(outcomes) => RuntimeResponse {
                    ok: true,
                    message: "bot order batch processed".to_string(),
                    status: Some(runtime_status(state)),
                    outcomes: Some(outcomes),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::BotCancelOrders { job_id, items } => {
            match execute_bot_cancels(paths, adapter, state, &job_id, &items).await {
                Ok(outcomes) => RuntimeResponse {
                    ok: true,
                    message: "bot cancellation batch processed".to_string(),
                    status: Some(runtime_status(state)),
                    outcomes: Some(outcomes),
                    ..RuntimeResponse::empty()
                },
                Err(error) => RuntimeResponse::error(format!("{error:#}"), state),
            }
        }
        RuntimeRequest::BotOutcomeAction {
            job_id,
            sequence,
            action,
        } => match execute_bot_outcome_action(paths, state, &job_id, sequence, action).await {
            Ok(value) => RuntimeResponse {
                ok: true,
                message: "bot outcome action processed".to_string(),
                status: Some(runtime_status(state)),
                action_response: Some(value),
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
    let receipt = match plan.venue {
        ExecutionVenue::Bulk => {
            adapter
                .submit_trade(credentials::active_bulk_credential()?, plan)
                .await?
        }
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => {
            crate::providers::execution::ExecutionAdapter::new(plan.venue, plan.testnet)
                .await?
                .submit_trade(plan)
                .await?
        }
    };
    record_trade_receipt(paths, state, plan, script_order_id, &receipt)?;
    Ok(receipt)
}

fn record_trade_receipt(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    plan: &TradePlan,
    script_order_id: Option<String>,
    receipt: &ExecutionReceipt,
) -> Result<()> {
    if let Err(error) = append_json_line(
        &paths.events,
        &TradeSubmissionEvent {
            ts_ms: now_ms()?,
            event: "order_submitted",
            plan,
            receipt,
        },
    ) {
        eprintln!("execution journal warning: {error:#}");
    }
    if !receipt.terminal {
        let order_id = receipt
            .order_id
            .as_deref()
            .context("non-terminal execution receipt omitted its order id")?;
        let order = TrackedOrder {
            venue: plan.venue,
            testnet: plan.testnet,
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
        state.tracked_orders.insert(
            tracked_order_key(order.venue, order.testnet, &order.order_id),
            order,
        );
        persist_state(paths, state)?;
    }
    Ok(())
}

async fn execute_cancel(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    plan: &CancelPlan,
) -> Result<ExecutionReceipt> {
    execute_cancel_with_priority(paths, adapter, state, plan, false).await
}

async fn execute_cancel_with_priority(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    plan: &CancelPlan,
    fast: bool,
) -> Result<ExecutionReceipt> {
    let expected_venue_symbol = if plan.venue == ExecutionVenue::HyperliquidOutcomes {
        crate::providers::hyperliquid::outcomes::resolve(
            crate::providers::hyperliquid::HyperliquidNetwork::from_testnet(plan.testnet),
            &plan.internal_symbol,
        )
        .await?
        .coin
    } else {
        let market =
            crate::markets::exchange_market(execution_exchange(plan.venue), &plan.internal_symbol)?;
        if matches!(
            plan.venue,
            ExecutionVenue::HyperliquidSpot | ExecutionVenue::HyperliquidXyz
        ) {
            market
                .network_variant(HyperliquidNetwork::from_testnet(plan.testnet).label())?
                .venue_symbol
        } else {
            market.venue_symbol.clone()
        }
    };
    if expected_venue_symbol != plan.venue_symbol {
        bail!("cancel plan symbol mapping does not match the installed market snapshot");
    }
    let configured = crate::providers::execution::ExecutionAdapter::configured_account(plan.venue)?;
    if !configured.eq_ignore_ascii_case(&plan.account) {
        bail!("cancel plan account no longer matches the configured venue account");
    }
    let receipt = match plan.venue {
        ExecutionVenue::Bulk => {
            adapter
                .cancel_order(
                    credentials::active_bulk_credential()?,
                    &plan.venue_symbol,
                    &plan.order_id,
                )
                .await?
        }
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => {
            let adapter =
                crate::providers::execution::ExecutionAdapter::new(plan.venue, plan.testnet)
                    .await?;
            if fast {
                adapter.cancel_order_fast(plan).await?
            } else {
                adapter.cancel_order(plan).await?
            }
        }
    };
    record_cancel_receipt(paths, state, plan, &receipt)?;
    Ok(receipt)
}

fn record_cancel_receipt(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    plan: &CancelPlan,
    receipt: &ExecutionReceipt,
) -> Result<()> {
    if let Err(error) = append_json_line(
        &paths.events,
        &CancelSubmissionEvent {
            ts_ms: now_ms()?,
            event: "order_cancelled",
            plan,
            receipt,
        },
    ) {
        eprintln!("execution journal warning: {error:#}");
    }
    if receipt.terminal
        && state
            .tracked_orders
            .remove(&tracked_order_key(plan.venue, plan.testnet, &plan.order_id))
            .is_some()
    {
        persist_state(paths, state)?;
    }
    Ok(())
}

fn ensure_account_supervisor(
    venue: ExecutionVenue,
    testnet: bool,
    account: &str,
    sender: &mpsc::Sender<AccountConnectionEvent>,
    supervisors: &mut HashSet<String>,
) {
    let key = account_cache_key(venue, testnet, account);
    if !supervisors.insert(key) {
        return;
    }
    let account = account.to_string();
    let sender = sender.clone();
    tokio::spawn(async move {
        supervise_account_stream(venue, testnet, account, sender).await;
    });
}

fn network_label(venue: ExecutionVenue, testnet: bool) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "testnet",
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes
            if testnet =>
        {
            "testnet"
        }
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => "mainnet",
    }
}

fn account_cache_key(venue: ExecutionVenue, testnet: bool, account: &str) -> String {
    format!(
        "{}:{}:{account}",
        execution_exchange(venue),
        network_label(venue, testnet)
    )
}

fn tracked_order_key(venue: ExecutionVenue, testnet: bool, order_id: &str) -> String {
    format!(
        "{}:{}:{order_id}",
        execution_exchange(venue),
        network_label(venue, testnet)
    )
}

fn execution_exchange(venue: ExecutionVenue) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "bulkf",
        ExecutionVenue::Hyperliquid => "hyperliquidf",
        ExecutionVenue::HyperliquidSpot => "hyperliquid",
        ExecutionVenue::HyperliquidXyz => "hyperliquidf-xyz",
        ExecutionVenue::HyperliquidOutcomes => "hyperliquid-outcomes",
    }
}

enum VenueAccountStream {
    Bulk(BulkAccountStream),
    Hyperliquid(HyperliquidAccountStream),
}

impl VenueAccountStream {
    async fn connect(venue: ExecutionVenue, testnet: bool, account: &str) -> Result<Self> {
        match venue {
            ExecutionVenue::Bulk => Ok(Self::Bulk(BulkAccountStream::connect(account).await?)),
            ExecutionVenue::Hyperliquid | ExecutionVenue::HyperliquidXyz => Ok(Self::Hyperliquid(
                HyperliquidAccountStream::connect_on(
                    account,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
            ExecutionVenue::HyperliquidSpot | ExecutionVenue::HyperliquidOutcomes => {
                Ok(Self::Hyperliquid(
                    HyperliquidAccountStream::connect_on(
                        account,
                        HyperliquidNetwork::from_testnet(testnet),
                    )
                    .await?,
                ))
            }
        }
    }

    async fn next_event(&mut self) -> Result<serde_json::Value> {
        match self {
            Self::Bulk(stream) => stream.next_event().await,
            Self::Hyperliquid(stream) => stream.next_event().await,
        }
    }
}

async fn supervise_account_stream(
    venue: ExecutionVenue,
    testnet: bool,
    account: String,
    sender: mpsc::Sender<AccountConnectionEvent>,
) {
    let mut connected_once = false;
    let mut reconnect_delay_secs = 1_u64;
    loop {
        match VenueAccountStream::connect(venue, testnet, &account).await {
            Ok(mut stream) => {
                if sender
                    .send(AccountConnectionEvent::Connected {
                        venue,
                        testnet,
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
                                    venue,
                                    testnet,
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
                                    venue,
                                    testnet,
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
                        venue,
                        testnet,
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
            venue,
            testnet,
            account,
            reconnected,
        } => {
            state.account_stream_connected = true;
            state.last_error = None;
            refresh_account_positions(venue, testnet, adapter, state, &account, true).await?;
            persist_state(paths, state)?;
            if reconnected {
                recover_account_gap(venue, testnet, paths, adapter, state, &account).await?;
            }
        }
        AccountConnectionEvent::Disconnected {
            venue,
            testnet,
            account,
            error,
        } => {
            state.account_stream_connected = false;
            state.account_disconnected_at_ms = Some(now_ms()?);
            record_runtime_error(
                paths,
                state,
                format!(
                    "{} {} account WebSocket disconnected for {account}: {error}",
                    execution_exchange(venue),
                    network_label(venue, testnet),
                ),
            );
        }
        AccountConnectionEvent::Data {
            venue,
            testnet,
            account,
            data,
        } => {
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
            apply_account_event(
                venue,
                testnet,
                paths,
                state,
                &account,
                &data,
                received_at_ms,
            )?;
            if matches!(
                venue,
                ExecutionVenue::Hyperliquid
                    | ExecutionVenue::HyperliquidSpot
                    | ExecutionVenue::HyperliquidXyz
            ) && data.get("channel").and_then(serde_json::Value::as_str) == Some("user")
            {
                refresh_account_positions(venue, testnet, adapter, state, &account, true).await?;
            }
            persist_state(paths, state)?;
        }
    }
    Ok(())
}

async fn refresh_account_positions(
    venue: ExecutionVenue,
    testnet: bool,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    account: &str,
    force: bool,
) -> Result<()> {
    let now = now_ms()?;
    if !force
        && state
            .account_positions_refreshed_at_ms
            .get(&account_cache_key(venue, testnet, account))
            .is_some_and(|last| now.saturating_sub(*last) < 250)
    {
        return Ok(());
    }
    let snapshot = match venue {
        ExecutionVenue::Bulk => adapter.account_snapshot(account).await?,
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => {
            crate::providers::execution::ExecutionAdapter::new(venue, testnet)
                .await?
                .account_snapshot(account)
                .await?
        }
    };
    let cache_key = account_cache_key(venue, testnet, account);
    state
        .account_positions
        .insert(cache_key.clone(), snapshot.positions);
    state
        .account_positions_refreshed_at_ms
        .insert(cache_key, snapshot.fetched_at_ms);
    Ok(())
}

async fn script_positions_in_daemon(
    paths: &RuntimePaths,
    adapter: &BulkExecutionAdapter,
    state: &mut RuntimeState,
    job_id: &str,
) -> Result<Vec<Position>> {
    let job = state
        .script_jobs
        .get(job_id)
        .cloned()
        .with_context(|| format!("script job `{job_id}` was not found"))?;
    let mut venues = job.definition.venue.into_iter().collect::<Vec<_>>();
    for venue in state
        .script_orders
        .values()
        .filter(|order| order.job_id == job_id)
        .map(|order| order.venue)
    {
        if !venues.contains(&venue) {
            venues.push(venue);
        }
    }
    if venues.is_empty() {
        return Ok(Vec::new());
    }
    let source_symbols = crate::scripting::inputs::parse_source_configs(&job.definition.sources)?
        .values()
        .map(crate::scripting::inputs::SourceConfig::market_symbol)
        .collect::<HashSet<_>>();
    let mut positions = Vec::new();
    for venue in venues {
        let account = crate::providers::execution::ExecutionAdapter::configured_account(venue)?;
        refresh_account_positions(
            venue,
            job.definition.testnet,
            adapter,
            state,
            &account,
            false,
        )
        .await?;
        positions.extend(
            state
                .account_positions
                .get(&account_cache_key(venue, job.definition.testnet, &account))
                .into_iter()
                .flatten()
                .filter(|position| {
                    source_symbols
                        .iter()
                        .any(|symbol| position.internal_symbol.eq_ignore_ascii_case(symbol))
                })
                .cloned(),
        );
    }
    persist_state(paths, state)?;
    Ok(positions)
}

fn apply_account_event(
    venue: ExecutionVenue,
    testnet: bool,
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    account: &str,
    data: &serde_json::Value,
    received_at_ms: u64,
) -> Result<()> {
    match venue {
        ExecutionVenue::Bulk => {
            apply_bulk_account_event(paths, state, account, data, received_at_ms)
        }
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => apply_hyperliquid_account_event(
            venue,
            testnet,
            paths,
            state,
            account,
            data,
            received_at_ms,
        ),
    }
}

fn apply_bulk_account_event(
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
                        (ExecutionVenue::Bulk, false),
                        account,
                        order_id,
                        status,
                        received_at_ms,
                    )?;
                    apply_script_order_status(
                        paths,
                        state,
                        (ExecutionVenue::Bulk, false),
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
            apply_tracked_order_status(
                paths,
                state,
                (ExecutionVenue::Bulk, false),
                account,
                order_id,
                status,
                event_ms,
            )?;
            apply_script_order_status(
                paths,
                state,
                (ExecutionVenue::Bulk, false),
                order_id,
                status,
                event_ms,
                data.clone(),
            )?;
        }
        _ => {}
    }
    route_account_event_to_scripts(ExecutionVenue::Bulk, false, paths, state, data)?;
    Ok(())
}

fn apply_hyperliquid_account_event(
    venue: ExecutionVenue,
    testnet: bool,
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    account: &str,
    data: &serde_json::Value,
    received_at_ms: u64,
) -> Result<()> {
    match data.get("channel").and_then(serde_json::Value::as_str) {
        Some("orderUpdates") => {
            let updates = data
                .get("data")
                .and_then(serde_json::Value::as_array)
                .context("Hyperliquid orderUpdates omitted its update list")?;
            for update in updates {
                let order = update
                    .get("order")
                    .context("Hyperliquid order update omitted order")?;
                let Some(coin) = order.get("coin").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                if !hyperliquid_event_matches_venue(venue, testnet, coin) {
                    continue;
                }
                let order_id = value_identifier(
                    order
                        .get("oid")
                        .context("Hyperliquid order update omitted oid")?,
                )?;
                let raw_status = update
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .context("Hyperliquid order update omitted status")?;
                let status = normalize_hyperliquid_order_status(raw_status);
                let event_ms = update
                    .get("statusTimestamp")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(received_at_ms);
                apply_tracked_order_status(
                    paths,
                    state,
                    (venue, testnet),
                    account,
                    &order_id,
                    status,
                    event_ms,
                )?;
                apply_script_order_status(
                    paths,
                    state,
                    (venue, testnet),
                    &order_id,
                    status,
                    event_ms,
                    update.clone(),
                )?;
            }
        }
        Some("user") => {
            let event = data
                .get("data")
                .context("Hyperliquid user event omitted data")?;
            if let Some(fills) = event.get("fills").and_then(serde_json::Value::as_array) {
                for fill in fills {
                    let Some(coin) = fill.get("coin").and_then(serde_json::Value::as_str) else {
                        continue;
                    };
                    if !hyperliquid_event_matches_venue(venue, testnet, coin) {
                        continue;
                    }
                    let product = hyperliquid_product(venue);
                    let symbol = crate::providers::hyperliquid::markets::market_for_wire(
                        product,
                        HyperliquidNetwork::from_testnet(testnet),
                        coin,
                    )
                    .map(|market| market.symbol.clone())
                    .unwrap_or_else(|_| coin.to_string());
                    let mut normalized = serde_json::json!({
                        "type": "fill",
                        "venue": execution_exchange(venue),
                        "symbol": symbol,
                        "price": fill.get("px").cloned().unwrap_or(serde_json::Value::Null),
                        "size": fill.get("sz").cloned().unwrap_or(serde_json::Value::Null),
                        "side": fill.get("side").cloned().unwrap_or(serde_json::Value::Null),
                        "fee": fill.get("fee").cloned().unwrap_or(serde_json::Value::Null),
                        "feeAsset": fill.get("feeToken").cloned().unwrap_or(serde_json::Value::Null),
                        "timestamp": fill.get("time").cloned().unwrap_or(serde_json::Value::Null),
                        "raw": fill,
                    });
                    if let Some(oid) = fill.get("oid") {
                        normalized.as_object_mut().expect("object").insert(
                            "orderId".to_string(),
                            serde_json::Value::String(value_identifier(oid)?),
                        );
                    }
                    route_account_event_to_scripts(venue, testnet, paths, state, &normalized)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn hyperliquid_product(venue: ExecutionVenue) -> crate::providers::hyperliquid::HyperliquidProduct {
    match venue {
        ExecutionVenue::Hyperliquid => crate::providers::hyperliquid::HyperliquidProduct::Perpetual,
        ExecutionVenue::HyperliquidSpot => crate::providers::hyperliquid::HyperliquidProduct::Spot,
        ExecutionVenue::HyperliquidOutcomes => {
            crate::providers::hyperliquid::HyperliquidProduct::Outcome
        }
        ExecutionVenue::HyperliquidXyz => {
            crate::providers::hyperliquid::HyperliquidProduct::XyzPerpetual
        }
        ExecutionVenue::Bulk => unreachable!("BULK is not a Hyperliquid product"),
    }
}

fn hyperliquid_event_matches_venue(venue: ExecutionVenue, testnet: bool, coin: &str) -> bool {
    if venue == ExecutionVenue::HyperliquidOutcomes {
        return crate::providers::hyperliquid::outcomes::parse_wire_symbol(coin).is_ok();
    }
    crate::providers::hyperliquid::markets::market_for_wire(
        hyperliquid_product(venue),
        HyperliquidNetwork::from_testnet(testnet),
        coin,
    )
    .is_ok()
}

fn value_identifier(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        _ => bail!("execution order id is neither a string nor an integer"),
    }
}

fn normalize_hyperliquid_order_status(status: &str) -> &str {
    if status.eq_ignore_ascii_case("open") {
        "resting"
    } else if status.eq_ignore_ascii_case("filled") {
        "filled"
    } else if status.eq_ignore_ascii_case("canceled") || status.eq_ignore_ascii_case("cancelled") {
        "cancelled"
    } else if status.eq_ignore_ascii_case("rejected") || status.ends_with("Canceled") {
        "rejected"
    } else {
        status
    }
}

fn apply_script_order_status(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    route: (ExecutionVenue, bool),
    venue_order_id: &str,
    status: &str,
    event_ms: u64,
    data: serde_json::Value,
) -> Result<()> {
    let (venue, testnet) = route;
    let local_ids = state
        .script_orders
        .iter()
        .filter(|(_, order)| {
            order.venue == venue
                && order.testnet == testnet
                && order.venue_order_id.as_deref() == Some(venue_order_id)
        })
        .map(|(local_id, _)| local_id.clone())
        .collect::<Vec<_>>();
    for local_id in local_ids {
        let (job_id, managed, changed) = {
            let managed = state
                .script_orders
                .get_mut(&local_id)
                .context("script order disappeared while applying account event")?;
            let changed = should_apply_script_order_status(&managed.status, status);
            if changed {
                managed.status = status.to_string();
                managed.updated_at_ms = event_ms;
            }
            (managed.job_id.clone(), managed.clone(), changed)
        };
        if changed {
            let event_type = if status == "filled" {
                "order.filled"
            } else if status.starts_with("cancelled") || status == "siblingCancelled" {
                "order.cancelled"
            } else if status.starts_with("rejected") || status == "triggerFailed" {
                "order.rejected"
            } else if matches!(status, "placed" | "resting") {
                "order.accepted"
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
    venue: ExecutionVenue,
    testnet: bool,
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
        .and_then(|symbol| crate::markets::exchange_market(execution_exchange(venue), symbol).ok())
        .map(|market| market.symbol.clone());
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
            .filter(|order| {
                order.venue == venue
                    && order.testnet == testnet
                    && order.venue_order_id.as_deref() == Some(venue_order_id)
            })
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
                && (job.definition.venue == Some(venue)
                    || state
                        .script_orders
                        .values()
                        .any(|order| order.job_id == job.id && order.venue == venue))
                && job.definition.testnet == testnet
                && internal_symbol
                    .as_deref()
                    .is_none_or(|symbol| script_job_tracks_symbol(job, symbol))
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

fn script_job_tracks_symbol(job: &ScriptJob, symbol: &str) -> bool {
    crate::scripting::inputs::parse_source_configs(&job.definition.sources)
        .map(|configs| {
            configs
                .values()
                .any(|config| config.market_symbol().eq_ignore_ascii_case(symbol))
        })
        .unwrap_or(false)
}

fn apply_tracked_order_status(
    paths: &RuntimePaths,
    state: &mut RuntimeState,
    route: (ExecutionVenue, bool),
    account: &str,
    order_id: &str,
    status: &str,
    event_ms: u64,
) -> Result<()> {
    let (venue, testnet) = route;
    let key = state
        .tracked_orders
        .iter()
        .find(|(_, order)| {
            order.venue == venue
                && order.testnet == testnet
                && order.account == account
                && order.order_id == order_id
        })
        .map(|(key, _)| key.clone());
    let Some(key) = key else {
        return Ok(());
    };
    let order = state
        .tracked_orders
        .get_mut(&key)
        .context("tracked order disappeared while applying account status")?;
    let changed = order.status != status;
    order.status = status.to_string();
    order.updated_at_ms = event_ms;
    let snapshot = order.clone();
    if changed {
        append_runtime_event(paths, "order_status", &snapshot)?;
    }
    if is_terminal_order_status(status) {
        state.tracked_orders.remove(&key);
    }
    Ok(())
}

async fn recover_account_gap(
    venue: ExecutionVenue,
    testnet: bool,
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
    match venue {
        ExecutionVenue::Bulk => {
            for record in adapter
                .order_history(account)
                .await?
                .into_iter()
                .filter(|record| record.ts_ms >= gap_started_ms)
            {
                apply_tracked_order_status(
                    paths,
                    state,
                    (ExecutionVenue::Bulk, false),
                    account,
                    &record.order_id,
                    &record.status,
                    record.ts_ms,
                )?;
                apply_script_order_status(
                    paths,
                    state,
                    (ExecutionVenue::Bulk, false),
                    &record.order_id,
                    &record.status,
                    record.ts_ms,
                    serde_json::to_value(&record)?,
                )?;
            }
        }
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => {
            let execution =
                crate::providers::execution::ExecutionAdapter::new(venue, testnet).await?;
            for order in execution.open_orders(account).await? {
                apply_tracked_order_status(
                    paths,
                    state,
                    (venue, testnet),
                    account,
                    &order.order_id,
                    "resting",
                    order.ts_ms,
                )?;
                apply_script_order_status(
                    paths,
                    state,
                    (venue, testnet),
                    &order.order_id,
                    "resting",
                    order.ts_ms,
                    serde_json::to_value(&order)?,
                )?;
            }
        }
    }

    let fills = match venue {
        ExecutionVenue::Bulk => adapter.fills(account).await?,
        ExecutionVenue::Hyperliquid
        | ExecutionVenue::HyperliquidSpot
        | ExecutionVenue::HyperliquidXyz
        | ExecutionVenue::HyperliquidOutcomes => {
            crate::providers::execution::ExecutionAdapter::new(venue, testnet)
                .await?
                .fills(account)
                .await?
        }
    };
    for fill in fills
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
        route_account_event_to_scripts(venue, testnet, paths, state, &routed)?;
    }
    state.last_recovery_ms = Some(now_ms()?);
    state.account_disconnected_at_ms = None;
    persist_state(paths, state)
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status,
        "filled"
            | "rejected"
            | "error"
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

fn should_apply_script_order_status(current: &str, incoming: &str) -> bool {
    current != incoming && !is_terminal_order_status(current)
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
    let config = daemon::load()?;
    let endpoint_override = std::env::var("MLAB_DAEMON_ENDPOINT").ok();
    if config.backend == DaemonBackend::Docker || endpoint_override.is_some() {
        let endpoint = endpoint_override
            .unwrap_or_else(|| format!("{}:{}", config.docker.host, config.docker.port));
        let stream = match TcpStream::connect(&endpoint).await {
            Ok(stream) => stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound
                        | std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Ok(None);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to connect to Docker mlabd at {endpoint}"));
            }
        };
        stream.set_nodelay(true)?;
        return send_runtime_request(stream, request, Some(daemon::read_token()?))
            .await
            .map(Some);
    }
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
    send_runtime_request(stream, request, None).await.map(Some)
}

async fn send_runtime_request<S>(
    stream: S,
    request: RuntimeRequest,
    auth: Option<String>,
) -> Result<RuntimeResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut encoded = if auth.is_some() {
        serde_json::to_vec(&RuntimeWireRequest { auth, request })
    } else {
        serde_json::to_vec(&request)
    }
    .context("failed to encode mlabd request")?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .context("failed to write mlabd request")?;
    writer.shutdown().await.ok();
    let mut line = String::new();
    let bytes_read = BufReader::new(reader)
        .read_line(&mut line)
        .await
        .context("failed to read mlabd response")?;
    if bytes_read == 0 {
        bail!("mlabd closed the local connection without a response");
    }
    let response = serde_json::from_str(&line).context("invalid mlabd response")?;
    Ok(response)
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
        strategy_jobs: state.strategy_jobs.values().cloned().collect(),
        bot_jobs: state.bot_jobs.values().cloned().collect(),
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
    let encoded: serde_json::Value = serde_json::from_str(&source)
        .with_context(|| format!("failed to parse {}", paths.state.display()))?;
    let version = encoded
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .context("mlabd state is missing its schema version")?;
    if version != u64::from(RUNTIME_VERSION) {
        return Ok(None);
    }
    let state: RuntimeState = serde_json::from_value(encoded)
        .with_context(|| format!("failed to parse {}", paths.state.display()))?;
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

fn tokens_equal(expected: &str, provided: &str) -> bool {
    let expected = expected.as_bytes();
    let provided = provided.as_bytes();
    if expected.len() != provided.len() {
        return false;
    }
    expected
        .iter()
        .zip(provided)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
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
    fn runtime_protocol_accepts_native_and_authenticated_envelopes() {
        let native: IncomingRuntimeRequest =
            serde_json::from_value(serde_json::json!({ "type": "ping" }))
                .expect("native request should decode");
        assert!(matches!(
            native,
            IncomingRuntimeRequest::Native(RuntimeRequest::Ping)
        ));

        let authenticated: IncomingRuntimeRequest = serde_json::from_value(serde_json::json!({
            "auth": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "request": { "type": "ping" }
        }))
        .expect("authenticated request should decode");
        assert!(matches!(
            authenticated,
            IncomingRuntimeRequest::Authenticated(RuntimeWireRequest {
                request: RuntimeRequest::Ping,
                ..
            })
        ));
    }

    #[test]
    fn daemon_tokens_compare_without_prefix_matches() {
        let token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert!(tokens_equal(token, token));
        assert!(!tokens_equal(token, &token[..63]));
        assert!(!tokens_equal(
            token,
            "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn docker_container_is_hardened_and_only_publishes_on_loopback() {
        let mut config = DaemonConfig::docker_for_version("1.2.3");
        config.docker.container = "marketlab-test".to_string();
        config.docker.port = 49_731;
        let home = PathBuf::from("/tmp/market lab");
        let args = docker_create_args(&config, &home, 501, 20);
        let command = args.join(" ");

        assert!(command.contains("--publish 127.0.0.1:49731:47831"));
        assert!(command.contains("--user 501:20"));
        assert!(command.contains("--read-only"));
        assert!(command.contains("--cap-drop ALL"));
        assert!(command.contains("--security-opt no-new-privileges:true"));
        assert!(!command.contains("--init"));
        assert!(
            command.contains("type=bind,source=/tmp/market lab,target=/home/marketlab/.market-lab")
        );
        assert!(command.contains("ghcr.io/emeraldls/market-lab-daemon:v1.2.3 serve"));
        assert!(!command.contains("docker.sock"));
    }

    #[test]
    fn stopped_status_has_no_process_or_workloads() {
        let status = RuntimeStatus::stopped();
        assert!(!status.running);
        assert!(status.pid.is_none());
        assert!(status.tracked_orders.is_empty());
        assert!(status.script_jobs.is_empty());
        assert!(status.strategy_jobs.is_empty());
        assert!(status.bot_jobs.is_empty());
    }

    #[test]
    fn partial_fills_keep_managed_orders_active() {
        assert!(!is_terminal_order_status("partiallyFilled"));
        assert!(is_terminal_order_status("filled"));
        assert!(is_terminal_order_status("cancelled"));
    }

    #[test]
    fn terminal_script_order_status_cannot_regress() {
        assert!(should_apply_script_order_status("submitted", "resting"));
        assert!(should_apply_script_order_status("resting", "filled"));
        assert!(!should_apply_script_order_status("filled", "resting"));
        assert!(!should_apply_script_order_status("cancelled", "resting"));
        assert!(!should_apply_script_order_status(
            "rejectedCrossing",
            "resting"
        ));
        assert!(!should_apply_script_order_status("rejected", "resting"));
    }

    #[test]
    fn hyperliquid_order_ids_and_statuses_normalize_for_runtime_tracking() {
        assert_eq!(
            value_identifier(&serde_json::json!(123)).expect("oid"),
            "123"
        );
        assert_eq!(normalize_hyperliquid_order_status("open"), "resting");
        assert_eq!(normalize_hyperliquid_order_status("filled"), "filled");
        assert_eq!(
            normalize_hyperliquid_order_status("scheduledCancelCanceled"),
            "rejected"
        );
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
        assert!(status.strategy_jobs.is_empty());
        assert!(status.bot_jobs.is_empty());
    }

    #[test]
    fn runtime_protocol_v37_decodes_oiwap_submissions() {
        assert_eq!(RUNTIME_VERSION, 37);

        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "submit_strategy_job",
            "submission": {
                "definition": {
                    "name": "oiwap",
                    "config": {
                        "venue": "bulkf",
                        "symbol": "ZEC",
                        "side": "buy",
                        "totalSize": 1.0,
                        "requestedMargin": 50.0,
                        "targetMargin": 50.0,
                        "targetExposure": 500.0,
                        "durationSeconds": 3_900,
                        "oiSources": [{"exchange": "hyperliquidf", "provider": "mmt"}],
                        "leverage": 10.0,
                        "reduceOnly": false
                    }
                }
            }
        }))
        .expect("runtime protocol should decode OIWAP submissions");

        assert!(matches!(
            request,
            RuntimeRequest::SubmitStrategyJob {
                submission: StrategyJobSubmission {
                    definition: StrategyJobDefinition::Oiwap(_)
                }
            }
        ));
    }

    #[test]
    fn runtime_protocol_v36_decodes_python_script_submissions() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "submit_script_job",
            "submission": {
                "scriptName": "python-sma",
                "originalPath": "/strategies/sma.py",
                "source": "script = {}",
                "language": "python_v2",
                "pythonRuntime": {
                    "interpreter": "/strategies/.venv/bin/python",
                    "version": "3.12.4"
                },
                "providers": ["mmt"],
                "exchanges": ["binancef"],
                "sources": ["btc@candles@binancef@mmt:timeframe=60"],
                "params": ["fast_period=20"],
                "testnet": false,
                "durationSeconds": 3_600,
                "verbose": false
            }
        }))
        .expect("runtime protocol should decode Python script submissions");

        assert!(matches!(
            request,
            RuntimeRequest::SubmitScriptJob {
                submission: ScriptJobSubmission {
                    language: crate::scripting::language::ScriptLanguage::PythonV2,
                    python_runtime: Some(_),
                    ..
                }
            }
        ));
    }

    #[test]
    fn runtime_protocol_v36_preserves_python_request_exchange() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "script_execute_order",
            "job_id": "script_python_1",
            "order": { "id": "ord_1", "key": "ask-1" },
            "exchange": "hyperliquidf",
            "request": {
                "key": "ask-1",
                "symbol": "BTC",
                "side": "sell",
                "size": 0.01,
                "order": { "type": "limit", "price": 65_000, "tif": "alo" }
            }
        }))
        .expect("request-routed Python order should decode");

        assert!(matches!(
            request,
            RuntimeRequest::ScriptExecuteOrder {
                exchange: Some(ExecutionVenue::Hyperliquid),
                ..
            }
        ));
    }

    #[test]
    fn runtime_protocol_v29_decodes_mid_price_bot_submissions() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "submit_bot_job",
            "submission": {
                "definition": {
                    "name": "mid_price",
                    "config": {
                        "venue": "bulkf",
                        "symbol": "BTC",
                        "maxInventorySize": 0.02,
                        "requestedMargin": 100.0,
                        "maxInventoryMargin": 100.0,
                        "maxInventoryExposure": 1_000.0,
                        "durationSeconds": 300,
                        "spreadBps": 2.0,
                        "refreshSeconds": 5.0,
                        "refreshToleranceBps": 0.5,
                        "directionalBiasPercent": 25.0,
                        "leverage": 10.0
                    }
                }
            }
        }))
        .expect("runtime protocol should decode mid-price bot submissions");

        assert!(matches!(
            request,
            RuntimeRequest::SubmitBotJob {
                submission: BotJobSubmission {
                    definition: BotJobDefinition::MidPrice(_)
                }
            }
        ));
    }

    #[test]
    fn runtime_protocol_v29_decodes_volume_mid_bot_submissions() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "submit_bot_job",
            "submission": {
                "definition": {
                    "name": "volume_mid",
                    "config": {
                        "venue": "bulkf",
                        "symbol": "BTC",
                        "maxInventorySize": 0.02,
                        "requestedMargin": 100.0,
                        "maxInventoryMargin": 100.0,
                        "maxInventoryExposure": 1_000.0,
                        "durationSeconds": 300,
                        "spreadBps": 6.0,
                        "refreshSeconds": 2.0,
                        "refreshToleranceBps": 1.0,
                        "directionalBiasPercent": 0.0,
                        "leverage": 10.0,
                        "stopLossPct": 5.0
                    }
                }
            }
        }))
        .expect("runtime protocol should decode volume-mid bot submissions");

        assert!(matches!(
            request,
            RuntimeRequest::SubmitBotJob {
                submission: BotJobSubmission {
                    definition: BotJobDefinition::VolumeMid(_)
                }
            }
        ));
    }

    #[test]
    fn runtime_protocol_v29_decodes_grid_bot_submissions() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "submit_bot_job",
            "submission": {
                "definition": {
                    "name": "grid",
                    "config": {
                        "venue": "hyperliquidf",
                        "symbol": "BTC",
                        "maxInventorySize": 0.02,
                        "requestedMargin": 100.0,
                        "maxInventoryMargin": 100.0,
                        "maxInventoryExposure": 1_000.0,
                        "durationSeconds": 300,
                        "levelsPerSide": 3,
                        "stepBps": 2.0,
                        "leverage": 10.0,
                        "stopLossPct": 5.0
                    }
                }
            }
        }))
        .expect("runtime protocol should decode grid bot submissions");

        assert!(matches!(
            request,
            RuntimeRequest::SubmitBotJob {
                submission: BotJobSubmission {
                    definition: BotJobDefinition::Grid(_)
                }
            }
        ));
    }

    #[test]
    fn runtime_protocol_v34_decodes_outcome_bot_actions() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "bot_outcome_action",
            "job_id": "bot_outcome_1",
            "sequence": 7,
            "action": {
                "type": "split",
                "outcome": 1_009,
                "amount": "25"
            }
        }))
        .expect("runtime protocol should decode outcome bot actions");

        assert!(matches!(
            request,
            RuntimeRequest::BotOutcomeAction {
                job_id,
                sequence: 7,
                action: UserOutcomeAction::Split { outcome: 1_009, amount }
            } if job_id == "bot_outcome_1" && amount == "25"
        ));
    }

    #[test]
    fn runtime_protocol_v17_decodes_raw_script_orders() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "script_execute_order",
            "job_id": "script_1",
            "order": { "id": "ord_1", "key": "ask-1" },
            "request": {
                "key": "ask-1",
                "symbol": "btc",
                "side": "short",
                "size": 1,
                "leverage": 5,
                "order": { "type": "limit", "price": 101, "tif": "alo" }
            }
        }))
        .expect("raw script order request decodes");

        assert!(matches!(request, RuntimeRequest::ScriptExecuteOrder { .. }));
    }

    #[test]
    fn runtime_protocol_v17_decodes_script_order_cleanup() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "type": "script_cancel_all_orders",
            "job_id": "script_1"
        }))
        .expect("script cleanup request decodes");

        assert!(matches!(
            request,
            RuntimeRequest::ScriptCancelAllOrders { job_id } if job_id == "script_1"
        ));
    }

    #[test]
    fn script_failure_records_explain_terminal_worker_errors() {
        let record = script_failure_record(1_780_000_000_000, "connection reset by peer");

        assert_eq!(record["type"], "script.run.failed");
        assert_eq!(record["error"], "connection reset by peer");
        assert_eq!(record["ts_ms"], 1_780_000_000_000_u64);
    }
}
