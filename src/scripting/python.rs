use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
use rand_core::{OsRng, RngCore};
use serde_json::{Value, json};

use super::engine::ScriptExecution;
use super::execution::{
    ScriptCommandBuffer, ScriptExecutionCommand, ScriptExecutionContext, queue_execution_call,
};
use super::language::PythonRuntime;
use super::limits::{
    SCRIPT_PYTHON_FINISH_TIMEOUT_MS, SCRIPT_PYTHON_HOOK_TIMEOUT_MS, SCRIPT_PYTHON_LOG_BYTES,
    SCRIPT_PYTHON_MAX_PROCESSES, SCRIPT_PYTHON_MEMORY_BYTES, SCRIPT_PYTHON_PROTOCOL_BYTES,
    SCRIPT_PYTHON_STARTUP_TIMEOUT_MS,
};
use super::manifest::ScriptManifest;
use super::output::ScriptOutput;
use super::studies::calculate_value;
use super::telemetry::ScriptHookStats;

const PYTHON_STARTUP_TIMEOUT: Duration = Duration::from_millis(SCRIPT_PYTHON_STARTUP_TIMEOUT_MS);
const PYTHON_HOOK_TIMEOUT: Duration = Duration::from_millis(SCRIPT_PYTHON_HOOK_TIMEOUT_MS);
const PYTHON_FINISH_TIMEOUT: Duration = Duration::from_millis(SCRIPT_PYTHON_FINISH_TIMEOUT_MS);
const PYTHON_RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
struct PythonProcessLimits {
    memory_bytes: usize,
    max_processes: usize,
    protocol_bytes: usize,
    log_bytes: usize,
}

impl Default for PythonProcessLimits {
    fn default() -> Self {
        Self {
            memory_bytes: SCRIPT_PYTHON_MEMORY_BYTES,
            max_processes: SCRIPT_PYTHON_MAX_PROCESSES,
            protocol_bytes: SCRIPT_PYTHON_PROTOCOL_BYTES,
            log_bytes: SCRIPT_PYTHON_LOG_BYTES,
        }
    }
}

pub(crate) fn inspect_python_script(
    path: &Path,
    runtime: &PythonRuntime,
) -> Result<PythonScriptInspection> {
    inspect_python_script_with_limits(
        path,
        runtime,
        PYTHON_STARTUP_TIMEOUT,
        PythonProcessLimits::default(),
    )
}

fn inspect_python_script_with_limits(
    path: &Path,
    runtime: &PythonRuntime,
    timeout: Duration,
    limits: PythonProcessLimits,
) -> Result<PythonScriptInspection> {
    let mut process =
        PythonProcess::spawn_mode(path, runtime, "describe", limits).with_context(|| {
            format!(
                "failed to inspect Python script {} with {}",
                path.display(),
                runtime.interpreter.display()
            )
        })?;
    let response = process.receive(timeout).inspect_err(|_| {
        process.kill();
    })?;
    if response.get("type").and_then(Value::as_str) == Some("error") {
        bail!(
            "failed to load Python script: {}",
            python_error_message(&response)
        );
    }
    if response.get("type").and_then(Value::as_str) != Some("ready") {
        bail!("Python bridge returned an unexpected manifest response");
    }
    let manifest: ScriptManifest = serde_json::from_value(
        response
            .get("manifest")
            .cloned()
            .context("Python script has no `script` manifest")?,
    )
    .context("failed to decode Python `script` manifest")?;
    let sources = serde_json::from_value(
        response
            .get("sources")
            .cloned()
            .context("Python script inspection omitted source selectors")?,
    )
    .context("failed to decode Python source selectors")?;
    let execution_venues = serde_json::from_value(
        response
            .get("executionVenues")
            .cloned()
            .context("Python script inspection omitted execution venues")?,
    )
    .context("failed to decode Python execution venues")?;
    Ok(PythonScriptInspection {
        manifest,
        sources,
        execution_venues,
    })
}

#[derive(Debug)]
pub(crate) struct PythonScriptInspection {
    pub(crate) manifest: ScriptManifest,
    pub(crate) sources: Vec<String>,
    pub(crate) execution_venues: Vec<String>,
}

pub(crate) struct PythonSession {
    process: Mutex<PythonProcess>,
    cancelled: Arc<AtomicBool>,
    commands: ScriptCommandBuffer,
    execution: ScriptExecutionContext,
}

impl PythonSession {
    pub(crate) fn start(
        path: &Path,
        runtime: &PythonRuntime,
        params: &Value,
        history_capacity: usize,
        configured_sources: Option<&[String]>,
        execution: ScriptExecutionContext,
    ) -> Result<Self> {
        let artifact_dir = artifact_directory(&execution.job_id)?;
        let execution_key_prefix = format!("{:016x}", OsRng.next_u64());
        let mut process = PythonProcess::spawn(path, runtime)?;
        process.send(&json!({
            "type": "init",
            "params": params,
            "historyCapacity": history_capacity,
            "configuredSources": configured_sources,
            "executionEnabled": execution.enabled,
            "executionKeyPrefix": execution_key_prefix,
            "artifactDir": artifact_dir,
        }))?;
        let response = process.receive(PYTHON_STARTUP_TIMEOUT)?;
        match response.get("type").and_then(Value::as_str) {
            Some("ready") => {}
            Some("error") => bail!(
                "failed to start Python script: {}",
                python_error_message(&response)
            ),
            _ => bail!("Python bridge returned an unexpected startup response"),
        }
        Ok(Self {
            process: Mutex::new(process),
            cancelled: Arc::new(AtomicBool::new(false)),
            commands: Arc::new(Mutex::new(Vec::new())),
            execution,
        })
    }

    pub(crate) fn cancel_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    pub(crate) fn run_event(
        &self,
        payload: Value,
        source: String,
        record: Value,
        identity: Option<u64>,
        pnl_history: Value,
    ) -> Result<ScriptExecution> {
        self.run_hook(
            "on_data",
            json!({
                "event": payload,
                "pnlHistory": pnl_history,
                "record": {
                    "source": source,
                    "value": record,
                    "identity": identity,
                }
            }),
            PYTHON_HOOK_TIMEOUT,
        )
        .map(Option::unwrap)
    }

    pub(crate) fn run_execution_event(&self, event: Value) -> Result<Option<ScriptExecution>> {
        self.run_hook(
            "on_execution",
            json!({ "event": event }),
            PYTHON_HOOK_TIMEOUT,
        )
    }

    pub(crate) fn run_finish(&self, pnl_history: Value) -> Result<Option<ScriptExecution>> {
        self.run_hook(
            "on_finish",
            json!({ "pnlHistory": pnl_history }),
            PYTHON_FINISH_TIMEOUT,
        )
    }

    fn run_hook(
        &self,
        hook: &str,
        mut request: Value,
        timeout: Duration,
    ) -> Result<Option<ScriptExecution>> {
        if self.is_cancelled() && hook != "on_finish" {
            bail!("script execution cancelled");
        }
        self.clear_commands()?;
        let started = Instant::now();
        let mut process = self
            .process
            .lock()
            .map_err(|_| anyhow::anyhow!("Python process lock poisoned"))?;
        let request_id = process.next_request_id();
        request["type"] = Value::String("hook".to_string());
        request["hook"] = Value::String(hook.to_string());
        request["id"] = Value::from(request_id);
        process.send(&request)?;

        loop {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                process.kill();
                self.clear_commands()?;
                bail!("Python {hook} exceeded {}ms", timeout.as_millis());
            }
            let response = match process.receive(remaining) {
                Ok(response) => response,
                Err(error) => {
                    process.kill();
                    self.clear_commands()?;
                    if started.elapsed() >= timeout {
                        bail!("Python {hook} exceeded {}ms", timeout.as_millis());
                    }
                    return Err(error).with_context(|| format!("Python {hook} failed"));
                }
            };
            match response.get("type").and_then(Value::as_str) {
                Some("execution") => {
                    self.handle_execution_request(&mut process, &response)?;
                }
                Some("study") => {
                    self.handle_study_request(&mut process, &response)?;
                }
                Some("hook_result") => {
                    if response.get("id").and_then(Value::as_u64) != Some(request_id) {
                        self.clear_commands()?;
                        bail!("Python bridge returned a result for the wrong hook request");
                    }
                    if response.get("hook").and_then(Value::as_str) != Some(hook) {
                        self.clear_commands()?;
                        bail!("Python bridge returned a result for the wrong hook");
                    }
                    if response.get("present").and_then(Value::as_bool) == Some(false) {
                        self.clear_commands()?;
                        return Ok(None);
                    }
                    let output = ScriptOutput::from_json(
                        response.get("output").cloned().unwrap_or(Value::Null),
                    )?;
                    let commands = self.drain_commands()?;
                    return Ok(Some(ScriptExecution {
                        output,
                        commands,
                        stats: ScriptHookStats {
                            duration_ms: started.elapsed().as_millis() as u64,
                            heap_used_bytes: None,
                        },
                    }));
                }
                Some("hook_error") | Some("error") => {
                    self.clear_commands()?;
                    bail!("Python {hook} failed: {}", python_error_message(&response));
                }
                Some(other) => {
                    self.clear_commands()?;
                    bail!("Python bridge returned unexpected message `{other}`");
                }
                None => {
                    self.clear_commands()?;
                    bail!("Python bridge response has no message type");
                }
            }
        }
    }

    fn handle_execution_request(
        &self,
        process: &mut PythonProcess,
        response: &Value,
    ) -> Result<()> {
        let request_id = response
            .get("requestId")
            .and_then(Value::as_u64)
            .context("Python execution request has no request id")?;
        let operation = response
            .get("operation")
            .and_then(Value::as_str)
            .context("Python execution request has no operation")?;
        let payload = serde_json::to_string(
            response
                .get("payload")
                .context("Python execution request has no payload")?,
        )?;
        let result = queue_execution_call(
            &self.execution.job_id,
            self.execution.enabled,
            self.execution.request_routed,
            &self.commands,
            operation,
            &payload,
        );
        let reply = match result {
            Ok(value) => json!({
                "type": "execution_result",
                "requestId": request_id,
                "ok": true,
                "value": value,
            }),
            Err(error) => json!({
                "type": "execution_result",
                "requestId": request_id,
                "ok": false,
                "error": format!("{error:#}"),
            }),
        };
        process.send(&reply)
    }

    fn handle_study_request(&self, process: &mut PythonProcess, response: &Value) -> Result<()> {
        let request_id = response
            .get("requestId")
            .and_then(Value::as_u64)
            .context("Python study request has no request id")?;
        let name = response
            .get("name")
            .and_then(Value::as_str)
            .context("Python study request has no function name")?;
        let input = response
            .get("input")
            .cloned()
            .context("Python study request has no input")?;
        let options = response
            .get("options")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let reply = match calculate_value(name, input, options) {
            Ok(value) => json!({
                "type": "study_result",
                "requestId": request_id,
                "ok": true,
                "value": value,
            }),
            Err(error) => json!({
                "type": "study_result",
                "requestId": request_id,
                "ok": false,
                "error": format!("{error:#}"),
            }),
        };
        process.send(&reply)
    }

    fn clear_commands(&self) -> Result<()> {
        self.commands
            .lock()
            .map_err(|_| anyhow::anyhow!("script execution queue lock poisoned"))?
            .clear();
        Ok(())
    }

    fn drain_commands(&self) -> Result<Vec<ScriptExecutionCommand>> {
        let mut commands = self
            .commands
            .lock()
            .map_err(|_| anyhow::anyhow!("script execution queue lock poisoned"))?;
        Ok(std::mem::take(&mut *commands))
    }
}

struct PythonProcess {
    child: Child,
    stdin: ChildStdin,
    messages: mpsc::Receiver<Result<Value, String>>,
    request_id: u64,
    monitor_stop: Arc<AtomicBool>,
    protocol_reader: Option<JoinHandle<()>>,
    log_forwarder: Option<JoinHandle<()>>,
    resource_monitor: Option<JoinHandle<()>>,
    protocol_bytes: usize,
    process_group: u32,
    limits: PythonProcessLimits,
    resource_failure: Arc<Mutex<Option<String>>>,
}

impl PythonProcess {
    fn spawn(path: &Path, runtime: &PythonRuntime) -> Result<Self> {
        Self::spawn_mode(path, runtime, "session", PythonProcessLimits::default())
    }

    fn spawn_mode(
        path: &Path,
        runtime: &PythonRuntime,
        mode: &str,
        limits: PythonProcessLimits,
    ) -> Result<Self> {
        let matplotlib_config = matplotlib_config_directory()?;
        let mut command = Command::new(&runtime.interpreter);
        command
            .args(["-u", "-c", PYTHON_RUNNER])
            .arg(path)
            .arg(mode)
            .env("MPLCONFIGDIR", matplotlib_config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().with_context(|| {
            format!(
                "failed to start Python script {} with {}",
                path.display(),
                runtime.interpreter.display()
            )
        })?;
        let process_group = child.id();
        let stdin = child.stdin.take().context("Python bridge has no stdin")?;
        let stdout = child.stdout.take().context("Python bridge has no stdout")?;
        let stderr = child.stderr.take().context("Python bridge has no stderr")?;
        let (sender, messages) = mpsc::channel();
        let protocol_sender = sender.clone();
        let protocol_reader = thread::Builder::new()
            .name("mlab-python-bridge".to_string())
            .spawn(move || {
                let mut reader = BufReader::new(stdout);
                loop {
                    let message = match read_bounded_line(&mut reader, limits.protocol_bytes) {
                        Ok(Some(line)) => serde_json::from_slice(&line).map_err(|error| {
                            let preview = String::from_utf8_lossy(&line)
                                .chars()
                                .take(512)
                                .collect::<String>();
                            format!("invalid Python bridge JSON: {error}; line={preview}")
                        }),
                        Ok(None) => {
                            let _ = protocol_sender
                                .send(Err("Python bridge closed its output".to_string()));
                            break;
                        }
                        Err(error) => Err(format!("failed to read Python bridge: {error}")),
                    };
                    let terminal = message.is_err();
                    if protocol_sender.send(message).is_err() || terminal {
                        break;
                    }
                }
            })
            .context("failed to start Python bridge reader")?;
        let log_forwarder = spawn_log_forwarder(stderr, limits.log_bytes)?;
        let monitor_stop = Arc::new(AtomicBool::new(false));
        let resource_failure = Arc::new(Mutex::new(None));
        let resource_monitor = spawn_resource_monitor(
            process_group,
            limits,
            Arc::clone(&monitor_stop),
            Arc::clone(&resource_failure),
            sender,
        )?;
        Ok(Self {
            child,
            stdin,
            messages,
            request_id: 0,
            monitor_stop,
            protocol_reader: Some(protocol_reader),
            log_forwarder: Some(log_forwarder),
            resource_monitor: Some(resource_monitor),
            protocol_bytes: limits.protocol_bytes,
            process_group,
            limits,
            resource_failure,
        })
    }

    fn next_request_id(&mut self) -> u64 {
        self.request_id = self.request_id.saturating_add(1);
        self.request_id
    }

    fn send(&mut self, value: &Value) -> Result<()> {
        if let Some(error) = self
            .resource_failure
            .lock()
            .map_err(|_| anyhow::anyhow!("Python resource monitor lock poisoned"))?
            .clone()
        {
            bail!("{error}");
        }
        let encoded =
            serde_json::to_vec(value).context("failed to encode Python bridge message")?;
        if encoded.len() > self.protocol_bytes {
            bail!(
                "Python bridge message is {} bytes; limit is {} bytes",
                encoded.len(),
                self.protocol_bytes
            );
        }
        self.stdin
            .write_all(&encoded)
            .context("failed to write to Python bridge")?;
        self.stdin
            .write_all(b"\n")
            .context("failed to delimit Python bridge message")?;
        self.stdin.flush().context("failed to flush Python bridge")
    }

    fn receive(&mut self, timeout: Duration) -> Result<Value> {
        match self.messages.recv_timeout(timeout) {
            Ok(Ok(value)) => {
                if let Some(error) = resource_violation(self.process_group, self.limits) {
                    self.kill();
                    bail!("{error}");
                }
                Ok(value)
            }
            Ok(Err(error)) => bail!("{error}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                bail!("timed out waiting for Python bridge")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let status = self.child.try_wait().ok().flatten();
                bail!(
                    "Python bridge stopped unexpectedly{}",
                    status.map_or_else(String::new, |status| format!(" ({status})"))
                )
            }
        }
    }

    fn kill(&mut self) {
        self.monitor_stop.store(true, Ordering::Relaxed);
        kill_process_group(self.child.id());
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_helpers();
    }

    fn join_helpers(&mut self) {
        for handle in [
            self.protocol_reader.take(),
            self.log_forwarder.take(),
            self.resource_monitor.take(),
        ]
        .into_iter()
        .flatten()
        {
            let _ = handle.join();
        }
    }
}

fn matplotlib_config_directory() -> Result<PathBuf> {
    let market_lab_home = env::var_os("MLAB_HOME").map_or_else(
        || {
            env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".market-lab"))
                .context("HOME or MLAB_HOME is required to configure the Python runtime cache")
        },
        |home| Ok(PathBuf::from(home)),
    )?;
    let directory = market_lab_home.join("cache").join("matplotlib");
    fs::create_dir_all(&directory)
        .with_context(|| format!("failed to create {}", directory.display()))?;
    #[cfg(unix)]
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to secure {}", directory.display()))?;
    Ok(directory)
}

impl Drop for PythonProcess {
    fn drop(&mut self) {
        let _ = self.send(&json!({ "type": "shutdown" }));
        if self.child.try_wait().ok().flatten().is_none() {
            self.kill();
        } else {
            self.monitor_stop.store(true, Ordering::Relaxed);
            self.join_helpers();
        }
    }
}

fn read_bounded_line<R: BufRead>(reader: &mut R, limit: usize) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let chunk_len = newline.unwrap_or(available.len());
        if line.len().saturating_add(chunk_len) > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Python protocol message exceeds the {limit}-byte limit"),
            ));
        }
        line.extend_from_slice(&available[..chunk_len]);
        let consumed = chunk_len + usize::from(newline.is_some());
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(line));
        }
    }
}

fn spawn_log_forwarder(stderr: ChildStderr, limit: usize) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("mlab-python-logs".to_string())
        .spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buffer = [0_u8; 8 * 1024];
            let mut forwarded = 0_usize;
            let mut warned = false;
            loop {
                let read = match reader.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                let allowed = limit.saturating_sub(forwarded).min(read);
                if allowed > 0 {
                    let mut output = io::stderr().lock();
                    let _ = output.write_all(&buffer[..allowed]);
                    let _ = output.flush();
                    forwarded = forwarded.saturating_add(allowed);
                }
                if allowed < read && !warned {
                    warned = true;
                    let _ = writeln!(
                        io::stderr(),
                        "\nmarketlab: Python log output exceeded {limit} bytes; further output is discarded"
                    );
                }
            }
        })
        .context("failed to start Python log forwarder")
}

fn spawn_resource_monitor(
    process_group: u32,
    limits: PythonProcessLimits,
    stop: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
    sender: mpsc::Sender<Result<Value, String>>,
) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("mlab-python-limits".to_string())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Some(error) = resource_violation(process_group, limits) {
                    if let Ok(mut failure) = failure.lock() {
                        *failure = Some(error.clone());
                    }
                    let _ = sender.send(Err(error));
                    kill_process_group(process_group);
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
                thread::sleep(PYTHON_RESOURCE_POLL_INTERVAL);
            }
        })
        .context("failed to start Python resource monitor")
}

fn resource_violation(process_group: u32, limits: PythonProcessLimits) -> Option<String> {
    let (processes, memory_bytes) = process_group_usage(process_group)?;
    if processes > limits.max_processes {
        return Some(format!(
            "Python process tree has {processes} processes; limit is {}",
            limits.max_processes
        ));
    }
    if memory_bytes > limits.memory_bytes as u64 {
        return Some(format!(
            "Python process tree uses {memory_bytes} bytes of memory; limit is {} bytes",
            limits.memory_bytes
        ));
    }
    None
}

#[cfg(unix)]
fn kill_process_group(process_group: u32) {
    if let Ok(process_group) = i32::try_from(process_group) {
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_process_group(_process_group: u32) {}

#[cfg(target_os = "linux")]
fn process_group_usage(process_group: u32) -> Option<(usize, u64)> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    let mut processes = 0_usize;
    let mut memory_bytes = 0_u64;
    let entries = fs::read_dir("/proc").ok()?;
    for entry in entries.flatten() {
        let Some(pid) = entry.file_name().to_string_lossy().parse::<i32>().ok() else {
            continue;
        };
        if unsafe { libc::getpgid(pid) } != process_group as i32 {
            continue;
        }
        processes = processes.saturating_add(1);
        let resident_pages = fs::read_to_string(entry.path().join("statm"))
            .ok()
            .and_then(|stat| {
                stat.split_whitespace()
                    .nth(1)
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .unwrap_or_default();
        memory_bytes = memory_bytes.saturating_add(resident_pages.saturating_mul(page_size as u64));
    }
    Some((processes, memory_bytes))
}

#[cfg(target_os = "macos")]
fn process_group_usage(process_group: u32) -> Option<(usize, u64)> {
    const MAX_PIDS: usize = 256;
    const RUSAGE_INFO_V2: i32 = 2;
    let process_group = i32::try_from(process_group).ok()?;
    let mut pids = [0_i32; MAX_PIDS];
    let count = unsafe {
        proc_listpgrppids(
            process_group,
            pids.as_mut_ptr().cast(),
            std::mem::size_of_val(&pids) as i32,
        )
    };
    if count < 0 {
        return None;
    }
    let count = (count as usize).min(pids.len());
    let mut memory_bytes = 0_u64;
    for pid in &pids[..count] {
        if *pid <= 0 {
            continue;
        }
        let mut usage = std::mem::MaybeUninit::<RusageInfoV2>::zeroed();
        let result = unsafe {
            proc_pid_rusage(
                *pid,
                RUSAGE_INFO_V2,
                usage.as_mut_ptr().cast::<std::ffi::c_void>(),
            )
        };
        if result == 0 {
            let usage = unsafe { usage.assume_init() };
            memory_bytes = memory_bytes.saturating_add(usage.phys_footprint);
        }
    }
    Some((count, memory_bytes))
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct RusageInfoV2 {
    uuid: [u8; 16],
    user_time: u64,
    system_time: u64,
    pkg_idle_wkups: u64,
    interrupt_wkups: u64,
    pageins: u64,
    wired_size: u64,
    resident_size: u64,
    phys_footprint: u64,
    proc_start_abstime: u64,
    proc_exit_abstime: u64,
    child_user_time: u64,
    child_system_time: u64,
    child_pkg_idle_wkups: u64,
    child_interrupt_wkups: u64,
    child_pageins: u64,
    child_elapsed_abstime: u64,
    diskio_bytesread: u64,
    diskio_byteswritten: u64,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn proc_listpgrppids(process_group: i32, buffer: *mut std::ffi::c_void, size: i32) -> i32;
    fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut std::ffi::c_void) -> i32;
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_group_usage(_process_group: u32) -> Option<(usize, u64)> {
    None
}

fn artifact_directory(job_id: &str) -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is required for Python script artifacts")?;
    let mut name = job_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if matches!(job_id, "analysis" | "backtest") {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        name = format!("{name}-{now}-{}", std::process::id());
    }
    let path = home.join(".market-lab").join("artifacts").join(name);
    fs::create_dir_all(&path)
        .with_context(|| format!("failed to create artifact directory {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to secure artifact directory {}", path.display()))?;
    }
    Ok(path)
}

fn python_error_message(response: &Value) -> String {
    response
        .get("traceback")
        .and_then(Value::as_str)
        .or_else(|| response.get("error").and_then(Value::as_str))
        .unwrap_or("unknown Python error")
        .trim()
        .to_string()
}

const PYTHON_RUNNER: &str = r#"
import copy
import ast
import importlib.util
import inspect
import json
import os
from pathlib import Path
import sys
import traceback

SCRIPT_PATH = Path(sys.argv[1]).resolve()
MODE = sys.argv[2]
PROTOCOL_OUT = sys.stdout
sys.stdout = sys.stderr

HOOK_PARAMETERS = {}
HOOK_INPUTS = {
    "on_data": ("ctx", "history"),
    "on_execution": ("ctx",),
    "on_finish": ("ctx", "history"),
}


def json_default(value):
    if isinstance(value, Path):
        return str(value)
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return tolist()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def write_message(message):
    PROTOCOL_OUT.write(json.dumps(message, default=json_default, separators=(",", ":")) + "\n")
    PROTOCOL_OUT.flush()


def configure_hook(module, name, required=False):
    function = getattr(module, name, None)
    if function is None:
        if required:
            raise TypeError(f"Python scripts require an `{name}` function")
        return
    if not callable(function):
        raise TypeError(f"Python `{name}` must be callable")

    signature = inspect.signature(function)
    available = HOOK_INPUTS[name]
    selected = []
    for parameter in signature.parameters.values():
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise TypeError(
                f"Python `{name}{signature}` must declare only named parameters from "
                f"{', '.join(available)}"
            )
        if parameter.name not in available:
            raise TypeError(
                f"Python `{name}` does not provide `{parameter.name}`; choose from "
                f"{', '.join(available)}"
            )
        selected.append(parameter.name)
    HOOK_PARAMETERS[name] = tuple(selected)


def invoke_hook(name, function, available):
    return function(**{
        parameter: available[parameter]
        for parameter in HOOK_PARAMETERS[name]
    })


def load_module():
    script_directory = str(SCRIPT_PATH.parent)
    if script_directory not in sys.path:
        sys.path.insert(0, script_directory)
    spec = importlib.util.spec_from_file_location("__mlab_strategy__", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load Python script {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manifest = getattr(module, "script", None)
    if not isinstance(manifest, dict):
        raise TypeError("Python scripts require a `script` dictionary")
    configure_hook(module, "on_data", required=True)
    configure_hook(module, "on_execution")
    configure_hook(module, "on_finish")
    return module, manifest


def module_string_constants(tree):
    constants = {}
    for statement in tree.body:
        if (
            isinstance(statement, ast.Assign)
            and len(statement.targets) == 1
            and isinstance(statement.targets[0], ast.Name)
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            constants[statement.targets[0].id] = statement.value.value
        elif (
            isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        ):
            constants[statement.target.id] = statement.value.value
    return constants


def static_string(node, constants):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    return None


def inspect_source_selectors(tree, constants):
    selectors = []
    seen = set()
    dynamic_lines = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if not (
            isinstance(function, ast.Attribute)
            and function.attr == "source"
            and isinstance(function.value, ast.Name)
            and function.value.id == "history"
        ):
            continue
        if not node.args:
            raise TypeError(f"history.source on line {node.lineno} requires a selector")
        selector = node.args[0]
        value = static_string(selector, constants)
        if value is None:
            dynamic_lines.append(node.lineno)
            continue
        if not value:
            raise TypeError(f"history.source on line {node.lineno} selector cannot be empty")
        if value not in seen:
            seen.add(value)
            selectors.append(value)
    if dynamic_lines and not selectors:
        line = dynamic_lines[0]
        raise TypeError(
            f"history.source on line {line} cannot declare a source dynamically; "
            "add at least one call with a literal selector or module-level string constant"
        )
    return selectors


def inspect_execution_venues(tree, constants):
    venues = []
    seen = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if not (
            isinstance(function, ast.Attribute)
            and function.attr in ("trade", "order")
            and isinstance(function.value, ast.Name)
            and function.value.id == "ctx"
        ):
            continue
        operation = f"ctx.{function.attr}"
        if not node.args:
            raise TypeError(f"{operation} on line {node.lineno} requires a request object")
        request = node.args[0]
        if not isinstance(request, ast.Dict):
            raise TypeError(
                f"{operation} on line {node.lineno} cannot declare its exchange dynamically; "
                "pass a dictionary literal with a literal exchange or module-level string constant"
            )
        exchange_node = None
        for key, value in zip(request.keys, request.values):
            if isinstance(key, ast.Constant) and key.value == "exchange":
                exchange_node = value
                break
        if exchange_node is None:
            raise TypeError(f"{operation} on line {node.lineno} requires an exchange")
        exchange = static_string(exchange_node, constants)
        if exchange is None:
            raise TypeError(
                f"{operation} exchange on line {node.lineno} cannot be dynamic; "
                "use a literal string or module-level string constant"
            )
        if not exchange:
            raise TypeError(f"{operation} exchange on line {node.lineno} cannot be empty")
        if exchange not in seen:
            seen.add(exchange)
            venues.append(exchange)
    return venues


def selector_without_options(name):
    last_at = name.rfind("@")
    option = name.find(":", last_at) if last_at >= 0 else -1
    return name if option < 0 else name[:option]


class History:
    def __init__(self, capacity, configured_sources):
        self.capacity = max(2, int(capacity))
        self.records = {}
        self.identities = {}
        self.configured_sources = None if configured_sources is None else set(configured_sources)

    def record(self, source, value, identity):
        records = self.records.setdefault(source, [])
        if identity is not None and self.identities.get(source) == identity and records:
            records[0] = value
            return
        records.insert(0, value)
        del records[self.capacity:]
        if identity is None:
            self.identities.pop(source, None)
        else:
            self.identities[source] = identity

    def source(self, name, offset=None):
        if not isinstance(name, str) or not name:
            raise TypeError("history.source name must be a non-empty string")
        selector = selector_without_options(name)
        if self.configured_sources is not None and selector not in self.configured_sources:
            raise ValueError(
                f"history.source `{name}` is not configured for this script"
            )
        records = self.records.get(selector, [])
        if offset is None:
            return copy.deepcopy(list(reversed(records)))
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("history.source offset must be a non-negative integer")
        return copy.deepcopy(records[offset]) if offset < len(records) else None


class Studies:
    def __init__(self, context):
        self._context = context

    def _call(self, name, input_value, options=None):
        request_id = self._context._next_request_id()
        write_message({
            "type": "study",
            "requestId": request_id,
            "name": name,
            "input": input_value,
            "options": {} if options is None else options,
        })
        line = sys.stdin.readline()
        if not line:
            raise RuntimeError("MarketLab closed the Python study channel")
        response = json.loads(line)
        if response.get("type") != "study_result" or response.get("requestId") != request_id:
            raise RuntimeError("MarketLab returned an invalid study response")
        if not response.get("ok"):
            raise RuntimeError(response.get("error", f"MarketLab rejected ctx.study.{name}"))
        return response.get("value")

    def sma(self, rows, options):
        return self._call("sma", rows, options)

    def ema(self, rows, options):
        return self._call("ema", rows, options)

    def cvd(self, rows, options):
        return self._call("cvd", rows, options)

    def spread(self, book):
        return self._call("spread", book)

    def depth(self, book, options=None):
        return self._call("depth", book, options)

    def imbalance(self, book, options=None):
        return self._call("imbalance", book, options)

    def slippage(self, book, options):
        return self._call("slippage", book, options)

    def vamp(self, book, options):
        return self._call("vamp", book, options)


class PositionView:
    def __init__(self, account):
        self.account = account
        self.open = []


class Positions:
    def __init__(self):
        self._accounts = {"main": PositionView("main")}

    def __call__(self, account="main"):
        if not isinstance(account, str) or not account.strip():
            raise TypeError("ctx.positions account must be a non-empty string")
        account = account.strip().lower()
        return self._accounts.setdefault(account, PositionView(account))

    def _replace(self, positions):
        positions = positions if isinstance(positions, dict) else {}
        accounts = {}
        for account, value in positions.items():
            if not isinstance(account, str) or not isinstance(value, dict):
                continue
            account = account.strip().lower()
            if not account:
                continue
            view = PositionView(account)
            view.open = copy.deepcopy(value.get("open", []))
            accounts[account] = view
        accounts.setdefault("main", PositionView("main"))
        self._accounts = accounts


class Context:
    def __init__(self, params, execution_enabled, execution_key_prefix, artifact_dir):
        self.params = copy.deepcopy(params)
        self.study = Studies(self)
        self.source = None
        self.source_type = None
        self.provider = None
        self.exchange = None
        self.symbol = None
        self.source_configs = {}
        self.positions = Positions()
        self.execution = None
        self._pnl_history = []
        self._execution_enabled = execution_enabled
        self._execution_key_prefix = execution_key_prefix
        self._artifact_dir = Path(artifact_dir).resolve()
        self._request_id = 0
        self._finishing = False

    def _next_request_id(self):
        self._request_id += 1
        return self._request_id

    def _set_market_state(self, event):
        event = event if isinstance(event, dict) else {}
        self.source = event.get("source")
        self.source_type = event.get("source_type")
        self.provider = event.get("provider")
        self.exchange = event.get("exchange")
        self.symbol = event.get("symbol")
        self.source_configs = copy.deepcopy(event.get("source_configs", {}))
        self.positions._replace(event.get("positions"))
        self.execution = None

    def _set_execution_state(self, event):
        self.execution = copy.deepcopy(event)

    def _set_pnl_history(self, values):
        if not isinstance(values, list):
            raise TypeError("MarketLab provided an invalid PnL history")
        history = []
        for value in values:
            if not isinstance(value, dict):
                raise TypeError("MarketLab provided an invalid PnL point")
            timestamp = value.get("t")
            pnl = value.get("pnl")
            if isinstance(timestamp, bool) or not isinstance(timestamp, int):
                raise TypeError("MarketLab provided an invalid PnL timestamp")
            if isinstance(pnl, bool) or not isinstance(pnl, (int, float)):
                raise TypeError("MarketLab provided an invalid PnL value")
            history.append({"t": timestamp, "pnl": float(pnl)})
        self._pnl_history = history

    def pnl(self, index=None):
        if index is None:
            return copy.deepcopy(self._pnl_history)
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ValueError("ctx.pnl index must be a non-negative integer")
        position = len(self._pnl_history) - 1 - index
        return copy.deepcopy(self._pnl_history[position]) if position >= 0 else None

    def _execution(self, operation, payload):
        if self._finishing:
            raise RuntimeError(f"ctx.{operation} is unavailable inside on_finish")
        if not self._execution_enabled:
            raise RuntimeError("script execution is disabled")
        if not isinstance(payload, dict):
            raise TypeError(f"ctx.{operation} requires a dictionary")
        request_id = self._next_request_id()
        payload = copy.deepcopy(payload)
        if payload.get("key") is None:
            label = operation
            if operation == "trade" and payload.get("position") in {
                "open-long", "open-short", "close-long", "close-short"
            }:
                label = payload["position"]
            elif operation == "order" and payload.get("side") in {"buy", "sell"}:
                label = payload["side"]
            payload["key"] = f"auto-{label}-{self._execution_key_prefix}-{request_id}"
        write_message({
            "type": "execution",
            "requestId": request_id,
            "operation": operation,
            "payload": payload,
        })
        line = sys.stdin.readline()
        if not line:
            raise RuntimeError("MarketLab closed the Python execution channel")
        response = json.loads(line)
        if response.get("type") != "execution_result" or response.get("requestId") != request_id:
            raise RuntimeError("MarketLab returned an invalid execution response")
        if not response.get("ok"):
            raise RuntimeError(response.get("error", "MarketLab rejected the execution request"))
        return response.get("value")

    def trade(self, request):
        return self._execution("trade", request)

    def order(self, request):
        return self._execution("order", request)

    def cancel(self, request):
        return self._execution("cancel", request)

    def artifact_path(self, name):
        if not isinstance(name, str) or not name.strip():
            raise TypeError("ctx.artifact_path name must be a non-empty string")
        candidate = (self._artifact_dir / name).resolve()
        try:
            candidate.relative_to(self._artifact_dir)
        except ValueError as error:
            raise ValueError("artifact path must stay inside the job artifact directory") from error
        candidate.parent.mkdir(parents=True, exist_ok=True)
        return str(candidate)


try:
    MODULE, MANIFEST = load_module()
    SOURCE_TREE = ast.parse(
        SCRIPT_PATH.read_text(encoding="utf-8"), filename=str(SCRIPT_PATH)
    )
    STRING_CONSTANTS = module_string_constants(SOURCE_TREE)
    SOURCE_SELECTORS = inspect_source_selectors(SOURCE_TREE, STRING_CONSTANTS)
    EXECUTION_VENUES = inspect_execution_venues(SOURCE_TREE, STRING_CONSTANTS)
    if MODE == "describe":
        write_message({
            "type": "ready",
            "manifest": MANIFEST,
            "sources": SOURCE_SELECTORS,
            "executionVenues": EXECUTION_VENUES,
            "hooks": {
                "onExecution": callable(getattr(MODULE, "on_execution", None)),
                "onFinish": callable(getattr(MODULE, "on_finish", None)),
            },
        })
        raise SystemExit(0)

    init_line = sys.stdin.readline()
    if not init_line:
        raise RuntimeError("MarketLab sent no Python initialization message")
    init = json.loads(init_line)
    if init.get("type") != "init":
        raise RuntimeError("expected Python initialization message")
    HISTORY = History(init["historyCapacity"], init.get("configuredSources"))
    CTX = Context(
        init.get("params", {}),
        init.get("executionEnabled", False),
        init["executionKeyPrefix"],
        init["artifactDir"],
    )
    write_message({"type": "ready", "manifest": MANIFEST})

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        request = json.loads(line)
        if request.get("type") == "shutdown":
            break
        if request.get("type") != "hook":
            raise RuntimeError("unexpected Python bridge request")
        hook = request.get("hook")
        request_id = request.get("id")
        try:
            if hook == "on_data":
                record = request["record"]
                HISTORY.record(record["source"], record["value"], record.get("identity"))
                CTX._set_pnl_history(request.get("pnlHistory", []))
                CTX._set_market_state(request["event"])
                output = invoke_hook("on_data", MODULE.on_data, {
                    "ctx": CTX,
                    "history": HISTORY,
                })
                present = True
            elif hook == "on_execution":
                function = getattr(MODULE, "on_execution", None)
                present = callable(function)
                CTX._set_execution_state(request["event"])
                output = invoke_hook("on_execution", function, {
                    "ctx": CTX,
                }) if present else None
            elif hook == "on_finish":
                function = getattr(MODULE, "on_finish", None)
                present = callable(function)
                CTX._set_pnl_history(request.get("pnlHistory", []))
                CTX._finishing = True
                try:
                    output = invoke_hook("on_finish", function, {
                        "ctx": CTX,
                        "history": HISTORY,
                    }) if present else None
                finally:
                    CTX._finishing = False
            else:
                raise RuntimeError(f"unknown Python hook {hook!r}")
            write_message({
                "type": "hook_result",
                "id": request_id,
                "hook": hook,
                "present": present,
                "output": output,
            })
        except BaseException as error:
            write_message({
                "type": "hook_error",
                "id": request_id,
                "hook": hook,
                "error": str(error),
                "traceback": traceback.format_exc(),
            })
except SystemExit:
    raise
except BaseException as error:
    write_message({
        "type": "error",
        "error": str(error),
        "traceback": traceback.format_exc(),
    })
    raise SystemExit(1)
"#;

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::*;
    use crate::domain::execution::ExecutionVenue;
    use crate::scripting::engine::Script;

    fn write_python_script(source: &str, name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mlab-python-{name}-{}-{}.py",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::write(&path, source).expect("write Python test script");
        path
    }

    fn python_available(path: &Path) -> bool {
        PythonRuntime::resolve(path, None).is_ok()
    }

    fn candle_event(timestamp: u64, close: f64) -> Value {
        json!({
            "source": "btc@candles@binancef",
            "source_type": "candles",
            "provider": "binancef",
            "exchange": "binancef",
            "symbol": "BTC",
            "source_configs": {
                "btc@candles@binancef": {
                    "symbol": "BTC",
                    "type": "candles",
                    "provider": "binancef",
                    "exchange": "binancef",
                    "timeframe_sec": 60
                }
            },
            "positions": {
                "main": {
                    "open": [{
                        "symbol": "BTC",
                        "side": "long"
                    }]
                }
            },
            "data": {
                "candle": {
                    "t": timestamp,
                    "o": close,
                    "h": close,
                    "l": close,
                    "c": close,
                    "vb": 1.0,
                    "vs": 0.5,
                    "tb": 1,
                    "ts": 1
                }
            }
        })
    }

    fn resolved_python(path: &Path) -> Option<PythonRuntime> {
        PythonRuntime::resolve(path, None).ok()
    }

    fn start_test_process(
        path: &Path,
        runtime: &PythonRuntime,
        limits: PythonProcessLimits,
    ) -> PythonProcess {
        let mut process = PythonProcess::spawn_mode(path, runtime, "session", limits)
            .expect("start limited Python process");
        process
            .send(&json!({
                "type": "init",
                "params": {},
                "historyCapacity": 2,
                "executionEnabled": false,
                "executionKeyPrefix": "test-session",
                "artifactDir": std::env::temp_dir(),
            }))
            .expect("initialize limited Python process");
        let ready = process
            .receive(PYTHON_STARTUP_TIMEOUT)
            .expect("limited Python process should start");
        assert_eq!(ready["type"], "ready");
        process
    }

    #[test]
    fn python_process_uses_writable_marketlab_matplotlib_cache() {
        let path = write_python_script(
            r#"
import os

script = {"name": "python-matplotlib-cache", "version": "2"}

def on_data(ctx):
    return {"metrics": {"mplconfigdir": os.environ["MPLCONFIGDIR"]}}
"#,
            "matplotlib-cache",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let expected = matplotlib_config_directory().expect("Matplotlib cache directory");
        let script = Script::load(&path).expect("load Python script");
        let session = script.start_session(&json!({})).expect("start session");
        let execution = session
            .run_event(candle_event(1_000, 100.0))
            .expect("run Python event");
        assert_eq!(
            execution.output.metrics["mplconfigdir"],
            expected.display().to_string()
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_keeps_state_history_execution_and_artifacts() {
        let path = write_python_script(
            r#"
script = {
    "name": "python-state",
    "version": "2",
    "lookback": 3,
    "params": {"threshold": {"type": "number", "required": True}},
}

calls = 0

def on_data(ctx, history):
    global calls
    calls += 1
    candles = history.source("btc@candles@binancef")
    latest = history.source("btc@candles@binancef", 0)
    result = None
    if len(candles) == 2:
        result = ctx.order({
            "key": "bid-1",
            "exchange": "bulkf",
            "symbol": "BTC",
            "side": "buy",
            "size": 0.1,
            "order": {"type": "limit", "price": 99, "tif": "alo"},
        })
    return {"metrics": {
        "calls": calls,
        "count": len(candles),
        "has_mode": hasattr(ctx, "mode"),
        "latest": latest["c"],
        "threshold": ctx.params["threshold"],
        "source": ctx.source,
        "source_type": ctx.source_type,
        "provider": ctx.provider,
        "exchange": ctx.exchange,
        "symbol": ctx.symbol,
        "source_exchange": ctx.source_configs[ctx.source]["exchange"],
        "position_symbol": ctx.positions().open[0]["symbol"],
        "named_positions": len(ctx.positions("trading-2").open),
        "pnl_history": ctx.pnl(),
        "latest_pnl": ctx.pnl(0),
        "previous_pnl": ctx.pnl(1),
        "missing_pnl": ctx.pnl(99),
        "order": result,
    }}

def on_execution(ctx):
    return {"metrics": {"execution_status": ctx.execution["status"]}}

def on_finish(ctx, history):
    path = ctx.artifact_path("summary.txt")
    with open(path, "w", encoding="utf-8") as output:
        output.write(str(len(history.source("btc@candles@binancef"))))
    return {"meta": {"artifact": path, "pnl_history": ctx.pnl(), "latest_pnl": ctx.pnl(0)}}
"#,
            "state",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        assert_eq!(script.manifest.name, "python-state");
        let session = script
            .start_session_with_execution(
                &json!({ "threshold": 7 }),
                ScriptExecutionContext {
                    job_id: format!("python-test-{}", std::process::id()),
                    enabled: true,
                    request_routed: true,
                },
            )
            .expect("start Python session");

        let first = session
            .run_event_with_pnl(
                candle_event(1_000, 100.0),
                json!([{ "t": 1_000, "pnl": 0.0 }]),
            )
            .expect("first Python event");
        assert_eq!(first.output.metrics["calls"], 1);
        assert_eq!(first.output.metrics["count"], 1);
        assert_eq!(first.output.metrics["has_mode"], false);
        assert!(first.commands.is_empty());

        let second = session
            .run_event_with_pnl(
                candle_event(2_000, 101.0),
                json!([
                    { "t": 1_000, "pnl": 0.0 },
                    { "t": 2_000, "pnl": 2.5 }
                ]),
            )
            .expect("second Python event");
        assert_eq!(second.output.metrics["calls"], 2);
        assert_eq!(second.output.metrics["count"], 2);
        assert_eq!(second.output.metrics["latest"], 101.0);
        assert_eq!(second.output.metrics["threshold"], 7);
        assert_eq!(second.output.metrics["source"], "btc@candles@binancef");
        assert_eq!(second.output.metrics["source_type"], "candles");
        assert_eq!(second.output.metrics["provider"], "binancef");
        assert_eq!(second.output.metrics["exchange"], "binancef");
        assert_eq!(second.output.metrics["symbol"], "BTC");
        assert_eq!(second.output.metrics["source_exchange"], "binancef");
        assert_eq!(second.output.metrics["position_symbol"], "BTC");
        assert_eq!(second.output.metrics["named_positions"], 0);
        assert_eq!(
            second.output.metrics["pnl_history"],
            json!([
                { "t": 1_000, "pnl": 0.0 },
                { "t": 2_000, "pnl": 2.5 }
            ])
        );
        assert_eq!(
            second.output.metrics["latest_pnl"],
            json!({ "t": 2_000, "pnl": 2.5 })
        );
        assert_eq!(
            second.output.metrics["previous_pnl"],
            json!({ "t": 1_000, "pnl": 0.0 })
        );
        assert!(second.output.metrics["missing_pnl"].is_null());
        assert_eq!(second.commands.len(), 1);
        assert_eq!(second.output.metrics["order"]["key"], "bid-1");
        assert!(matches!(
            second.commands.as_slice(),
            [ScriptExecutionCommand::Order {
                exchange: Some(crate::domain::execution::ExecutionVenue::Bulk),
                ..
            }]
        ));

        let execution = session
            .run_execution_event(json!({ "status": "filled" }))
            .expect("run Python execution hook")
            .expect("execution hook exists");
        assert_eq!(execution.output.metrics["execution_status"], "filled");

        let finish = session
            .run_finish_with_pnl(json!([
                { "t": 1_000, "pnl": 0.0 },
                { "t": 2_000, "pnl": 2.5 },
                { "t": 3_000, "pnl": 3.0 }
            ]))
            .expect("run Python finish hook")
            .expect("finish hook exists");
        assert_eq!(
            finish.output.meta["pnl_history"],
            json!([
                { "t": 1_000, "pnl": 0.0 },
                { "t": 2_000, "pnl": 2.5 },
                { "t": 3_000, "pnl": 3.0 }
            ])
        );
        assert_eq!(
            finish.output.meta["latest_pnl"],
            json!({ "t": 3_000, "pnl": 3.0 })
        );
        let artifact = PathBuf::from(
            finish.output.meta["artifact"]
                .as_str()
                .expect("artifact path"),
        );
        assert_eq!(fs::read_to_string(&artifact).unwrap(), "2");

        let _ = fs::remove_file(artifact);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_generates_distinct_execution_keys_and_preserves_explicit_keys() {
        let path = write_python_script(
            r#"
script = {"name": "automatic-execution-keys", "version": "2"}

def on_data(ctx):
    first = ctx.order({
        "exchange": "hyperliquidf",
        "symbol": "ETH",
        "side": "buy",
        "size": 0.1,
    })
    second = ctx.order({
        "exchange": "hyperliquidf",
        "symbol": "ETH",
        "side": "buy",
        "size": 0.1,
    })
    explicit = ctx.trade({
        "key": "semantic-entry",
        "exchange": "hyperliquidf",
        "account": "trading-2",
        "symbol": "ETH",
        "position": "open-long",
        "margin": 100,
    })
    cancel = ctx.cancel({"order": first["id"]})
    return {"metrics": {
        "first_key": first["key"],
        "second_key": second["key"],
        "explicit_key": explicit["key"],
        "cancel_key": cancel["key"],
    }}
"#,
            "automatic-execution-keys",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script
            .start_session_with_execution(
                &json!({}),
                ScriptExecutionContext {
                    job_id: format!("python-test-{}", std::process::id()),
                    enabled: true,
                    request_routed: true,
                },
            )
            .expect("start Python session");
        let execution = session
            .run_event(candle_event(1_000, 100.0))
            .expect("run Python event");

        let first_key = execution.output.metrics["first_key"]
            .as_str()
            .expect("first automatic key");
        let second_key = execution.output.metrics["second_key"]
            .as_str()
            .expect("second automatic key");
        let cancel_key = execution.output.metrics["cancel_key"]
            .as_str()
            .expect("cancel automatic key");
        assert!(first_key.starts_with("auto-"));
        assert!(second_key.starts_with("auto-"));
        assert!(cancel_key.starts_with("auto-"));
        assert_ne!(first_key, second_key);
        assert_ne!(second_key, cancel_key);
        assert_eq!(execution.output.metrics["explicit_key"], "semantic-entry");
        assert_eq!(execution.commands.len(), 4);
        assert!(matches!(
            &execution.commands[2],
            crate::scripting::execution::ScriptExecutionCommand::Trade { request, .. }
                if request.account == "trading-2"
        ));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_reports_tracebacks() {
        let path = write_python_script(
            r#"
script = {"name": "python-error", "version": "2"}

def on_data(ctx, history):
    raise ValueError("broken signal")
"#,
            "error",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script.start_session(&json!({})).expect("start session");
        let error = session
            .run_event(candle_event(1_000, 100.0))
            .expect_err("hook should fail");
        let message = format!("{error:#}");
        assert!(message.contains("ValueError"));
        assert!(message.contains("broken signal"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_distinguishes_unconfigured_sources_from_empty_history() {
        let path = write_python_script(
            r#"
script = {"name": "configured-history", "version": "2"}

def on_data(history):
    history.source("eth@candles@hyperliquidf@mmt", 0)
"#,
            "configured-history",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script
            .start_session_with_execution_and_sources(
                &json!({}),
                ScriptExecutionContext {
                    job_id: format!("python-test-{}", std::process::id()),
                    enabled: false,
                    request_routed: true,
                },
                Some(&["btc@candles@binancef@mmt".to_string()]),
            )
            .expect("start Python session");
        let error = session
            .run_event(candle_event(1_000, 100.0))
            .expect_err("unconfigured history lookup must fail");
        let message = format!("{error:#}");
        assert!(message.contains("eth@candles@hyperliquidf@mmt"));
        assert!(message.contains("is not configured"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_hooks_receive_only_the_parameters_they_declare() {
        let path = write_python_script(
            r#"
script = {"name": "optional-hook-inputs", "version": "2"}

def on_data(ctx):
    return {"metrics": {"has_params": hasattr(ctx, "params")}}

def on_execution(ctx):
    return {"metrics": {"status": ctx.execution["status"]}}

def on_finish():
    return {"meta": {"finished": True}}
"#,
            "optional-hook-inputs",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script.start_session(&json!({})).expect("start session");

        let data = session
            .run_event(candle_event(1_000, 100.0))
            .expect("run one-parameter data hook");
        assert_eq!(data.output.metrics["has_params"], true);

        let execution = session
            .run_execution_event(json!({ "status": "filled" }))
            .expect("run one-parameter execution hook")
            .expect("execution hook exists");
        assert_eq!(execution.output.metrics["status"], "filled");

        let finish = session
            .run_finish()
            .expect("run zero-parameter finish hook")
            .expect("finish hook exists");
        assert_eq!(finish.output.meta["finished"], true);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_rejects_the_removed_event_hook_parameter() {
        let path = write_python_script(
            r#"
script = {"name": "removed-event-parameter", "version": "2"}

def on_data(ctx, event, history):
    return None
"#,
            "removed-event-parameter",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let error = match Script::load(&path) {
            Ok(_) => panic!("removed V2 event parameter should be rejected"),
            Err(error) => error,
        };
        let message = format!("{error:#}");
        assert!(message.contains("does not provide `event`"));
        assert!(message.contains("choose from ctx, history"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_exposes_every_v1_study_helper() {
        let path = write_python_script(
            r#"
script = {"name": "study-parity", "version": "2"}

def on_data(ctx):
    rows = [{"c": 10.0}, {"c": 20.0}, {"c": 30.0}, {"c": 40.0}]
    vd = [
        {"t": 1, "o": 100.0, "h": 115.0, "l": 95.0, "c": 110.0, "n": 10},
        {"t": 2, "o": 110.0, "h": 135.0, "l": 108.0, "c": 130.0, "n": 20},
        {"t": 3, "o": 130.0, "h": 140.0, "l": 120.0, "c": 125.0, "n": 30},
    ]
    book = {
        "exchange": "test",
        "symbol": "BTC",
        "timestamp_ms": 1,
        "bids": [
            {"price": 99.0, "quantity": 1.0},
            {"price": 98.0, "quantity": 2.0},
        ],
        "asks": [
            {"price": 101.0, "quantity": 1.0},
            {"price": 102.0, "quantity": 2.0},
        ],
    }
    sma = ctx.study.sma(rows, {"field": "c", "window": 3})
    ema = ctx.study.ema(rows, {"field": "c", "window": 3})
    cvd = ctx.study.cvd(vd, {"bucket": 7})
    spread = ctx.study.spread(book)
    depth = ctx.study.depth(book, {"levels": 2})
    imbalance = ctx.study.imbalance(book, {"depth": 2})
    slippage = ctx.study.slippage(book, {"side": "buy", "notional": 150})
    vamp = ctx.study.vamp(book, {"dollar_depth": 150})
    return {"metrics": {
        "sma_latest": sma["latest"],
        "ema_latest": ema["latest"],
        "cvd_latest": cvd["latest"],
        "spread_bps": spread["spread_bps"],
        "bid_quote": depth["bid_quote"],
        "imbalance": imbalance["imbalance"],
        "slippage_levels": slippage["levels_consumed"],
        "vamp": vamp["vamp"],
        "vamp_complete": vamp["complete"],
    }}
"#,
            "study-parity",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script.start_session(&json!({})).expect("start session");
        let execution = session
            .run_event(candle_event(1_000, 100.0))
            .expect("run Python studies");

        assert_eq!(execution.output.metrics["sma_latest"], 30.0);
        assert_eq!(execution.output.metrics["ema_latest"], 30.0);
        assert_eq!(execution.output.metrics["cvd_latest"], 25.0);
        assert_eq!(execution.output.metrics["bid_quote"], 295.0);
        assert_eq!(execution.output.metrics["imbalance"], 0.0);
        assert_eq!(execution.output.metrics["slippage_levels"], 2);
        assert_eq!(execution.output.metrics["vamp_complete"], true);
        assert!(
            execution.output.metrics["spread_bps"]
                .as_f64()
                .is_some_and(|value| value > 0.0)
        );
        assert!(
            execution.output.metrics["vamp"]
                .as_f64()
                .is_some_and(|value| value > 0.0)
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_v2_study_helpers_use_v1_validation() {
        let path = write_python_script(
            r#"
script = {"name": "study-validation", "version": "2"}

def on_data(ctx):
    ctx.study.sma([{"c": 1.0}], {"field": "missing", "window": 1})
"#,
            "study-validation",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script.start_session(&json!({})).expect("start session");
        let error = session
            .run_event(candle_event(1_000, 100.0))
            .expect_err("invalid study input should fail");
        assert!(format!("{error:#}").contains("field missing"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_extension_requires_v2_manifest() {
        let path = write_python_script(
            r#"
script = {"name": "wrong-version", "version": "1", "sources": ["candles"]}

def on_data(ctx, history):
    return None
"#,
            "wrong-version",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let error = match Script::load(&path) {
            Ok(_) => panic!("Python v1 manifest should be rejected"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("script.version must be \"2\""));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_inspection_derives_literal_history_sources() {
        let path = write_python_script(
            r#"
script = {"name": "literal-sources", "version": "2"}
CANDLES = "btc@candles@binancef:timeframe=60"

def on_data(ctx, history):
    candles = history.source(CANDLES)
    history.source(ctx.source, 0)
    history.source("eth@orderbook@bybitf@mmt:depth=20")
    return {"metrics": {"close": candles[-1]["c"]}}

def on_finish(ctx, history):
    history.source(CANDLES)
"#,
            "literal-sources",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        assert_eq!(
            script.source_declarations(),
            [
                "btc@candles@binancef:timeframe=60",
                "eth@orderbook@bybitf@mmt:depth=20",
            ]
        );
        let configured = [
            "btc@candles@binancef".to_string(),
            "eth@orderbook@bybitf@mmt".to_string(),
        ];
        let session = script
            .start_session_with_execution_and_sources(
                &json!({}),
                ScriptExecutionContext::disabled(),
                Some(&configured),
            )
            .expect("start Python session");
        let execution = session
            .run_event(candle_event(1_000, 100.0))
            .expect("read a configured source with inline options");
        assert_eq!(execution.output.metrics["close"], 100.0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_inspection_rejects_dynamic_history_sources() {
        let path = write_python_script(
            r#"
script = {"name": "dynamic-source", "version": "2"}

def on_data(ctx, history):
    symbol = "btc"
    history.source(f"{symbol}@candles@hyperliquidf:timeframe=60")
"#,
            "dynamic-source",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let error = match Script::load(&path) {
            Ok(_) => panic!("dynamic source selector should be rejected"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("cannot declare a source dynamically"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_inspection_derives_execution_venues() {
        let path = write_python_script(
            r#"
script = {"name": "execution-venues", "version": "2"}
PRIMARY_EXCHANGE = "hyperlinkf"

def on_data(ctx):
    ctx.trade({
        "exchange": PRIMARY_EXCHANGE,
        "symbol": "BTC",
        "position": "open-long",
        "margin": 10,
    })
    ctx.order({
        "exchange": "hyperliquidf-xyz",
        "symbol": "HYPE",
        "side": "sell",
        "size": 1,
    })
    ctx.trade({
        "exchange": PRIMARY_EXCHANGE,
        "symbol": "ETH",
        "position": "open-short",
        "margin": 10,
    })
"#,
            "execution-venues",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        assert_eq!(
            script.execution_venues(),
            [
                ExecutionVenue::Hyperlink,
                ExecutionVenue::parse("hyperliquidf-xyz").expect("XYZ venue"),
            ]
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_inspection_rejects_dynamic_execution_venues() {
        let path = write_python_script(
            r#"
script = {"name": "dynamic-execution-venue", "version": "2"}

def on_data(ctx):
    exchange = "hyperlinkf"
    ctx.trade({
        "exchange": exchange,
        "symbol": "BTC",
        "position": "open-long",
        "margin": 10,
    })
"#,
            "dynamic-execution-venue",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let error = match Script::load(&path) {
            Ok(_) => panic!("dynamic execution venue should be rejected"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("exchange on line"));
        assert!(format!("{error:#}").contains("cannot be dynamic"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_finish_cannot_submit_orders() {
        let path = write_python_script(
            r#"
script = {"name": "finish-order", "version": "2"}

def on_data(ctx, history):
    return None

def on_finish(ctx, history):
    ctx.cancel({"key": "cancel-at-finish"})
"#,
            "finish-order",
        );
        if !python_available(&path) {
            let _ = fs::remove_file(path);
            return;
        }

        let script = Script::load(&path).expect("load Python script");
        let session = script
            .start_session_with_execution(
                &json!({}),
                ScriptExecutionContext {
                    job_id: format!("python-test-{}", std::process::id()),
                    enabled: true,
                    request_routed: true,
                },
            )
            .expect("start Python session");
        let error = session
            .run_finish()
            .expect_err("finish hook should reject execution");
        assert!(format!("{error:#}").contains("unavailable inside on_finish"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_manifest_import_is_time_bounded() {
        let path = write_python_script(
            r#"
while True:
    pass
"#,
            "import-timeout",
        );
        let Some(runtime) = resolved_python(&path) else {
            let _ = fs::remove_file(path);
            return;
        };

        let error = inspect_python_script_with_limits(
            &path,
            &runtime,
            Duration::from_millis(150),
            PythonProcessLimits::default(),
        )
        .expect_err("infinite import should time out");
        assert!(format!("{error:#}").contains("timed out waiting for Python bridge"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_hook_infinite_loop_is_time_bounded() {
        let path = write_python_script(
            r#"
script = {"name": "hook-timeout", "version": "2"}

def on_data(ctx, history):
    while True:
        pass
"#,
            "hook-timeout",
        );
        let Some(runtime) = resolved_python(&path) else {
            let _ = fs::remove_file(path);
            return;
        };
        let session = PythonSession::start(
            &path,
            &runtime,
            &json!({}),
            2,
            None,
            ScriptExecutionContext::disabled(),
        )
        .expect("start timeout session");

        let error = session
            .run_hook(
                "on_data",
                json!({
                    "event": {},
                    "record": {"source": "candles", "value": {}, "identity": null},
                }),
                Duration::from_millis(150),
            )
            .expect_err("infinite hook should time out");
        assert!(format!("{error:#}").contains("Python on_data exceeded 150ms"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_process_tree_memory_is_limited() {
        let path = write_python_script(
            r#"
script = {"name": "memory-limit", "version": "2"}

def on_data(ctx, history):
    global allocation
    allocation = bytearray(256 * 1024 * 1024)
"#,
            "memory-limit",
        );
        let Some(runtime) = resolved_python(&path) else {
            let _ = fs::remove_file(path);
            return;
        };
        let limits = PythonProcessLimits {
            memory_bytes: 96 * 1024 * 1024,
            ..PythonProcessLimits::default()
        };
        let mut process = start_test_process(&path, &runtime, limits);
        process
            .send(&json!({
                "type": "hook",
                "hook": "on_data",
                "id": 1,
                "event": {},
                "record": {"source": "candles", "value": {}, "identity": null},
            }))
            .expect("send allocation hook");
        let error = process
            .receive(Duration::from_secs(5))
            .expect_err("memory-heavy hook should be terminated");
        assert!(format!("{error:#}").contains("memory; limit is"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_subprocess_count_is_limited_and_tree_is_killed() {
        let child_pid_path = std::env::temp_dir().join(format!(
            "mlab-python-child-{}-{}.pid",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let source = format!(
            r#"
import subprocess
import sys
import time
from pathlib import Path

script = {{"name": "process-limit", "version": "2"}}

def on_data(ctx, history):
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    Path({pid_path:?}).write_text(str(child.pid))
    time.sleep(60)
"#,
            pid_path = child_pid_path.display().to_string(),
        );
        let path = write_python_script(&source, "process-limit");
        let Some(runtime) = resolved_python(&path) else {
            let _ = fs::remove_file(path);
            return;
        };
        let limits = PythonProcessLimits {
            max_processes: 1,
            ..PythonProcessLimits::default()
        };
        let mut process = start_test_process(&path, &runtime, limits);
        process
            .send(&json!({
                "type": "hook",
                "hook": "on_data",
                "id": 1,
                "event": {},
                "record": {"source": "candles", "value": {}, "identity": null},
            }))
            .expect("send subprocess hook");
        let error = process
            .receive(Duration::from_secs(5))
            .expect_err("subprocess-heavy hook should be terminated");
        assert!(format!("{error:#}").contains("processes; limit is 1"));

        if let Ok(pid) = fs::read_to_string(&child_pid_path)
            && let Ok(pid) = pid.parse::<i32>()
        {
            for _ in 0..50 {
                let exists = unsafe { libc::kill(pid, 0) } == 0;
                if !exists {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            assert_ne!(
                unsafe { libc::kill(pid, 0) },
                0,
                "subprocess survived group kill"
            );
        }

        let _ = fs::remove_file(child_pid_path);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn python_protocol_lines_are_bounded() {
        let mut accepted = io::Cursor::new(b"1234\n".to_vec());
        assert_eq!(
            read_bounded_line(&mut accepted, 4).expect("bounded line"),
            Some(b"1234".to_vec())
        );

        let mut rejected = io::Cursor::new(b"12345\n".to_vec());
        let error = read_bounded_line(&mut rejected, 4).expect_err("oversized line must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
