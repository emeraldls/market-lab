use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
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
) -> Result<ScriptManifest> {
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
) -> Result<ScriptManifest> {
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
    Ok(manifest)
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
        execution: ScriptExecutionContext,
    ) -> Result<Self> {
        let artifact_dir = artifact_directory(&execution.job_id)?;
        let mut process = PythonProcess::spawn(path, runtime)?;
        process.send(&json!({
            "type": "init",
            "params": params,
            "historyCapacity": history_capacity,
            "executionEnabled": execution.enabled,
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
    ) -> Result<ScriptExecution> {
        self.run_hook(
            "on_data",
            json!({
                "event": payload,
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

    pub(crate) fn run_finish(&self) -> Result<Option<ScriptExecution>> {
        self.run_hook("on_finish", json!({}), PYTHON_FINISH_TIMEOUT)
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
        let mut command = Command::new(&runtime.interpreter);
        command
            .args(["-u", "-c", PYTHON_RUNNER])
            .arg(path)
            .arg(mode)
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
    "on_data": ("ctx", "event", "history"),
    "on_execution": ("ctx", "event"),
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


class History:
    def __init__(self, capacity):
        self.capacity = max(2, int(capacity))
        self.records = {}
        self.identities = {}

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
        records = self.records.get(name, [])
        if offset is None:
            return copy.deepcopy(list(reversed(records)))
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("history.source offset must be a non-negative integer")
        return copy.deepcopy(records[offset]) if offset < len(records) else None


class Context:
    def __init__(self, params, execution_enabled, artifact_dir):
        self.params = copy.deepcopy(params)
        self._execution_enabled = execution_enabled
        self._artifact_dir = Path(artifact_dir).resolve()
        self._request_id = 0
        self._finishing = False

    def _execution(self, operation, payload):
        if self._finishing:
            raise RuntimeError(f"ctx.{operation} is unavailable inside on_finish")
        if not self._execution_enabled:
            raise RuntimeError("script execution is disabled")
        if not isinstance(payload, dict):
            raise TypeError(f"ctx.{operation} requires a dictionary")
        self._request_id += 1
        write_message({
            "type": "execution",
            "requestId": self._request_id,
            "operation": operation,
            "payload": payload,
        })
        line = sys.stdin.readline()
        if not line:
            raise RuntimeError("MarketLab closed the Python execution channel")
        response = json.loads(line)
        if response.get("type") != "execution_result" or response.get("requestId") != self._request_id:
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
    if MODE == "describe":
        write_message({
            "type": "ready",
            "manifest": MANIFEST,
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
    HISTORY = History(init["historyCapacity"])
    CTX = Context(init.get("params", {}), init.get("executionEnabled", False), init["artifactDir"])
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
                output = invoke_hook("on_data", MODULE.on_data, {
                    "ctx": CTX,
                    "event": request["event"],
                    "history": HISTORY,
                })
                present = True
            elif hook == "on_execution":
                function = getattr(MODULE, "on_execution", None)
                present = callable(function)
                output = invoke_hook("on_execution", function, {
                    "ctx": CTX,
                    "event": request["event"],
                }) if present else None
            elif hook == "on_finish":
                function = getattr(MODULE, "on_finish", None)
                present = callable(function)
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
            "symbol": "BTC",
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
    fn python_v2_keeps_state_history_execution_and_artifacts() {
        let path = write_python_script(
            r#"
script = {
    "name": "python-state",
    "version": "2",
    "sources": ["candles"],
    "lookback": 3,
    "params": {"threshold": {"type": "number", "required": True}},
}

calls = 0

def on_data(ctx, event, history):
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
        "order": result,
    }}

def on_execution(ctx, event):
    return {"metrics": {"execution_status": event["status"]}}

def on_finish(ctx, history):
    path = ctx.artifact_path("summary.txt")
    with open(path, "w", encoding="utf-8") as output:
        output.write(str(len(history.source("btc@candles@binancef"))))
    return {"meta": {"artifact": path}}
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
            .run_event(candle_event(1_000, 100.0))
            .expect("first Python event");
        assert_eq!(first.output.metrics["calls"], 1);
        assert_eq!(first.output.metrics["count"], 1);
        assert_eq!(first.output.metrics["has_mode"], false);
        assert!(first.commands.is_empty());

        let second = session
            .run_event(candle_event(2_000, 101.0))
            .expect("second Python event");
        assert_eq!(second.output.metrics["calls"], 2);
        assert_eq!(second.output.metrics["count"], 2);
        assert_eq!(second.output.metrics["latest"], 101.0);
        assert_eq!(second.output.metrics["threshold"], 7);
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
            .run_finish()
            .expect("run Python finish hook")
            .expect("finish hook exists");
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
    fn python_v2_reports_tracebacks() {
        let path = write_python_script(
            r#"
script = {"name": "python-error", "version": "2", "sources": ["candles"]}

def on_data(ctx, event, history):
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
    fn python_hooks_receive_only_the_parameters_they_declare() {
        let path = write_python_script(
            r#"
script = {"name": "optional-hook-inputs", "version": "2", "sources": ["candles"]}

def on_data(ctx):
    return {"metrics": {"has_params": hasattr(ctx, "params")}}

def on_execution(event):
    return {"metrics": {"status": event["status"]}}

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
    fn python_extension_requires_v2_manifest() {
        let path = write_python_script(
            r#"
script = {"name": "wrong-version", "version": "1", "sources": ["candles"]}

def on_data(ctx, event, history):
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
    fn python_finish_cannot_submit_orders() {
        let path = write_python_script(
            r#"
script = {"name": "finish-order", "version": "2", "sources": ["candles"]}

def on_data(ctx, event, history):
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
script = {"name": "hook-timeout", "version": "2", "sources": ["candles"]}

def on_data(ctx, event, history):
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
script = {"name": "memory-limit", "version": "2", "sources": ["candles"]}

def on_data(ctx, event, history):
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

script = {{"name": "process-limit", "version": "2", "sources": ["candles"]}}

def on_data(ctx, event, history):
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
