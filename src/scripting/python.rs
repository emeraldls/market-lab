use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use super::engine::ScriptExecution;
use super::execution::{
    ScriptCommandBuffer, ScriptExecutionCommand, ScriptExecutionContext, queue_execution_call,
};
use super::language::PythonRuntime;
use super::limits::SCRIPT_PYTHON_HOOK_TIMEOUT_MS;
use super::manifest::ScriptManifest;
use super::output::ScriptOutput;
use super::telemetry::ScriptHookStats;

const PYTHON_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const PYTHON_HOOK_TIMEOUT: Duration = Duration::from_millis(SCRIPT_PYTHON_HOOK_TIMEOUT_MS);
const PYTHON_FINISH_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) fn inspect_python_script(
    path: &Path,
    runtime: &PythonRuntime,
) -> Result<ScriptManifest> {
    let output = Command::new(&runtime.interpreter)
        .args(["-u", "-c", PYTHON_RUNNER])
        .arg(path)
        .arg("describe")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| {
            format!(
                "failed to inspect Python script {} with {}",
                path.display(),
                runtime.interpreter.display()
            )
        })?;
    let stdout =
        String::from_utf8(output.stdout).context("Python bridge returned non-UTF-8 data")?;
    let response = stdout
        .lines()
        .find_map(|line| serde_json::from_str::<Value>(line).ok());
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        if let Some(response) = response.as_ref()
            && response.get("type").and_then(Value::as_str) == Some("error")
        {
            bail!(
                "failed to load Python script: {}",
                python_error_message(response)
            );
        }
        bail!(
            "failed to load Python script {}{}",
            path.display(),
            suffix_error(&stderr)
        );
    }
    let response = response.context("Python bridge returned no manifest")?;
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
}

impl PythonProcess {
    fn spawn(path: &Path, runtime: &PythonRuntime) -> Result<Self> {
        let mut child = Command::new(&runtime.interpreter)
            .args(["-u", "-c", PYTHON_RUNNER])
            .arg(path)
            .arg("session")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start Python script {} with {}",
                    path.display(),
                    runtime.interpreter.display()
                )
            })?;
        let stdin = child.stdin.take().context("Python bridge has no stdin")?;
        let stdout = child.stdout.take().context("Python bridge has no stdout")?;
        let (sender, messages) = mpsc::channel();
        thread::Builder::new()
            .name("mlab-python-bridge".to_string())
            .spawn(move || {
                let reader = BufReader::new(stdout);
                for line in reader.lines() {
                    let message = match line {
                        Ok(line) => serde_json::from_str(&line).map_err(|error| {
                            format!("invalid Python bridge JSON: {error}; line={line}")
                        }),
                        Err(error) => Err(format!("failed to read Python bridge: {error}")),
                    };
                    if sender.send(message).is_err() {
                        break;
                    }
                }
            })
            .context("failed to start Python bridge reader")?;
        Ok(Self {
            child,
            stdin,
            messages,
            request_id: 0,
        })
    }

    fn next_request_id(&mut self) -> u64 {
        self.request_id = self.request_id.saturating_add(1);
        self.request_id
    }

    fn send(&mut self, value: &Value) -> Result<()> {
        serde_json::to_writer(&mut self.stdin, value)
            .context("failed to write to Python bridge")?;
        self.stdin
            .write_all(b"\n")
            .context("failed to delimit Python bridge message")?;
        self.stdin.flush().context("failed to flush Python bridge")
    }

    fn receive(&mut self, timeout: Duration) -> Result<Value> {
        match self.messages.recv_timeout(timeout) {
            Ok(Ok(value)) => Ok(value),
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
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for PythonProcess {
    fn drop(&mut self) {
        let _ = self.send(&json!({ "type": "shutdown" }));
        if self.child.try_wait().ok().flatten().is_none() {
            self.kill();
        }
    }
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

fn suffix_error(error: &str) -> String {
    if error.is_empty() {
        String::new()
    } else {
        format!(": {error}")
    }
}

const PYTHON_RUNNER: &str = r#"
import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import traceback

SCRIPT_PATH = Path(sys.argv[1]).resolve()
MODE = sys.argv[2]
PROTOCOL_OUT = sys.stdout
sys.stdout = sys.stderr


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
    if not callable(getattr(module, "on_data", None)):
        raise TypeError("Python scripts require `on_data(ctx, event, history)`")
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
            raise RuntimeError("script execution is disabled; deploy the script with --venue")
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
                output = MODULE.on_data(CTX, request["event"], HISTORY)
                present = True
            elif hook == "on_execution":
                function = getattr(MODULE, "on_execution", None)
                present = callable(function)
                output = function(CTX, request["event"]) if present else None
            elif hook == "on_finish":
                function = getattr(MODULE, "on_finish", None)
                present = callable(function)
                CTX._finishing = True
                try:
                    output = function(CTX, HISTORY) if present else None
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
                },
            )
            .expect("start Python session");
        let error = session
            .run_finish()
            .expect_err("finish hook should reject execution");
        assert!(format!("{error:#}").contains("unavailable inside on_finish"));

        let _ = fs::remove_file(path);
    }
}
