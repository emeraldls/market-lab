use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context as AnyhowContext, Result};
use rquickjs::{CatchResultExt, Context, Ctx, Function, Module, Object, Promise, Runtime, Value};
use serde_json::Value as JsonValue;

use super::execution::{
    ScriptCommandBuffer, ScriptExecutionCommand, ScriptExecutionContext, attach_execution_helpers,
};
use super::limits::{SCRIPT_DEFAULT_LOOKBACK_CANDLES, default_limits};
use super::manifest::ScriptManifest;
use super::output::ScriptOutput;
use super::studies::attach_study_helpers;
use super::telemetry::ScriptHookStats;

const MIN_SOURCE_HISTORY_RECORDS: usize = 2;

#[derive(Debug)]
struct SourceHistory {
    capacity: usize,
    records: BTreeMap<String, VecDeque<JsonValue>>,
    identities: BTreeMap<String, u64>,
}

impl SourceHistory {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(MIN_SOURCE_HISTORY_RECORDS),
            records: BTreeMap::new(),
            identities: BTreeMap::new(),
        }
    }

    fn record(&mut self, source: String, value: JsonValue, identity: Option<u64>) {
        let records = self.records.entry(source.clone()).or_default();
        let replaces_current = identity.is_some()
            && self.identities.get(&source) == identity.as_ref()
            && !records.is_empty();

        if replaces_current {
            records[0] = value;
            return;
        }

        records.push_front(value);
        records.truncate(self.capacity);
        if let Some(identity) = identity {
            self.identities.insert(source, identity);
        } else {
            self.identities.remove(&source);
        }
    }

    fn record_at(&self, source: &str, offset: usize) -> Option<JsonValue> {
        self.records
            .get(source)
            .and_then(|records| records.get(offset))
            .cloned()
    }

    fn records(&self, source: &str) -> JsonValue {
        JsonValue::Array(
            self.records
                .get(source)
                .into_iter()
                .flat_map(|records| records.iter().rev())
                .cloned()
                .collect(),
        )
    }
}

pub struct Script {
    pub path: PathBuf,
    pub manifest: ScriptManifest,
    source: String,
}

impl Script {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read script {}", path.display()))?;
        let manifest = inspect_manifest(path, &source)?;
        Ok(Self {
            path: path.to_path_buf(),
            manifest,
            source,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn history_capacity(&self, params: &JsonValue) -> usize {
        if let Some(lookback) = self.manifest.lookback {
            return lookback;
        }

        params
            .as_object()
            .and_then(|params| params.get("lookback").and_then(JsonValue::as_f64))
            .filter(|value| value.is_finite() && *value >= 1.0)
            .map(|value| value.floor() as usize)
            .unwrap_or(SCRIPT_DEFAULT_LOOKBACK_CANDLES)
            .min(SCRIPT_DEFAULT_LOOKBACK_CANDLES)
    }

    pub fn start_session(&self, params: &JsonValue) -> Result<ScriptSession> {
        self.start_session_with_execution(params, ScriptExecutionContext::disabled())
    }

    pub fn start_session_with_execution(
        &self,
        params: &JsonValue,
        execution: ScriptExecutionContext,
    ) -> Result<ScriptSession> {
        let limits = default_limits();
        let rt = Runtime::new().context("failed to create QuickJS runtime")?;
        rt.set_memory_limit(limits.heap_bytes);
        rt.set_max_stack_size(limits.stack_bytes);
        let hook_started = Arc::new(Mutex::new(Instant::now()));
        let interrupt_started = Arc::clone(&hook_started);
        let cancelled = Arc::new(AtomicBool::new(false));
        let interrupt_cancelled = Arc::clone(&cancelled);
        rt.set_interrupt_handler(Some(Box::new(move || {
            if interrupt_cancelled.load(Ordering::Relaxed) {
                return true;
            }
            interrupt_started
                .lock()
                .map(|started| started.elapsed().as_millis() as u64 > limits.hook_timeout_ms)
                .unwrap_or(true)
        })));
        let ctx = Context::full(&rt).context("failed to create QuickJS context")?;
        let commands: ScriptCommandBuffer = Arc::new(Mutex::new(Vec::new()));
        let history = Arc::new(Mutex::new(SourceHistory::new(
            self.history_capacity(params),
        )));

        ctx.with(|ctx| -> Result<()> {
            let module =
                Module::declare(ctx.clone(), module_name(&self.path), self.source.as_str())
                    .context("failed to declare JS module")?;
            let (module, promise) = module
                .eval()
                .catch(&ctx)
                .map_err(|err| anyhow::anyhow!("failed to evaluate JS module: {}", err))?;
            finish_promise(&ctx, &promise).context("failed to finish JS module evaluation")?;

            let namespace = module
                .namespace()
                .context("failed to read JS module namespace")?;
            let hook: Function = namespace
                .get("onData")
                .context("scripts require export `onData(ctx, input, history)`")?;
            let execution_hook: Option<Function> = namespace
                .get("onExecution")
                .context("failed to inspect optional onExecution hook")?;

            let script_ctx = Object::new(ctx.clone()).context("failed to create script ctx")?;
            let params_val = json_to_js(ctx.clone(), params)?;
            script_ctx
                .set("params", params_val)
                .context("failed to assign ctx.params")?;
            attach_study_helpers(ctx.clone(), &script_ctx)?;
            attach_execution_helpers(ctx.clone(), &script_ctx, &execution, &commands)?;
            let history_helper = attach_history_helper(ctx.clone(), Arc::clone(&history))?;

            let globals = ctx.globals();
            globals
                .set("__mlab_onData", hook)
                .context("failed to store onData hook")?;
            globals
                .set("__mlab_ctx", script_ctx)
                .context("failed to store script ctx")?;
            globals
                .set("__mlab_history", history_helper)
                .context("failed to store history helper")?;
            if let Some(execution_hook) = execution_hook {
                globals
                    .set("__mlab_onExecution", execution_hook)
                    .context("failed to store onExecution hook")?;
            }
            Ok(())
        })?;

        Ok(ScriptSession {
            rt,
            ctx,
            hook_started,
            cancelled,
            commands,
            history,
        })
    }
}

pub struct ScriptSession {
    ctx: Context,
    rt: Runtime,
    hook_started: Arc<Mutex<Instant>>,
    cancelled: Arc<AtomicBool>,
    commands: ScriptCommandBuffer,
    history: Arc<Mutex<SourceHistory>>,
}

impl ScriptSession {
    pub fn cancel_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn run_candles_window(&self, candles: &JsonValue) -> Result<ScriptExecution> {
        let candles = candles
            .as_array()
            .context("test candle window must be an array")?;
        for candle in candles {
            self.record_source(
                "candles@binancef@mmt",
                candle.clone(),
                candle.get("t").and_then(JsonValue::as_u64),
            )?;
        }
        self.run_on_data(serde_json::json!({ "mode": "window" }))
    }

    #[cfg(test)]
    pub fn run_orderbook_window(&self, books: &JsonValue) -> Result<ScriptExecution> {
        let books = books
            .as_array()
            .context("test orderbook window must be an array")?;
        for book in books {
            self.record_source("orderbook@binancef@mmt", book.clone(), None)?;
        }
        self.run_on_data(serde_json::json!({ "mode": "window" }))
    }

    pub fn run_window(&self, payload: JsonValue) -> Result<ScriptExecution> {
        self.run_on_data(payload)
    }

    pub fn run_stream(&self, mut payload: JsonValue) -> Result<ScriptExecution> {
        let (source, record, identity) = stream_history_entry(&payload)?;
        self.record_source(&source, record, identity)?;
        strip_source_data(&mut payload);
        self.run_on_data(payload)
    }

    pub(crate) fn record_source(
        &self,
        source: &str,
        record: JsonValue,
        identity: Option<u64>,
    ) -> Result<()> {
        self.history
            .lock()
            .map_err(|_| anyhow::anyhow!("script source history lock poisoned"))?
            .record(source.to_string(), record, identity);
        Ok(())
    }

    pub fn run_execution_event(&self, event: JsonValue) -> Result<Option<ScriptExecution>> {
        let has_hook = self.ctx.with(|ctx| -> Result<bool> {
            Ok(!ctx
                .globals()
                .get::<_, Value>("__mlab_onExecution")?
                .is_undefined())
        })?;
        if !has_hook {
            return Ok(None);
        }
        self.run_hook("__mlab_onExecution", "onExecution", event, false)
            .map(Some)
    }

    fn run_on_data(&self, input_payload: JsonValue) -> Result<ScriptExecution> {
        self.run_hook("__mlab_onData", "onData", input_payload, true)
    }

    fn run_hook(
        &self,
        global_name: &str,
        display_name: &str,
        input_payload: JsonValue,
        include_history: bool,
    ) -> Result<ScriptExecution> {
        let started = Instant::now();
        {
            let mut hook_started = self
                .hook_started
                .lock()
                .map_err(|_| anyhow::anyhow!("script runtime timer lock poisoned"))?;
            *hook_started = started;
        }

        self.clear_commands()?;
        let output = self.ctx.with(|ctx| -> Result<ScriptOutput> {
            let globals = ctx.globals();
            let hook: Function = globals
                .get(global_name)
                .with_context(|| format!("script has no `{display_name}` hook"))?;
            let script_ctx: Object = globals
                .get("__mlab_ctx")
                .context("failed to read script ctx")?;
            let input_val = json_to_js(ctx.clone(), &input_payload)?;
            let result: Value = if include_history {
                let history: Object = globals
                    .get("__mlab_history")
                    .context("failed to read history helper")?;
                hook.call((script_ctx, input_val, history))
                    .catch(&ctx)
                    .map_err(|err| anyhow::anyhow!("{display_name} failed: {}", err))?
            } else {
                hook.call((script_ctx, input_val))
                    .catch(&ctx)
                    .map_err(|err| anyhow::anyhow!("{display_name} failed: {}", err))?
            };
            let result_json = js_to_json_or_null(ctx.clone(), result)?;
            ScriptOutput::from_json(result_json)
        });
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                self.clear_commands()?;
                return Err(error);
            }
        };
        let commands = self.drain_commands()?;

        let memory_usage = self.rt.memory_usage();
        Ok(ScriptExecution {
            output,
            commands,
            stats: ScriptHookStats {
                duration_ms: started.elapsed().as_millis() as u64,
                heap_used_bytes: u64::try_from(memory_usage.memory_used_size).ok(),
            },
        })
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

#[derive(Debug)]
pub struct ScriptExecution {
    pub output: ScriptOutput,
    pub commands: Vec<ScriptExecutionCommand>,
    pub stats: ScriptHookStats,
}

fn inspect_manifest(path: &Path, source: &str) -> Result<ScriptManifest> {
    let limits = default_limits();
    let rt = Runtime::new().context("failed to create QuickJS runtime")?;
    rt.set_memory_limit(limits.heap_bytes);
    rt.set_max_stack_size(limits.stack_bytes);
    let ctx = Context::full(&rt).context("failed to create QuickJS context")?;

    ctx.with(|ctx| -> Result<ScriptManifest> {
        let module = Module::declare(ctx.clone(), module_name(path), source)
            .context("failed to declare JS module")?;
        let (module, promise) = module
            .eval()
            .catch(&ctx)
            .map_err(|err| anyhow::anyhow!("failed to evaluate JS module: {}", err))?;
        finish_promise(&ctx, &promise).context("failed to finish JS module evaluation")?;

        let namespace = module
            .namespace()
            .context("failed to read JS module namespace")?;
        let manifest_value: Value = namespace
            .get("script")
            .context("script has no `script` export")?;
        if manifest_value.is_undefined() {
            anyhow::bail!("script has no `script` export");
        }
        let manifest_json = js_to_json(ctx.clone(), manifest_value)?;
        let manifest: ScriptManifest =
            serde_json::from_value(manifest_json).context("failed to decode `script` manifest")?;
        manifest.validate()?;
        Ok(manifest)
    })
}

fn finish_promise<'js>(ctx: &Ctx<'js>, promise: &Promise<'js>) -> Result<()> {
    promise
        .finish::<()>()
        .catch(ctx)
        .map_err(|err| anyhow::anyhow!("{}", err))
}

fn json_to_js<'js>(ctx: rquickjs::Ctx<'js>, value: &JsonValue) -> Result<Value<'js>> {
    let json = serde_json::to_string(value).context("failed to encode JSON for JS")?;
    ctx.json_parse(json)
        .map_err(|err| anyhow::anyhow!(err.to_string()))
}

fn js_to_json<'js>(ctx: rquickjs::Ctx<'js>, value: Value<'js>) -> Result<JsonValue> {
    let json = ctx
        .json_stringify(value)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
        .ok_or_else(|| anyhow::anyhow!("script return value is not JSON-serializable"))?;
    serde_json::from_str(&json.to_string()?).context("failed to decode JS JSON")
}

fn js_to_json_or_null<'js>(ctx: rquickjs::Ctx<'js>, value: Value<'js>) -> Result<JsonValue> {
    if value.is_undefined() || value.is_null() {
        Ok(JsonValue::Null)
    } else {
        js_to_json(ctx, value)
    }
}

fn attach_history_helper<'js>(
    ctx: Ctx<'js>,
    history: Arc<Mutex<SourceHistory>>,
) -> Result<Object<'js>> {
    let native = Function::new(ctx.clone(), move |source: String, offset: i64| {
        native_history_source(&history, &source, offset)
    })
    .context("failed to create native history function")?;
    ctx.globals()
        .set("__mlab_history_source", native)
        .context("failed to expose native history function")?;
    ctx.eval(HISTORY_HELPER_JS)
        .context("failed to create history helper")
}

fn native_history_source(history: &Arc<Mutex<SourceHistory>>, source: &str, offset: i64) -> String {
    let response = match history.lock() {
        Ok(history) if offset < 0 => {
            serde_json::json!({ "found": true, "value": history.records(source) })
        }
        Ok(history) => match usize::try_from(offset)
            .ok()
            .and_then(|offset| history.record_at(source, offset))
        {
            Some(value) => serde_json::json!({ "found": true, "value": value }),
            None => serde_json::json!({ "found": false }),
        },
        Err(_) => serde_json::json!({ "error": "script source history lock poisoned" }),
    };
    serde_json::to_string(&response).expect("history response must serialize")
}

fn stream_history_entry(input: &JsonValue) -> Result<(String, JsonValue, Option<u64>)> {
    let selector = input
        .get("source")
        .and_then(JsonValue::as_str)
        .context("stream input.source is required for source history")?;
    let source = input
        .get("source_type")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            selector
                .split_once('@')
                .map_or(selector, |(source, _)| source)
        });
    let current = input.get("data");
    let record = match source {
        "candles" => current.and_then(|value| value.get("candle")),
        "orderbook" => current.and_then(|value| value.get("snapshot")),
        "vd" => current.and_then(|value| value.get("record").or_else(|| value.get("candle"))),
        "oi" => current.and_then(|value| value.get("record").or_else(|| value.get("candle"))),
        "volumes" => current.and_then(|value| value.get("record").or_else(|| value.get("profile"))),
        _ => anyhow::bail!("unknown stream input.source `{selector}`"),
    }
    .with_context(|| format!("stream input has no current {source} record"))?;

    let replaces_same_timestamp = match source {
        "candles" | "volumes" => true,
        "vd" => record.get("delta").is_none(),
        "oi" => record.get("mark_price").is_none(),
        "orderbook" => false,
        _ => unreachable!(),
    };
    let identity = replaces_same_timestamp
        .then(|| record.get("t").and_then(JsonValue::as_u64))
        .flatten();

    Ok((selector.to_string(), record.clone(), identity))
}

fn strip_source_data(input: &mut JsonValue) {
    let Some(input) = input.as_object_mut() else {
        return;
    };
    for key in [
        "data",
        "sources",
        "candles",
        "orderbook",
        "vd",
        "oi",
        "volumes",
    ] {
        input.remove(key);
    }
}

const HISTORY_HELPER_JS: &str = r#"
(() => {
  const deepFreeze = (value) => {
    if (value && typeof value === "object" && !Object.isFrozen(value)) {
      Object.freeze(value);
      for (const child of Object.values(value)) deepFreeze(child);
    }
    return value;
  };

  return Object.freeze({
    source(name, offset) {
      if (typeof name !== "string" || name.length === 0) {
        throw new TypeError("history.source name must be a non-empty string");
      }
      const list = arguments.length < 2 || offset === undefined;
      if (!list && (!Number.isSafeInteger(offset) || offset < 0)) {
        throw new RangeError("history.source offset must be a non-negative integer");
      }

      const response = JSON.parse(globalThis.__mlab_history_source(name, list ? -1 : offset));
      if (response.error) throw new Error(response.error);
      return response.found ? deepFreeze(response.value) : undefined;
    }
  });
})()
"#;

fn module_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("script.js")
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Instant;

    use serde_json::json;

    use super::Script;

    fn write_temp_script(contents: &str, stem: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("mlab-script-{}-{}.js", stem, std::process::id()));
        fs::write(&path, contents).expect("write temp script");
        path
    }

    fn synthetic_candles(count: usize) -> serde_json::Value {
        serde_json::Value::Array(
            (0..count)
                .map(|idx| {
                    let close = 100.0 + idx as f64 * 0.01;
                    json!({
                        "t": 1_700_000_000_i64 + idx as i64 * 60,
                        "o": close - 0.1,
                        "h": close + 0.2,
                        "l": close - 0.2,
                        "c": close,
                        "vb": 100_000.0 + idx as f64,
                        "vs": 95_000.0,
                        "tb": 100 + idx,
                        "ts": 90 + idx
                    })
                })
                .collect(),
        )
    }

    #[test]
    fn loads_manifest_from_js_module() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "buy-pressure-filter",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {
    min_vbuy: { type: "number", required: true }
  }
};

export function onData(ctx, input, history) {
  return { metrics: { count: history.source("candles@binancef@mmt").length, threshold: ctx.params.min_vbuy } };
}
"#,
            "manifest",
        );

        let script = Script::load(&path).expect("load script");
        assert_eq!(script.manifest.name, "buy-pressure-filter");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_legacy_study_export() {
        let path = write_temp_script(
            r#"
export const study = {
  name: "legacy-study",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  return { metrics: { candles: history.source("candles@binancef@mmt").length } };
}
"#,
            "legacy-study",
        );

        let err = match Script::load(&path) {
            Ok(_) => panic!("study export should not be accepted"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("script has no `script` export"),
            "{err:?}"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn runs_candles_window_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "buy-pressure-filter",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {
    min_vbuy: { type: "number", required: true }
  }
};

export function onData(ctx, input, history) {
  const candles = history.source("candles@binancef@mmt");
  const filtered = candles.filter((c) => c.vb >= ctx.params.min_vbuy);
  return {
    metrics: {
      qualifying_candles: filtered.length,
      latest_close: candles[candles.length - 1].c
    }
  };
}
"#,
            "run",
        );

        let script = Script::load(&path).expect("load script");
        let inputs = json!({ "min_vbuy": 150.0 });
        let candles = json!([
            { "t": 1, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "vb": 100.0, "vs": 80.0, "tb": 10, "ts": 9 },
            { "t": 2, "o": 1.5, "h": 2.2, "l": 1.0, "c": 2.0, "vb": 200.0, "vs": 90.0, "tb": 12, "ts": 10 }
        ]);
        let session = script.start_session(&inputs).expect("start session");
        let execution = session.run_candles_window(&candles).expect("run script");

        assert_eq!(execution.output.metrics["qualifying_candles"], 1);
        assert_eq!(execution.output.metrics["latest_close"], 2.0);
        assert!(execution.stats.heap_used_bytes.is_some());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn exposes_candle_study_helpers_to_js() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "helper-script",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  const candles = history.source("candles@binancef@mmt");
  const sma = ctx.study.sma(candles, { field: "c", window: 3 });
  const ema = ctx.study.ema(candles, { field: "c", window: 3 });
  return {
    metrics: {
      sma_latest: sma.latest,
      sma_previous: sma.previous,
      ema_latest: ema.latest
    }
  };
}
"#,
            "helpers",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let candles = json!([
            { "t": 1, "o": 1.0, "h": 1.0, "l": 1.0, "c": 10.0, "vb": 100.0, "vs": 80.0, "tb": 1, "ts": 1 },
            { "t": 2, "o": 1.0, "h": 1.0, "l": 1.0, "c": 20.0, "vb": 120.0, "vs": 90.0, "tb": 1, "ts": 1 },
            { "t": 3, "o": 1.0, "h": 1.0, "l": 1.0, "c": 30.0, "vb": 130.0, "vs": 100.0, "tb": 1, "ts": 1 },
            { "t": 4, "o": 1.0, "h": 1.0, "l": 1.0, "c": 40.0, "vb": 140.0, "vs": 100.0, "tb": 1, "ts": 1 }
        ]);
        let execution = session.run_candles_window(&candles).expect("run script");

        assert_eq!(execution.output.metrics["sma_latest"], 30.0);
        assert_eq!(execution.output.metrics["sma_previous"], 20.0);
        assert_eq!(execution.output.metrics["ema_latest"], 30.0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn exposes_cvd_helper_for_vd_candles_to_js() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "vd-cvd-helper",
  version: "1",
  sources: ["vd"],
  params: {}
};

export function onData(ctx, input, history) {
  const candles = history.source("vd@hyperliquid@mmt");
  const bucket = input.source_configs["vd@hyperliquid@mmt"].bucket;
  const cvd = ctx.study.cvd(candles, { bucket });
  const single = ctx.study.cvd(candles[candles.length - 1], { bucket });
  return {
    metrics: {
      bucket: cvd.bucket,
      latest: cvd.latest,
      previous: cvd.previous,
      delta: cvd.delta,
      points: cvd.points.length,
      single_delta: single.delta
    }
  };
}
"#,
            "vd-cvd-helper",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let vd = json!([
            { "t": 1, "o": 100.0, "h": 115.0, "l": 95.0, "c": 110.0, "n": 10 },
            { "t": 2, "o": 110.0, "h": 135.0, "l": 108.0, "c": 130.0, "n": 20 },
            { "t": 3, "o": 130.0, "h": 140.0, "l": 120.0, "c": 125.0, "n": 30 }
        ]);
        for candle in vd.as_array().unwrap() {
            session
                .record_source(
                    "vd@hyperliquid@mmt",
                    candle.clone(),
                    candle.get("t").and_then(serde_json::Value::as_u64),
                )
                .expect("record vd history");
        }
        let execution = session
            .run_window(json!({
                "mode": "window",
                "source_configs": {
                    "vd@hyperliquid@mmt": {
                        "type": "vd",
                        "exchange": "hyperliquid",
                        "bucket": 7,
                        "timeframe_sec": 60
                    }
                }
            }))
            .expect("run script");

        assert_eq!(execution.output.metrics["bucket"], 7);
        assert_eq!(execution.output.metrics["latest"], 25.0);
        assert_eq!(execution.output.metrics["previous"], 30.0);
        assert_eq!(execution.output.metrics["delta"], 25.0);
        assert_eq!(execution.output.metrics["points"], 3);
        assert_eq!(execution.output.metrics["single_delta"], -5.0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn cvd_helper_rejects_candle_rows() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "bad-cvd-helper",
  version: "1",
  sources: ["candles"],
  params: {}
};

export function onData(ctx, input, history) {
  return {
    metrics: ctx.study.cvd(history.source("candles@binancef@mmt"), { bucket: 1 })
  };
}
"#,
            "bad-cvd-helper",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let err = session
            .run_candles_window(&json!([
                { "t": 1, "o": 1.0, "h": 1.0, "l": 1.0, "c": 10.0, "vb": 100.0, "vs": 80.0, "tb": 1, "ts": 1 }
            ]))
            .expect_err("cvd must reject non-vd candles");

        assert!(err.to_string().contains("MMT VD candle"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn study_helpers_validate_inputs() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "bad-helper-script",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  return { metrics: ctx.study.sma(history.source("candles@binancef@mmt"), { field: "missing", window: 2 }) };
}
"#,
            "bad-helper",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let err = session
            .run_candles_window(&json!([{ "c": 1.0 }]))
            .expect_err("helper should reject missing field");
        let message = err.to_string();

        assert!(message.contains("onData failed"));
        assert!(message.contains("missing"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn exposes_orderbook_study_helpers_to_js() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "orderbook-helper-script",
  version: "1",
  sources: ["orderbook"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  const book = history.source("orderbook@binancef@mmt", 0);
  const spread = ctx.study.spread(book);
  const depth = ctx.study.depth(book, { levels: 2 });
  const imbalance = ctx.study.imbalance(book, { depth: 2 });
  const slippage = ctx.study.slippage(book, { side: "buy", notional: 150 });
  const vamp = ctx.study.vamp(book, { dollar_depth: 150 });
  return {
    metrics: {
      mode: input.mode,
      spread_bps: spread.spread_bps,
      bid_quote: depth.bid_quote,
      ask_quote: depth.ask_quote,
      imbalance: imbalance.imbalance,
      slippage_levels: slippage.levels_consumed,
      slippage_bps: slippage.slippage_bps,
      vamp: vamp.vamp,
      vamp_complete: vamp.complete
    }
  };
}
"#,
            "orderbook-helpers",
        );

        let book = json!({
            "exchange": "test",
            "symbol": "BTC/USDT",
            "timestamp_ms": 1,
            "bids": [
                { "price": 99.0, "quantity": 1.0 },
                { "price": 98.0, "quantity": 2.0 }
            ],
            "asks": [
                { "price": 101.0, "quantity": 1.0 },
                { "price": 102.0, "quantity": 2.0 }
            ]
        });
        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let execution = session
            .run_orderbook_window(&json!([book]))
            .expect("run script");

        assert_eq!(execution.output.metrics["mode"], "window");
        assert_eq!(execution.output.metrics["bid_quote"], 295.0);
        assert_eq!(execution.output.metrics["ask_quote"], 305.0);
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
    fn runs_orderbook_window_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "orderbook-window-script",
  version: "1",
  sources: ["orderbook"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  const books = history.source("orderbook@binancef@mmt");
  const latest = history.source("orderbook@binancef@mmt", 0);
  const spread = ctx.study.spread(latest);
  return {
    metrics: {
      mode: input.mode,
      books: books.length,
      latest_ts: latest.timestamp_ms,
      spread_bps: spread.spread_bps
    }
  };
}
"#,
            "orderbook-window",
        );

        let books = json!([
            {
                "exchange": "test",
                "symbol": "BTC/USDT",
                "timestamp_ms": 1,
                "bids": [{ "price": 99.0, "quantity": 1.0 }],
                "asks": [{ "price": 101.0, "quantity": 1.0 }]
            },
            {
                "exchange": "test",
                "symbol": "BTC/USDT",
                "timestamp_ms": 2,
                "bids": [{ "price": 100.0, "quantity": 1.0 }],
                "asks": [{ "price": 102.0, "quantity": 1.0 }]
            }
        ]);
        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let execution = session.run_orderbook_window(&books).expect("run script");

        assert_eq!(execution.output.metrics["mode"], "window");
        assert_eq!(execution.output.metrics["books"], 2);
        assert_eq!(execution.output.metrics["latest_ts"], 2);
        assert!(
            execution.output.metrics["spread_bps"]
                .as_f64()
                .is_some_and(|value| value > 0.0)
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn runs_vd_window_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "vd-window-script",
  version: "1",
  sources: ["vd"],
  params: {}
};

export function onData(ctx, input, history) {
  const candles = history.source("vd@hyperliquid@mmt");
  const latest = history.source("vd@hyperliquid@mmt", 0);
  return {
    metrics: {
      mode: input.mode,
      candles: candles.length,
      latest_close: latest.c,
      trades: candles.reduce((sum, candle) => sum + candle.n, 0)
    }
  };
}
"#,
            "vd-window",
        );

        let candles = json!([
            { "t": 1, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "n": 10 },
            { "t": 2, "o": 1.5, "h": 2.2, "l": 1.0, "c": 2.0, "n": 12 }
        ]);
        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        for candle in candles.as_array().unwrap() {
            session
                .record_source(
                    "vd@hyperliquid@mmt",
                    candle.clone(),
                    candle.get("t").and_then(serde_json::Value::as_u64),
                )
                .expect("record vd history");
        }
        let execution = session
            .run_window(json!({ "mode": "window" }))
            .expect("run script");

        assert_eq!(execution.output.metrics["mode"], "window");
        assert_eq!(execution.output.metrics["candles"], 2);
        assert_eq!(execution.output.metrics["latest_close"], 2.0);
        assert_eq!(execution.output.metrics["trades"], 22);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn session_preserves_js_state_between_hook_calls() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "stateful-script",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

let calls = 0;

export function onData(ctx, input, history) {
  calls += 1;
  return {
    metrics: {
      calls,
      candles: history.source("candles@binancef@mmt").length
    }
  };
}
"#,
            "stateful",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let first = session
            .run_candles_window(&json!([{ "c": 1.0 }]))
            .expect("first run");
        let second = session
            .run_candles_window(&json!([{ "c": 2.0 }]))
            .expect("second run");

        assert_eq!(first.output.metrics["calls"], 1);
        assert_eq!(second.output.metrics["calls"], 2);
        assert_eq!(second.output.metrics["candles"], 2);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn runs_candle_stream_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "stream-script",
  version: "1",
  sources: ["candles"],
  modes: ["stream"],
  params: {}
};

let calls = 0;

export function onData(ctx, input, history) {
  calls += 1;
  return {
    metrics: {
      calls,
      close: history.source("candles@binancef@mmt", 0).c,
      source_data_removed: input.data === undefined && input.candles === undefined
    }
  };
}
"#,
            "stream",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let first = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": {
                    "candle": { "t": 1, "o": 1.0, "h": 1.0, "l": 1.0, "c": 10.0, "vb": 1.0, "vs": 1.0, "tb": 1, "ts": 1 }
                }
            }))
            .expect("first stream run");
        let second = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": {
                    "candle": { "t": 2, "o": 1.0, "h": 1.0, "l": 1.0, "c": 11.0, "vb": 1.0, "vs": 1.0, "tb": 1, "ts": 1 }
                }
            }))
            .expect("second stream run");

        assert_eq!(first.output.metrics["calls"], 1);
        assert_eq!(first.output.metrics["close"], 10.0);
        assert_eq!(second.output.metrics["calls"], 2);
        assert_eq!(second.output.metrics["close"], 11.0);
        assert_eq!(second.output.metrics["source_data_removed"], true);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn stream_history_exposes_bounded_current_and_previous_records() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "stream-history",
  version: "1",
  sources: ["candles"],
  modes: ["stream"],
  lookback: 2,
  params: {}
};

export function onData(ctx, input, history) {
  const current = history.source("candles@binancef@mmt", 0);
  const previous = history.source("candles@binancef@mmt", 1);
  const candles = history.source("candles@binancef@mmt");
  return {
    metrics: {
      current: current.c,
      previous: previous?.c ?? null,
      out_of_range: history.source("candles@binancef@mmt", 2) === undefined,
      list_length: candles.length,
      first: candles[0].c,
      latest: candles[candles.length - 1].c,
      missing_list: history.source("candles@missing@mmt").length,
      frozen: Object.isFrozen(current) && Object.isFrozen(candles) && Object.isFrozen(candles[0])
    }
  };
}
"#,
            "stream-history",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let first = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 10.0 } }
            }))
            .expect("first stream run");
        let second = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 2, "c": 11.0 } }
            }))
            .expect("second stream run");
        let third = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 3, "c": 12.0 } }
            }))
            .expect("third stream run");

        assert_eq!(first.output.metrics["current"], 10.0);
        assert!(first.output.metrics["previous"].is_null());
        assert_eq!(second.output.metrics["current"], 11.0);
        assert_eq!(second.output.metrics["previous"], 10.0);
        assert_eq!(third.output.metrics["current"], 12.0);
        assert_eq!(third.output.metrics["previous"], 11.0);
        assert_eq!(third.output.metrics["out_of_range"], true);
        assert_eq!(third.output.metrics["list_length"], 2);
        assert_eq!(third.output.metrics["first"], 11.0);
        assert_eq!(third.output.metrics["latest"], 12.0);
        assert_eq!(third.output.metrics["missing_list"], 0);
        assert_eq!(third.output.metrics["frozen"], true);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn stream_history_replaces_the_current_bar_with_the_same_timestamp() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "stream-history-bar-replacement",
  version: "1",
  sources: ["candles"],
  modes: ["stream"],
  params: {}
};

export function onData(ctx, input, history) {
  return {
    metrics: {
      current: history.source("candles@binancef@mmt", 0).c,
      previous: history.source("candles@binancef@mmt", 1)?.c ?? null
    }
  };
}
"#,
            "stream-history-bar-replacement",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 10.0 } }
            }))
            .expect("first stream run");
        let replacement = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 10.5 } }
            }))
            .expect("replacement stream run");
        let next = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 2, "c": 11.0 } }
            }))
            .expect("next stream run");

        assert_eq!(replacement.output.metrics["current"], 10.5);
        assert!(replacement.output.metrics["previous"].is_null());
        assert_eq!(next.output.metrics["current"], 11.0);
        assert_eq!(next.output.metrics["previous"], 10.5);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn stream_history_is_independent_for_each_source() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "stream-history-sources",
  version: "1",
  sources: ["candles", "orderbook"],
  modes: ["stream"],
  params: {}
};

export function onData(ctx, input, history) {
  return {
    metrics: {
      candle: history.source("candles@binancef@mmt", 0)?.c ?? null,
      previous_candle: history.source("candles@binancef@mmt", 1)?.c ?? null,
      book_ts: history.source("orderbook@bulk", 0)?.timestamp_ms ?? null
    }
  };
}
"#,
            "stream-history-sources",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 10.0 } }
            }))
            .expect("candle stream run");
        let book = session
            .run_stream(json!({
                "mode": "stream",
                "source": "orderbook@bulk",
                "source_type": "orderbook",
                "data": {
                    "snapshot": { "timestamp_ms": 2, "bids": [], "asks": [] }
                }
            }))
            .expect("orderbook stream run");
        let candle = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 3, "c": 11.0 } }
            }))
            .expect("second candle stream run");

        assert_eq!(book.output.metrics["candle"], 10.0);
        assert_eq!(book.output.metrics["book_ts"], 2);
        assert_eq!(candle.output.metrics["candle"], 11.0);
        assert_eq!(candle.output.metrics["previous_candle"], 10.0);
        assert_eq!(candle.output.metrics["book_ts"], 2);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn stream_history_is_independent_for_exchange_qualified_sources() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "stream-history-exchanges",
  version: "1",
  sources: ["candles"],
  modes: ["stream"],
  params: {}
};

export function onData(ctx, input, history) {
  return {
    metrics: {
      binance: history.source("candles@binancef@mmt", 0)?.c ?? null,
      previous_binance: history.source("candles@binancef@mmt", 1)?.c ?? null,
      okx: history.source("candles@okx@mmt", 0)?.c ?? null
    }
  };
}
"#,
            "stream-history-exchanges",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 10.0 } }
            }))
            .expect("first binance stream run");
        let okx = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@okx@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 20.0 } }
            }))
            .expect("okx stream run");
        let binance = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 2, "c": 11.0 } }
            }))
            .expect("second binance stream run");

        assert_eq!(okx.output.metrics["binance"], 10.0);
        assert_eq!(okx.output.metrics["okx"], 20.0);
        assert_eq!(binance.output.metrics["binance"], 11.0);
        assert_eq!(binance.output.metrics["previous_binance"], 10.0);
        assert_eq!(binance.output.metrics["okx"], 20.0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reports_js_exception_message_from_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "bad-script",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  return { metrics: { count: input.cande.length } };
}
"#,
            "bad",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        let err = session
            .run_candles_window(&json!([{ "c": 1.0 }]))
            .expect_err("script should fail");
        let message = err.to_string();

        assert!(message.contains("onData failed"));
        assert!(message.contains("cande") || message.contains("undefined"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn session_exposes_cancellation_state() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "cancel-script",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  return { metrics: { candles: history.source("candles@binancef@mmt").length } };
}
"#,
            "cancel",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");
        assert!(!session.is_cancelled());
        session
            .cancel_handle()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(session.is_cancelled());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn requires_on_data_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "missing-hook",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};
"#,
            "missing-hook",
        );

        let script = Script::load(&path).expect("load script");
        let err = match script.start_session(&json!({})) {
            Ok(_) => panic!("missing onData should fail"),
            Err(err) => err,
        };
        let message = err.to_string();

        assert!(message.contains("onData(ctx, input, history)"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn queues_trade_and_cancel_without_requiring_script_returns() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "execution-script",
  version: "1",
  sources: ["candles"],
  modes: ["stream"],
  params: {}
};

export function onData(ctx, input, history) {
  ctx.trade({
    key: "entry-1",
    side: "long",
    notional: 100,
    leverage: 5,
    order: { type: "limit", price: 64000, tif: "gtc" },
    sl: 63000,
    tp: 67000
  });
}

export function onExecution(ctx, event) {
  if (event.type === "order.accepted") {
    ctx.cancel({ key: "cancel-entry-1", order: event.orderId });
  }
}
"#,
            "execution",
        );

        let script = Script::load(&path).expect("load script");
        let session = script
            .start_session_with_execution(
                &json!({}),
                crate::scripting::execution::ScriptExecutionContext {
                    job_id: "job_test".to_string(),
                    enabled: true,
                },
            )
            .expect("start execution session");
        let execution = session
            .run_stream(json!({
                "mode": "stream",
                "source": "candles@binancef@mmt",
                "source_type": "candles",
                "data": { "candle": { "t": 1, "c": 1.0 } }
            }))
            .expect("run onData");
        assert!(execution.output.metrics.as_object().unwrap().is_empty());
        assert_eq!(execution.commands.len(), 1);
        let order_id = match &execution.commands[0] {
            crate::scripting::execution::ScriptExecutionCommand::Trade { order, request } => {
                assert_eq!(request.sl, Some(63_000.0));
                assert_eq!(request.tp, Some(67_000.0));
                order.id.clone()
            }
            _ => panic!("expected trade command"),
        };

        let execution = session
            .run_execution_event(json!({
                "type": "order.accepted",
                "orderId": order_id
            }))
            .expect("run onExecution")
            .expect("onExecution exists");
        assert!(matches!(
            execution.commands.as_slice(),
            [crate::scripting::execution::ScriptExecutionCommand::Cancel { .. }]
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    #[ignore = "local benchmark; run with `cargo test bench_candle_window_payload_sizes -- --ignored --nocapture`"]
    fn bench_candle_window_payload_sizes() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "payload-bench",
  version: "1",
  sources: ["candles"],
  modes: ["window"],
  params: {}
};

export function onData(ctx, input, history) {
  const candles = history.source("candles@binancef@mmt");
  let total = 0;
  for (const candle of candles) {
    total += candle.c;
  }
  return {
    metrics: {
      candles: candles.length,
      avg_close: total / candles.length
    }
  };
}
"#,
            "payload-bench",
        );

        let script = Script::load(&path).expect("load script");
        let session = script.start_session(&json!({})).expect("start session");

        for size in [1_000_usize, 5_000] {
            let candles = synthetic_candles(size);
            let started = Instant::now();
            let execution = session.run_candles_window(&candles).expect("run hook");
            let elapsed = started.elapsed();

            println!(
                "size={} elapsed_ms={} hook_ms={} heap={:?}",
                size,
                elapsed.as_millis(),
                execution.stats.duration_ms,
                execution.stats.heap_used_bytes
            );
            assert_eq!(execution.output.metrics["candles"], size);
        }

        let _ = fs::remove_file(path);
    }
}
