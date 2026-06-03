use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context as AnyhowContext, Result};
use rquickjs::{CatchResultExt, Context, Ctx, Function, Module, Object, Promise, Runtime, Value};
use serde_json::Value as JsonValue;

use super::limits::default_limits;
use super::manifest::ScriptManifest;
use super::output::ScriptOutput;
use super::studies::attach_study_helpers;
use super::telemetry::ScriptHookStats;

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

    pub fn start_session(&self, params: &JsonValue) -> Result<ScriptSession> {
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
                .context("scripts require export `onData(ctx, input)`")?;

            let script_ctx = Object::new(ctx.clone()).context("failed to create script ctx")?;
            let params_val = json_to_js(ctx.clone(), params)?;
            script_ctx
                .set("params", params_val)
                .context("failed to assign ctx.params")?;
            attach_study_helpers(ctx.clone(), &script_ctx)?;

            let globals = ctx.globals();
            globals
                .set("__mlab_onData", hook)
                .context("failed to store onData hook")?;
            globals
                .set("__mlab_ctx", script_ctx)
                .context("failed to store script ctx")?;
            Ok(())
        })?;

        Ok(ScriptSession {
            rt,
            ctx,
            hook_started,
            cancelled,
        })
    }
}

pub struct ScriptSession {
    ctx: Context,
    rt: Runtime,
    hook_started: Arc<Mutex<Instant>>,
    cancelled: Arc<AtomicBool>,
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
        let input_payload = serde_json::json!({
            "mode": "window",
            "candles": {
                "candles": candles,
            },
        });
        self.run_with_input(input_payload)
    }

    #[cfg(test)]
    pub fn run_orderbook_window(&self, books: &JsonValue) -> Result<ScriptExecution> {
        let input_payload = serde_json::json!({
            "mode": "window",
            "orderbook": {
                "books": books,
            },
        });
        self.run_with_input(input_payload)
    }

    pub fn run_window(&self, payload: JsonValue) -> Result<ScriptExecution> {
        self.run_with_input(payload)
    }

    fn run_with_input(&self, input_payload: JsonValue) -> Result<ScriptExecution> {
        let started = Instant::now();
        {
            let mut hook_started = self
                .hook_started
                .lock()
                .map_err(|_| anyhow::anyhow!("script runtime timer lock poisoned"))?;
            *hook_started = started;
        }

        let output = self.ctx.with(|ctx| -> Result<ScriptOutput> {
            let globals = ctx.globals();
            let hook: Function = globals
                .get("__mlab_onData")
                .context("scripts require export `onData(ctx, input)`")?;
            let script_ctx: Object = globals
                .get("__mlab_ctx")
                .context("failed to read script ctx")?;
            let input_val = json_to_js(ctx.clone(), &input_payload)?;
            let result: Value = hook
                .call((script_ctx, input_val))
                .catch(&ctx)
                .map_err(|err| anyhow::anyhow!("onData failed: {}", err))?;
            let result_json = js_to_json(ctx.clone(), result)?;
            ScriptOutput::from_json(result_json)
        })?;

        let memory_usage = self.rt.memory_usage();
        Ok(ScriptExecution {
            output,
            stats: ScriptHookStats {
                duration_ms: started.elapsed().as_millis() as u64,
                heap_used_bytes: u64::try_from(memory_usage.memory_used_size).ok(),
            },
        })
    }
}

#[derive(Debug)]
pub struct ScriptExecution {
    pub output: ScriptOutput,
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
    candles: {
      min_vbuy: { type: "number", required: true }
    }
  }
};

export function onData(ctx, input) {
  return { metrics: { count: input.candles.candles.length, threshold: ctx.params.candles.min_vbuy } };
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

export function onData(ctx, input) {
  return { metrics: { candles: input.candles.candles.length } };
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
    candles: {
      min_vbuy: { type: "number", required: true }
    }
  }
};

export function onData(ctx, input) {
  const filtered = input.candles.candles.filter((c) => c.vb >= ctx.params.candles.min_vbuy);
  return {
    metrics: {
      qualifying_candles: filtered.length,
      latest_close: input.candles.candles[input.candles.candles.length - 1].c
    }
  };
}
"#,
            "run",
        );

        let script = Script::load(&path).expect("load script");
        let inputs = json!({ "candles": { "min_vbuy": 150.0 } });
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

export function onData(ctx, input) {
  const sma = ctx.study.sma(input.candles.candles, { field: "c", window: 3 });
  const ema = ctx.study.ema(input.candles.candles, { field: "c", window: 3 });
  const cvd = ctx.study.cvd(input.candles.candles);
  return {
    metrics: {
      sma_latest: sma.latest,
      sma_previous: sma.previous,
      ema_latest: ema.latest,
      cvd_latest: cvd.latest,
      cvd_delta: cvd.delta,
      cvd_points: cvd.points.length
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
        assert_eq!(execution.output.metrics["cvd_latest"], 120.0);
        assert_eq!(execution.output.metrics["cvd_delta"], 120.0);
        assert_eq!(execution.output.metrics["cvd_points"], 4);
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

export function onData(ctx, input) {
  return { metrics: ctx.study.sma(input.candles.candles, { field: "missing", window: 2 }) };
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

export function onData(ctx, input) {
  const book = input.orderbook.books[input.orderbook.books.length - 1];
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

export function onData(ctx, input) {
  const latest = input.orderbook.books[input.orderbook.books.length - 1];
  const spread = ctx.study.spread(latest);
  return {
    metrics: {
      mode: input.mode,
      books: input.orderbook.books.length,
      latest_ts: latest.timestamp_ms,
      spread_bps: spread.spread_bps
    },
    signal: {
      triggered: true,
      side: "buy"
    },
    intent: {
      side: "buy"
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
        assert_eq!(execution.output.signal["side"], "buy");
        assert!(
            execution.output.metrics["spread_bps"]
                .as_f64()
                .is_some_and(|value| value > 0.0)
        );
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

export function onData(ctx, input) {
  calls += 1;
  return {
    metrics: {
      calls,
      candles: input.candles.candles.length
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
            .run_candles_window(&json!([{ "c": 1.0 }, { "c": 2.0 }]))
            .expect("second run");

        assert_eq!(first.output.metrics["calls"], 1);
        assert_eq!(second.output.metrics["calls"], 2);
        assert_eq!(second.output.metrics["candles"], 2);
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

export function onData(ctx, input) {
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

export function onData(ctx, input) {
  return { metrics: { candles: input.candles.candles.length } };
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

        assert!(message.contains("onData(ctx, input)"));
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

export function onData(ctx, input) {
  let total = 0;
  for (const candle of input.candles.candles) {
    total += candle.c;
  }
  return {
    metrics: {
      candles: input.candles.candles.length,
      avg_close: total / input.candles.candles.length
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
