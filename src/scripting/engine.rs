use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context as AnyhowContext, Result};
use rquickjs::{CatchResultExt, Context, Ctx, Function, Module, Object, Promise, Runtime, Value};
use serde_json::Value as JsonValue;

use super::limits::default_limits;
use super::manifest::ScriptManifest;
use super::output::ScriptOutput;
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

    pub fn run_candles_window(
        &self,
        inputs: &JsonValue,
        candles: &JsonValue,
    ) -> Result<ScriptExecution> {
        let limits = default_limits();
        let rt = Runtime::new().context("failed to create QuickJS runtime")?;
        rt.set_memory_limit(limits.heap_bytes);
        rt.set_max_stack_size(limits.stack_bytes);
        let hook_started = Instant::now();
        rt.set_interrupt_handler(Some(Box::new(move || {
            hook_started.elapsed().as_millis() as u64 > limits.hook_timeout_ms
        })));
        let ctx = Context::full(&rt).context("failed to create QuickJS context")?;

        let started = Instant::now();
        let output = ctx.with(|ctx| -> Result<ScriptOutput> {
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
            let inputs_val = json_to_js(ctx.clone(), inputs)?;
            script_ctx
                .set("inputs", inputs_val)
                .context("failed to assign ctx.inputs")?;

            let input_payload = serde_json::json!({
                "mode": "window",
                "candles": candles,
            });
            let input_val = json_to_js(ctx.clone(), &input_payload)?;
            let result: Value = hook
                .call((script_ctx, input_val))
                .catch(&ctx)
                .map_err(|err| anyhow::anyhow!("onData failed: {}", err))?;
            let result_json = js_to_json(ctx.clone(), result)?;
            ScriptOutput::from_json(result_json)
        })?;

        let memory_usage = rt.memory_usage();
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
            .or_else(|_| namespace.get("study"))
            .context("script has no `script` export")?;
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

    use serde_json::json;

    use super::Script;

    fn write_temp_script(contents: &str, stem: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("mlab-script-{}-{}.js", stem, std::process::id()));
        fs::write(&path, contents).expect("write temp script");
        path
    }

    #[test]
    fn loads_manifest_from_js_module() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "buy-pressure-filter",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {
    min_vbuy: { type: "number", required: true }
  }
};

export function onData(ctx, input) {
  return { metrics: { count: input.candles.length, threshold: ctx.inputs.min_vbuy } };
}
"#,
            "manifest",
        );

        let script = Script::load(&path).expect("load script");
        assert_eq!(script.manifest.name, "buy-pressure-filter");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn runs_candles_window_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "buy-pressure-filter",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {
    min_vbuy: { type: "number", required: true }
  }
};

export function onData(ctx, input) {
  const filtered = input.candles.filter((c) => c.vb >= ctx.inputs.min_vbuy);
  return {
    metrics: {
      qualifying_candles: filtered.length,
      latest_close: input.candles[input.candles.length - 1].c
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
        let execution = script
            .run_candles_window(&inputs, &candles)
            .expect("run script");

        assert_eq!(execution.output.metrics["qualifying_candles"], 1);
        assert_eq!(execution.output.metrics["latest_close"], 2.0);
        assert!(execution.stats.heap_used_bytes.is_some());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reports_js_exception_message_from_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "bad-script",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {}
};

export function onData(ctx, input) {
  return { metrics: { count: input.cande.length } };
}
"#,
            "bad",
        );

        let script = Script::load(&path).expect("load script");
        let err = script
            .run_candles_window(&json!({}), &json!([{ "c": 1.0 }]))
            .expect_err("script should fail");
        let message = err.to_string();

        assert!(message.contains("onData failed"));
        assert!(message.contains("cande") || message.contains("undefined"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn requires_on_data_hook() {
        let path = write_temp_script(
            r#"
export const script = {
  name: "missing-hook",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {}
};
"#,
            "missing-hook",
        );

        let script = Script::load(&path).expect("load script");
        let err = script
            .run_candles_window(&json!({}), &json!([{ "c": 1.0 }]))
            .expect_err("missing onData should fail");
        let message = err.to_string();

        assert!(message.contains("onData(ctx, input)"));
        let _ = fs::remove_file(path);
    }
}
