use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as AnyhowContext, Result, bail};
use rquickjs::{CatchResultExt, Context, Ctx, Function, Module, Object, Promise, Runtime, Value};
use serde_json::Value as JsonValue;

use super::manifest::{StudyManifest, StudyMode};

pub struct StudyScript {
    pub path: PathBuf,
    pub manifest: StudyManifest,
    source: String,
}

impl StudyScript {
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
    ) -> Result<super::study::ScriptStudyOutput> {
        let rt = Runtime::new().context("failed to create QuickJS runtime")?;
        let ctx = Context::full(&rt).context("failed to create QuickJS context")?;

        ctx.with(|ctx| -> Result<super::study::ScriptStudyOutput> {
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
                .get("onCandle")
                .context("study source=candles requires export `onCandle`")?;

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
                .map_err(|err| anyhow::anyhow!("onCandle failed: {}", err))?;
            let result_json = js_to_json(ctx.clone(), result)?;
            super::study::ScriptStudyOutput::from_json(result_json)
        })
    }
}

fn inspect_manifest(path: &Path, source: &str) -> Result<StudyManifest> {
    let rt = Runtime::new().context("failed to create QuickJS runtime")?;
    let ctx = Context::full(&rt).context("failed to create QuickJS context")?;

    ctx.with(|ctx| -> Result<StudyManifest> {
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
        let study_value: Value = namespace
            .get("study")
            .context("script has no `study` export")?;
        let study_json = js_to_json(ctx.clone(), study_value)?;
        let manifest: StudyManifest =
            serde_json::from_value(study_json).context("failed to decode `study` manifest")?;
        manifest.validate()?;
        if !manifest.supports_mode(StudyMode::Window) {
            bail!("phase 1 requires study.modes to include \"window\"");
        }
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
        .unwrap_or("study.js")
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::StudyScript;

    fn write_temp_script(contents: &str, stem: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("mlab-study-{}-{}.js", stem, std::process::id()));
        fs::write(&path, contents).expect("write temp script");
        path
    }

    #[test]
    fn loads_manifest_from_js_module() {
        let path = write_temp_script(
            r#"
export const study = {
  name: "buy-pressure-filter",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {
    min_vbuy: { type: "number", required: true }
  }
};

export function onCandle(ctx, input) {
  return { metrics: { count: input.candles.length, threshold: ctx.inputs.min_vbuy } };
}
"#,
            "manifest",
        );

        let script = StudyScript::load(&path).expect("load script");
        assert_eq!(script.manifest.name, "buy-pressure-filter");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn runs_candles_window_hook() {
        let path = write_temp_script(
            r#"
export const study = {
  name: "buy-pressure-filter",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {
    min_vbuy: { type: "number", required: true }
  }
};

export function onCandle(ctx, input) {
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

        let script = StudyScript::load(&path).expect("load script");
        let inputs = json!({ "min_vbuy": 150.0 });
        let candles = json!([
            { "t": 1, "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "vb": 100.0, "vs": 80.0, "tb": 10, "ts": 9 },
            { "t": 2, "o": 1.5, "h": 2.2, "l": 1.0, "c": 2.0, "vb": 200.0, "vs": 90.0, "tb": 12, "ts": 10 }
        ]);
        let out = script
            .run_candles_window(&inputs, &candles)
            .expect("run script");

        assert_eq!(out.metrics["qualifying_candles"], 1);
        assert_eq!(out.metrics["latest_close"], 2.0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn reports_js_exception_message_from_hook() {
        let path = write_temp_script(
            r#"
export const study = {
  name: "bad-study",
  version: "1",
  source: "candles",
  modes: ["window"],
  inputs: {}
};

export function onCandle(ctx, input) {
  return { metrics: { count: input.cande.length } };
}
"#,
            "bad",
        );

        let script = StudyScript::load(&path).expect("load script");
        let err = script
            .run_candles_window(&json!({}), &json!([{ "c": 1.0 }]))
            .expect_err("script should fail");
        let message = err.to_string();

        assert!(message.contains("onCandle failed"));
        assert!(message.contains("cande") || message.contains("undefined"));
        let _ = fs::remove_file(path);
    }
}
