use anyhow::{Result, bail};

use crate::cli::{OutputFormat, ScriptRunArgs};
use crate::commands::script::{report_builder, write_report_best_effort};
use crate::domain::enums::ProviderKind;
use crate::scripting::engine::Script;
use crate::scripting::inputs::{
    parse_param_values, parse_source_configs, resolve_params, validate_source_configs,
};
use crate::scripting::manifest::ScriptMode;

pub async fn handle(args: ScriptRunArgs) -> Result<()> {
    args.validate()?;
    if !matches!(args.provider.into(), ProviderKind::Mmt) {
        bail!("scripts currently support only --provider mmt");
    }
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("scripts currently support only --output terminal|json|jsonl");
    }

    let script = Script::load(&args.script)?;
    let report = report_builder(
        "script.run",
        &script,
        Some("mmt".to_string()),
        args.exchange.clone(),
        args.symbol.clone(),
    );
    let result = validate_run(args, &script);
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(err) => report.finish_error(err),
    };
    write_report_best_effort(&runtime_report);
    result
}

fn validate_run(args: ScriptRunArgs, script: &Script) -> Result<()> {
    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical data"
        );
    }
    if !args.stream {
        bail!("script run requires --stream");
    }
    if !script.manifest.supports_mode(ScriptMode::Stream) {
        bail!("script does not support stream mode");
    }

    require_non_empty(args.exchange.as_deref(), "--exchange")?;
    require_symbol(args.symbol.as_deref())?;

    let source_configs = parse_source_configs(&args.source)?;
    validate_source_configs(&script.manifest, &source_configs)?;
    let raw_params = parse_param_values(&args.param)?;
    let _resolved_params = resolve_params(&script.manifest, &raw_params)?;

    bail!("script stream execution is not implemented yet")
}

fn require_non_empty<'a>(value: Option<&'a str>, flag: &str) -> Result<&'a str> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{flag} is required"))?;
    if value.trim().is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(value)
}

fn require_symbol(value: Option<&str>) -> Result<&str> {
    let symbol = require_non_empty(value, "--symbol")?;
    if !symbol.contains('/') {
        bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
    }
    Ok(symbol)
}
