use anyhow::{Result, bail};

use crate::cli::{OutputFormat, ScriptRunArgs};
use crate::commands::script::{report_builder, write_report_best_effort};
use crate::domain::enums::ProviderKind;
use crate::scripting::engine::Script;
use crate::scripting::inputs::{parse_kv_inputs, resolve_inputs};
use crate::scripting::manifest::{ScriptMode, ScriptSource};

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
    let raw_inputs = match parse_kv_inputs(&args.input) {
        Ok(raw_inputs) => raw_inputs,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };
    let _resolved_inputs = match resolve_inputs(&script.manifest, &raw_inputs) {
        Ok(resolved_inputs) => resolved_inputs,
        Err(err) => {
            let runtime_report = report.finish_error(&err);
            write_report_best_effort(&runtime_report);
            return Err(err);
        }
    };

    let result = match script.manifest.source {
        ScriptSource::Candles => validate_candles_run(&args, &script),
        ScriptSource::Orderbook => validate_orderbook_run(&args, &script),
    };
    let runtime_report = match &result {
        Ok(_) => report.finish_ok(),
        Err(err) => report.finish_error(err),
    };
    write_report_best_effort(&runtime_report);
    result
}

fn validate_orderbook_run(args: &ScriptRunArgs, script: &Script) -> Result<()> {
    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical orderbook data"
        );
    }
    if args.timeframe.is_some() {
        bail!("--timeframe is not used with source=orderbook stream");
    }
    if !args.stream {
        bail!("script run for source=orderbook requires --stream");
    }
    if !script.manifest.supports_mode(ScriptMode::Stream) {
        bail!("script does not support stream mode");
    }

    require_non_empty(args.exchange.as_deref(), "--exchange")?;
    require_symbol(args.symbol.as_deref())?;

    bail!("script stream execution is not implemented yet")
}

fn validate_candles_run(args: &ScriptRunArgs, script: &Script) -> Result<()> {
    if args.from.is_some() || args.to.is_some() {
        bail!(
            "--from/--to are not allowed with script run; use script backtest for historical candles"
        );
    }
    if !args.stream {
        bail!(
            "script run for source=candles requires --stream; use script backtest for historical candles"
        );
    }
    if !script.manifest.supports_mode(ScriptMode::Stream) {
        bail!("script does not support stream mode");
    }

    require_non_empty(args.exchange.as_deref(), "--exchange")?;
    require_symbol(args.symbol.as_deref())?;
    args.mmt_tf()?;

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
