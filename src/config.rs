use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const PROJECT_CONFIG: &str = "marketlab.toml";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketLabConfig {
    version: u32,
    market: Option<MarketConfig>,
    output: Option<OutputConfig>,
    sources: Option<BTreeMap<String, BTreeMap<String, toml::Value>>>,
    script: Option<ScriptConfig>,
    backtest: Option<BacktestConfig>,
    execution: Option<ExecutionConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketConfig {
    provider: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputConfig {
    format: Option<String>,
    verbose: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptConfig {
    path: Option<PathBuf>,
    params: Option<BTreeMap<String, BTreeMap<String, toml::Value>>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BacktestConfig {
    from: Option<u64>,
    to: Option<u64>,
    leverage: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionConfig {
    venue: Option<String>,
    size: Option<f64>,
    notional: Option<f64>,
    order_type: Option<String>,
    price: Option<f64>,
    tif: Option<String>,
    leverage: Option<f64>,
    reduce_only: Option<bool>,
    dry_run: Option<bool>,
    yes: Option<bool>,
}

pub fn expand_args(args: impl IntoIterator<Item = OsString>) -> Result<Vec<OsString>> {
    let args = args.into_iter().collect::<Vec<_>>();
    let explicit_config = explicit_config_path(&args)?;
    let command = config_command(&args);
    if command.is_none() && explicit_config.is_none() {
        return Ok(args);
    }
    let Some(command) = command else {
        bail!("--config supports `script run`, `script backtest`, and `trade long|short`");
    };

    let config_path = explicit_config.or_else(|| {
        let path = PathBuf::from(PROJECT_CONFIG);
        path.exists().then_some(path)
    });
    let Some(config_path) = config_path else {
        return Ok(args);
    };

    let config = load_config(&config_path)?;

    match command {
        ConfigCommand::Script {
            script_idx,
            mode_idx,
            mode,
        } => {
            let mut expanded = args[..=mode_idx].to_vec();
            if !has_script_positional(&args, mode_idx + 1) {
                let path = config
                    .script
                    .as_ref()
                    .and_then(|script| script.path.as_ref())
                    .context("script path is required in the command or [script].path")?;
                expanded.push(resolve_config_path(&config_path, path).into_os_string());
            }
            append_script_config_flags(&mut expanded, &config, mode)?;
            expanded.extend_from_slice(&args[mode_idx + 1..]);
            debug_assert_eq!(args[script_idx].to_string_lossy(), "script");
            Ok(expanded)
        }
        ConfigCommand::Trade {
            trade_idx,
            direction_idx,
        } => {
            let mut expanded = args[..=direction_idx].to_vec();
            if !has_trade_positional(&args, direction_idx + 1) {
                let symbol = config
                    .market
                    .as_ref()
                    .and_then(|market| market.symbol.as_deref())
                    .context("trade symbol is required in the command or [market].symbol")?;
                expanded.push(symbol.into());
            }
            append_trade_config_flags(&mut expanded, &config)?;
            expanded.extend_from_slice(&args[direction_idx + 1..]);
            debug_assert_eq!(args[trade_idx].to_string_lossy(), "trade");
            Ok(expanded)
        }
    }
}

fn load_config(path: &Path) -> Result<MarketLabConfig> {
    let source = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let config: MarketLabConfig = toml::from_str(&source)
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    if config.version != 1 {
        bail!("unsupported config version {} (expected 1)", config.version);
    }
    Ok(config)
}

fn append_script_config_flags(
    args: &mut Vec<OsString>,
    config: &MarketLabConfig,
    mode: &str,
) -> Result<()> {
    if let Some(market) = &config.market {
        append_optional(args, "--provider", market.provider.as_deref());
        append_optional(args, "--exchange", market.exchange.as_deref());
        append_optional(args, "--symbol", market.symbol.as_deref());
    }

    if let Some(sources) = &config.sources {
        for (source, values) in sources {
            for (name, value) in values {
                append_pair(
                    args,
                    "--source",
                    &format!("{source}:{name}={}", scalar(value)?),
                );
            }
        }
    }

    if let Some(params) = config
        .script
        .as_ref()
        .and_then(|script| script.params.as_ref())
    {
        for (source, values) in params {
            for (name, value) in values {
                append_pair(
                    args,
                    "--param",
                    &format!("{source}:{name}={}", scalar(value)?),
                );
            }
        }
    }

    if mode == "backtest"
        && let Some(backtest) = &config.backtest
    {
        append_optional_owned(args, "--from", backtest.from.map(|value| value.to_string()));
        append_optional_owned(args, "--to", backtest.to.map(|value| value.to_string()));
        append_optional_owned(
            args,
            "--leverage",
            backtest.leverage.map(|value| value.to_string()),
        );
    }

    if let Some(output) = &config.output {
        append_optional(args, "--output", output.format.as_deref());
        if output.verbose.unwrap_or(false) {
            args.push("--verbose".into());
        }
    }

    Ok(())
}

fn append_trade_config_flags(args: &mut Vec<OsString>, config: &MarketLabConfig) -> Result<()> {
    if let Some(execution) = &config.execution {
        if execution.size.is_some() && execution.notional.is_some() {
            bail!("[execution] cannot set both size and notional");
        }
        append_optional(args, "--venue", execution.venue.as_deref());
        append_optional_owned(
            args,
            "--size",
            execution.size.map(|value| value.to_string()),
        );
        append_optional_owned(
            args,
            "--notional",
            execution.notional.map(|value| value.to_string()),
        );
        append_optional(args, "--type", execution.order_type.as_deref());
        append_optional_owned(
            args,
            "--price",
            execution.price.map(|value| value.to_string()),
        );
        append_optional(args, "--tif", execution.tif.as_deref());
        append_optional_owned(
            args,
            "--leverage",
            execution.leverage.map(|value| value.to_string()),
        );
        if execution.reduce_only.unwrap_or(false) {
            args.push("--reduce-only".into());
        }
        if execution.dry_run.unwrap_or(false) {
            args.push("--dry-run".into());
        }
        if execution.yes.unwrap_or(false) {
            args.push("--yes".into());
        }
    }
    if let Some(output) = &config.output {
        append_optional(args, "--output", output.format.as_deref());
    }
    Ok(())
}

fn explicit_config_path(args: &[OsString]) -> Result<Option<PathBuf>> {
    for (idx, arg) in args.iter().enumerate() {
        let arg = arg.to_string_lossy();
        if arg == "--config" {
            let value = args.get(idx + 1).context("--config requires a path")?;
            return Ok(Some(PathBuf::from(value)));
        }
        if let Some(value) = arg.strip_prefix("--config=") {
            return Ok(Some(PathBuf::from(value)));
        }
    }
    Ok(None)
}

#[derive(Clone, Copy)]
enum ConfigCommand<'a> {
    Script {
        script_idx: usize,
        mode_idx: usize,
        mode: &'a str,
    },
    Trade {
        trade_idx: usize,
        direction_idx: usize,
    },
}

fn config_command(args: &[OsString]) -> Option<ConfigCommand<'_>> {
    if let Some(script_idx) = args.iter().position(|arg| arg == "script") {
        let mode_idx = script_idx + 1;
        let mode = args.get(mode_idx)?.to_str()?;
        return matches!(mode, "run" | "backtest").then_some(ConfigCommand::Script {
            script_idx,
            mode_idx,
            mode,
        });
    }
    let trade_idx = args.iter().position(|arg| arg == "trade")?;
    let direction_idx = trade_idx + 1;
    let direction = args.get(direction_idx)?.to_str()?;
    matches!(direction, "long" | "short" | "buy" | "sell").then_some(ConfigCommand::Trade {
        trade_idx,
        direction_idx,
    })
}

fn has_script_positional(args: &[OsString], start: usize) -> bool {
    let value_flags = [
        "--config",
        "--provider",
        "--exchange",
        "--symbol",
        "--from",
        "--to",
        "--source",
        "--param",
        "--leverage",
        "--output",
    ];
    let mut skip_value = false;
    for arg in &args[start..] {
        let arg = arg.to_string_lossy();
        if skip_value {
            skip_value = false;
            continue;
        }
        if arg.starts_with("--") {
            if !arg.contains('=') && value_flags.contains(&arg.as_ref()) {
                skip_value = true;
            }
            continue;
        }
        return true;
    }
    false
}

fn has_trade_positional(args: &[OsString], start: usize) -> bool {
    let value_flags = [
        "--config",
        "--venue",
        "--size",
        "--notional",
        "--type",
        "--price",
        "--tif",
        "--leverage",
        "--output",
    ];
    let mut skip_value = false;
    for arg in &args[start..] {
        let arg = arg.to_string_lossy();
        if skip_value {
            skip_value = false;
            continue;
        }
        if arg.starts_with("--") {
            if !arg.contains('=') && value_flags.contains(&arg.as_ref()) {
                skip_value = true;
            }
            continue;
        }
        return true;
    }
    false
}

fn resolve_config_path(config_path: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(path)
    }
}

fn scalar(value: &toml::Value) -> Result<String> {
    match value {
        toml::Value::String(value) => Ok(value.clone()),
        toml::Value::Integer(value) => Ok(value.to_string()),
        toml::Value::Float(value) => Ok(value.to_string()),
        toml::Value::Boolean(value) => Ok(value.to_string()),
        _ => bail!("source and script parameter values must be scalar TOML values"),
    }
}

fn append_optional(args: &mut Vec<OsString>, flag: &str, value: Option<&str>) {
    if let Some(value) = value {
        append_pair(args, flag, value);
    }
}

fn append_optional_owned(args: &mut Vec<OsString>, flag: &str, value: Option<String>) {
    if let Some(value) = value {
        append_pair(args, flag, &value);
    }
}

fn append_pair(args: &mut Vec<OsString>, flag: &str, value: &str) {
    args.push(flag.into());
    args.push(value.into());
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    use crate::cli::{Cli, Commands, ScriptCommands};

    #[test]
    fn cli_values_override_config_values() {
        let dir = std::env::temp_dir().join(format!("mlab-config-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("create config test directory");
        let path = dir.join("marketlab.toml");
        fs::write(
            &path,
            r#"
version = 1

[market]
provider = "mmt"
exchange = "bybitf"
symbol = "HYPE/USDT"

[script]
path = "strategy.js"

[sources.candles]
timeframe = 60

[script.params.candles]
fast = 20

[backtest]
from = 1000
to = 2000
leverage = 2
"#,
        )
        .expect("write config");

        let expanded = expand_args(
            [
                "mlab",
                "script",
                "backtest",
                "--config",
                path.to_str().expect("utf8 path"),
                "--symbol",
                "BTC/USDT",
                "--leverage",
                "5",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("expand args");
        let cli = Cli::try_parse_from(expanded).expect("parse expanded args");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => {
                assert_eq!(args.symbol, "BTC/USDT");
                assert_eq!(args.exchange.as_deref(), Some("bybitf"));
                assert_eq!(args.leverage, 5.0);
                assert_eq!(args.source, vec!["candles:timeframe=60"]);
                assert_eq!(args.param, vec!["candles:fast=20"]);
                assert!(args.script.ends_with("strategy.js"));
            }
            _ => panic!("expected script backtest"),
        }
    }

    #[test]
    fn expands_trade_config_and_keeps_cli_precedence() {
        let dir = std::env::temp_dir().join(format!("mlab-trade-config-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("create config test directory");
        let path = dir.join("marketlab.toml");
        fs::write(
            &path,
            r#"
version = 1

[market]
symbol = "BTC/USDT"

[execution]
venue = "bulk"
notional = 100
order_type = "market"
leverage = 3
dry_run = true

[output]
format = "json"
"#,
        )
        .expect("write config");

        let expanded = expand_args(
            [
                "mlab",
                "trade",
                "long",
                "--config",
                path.to_str().expect("utf8 path"),
                "--notional",
                "250",
                "--leverage",
                "5",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .expect("expand trade args");
        let cli = Cli::try_parse_from(expanded).expect("parse expanded trade args");

        match cli.command {
            Commands::Trade {
                command: crate::cli::TradeCommands::Long(args),
            } => {
                assert_eq!(args.symbol, "BTC/USDT");
                assert_eq!(args.notional, Some(250.0));
                assert_eq!(args.leverage, 5.0);
                assert!(args.dry_run);
                assert!(matches!(args.output, crate::cli::OutputFormat::Json));
            }
            _ => panic!("expected trade long"),
        }
    }
}
