use std::collections::VecDeque;
use std::io::{self, Write};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{BacktestSmaCrossoverArgs, CliProviderKind, OutputFormat, RunSmaCrossoverArgs};
use crate::providers::mmt::MmtProvider;
use crate::providers::mmt::ws_candles::MmtCandlesStream;

#[derive(Debug, Clone, Serialize)]
struct StrategyResult {
    r#type: &'static str,
    version: &'static str,
    strategy: &'static str,
    provider: String,
    exchange: String,
    symbol: String,
    ts_ms: u64,
    mode: &'static str,
    window: StrategyWindow,
    inputs: StrategyInputs,
    signal: StrategySignal,
    decision: StrategyDecision,
    metrics: StrategyMetrics,
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BacktestResult {
    r#type: &'static str,
    version: &'static str,
    strategy: &'static str,
    provider: String,
    exchange: String,
    symbol: String,
    ts_ms: u64,
    window: StrategyWindow,
    inputs: StrategyInputs,
    performance: BacktestPerformance,
    latest_state: StrategyMetrics,
    reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CompactStrategyResult<'a> {
    r#type: &'static str,
    version: &'static str,
    strategy: &'static str,
    provider: &'a str,
    exchange: &'a str,
    symbol: &'a str,
    ts_ms: u64,
    mode: &'static str,
    signal: &'a StrategySignal,
    decision: &'a StrategyDecision,
    metrics: &'a StrategyMetrics,
}

#[derive(Debug, Serialize)]
struct CompactBacktestResult<'a> {
    r#type: &'static str,
    version: &'static str,
    strategy: &'static str,
    provider: &'a str,
    exchange: &'a str,
    symbol: &'a str,
    ts_ms: u64,
    performance: &'a BacktestPerformance,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyWindow {
    from: Option<u64>,
    to: Option<u64>,
    timeframe_sec: u32,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyInputs {
    fast: usize,
    slow: usize,
    confirm_bars: usize,
}

#[derive(Debug, Clone, Serialize)]
struct StrategySignal {
    event: &'static str,
    side: &'static str,
    triggered: bool,
    strength: f64,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyDecision {
    allow: bool,
    status: &'static str,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyMetrics {
    prev_fast: f64,
    prev_slow: f64,
    curr_fast: f64,
    curr_slow: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BacktestPerformance {
    trades: usize,
    sharpe: Option<f64>,
    max_drawdown: Option<f64>,
}

#[derive(Debug, Clone)]
struct CrossoverState {
    event: &'static str,
    side: &'static str,
    triggered: bool,
    strength_bps: f64,
    prev_fast: f64,
    prev_slow: f64,
    curr_fast: f64,
    curr_slow: f64,
}

#[derive(Debug, Clone, Default)]
struct PerformanceMetrics {
    trades: usize,
    sharpe: Option<f64>,
    max_drawdown: Option<f64>,
}

#[derive(Debug, Clone)]
struct StrategyCommonArgs {
    provider: CliProviderKind,
    exchange: String,
    symbol: String,
    timeframe: u32,
    fast: usize,
    slow: usize,
    confirm_bars: usize,
    buffer_size: u16,
    output: OutputFormat,
    verbose: bool,
}

impl From<&RunSmaCrossoverArgs> for StrategyCommonArgs {
    fn from(value: &RunSmaCrossoverArgs) -> Self {
        Self {
            provider: value.provider,
            exchange: value.exchange.clone(),
            symbol: value.symbol.clone(),
            timeframe: value.timeframe,
            fast: value.fast,
            slow: value.slow,
            confirm_bars: value.confirm_bars,
            buffer_size: value.buffer_size,
            output: value.output,
            verbose: value.verbose,
        }
    }
}

impl From<&BacktestSmaCrossoverArgs> for StrategyCommonArgs {
    fn from(value: &BacktestSmaCrossoverArgs) -> Self {
        Self {
            provider: value.provider,
            exchange: value.exchange.clone(),
            symbol: value.symbol.clone(),
            timeframe: value.timeframe,
            fast: value.fast,
            slow: value.slow,
            confirm_bars: value.confirm_bars,
            buffer_size: 50,
            output: value.output,
            verbose: value.verbose,
        }
    }
}

impl StrategyCommonArgs {
    fn mmt_tf(&self) -> Result<&'static str> {
        match self.timeframe {
            60 => Ok("1m"),
            300 => Ok("5m"),
            900 => Ok("15m"),
            1800 => Ok("30m"),
            3600 => Ok("1h"),
            14_400 => Ok("4h"),
            86_400 => Ok("1d"),
            _ => bail!(
                "unsupported --timeframe seconds: {} (supported: 60,300,900,1800,3600,14400,86400)",
                self.timeframe
            ),
        }
    }
}

pub async fn handle_run(args: RunSmaCrossoverArgs) -> Result<()> {
    args.validate()?;
    let common = StrategyCommonArgs::from(&args);

    if !matches!(common.provider, CliProviderKind::Mmt) {
        bail!("strategy sma-crossover currently supports only --provider mmt");
    }

    stream_strategy(args, common).await
}

pub async fn handle_backtest(args: BacktestSmaCrossoverArgs) -> Result<()> {
    args.validate()?;
    let common = StrategyCommonArgs::from(&args);

    if !matches!(common.provider, CliProviderKind::Mmt) {
        bail!("strategy sma-crossover currently supports only --provider mmt");
    }

    evaluate_window(args, common).await
}

async fn evaluate_window(args: BacktestSmaCrossoverArgs, common: StrategyCommonArgs) -> Result<()> {
    let from = args.from;
    let to = args.to;

    let series = MmtProvider::candles(
        &common.exchange,
        &common.symbol,
        common.mmt_tf()?,
        from,
        to,
    )
    .await?;

    let closes: Vec<f64> = series.data.iter().map(|x| x.c).collect();
    let crossover = crossover_state(&closes, common.fast, common.slow, common.confirm_bars)?;
    let performance = backtest_metrics(&closes, &common)?;
    let result = BacktestResult {
        r#type: "strategy.backtest.result",
        version: "1",
        strategy: "sma-crossover",
        provider: "mmt".to_string(),
        exchange: common.exchange.to_lowercase(),
        symbol: common.symbol.to_uppercase(),
        ts_ms: to * 1000,
        window: StrategyWindow {
            from: Some(from),
            to: Some(to),
            timeframe_sec: common.timeframe,
        },
        inputs: strategy_inputs(&common),
        performance: BacktestPerformance {
            trades: performance.trades,
            sharpe: performance.sharpe,
            max_drawdown: performance.max_drawdown,
        },
        latest_state: StrategyMetrics {
            prev_fast: crossover.prev_fast,
            prev_slow: crossover.prev_slow,
            curr_fast: crossover.curr_fast,
            curr_slow: crossover.curr_slow,
        },
        reasons: validation_reasons(&common, &performance, &crossover),
    };

    render_backtest_result(&result, common.output, common.verbose)
}

async fn stream_strategy(args: RunSmaCrossoverArgs, common: StrategyCommonArgs) -> Result<()> {
    if matches!(common.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("stream mode currently supports only --output terminal|json|jsonl");
    }

    let mut stream = MmtCandlesStream::connect(&common.exchange, &common.symbol, common.mmt_tf()?).await?;
    let first_candle = stream.next_candle().await?;
    let mut history = load_stream_warmup_closes(&args, &common, first_candle.t).await?;
    let history_cap = (common.slow + common.confirm_bars + 8).max(64);
    trim_history(&mut history, history_cap);
    let mut open_candle: Option<(u64, f64)> = Some((first_candle.t, first_candle.c));
    let mut buf: VecDeque<String> = VecDeque::with_capacity(args.buffer_size as usize);
    emit_startup_state(
        &common,
        args.from,
        &history,
        first_candle.t,
        first_candle.c,
        &mut buf,
    )?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nstream stopped");
                break;
            }
            candle = stream.next_candle() => {
                let candle = candle?;
                match open_candle {
                    None => {
                        open_candle = Some((candle.t, candle.c));
                    }
                    Some((open_ts, _)) if candle.t == open_ts => {
                        open_candle = Some((candle.t, candle.c));
                    }
                    Some((closed_ts, closed_close)) => {
                        if history.back().copied() != Some(closed_close) {
                            if history.len() >= history_cap {
                                history.pop_front();
                            }
                            history.push_back(closed_close);
                        }

                        let closes: Vec<f64> = history.iter().copied().collect();
                        if let Ok(crossover) = crossover_state(&closes, args.fast, args.slow, args.confirm_bars) {
                            let result = build_stream_result(
                                &common,
                                args.from,
                                closed_ts,
                                &crossover,
                                live_decision(&crossover),
                                vec![
                                    format!("live tf={} on_close=true", common.mmt_tf()?),
                                    format!(
                                        "prev_fast={:.6} prev_slow={:.6} curr_fast={:.6} curr_slow={:.6}",
                                        crossover.prev_fast, crossover.prev_slow, crossover.curr_fast, crossover.curr_slow
                                    ),
                                ],
                            );

                            emit_stream_result(&result, common.output, common.verbose, args.buffer_size, &mut buf)?;
                        }

                        open_candle = Some((candle.t, candle.c));
                    }
                }
            }
        }
    }

    Ok(())
}

fn strategy_inputs(args: &StrategyCommonArgs) -> StrategyInputs {
    StrategyInputs {
        fast: args.fast,
        slow: args.slow,
        confirm_bars: args.confirm_bars,
    }
}

fn emit_startup_state(
    args: &StrategyCommonArgs,
    from: Option<u64>,
    history: &VecDeque<f64>,
    open_ts: u64,
    open_close: f64,
    buf: &mut VecDeque<String>,
) -> Result<()> {
    let mut closes: Vec<f64> = history.iter().copied().collect();
    closes.push(open_close);

    let crossover = crossover_state(&closes, args.fast, args.slow, args.confirm_bars)?;
    let decision = StrategyDecision {
        allow: false,
        status: "OPEN_CANDLE",
        reason: "startup state from warmup history and current open candle".to_string(),
    };
    let reasons = vec![
        format!("live tf={} on_close=true startup=true", args.mmt_tf()?),
        format!(
            "prev_fast={:.6} prev_slow={:.6} curr_fast={:.6} curr_slow={:.6}",
            crossover.prev_fast, crossover.prev_slow, crossover.curr_fast, crossover.curr_slow
        ),
    ];
    let result = build_stream_result(args, from, open_ts, &crossover, decision, reasons);

    emit_stream_result(&result, args.output, args.verbose, args.buffer_size, buf)
}

fn live_decision(crossover: &CrossoverState) -> StrategyDecision {
    if crossover.event == "cross_up" {
        StrategyDecision {
            allow: true,
            status: "ENTER_LONG",
            reason: "fast SMA crossed above slow SMA".to_string(),
        }
    } else if crossover.event == "cross_down" {
        StrategyDecision {
            allow: true,
            status: "ENTER_SHORT",
            reason: "fast SMA crossed below slow SMA".to_string(),
        }
    } else {
        StrategyDecision {
            allow: false,
            status: "WAIT",
            reason: "no fresh crossover on candle close".to_string(),
        }
    }
}

fn build_stream_result(
    args: &StrategyCommonArgs,
    from: Option<u64>,
    ts_s: u64,
    crossover: &CrossoverState,
    decision: StrategyDecision,
    reasons: Vec<String>,
) -> StrategyResult {
    StrategyResult {
        r#type: "strategy.result",
        version: "1",
        strategy: "sma-crossover",
        provider: "mmt".to_string(),
        exchange: args.exchange.to_lowercase(),
        symbol: args.symbol.to_uppercase(),
        ts_ms: ts_s * 1000,
        mode: "stream",
        window: StrategyWindow {
            from,
            to: None,
            timeframe_sec: args.timeframe,
        },
        inputs: strategy_inputs(args),
        signal: StrategySignal {
            event: crossover.event,
            side: crossover.side,
            triggered: crossover.triggered,
            strength: crossover.strength_bps,
        },
        decision,
        metrics: StrategyMetrics {
            prev_fast: crossover.prev_fast,
            prev_slow: crossover.prev_slow,
            curr_fast: crossover.curr_fast,
            curr_slow: crossover.curr_slow,
        },
        reasons,
    }
}

fn emit_stream_result(
    result: &StrategyResult,
    output: OutputFormat,
    verbose: bool,
    buffer_size: u16,
    buf: &mut VecDeque<String>,
) -> Result<()> {
    match output {
        OutputFormat::Json | OutputFormat::Jsonl => {
            print_strategy_json(result, output, verbose)?;
        }
        OutputFormat::Terminal => {
            let line = format!(
                "ts={} signal={} side={} strength_bps={:.4} status={}",
                result.ts_ms,
                result.signal.event,
                result.signal.side,
                result.signal.strength,
                result.decision.status
            );
            if buf.len() >= buffer_size as usize {
                buf.pop_front();
            }
            buf.push_back(line);
            render_terminal(buf)?;
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }

    Ok(())
}

async fn load_stream_warmup_closes(
    args: &RunSmaCrossoverArgs,
    common: &StrategyCommonArgs,
    first_stream_ts: u64,
) -> Result<VecDeque<f64>> {
    let tf_sec = common.timeframe as u64;
    let warmup_bars = (common.slow + common.confirm_bars + 8).max(64) as u64;
    let aligned_first = first_stream_ts - (first_stream_ts % tf_sec);
    let aligned_first_ms = aligned_first * 1000;
    let from = args
        .from
        .unwrap_or_else(|| aligned_first_ms.saturating_sub(warmup_bars * tf_sec * 1000));

    let series = MmtProvider::candles(
        &common.exchange,
        &common.symbol,
        common.mmt_tf()?,
        from,
        aligned_first_ms,
    )
    .await?;

    let mut history = VecDeque::with_capacity(series.data.len());
    for candle in series.data {
        if candle.t < aligned_first_ms {
            history.push_back(candle.c);
        }
    }
    Ok(history)
}

fn trim_history(history: &mut VecDeque<f64>, cap: usize) {
    while history.len() > cap {
        history.pop_front();
    }
}

fn validation_reasons(
    args: &StrategyCommonArgs,
    metrics: &PerformanceMetrics,
    crossover: &CrossoverState,
) -> Vec<String> {
    vec![
        format!(
            "series=candle_close tf={} fast={} slow={} confirm={}",
            args.mmt_tf().unwrap_or("unknown"),
            args.fast,
            args.slow,
            args.confirm_bars
        ),
        format!(
            "trades={} sharpe={:.4?} max_drawdown={:.4?}",
            metrics.trades,
            metrics.sharpe,
            metrics.max_drawdown
        ),
        format!(
            "latest_signal={} side={} strength_bps={:.4}",
            crossover.event, crossover.side, crossover.strength_bps
        ),
    ]
}

fn backtest_metrics(closes: &[f64], args: &StrategyCommonArgs) -> Result<PerformanceMetrics> {
    let returns = strategy_returns(closes, args.fast, args.slow)?;
    if returns.is_empty() {
        return Ok(PerformanceMetrics::default());
    }

    let trades = count_trades(closes, args.fast, args.slow)?;
    let sharpe = sharpe(&returns);
    let max_drawdown = max_drawdown(&returns);

    Ok(PerformanceMetrics {
        trades,
        sharpe,
        max_drawdown,
    })
}

fn strategy_returns(closes: &[f64], fast: usize, slow: usize) -> Result<Vec<f64>> {
    if closes.len() <= slow {
        return Ok(Vec::new());
    }

    let mut returns = Vec::new();
    let mut position = 0.0_f64;

    for idx in slow..(closes.len() - 1) {
        let fast_now = sma_at(closes, fast, idx)?;
        let slow_now = sma_at(closes, slow, idx)?;
        let fast_prev = sma_at(closes, fast, idx - 1)?;
        let slow_prev = sma_at(closes, slow, idx - 1)?;

        if fast_prev <= slow_prev && fast_now > slow_now {
            position = 1.0;
        } else if fast_prev >= slow_prev && fast_now < slow_now {
            position = -1.0;
        }

        let next_delta = closes[idx + 1] - closes[idx];
        let denom = closes[idx].abs().max(1.0);
        returns.push(position * (next_delta / denom));
    }

    Ok(returns)
}

fn count_trades(closes: &[f64], fast: usize, slow: usize) -> Result<usize> {
    if closes.len() <= slow {
        return Ok(0);
    }

    let mut trades = 0_usize;
    for idx in slow..closes.len() {
        let fast_now = sma_at(closes, fast, idx)?;
        let slow_now = sma_at(closes, slow, idx)?;
        let fast_prev = sma_at(closes, fast, idx - 1)?;
        let slow_prev = sma_at(closes, slow, idx - 1)?;
        if (fast_prev <= slow_prev && fast_now > slow_now)
            || (fast_prev >= slow_prev && fast_now < slow_now)
        {
            trades += 1;
        }
    }
    Ok(trades)
}

fn crossover_state(
    closes: &[f64],
    fast: usize,
    slow: usize,
    confirm_bars: usize,
) -> Result<CrossoverState> {
    let need = slow + confirm_bars;
    if closes.len() < need {
        bail!(
            "insufficient points for sma-crossover: got {}, need at least {}",
            closes.len(),
            need
        );
    }

    let idx_curr = closes.len() - 1;
    let idx_prev = closes.len() - 1 - confirm_bars;

    let curr_fast = sma_at(closes, fast, idx_curr)?;
    let curr_slow = sma_at(closes, slow, idx_curr)?;
    let prev_fast = sma_at(closes, fast, idx_prev)?;
    let prev_slow = sma_at(closes, slow, idx_prev)?;

    let cross_up = prev_fast <= prev_slow && curr_fast > curr_slow;
    let cross_down = prev_fast >= prev_slow && curr_fast < curr_slow;

    let event = if cross_up {
        "cross_up"
    } else if cross_down {
        "cross_down"
    } else {
        "no_cross"
    };

    let side = if cross_up {
        "buy"
    } else if cross_down {
        "sell"
    } else {
        "neutral"
    };

    let strength_bps = if curr_slow.abs() > 0.0 {
        ((curr_fast - curr_slow).abs() / curr_slow.abs()) * 10_000.0
    } else {
        0.0
    };

    Ok(CrossoverState {
        event,
        side,
        triggered: cross_up || cross_down,
        strength_bps,
        prev_fast,
        prev_slow,
        curr_fast,
        curr_slow,
    })
}

fn sharpe(returns: &[f64]) -> Option<f64> {
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let var = returns
        .iter()
        .map(|r| {
            let d = r - mean;
            d * d
        })
        .sum::<f64>()
        / (returns.len() as f64 - 1.0);
    let std = var.sqrt();
    if std <= f64::EPSILON {
        None
    } else {
        Some((mean / std) * (returns.len() as f64).sqrt())
    }
}

fn max_drawdown(returns: &[f64]) -> Option<f64> {
    if returns.is_empty() {
        return None;
    }
    let mut equity = 1.0_f64;
    let mut peak = 1.0_f64;
    let mut max_dd = 0.0_f64;

    for r in returns {
        equity *= 1.0 + r;
        if equity > peak {
            peak = equity;
        }
        let dd = if peak > 0.0 {
            (peak - equity) / peak
        } else {
            0.0
        };
        if dd > max_dd {
            max_dd = dd;
        }
    }

    Some(max_dd)
}

fn render_backtest_result(result: &BacktestResult, output: OutputFormat, verbose: bool) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} tf={} [{}-{}]",
                result.symbol,
                result.window.timeframe_sec,
                result.window.from.unwrap_or(0),
                result.window.to.unwrap_or(0)
            );
            println!(
                "strategy: {} fast={} slow={} confirm={}",
                result.strategy,
                result.inputs.fast,
                result.inputs.slow,
                result.inputs.confirm_bars
            );
            println!(
                "trades={} sharpe={:.4?} max_drawdown={:.4?}",
                result.performance.trades,
                result.performance.sharpe,
                result.performance.max_drawdown
            );
            if verbose {
                println!(
                    "latest_state: prev_fast={:.4} prev_slow={:.4} curr_fast={:.4} curr_slow={:.4}",
                    result.latest_state.prev_fast,
                    result.latest_state.prev_slow,
                    result.latest_state.curr_fast,
                    result.latest_state.curr_slow
                );
            }
        }
        OutputFormat::Json | OutputFormat::Jsonl => print_backtest_json(result, output, verbose)?,
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO strategy sma-crossover export: {:?}", output);
        }
    }
    Ok(())
}

fn print_strategy_json(result: &StrategyResult, output: OutputFormat, verbose: bool) -> Result<()> {
    if verbose {
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(result)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(result)?),
            _ => unreachable!(),
        }
    } else {
        let compact = CompactStrategyResult {
            r#type: result.r#type,
            version: result.version,
            strategy: result.strategy,
            provider: &result.provider,
            exchange: &result.exchange,
            symbol: &result.symbol,
            ts_ms: result.ts_ms,
            mode: result.mode,
            signal: &result.signal,
            decision: &result.decision,
            metrics: &result.metrics,
        };
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&compact)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(&compact)?),
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn print_backtest_json(result: &BacktestResult, output: OutputFormat, verbose: bool) -> Result<()> {
    if verbose {
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(result)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(result)?),
            _ => unreachable!(),
        }
    } else {
        let compact = CompactBacktestResult {
            r#type: result.r#type,
            version: result.version,
            strategy: result.strategy,
            provider: &result.provider,
            exchange: &result.exchange,
            symbol: &result.symbol,
            ts_ms: result.ts_ms,
            performance: &result.performance,
        };
        match output {
            OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&compact)?),
            OutputFormat::Jsonl => println!("{}", serde_json::to_string(&compact)?),
            _ => unreachable!(),
        }
    }

    Ok(())
}

fn render_terminal(buf: &VecDeque<String>) -> Result<()> {
    print!("\x1B[2J\x1B[H");
    println!("market-lab strategy sma-crossover stream (latest {} updates)", buf.len());
    println!("---------------------------------------------------------------");
    for line in buf {
        println!("{}", line);
    }
    io::stdout().flush().context("flush failed")?;
    Ok(())
}

fn sma_at(values: &[f64], period: usize, idx: usize) -> Result<f64> {
    if period == 0 {
        bail!("period must be > 0");
    }
    if idx + 1 < period {
        bail!("insufficient data for period {}", period);
    }
    let start = idx + 1 - period;
    let sum: f64 = values[start..=idx].iter().sum();
    Ok(sum / period as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_strategy_result_uses_normalized_type() {
        let out = StrategyResult {
            r#type: "strategy.result",
            version: "1",
            strategy: "sma-crossover",
            provider: "mmt".to_string(),
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            mode: "validation",
            window: StrategyWindow {
                from: Some(0),
                to: Some(1),
                timeframe_sec: 60,
            },
            inputs: StrategyInputs {
                fast: 20,
                slow: 50,
                confirm_bars: 1,
            },
            signal: StrategySignal {
                event: "no_cross",
                side: "neutral",
                triggered: false,
                strength: 0.0,
            },
            decision: StrategyDecision {
                allow: false,
                status: "REJECT",
                reason: "test".to_string(),
            },
            metrics: StrategyMetrics {
                prev_fast: 0.0,
                prev_slow: 0.0,
                curr_fast: 0.0,
                curr_slow: 0.0,
            },
            reasons: vec![],
        };
        let v = serde_json::to_value(out).expect("serialize strategy result");
        assert_eq!(v["type"], "strategy.result");
        assert_eq!(v["version"], "1");
        assert_eq!(v["mode"], "validation");
        assert!(v.get("signal").is_some());
        assert!(v.get("decision").is_some());
    }

    #[test]
    fn backtest_result_uses_backtest_type() {
        let out = BacktestResult {
            r#type: "strategy.backtest.result",
            version: "1",
            strategy: "sma-crossover",
            provider: "mmt".to_string(),
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            window: StrategyWindow {
                from: Some(0),
                to: Some(1),
                timeframe_sec: 60,
            },
            inputs: StrategyInputs {
                fast: 20,
                slow: 50,
                confirm_bars: 1,
            },
            performance: BacktestPerformance {
                trades: 5,
                sharpe: Some(1.0),
                max_drawdown: Some(0.1),
            },
            latest_state: StrategyMetrics {
                prev_fast: 0.0,
                prev_slow: 0.0,
                curr_fast: 0.0,
                curr_slow: 0.0,
            },
            reasons: vec![],
        };
        let v = serde_json::to_value(out).expect("serialize backtest result");
        assert_eq!(v["type"], "strategy.backtest.result");
        assert!(v.get("performance").is_some());
        assert!(v.get("latest_state").is_some());
    }

    #[test]
    fn counts_trades_from_crosses() {
        let closes = vec![1.0, 2.0, 3.0, 2.0, 1.0, 2.0, 3.0, 2.0];
        let trades = count_trades(&closes, 2, 3).expect("trade count");
        assert!(trades > 0);
    }

    #[test]
    fn compact_strategy_json_omits_inputs_window_and_reasons() {
        let out = StrategyResult {
            r#type: "strategy.result",
            version: "1",
            strategy: "sma-crossover",
            provider: "mmt".to_string(),
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            mode: "validation",
            window: StrategyWindow {
                from: Some(0),
                to: Some(1),
                timeframe_sec: 60,
            },
            inputs: StrategyInputs {
                fast: 20,
                slow: 50,
                confirm_bars: 1,
            },
            signal: StrategySignal {
                event: "no_cross",
                side: "neutral",
                triggered: false,
                strength: 0.0,
            },
            decision: StrategyDecision {
                allow: false,
                status: "WAIT",
                reason: "test".to_string(),
            },
            metrics: StrategyMetrics {
                prev_fast: 1.0,
                prev_slow: 2.0,
                curr_fast: 3.0,
                curr_slow: 4.0,
            },
            reasons: vec!["debug".to_string()],
        };
        let compact = CompactStrategyResult {
            r#type: out.r#type,
            version: out.version,
            strategy: out.strategy,
            provider: &out.provider,
            exchange: &out.exchange,
            symbol: &out.symbol,
            ts_ms: out.ts_ms,
            mode: out.mode,
            signal: &out.signal,
            decision: &out.decision,
            metrics: &out.metrics,
        };
        let v = serde_json::to_value(compact).expect("serialize compact strategy");
        assert!(v.get("window").is_none());
        assert!(v.get("inputs").is_none());
        assert!(v.get("reasons").is_none());
        assert!(v.get("metrics").is_some());
    }

    #[test]
    fn compact_backtest_json_omits_window_inputs_and_reasons() {
        let out = BacktestResult {
            r#type: "strategy.backtest.result",
            version: "1",
            strategy: "sma-crossover",
            provider: "mmt".to_string(),
            exchange: "bybitf".to_string(),
            symbol: "BTC/USDT".to_string(),
            ts_ms: 1,
            window: StrategyWindow {
                from: Some(0),
                to: Some(1),
                timeframe_sec: 60,
            },
            inputs: StrategyInputs {
                fast: 20,
                slow: 50,
                confirm_bars: 1,
            },
            performance: BacktestPerformance {
                trades: 5,
                sharpe: Some(1.0),
                max_drawdown: Some(0.1),
            },
            latest_state: StrategyMetrics {
                prev_fast: 1.0,
                prev_slow: 2.0,
                curr_fast: 3.0,
                curr_slow: 4.0,
            },
            reasons: vec!["debug".to_string()],
        };
        let compact = CompactBacktestResult {
            r#type: out.r#type,
            version: out.version,
            strategy: out.strategy,
            provider: &out.provider,
            exchange: &out.exchange,
            symbol: &out.symbol,
            ts_ms: out.ts_ms,
            performance: &out.performance,
        };
        let v = serde_json::to_value(compact).expect("serialize compact backtest");
        assert!(v.get("window").is_none());
        assert!(v.get("inputs").is_none());
        assert!(v.get("reasons").is_none());
        assert!(v.get("performance").is_some());
    }
}
