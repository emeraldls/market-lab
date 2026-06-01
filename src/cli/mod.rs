use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::domain::enums::{BookMode, ProviderKind, Side};
use crate::domain::requests::{
    DepthRequest, ImbalanceRequest, InspectRequest, ReplayRequest, SlippageRequest, SpreadRequest,
    VampRequest,
};

#[derive(Parser, Debug)]
#[command(name = "mlab")]
#[command(version, about = "Deterministic market replay CLI", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Inspect(InspectArgs),
    Replay(ReplayArgs),
    Source {
        #[command(subcommand)]
        command: SourceCommands,
    },
    Study {
        #[command(subcommand)]
        command: StudyCommands,
    },
    Script {
        #[command(subcommand)]
        command: ScriptCommands,
    },
    Strategy {
        #[command(subcommand)]
        command: StrategyCommands,
    },
    Health(HealthArgs),
    Status(StatusArgs),
    Upgrade(UpgradeArgs),
}

#[derive(Subcommand, Debug)]
pub enum SourceCommands {
    Orderbook(SourceOrderbookArgs),
    Vd(SourceVdArgs),
    Candles(SourceCandlesArgs),
}

#[derive(Subcommand, Debug)]
pub enum StudyCommands {
    Slippage(SlippageArgs),
    Imbalance(ImbalanceArgs),
    Spread(SpreadArgs),
    Depth(DepthArgs),
    Vamp(VampArgs),
    Cvd(CvdArgs),
}

#[derive(Subcommand, Debug)]
pub enum ScriptCommands {
    Run(ScriptRunArgs),
    Backtest(ScriptBacktestArgs),
    Runs {
        #[command(subcommand)]
        command: ScriptRunHistoryCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum ScriptRunHistoryCommands {
    List(ScriptRunsListArgs),
    Show(ScriptRunsShowArgs),
}

#[derive(Subcommand, Debug)]
pub enum StrategyCommands {
    Run {
        #[command(subcommand)]
        command: StrategyRunCommands,
    },
    Backtest {
        #[command(subcommand)]
        command: StrategyBacktestCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum StrategyRunCommands {
    SmaCrossover(RunSmaCrossoverArgs),
}

#[derive(Subcommand, Debug)]
pub enum StrategyBacktestCommands {
    SmaCrossover(BacktestSmaCrossoverArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ScriptRunArgs {
    pub script: String,
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: Option<String>,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long)]
    pub timeframe: Option<u32>,
    #[arg(long)]
    pub from: Option<u64>,
    #[arg(long)]
    pub to: Option<u64>,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long = "input")]
    pub input: Vec<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl ScriptRunArgs {
    pub fn validate(&self) -> Result<()> {
        if self.script.trim().is_empty() {
            bail!("script path is required");
        }
        Ok(())
    }

    pub fn mmt_tf(&self) -> Result<&'static str> {
        let timeframe = self
            .timeframe
            .ok_or_else(|| anyhow::anyhow!("--timeframe is required for source=candles"))?;
        mmt_timeframe_from_seconds(timeframe)
    }
}

#[derive(Clone, Debug, Args)]
pub struct ScriptBacktestArgs {
    pub script: String,
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: u32,
    #[arg(long)]
    pub from: u64,
    #[arg(long)]
    pub to: u64,
    #[arg(long = "input")]
    pub input: Vec<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl ScriptBacktestArgs {
    pub fn validate(&self) -> Result<()> {
        if self.script.trim().is_empty() {
            bail!("script path is required");
        }
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        mmt_timeframe_from_seconds(self.timeframe)?;
        if self.from >= self.to {
            bail!("--from must be less than --to");
        }
        Ok(())
    }

    pub fn mmt_tf(&self) -> Result<&'static str> {
        mmt_timeframe_from_seconds(self.timeframe)
    }
}

#[derive(Clone, Debug, Args)]
pub struct ScriptRunsListArgs {
    #[arg(long, default_value_t = 5)]
    pub limit: usize,
    #[arg(long, default_value_t = false)]
    pub all: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ScriptRunsListArgs {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 {
            bail!("--limit must be >= 1");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct ScriptRunsShowArgs {
    pub run: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ScriptRunsShowArgs {
    pub fn validate(&self) -> Result<()> {
        if self.run.trim().is_empty() {
            bail!("run id, file name, or path is required");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct SourceVdArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: u32,
    #[arg(long)]
    pub from: Option<u64>,
    #[arg(long)]
    pub to: Option<u64>,
    #[arg(long, default_value_t = 1)]
    pub bucket: u8,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl SourceVdArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        mmt_timeframe_from_seconds(self.timeframe)?;
        if self.stream {
            if self.from.is_some() || self.to.is_some() {
                bail!("--from/--to are not allowed with --stream");
            }
        } else {
            let from = self
                .from
                .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?;
            let to = self
                .to
                .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?;
            if from >= to {
                bail!("--from must be less than --to");
            }
        }
        if !(1..=11).contains(&self.bucket) {
            bail!("--bucket must be in range 1..=11");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        if self.interval_ms == 0 {
            bail!("--interval-ms must be >= 1");
        }
        Ok(())
    }

    pub fn mmt_tf(&self) -> Result<&'static str> {
        mmt_timeframe_from_seconds(self.timeframe)
    }
}

#[derive(Clone, Debug, Args)]
pub struct CvdArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: u32,
    #[arg(long)]
    pub from: Option<u64>,
    #[arg(long)]
    pub to: Option<u64>,
    #[arg(long, default_value_t = 1)]
    pub bucket: u8,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

#[derive(Clone, Debug, Args)]
pub struct SourceCandlesArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: u32,
    #[arg(long)]
    pub from: Option<u64>,
    #[arg(long)]
    pub to: Option<u64>,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl SourceCandlesArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        mmt_timeframe_from_seconds(self.timeframe)?;
        if self.stream {
            if self.from.is_some() || self.to.is_some() {
                bail!("--from/--to are not allowed with --stream");
            }
        } else {
            let from = self
                .from
                .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?;
            let to = self
                .to
                .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?;
            if from >= to {
                bail!("--from must be less than --to");
            }
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        if self.interval_ms == 0 {
            bail!("--interval-ms must be >= 1");
        }
        Ok(())
    }

    pub fn mmt_tf(&self) -> Result<&'static str> {
        mmt_timeframe_from_seconds(self.timeframe)
    }
}

impl CvdArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        mmt_timeframe_from_seconds(self.timeframe)?;
        if self.stream {
            if self.from.is_some() || self.to.is_some() {
                bail!("--from/--to are not allowed with --stream");
            }
        } else {
            let from = self
                .from
                .ok_or_else(|| anyhow::anyhow!("--from is required when not streaming"))?;
            let to = self
                .to
                .ok_or_else(|| anyhow::anyhow!("--to is required when not streaming"))?;
            if from >= to {
                bail!("--from must be less than --to");
            }
        }
        if !(1..=11).contains(&self.bucket) {
            bail!("--bucket must be in range 1..=11");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        if self.interval_ms == 0 {
            bail!("--interval-ms must be >= 1");
        }
        Ok(())
    }

    pub fn mmt_tf(&self) -> Result<&'static str> {
        mmt_timeframe_from_seconds(self.timeframe)
    }
}

#[derive(Clone, Debug, Args)]
pub struct SourceOrderbookArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value_t = 100)]
    pub depth: u16,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    #[arg(long)]
    pub min_size: Option<f64>,
    #[arg(long)]
    pub max_size: Option<f64>,
    #[arg(long)]
    pub price_group: Option<f64>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl SourceOrderbookArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        if self.interval_ms == 0 {
            bail!("--interval must be >= 1");
        }
        if let Some(pg) = self.price_group
            && pg <= 0.0
        {
            bail!("--price-group must be > 0");
        }
        if let (Some(min), Some(max)) = (self.min_size, self.max_size)
            && min > max
        {
            bail!("--min-size cannot be greater than --max-size");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct HealthArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct StatusArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct UpgradeArgs {
    #[arg(long, default_value_t = false)]
    pub check: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct InspectArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub at: u64,
    #[arg(long, default_value_t = 20)]
    pub depth: u16,
    #[arg(long, value_enum, default_value_t = CliBookMode::Binned)]
    pub book_mode: CliBookMode,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl InspectArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> InspectRequest {
        InspectRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            at: self.at,
            depth: self.depth,
            book_mode: self.book_mode.into(),
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct ReplayArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub from: u64,
    #[arg(long)]
    pub to: u64,
    #[arg(long, default_value_t = 1)]
    pub speed: u32,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ReplayArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.from >= self.to {
            bail!("--from must be less than --to");
        }
        if self.speed < 1 {
            bail!("--speed must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> ReplayRequest {
        ReplayRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            from: self.from,
            to: self.to,
            speed: self.speed,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct SlippageArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, value_enum)]
    pub side: CliSide,
    #[arg(long)]
    pub notional: f64,
    #[arg(long, default_value_t = 200)]
    pub depth: u16,
    #[arg(long, value_enum, default_value_t = CliBookMode::Binned)]
    pub book_mode: CliBookMode,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl SlippageArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.notional <= 0.0 {
            bail!("--notional must be > 0");
        }
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> SlippageRequest {
        SlippageRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            side: self.side.into(),
            notional: self.notional,
            depth: self.depth,
            book_mode: self.book_mode.into(),
            stream: self.stream,
            buffer_size: self.buffer_size,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct ImbalanceArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value_t = 20)]
    pub depth: u16,
    #[arg(long, value_enum, default_value_t = CliBookMode::Binned)]
    pub book_mode: CliBookMode,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl ImbalanceArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> ImbalanceRequest {
        ImbalanceRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            depth: self.depth,
            book_mode: self.book_mode.into(),
            stream: self.stream,
            buffer_size: self.buffer_size,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct VampArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value_t = 200)]
    pub depth: u16,
    #[arg(long)]
    pub dollar_depth: f64,
    #[arg(long, value_enum, default_value_t = CliBookMode::Binned)]
    pub book_mode: CliBookMode,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

#[derive(Clone, Debug, Args)]
pub struct SpreadArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value_t = 20)]
    pub depth: u16,
    #[arg(long, value_enum, default_value_t = CliBookMode::Binned)]
    pub book_mode: CliBookMode,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl SpreadArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> SpreadRequest {
        SpreadRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            depth: self.depth,
            book_mode: self.book_mode.into(),
            stream: self.stream,
            buffer_size: self.buffer_size,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct DepthArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::MarketLab)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value_t = 20)]
    pub levels: u16,
    #[arg(long, value_enum, default_value_t = CliBookMode::Binned)]
    pub book_mode: CliBookMode,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

#[derive(Clone, Debug, Args)]
pub struct RunSmaCrossoverArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: u32,
    #[arg(long)]
    pub from: Option<u64>,
    #[arg(long, default_value_t = 20)]
    pub fast: usize,
    #[arg(long, default_value_t = 50)]
    pub slow: usize,
    #[arg(long, default_value_t = 1)]
    pub confirm_bars: usize,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl RunSmaCrossoverArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        mmt_timeframe_from_seconds(self.timeframe)?;
        if self.fast < 2 {
            bail!("--fast must be >= 2");
        }
        if self.slow <= self.fast {
            bail!("--slow must be greater than --fast");
        }
        if self.confirm_bars < 1 {
            bail!("--confirm-bars must be >= 1");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct BacktestSmaCrossoverArgs {
    #[arg(long, value_enum, default_value_t = CliProviderKind::Mmt)]
    pub provider: CliProviderKind,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: u32,
    #[arg(long)]
    pub from: u64,
    #[arg(long)]
    pub to: u64,
    #[arg(long, default_value_t = 20)]
    pub fast: usize,
    #[arg(long, default_value_t = 50)]
    pub slow: usize,
    #[arg(long, default_value_t = 1)]
    pub confirm_bars: usize,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl BacktestSmaCrossoverArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        mmt_timeframe_from_seconds(self.timeframe)?;
        if self.fast < 2 {
            bail!("--fast must be >= 2");
        }
        if self.slow <= self.fast {
            bail!("--slow must be greater than --fast");
        }
        if self.confirm_bars < 1 {
            bail!("--confirm-bars must be >= 1");
        }
        if self.from >= self.to {
            bail!("--from must be less than --to");
        }
        Ok(())
    }
}

impl DepthArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.levels == 0 {
            bail!("--levels must be >= 1");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> DepthRequest {
        DepthRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            levels: self.levels,
            book_mode: self.book_mode.into(),
            stream: self.stream,
            buffer_size: self.buffer_size,
        }
    }
}

impl VampArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        if self.dollar_depth <= 0.0 {
            bail!("--dollar-depth must be > 0");
        }
        if self.buffer_size == 0 {
            bail!("--buffer-size must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> VampRequest {
        VampRequest {
            provider: self.provider.into(),
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            depth: self.depth,
            dollar_depth: self.dollar_depth,
            book_mode: self.book_mode.into(),
            stream: self.stream,
            buffer_size: self.buffer_size,
        }
    }
}

fn is_valid_symbol(symbol: &str) -> bool {
    let mut parts = symbol.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(base), Some(quote), None) => !base.trim().is_empty() && !quote.trim().is_empty(),
        _ => false,
    }
}

fn mmt_timeframe_from_seconds(seconds: u32) -> Result<&'static str> {
    match seconds {
        60 => Ok("1m"),
        300 => Ok("5m"),
        900 => Ok("15m"),
        1800 => Ok("30m"),
        3600 => Ok("1h"),
        14_400 => Ok("4h"),
        86_400 => Ok("1d"),
        _ => bail!(
            "unsupported --timeframe seconds: {} (supported: 60,300,900,1800,3600,14400,86400)",
            seconds
        ),
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CliProviderKind {
    MarketLab,
    Mmt,
}

impl From<CliProviderKind> for ProviderKind {
    fn from(value: CliProviderKind) -> Self {
        match value {
            CliProviderKind::MarketLab => ProviderKind::MarketLab,
            CliProviderKind::Mmt => ProviderKind::Mmt,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CliSide {
    Buy,
    Sell,
}

impl From<CliSide> for Side {
    fn from(value: CliSide) -> Self {
        match value {
            CliSide::Buy => Side::Buy,
            CliSide::Sell => Side::Sell,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CliBookMode {
    Binned,
    Raw,
}

impl From<CliBookMode> for BookMode {
    fn from(value: CliBookMode) -> Self {
        match value {
            CliBookMode::Binned => BookMode::Binned,
            CliBookMode::Raw => BookMode::Raw,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutputFormat {
    Terminal,
    Json,
    Jsonl,
    Csv,
    Parquet,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_inspect_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "inspect",
            "--exchange",
            "bybit",
            "--symbol",
            "BTC/USDT",
            "--at",
            "1716200000000",
        ])
        .expect("inspect parse should succeed");

        match cli.command {
            Commands::Inspect(args) => {
                assert_eq!(args.exchange, "bybit");
                assert_eq!(args.symbol, "BTC/USDT");
                assert!(matches!(args.book_mode, CliBookMode::Binned));
            }
            _ => panic!("expected inspect command"),
        }
    }

    #[test]
    fn parse_source_vd_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "vd",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
            "--bucket",
            "1",
            "--output",
            "json",
        ])
        .expect("source vd parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Vd(args),
            } => {
                assert_eq!(args.bucket, 1);
                assert_eq!(args.timeframe, 60);
                assert_eq!(args.from, Some(1704067200));
                assert_eq!(args.to, Some(1704067800));
            }
            _ => panic!("expected source vd command"),
        }
    }

    #[test]
    fn parse_study_cvd_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "study",
            "cvd",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
            "--bucket",
            "1",
            "--output",
            "json",
        ])
        .expect("study cvd parse should succeed");

        match cli.command {
            Commands::Study {
                command: StudyCommands::Cvd(args),
            } => {
                assert_eq!(args.bucket, 1);
                assert_eq!(args.timeframe, 60);
                assert_eq!(args.from, Some(1704067200));
                assert_eq!(args.to, Some(1704067800));
            }
            _ => panic!("expected study cvd command"),
        }
    }

    #[test]
    fn reject_source_vd_from_to_in_stream_mode() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "vd",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--bucket",
            "1",
            "--stream",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
        ])
        .expect("parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Vd(args),
            } => {
                let err = args.validate().expect_err("validate should fail");
                assert!(
                    err.to_string()
                        .contains("--from/--to are not allowed with --stream")
                );
            }
            _ => panic!("expected source vd command"),
        }
    }

    #[test]
    fn parse_source_candles_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "candles",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
            "--output",
            "json",
        ])
        .expect("source candles parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                assert_eq!(args.timeframe, 60);
                assert_eq!(args.from, Some(1704067200));
                assert_eq!(args.to, Some(1704067800));
            }
            _ => panic!("expected source candles command"),
        }
    }

    #[test]
    fn reject_source_candles_from_to_in_stream_mode() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "candles",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--stream",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
        ])
        .expect("parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                let err = args.validate().expect_err("validate should fail");
                assert!(
                    err.to_string()
                        .contains("--from/--to are not allowed with --stream")
                );
            }
            _ => panic!("expected source candles command"),
        }
    }

    #[test]
    fn parse_health_command() {
        let cli = Cli::try_parse_from(["market-lab", "health", "--provider", "mmt"])
            .expect("health parse should succeed");
        match cli.command {
            Commands::Health(args) => assert!(matches!(args.provider, CliProviderKind::Mmt)),
            _ => panic!("expected health command"),
        }
    }

    #[test]
    fn parse_status_command() {
        let cli = Cli::try_parse_from(["market-lab", "status", "--provider", "mmt"])
            .expect("status parse should succeed");
        match cli.command {
            Commands::Status(args) => assert!(matches!(args.provider, CliProviderKind::Mmt)),
            _ => panic!("expected status command"),
        }
    }

    #[test]
    fn parse_upgrade_check_command() {
        let cli = Cli::try_parse_from(["mlab", "upgrade", "--check", "--output", "json"])
            .expect("upgrade parse should succeed");

        match cli.command {
            Commands::Upgrade(args) => {
                assert!(args.check);
                assert!(matches!(args.output, OutputFormat::Json));
            }
            _ => panic!("expected upgrade command"),
        }
    }

    #[test]
    fn parse_study_imbalance_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "study",
            "imbalance",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--depth",
            "25",
            "--stream",
        ])
        .expect("study imbalance parse should succeed");

        match cli.command {
            Commands::Study {
                command: StudyCommands::Imbalance(args),
            } => {
                assert!(matches!(args.provider, CliProviderKind::Mmt));
                assert_eq!(args.depth, 25);
                assert!(args.stream);
            }
            _ => panic!("expected study imbalance command"),
        }
    }

    #[test]
    fn parse_study_vamp_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "study",
            "vamp",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--depth",
            "100",
            "--dollar-depth",
            "50000",
        ])
        .expect("study vamp parse should succeed");

        match cli.command {
            Commands::Study {
                command: StudyCommands::Vamp(args),
            } => {
                assert!(matches!(args.provider, CliProviderKind::Mmt));
                assert_eq!(args.depth, 100);
                assert_eq!(args.dollar_depth, 50000.0);
            }
            _ => panic!("expected study vamp command"),
        }
    }

    #[test]
    fn parse_source_orderbook_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "orderbook",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--depth",
            "100",
            "--stream",
            "--interval-ms",
            "500",
            "--min-size",
            "0.1",
            "--price-group",
            "1",
        ])
        .expect("source orderbook parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                assert!(matches!(args.provider, CliProviderKind::Mmt));
                assert!(args.stream);
                assert_eq!(args.interval_ms, 500);
            }
            _ => panic!("expected source orderbook command"),
        }
    }

    #[test]
    fn parse_strategy_sma_crossover_window_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "strategy",
            "backtest",
            "sma-crossover",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
            "--fast",
            "20",
            "--slow",
            "50",
        ])
        .expect("strategy parse should succeed");

        match cli.command {
            Commands::Strategy {
                command:
                    StrategyCommands::Backtest {
                        command: StrategyBacktestCommands::SmaCrossover(args),
                    },
            } => {
                assert_eq!(args.from, 1704067200);
                assert_eq!(args.to, 1704067800);
            }
            _ => panic!("expected strategy backtest sma-crossover command"),
        }
    }

    #[test]
    fn parse_strategy_run_with_from_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "strategy",
            "run",
            "sma-crossover",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200",
        ])
        .expect("strategy run parse should succeed");

        match cli.command {
            Commands::Strategy {
                command:
                    StrategyCommands::Run {
                        command: StrategyRunCommands::SmaCrossover(args),
                    },
            } => {
                args.validate().expect("validate should succeed");
                assert_eq!(args.from, Some(1704067200));
            }
            _ => panic!("expected strategy run sma-crossover command"),
        }
    }

    #[test]
    fn reject_strategy_run_with_to() {
        let err = Cli::try_parse_from([
            "market-lab",
            "strategy",
            "run",
            "sma-crossover",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--to",
            "1704067800",
        ])
        .expect_err("strategy run parse should fail");
        assert!(err.to_string().contains("--to"));
    }

    #[test]
    fn parse_script_run_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "./studies/buy-pressure.js",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--stream",
            "--input",
            "min_vbuy=50000",
            "--output",
            "json",
        ])
        .expect("script run parse should succeed");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => {
                assert_eq!(args.script, "./studies/buy-pressure.js");
                assert_eq!(args.exchange.as_deref(), Some("bybitf"));
                assert_eq!(args.symbol.as_deref(), Some("BTC/USDT"));
                assert_eq!(args.timeframe, Some(60));
                assert!(args.stream);
                assert_eq!(args.input, vec!["min_vbuy=50000"]);
                args.validate().expect("validate should succeed");
            }
            _ => panic!("expected script run command"),
        }
    }

    #[test]
    fn parse_script_run_without_source_flags() {
        let cli = Cli::try_parse_from(["mlab", "script", "run", "test/buy-pressure.js"])
            .expect("script run should parse before source-specific validation");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => {
                assert_eq!(args.script, "test/buy-pressure.js");
                assert!(args.exchange.is_none());
                assert!(args.symbol.is_none());
                assert!(args.timeframe.is_none());
                assert!(args.from.is_none());
                assert!(args.to.is_none());
                args.validate().expect("base validate should succeed");
            }
            _ => panic!("expected script run command"),
        }
    }

    #[test]
    fn parse_script_backtest_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "script",
            "backtest",
            "./scripts/sma-cross.js",
            "--provider",
            "mmt",
            "--exchange",
            "bybitf",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--input",
            "fast=20",
            "--output",
            "json",
        ])
        .expect("script backtest parse should succeed");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => {
                assert_eq!(args.script, "./scripts/sma-cross.js");
                assert_eq!(args.input, vec!["fast=20"]);
                args.validate().expect("validate should succeed");
            }
            _ => panic!("expected script backtest command"),
        }
    }

    #[test]
    fn parse_script_runs_command() {
        let cli = Cli::try_parse_from([
            "mlab", "script", "runs", "list", "--limit", "10", "--output", "json",
        ])
        .expect("script runs list parse should succeed");

        match cli.command {
            Commands::Script {
                command:
                    ScriptCommands::Runs {
                        command: ScriptRunHistoryCommands::List(args),
                    },
            } => {
                assert_eq!(args.limit, 10);
                assert!(matches!(args.output, OutputFormat::Json));
                args.validate().expect("validate should succeed");
            }
            _ => panic!("expected script runs list command"),
        }
    }

    #[test]
    fn parse_script_show_command() {
        let cli = Cli::try_parse_from(["mlab", "script", "runs", "show", "1780-script-run-test"])
            .expect("script runs show parse should succeed");

        match cli.command {
            Commands::Script {
                command:
                    ScriptCommands::Runs {
                        command: ScriptRunHistoryCommands::Show(args),
                    },
            } => {
                assert_eq!(args.run, "1780-script-run-test");
                args.validate().expect("validate should succeed");
            }
            _ => panic!("expected script runs show command"),
        }
    }
}
