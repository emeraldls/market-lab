use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::domain::enums::{BookMode, ProviderKind, Side};
use crate::domain::execution::{ExecutionVenue, OrderKind, TimeInForce};
use crate::domain::requests::{
    DepthRequest, ImbalanceRequest, InspectRequest, ReplayRequest, SlippageRequest, SpreadRequest,
    VampRequest,
};

#[derive(Parser, Debug)]
#[command(name = "mlab")]
#[command(version, about = "Deterministic market replay CLI", long_about = None)]
#[command(args_override_self = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Markets(MarketsArgs),
    Trade {
        #[command(subcommand)]
        command: TradeCommands,
    },
    Positions(AccountQueryArgs),
    Orders(AccountQueryArgs),
    Fills(AccountQueryArgs),
    Cancel(CancelOrderArgs),
    Close(ClosePositionArgs),
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
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
    Bot {
        #[command(subcommand)]
        command: BotCommands,
    },
    Health(HealthArgs),
    Status(StatusArgs),
    Upgrade(UpgradeArgs),
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
}

#[derive(Subcommand, Debug)]
pub enum TradeCommands {
    #[command(alias = "buy")]
    Long(TradeArgs),
    #[command(alias = "sell")]
    Short(TradeArgs),
}

#[derive(Subcommand, Debug)]
pub enum DaemonCommands {
    Start(DaemonOutputArgs),
    Status(DaemonOutputArgs),
    Stop(DaemonOutputArgs),
    Events(DaemonEventsArgs),
}

#[derive(Clone, Debug, Args)]
pub struct DaemonOutputArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct DaemonEventsArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct TradeArgs {
    pub symbol: String,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    /// Exact base-asset exposure; leverage does not multiply an explicit size.
    #[arg(long, conflicts_with = "margin", required_unless_present = "margin")]
    pub size: Option<f64>,
    /// Quote collateral to commit; exchange exposure is margin multiplied by leverage.
    #[arg(long, conflicts_with = "size", required_unless_present = "size")]
    pub margin: Option<f64>,
    #[arg(long = "type", value_enum, default_value_t = TradeOrderKind::Market)]
    pub order_kind: TradeOrderKind,
    #[arg(long)]
    pub price: Option<f64>,
    #[arg(long, value_enum, default_value_t = TradeTimeInForce::Gtc)]
    pub tif: TradeTimeInForce,
    /// Exposure multiplier for margin sizing and the leverage setting sent to BULK.
    #[arg(long, default_value_t = 1.0)]
    pub leverage: f64,
    #[arg(long, default_value_t = false)]
    pub reduce_only: bool,
    /// Native stop-loss trigger price attached after the entry first fills.
    #[arg(long)]
    pub sl: Option<f64>,
    /// Native take-profit trigger price attached after the entry first fills.
    #[arg(long)]
    pub tp: Option<f64>,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl TradeArgs {
    pub fn validate_shape(&self) -> Result<()> {
        if !is_valid_symbol(&self.symbol) {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if let Some(size) = self.size
            && (!size.is_finite() || size <= 0.0)
        {
            bail!("--size must be > 0");
        }
        if let Some(margin) = self.margin
            && (!margin.is_finite() || margin <= 0.0)
        {
            bail!("--margin must be > 0");
        }
        match (self.size, self.margin) {
            (Some(_), Some(_)) => bail!("set only one of --size or --margin"),
            (None, None) => bail!("one of --size or --margin is required"),
            _ => {}
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("--leverage must be at least 1");
        }
        if self
            .margin
            .is_some_and(|margin| !(margin * self.leverage).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        for (flag, price) in [("--sl", self.sl), ("--tp", self.tp)] {
            if price.is_some_and(|price| !price.is_finite() || price <= 0.0) {
                bail!("{flag} must be > 0");
            }
        }
        if self.sl.is_some() || self.tp.is_some() {
            if self.reduce_only {
                bail!("--sl/--tp cannot be attached to a reduce-only order");
            }
            if self.sl == self.tp {
                bail!("--sl and --tp must use different prices");
            }
        }
        match self.order_kind {
            TradeOrderKind::Market if self.price.is_some() => {
                bail!("--price is only valid with --type limit")
            }
            TradeOrderKind::Market if self.tif != TradeTimeInForce::Gtc => {
                bail!("--tif is only valid with --type limit")
            }
            TradeOrderKind::Limit => {
                let price = self
                    .price
                    .context("--price is required with --type limit")?;
                if !price.is_finite() || price <= 0.0 {
                    bail!("--price must be > 0");
                }
            }
            TradeOrderKind::Market => {}
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("trade supports only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct AccountQueryArgs {
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct CancelOrderArgs {
    pub symbol: String,
    pub order_id: String,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl CancelOrderArgs {
    pub fn validate(&self) -> Result<()> {
        if !is_valid_symbol(&self.symbol) {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.order_id.trim().is_empty() {
            bail!("order id cannot be empty");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("cancel supports only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct ClosePositionArgs {
    pub symbol: Option<String>,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ClosePositionArgs {
    pub fn validate(&self) -> Result<()> {
        if let Some(symbol) = &self.symbol
            && !is_valid_symbol(symbol)
        {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("close supports only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

impl AccountQueryArgs {
    pub fn validate(&self) -> Result<()> {
        if let Some(symbol) = &self.symbol
            && !is_valid_symbol(symbol)
        {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("account queries support only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ExecutionVenueArg {
    Bulk,
}

impl From<ExecutionVenueArg> for ExecutionVenue {
    fn from(value: ExecutionVenueArg) -> Self {
        match value {
            ExecutionVenueArg::Bulk => ExecutionVenue::Bulk,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum TradeOrderKind {
    Market,
    Limit,
}

impl From<TradeOrderKind> for OrderKind {
    fn from(value: TradeOrderKind) -> Self {
        match value {
            TradeOrderKind::Market => OrderKind::Market,
            TradeOrderKind::Limit => OrderKind::Limit,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum TradeTimeInForce {
    Gtc,
    Ioc,
    Alo,
}

impl From<TradeTimeInForce> for TimeInForce {
    fn from(value: TradeTimeInForce) -> Self {
        match value {
            TradeTimeInForce::Gtc => TimeInForce::Gtc,
            TradeTimeInForce::Ioc => TimeInForce::Ioc,
            TradeTimeInForce::Alo => TimeInForce::Alo,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct MarketsArgs {
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: Option<String>,
    /// Replace the installed snapshot with current provider markets.
    #[arg(long, default_value_t = false)]
    pub refresh: bool,
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

impl MarketsArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        Ok(())
    }
}

#[derive(Subcommand, Debug)]
pub enum AuthCommands {
    Set(AuthSetArgs),
    Status,
    Remove(AuthProviderArgs),
}

#[derive(Clone, Debug, Args)]
pub struct AuthSetArgs {
    #[arg(value_enum)]
    pub provider: AuthProvider,
    /// Reauthorize the existing remote credential without replacing it locally first.
    #[arg(long, default_value_t = false)]
    pub reauthorize: bool,
}

#[derive(Clone, Debug, Args)]
pub struct AuthProviderArgs {
    #[arg(value_enum)]
    pub provider: AuthProvider,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AuthProvider {
    Mmt,
    Bulk,
}

#[derive(Subcommand, Debug)]
pub enum SourceCommands {
    Orderbook(SourceOrderbookArgs),
    Vd(SourceVdArgs),
    Candles(SourceCandlesArgs),
    Oi(SourceOiArgs),
    Volumes(SourceVolumesArgs),
    Stats(SourceStatsArgs),
    Funding(SourceFundingArgs),
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
    Jobs(ScriptJobsArgs),
    Status(ScriptJobArgs),
    Logs(ScriptLogsArgs),
    Stop(ScriptJobArgs),
    Restart(ScriptJobArgs),
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
    Jobs(StrategyJobsArgs),
    Status(StrategyJobArgs),
    Logs(StrategyLogsArgs),
    Stop(StrategyJobArgs),
}

#[derive(Subcommand, Debug)]
pub enum StrategyRunCommands {
    Twap(RunTwapArgs),
    Vwap(RunVwapArgs),
    Oiwap(RunOiwapArgs),
}

#[derive(Subcommand, Debug)]
pub enum BotCommands {
    Run {
        #[command(subcommand)]
        command: BotRunCommands,
    },
    Jobs(BotJobsArgs),
    Status(BotJobArgs),
    Logs(BotLogsArgs),
    Stop(BotJobArgs),
}

#[derive(Subcommand, Debug)]
pub enum BotRunCommands {
    MidPrice(RunMidPriceArgs),
    VolumeMid(RunVolumeMidArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ScriptRunArgs {
    pub script: String,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub symbol: Option<String>,
    /// Arms live execution for ctx.trade/ctx.cancel while data may come from any provider.
    #[arg(long, value_enum)]
    pub venue: Option<ExecutionVenueArg>,
    #[arg(long)]
    pub from: Option<u64>,
    #[arg(long)]
    pub to: Option<u64>,
    #[arg(long = "source")]
    pub source: Vec<String>,
    #[arg(long = "param")]
    pub param: Vec<String>,
    /// Maximum live runtime in seconds. Omit to run until manually stopped.
    #[arg(long)]
    pub duration: Option<u64>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

#[derive(Clone, Debug, Args)]
pub struct ScriptJobsArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct ScriptJobArgs {
    pub job: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ScriptJobArgs {
    pub fn validate(&self) -> Result<()> {
        if self.job.trim().is_empty() {
            bail!("script job id is required");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("script job commands support only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct ScriptLogsArgs {
    pub job: String,
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
    #[arg(long, default_value_t = false)]
    pub follow: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ScriptLogsArgs {
    pub fn validate(&self) -> Result<()> {
        if self.job.trim().is_empty() {
            bail!("script job id is required");
        }
        if self.limit == 0 {
            bail!("--limit must be >= 1");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("script logs supports only --output terminal|json|jsonl");
        }
        if self.follow && matches!(self.output, OutputFormat::Json) {
            bail!("--follow supports terminal or jsonl output");
        }
        Ok(())
    }
}

impl ScriptRunArgs {
    pub fn validate(&self) -> Result<()> {
        if self.script.trim().is_empty() {
            bail!("script path is required");
        }
        if let Some(from) = self.from {
            validate_millisecond_timestamp(from, "--from")?;
        }
        if let Some(to) = self.to {
            validate_millisecond_timestamp(to, "--to")?;
        }
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from >= to
        {
            bail!("--from must be less than --to");
        }
        if self.duration == Some(0) {
            bail!("--duration must be at least 1 second");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct ScriptBacktestArgs {
    pub script: String,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub from: u64,
    #[arg(long)]
    pub to: u64,
    #[arg(long = "source")]
    pub source: Vec<String>,
    #[arg(long = "param")]
    pub param: Vec<String>,
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
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        validate_millisecond_timestamp(self.from, "--from")?;
        validate_millisecond_timestamp(self.to, "--to")?;
        if self.from >= self.to {
            bail!("--from must be less than --to");
        }
        Ok(())
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: Option<u32>,
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
        let provider = validate_source_identity(self.provider, &self.exchange, &self.symbol)?;
        if provider == CliProviderKind::Bulk {
            if !self.stream {
                bail!("BULK volume delta is derived from live trades and requires --stream");
            }
            if self.timeframe.is_some() || self.from.is_some() || self.to.is_some() {
                bail!("BULK live volume delta does not use --timeframe/--from/--to");
            }
        } else {
            mmt_timeframe_from_seconds(
                self.timeframe.ok_or_else(|| {
                    anyhow::anyhow!("--timeframe is required for MMT volume delta")
                })?,
            )?;
        }
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
            validate_millisecond_timestamp(from, "--from")?;
            validate_millisecond_timestamp(to, "--to")?;
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
        mmt_timeframe_from_seconds(
            self.timeframe
                .ok_or_else(|| anyhow::anyhow!("--timeframe is required for MMT volume delta"))?,
        )
    }

    pub fn exchange_name(&self) -> Result<&str> {
        Ok(&self.exchange)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(self.provider, &self.exchange)
    }
}

#[derive(Clone, Debug, Args)]
pub struct CvdArgs {
    #[arg(long, value_enum, default_value_t = CliDataProvider::Mmt)]
    pub provider: CliDataProvider,
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        TimeframeSourceValidation {
            provider: self.provider_kind()?,
            exchange: &self.exchange,
            symbol: &self.symbol,
            timeframe: self.timeframe,
            from: self.from,
            to: self.to,
            stream: self.stream,
            buffer_size: self.buffer_size,
            interval_ms: self.interval_ms,
        }
        .validate()
    }

    pub fn timeframe_name(&self) -> Result<&'static str> {
        provider_timeframe_from_seconds(self.provider_kind()?, self.timeframe)
    }

    pub fn exchange_name(&self) -> Result<&str> {
        Ok(&self.exchange)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(self.provider, &self.exchange)
    }
}

#[derive(Clone, Debug, Args)]
pub struct SourceOiArgs {
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long)]
    pub timeframe: Option<u32>,
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

impl SourceOiArgs {
    pub fn validate(&self) -> Result<()> {
        let provider = validate_source_identity(self.provider, &self.exchange, &self.symbol)?;
        if !crate::markets::is_futures_exchange(&self.exchange)? {
            bail!(
                "open interest requires a futures exchange; `{}` is spot",
                self.exchange
            );
        }
        if provider == CliProviderKind::Bulk {
            if self.timeframe.is_some() || self.from.is_some() || self.to.is_some() {
                bail!("BULK open interest is current/live only; omit --timeframe/--from/--to");
            }
        } else {
            let timeframe = self
                .timeframe
                .ok_or_else(|| anyhow::anyhow!("--timeframe is required for MMT open interest"))?;
            TimeframeSourceValidation {
                provider,
                exchange: &self.exchange,
                symbol: &self.symbol,
                timeframe,
                from: self.from,
                to: self.to,
                stream: self.stream,
                buffer_size: self.buffer_size,
                interval_ms: self.interval_ms,
            }
            .validate()?;
        }
        validate_stream_controls(self.buffer_size, self.interval_ms)
    }

    pub fn mmt_tf(&self) -> Result<&'static str> {
        mmt_timeframe_from_seconds(
            self.timeframe
                .ok_or_else(|| anyhow::anyhow!("--timeframe is required for MMT open interest"))?,
        )
    }

    pub fn exchange_name(&self) -> Result<&str> {
        Ok(&self.exchange)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(self.provider, &self.exchange)
    }
}

#[derive(Clone, Debug, Args)]
pub struct SourceVolumesArgs {
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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

impl SourceVolumesArgs {
    pub fn validate(&self) -> Result<()> {
        TimeframeSourceValidation {
            provider: self.provider_kind()?,
            exchange: &self.exchange,
            symbol: &self.symbol,
            timeframe: self.timeframe,
            from: self.from,
            to: self.to,
            stream: self.stream,
            buffer_size: self.buffer_size,
            interval_ms: self.interval_ms,
        }
        .validate()
    }

    pub fn timeframe_name(&self) -> Result<&'static str> {
        provider_timeframe_from_seconds(self.provider_kind()?, self.timeframe)
    }

    pub fn exchange_name(&self) -> Result<&str> {
        Ok(&self.exchange)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(self.provider, &self.exchange)
    }
}

struct TimeframeSourceValidation<'a> {
    provider: CliProviderKind,
    exchange: &'a str,
    symbol: &'a str,
    timeframe: u32,
    from: Option<u64>,
    to: Option<u64>,
    stream: bool,
    buffer_size: u16,
    interval_ms: u64,
}

impl TimeframeSourceValidation<'_> {
    fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        provider_timeframe_from_seconds(self.provider, self.timeframe)?;
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
            validate_millisecond_timestamp(from, "--from")?;
            validate_millisecond_timestamp(to, "--to")?;
            if from >= to {
                bail!("--from must be less than --to");
            }
        }
        validate_stream_controls(self.buffer_size, self.interval_ms)
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
            validate_millisecond_timestamp(from, "--from")?;
            validate_millisecond_timestamp(to, "--to")?;
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        validate_source_identity(self.provider, &self.exchange, &self.symbol)?;
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

    pub fn exchange_name(&self) -> Result<&str> {
        Ok(&self.exchange)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(self.provider, &self.exchange)
    }
}

#[derive(Clone, Debug, Args)]
pub struct SourceStatsArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: Option<String>,
    #[arg(long, default_value = "1d")]
    pub period: String,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl SourceStatsArgs {
    pub fn validate(&self) -> Result<()> {
        validate_bulk_exchange(&self.exchange, "source stats")?;
        if let Some(symbol) = &self.symbol
            && !is_valid_symbol(symbol)
        {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if !matches!(
            self.period.as_str(),
            "1d" | "7d" | "30d" | "90d" | "1y" | "all"
        ) {
            bail!("--period must be one of 1d,7d,30d,90d,1y,all");
        }
        if self.stream && self.symbol.is_none() {
            bail!("--symbol is required when streaming BULK statistics");
        }
        if self.stream && matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        validate_stream_controls(self.buffer_size, self.interval_ms)
    }
}

#[derive(Clone, Debug, Args)]
pub struct SourceFundingArgs {
    #[arg(long)]
    pub exchange: String,
    #[arg(long)]
    pub symbol: String,
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    #[arg(long, default_value_t = 50)]
    pub buffer_size: u16,
    #[arg(long, default_value_t = 1000)]
    pub interval_ms: u64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl SourceFundingArgs {
    pub fn validate(&self) -> Result<()> {
        validate_bulk_exchange(&self.exchange, "source funding")?;
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self.stream && matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        validate_stream_controls(self.buffer_size, self.interval_ms)
    }
}

#[derive(Clone, Debug, Args)]
pub struct HealthArgs {
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
    #[arg(long)]
    pub exchange: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct StatusArgs {
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
    #[arg(long)]
    pub exchange: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl HealthArgs {
    pub fn provider_kind(&self) -> Result<ProviderKind> {
        resolve_system_provider(self.provider, self.exchange.as_deref())
    }
}

impl StatusArgs {
    pub fn provider_kind(&self) -> Result<ProviderKind> {
        resolve_system_provider(self.provider, self.exchange.as_deref())
    }
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        resolve_market_provider(self.provider, &self.exchange)?;
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        validate_millisecond_timestamp(self.at, "--at")?;
        if self.depth == 0 {
            bail!("--depth must be >= 1");
        }
        Ok(())
    }

    pub fn to_request(&self) -> InspectRequest {
        InspectRequest {
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        resolve_market_provider(self.provider, &self.exchange)?;
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        if !is_valid_symbol(&self.symbol) {
            bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        validate_millisecond_timestamp(self.from, "--from")?;
        validate_millisecond_timestamp(self.to, "--to")?;
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
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        resolve_market_provider(self.provider, &self.exchange)?;
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
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        resolve_market_provider(self.provider, &self.exchange)?;
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
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
        resolve_market_provider(self.provider, &self.exchange)?;
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
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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
    #[arg(long, value_enum)]
    pub provider: Option<CliDataProvider>,
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
pub struct RunTwapArgs {
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    #[arg(long, value_enum)]
    pub side: CliSide,
    /// Exact total base-asset exposure; leverage does not multiply an explicit size.
    #[arg(long, conflicts_with = "margin", required_unless_present = "margin")]
    pub size: Option<f64>,
    /// Total quote collateral for the TWAP; exposure is margin multiplied by leverage.
    #[arg(long, conflicts_with = "size", required_unless_present = "size")]
    pub margin: Option<f64>,
    /// Total execution window in seconds.
    #[arg(long)]
    pub duration: u64,
    /// Seconds between child orders.
    #[arg(long, default_value_t = 60)]
    pub interval: u64,
    /// Exposure multiplier for margin sizing and the leverage setting sent to BULK.
    #[arg(long, default_value_t = 1.0)]
    pub leverage: f64,
    #[arg(long, default_value_t = false)]
    pub reduce_only: bool,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct RunVwapArgs {
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    #[arg(long, value_enum)]
    pub side: CliSide,
    /// Exact total base-asset exposure; leverage does not multiply an explicit size.
    #[arg(long, conflicts_with = "margin", required_unless_present = "margin")]
    pub size: Option<f64>,
    /// Total quote collateral; exposure is margin multiplied by leverage.
    #[arg(long, conflicts_with = "size", required_unless_present = "size")]
    pub margin: Option<f64>,
    /// Total execution window in seconds. VWAP requires at least one minute.
    #[arg(long)]
    pub duration: u64,
    /// Comma-separated volume venues, for example binancef@mmt,okxf@mmt,bulk.
    #[arg(long, value_delimiter = ',')]
    pub volume_sources: Vec<String>,
    /// Exposure multiplier for margin sizing and the leverage setting sent to BULK.
    #[arg(long, default_value_t = 1.0)]
    pub leverage: f64,
    #[arg(long, default_value_t = false)]
    pub reduce_only: bool,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct RunOiwapArgs {
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    #[arg(long, value_enum)]
    pub side: CliSide,
    /// Exact total base-asset exposure; leverage does not multiply an explicit size.
    #[arg(long, conflicts_with = "margin", required_unless_present = "margin")]
    pub size: Option<f64>,
    /// Total quote collateral; exposure is margin multiplied by leverage.
    #[arg(long, conflicts_with = "size", required_unless_present = "size")]
    pub margin: Option<f64>,
    /// Total execution window in seconds. OIWAP requires at least one minute.
    #[arg(long)]
    pub duration: u64,
    /// Comma-separated normalized MMT OI venues, for example binancef@mmt,bybitf@mmt.
    #[arg(long, value_delimiter = ',')]
    pub oi_sources: Vec<String>,
    /// Exposure multiplier for margin sizing and the leverage setting sent to the venue.
    #[arg(long, default_value_t = 1.0)]
    pub leverage: f64,
    #[arg(long, default_value_t = false)]
    pub reduce_only: bool,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct RunMidPriceArgs {
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    /// Hard one-sided inventory limit in base-asset units.
    #[arg(long, conflicts_with = "margin", required_unless_present = "margin")]
    pub size: Option<f64>,
    /// Collateral allocated to the one-sided inventory limit.
    #[arg(long, conflicts_with = "size", required_unless_present = "size")]
    pub margin: Option<f64>,
    /// Maximum bot runtime in seconds.
    #[arg(long)]
    pub duration: u64,
    /// Total distance between bid and ask around the current midpoint.
    #[arg(long, default_value_t = 2.0)]
    pub spread_bps: f64,
    /// Percentage size bias: -100 favors asks, +100 favors bids, 0 is neutral.
    #[arg(long = "directional-bias", alias = "bias", default_value_t = 0.0)]
    pub directional_bias: f64,
    #[arg(long, default_value_t = 1.0)]
    pub leverage: f64,
    /// Stop after net bot PnL loses this percentage of allocated margin. Zero disables it.
    #[arg(long)]
    pub stop_loss_pct: Option<f64>,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct RunVolumeMidArgs {
    #[command(flatten)]
    pub common: RunMidPriceArgs,
    /// Minimum lifetime of each working quote, in seconds.
    #[arg(long)]
    pub refresh_time: f64,
    /// Price drift allowed before a quote moving away from the market is replaced.
    #[arg(long)]
    pub refresh_tolerance_bps: f64,
}

#[derive(Clone, Debug, Args)]
pub struct StrategyJobsArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct StrategyJobArgs {
    pub job: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl StrategyJobArgs {
    pub fn validate(&self) -> Result<()> {
        if self.job.trim().is_empty() {
            bail!("strategy job id is required");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("strategy job commands support only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct StrategyLogsArgs {
    pub job: String,
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
    #[arg(long, default_value_t = false)]
    pub follow: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct BotJobsArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Debug, Args)]
pub struct BotJobArgs {
    pub job: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl BotJobArgs {
    pub fn validate(&self) -> Result<()> {
        if self.job.trim().is_empty() {
            bail!("bot job id is required");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("bot job commands support only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct BotLogsArgs {
    pub job: String,
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
    #[arg(long, default_value_t = false)]
    pub follow: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl BotLogsArgs {
    pub fn validate(&self) -> Result<()> {
        if self.job.trim().is_empty() {
            bail!("bot job id is required");
        }
        if self.limit == 0 {
            bail!("--limit must be >= 1");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("bot logs supports only --output terminal|json|jsonl");
        }
        if self.follow && matches!(self.output, OutputFormat::Json) {
            bail!("--follow supports terminal or jsonl output");
        }
        Ok(())
    }
}

impl StrategyLogsArgs {
    pub fn validate(&self) -> Result<()> {
        if self.job.trim().is_empty() {
            bail!("strategy job id is required");
        }
        if self.limit == 0 {
            bail!("--limit must be >= 1");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("strategy logs supports only --output terminal|json|jsonl");
        }
        if self.follow && matches!(self.output, OutputFormat::Json) {
            bail!("--follow supports terminal or jsonl output");
        }
        Ok(())
    }
}

impl RunTwapArgs {
    pub fn validate(&self) -> Result<()> {
        if !is_valid_symbol(&self.symbol) {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self
            .size
            .is_some_and(|size| !size.is_finite() || size <= 0.0)
        {
            bail!("--size must be > 0");
        }
        if self
            .margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("--margin must be > 0");
        }
        match (self.size, self.margin) {
            (Some(_), Some(_)) => bail!("set only one of --size or --margin"),
            (None, None) => bail!("one of --size or --margin is required"),
            _ => {}
        }
        if self.duration == 0 {
            bail!("--duration must be >= 1 second");
        }
        if self.interval == 0 {
            bail!("--interval must be >= 1 second");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("--leverage must be at least 1");
        }
        if self
            .margin
            .is_some_and(|margin| !(margin * self.leverage).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("strategy run supports only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

impl RunVwapArgs {
    pub fn validate(&self) -> Result<()> {
        if !is_valid_symbol(&self.symbol) {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self
            .size
            .is_some_and(|size| !size.is_finite() || size <= 0.0)
        {
            bail!("--size must be > 0");
        }
        if self
            .margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("--margin must be > 0");
        }
        match (self.size, self.margin) {
            (Some(_), Some(_)) => bail!("set only one of --size or --margin"),
            (None, None) => bail!("one of --size or --margin is required"),
            _ => {}
        }
        if self.duration < 60 {
            bail!("--duration must be at least 60 seconds for VWAP");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("--leverage must be at least 1");
        }
        if self
            .margin
            .is_some_and(|margin| !(margin * self.leverage).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("strategy run supports only --output terminal|json|jsonl");
        }
        let execution_venue = match self.venue {
            ExecutionVenueArg::Bulk => "bulk",
        };
        crate::strategies::vwap::VolumeSourceSelector::parse(
            &self.volume_sources,
            execution_venue,
            &self.symbol,
        )?;
        Ok(())
    }
}

impl RunOiwapArgs {
    pub fn validate(&self) -> Result<()> {
        if !is_valid_symbol(&self.symbol) {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self
            .size
            .is_some_and(|size| !size.is_finite() || size <= 0.0)
        {
            bail!("--size must be > 0");
        }
        if self
            .margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("--margin must be > 0");
        }
        match (self.size, self.margin) {
            (Some(_), Some(_)) => bail!("set only one of --size or --margin"),
            (None, None) => bail!("one of --size or --margin is required"),
            _ => {}
        }
        if self.duration < 60 {
            bail!("--duration must be at least 60 seconds for OIWAP");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("--leverage must be at least 1");
        }
        if self
            .margin
            .is_some_and(|margin| !(margin * self.leverage).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("strategy run supports only --output terminal|json|jsonl");
        }
        crate::strategies::oiwap::OpenInterestSourceSelector::parse(
            &self.oi_sources,
            &self.symbol,
        )?;
        Ok(())
    }
}

impl RunMidPriceArgs {
    pub fn validate(&self) -> Result<()> {
        if !is_valid_symbol(&self.symbol) {
            bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT");
        }
        if self
            .size
            .is_some_and(|size| !size.is_finite() || size <= 0.0)
        {
            bail!("--size must be > 0");
        }
        if self
            .margin
            .is_some_and(|margin| !margin.is_finite() || margin <= 0.0)
        {
            bail!("--margin must be > 0");
        }
        match (self.size, self.margin) {
            (Some(_), Some(_)) => bail!("set only one of --size or --margin"),
            (None, None) => bail!("one of --size or --margin is required"),
            _ => {}
        }
        if self.duration == 0 {
            bail!("--duration must be >= 1 second");
        }
        if !self.spread_bps.is_finite() || self.spread_bps < 0.0 {
            bail!("--spread-bps must be zero or greater");
        }
        if !self.directional_bias.is_finite() || !(-100.0..=100.0).contains(&self.directional_bias)
        {
            bail!("--directional-bias must be between -100 and 100 percent");
        }
        if !self.leverage.is_finite() || self.leverage < 1.0 {
            bail!("--leverage must be at least 1");
        }
        if self
            .stop_loss_pct
            .is_some_and(|percent| !percent.is_finite() || !(0.0..=100.0).contains(&percent))
        {
            bail!("--stop-loss-pct must be between 0 and 100 percent");
        }
        if self
            .margin
            .is_some_and(|margin| !(margin * self.leverage).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("bot run supports only --output terminal|json|jsonl");
        }
        Ok(())
    }
}

impl RunVolumeMidArgs {
    pub fn validate(&self) -> Result<()> {
        self.common.validate()?;
        if !self.refresh_time.is_finite() || self.refresh_time <= 0.0 {
            bail!("--refresh-time must be greater than zero seconds");
        }
        if !self.refresh_tolerance_bps.is_finite() || self.refresh_tolerance_bps < 0.0 {
            bail!("--refresh-tolerance-bps must be zero or greater");
        }
        Ok(())
    }
}

impl DepthArgs {
    pub fn validate(&self) -> Result<()> {
        resolve_market_provider(self.provider, &self.exchange)?;
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
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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
        resolve_market_provider(self.provider, &self.exchange)?;
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
            provider: resolve_market_provider(self.provider, &self.exchange)
                .expect("validated market provider"),
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

fn validate_source_identity(
    provider: Option<CliDataProvider>,
    exchange: &str,
    symbol: &str,
) -> Result<CliProviderKind> {
    let provider = resolve_source_provider(provider, exchange)?;
    if !is_valid_symbol(symbol) {
        bail!("--symbol must look like BASE/QUOTE, e.g. BTC/USDT");
    }
    Ok(provider)
}

fn resolve_source_provider(
    provider: Option<CliDataProvider>,
    exchange: &str,
) -> Result<CliProviderKind> {
    if exchange.trim().is_empty() {
        bail!("--exchange cannot be empty");
    }
    if provider.is_some() {
        if exchange.eq_ignore_ascii_case("bulk") {
            bail!("omit --provider for the standalone `bulk` exchange");
        }
        return Ok(CliProviderKind::Mmt);
    }
    if exchange.eq_ignore_ascii_case("bulk") {
        return Ok(CliProviderKind::Bulk);
    }
    bail!(
        "standalone exchange `{exchange}` is not supported yet; use --provider mmt when `{exchange}` is routed through MMT"
    )
}

fn resolve_market_provider(
    provider: Option<CliDataProvider>,
    exchange: &str,
) -> Result<ProviderKind> {
    resolve_source_provider(provider, exchange).map(Into::into)
}

fn resolve_system_provider(
    provider: Option<CliDataProvider>,
    exchange: Option<&str>,
) -> Result<ProviderKind> {
    match (provider, exchange) {
        (Some(_), Some(exchange)) if exchange.eq_ignore_ascii_case("bulk") => {
            bail!("omit --provider for the standalone `bulk` exchange")
        }
        (Some(_), _) => Ok(ProviderKind::Mmt),
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("bulk") => Ok(ProviderKind::Bulk),
        (None, Some(exchange)) => bail!("unsupported standalone exchange `{exchange}`"),
        (None, None) => Ok(ProviderKind::MarketLab),
    }
}

fn validate_bulk_exchange(exchange: &str, command: &str) -> Result<()> {
    if !exchange.eq_ignore_ascii_case("bulk") {
        bail!("{command} currently supports only --exchange bulk");
    }
    Ok(())
}

fn provider_timeframe_from_seconds(
    provider: CliProviderKind,
    seconds: u32,
) -> Result<&'static str> {
    match provider {
        CliProviderKind::Bulk => {
            crate::providers::bulk::market_data::timeframe_from_seconds(seconds)
        }
        CliProviderKind::Mmt | CliProviderKind::MarketLab => mmt_timeframe_from_seconds(seconds),
    }
}

fn validate_stream_controls(buffer_size: u16, interval_ms: u64) -> Result<()> {
    if buffer_size == 0 {
        bail!("--buffer-size must be >= 1");
    }
    if interval_ms == 0 {
        bail!("--interval-ms must be >= 1");
    }
    Ok(())
}

fn validate_millisecond_timestamp(timestamp: u64, flag: &str) -> Result<()> {
    if !(10_000_000_000..10_000_000_000_000).contains(&timestamp) {
        bail!("{flag} must be a millisecond timestamp");
    }
    Ok(())
}

pub(crate) fn mmt_timeframe_from_seconds(seconds: u32) -> Result<&'static str> {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum CliDataProvider {
    Mmt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CliProviderKind {
    MarketLab,
    Mmt,
    Bulk,
}

impl From<CliDataProvider> for CliProviderKind {
    fn from(value: CliDataProvider) -> Self {
        match value {
            CliDataProvider::Mmt => Self::Mmt,
        }
    }
}

impl From<CliProviderKind> for ProviderKind {
    fn from(value: CliProviderKind) -> Self {
        match value {
            CliProviderKind::MarketLab => ProviderKind::MarketLab,
            CliProviderKind::Mmt => ProviderKind::Mmt,
            CliProviderKind::Bulk => ProviderKind::Bulk,
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
    fn parse_bulk_markets_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "markets",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
            "--json",
        ])
        .expect("markets command should parse");

        match cli.command {
            Commands::Markets(args) => {
                assert!(args.provider.is_none());
                assert_eq!(args.exchange, "bulk");
                assert_eq!(args.symbol.as_deref(), Some("BTC/USDT"));
                assert!(!args.refresh);
                assert!(args.json);
                args.validate().expect("BULK markets should validate");
            }
            _ => panic!("expected markets command"),
        }
    }

    #[test]
    fn parse_markets_refresh_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "markets",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--refresh",
        ])
        .expect("markets refresh command should parse");

        match cli.command {
            Commands::Markets(args) => {
                assert_eq!(args.provider, Some(CliDataProvider::Mmt));
                assert_eq!(args.exchange, "binancef");
                assert!(args.refresh);
            }
            _ => panic!("expected markets command"),
        }
    }

    #[test]
    fn parse_mmt_markets_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "markets",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
        ])
        .expect("MMT markets command should parse");

        match cli.command {
            Commands::Markets(args) => {
                assert_eq!(args.provider, Some(CliDataProvider::Mmt));
                assert_eq!(args.exchange, "binancef");
                args.validate().expect("MMT snapshot should validate");
            }
            _ => panic!("expected markets command"),
        }
    }

    #[test]
    fn parse_trade_long_dry_run() {
        let cli = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "BTC/USDT",
            "--venue",
            "bulk",
            "--size",
            "0.001",
            "--type",
            "limit",
            "--price",
            "65000.001",
            "--tif",
            "alo",
            "--leverage",
            "5",
            "--sl",
            "64000",
            "--tp",
            "67000",
            "--dry-run",
        ])
        .expect("trade command should parse");

        match cli.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape().expect("trade shape is valid");
                assert_eq!(args.symbol, "BTC/USDT");
                assert_eq!(args.size, Some(0.001));
                assert!(matches!(args.order_kind, TradeOrderKind::Limit));
                assert!(matches!(args.tif, TradeTimeInForce::Alo));
                assert_eq!(args.leverage, 5.0);
                assert_eq!(args.sl, Some(64_000.0));
                assert_eq!(args.tp, Some(67_000.0));
                assert!(args.dry_run);
            }
            _ => panic!("expected trade long command"),
        }
    }

    #[test]
    fn trade_buy_and_sell_aliases_map_to_position_directions() {
        let buy = Cli::try_parse_from([
            "mlab",
            "trade",
            "buy",
            "BTC/USDT",
            "--margin",
            "100",
            "--dry-run",
        ])
        .expect("buy alias should parse");
        let sell = Cli::try_parse_from([
            "mlab",
            "trade",
            "sell",
            "BTC/USDT",
            "--size",
            "0.001",
            "--dry-run",
        ])
        .expect("sell alias should parse");
        assert!(matches!(
            buy.command,
            Commands::Trade {
                command: TradeCommands::Long(_)
            }
        ));
        assert!(matches!(
            sell.command,
            Commands::Trade {
                command: TradeCommands::Short(_)
            }
        ));
    }

    #[test]
    fn live_trade_rejects_legacy_notional_sizing() {
        let error = Cli::try_parse_from(["mlab", "trade", "long", "BTC/USDT", "--notional", "100"])
            .expect_err("live trade sizing must use margin or size");

        assert!(error.to_string().contains("--notional"));
    }

    #[test]
    fn parse_execution_management_commands() {
        let cancel = Cli::try_parse_from([
            "mlab",
            "cancel",
            "BTC/USDT",
            "Fpa3oVuL3UzjNANAMZZdmrn6D1Zhk83GmBuJpuAWG51F",
            "--dry-run",
        ])
        .expect("cancel should parse");
        assert!(matches!(cancel.command, Commands::Cancel(_)));

        let close = Cli::try_parse_from(["mlab", "close", "BTC/USDT", "--dry-run"])
            .expect("close should parse");
        assert!(matches!(close.command, Commands::Close(_)));

        let daemon = Cli::try_parse_from(["mlab", "daemon", "events", "--limit", "10"])
            .expect("daemon events should parse");
        assert!(matches!(
            daemon.command,
            Commands::Daemon {
                command: DaemonCommands::Events(DaemonEventsArgs { limit: 10, .. })
            }
        ));
    }

    #[test]
    fn parse_auth_commands() {
        let set =
            Cli::try_parse_from(["mlab", "auth", "set", "mmt"]).expect("auth set should parse");
        assert!(matches!(
            set.command,
            Commands::Auth {
                command: AuthCommands::Set(AuthSetArgs {
                    provider: AuthProvider::Mmt,
                    reauthorize: false
                })
            }
        ));

        let status =
            Cli::try_parse_from(["mlab", "auth", "status"]).expect("auth status should parse");
        assert!(matches!(
            status.command,
            Commands::Auth {
                command: AuthCommands::Status
            }
        ));

        let bulk =
            Cli::try_parse_from(["mlab", "auth", "set", "bulk"]).expect("bulk auth should parse");
        assert!(matches!(
            bulk.command,
            Commands::Auth {
                command: AuthCommands::Set(AuthSetArgs {
                    provider: AuthProvider::Bulk,
                    reauthorize: false
                })
            }
        ));

        let bulk_reauthorize =
            Cli::try_parse_from(["mlab", "auth", "set", "bulk", "--reauthorize"])
                .expect("bulk reauthorization should parse");
        assert!(matches!(
            bulk_reauthorize.command,
            Commands::Auth {
                command: AuthCommands::Set(AuthSetArgs {
                    provider: AuthProvider::Bulk,
                    reauthorize: true
                })
            }
        ));
    }

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
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
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
                assert_eq!(args.timeframe, Some(60));
                assert_eq!(args.from, Some(1704067200000));
                assert_eq!(args.to, Some(1704067800000));
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
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
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
                assert_eq!(args.from, Some(1704067200000));
                assert_eq!(args.to, Some(1704067800000));
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
            "1704067200000",
            "--to",
            "1704067800000",
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
            "1704067200000",
            "--to",
            "1704067800000",
            "--output",
            "json",
        ])
        .expect("source candles parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                assert_eq!(args.timeframe, 60);
                assert_eq!(args.from, Some(1704067200000));
                assert_eq!(args.to, Some(1704067800000));
            }
            _ => panic!("expected source candles command"),
        }
    }

    #[test]
    fn parse_source_oi_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "oi",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--output",
            "json",
        ])
        .expect("source oi parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Oi(args),
            } => {
                assert_eq!(args.timeframe, Some(60));
                assert_eq!(args.from, Some(1704067200000));
                assert_eq!(args.to, Some(1704067800000));
            }
            _ => panic!("expected source oi command"),
        }
    }

    #[test]
    fn reject_source_oi_for_spot_exchange() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "oi",
            "--provider",
            "mmt",
            "--exchange",
            "binance",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--stream",
        ])
        .expect("source OI shape parses");

        let Commands::Source {
            command: SourceCommands::Oi(args),
        } = cli.command
        else {
            panic!("expected source OI command");
        };
        let error = args
            .validate()
            .expect_err("spot exchange must reject open interest");
        assert!(error.to_string().contains("requires a futures exchange"));
    }

    #[test]
    fn parse_source_volumes_command() {
        let cli = Cli::try_parse_from([
            "market-lab",
            "source",
            "volumes",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--output",
            "json",
        ])
        .expect("source volumes parse should succeed");

        match cli.command {
            Commands::Source {
                command: SourceCommands::Volumes(args),
            } => {
                assert_eq!(args.timeframe, 60);
                assert_eq!(args.from, Some(1704067200000));
                assert_eq!(args.to, Some(1704067800000));
            }
            _ => panic!("expected source volumes command"),
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
            "1704067200000",
            "--to",
            "1704067800000",
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
            Commands::Health(args) => {
                assert!(matches!(args.provider, Some(CliDataProvider::Mmt)))
            }
            _ => panic!("expected health command"),
        }
    }

    #[test]
    fn parse_status_command() {
        let cli = Cli::try_parse_from(["market-lab", "status", "--provider", "mmt"])
            .expect("status parse should succeed");
        match cli.command {
            Commands::Status(args) => {
                assert!(matches!(args.provider, Some(CliDataProvider::Mmt)))
            }
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
                assert!(matches!(args.provider, Some(CliDataProvider::Mmt)));
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
                assert!(matches!(args.provider, Some(CliDataProvider::Mmt)));
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
                assert!(matches!(args.provider, Some(CliDataProvider::Mmt)));
                assert!(args.stream);
                assert_eq!(args.interval_ms, 500);
            }
            _ => panic!("expected source orderbook command"),
        }
    }

    #[test]
    fn bulk_market_data_sources_use_exchange_without_mmt_auth() {
        let candles = Cli::try_parse_from([
            "mlab",
            "source",
            "candles",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
        ])
        .expect("standalone BULK candles should parse");
        match candles.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                assert_eq!(args.exchange, "bulk");
                assert!(args.provider.is_none());
                assert_eq!(
                    args.provider_kind().expect("BULK provider should resolve"),
                    CliProviderKind::Bulk
                );
                args.validate().expect("standalone BULK candles validate");
            }
            _ => panic!("expected BULK candles command"),
        }

        let stats = Cli::try_parse_from([
            "mlab",
            "source",
            "stats",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
        ])
        .expect("BULK stats should parse");
        assert!(matches!(
            stats.command,
            Commands::Source {
                command: SourceCommands::Stats(_)
            }
        ));

        let funding = Cli::try_parse_from([
            "mlab",
            "source",
            "funding",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
        ])
        .expect("BULK funding should parse");
        assert!(matches!(
            funding.command,
            Commands::Source {
                command: SourceCommands::Funding(_)
            }
        ));
    }

    #[test]
    fn mmt_is_the_only_public_provider_value() {
        let error = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--provider",
            "bulk",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
        ])
        .expect_err("BULK must not be accepted as a provider");

        let message = error.to_string();
        assert!(message.contains("invalid value 'bulk'"));
        assert!(message.contains("mmt"));
    }

    #[test]
    fn mmt_routes_an_exchange_while_bulk_is_standalone() {
        let mmt = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
        ])
        .expect("MMT source should parse");
        match mmt.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                args.validate().expect("MMT source should validate");
                assert_eq!(
                    args.provider_kind().expect("MMT provider should resolve"),
                    CliProviderKind::Mmt
                );
            }
            _ => panic!("expected MMT orderbook command"),
        }

        let invalid = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--provider",
            "mmt",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
        ])
        .expect("syntax should parse before provider validation");
        match invalid.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                let error = args
                    .validate()
                    .expect_err("BULK must not be routed through MMT");
                assert!(error.to_string().contains("omit --provider"));
            }
            _ => panic!("expected invalid BULK orderbook command"),
        }
    }

    #[test]
    fn unsupported_standalone_exchange_explains_mmt_routing() {
        let cli = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC/USDT",
        ])
        .expect("syntax should parse before exchange validation");
        match cli.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                let error = args
                    .validate()
                    .expect_err("binancef is not a standalone exchange yet");
                assert!(error.to_string().contains("--provider mmt"));
            }
            _ => panic!("expected standalone orderbook command"),
        }
    }

    #[test]
    fn rejects_seconds_at_the_market_lab_boundary() {
        let cli = Cli::try_parse_from([
            "mlab",
            "source",
            "candles",
            "--exchange",
            "bulk",
            "--symbol",
            "BTC/USDT",
            "--timeframe",
            "60",
            "--from",
            "1704067200",
            "--to",
            "1704067800",
        ])
        .expect("syntax should parse before unit validation");
        match cli.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                let error = args.validate().expect_err("seconds must be rejected");
                assert!(error.to_string().contains("millisecond timestamp"));
            }
            _ => panic!("expected BULK candles command"),
        }
    }

    #[test]
    fn parse_strategy_twap_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "twap",
            "BTC/USDT",
            "--venue",
            "bulk",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "300",
            "--interval",
            "30",
            "--dry-run",
        ])
        .expect("strategy parse should succeed");

        match cli.command {
            Commands::Strategy {
                command:
                    StrategyCommands::Run {
                        command: StrategyRunCommands::Twap(args),
                    },
            } => {
                args.validate().expect("TWAP arguments should validate");
                assert_eq!(args.margin, Some(1000.0));
                assert_eq!(args.duration, 300);
                assert_eq!(args.interval, 30);
                assert!(args.dry_run);
            }
            _ => panic!("expected strategy run twap command"),
        }
    }

    #[test]
    fn parse_mid_price_bot_command_without_a_side() {
        let cli = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "mid-price",
            "BTC/USDT",
            "--venue",
            "bulk",
            "--margin",
            "100",
            "--duration",
            "300",
            "--spread-bps",
            "2",
            "--leverage",
            "10",
            "--stop-loss-pct",
            "5",
            "--dry-run",
        ])
        .expect("mid-price bot should parse");

        match cli.command {
            Commands::Bot {
                command:
                    BotCommands::Run {
                        command: BotRunCommands::MidPrice(args),
                    },
            } => {
                args.validate()
                    .expect("mid-price arguments should validate");
                assert_eq!(args.margin, Some(100.0));
                assert_eq!(args.duration, 300);
                assert_eq!(args.spread_bps, 2.0);
                assert_eq!(args.directional_bias, 0.0);
                assert_eq!(args.leverage, 10.0);
                assert_eq!(args.stop_loss_pct, Some(5.0));
                assert!(args.dry_run);
            }
            _ => panic!("expected bot run mid-price command"),
        }
    }

    #[test]
    fn parse_volume_mid_bot_with_fill_priority_refresh_controls() {
        let cli = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "volume-mid",
            "BTC/USDT",
            "--margin",
            "100",
            "--duration",
            "300",
            "--refresh-time",
            "5",
            "--refresh-tolerance-bps",
            "0.5",
            "--dry-run",
        ])
        .expect("volume-mid bot should parse");

        match cli.command {
            Commands::Bot {
                command:
                    BotCommands::Run {
                        command: BotRunCommands::VolumeMid(args),
                    },
            } => {
                args.validate()
                    .expect("volume-mid arguments should validate");
                assert_eq!(args.common.margin, Some(100.0));
                assert_eq!(args.refresh_time, 5.0);
                assert_eq!(args.refresh_tolerance_bps, 0.5);
            }
            _ => panic!("expected bot run volume-mid command"),
        }
    }

    #[test]
    fn parse_strategy_vwap_command_without_interval() {
        let cli = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "vwap",
            "BTC/USDT",
            "--venue",
            "bulk",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "3600",
            "--volume-sources",
            "binancef@mmt,hyperliquid@mmt,bulk",
            "--dry-run",
        ])
        .expect("VWAP should parse");

        match cli.command {
            Commands::Strategy {
                command:
                    StrategyCommands::Run {
                        command: StrategyRunCommands::Vwap(args),
                    },
            } => {
                args.validate().expect("VWAP arguments should validate");
                assert_eq!(args.duration, 3600);
                assert_eq!(
                    args.volume_sources,
                    ["binancef@mmt", "hyperliquid@mmt", "bulk"]
                );
                assert!(args.dry_run);
            }
            _ => panic!("expected strategy run vwap command"),
        }
    }

    #[test]
    fn strategy_vwap_does_not_accept_an_interval() {
        let error = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "vwap",
            "BTC/USDT",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "300",
            "--interval",
            "30",
        ])
        .expect_err("VWAP must not expose a child interval");
        assert!(error.to_string().contains("--interval"));
    }

    #[test]
    fn parse_strategy_oiwap_command_without_interval() {
        let cli = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "oiwap",
            "BTC/USDT",
            "--venue",
            "bulk",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "3600",
            "--oi-sources",
            "binancef@mmt,hyperliquid@mmt",
            "--dry-run",
        ])
        .expect("OIWAP should parse");

        match cli.command {
            Commands::Strategy {
                command:
                    StrategyCommands::Run {
                        command: StrategyRunCommands::Oiwap(args),
                    },
            } => {
                args.validate().expect("OIWAP arguments should validate");
                assert_eq!(args.duration, 3600);
                assert_eq!(args.oi_sources, ["binancef@mmt", "hyperliquid@mmt"]);
                assert!(args.dry_run);
            }
            _ => panic!("expected strategy run oiwap command"),
        }
    }

    #[test]
    fn strategy_oiwap_requires_explicit_oi_sources() {
        let cli = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "oiwap",
            "BTC/USDT",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "300",
        ])
        .expect("CLI shape parses before semantic validation");
        let Commands::Strategy {
            command:
                StrategyCommands::Run {
                    command: StrategyRunCommands::Oiwap(args),
                },
        } = cli.command
        else {
            panic!("expected strategy run oiwap command");
        };
        assert!(
            args.validate()
                .expect_err("OI sources must be explicit")
                .to_string()
                .contains("requires --oi-sources")
        );
    }

    #[test]
    fn strategy_oiwap_does_not_accept_an_interval() {
        let error = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "oiwap",
            "BTC/USDT",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "300",
            "--oi-sources",
            "binancef@mmt",
            "--interval",
            "30",
        ])
        .expect_err("OIWAP must not expose a child interval");
        assert!(error.to_string().contains("--interval"));
    }

    #[test]
    fn strategy_twap_requires_side() {
        let error = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "twap",
            "BTC/USDT",
            "--margin",
            "1000",
            "--duration",
            "300",
        ])
        .expect_err("TWAP must require a side");

        assert!(error.to_string().contains("--side"));
    }

    #[test]
    fn strategy_twap_rejects_zero_duration() {
        let cli = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "twap",
            "BTC/USDT",
            "--side",
            "sell",
            "--size",
            "1",
            "--duration",
            "0",
        ])
        .expect("syntax should parse before semantic validation");

        match cli.command {
            Commands::Strategy {
                command:
                    StrategyCommands::Run {
                        command: StrategyRunCommands::Twap(args),
                    },
            } => {
                let error = args.validate().expect_err("zero duration must fail");
                assert!(error.to_string().contains("--duration"));
            }
            _ => panic!("expected strategy run twap command"),
        }
    }

    #[test]
    fn parse_strategy_job_management_commands() {
        let status = Cli::try_parse_from([
            "mlab",
            "strategy",
            "status",
            "strategy_123",
            "--output",
            "json",
        ])
        .expect("strategy status should parse");
        match status.command {
            Commands::Strategy {
                command: StrategyCommands::Status(args),
            } => {
                args.validate().expect("strategy status should validate");
                assert_eq!(args.job, "strategy_123");
            }
            _ => panic!("expected strategy status command"),
        }

        let logs = Cli::try_parse_from(["mlab", "strategy", "logs", "strategy_123", "--follow"])
            .expect("strategy logs should parse");
        match logs.command {
            Commands::Strategy {
                command: StrategyCommands::Logs(args),
            } => {
                args.validate().expect("strategy logs should validate");
                assert!(args.follow);
            }
            _ => panic!("expected strategy logs command"),
        }
    }

    #[test]
    fn parse_script_run_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "./studies/buy-pressure.js",
            "--symbol",
            "BTC/USDT",
            "--source",
            "candles@bybitf@mmt:timeframe=60",
            "--param",
            "min_vbuy=50000",
            "--duration",
            "3600",
            "--output",
            "json",
        ])
        .expect("script run parse should succeed");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => {
                assert_eq!(args.script, "./studies/buy-pressure.js");
                assert_eq!(args.symbol.as_deref(), Some("BTC/USDT"));
                assert_eq!(args.source, vec!["candles@bybitf@mmt:timeframe=60"]);
                assert_eq!(args.param, vec!["min_vbuy=50000"]);
                assert_eq!(args.duration, Some(3600));
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
                assert!(args.symbol.is_none());
                assert!(args.from.is_none());
                assert!(args.to.is_none());
                assert!(args.duration.is_none());
                args.validate().expect("base validate should succeed");
            }
            _ => panic!("expected script run command"),
        }
    }

    #[test]
    fn reject_zero_script_run_duration() {
        let cli = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "test/market-maker.js",
            "--duration",
            "0",
        ])
        .expect("duration syntax should parse before validation");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => {
                let error = args.validate().expect_err("zero duration must fail");
                assert!(error.to_string().contains("at least 1 second"));
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
            "--symbol",
            "BTC/USDT",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--source",
            "candles@bybitf@mmt:timeframe=60",
            "--param",
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
                assert_eq!(args.source, vec!["candles@bybitf@mmt:timeframe=60"]);
                assert_eq!(args.param, vec!["fast=20"]);
                args.validate().expect("validate should succeed");
            }
            _ => panic!("expected script backtest command"),
        }
    }

    #[test]
    fn exchange_qualified_script_sources_do_not_require_global_exchange() {
        let cli = Cli::try_parse_from([
            "mlab",
            "script",
            "backtest",
            "./scripts/cross-exchange.js",
            "--symbol",
            "BTC/USDT",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--source",
            "candles@binancef@mmt:timeframe=60",
            "--source",
            "candles@okx@mmt:timeframe=60",
        ])
        .expect("qualified script sources should parse without --exchange");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => {
                assert_eq!(
                    args.source,
                    vec![
                        "candles@binancef@mmt:timeframe=60",
                        "candles@okx@mmt:timeframe=60"
                    ]
                );
                args.validate().expect("backtest should validate");
            }
            _ => panic!("expected script backtest command"),
        }
    }

    #[test]
    fn bulk_scripts_do_not_require_exchange() {
        let run = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "./examples/candle-summary.js",
            "--symbol",
            "BTC/USDT",
            "--source",
            "candles@bulk:timeframe=60",
        ])
        .expect("BULK script run should parse without exchange");
        match run.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => assert_eq!(args.source, vec!["candles@bulk:timeframe=60"]),
            _ => panic!("expected script run command"),
        }

        let backtest = Cli::try_parse_from([
            "mlab",
            "script",
            "backtest",
            "./examples/sma-cross.js",
            "--symbol",
            "BTC/USDT",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--source",
            "candles@bulk:timeframe=60",
        ])
        .expect("BULK script backtest should parse without exchange");
        match backtest.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => {
                assert_eq!(args.source, vec!["candles@bulk:timeframe=60"]);
                args.validate().expect("BULK backtest should validate");
            }
            _ => panic!("expected script backtest command"),
        }
    }

    #[test]
    fn reject_script_run_with_leverage() {
        let err = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "./scripts/sma-cross.js",
            "--leverage",
            "5",
        ])
        .expect_err("script run should not accept leverage");
        assert!(err.to_string().contains("--leverage"));
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

    #[test]
    fn parses_detached_script_execution_and_job_commands() {
        let run = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "strategy.js",
            "--symbol",
            "BTC/USDT",
            "--venue",
            "bulk",
        ])
        .expect("detached script execution should parse");
        match run.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => assert!(matches!(args.venue, Some(ExecutionVenueArg::Bulk))),
            _ => panic!("expected script run command"),
        }

        let logs = Cli::try_parse_from(["mlab", "script", "logs", "job_123", "--follow"])
            .expect("script logs should parse");
        match logs.command {
            Commands::Script {
                command: ScriptCommands::Logs(args),
            } => {
                assert_eq!(args.job, "job_123");
                assert!(args.follow);
            }
            _ => panic!("expected script logs command"),
        }
    }
}
