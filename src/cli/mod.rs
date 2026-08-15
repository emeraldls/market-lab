use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::bots::grid::MAX_GRID_LEVELS_PER_SIDE;
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
    Outcome {
        #[command(subcommand)]
        command: OutcomeCommands,
    },
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
    /// Show or change where mlabd runs.
    Backend(DaemonBackendArgs),
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
pub struct DaemonBackendArgs {
    /// Set the persistent daemon backend. Omit to show the current selection.
    #[arg(value_enum)]
    pub backend: Option<DaemonBackendArg>,
    /// Docker image to run. Valid only when selecting the Docker backend.
    #[arg(long, value_name = "IMAGE")]
    pub image: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum DaemonBackendArg {
    Native,
    Docker,
}

impl From<DaemonBackendArg> for crate::daemon::DaemonBackend {
    fn from(value: DaemonBackendArg) -> Self {
        match value {
            DaemonBackendArg::Native => Self::Native,
            DaemonBackendArg::Docker => Self::Docker,
        }
    }
}

#[derive(Clone, Debug, Args)]
pub struct TradeArgs {
    #[arg(default_value = "")]
    pub symbol: String,
    /// Explicit HIP-4 outcome side, e.g. 1001:0. Outcome trades may instead
    /// omit this flag and use the interactive market picker.
    #[arg(long = "symbol", value_name = "SYMBOL", conflicts_with = "symbol")]
    pub symbol_flag: Option<String>,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    /// Use Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
    /// Exposure multiplier for perpetual markets. Not accepted for spot execution.
    #[arg(long)]
    pub leverage: Option<f64>,
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
    pub fn requested_symbol(&self) -> &str {
        self.symbol_flag.as_deref().unwrap_or(&self.symbol)
    }

    pub fn apply_symbol_flag(&mut self) {
        if let Some(symbol) = self.symbol_flag.take() {
            self.symbol = symbol;
        }
    }

    pub fn validate_shape(&self) -> Result<()> {
        if self.symbol_flag.is_some() && self.venue != ExecutionVenueArg::HyperliquidOutcomes {
            bail!(
                "--symbol is available only for hyperliquid-outcomes; other venues use the positional symbol"
            );
        }
        let symbol = self.requested_symbol();
        if symbol.trim().is_empty() {
            if self.venue != ExecutionVenueArg::HyperliquidOutcomes {
                bail!("a symbol is required for this execution venue");
            }
            if self.yes || !matches!(self.output, OutputFormat::Terminal) {
                bail!("non-interactive outcome execution requires a symbol such as `1001:0`");
            }
        } else {
            validate_execution_symbol(self.venue, symbol)?;
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
        if let Some(leverage) = self.leverage
            && (!leverage.is_finite() || leverage < 1.0)
        {
            bail!("--leverage must be at least 1");
        }
        if self
            .margin
            .is_some_and(|margin| !(margin * self.leverage.unwrap_or(1.0)).is_finite())
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
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct AccountQueryArgs {
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    /// Query Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
    /// Cancel on Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl CancelOrderArgs {
    pub fn validate(&self) -> Result<()> {
        validate_execution_symbol(self.venue, &self.symbol)?;
        if self.order_id.trim().is_empty() {
            bail!("order id cannot be empty");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("cancel supports only --output terminal|json|jsonl");
        }
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct ClosePositionArgs {
    pub symbol: Option<String>,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    /// Close on Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl ClosePositionArgs {
    pub fn validate(&self) -> Result<()> {
        if let Some(symbol) = &self.symbol {
            validate_execution_symbol(self.venue, symbol)?;
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("close supports only --output terminal|json|jsonl");
        }
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

impl AccountQueryArgs {
    pub fn validate(&self) -> Result<()> {
        if let Some(symbol) = &self.symbol {
            validate_execution_symbol(self.venue, symbol)?;
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("account queries support only --output terminal|json|jsonl");
        }
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ExecutionVenueArg {
    #[value(name = "bulkf")]
    Bulk,
    #[value(name = "hyperliquidf")]
    Hyperliquid,
    #[value(name = "hyperliquidf-xyz")]
    HyperliquidXyz,
    #[value(name = "hyperliquid")]
    HyperliquidSpot,
    #[value(name = "hyperliquid-outcomes")]
    HyperliquidOutcomes,
}

fn validate_execution_network(venue: ExecutionVenueArg, testnet: bool) -> Result<()> {
    if testnet
        && !matches!(
            venue,
            ExecutionVenueArg::Hyperliquid
                | ExecutionVenueArg::HyperliquidXyz
                | ExecutionVenueArg::HyperliquidSpot
                | ExecutionVenueArg::HyperliquidOutcomes
        )
    {
        bail!("--testnet is only valid with a Hyperliquid venue");
    }
    Ok(())
}

impl From<ExecutionVenueArg> for ExecutionVenue {
    fn from(value: ExecutionVenueArg) -> Self {
        match value {
            ExecutionVenueArg::Bulk => ExecutionVenue::Bulk,
            ExecutionVenueArg::Hyperliquid => ExecutionVenue::Hyperliquid,
            ExecutionVenueArg::HyperliquidXyz => ExecutionVenue::HyperliquidXyz,
            ExecutionVenueArg::HyperliquidSpot => ExecutionVenue::HyperliquidSpot,
            ExecutionVenueArg::HyperliquidOutcomes => ExecutionVenue::HyperliquidOutcomes,
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
    /// Filter dynamic outcome markets by question, outcome, side, or id.
    #[arg(long)]
    pub search: Option<String>,
    /// Fetch Hyperliquid testnet markets instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
        if self.testnet && !self.exchange.eq_ignore_ascii_case("hyperliquid-outcomes") {
            bail!("--testnet is currently supported by markets only for hyperliquid-outcomes");
        }
        if self
            .search
            .as_ref()
            .is_some_and(|search| search.trim().is_empty())
        {
            bail!("--search cannot be empty");
        }
        Ok(())
    }
}

#[derive(Subcommand, Debug)]
pub enum OutcomeCommands {
    /// Convert quote collateral into equal shares of both outcome sides.
    Split(OutcomeAmountArgs),
    /// Convert equal shares of both outcome sides back into quote collateral.
    Merge(OutcomeOptionalAmountArgs),
    /// Merge complete question baskets back into quote collateral.
    MergeQuestion(OutcomeQuestionArgs),
    /// Convert one outcome's negative side into positive shares of the alternatives.
    Negate(OutcomeNegateArgs),
}

#[derive(Clone, Debug, Args)]
pub struct OutcomeAmountArgs {
    pub outcome: u32,
    #[arg(long)]
    pub amount: f64,
    #[command(flatten)]
    pub common: OutcomeActionCommonArgs,
}

#[derive(Clone, Debug, Args)]
pub struct OutcomeOptionalAmountArgs {
    pub outcome: u32,
    /// Amount to merge. Omit to merge the maximum balanced amount.
    #[arg(long)]
    pub amount: Option<f64>,
    #[command(flatten)]
    pub common: OutcomeActionCommonArgs,
}

#[derive(Clone, Debug, Args)]
pub struct OutcomeQuestionArgs {
    pub question: u32,
    /// Amount to merge. Omit to merge the maximum complete basket.
    #[arg(long)]
    pub amount: Option<f64>,
    #[command(flatten)]
    pub common: OutcomeActionCommonArgs,
}

#[derive(Clone, Debug, Args)]
pub struct OutcomeNegateArgs {
    pub question: u32,
    pub outcome: u32,
    #[arg(long)]
    pub amount: f64,
    #[command(flatten)]
    pub common: OutcomeActionCommonArgs,
}

#[derive(Clone, Debug, Args)]
pub struct OutcomeActionCommonArgs {
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub yes: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

impl OutcomeActionCommonArgs {
    pub fn validate(&self) -> Result<()> {
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("outcome actions support only --output terminal|json|jsonl");
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
    /// Replace remote execution credentials after their replacements are confirmed.
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
    Hyperliquid,
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
    Grid(RunGridArgs),
    MidPrice(RunMidPriceArgs),
    VolumeMid(RunVolumeMidArgs),
}

#[derive(Clone, Debug, Args)]
pub struct ScriptRunArgs {
    pub script: String,
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Python interpreter for a .py script. Defaults to an adjacent .venv, then python3.
    #[arg(long)]
    pub python: Option<PathBuf>,
    /// JavaScript Scripting V1 execution venue. Python V2 selects exchange per request.
    #[arg(long, value_enum)]
    pub venue: Option<ExecutionVenueArg>,
    /// Route all Hyperliquid execution and data in this job through testnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
        let language = crate::scripting::language::ScriptLanguage::from_path(
            std::path::Path::new(&self.script),
        )?;
        if language == crate::scripting::language::ScriptLanguage::PythonV2 {
            if self.venue.is_some() {
                bail!(
                    "Python Scripting V2 routes execution through ctx.trade/ctx.order exchange; remove --venue"
                );
            }
        } else if self.testnet
            && !matches!(
                self.venue,
                Some(
                    ExecutionVenueArg::Hyperliquid
                        | ExecutionVenueArg::HyperliquidXyz
                        | ExecutionVenueArg::HyperliquidSpot
                        | ExecutionVenueArg::HyperliquidOutcomes
                )
            )
        {
            bail!("--testnet requires a Hyperliquid execution venue");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
pub struct ScriptBacktestArgs {
    pub script: String,
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Python interpreter for a .py script. Defaults to an adjacent .venv, then python3.
    #[arg(long)]
    pub python: Option<PathBuf>,
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
        if matches!(
            provider,
            CliProviderKind::Binance | CliProviderKind::BinanceFutures
        ) {
            bail!("Binance live volume delta is not implemented");
        }
        if matches!(
            provider,
            CliProviderKind::Bulk | CliProviderKind::Hyperliquid
        ) {
            if !self.stream {
                bail!("standalone volume delta is derived from live trades and requires --stream");
            }
            if self.timeframe.is_some() || self.from.is_some() || self.to.is_some() {
                bail!("standalone live volume delta does not use --timeframe/--from/--to");
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
        if matches!(
            provider,
            CliProviderKind::Binance | CliProviderKind::BinanceFutures
        ) {
            bail!("Binance open interest is not implemented");
        }
        if !crate::markets::is_futures_exchange(&self.exchange)? {
            bail!(
                "open interest requires a futures exchange; `{}` is spot",
                self.exchange
            );
        }
        if matches!(
            provider,
            CliProviderKind::Bulk | CliProviderKind::Hyperliquid
        ) {
            if self.timeframe.is_some() || self.from.is_some() || self.to.is_some() {
                bail!(
                    "standalone open interest is current/live only; omit --timeframe/--from/--to"
                );
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
        validate_exchange_symbol(self.exchange, self.symbol)?;
        provider_timeframe_from_seconds(self.provider, self.timeframe)?;
        if self.stream
            && matches!(
                self.provider,
                CliProviderKind::Binance | CliProviderKind::BinanceFutures
            )
        {
            bail!("Binance live candle and volume streaming is not implemented");
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
        validate_stream_controls(self.buffer_size, self.interval_ms)
    }
}

impl CvdArgs {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
        resolve_source_provider(None, &self.exchange)?;
        if let Some(symbol) = &self.symbol {
            validate_exchange_symbol(&self.exchange, symbol)?;
        }
        if !matches!(
            self.period.as_str(),
            "1d" | "7d" | "30d" | "90d" | "1y" | "all"
        ) {
            bail!("--period must be one of 1d,7d,30d,90d,1y,all");
        }
        if self.stream && self.symbol.is_none() {
            bail!("--symbol is required when streaming statistics");
        }
        if self.stream && matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        validate_stream_controls(self.buffer_size, self.interval_ms)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(None, &self.exchange)
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
        resolve_source_provider(None, &self.exchange)?;
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
        if !crate::markets::is_futures_exchange(&self.exchange)? {
            bail!(
                "funding requires a futures exchange; `{}` is spot",
                self.exchange
            );
        }
        if self.stream && matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json|jsonl");
        }
        validate_stream_controls(self.buffer_size, self.interval_ms)
    }

    pub fn provider_kind(&self) -> Result<CliProviderKind> {
        resolve_source_provider(None, &self.exchange)
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
    /// Replacement custom mlabd image built for the release being installed.
    #[arg(long, value_name = "IMAGE", conflicts_with = "check")]
    pub daemon_image: Option<String>,
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
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
    /// Execute through Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
    /// Execute through Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
    /// Comma-separated volume venues, for example binancef@mmt,okxf@mmt,bulkf.
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
    /// Execute through Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
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
    /// Execute through Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
    /// Total base-asset quantity allocated across both initial grid ladders.
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
    /// Exposure multiplier for perpetual markets. Not applicable to outcome markets.
    #[arg(long)]
    pub leverage: Option<f64>,
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
pub struct RunGridArgs {
    pub symbol: String,
    #[arg(long, value_enum, default_value_t = ExecutionVenueArg::Bulk)]
    pub venue: ExecutionVenueArg,
    /// Execute through Hyperliquid testnet instead of the default mainnet.
    #[arg(long, default_value_t = false)]
    pub testnet: bool,
    /// Hard one-sided inventory limit in base-asset units.
    #[arg(long, conflicts_with = "margin", required_unless_present = "margin")]
    pub size: Option<f64>,
    /// Collateral allocated to the bot. On perpetuals, leverage multiplies this exposure.
    #[arg(long, conflicts_with = "size", required_unless_present = "size")]
    pub margin: Option<f64>,
    /// Maximum bot runtime in seconds.
    #[arg(long)]
    pub duration: u64,
    /// Number of fixed grid levels initially placed on each side.
    #[arg(long, default_value_t = 3)]
    pub levels: u16,
    /// Distance in basis points between adjacent fixed grid prices.
    #[arg(long, default_value_t = 2.0)]
    pub step_bps: f64,
    /// Exposure multiplier for perpetual markets. Not applicable to outcome markets.
    #[arg(long)]
    pub leverage: Option<f64>,
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
        if matches!(
            self.venue,
            ExecutionVenueArg::HyperliquidSpot | ExecutionVenueArg::HyperliquidOutcomes
        ) {
            bail!("TWAP does not support spot or outcome-market execution");
        }
        validate_execution_symbol(self.venue, &self.symbol)?;
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
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

impl RunVwapArgs {
    pub fn validate(&self) -> Result<()> {
        validate_execution_symbol(self.venue, &self.symbol)?;
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
            ExecutionVenueArg::Bulk => "bulkf",
            ExecutionVenueArg::Hyperliquid => "hyperliquidf",
            ExecutionVenueArg::HyperliquidXyz => "hyperliquidf-xyz",
            ExecutionVenueArg::HyperliquidSpot => {
                bail!("VWAP does not support spot execution yet")
            }
            ExecutionVenueArg::HyperliquidOutcomes => {
                bail!("VWAP does not support outcome-market execution")
            }
        };
        crate::strategies::vwap::VolumeSourceSelector::parse(
            &self.volume_sources,
            execution_venue,
            &self.symbol,
        )?;
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

impl RunOiwapArgs {
    pub fn validate(&self) -> Result<()> {
        if matches!(
            self.venue,
            ExecutionVenueArg::HyperliquidSpot | ExecutionVenueArg::HyperliquidOutcomes
        ) {
            bail!("OIWAP does not support spot or outcome-market execution");
        }
        validate_execution_symbol(self.venue, &self.symbol)?;
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
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

impl RunMidPriceArgs {
    pub fn validate(&self) -> Result<()> {
        if self.venue == ExecutionVenueArg::HyperliquidSpot {
            bail!("market-making bots do not support spot execution");
        }
        validate_bot_execution_symbol(self.venue, &self.symbol)?;
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
        if self.venue == ExecutionVenueArg::HyperliquidOutcomes {
            if self.leverage.is_some() {
                bail!("--leverage is not used with outcome markets");
            }
            if self.directional_bias != 0.0 {
                bail!("--directional-bias is not supported for outcome markets");
            }
        } else if self
            .leverage
            .is_some_and(|leverage| !leverage.is_finite() || leverage < 1.0)
        {
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
            .is_some_and(|margin| !(margin * self.leverage.unwrap_or(1.0)).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("bot run supports only --output terminal|json|jsonl");
        }
        validate_execution_network(self.venue, self.testnet)?;
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

impl RunGridArgs {
    pub fn validate(&self) -> Result<()> {
        if self.venue == ExecutionVenueArg::HyperliquidSpot {
            bail!("grid does not support spot execution");
        }
        validate_bot_execution_symbol(self.venue, &self.symbol)?;
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
        if !(1..=MAX_GRID_LEVELS_PER_SIDE).contains(&self.levels) {
            bail!("--levels must be between 1 and {MAX_GRID_LEVELS_PER_SIDE}");
        }
        if !self.step_bps.is_finite() || self.step_bps <= 0.0 {
            bail!("--step-bps must be greater than zero");
        }
        if self.venue == ExecutionVenueArg::HyperliquidOutcomes {
            if self.leverage.is_some() {
                bail!("--leverage is not used with outcome markets");
            }
        } else if self
            .leverage
            .is_some_and(|leverage| !leverage.is_finite() || leverage < 1.0)
        {
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
            .is_some_and(|margin| !(margin * self.leverage.unwrap_or(1.0)).is_finite())
        {
            bail!("--margin multiplied by --leverage is too large");
        }
        if self.dry_run && self.yes {
            bail!("--yes is not used with --dry-run");
        }
        if matches!(self.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("bot run supports only --output terminal|json|jsonl");
        }
        validate_execution_network(self.venue, self.testnet)?;
        Ok(())
    }
}

impl DepthArgs {
    pub fn validate(&self) -> Result<()> {
        resolve_market_provider(self.provider, &self.exchange)?;
        if self.exchange.trim().is_empty() {
            bail!("--exchange cannot be empty");
        }
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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
        validate_exchange_symbol(&self.exchange, &self.symbol)?;
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

fn validate_execution_symbol(venue: ExecutionVenueArg, symbol: &str) -> Result<()> {
    if venue == ExecutionVenueArg::HyperliquidOutcomes {
        return crate::providers::hyperliquid::outcomes::parse_symbol(symbol).map(|_| ());
    }
    let market_type = match venue {
        ExecutionVenueArg::Bulk
        | ExecutionVenueArg::Hyperliquid
        | ExecutionVenueArg::HyperliquidXyz => crate::markets::MarketType::Futures,
        ExecutionVenueArg::HyperliquidSpot => crate::markets::MarketType::Spot,
        ExecutionVenueArg::HyperliquidOutcomes => unreachable!("handled above"),
    };
    crate::markets::canonical_market_symbol(symbol, market_type).map(|_| ())
}

fn validate_bot_execution_symbol(venue: ExecutionVenueArg, symbol: &str) -> Result<()> {
    if venue == ExecutionVenueArg::HyperliquidOutcomes {
        return crate::providers::hyperliquid::outcomes::parse_market_id(symbol).map(|_| ());
    }
    validate_execution_symbol(venue, symbol)
}

fn validate_source_identity(
    provider: Option<CliDataProvider>,
    exchange: &str,
    symbol: &str,
) -> Result<CliProviderKind> {
    let provider = resolve_source_provider(provider, exchange)?;
    validate_exchange_symbol(exchange, symbol)?;
    Ok(provider)
}

fn validate_exchange_symbol(exchange: &str, symbol: &str) -> Result<()> {
    if exchange.eq_ignore_ascii_case("hyperliquid-outcomes") {
        return crate::providers::hyperliquid::outcomes::parse_symbol(symbol).map(|_| ());
    }
    let market_type = if crate::markets::is_futures_exchange(exchange)? {
        crate::markets::MarketType::Futures
    } else {
        crate::markets::MarketType::Spot
    };
    crate::markets::canonical_market_symbol(symbol, market_type).map(|_| ())
}

fn resolve_source_provider(
    provider: Option<CliDataProvider>,
    exchange: &str,
) -> Result<CliProviderKind> {
    if exchange.trim().is_empty() {
        bail!("--exchange cannot be empty");
    }
    if provider.is_some() {
        if exchange.eq_ignore_ascii_case("bulkf") {
            bail!("omit --provider for the standalone `{exchange}` exchange");
        }
        return Ok(CliProviderKind::Mmt);
    }
    if exchange.eq_ignore_ascii_case("bulkf") {
        return Ok(CliProviderKind::Bulk);
    }
    if exchange.eq_ignore_ascii_case("hyperliquidf")
        || exchange.eq_ignore_ascii_case("hyperliquidf-xyz")
        || exchange.eq_ignore_ascii_case("hyperliquid")
        || exchange.eq_ignore_ascii_case("hyperliquid-outcomes")
    {
        return Ok(CliProviderKind::Hyperliquid);
    }
    if exchange.eq_ignore_ascii_case("binance") {
        return Ok(CliProviderKind::Binance);
    }
    if exchange.eq_ignore_ascii_case("binancef") {
        return Ok(CliProviderKind::BinanceFutures);
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
        (Some(_), Some(exchange)) if exchange.eq_ignore_ascii_case("bulkf") => {
            bail!("omit --provider for the standalone `{exchange}` exchange")
        }
        (Some(_), _) => Ok(ProviderKind::Mmt),
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("bulkf") => Ok(ProviderKind::Bulk),
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("hyperliquidf") => {
            Ok(ProviderKind::Hyperliquid)
        }
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("hyperliquidf-xyz") => {
            Ok(ProviderKind::Hyperliquid)
        }
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("hyperliquid") => {
            Ok(ProviderKind::Hyperliquid)
        }
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("hyperliquid-outcomes") => {
            Ok(ProviderKind::Hyperliquid)
        }
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("binance") => {
            Ok(ProviderKind::Binance)
        }
        (None, Some(exchange)) if exchange.eq_ignore_ascii_case("binancef") => {
            Ok(ProviderKind::BinanceFutures)
        }
        (None, Some(exchange)) => bail!("unsupported standalone exchange `{exchange}`"),
        (None, None) => Ok(ProviderKind::MarketLab),
    }
}

fn provider_timeframe_from_seconds(
    provider: CliProviderKind,
    seconds: u32,
) -> Result<&'static str> {
    match provider {
        CliProviderKind::Bulk => {
            crate::providers::bulk::market_data::timeframe_from_seconds(seconds)
        }
        CliProviderKind::Hyperliquid => {
            crate::providers::hyperliquid::market_data::timeframe_from_seconds(seconds)
        }
        CliProviderKind::Binance | CliProviderKind::BinanceFutures => {
            crate::providers::binance::market_data::timeframe_from_seconds(seconds)
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
    Hyperliquid,
    Binance,
    BinanceFutures,
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
            CliProviderKind::Hyperliquid => ProviderKind::Hyperliquid,
            CliProviderKind::Binance => ProviderKind::Binance,
            CliProviderKind::BinanceFutures => ProviderKind::BinanceFutures,
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
            "bulkf",
            "--symbol",
            "BTC",
            "--json",
        ])
        .expect("markets command should parse");

        match cli.command {
            Commands::Markets(args) => {
                assert!(args.provider.is_none());
                assert_eq!(args.exchange, "bulkf");
                assert_eq!(args.symbol.as_deref(), Some("BTC"));
                assert!(!args.refresh);
                assert!(args.json);
                args.validate().expect("BULK markets should validate");
            }
            _ => panic!("expected markets command"),
        }
    }

    #[test]
    fn rejects_bare_bulk_execution_id() {
        let execution = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "BTC",
            "--venue",
            "bulk",
            "--margin",
            "10",
            "--dry-run",
        ])
        .expect_err("bare bulk must not be accepted as an execution venue");
        assert!(execution.to_string().contains("invalid value 'bulk'"));
    }

    #[test]
    fn parse_hyperliquid_standalone_commands() {
        let markets =
            Cli::try_parse_from(["mlab", "markets", "--exchange", "hyperliquidf", "--refresh"])
                .expect("Hyperliquid markets command should parse");
        match markets.command {
            Commands::Markets(args) => {
                assert!(args.provider.is_none());
                assert_eq!(args.exchange, "hyperliquidf");
                assert!(args.refresh);
                args.validate()
                    .expect("standalone Hyperliquid markets should validate");
            }
            _ => panic!("expected markets command"),
        }

        let source = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--exchange",
            "hyperliquidf",
            "--symbol",
            "BTC",
            "--depth",
            "20",
        ])
        .expect("Hyperliquid source command should parse");
        match source.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                args.validate()
                    .expect("standalone Hyperliquid source should validate");
                assert_eq!(
                    args.provider_kind().expect("provider resolves"),
                    CliProviderKind::Hyperliquid
                );
            }
            _ => panic!("expected source orderbook command"),
        }

        let mmt_source = Cli::try_parse_from([
            "mlab",
            "source",
            "candles",
            "--provider",
            "mmt",
            "--exchange",
            "hyperliquidf",
            "--symbol",
            "BTC",
            "--timeframe",
            "60",
            "--stream",
        ])
        .expect("MMT Hyperliquid command should parse");
        match mmt_source.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                args.validate().expect("MMT Hyperliquid source validates");
                assert_eq!(
                    args.provider_kind().expect("MMT route resolves"),
                    CliProviderKind::Mmt
                );
            }
            _ => panic!("expected source candles command"),
        }

        let trade = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "BTC",
            "--venue",
            "hyperliquidf",
            "--margin",
            "100",
            "--leverage",
            "5",
            "--dry-run",
        ])
        .expect("Hyperliquid trade command should parse");
        match trade.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape()
                    .expect("Hyperliquid trade shape should validate");
                assert!(matches!(args.venue, ExecutionVenueArg::Hyperliquid));
            }
            _ => panic!("expected trade long command"),
        }

        let xyz_source = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--exchange",
            "hyperliquidf-xyz",
            "--symbol",
            "TSLA",
            "--depth",
            "20",
        ])
        .expect("XYZ source command should parse");
        match xyz_source.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                args.validate().expect("standalone XYZ source validates");
                assert_eq!(
                    args.provider_kind().expect("XYZ provider resolves"),
                    CliProviderKind::Hyperliquid
                );
            }
            _ => panic!("expected XYZ source orderbook command"),
        }

        let xyz_trade = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "TSLA",
            "--venue",
            "hyperliquidf-xyz",
            "--margin",
            "100",
            "--leverage",
            "5",
            "--dry-run",
        ])
        .expect("XYZ trade command should parse");
        match xyz_trade.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape().expect("XYZ trade shape validates");
                assert!(matches!(args.venue, ExecutionVenueArg::HyperliquidXyz));
            }
            _ => panic!("expected XYZ trade long command"),
        }

        let spot_trade = Cli::try_parse_from([
            "mlab",
            "trade",
            "buy",
            "BTC/USDC",
            "--venue",
            "hyperliquid",
            "--size",
            "0.001",
            "--dry-run",
        ])
        .expect("Hyperliquid spot trade command should parse");
        match spot_trade.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape()
                    .expect("Hyperliquid spot trade shape should validate");
                assert!(matches!(args.venue, ExecutionVenueArg::HyperliquidSpot));
            }
            _ => panic!("expected trade buy command"),
        }

        assert!(
            Cli::try_parse_from([
                "mlab",
                "trade",
                "long",
                "BTC/USDC",
                "--venue",
                "hyperliquid",
                "--margin",
                "100",
            ])
            .is_ok(),
            "`hyperliquid` must resolve deterministically to spot execution"
        );
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
    fn parses_outcome_market_data_execution_and_user_actions() {
        let source = Cli::try_parse_from([
            "mlab",
            "source",
            "orderbook",
            "--exchange",
            "hyperliquid-outcomes",
            "--symbol",
            "1001:0",
            "--depth",
            "20",
        ])
        .expect("outcome source parses");
        match source.command {
            Commands::Source {
                command: SourceCommands::Orderbook(args),
            } => {
                args.validate().expect("outcome source validates");
                assert_eq!(
                    args.provider_kind().expect("provider resolves"),
                    CliProviderKind::Hyperliquid
                );
            }
            _ => panic!("expected outcome orderbook source"),
        }

        let trade = Cli::try_parse_from([
            "mlab",
            "trade",
            "buy",
            "1001:1",
            "--venue",
            "hyperliquid-outcomes",
            "--size",
            "10",
            "--dry-run",
        ])
        .expect("outcome trade parses");
        match trade.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape().expect("outcome trade validates");
                assert_eq!(args.symbol, "1001:1");
                assert_eq!(args.venue, ExecutionVenueArg::HyperliquidOutcomes);
                assert!(args.leverage.is_none());
            }
            _ => panic!("expected outcome buy command"),
        }

        let flagged_trade = Cli::try_parse_from([
            "mlab",
            "trade",
            "buy",
            "--venue",
            "hyperliquid-outcomes",
            "--symbol",
            "1001:0",
            "--size",
            "10",
            "--dry-run",
        ])
        .expect("explicit outcome symbol flag parses");
        match flagged_trade.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape()
                    .expect("outcome symbol flag validates");
                assert_eq!(args.requested_symbol(), "1001:0");
            }
            _ => panic!("expected flagged outcome buy command"),
        }

        let split = Cli::try_parse_from([
            "mlab",
            "outcome",
            "split",
            "1001",
            "--amount",
            "10",
            "--testnet",
            "--dry-run",
        ])
        .expect("outcome split parses");
        match split.command {
            Commands::Outcome {
                command: OutcomeCommands::Split(args),
            } => {
                assert_eq!(args.outcome, 1001);
                assert_eq!(args.amount, 10.0);
                assert!(args.common.testnet);
            }
            _ => panic!("expected outcome split command"),
        }
    }

    #[test]
    fn interactive_outcome_trade_may_omit_symbol_but_automation_may_not() {
        let interactive = Cli::try_parse_from([
            "mlab",
            "trade",
            "buy",
            "--venue",
            "hyperliquid-outcomes",
            "--margin",
            "100",
        ])
        .expect("interactive outcome trade parses");
        match interactive.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => args.validate_shape().expect("interactive shape validates"),
            _ => panic!("expected outcome buy command"),
        }

        let automated = Cli::try_parse_from([
            "mlab",
            "trade",
            "buy",
            "--venue",
            "hyperliquid-outcomes",
            "--margin",
            "100",
            "--yes",
        ])
        .expect("automated outcome trade parses");
        match automated.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => assert!(args.validate_shape().is_err()),
            _ => panic!("expected outcome buy command"),
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
            "BTC",
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
            "BTC",
            "--venue",
            "bulkf",
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
                assert_eq!(args.symbol, "BTC");
                assert_eq!(args.size, Some(0.001));
                assert!(matches!(args.order_kind, TradeOrderKind::Limit));
                assert!(matches!(args.tif, TradeTimeInForce::Alo));
                assert_eq!(args.leverage, Some(5.0));
                assert_eq!(args.sl, Some(64_000.0));
                assert_eq!(args.tp, Some(67_000.0));
                assert!(args.dry_run);
            }
            _ => panic!("expected trade long command"),
        }
    }

    #[test]
    fn spot_trade_does_not_collect_leverage() {
        let cli = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "HYPE/USDC",
            "--venue",
            "hyperliquid",
            "--margin",
            "100",
            "--dry-run",
        ])
        .expect("spot trade should parse without leverage");

        match cli.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                args.validate_shape().expect("spot trade shape is valid");
                assert_eq!(args.leverage, None);
            }
            _ => panic!("expected trade long command"),
        }
    }

    #[test]
    fn execution_symbols_distinguish_futures_from_spot() {
        let futures = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "BTC/USDT",
            "--venue",
            "bulkf",
            "--size",
            "0.001",
            "--dry-run",
        ])
        .expect("futures command should parse before semantic validation");

        match futures.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                let error = args
                    .validate_shape()
                    .expect_err("futures pair syntax must be rejected");
                assert!(error.to_string().contains("base asset"));
            }
            _ => panic!("expected trade long command"),
        }

        let spot = Cli::try_parse_from([
            "mlab",
            "trade",
            "long",
            "HYPE",
            "--venue",
            "hyperliquid",
            "--size",
            "1",
            "--dry-run",
        ])
        .expect("spot command should parse before semantic validation");

        match spot.command {
            Commands::Trade {
                command: TradeCommands::Long(args),
            } => {
                let error = args
                    .validate_shape()
                    .expect_err("bare spot asset syntax must be rejected");
                assert!(error.to_string().contains("BASE/QUOTE"));
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
            "BTC",
            "--margin",
            "100",
            "--dry-run",
        ])
        .expect("buy alias should parse");
        let sell = Cli::try_parse_from([
            "mlab",
            "trade",
            "sell",
            "BTC",
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
        let error = Cli::try_parse_from(["mlab", "trade", "long", "BTC", "--notional", "100"])
            .expect_err("live trade sizing must use margin or size");

        assert!(error.to_string().contains("--notional"));
    }

    #[test]
    fn parse_execution_management_commands() {
        let cancel = Cli::try_parse_from([
            "mlab",
            "cancel",
            "BTC",
            "Fpa3oVuL3UzjNANAMZZdmrn6D1Zhk83GmBuJpuAWG51F",
            "--dry-run",
        ])
        .expect("cancel should parse");
        assert!(matches!(cancel.command, Commands::Cancel(_)));

        let close =
            Cli::try_parse_from(["mlab", "close", "BTC", "--dry-run"]).expect("close should parse");
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
    fn parse_daemon_backend_commands() {
        let show = Cli::try_parse_from(["mlab", "daemon", "backend"])
            .expect("daemon backend query should parse");
        assert!(matches!(
            show.command,
            Commands::Daemon {
                command: DaemonCommands::Backend(DaemonBackendArgs { backend: None, .. })
            }
        ));

        let docker = Cli::try_parse_from(["mlab", "daemon", "backend", "docker"])
            .expect("Docker daemon backend should parse");
        assert!(matches!(
            docker.command,
            Commands::Daemon {
                command: DaemonCommands::Backend(DaemonBackendArgs {
                    backend: Some(DaemonBackendArg::Docker),
                    ..
                })
            }
        ));

        let custom = Cli::try_parse_from([
            "mlab",
            "daemon",
            "backend",
            "docker",
            "--image",
            "marketlab-python:latest",
        ])
        .expect("custom Docker daemon image should parse");
        assert!(matches!(
            custom.command,
            Commands::Daemon {
                command: DaemonCommands::Backend(DaemonBackendArgs {
                    backend: Some(DaemonBackendArg::Docker),
                    image: Some(image),
                    ..
                })
            } if image == "marketlab-python:latest"
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
    fn parse_upgrade_with_custom_daemon_image() {
        let cli = Cli::try_parse_from([
            "mlab",
            "upgrade",
            "--daemon-image",
            "marketlab-python:v0.0.8",
        ])
        .expect("custom daemon upgrade image should parse");

        match cli.command {
            Commands::Upgrade(args) => {
                assert_eq!(
                    args.daemon_image.as_deref(),
                    Some("marketlab-python:v0.0.8")
                );
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "bulkf",
            "--symbol",
            "BTC",
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
                assert_eq!(args.exchange, "bulkf");
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
            "bulkf",
            "--symbol",
            "BTC",
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
            "bulkf",
            "--symbol",
            "BTC",
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
            "bulkf",
            "--exchange",
            "bulkf",
            "--symbol",
            "BTC",
        ])
        .expect_err("BULK must not be accepted as a provider");

        let message = error.to_string();
        assert!(message.contains("invalid value 'bulkf'"));
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
            "BTC",
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
            "bulkf",
            "--symbol",
            "BTC",
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
    fn binance_standalone_and_mmt_routes_are_distinct() {
        let standalone = Cli::try_parse_from([
            "mlab",
            "source",
            "candles",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC",
            "--timeframe",
            "60",
        ])
        .expect("standalone Binance futures command should parse");
        match standalone.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                assert_eq!(
                    args.provider_kind().expect("standalone route resolves"),
                    CliProviderKind::BinanceFutures
                );
            }
            _ => panic!("expected standalone candles command"),
        }

        let mmt = Cli::try_parse_from([
            "mlab",
            "source",
            "candles",
            "--provider",
            "mmt",
            "--exchange",
            "binancef",
            "--symbol",
            "BTC",
            "--timeframe",
            "60",
        ])
        .expect("MMT Binance futures command should parse");
        match mmt.command {
            Commands::Source {
                command: SourceCommands::Candles(args),
            } => {
                assert_eq!(
                    args.provider_kind().expect("MMT route resolves"),
                    CliProviderKind::Mmt
                );
            }
            _ => panic!("expected MMT candles command"),
        }
    }

    #[test]
    fn rejects_seconds_at_the_market_lab_boundary() {
        let cli = Cli::try_parse_from([
            "mlab",
            "source",
            "candles",
            "--exchange",
            "bulkf",
            "--symbol",
            "BTC",
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
            "BTC",
            "--venue",
            "bulkf",
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
            "BTC",
            "--venue",
            "bulkf",
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
                assert_eq!(args.leverage, Some(10.0));
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
            "BTC",
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
    fn existing_mid_price_bots_accept_outcome_execution_without_leverage() {
        let mid = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "mid-price",
            "1009",
            "--venue",
            "hyperliquid-outcomes",
            "--margin",
            "100",
            "--duration",
            "300",
            "--spread-bps",
            "20",
            "--dry-run",
        ])
        .expect("outcome mid-price bot should parse");
        let Commands::Bot {
            command:
                BotCommands::Run {
                    command: BotRunCommands::MidPrice(mid),
                },
        } = mid.command
        else {
            panic!("expected outcome-enabled mid-price command");
        };
        mid.validate()
            .expect("outcome mid-price arguments should validate");
        assert_eq!(mid.venue, ExecutionVenueArg::HyperliquidOutcomes);
        assert_eq!(mid.leverage, None);

        let volume = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "volume-mid",
            "1009",
            "--venue",
            "hyperliquid-outcomes",
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
        .expect("outcome volume-mid bot should parse");
        let Commands::Bot {
            command:
                BotCommands::Run {
                    command: BotRunCommands::VolumeMid(volume),
                },
        } = volume.command
        else {
            panic!("expected outcome-enabled volume-mid command");
        };
        volume
            .validate()
            .expect("outcome volume-mid arguments should validate");
        assert_eq!(volume.common.leverage, None);
    }

    #[test]
    fn existing_grid_bot_accepts_outcome_execution_without_leverage() {
        let cli = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "grid",
            "1009",
            "--venue",
            "hyperliquid-outcomes",
            "--margin",
            "100",
            "--duration",
            "300",
            "--levels",
            "4",
            "--step-bps",
            "20",
            "--dry-run",
        ])
        .expect("outcome grid bot should parse");
        let Commands::Bot {
            command:
                BotCommands::Run {
                    command: BotRunCommands::Grid(grid),
                },
        } = cli.command
        else {
            panic!("expected outcome-enabled grid command");
        };
        grid.validate()
            .expect("outcome grid arguments should validate");
        assert_eq!(grid.venue, ExecutionVenueArg::HyperliquidOutcomes);
        assert_eq!(grid.leverage, None);
    }

    #[test]
    fn outcome_bots_reject_leverage_and_have_no_standalone_command() {
        let cli = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "mid-price",
            "1009",
            "--venue",
            "hyperliquid-outcomes",
            "--margin",
            "100",
            "--duration",
            "300",
            "--leverage",
            "1",
            "--dry-run",
        ])
        .expect("outcome mid-price bot should parse before semantic validation");
        let Commands::Bot {
            command:
                BotCommands::Run {
                    command: BotRunCommands::MidPrice(mid),
                },
        } = cli.command
        else {
            panic!("expected outcome-enabled mid-price command");
        };
        assert!(mid.validate().is_err());

        assert!(
            Cli::try_parse_from([
                "mlab",
                "bot",
                "run",
                "outcome",
                "1009:0",
                "--margin",
                "100",
                "--duration",
                "300",
            ])
            .is_err()
        );
    }

    #[test]
    fn parse_multi_level_grid_bot() {
        let cli = Cli::try_parse_from([
            "mlab",
            "bot",
            "run",
            "grid",
            "BTC",
            "--venue",
            "hyperliquidf",
            "--margin",
            "100",
            "--duration",
            "300",
            "--levels",
            "4",
            "--step-bps",
            "2",
            "--leverage",
            "10",
            "--stop-loss-pct",
            "5",
            "--dry-run",
        ])
        .expect("grid bot should parse");

        match cli.command {
            Commands::Bot {
                command:
                    BotCommands::Run {
                        command: BotRunCommands::Grid(args),
                    },
            } => {
                args.validate().expect("grid arguments should validate");
                assert_eq!(args.margin, Some(100.0));
                assert_eq!(args.levels, 4);
                assert_eq!(args.step_bps, 2.0);
                assert_eq!(args.stop_loss_pct, Some(5.0));
            }
            _ => panic!("expected bot run grid command"),
        }
    }

    #[test]
    fn parse_strategy_vwap_command_without_interval() {
        let cli = Cli::try_parse_from([
            "mlab",
            "strategy",
            "run",
            "vwap",
            "BTC",
            "--venue",
            "bulkf",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "3600",
            "--volume-sources",
            "binancef@mmt,hyperliquidf@mmt,bulkf",
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
                    ["binancef@mmt", "hyperliquidf@mmt", "bulkf"]
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
            "BTC",
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
            "BTC",
            "--venue",
            "bulkf",
            "--side",
            "buy",
            "--margin",
            "1000",
            "--duration",
            "3600",
            "--oi-sources",
            "binancef@mmt,hyperliquidf@mmt",
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
                assert_eq!(args.oi_sources, ["binancef@mmt", "hyperliquidf@mmt"]);
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "BTC",
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
            "--source",
            "btc@candles@bybitf@mmt:timeframe=60",
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
                assert_eq!(args.source, vec!["btc@candles@bybitf@mmt:timeframe=60"]);
                assert_eq!(args.param, vec!["min_vbuy=50000"]);
                assert_eq!(args.duration, Some(3600));
                args.validate().expect("validate should succeed");
            }
            _ => panic!("expected script run command"),
        }
    }

    #[test]
    fn parse_python_script_commands() {
        let run = Cli::try_parse_from([
            "mlab",
            "script",
            "run",
            "strategy.py",
            "--python",
            ".venv/bin/python",
            "--source",
            "btc@candles@hyperliquidf:timeframe=60",
        ])
        .expect("Python script run should parse");
        match run.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => {
                assert_eq!(args.script, "strategy.py");
                assert_eq!(args.python, Some(PathBuf::from(".venv/bin/python")));
            }
            _ => panic!("expected Python script run"),
        }

        let backtest = Cli::try_parse_from([
            "mlab",
            "script",
            "backtest",
            "strategy.py",
            "--python",
            ".venv/bin/python",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
        ])
        .expect("Python script backtest should parse");
        match backtest.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => assert_eq!(args.python, Some(PathBuf::from(".venv/bin/python"))),
            _ => panic!("expected Python script backtest"),
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
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--source",
            "btc@candles@bybitf@mmt:timeframe=60",
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
                assert_eq!(args.source, vec!["btc@candles@bybitf@mmt:timeframe=60"]);
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
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--source",
            "btc@candles@binancef@mmt:timeframe=60",
            "--source",
            "btc@candles@okx@mmt:timeframe=60",
        ])
        .expect("qualified script sources should parse without --exchange");

        match cli.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => {
                assert_eq!(
                    args.source,
                    vec![
                        "btc@candles@binancef@mmt:timeframe=60",
                        "btc@candles@okx@mmt:timeframe=60"
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
            "--source",
            "btc@candles@bulkf:timeframe=60",
        ])
        .expect("BULK script run should parse without exchange");
        match run.command {
            Commands::Script {
                command: ScriptCommands::Run(args),
            } => assert_eq!(args.source, vec!["btc@candles@bulkf:timeframe=60"]),
            _ => panic!("expected script run command"),
        }

        let backtest = Cli::try_parse_from([
            "mlab",
            "script",
            "backtest",
            "./examples/sma-cross.js",
            "--from",
            "1704067200000",
            "--to",
            "1704067800000",
            "--source",
            "btc@candles@bulkf:timeframe=60",
        ])
        .expect("BULK script backtest should parse without exchange");
        match backtest.command {
            Commands::Script {
                command: ScriptCommands::Backtest(args),
            } => {
                assert_eq!(args.source, vec!["btc@candles@bulkf:timeframe=60"]);
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
        let run = Cli::try_parse_from(["mlab", "script", "run", "strategy.js", "--venue", "bulkf"])
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

    #[test]
    fn python_v2_rejects_job_wide_venue_and_accepts_job_wide_testnet() {
        let with_venue =
            Cli::try_parse_from(["mlab", "script", "run", "strategy.py", "--venue", "bulkf"])
                .expect("Python command should parse before semantic validation");
        let Commands::Script {
            command: ScriptCommands::Run(args),
        } = with_venue.command
        else {
            panic!("expected script run command");
        };
        assert!(
            args.validate()
                .expect_err("Python --venue must fail")
                .to_string()
                .contains("remove --venue")
        );

        let testnet = Cli::try_parse_from(["mlab", "script", "run", "strategy.py", "--testnet"])
            .expect("Python testnet command should parse");
        let Commands::Script {
            command: ScriptCommands::Run(args),
        } = testnet.command
        else {
            panic!("expected script run command");
        };
        args.validate()
            .expect("Python --testnet does not require --venue");
    }

    #[test]
    fn script_commands_reject_the_removed_symbol_flag() {
        let error =
            Cli::try_parse_from(["mlab", "script", "run", "strategy.js", "--symbol", "BTC"])
                .expect_err("script --symbol must remain removed");

        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }
}
