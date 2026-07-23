use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use futures_util::future::join_all;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::bots::jobs::{
    BotJob, BotJobDefinition, BotJobSubmission, BotPerformance, MidPriceJobDefinition,
};
use crate::bots::mid_price::{MidPriceQuotes, quote_prices, quote_sizes};
use crate::cli::{
    ExecutionVenueArg, OutputFormat, RunMidPriceArgs, RunVolumeMidArgs, TradeArgs, TradeOrderKind,
    TradeTimeInForce,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::{
    CancelPlan, ExecutionReceipt, ExecutionVenue, Fill, OpenOrder, OrderKind, OrderSide,
    PositionDirection, TimeInForce, TradePlan,
};
use crate::domain::types::{OrderBookSnapshot, TopOfBook};
use crate::providers::bulk::market_data::{BulkProvider, normalize_timestamp_ms};
use crate::providers::bulk::ws::{BulkAccountStream, BulkOrderBookStream};
use crate::providers::execution::ExecutionAdapter;
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::market_data::HyperliquidProvider;
use crate::providers::hyperliquid::ws::{HyperliquidAccountStream, HyperliquidOrderBookStream};

const BOOK_DEPTH: u16 = 1;
const RECONNECT_MAX_SECONDS: u64 = 5;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const STRICT_MINIMUM_QUOTE_AGE: Duration = Duration::from_millis(500);
const STRICT_REFRESH_TOLERANCE_BPS: f64 = 0.25;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MidMode {
    QuoteProtection,
    FillPriority,
}

impl MidMode {
    fn name(self) -> &'static str {
        match self {
            Self::QuoteProtection => "mid-price",
            Self::FillPriority => "volume-mid",
        }
    }

    fn minimum_quote_age(self, definition: &MidPriceJobDefinition) -> Duration {
        match self {
            Self::QuoteProtection => STRICT_MINIMUM_QUOTE_AGE,
            Self::FillPriority => Duration::from_secs_f64(definition.refresh_seconds),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MidPricePlanView<'a> {
    r#type: &'static str,
    bot: &'static str,
    venue: &'static str,
    symbol: &'a str,
    max_inventory_size: f64,
    requested_margin: Option<f64>,
    max_inventory_margin: f64,
    max_inventory_exposure: f64,
    reference_price: f64,
    initial_bid: f64,
    initial_ask: f64,
    initial_bid_size: f64,
    initial_ask_size: f64,
    initial_bid_exposure: f64,
    initial_ask_exposure: f64,
    initial_working_exposure: f64,
    spread_bps: f64,
    refresh_seconds: f64,
    refresh_tolerance_bps: f64,
    stop_loss_pct: Option<f64>,
    directional_bias_percent: f64,
    sizing: &'static str,
    duration_secs: u64,
    leverage: f64,
    execution: &'static str,
    shutdown: &'static str,
    dry_run: bool,
}

#[derive(Debug)]
pub(super) struct BotStopped;

impl fmt::Display for BotStopped {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bot worker stopped")
    }
}

impl Error for BotStopped {}

pub async fn handle_mid_price(args: RunMidPriceArgs) -> Result<()> {
    handle(
        args,
        MidMode::QuoteProtection,
        STRICT_MINIMUM_QUOTE_AGE.as_secs_f64(),
        STRICT_REFRESH_TOLERANCE_BPS,
    )
    .await
}

pub async fn handle_volume_mid(args: RunVolumeMidArgs) -> Result<()> {
    args.validate()?;
    let RunVolumeMidArgs {
        common,
        refresh_time,
        refresh_tolerance_bps,
    } = args;
    handle(
        common,
        MidMode::FillPriority,
        refresh_time,
        refresh_tolerance_bps,
    )
    .await
}

async fn handle(
    args: RunMidPriceArgs,
    mode: MidMode,
    refresh_seconds: f64,
    refresh_tolerance_bps: f64,
) -> Result<()> {
    args.validate()?;
    let parent = build_trade_plan(
        &trade_args(&args, args.size, args.margin),
        PositionDirection::Long,
    )
    .await?;
    let market = execution_market(parent.venue, &parent.internal_symbol)?;
    let rules = market.execution_rules()?;
    let top = live_orderbook(parent.venue, &parent.internal_symbol, parent.testnet).await?;
    let best_bid = top
        .bids
        .first()
        .copied()
        .with_context(|| format!("{} book has no bid", venue_label(parent.venue)))?;
    let best_ask = top
        .asks
        .first()
        .copied()
        .with_context(|| format!("{} book has no ask", venue_label(parent.venue)))?;
    let quotes = quote_prices(
        best_bid,
        best_ask,
        args.spread_bps,
        rules.tick_size,
        rules.price_precision,
    )?;
    let initial_sizes = quote_sizes(parent.size, 0.0, args.directional_bias)?;
    let initial_bid = desired_quote(
        initial_sizes.bid_size,
        quotes.bid_price,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    );
    let initial_ask = desired_quote(
        initial_sizes.ask_size,
        quotes.ask_price,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    );
    if initial_bid.is_none() && initial_ask.is_none() {
        bail!("mid-price amount is too small to create a quote at the venue minimum");
    }
    let definition = MidPriceJobDefinition {
        venue: parent.venue,
        testnet: parent.testnet,
        symbol: parent.internal_symbol.clone(),
        max_inventory_size: parent.size,
        requested_margin: parent.requested_margin,
        max_inventory_margin: parent.estimated_margin,
        max_inventory_exposure: parent.estimated_exposure,
        duration_seconds: args.duration,
        spread_bps: args.spread_bps,
        refresh_seconds,
        refresh_tolerance_bps,
        directional_bias_percent: args.directional_bias,
        leverage: args.leverage,
        stop_loss_pct: args.stop_loss_pct.filter(|percent| *percent > 0.0),
    };
    let view = plan_view(
        mode.name(),
        &parent,
        &definition,
        quotes,
        initial_bid.map_or(0.0, |(_, size)| size),
        initial_ask.map_or(0.0, |(_, size)| size),
        args.dry_run,
    );

    if args.dry_run {
        render_plan(&view, args.output)?;
        return Ok(());
    }
    if !args.yes && !matches!(args.output, OutputFormat::Terminal) {
        bail!("live bot execution with structured output requires --yes");
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_plan(&view, args.output)?;
        if !args.yes && !confirm_live_execution(parent.venue, parent.testnet)? {
            println!("cancelled; no bot job was submitted");
            return Ok(());
        }
    }

    let submission = BotJobSubmission {
        definition: match mode {
            MidMode::QuoteProtection => BotJobDefinition::MidPrice(definition),
            MidMode::FillPriority => BotJobDefinition::VolumeMid(definition),
        },
    };
    let job = crate::runtime::submit_bot_job(submission).await?;
    render_submission(&job, args.output)
}

pub async fn handle_worker_job(job_id: &str, job: BotJob) -> Result<()> {
    let (mode, definition) = match job.definition {
        BotJobDefinition::Grid(_) => bail!("mid-price worker received a grid job"),
        BotJobDefinition::MidPrice(definition) => (MidMode::QuoteProtection, definition),
        BotJobDefinition::VolumeMid(definition) => (MidMode::FillPriority, definition),
    };
    let pid = std::process::id();
    crate::runtime::bot_worker_started(job_id, pid).await?;
    let result = run_worker(job_id, mode, &definition).await;
    let error = result
        .as_ref()
        .err()
        .and_then(|error| (!error.is::<BotStopped>()).then(|| format!("{error:#}")));
    if let Some(message) = &error {
        let _ = crate::runtime::append_bot_output(
            job_id,
            &serde_json::json!({
                "type": "bot.run.failed",
                "bot": mode.name(),
                "jobId": job_id,
                "error": message,
            }),
        );
    }
    let _ = crate::runtime::bot_worker_finished(job_id, pid, error).await;
    match result {
        Err(error) if error.is::<BotStopped>() => Ok(()),
        result => result,
    }
}

fn trade_args(args: &RunMidPriceArgs, size: Option<f64>, margin: Option<f64>) -> TradeArgs {
    TradeArgs {
        symbol: args.symbol.clone(),
        config: None,
        venue: args.venue,
        testnet: args.testnet,
        size,
        margin,
        order_kind: TradeOrderKind::Market,
        price: None,
        tif: TradeTimeInForce::Gtc,
        leverage: args.leverage,
        reduce_only: false,
        sl: None,
        tp: None,
        dry_run: false,
        yes: true,
        output: args.output,
    }
}

fn worker_trade_args(definition: &MidPriceJobDefinition) -> TradeArgs {
    TradeArgs {
        symbol: definition.symbol.clone(),
        config: None,
        venue: match definition.venue {
            ExecutionVenue::Bulk => ExecutionVenueArg::Bulk,
            ExecutionVenue::Hyperliquid => ExecutionVenueArg::Hyperliquid,
        },
        testnet: definition.testnet,
        size: Some(definition.max_inventory_size),
        margin: None,
        order_kind: TradeOrderKind::Market,
        price: None,
        tif: TradeTimeInForce::Gtc,
        leverage: definition.leverage,
        reduce_only: false,
        sl: None,
        tp: None,
        dry_run: false,
        yes: true,
        output: OutputFormat::Jsonl,
    }
}

fn plan_view<'a>(
    bot: &'static str,
    parent: &'a TradePlan,
    definition: &MidPriceJobDefinition,
    quotes: MidPriceQuotes,
    initial_bid_size: f64,
    initial_ask_size: f64,
    dry_run: bool,
) -> MidPricePlanView<'a> {
    let initial_bid_exposure = initial_bid_size * quotes.bid_price;
    let initial_ask_exposure = initial_ask_size * quotes.ask_price;
    MidPricePlanView {
        r#type: "bot.plan",
        bot,
        venue: venue_key(parent.venue),
        symbol: &parent.internal_symbol,
        max_inventory_size: definition.max_inventory_size,
        requested_margin: definition.requested_margin,
        max_inventory_margin: definition.max_inventory_margin,
        max_inventory_exposure: definition.max_inventory_exposure,
        reference_price: quotes.reference_price,
        initial_bid: quotes.bid_price,
        initial_ask: quotes.ask_price,
        initial_bid_size,
        initial_ask_size,
        initial_bid_exposure,
        initial_ask_exposure,
        initial_working_exposure: initial_bid_exposure + initial_ask_exposure,
        spread_bps: definition.spread_bps,
        refresh_seconds: definition.refresh_seconds,
        refresh_tolerance_bps: definition.refresh_tolerance_bps,
        stop_loss_pct: definition.stop_loss_pct,
        directional_bias_percent: definition.directional_bias_percent,
        sizing: "continuous, inventory-skewed",
        duration_secs: definition.duration_seconds,
        leverage: parent.leverage,
        execution: "maker-only post-only ALO quotes",
        shutdown: "cancel owned quotes, then unwind bot-owned inventory",
        dry_run,
    }
}

fn render_plan(plan: &MidPricePlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "{} market maker{}",
                plan.bot,
                if plan.dry_run {
                    " (dry run — nothing will be submitted)"
                } else {
                    ""
                }
            );
            println!("  venue:              {}", plan.venue);
            println!("  symbol:             {}", plan.symbol);
            println!("  max inventory size: {}", plan.max_inventory_size);
            if let Some(margin) = plan.requested_margin {
                println!("  requested margin:   {margin:.8}");
            }
            println!(
                "  max inventory:      {:.8} margin",
                plan.max_inventory_margin
            );
            println!("  max exposure:       {:.8}", plan.max_inventory_exposure);
            println!("  reference midpoint: {}", plan.reference_price);
            println!(
                "  initial bid / ask:  {} / {}",
                plan.initial_bid, plan.initial_ask
            );
            println!(
                "  initial bid size:   {} ({:.8} exposure)",
                plan.initial_bid_size, plan.initial_bid_exposure
            );
            println!(
                "  initial ask size:   {} ({:.8} exposure)",
                plan.initial_ask_size, plan.initial_ask_exposure
            );
            println!(
                "  initial working:    {:.8} exposure",
                plan.initial_working_exposure
            );
            println!("  spread:             {} bps", plan.spread_bps);
            println!("  refresh time:       {}s", plan.refresh_seconds);
            println!("  refresh tolerance:  {} bps", plan.refresh_tolerance_bps);
            if let Some(percent) = plan.stop_loss_pct {
                println!("  stop loss:          {percent}% of allocated margin");
            }
            println!(
                "  directional bias:  {}% ({})",
                plan.directional_bias_percent,
                bias_name(plan.directional_bias_percent)
            );
            println!("  sizing:             {}", plan.sizing);
            println!("  duration:           {}s", plan.duration_secs);
            println!("  leverage:           {}x", plan.leverage);
            println!("  execution:          {}", plan.execution);
            println!("  shutdown:           {}", plan.shutdown);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub(super) fn render_submission(job: &BotJob, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(job)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(job)?),
        OutputFormat::Terminal => {
            println!("bot deployed");
            println!("  job:     {}", job.id);
            println!("  bot:     {}", job.definition.name());
            println!("  status:  starting");
            println!("  symbol:  {}", job.definition.symbol());
            println!("  logs:    mlab bot logs {} --follow", job.id);
            println!("  stop:    mlab bot stop {}", job.id);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

pub(super) fn confirm_live_execution(venue: ExecutionVenue, testnet: bool) -> Result<bool> {
    print!(
        "Deploy this live maker-only bot on {}? [y/N]: ",
        execution_venue_label(venue, testnet)
    );
    io::stdout()
        .flush()
        .context("failed to flush confirmation prompt")?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("failed to read confirmation")?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn execution_venue_label(venue: ExecutionVenue, testnet: bool) -> &'static str {
    match (venue, testnet) {
        (ExecutionVenue::Bulk, _) => "BULK testnet",
        (ExecutionVenue::Hyperliquid, true) => "Hyperliquid testnet",
        (ExecutionVenue::Hyperliquid, false) => "Hyperliquid mainnet",
    }
}

pub(super) fn venue_key(venue: ExecutionVenue) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "bulk",
        ExecutionVenue::Hyperliquid => "hyperliquid",
    }
}

pub(super) fn venue_label(venue: ExecutionVenue) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "BULK",
        ExecutionVenue::Hyperliquid => "Hyperliquid",
    }
}

pub(super) fn execution_market(
    venue: ExecutionVenue,
    symbol: &str,
) -> Result<std::sync::Arc<crate::markets::Market>> {
    crate::markets::exchange_market(venue_key(venue), symbol)
}

pub(super) async fn live_orderbook(
    venue: ExecutionVenue,
    symbol: &str,
    testnet: bool,
) -> Result<OrderBookSnapshot> {
    match venue {
        ExecutionVenue::Bulk => BulkProvider::live_orderbook(symbol, BOOK_DEPTH, None).await,
        ExecutionVenue::Hyperliquid => {
            HyperliquidProvider::live_orderbook_on(
                symbol,
                BOOK_DEPTH,
                None,
                HyperliquidNetwork::from_testnet(testnet),
            )
            .await
        }
    }
}

fn bias_name(bias: f64) -> &'static str {
    if bias > f64::EPSILON {
        "long"
    } else if bias < -f64::EPSILON {
        "short"
    } else {
        "neutral"
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum QuoteSide {
    Buy,
    Sell,
}

impl QuoteSide {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub(super) fn order_side(self) -> OrderSide {
        match self {
            Self::Buy => OrderSide::Buy,
            Self::Sell => OrderSide::Sell,
        }
    }

    pub(super) fn direction(self) -> PositionDirection {
        match self {
            Self::Buy => PositionDirection::Long,
            Self::Sell => PositionDirection::Short,
        }
    }
}

#[derive(Clone, Debug)]
struct WorkingQuote {
    order_id: String,
    price: f64,
    size: f64,
    submitted_at: Instant,
    cancel_requested: bool,
}

#[derive(Clone, Copy)]
struct ReconcilePolicy {
    replace_price: bool,
    replace_size: bool,
}

impl ReconcilePolicy {
    const PASSIVE: Self = Self {
        replace_price: true,
        replace_size: false,
    };
    const EXECUTION: Self = Self {
        replace_price: false,
        replace_size: true,
    };
}

#[derive(Default)]
struct QuoteSlot {
    live: Option<WorkingQuote>,
    busy: bool,
    retry_after_book_revision: Option<u64>,
}

impl QuoteSlot {
    fn for_side<'a>(side: QuoteSide, buy: &'a mut Self, sell: &'a mut Self) -> &'a mut Self {
        match side {
            QuoteSide::Buy => buy,
            QuoteSide::Sell => sell,
        }
    }

    fn accepts_book_revision(&mut self, revision: u64) -> bool {
        match self.retry_after_book_revision {
            Some(rejected_revision) if revision > rejected_revision => {
                self.retry_after_book_revision = None;
                true
            }
            Some(_) => false,
            None => true,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct BookFeedState {
    pub(super) revision: u64,
    pub(super) top: Option<TopOfBook>,
    pub(super) error: Option<String>,
}

pub(super) enum AccountFeedEvent {
    Connected,
    Disconnected(String),
    Recovery {
        open_orders: Vec<OpenOrder>,
        fills: Vec<Fill>,
    },
    Data(Value),
}

enum ActionKind {
    Submit {
        price: f64,
        size: f64,
        book_revision: u64,
    },
    Cancel {
        order_id: String,
    },
}

struct ActionCompletion {
    side: QuoteSide,
    kind: ActionKind,
    result: std::result::Result<ExecutionReceipt, String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct FillKey {
    order_id: String,
    timestamp: u64,
    size_bits: u64,
    price_bits: u64,
    buy: bool,
}

pub(super) struct FillLedger {
    allocated_margin: f64,
    bought_size: f64,
    sold_size: f64,
    bought_notional: f64,
    sold_notional: f64,
    position_size: f64,
    average_entry_price: f64,
    gross_realized_pnl: f64,
    fees: f64,
    fees_complete: bool,
    revision: u64,
    seen_counts: HashMap<FillKey, usize>,
}

impl Default for FillLedger {
    fn default() -> Self {
        Self {
            allocated_margin: 0.0,
            bought_size: 0.0,
            sold_size: 0.0,
            bought_notional: 0.0,
            sold_notional: 0.0,
            position_size: 0.0,
            average_entry_price: 0.0,
            gross_realized_pnl: 0.0,
            fees: 0.0,
            fees_complete: true,
            revision: 0,
            seen_counts: HashMap::new(),
        }
    }
}

impl FillLedger {
    pub(super) fn with_allocated_margin(allocated_margin: f64) -> Self {
        Self {
            allocated_margin,
            ..Self::default()
        }
    }

    pub(super) fn inventory(&self) -> f64 {
        self.position_size
    }

    pub(super) fn average_entry_price(&self) -> Option<f64> {
        (self.position_size.abs() > f64::EPSILON).then_some(self.average_entry_price)
    }

    pub(super) fn key(
        order_id: &str,
        timestamp: u64,
        buy: bool,
        size: f64,
        price: f64,
    ) -> Option<FillKey> {
        if !size.is_finite() || size <= 0.0 || !price.is_finite() || price <= 0.0 {
            return None;
        }
        Some(FillKey {
            order_id: order_id.to_string(),
            timestamp,
            size_bits: size.to_bits(),
            price_bits: price.to_bits(),
            buy,
        })
    }

    pub(super) fn record_live(&mut self, order_id: &str, fill: &ObservedFill) -> bool {
        let Some(key) = Self::key(order_id, fill.timestamp, fill.buy, fill.size, fill.price) else {
            return false;
        };
        *self.seen_counts.entry(key).or_default() += 1;
        self.add(fill)
    }

    pub(super) fn record_recovery_occurrence(
        &mut self,
        order_id: &str,
        fill: &ObservedFill,
        occurrence: usize,
    ) -> bool {
        let Some(key) = Self::key(order_id, fill.timestamp, fill.buy, fill.size, fill.price) else {
            return false;
        };
        let seen = self.seen_counts.entry(key).or_default();
        if occurrence <= *seen {
            return false;
        }
        *seen = occurrence;
        self.add(fill)
    }

    fn add(&mut self, fill: &ObservedFill) -> bool {
        if fill.buy {
            self.bought_size += fill.size;
            self.bought_notional += fill.size * fill.price;
        } else {
            self.sold_size += fill.size;
            self.sold_notional += fill.size * fill.price;
        }
        match fill.fee {
            Some(fee) if fee.is_finite() => self.fees += fee,
            _ => self.fees_complete = false,
        }

        let signed_size = if fill.buy { fill.size } else { -fill.size };
        if self.position_size.abs() <= f64::EPSILON
            || self.position_size.signum() == signed_size.signum()
        {
            let current_size = self.position_size.abs();
            let new_size = current_size + fill.size;
            self.average_entry_price = if new_size > 0.0 {
                (self.average_entry_price * current_size + fill.price * fill.size) / new_size
            } else {
                0.0
            };
            self.position_size += signed_size;
        } else {
            let previous_position = self.position_size;
            let closing_size = previous_position.abs().min(fill.size);
            self.gross_realized_pnl += if previous_position > 0.0 {
                (fill.price - self.average_entry_price) * closing_size
            } else {
                (self.average_entry_price - fill.price) * closing_size
            };
            self.position_size += signed_size;
            if self.position_size.abs() <= f64::EPSILON {
                self.position_size = 0.0;
                self.average_entry_price = 0.0;
            } else if self.position_size.signum() != previous_position.signum() {
                self.average_entry_price = fill.price;
            }
        }
        self.revision += 1;
        true
    }

    pub(super) fn performance(&self, mark_price: f64) -> BotPerformance {
        let unrealized_pnl = if self.position_size > 0.0 {
            (mark_price - self.average_entry_price) * self.position_size
        } else if self.position_size < 0.0 {
            (self.average_entry_price - mark_price) * self.position_size.abs()
        } else {
            0.0
        };
        let trading_pnl = self
            .fees_complete
            .then_some(self.gross_realized_pnl + unrealized_pnl + self.fees);
        BotPerformance {
            allocated_margin: self.allocated_margin,
            bought_size: self.bought_size,
            sold_size: self.sold_size,
            matched_size: self.bought_size.min(self.sold_size),
            average_buy_price: (self.bought_size > 0.0)
                .then_some(self.bought_notional / self.bought_size),
            average_sell_price: (self.sold_size > 0.0)
                .then_some(self.sold_notional / self.sold_size),
            inventory_size: self.position_size,
            average_entry_price: (self.position_size != 0.0).then_some(self.average_entry_price),
            mark_price,
            gross_realized_pnl: self.gross_realized_pnl,
            unrealized_pnl,
            fees: self.fees,
            fees_complete: self.fees_complete,
            trading_pnl,
            return_on_margin_pct: trading_pnl.and_then(|pnl| {
                (self.allocated_margin > 0.0).then_some(pnl / self.allocated_margin * 100.0)
            }),
        }
    }
}

enum BotOrderBookStream {
    Bulk(BulkOrderBookStream),
    Hyperliquid(HyperliquidOrderBookStream),
}

impl BotOrderBookStream {
    async fn connect(venue: ExecutionVenue, symbol: &str, testnet: bool) -> Result<Self> {
        match venue {
            ExecutionVenue::Bulk => Ok(Self::Bulk(
                BulkOrderBookStream::connect(symbol, BOOK_DEPTH).await?,
            )),
            ExecutionVenue::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidOrderBookStream::connect_on(
                    symbol,
                    BOOK_DEPTH,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
        }
    }

    async fn next_top(&mut self) -> Result<TopOfBook> {
        match self {
            Self::Bulk(stream) => stream.next_top().await,
            Self::Hyperliquid(stream) => stream.next_top().await,
        }
    }
}

pub(super) fn spawn_book_feed(
    venue: ExecutionVenue,
    testnet: bool,
    symbol: String,
) -> watch::Receiver<BookFeedState> {
    let (sender, receiver) = watch::channel(BookFeedState::default());
    tokio::spawn(async move {
        let mut delay = 1_u64;
        let mut revision = 0_u64;
        loop {
            match BotOrderBookStream::connect(venue, &symbol, testnet).await {
                Ok(mut stream) => {
                    delay = 1;
                    loop {
                        match stream.next_top().await {
                            Ok(top) => {
                                revision = revision.saturating_add(1);
                                if sender
                                    .send(BookFeedState {
                                        revision,
                                        top: Some(top),
                                        error: None,
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(error) => {
                                if sender
                                    .send(BookFeedState {
                                        revision,
                                        top: None,
                                        error: Some(format!("{error:#}")),
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    if sender
                        .send(BookFeedState {
                            revision,
                            top: None,
                            error: Some(format!("{error:#}")),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(delay)).await;
            delay = (delay * 2).min(RECONNECT_MAX_SECONDS);
        }
    });
    receiver
}

enum BotAccountStream {
    Bulk(BulkAccountStream),
    Hyperliquid(HyperliquidAccountStream),
}

impl BotAccountStream {
    async fn connect(venue: ExecutionVenue, account: &str, testnet: bool) -> Result<Self> {
        match venue {
            ExecutionVenue::Bulk => Ok(Self::Bulk(BulkAccountStream::connect(account).await?)),
            ExecutionVenue::Hyperliquid => Ok(Self::Hyperliquid(
                HyperliquidAccountStream::connect_on(
                    account,
                    HyperliquidNetwork::from_testnet(testnet),
                )
                .await?,
            )),
        }
    }

    async fn next_events(&mut self) -> Result<Vec<Value>> {
        match self {
            Self::Bulk(stream) => Ok(vec![stream.next_event().await?]),
            Self::Hyperliquid(stream) => {
                normalize_hyperliquid_account_events(stream.next_event().await?)
            }
        }
    }
}

pub(super) fn spawn_account_feed(
    venue: ExecutionVenue,
    testnet: bool,
    account: String,
) -> mpsc::Receiver<AccountFeedEvent> {
    let (sender, receiver) = mpsc::channel(1024);
    tokio::spawn(async move {
        let adapter = match ExecutionAdapter::new(venue, testnet).await {
            Ok(adapter) => adapter,
            Err(error) => {
                let _ = sender
                    .send(AccountFeedEvent::Disconnected(format!("{error:#}")))
                    .await;
                return;
            }
        };
        let mut delay = 1_u64;
        loop {
            match BotAccountStream::connect(venue, &account, testnet).await {
                Ok(mut stream) => {
                    let (open_orders, fills) =
                        tokio::join!(adapter.open_orders(&account), adapter.fills(&account),);
                    match (open_orders, fills) {
                        (Ok(open_orders), Ok(fills)) => {
                            if sender
                                .send(AccountFeedEvent::Recovery { open_orders, fills })
                                .await
                                .is_err()
                                || sender.send(AccountFeedEvent::Connected).await.is_err()
                            {
                                return;
                            }
                        }
                        (open_orders, fills) => {
                            let error = format!(
                                "account recovery failed: openOrders={}; fills={}",
                                result_error(open_orders),
                                result_error(fills),
                            );
                            if sender
                                .send(AccountFeedEvent::Disconnected(error))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            tokio::time::sleep(Duration::from_secs(delay)).await;
                            delay = (delay * 2).min(RECONNECT_MAX_SECONDS);
                            continue;
                        }
                    }
                    delay = 1;
                    loop {
                        match stream.next_events().await {
                            Ok(values) => {
                                for value in values {
                                    if sender.send(AccountFeedEvent::Data(value)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            Err(error) => {
                                if sender
                                    .send(AccountFeedEvent::Disconnected(format!("{error:#}")))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    if sender
                        .send(AccountFeedEvent::Disconnected(format!("{error:#}")))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(delay)).await;
            delay = (delay * 2).min(RECONNECT_MAX_SECONDS);
        }
    });
    receiver
}

fn result_error<T>(result: Result<T>) -> String {
    result.map_or_else(|error| format!("{error:#}"), |_| "ok".to_string())
}

fn normalize_hyperliquid_account_events(value: Value) -> Result<Vec<Value>> {
    match value.get("channel").and_then(Value::as_str) {
        Some("orderUpdates") => value
            .get("data")
            .and_then(Value::as_array)
            .context("Hyperliquid orderUpdates omitted its update list")?
            .iter()
            .map(normalize_hyperliquid_order_update)
            .collect(),
        Some("user") => value
            .pointer("/data/fills")
            .and_then(Value::as_array)
            .map_or_else(
                || Ok(Vec::new()),
                |fills| fills.iter().map(normalize_hyperliquid_fill).collect(),
            ),
        _ => Ok(Vec::new()),
    }
}

fn normalize_hyperliquid_order_update(update: &Value) -> Result<Value> {
    let order = update
        .get("order")
        .context("Hyperliquid order update omitted order")?;
    let order_id = json_identifier(
        order
            .get("oid")
            .context("Hyperliquid order update omitted oid")?,
    )?;
    let raw_status = update
        .get("status")
        .and_then(Value::as_str)
        .context("Hyperliquid order update omitted status")?;
    let size = json_number(order.get("sz"), "order size")?.unwrap_or_default();
    let original_size = json_number(order.get("origSz"), "original order size")?.unwrap_or(size);
    let is_buy = order.get("side").and_then(Value::as_str) != Some("A");
    let signed_size = if is_buy { size } else { -size };
    Ok(serde_json::json!({
        "type": "orderUpdate",
        "oid": order_id,
        "status": normalize_hyperliquid_order_status(raw_status),
        "ts": update.get("statusTimestamp").and_then(Value::as_u64).unwrap_or_default(),
        "px": json_number(order.get("limitPx"), "order price")?.unwrap_or_default(),
        "origSz": original_size,
        "sz": signed_size,
        "isBuy": is_buy,
    }))
}

fn normalize_hyperliquid_fill(fill: &Value) -> Result<Value> {
    let order_id = json_identifier(fill.get("oid").context("Hyperliquid fill omitted oid")?)?;
    let raw_fee = json_number(fill.get("fee"), "fill fee")?;
    Ok(serde_json::json!({
        "type": "fill",
        "orderId": order_id,
        "timestamp": fill.get("time").and_then(Value::as_u64).unwrap_or_default(),
        "isBuy": fill.get("side").and_then(Value::as_str) == Some("B"),
        "size": json_number(fill.get("sz"), "fill size")?.unwrap_or_default(),
        "price": json_number(fill.get("px"), "fill price")?.unwrap_or_default(),
        // Hyperliquid reports a positive number for a cost and a negative number
        // for a rebate. Market Lab's performance ledger uses the opposite sign.
        "fee": raw_fee.map(|fee| -fee),
    }))
}

fn json_identifier(value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => bail!("Hyperliquid order id is neither a string nor an integer"),
    }
}

fn json_number(value: Option<&Value>, field: &str) -> Result<Option<f64>> {
    value
        .map(|value| match value {
            Value::Number(number) => number
                .as_f64()
                .with_context(|| format!("invalid Hyperliquid {field}")),
            Value::String(number) => number
                .parse::<f64>()
                .with_context(|| format!("invalid Hyperliquid {field} `{number}`")),
            Value::Null => Ok(0.0),
            _ => bail!("invalid Hyperliquid {field}"),
        })
        .transpose()
}

fn normalize_hyperliquid_order_status(status: &str) -> &str {
    if status.eq_ignore_ascii_case("open") {
        "resting"
    } else if status.eq_ignore_ascii_case("filled") {
        "filled"
    } else if status.eq_ignore_ascii_case("canceled") || status.eq_ignore_ascii_case("cancelled") {
        "cancelled"
    } else if status.eq_ignore_ascii_case("rejected") || status.ends_with("Canceled") {
        "rejected"
    } else {
        status
    }
}

#[derive(Clone, Debug)]
pub(super) struct ObservedFill {
    pub(super) timestamp: u64,
    pub(super) recovered: bool,
    pub(super) buy: bool,
    pub(super) size: f64,
    pub(super) price: f64,
    /// Signed venue fee: negative is a cost and positive is a rebate.
    pub(super) fee: Option<f64>,
}

async fn run_worker(job_id: &str, mode: MidMode, definition: &MidPriceJobDefinition) -> Result<()> {
    let parent = build_trade_plan(&worker_trade_args(definition), PositionDirection::Long).await?;
    let market = execution_market(definition.venue, &definition.symbol)?;
    let rules = market.execution_rules()?;
    let adapter = ExecutionAdapter::new(definition.venue, definition.testnet).await?;

    let initial_book =
        live_orderbook(definition.venue, &definition.symbol, definition.testnet).await?;
    let initial_quotes = quote_prices(
        initial_book
            .bids
            .first()
            .copied()
            .with_context(|| format!("{} book has no bid", venue_label(definition.venue)))?,
        initial_book
            .asks
            .first()
            .copied()
            .with_context(|| format!("{} book has no ask", venue_label(definition.venue)))?,
        definition.spread_bps,
        rules.tick_size,
        rules.price_precision,
    )?;
    let initial_sizes = quote_sizes(
        definition.max_inventory_size,
        0.0,
        definition.directional_bias_percent,
    )?;
    let initial_bid_size = desired_quote(
        initial_sizes.bid_size,
        initial_quotes.bid_price,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    )
    .map_or(0.0, |(_, size)| size);
    let initial_ask_size = desired_quote(
        initial_sizes.ask_size,
        initial_quotes.ask_price,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    )
    .map_or(0.0, |(_, size)| size);
    crate::runtime::append_bot_output(
        job_id,
        &plan_view(
            mode.name(),
            &parent,
            definition,
            initial_quotes,
            initial_bid_size,
            initial_ask_size,
            false,
        ),
    )?;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(definition.duration_seconds);
    let mut book = spawn_book_feed(
        definition.venue,
        definition.testnet,
        definition.symbol.clone(),
    );
    let mut account_events =
        spawn_account_feed(definition.venue, definition.testnet, parent.account.clone());
    let mut account_connected = false;
    let mut buy = QuoteSlot::default();
    let mut sell = QuoteSlot::default();
    let allocated_margin = definition
        .requested_margin
        .unwrap_or(definition.max_inventory_margin);
    let mut ledger = FillLedger::with_allocated_margin(allocated_margin);
    let mut persisted_revision = 0_u64;
    let mut owned_orders = HashSet::new();
    let mut pending_fills = HashMap::<String, Vec<ObservedFill>>::new();
    let mut terminal_statuses = HashMap::<String, String>::new();
    let mut actions = JoinSet::<ActionCompletion>::new();
    let mut order_sequence = 0_u64;
    let mut cancel_sequence = 0_u64;
    let minimum_quote_age = mode.minimum_quote_age(definition);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install bot worker termination handler")?;

    let outcome: Result<&'static str> = async {
        let outcome = loop {
            let policy = tokio::select! {
                changed = book.changed() => {
                    if changed.is_err() {
                        bail!("mid-price order-book task stopped");
                    }
                    let state = book.borrow().clone();
                    if let Some(error) = state.error {
                        append_market_data(
                            job_id,
                            mode.name(),
                            "orderbook",
                            "disconnected",
                            Some(&error),
                        )?;
                    }
                    Some(ReconcilePolicy::PASSIVE)
                }
                event = account_events.recv() => {
                    match event.context("mid-price account-event task stopped")? {
                        AccountFeedEvent::Connected => {
                            account_connected = true;
                            append_market_data(
                                job_id,
                                mode.name(),
                                "account",
                                "connected",
                                None,
                            )?;
                        }
                        AccountFeedEvent::Disconnected(error) => {
                            account_connected = false;
                            append_market_data(
                                job_id,
                                mode.name(),
                                "account",
                                "disconnected",
                                Some(&error),
                            )?;
                        }
                        AccountFeedEvent::Recovery { open_orders, fills } => {
                            reconcile_recovery(
                                RecoveryContext {
                                    job_id,
                                    bot: mode.name(),
                                    mark_price: current_mark(&book, parent.reference_price),
                                    open_orders: &open_orders,
                                    owned_orders: &owned_orders,
                                },
                                fills,
                                &mut buy,
                                &mut sell,
                                &mut ledger,
                            )?;
                        }
                        AccountFeedEvent::Data(value) => {
                            apply_account_event(
                                job_id,
                                mode.name(),
                                current_mark(&book, parent.reference_price),
                                value,
                                &owned_orders,
                                &mut pending_fills,
                                &mut terminal_statuses,
                                &mut buy,
                                &mut sell,
                                &mut ledger,
                            )?;
                        }
                    }
                    Some(ReconcilePolicy::EXECUTION)
                }
                completion = actions.join_next(), if !actions.is_empty() => {
                    let completion = completion
                        .context("mid-price action set ended unexpectedly")?
                        .context("mid-price action task panicked")?;
                    apply_action_completion(
                        job_id,
                        mode.name(),
                        current_mark(&book, parent.reference_price),
                        completion,
                        &mut buy,
                        &mut sell,
                        &mut owned_orders,
                        &mut pending_fills,
                        &mut terminal_statuses,
                        &mut ledger,
                    )?;
                    Some(ReconcilePolicy::EXECUTION)
                }
                _ = heartbeat.tick() => {
                    let performance = ledger.performance(current_mark(&book, parent.reference_price));
                    crate::runtime::bot_worker_heartbeat(
                        job_id,
                        std::process::id(),
                        Some(&performance),
                    ).await?;
                    persisted_revision = ledger.revision;
                    None
                }
                _ = &mut deadline_sleep => break "duration_elapsed",
                _ = terminate.recv() => break "stopped",
                _ = tokio::signal::ctrl_c() => break "stopped",
            };

            let mark_price = current_mark(&book, parent.reference_price);
            let performance = ledger.performance(mark_price);
            if let Some(max_loss) = stop_loss_amount(definition, allocated_margin)
                && stop_loss_triggered(&performance, max_loss)
            {
                crate::runtime::bot_worker_heartbeat(
                    job_id,
                    std::process::id(),
                    Some(&performance),
                )
                .await?;
                append_stop_loss(
                    job_id,
                    mode.name(),
                    definition.stop_loss_pct.unwrap_or_default(),
                    max_loss,
                    mark_price,
                    &performance,
                )?;
                break "stop_loss";
            }

            let Some(policy) = policy else {
                continue;
            };

            let state = book.borrow().clone();
            let book_revision = state.revision;
            let desired = if account_connected {
                state.top.and_then(|top| {
                    let bid = top.best_bid?;
                    let ask = top.best_ask?;
                    quote_prices(
                        bid,
                        ask,
                        definition.spread_bps,
                        rules.tick_size,
                        rules.price_precision,
                    )
                    .ok()
                })
            } else {
                None
            };
            let inventory = ledger.inventory();
            let quote_sizes = quote_sizes(
                definition.max_inventory_size,
                inventory,
                definition.directional_bias_percent,
            )?;
            let buy_desired = desired.and_then(|quotes| {
                desired_quote(
                    quote_sizes.bid_size,
                    quotes.bid_price,
                    rules.lot_size,
                    rules.size_precision,
                    rules.min_notional,
                )
            });
            let sell_desired = desired.and_then(|quotes| {
                desired_quote(
                    quote_sizes.ask_size,
                    quotes.ask_price,
                    rules.lot_size,
                    rules.size_precision,
                    rules.min_notional,
                )
            });

            reconcile_quote(
                job_id,
                QuoteSide::Buy,
                &parent,
                buy_desired,
                rules.lot_size,
                rules.tick_size,
                rules.min_notional,
                minimum_quote_age,
                definition.refresh_tolerance_bps,
                mode,
                policy,
                book_revision,
                &mut buy,
                &mut actions,
                &mut order_sequence,
                &mut cancel_sequence,
            )?;

            reconcile_quote(
                job_id,
                QuoteSide::Sell,
                &parent,
                sell_desired,
                rules.lot_size,
                rules.tick_size,
                rules.min_notional,
                minimum_quote_age,
                definition.refresh_tolerance_bps,
                mode,
                policy,
                book_revision,
                &mut sell,
                &mut actions,
                &mut order_sequence,
                &mut cancel_sequence,
            )?;

            if ledger.revision != persisted_revision {
                let performance = ledger.performance(
                    desired.map_or(parent.reference_price, |quotes| quotes.reference_price),
                );
                crate::runtime::bot_worker_heartbeat(
                    job_id,
                    std::process::id(),
                    Some(&performance),
                )
                .await?;
                persisted_revision = ledger.revision;
            }

        };
        Ok(outcome)
    }
    .await;

    let mut action_error = None;
    while let Some(completion) = actions.join_next().await {
        match completion {
            Ok(completion) => {
                if let Err(error) = apply_action_completion(
                    job_id,
                    mode.name(),
                    current_mark(&book, parent.reference_price),
                    completion,
                    &mut buy,
                    &mut sell,
                    &mut owned_orders,
                    &mut pending_fills,
                    &mut terminal_statuses,
                    &mut ledger,
                ) {
                    action_error.get_or_insert(error);
                }
            }
            Err(error) => {
                action_error
                    .get_or_insert_with(|| anyhow::anyhow!("bot action task failed: {error}"));
            }
        }
    }
    let outcome = match (outcome, action_error) {
        (Ok(outcome), None) => Ok(outcome),
        (Err(error), None) | (_, Some(error)) => Err(error),
    };
    let cleanup_result = cleanup(
        job_id,
        mode.name(),
        current_mark(&book, parent.reference_price),
        definition,
        &parent,
        &adapter,
        &mut buy,
        &mut sell,
        &mut ledger,
        &mut owned_orders,
    )
    .await;
    let performance = ledger.performance(current_mark(&book, parent.reference_price));
    let performance_update =
        crate::runtime::bot_worker_heartbeat(job_id, std::process::id(), Some(&performance)).await;
    let outcome = match (outcome, cleanup_result) {
        (Ok(outcome), Ok(())) => outcome,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(cleanup)) => return Err(cleanup),
        (Err(error), Err(cleanup)) => {
            return Err(error).context(format!("bot cleanup also failed: {cleanup:#}"));
        }
    };
    performance_update?;
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.run.finished",
            "bot": mode.name(),
            "jobId": job_id,
            "status": outcome,
            "boughtSize": ledger.bought_size,
            "soldSize": ledger.sold_size,
            "residualSize": ledger.inventory(),
            "performance": performance,
            "elapsedMs": started.elapsed().as_millis(),
        }),
    )?;
    if outcome == "stopped" {
        Err(BotStopped.into())
    } else {
        Ok(())
    }
}

fn desired_quote(
    requested_size: f64,
    price: f64,
    lot_size: f64,
    size_precision: u8,
    min_notional: f64,
) -> Option<(f64, f64)> {
    let size = floor_to_step(requested_size, lot_size, size_precision);
    (size >= lot_size / 2.0 && size * price >= min_notional).then_some((price, size))
}

fn quote_is_stale_away_from_market(
    side: QuoteSide,
    current_price: f64,
    proposed_price: f64,
    tolerance_bps: f64,
) -> bool {
    let moved_away = match side {
        QuoteSide::Buy => proposed_price > current_price,
        QuoteSide::Sell => proposed_price < current_price,
    };
    moved_away && (proposed_price - current_price).abs() / current_price * 10_000.0 > tolerance_bps
}

#[derive(Clone, Copy)]
struct QuoteRefreshCheck {
    mode: MidMode,
    side: QuoteSide,
    current_price: f64,
    proposed_price: f64,
    tick_size: f64,
    tolerance_bps: f64,
    resting_age: Duration,
    minimum_age: Duration,
}

fn should_refresh_quote_price(check: QuoteRefreshCheck) -> bool {
    if check.resting_age < check.minimum_age {
        return false;
    }
    match check.mode {
        MidMode::QuoteProtection => {
            let absolute_threshold = (check.tick_size * 2.0)
                .max(check.current_price * STRICT_REFRESH_TOLERANCE_BPS / 10_000.0);
            (check.proposed_price - check.current_price).abs() >= absolute_threshold
        }
        MidMode::FillPriority => quote_is_stale_away_from_market(
            check.side,
            check.current_price,
            check.proposed_price,
            check.tolerance_bps,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn reconcile_quote(
    job_id: &str,
    side: QuoteSide,
    parent: &TradePlan,
    desired: Option<(f64, f64)>,
    lot_size: f64,
    tick_size: f64,
    min_notional: f64,
    minimum_quote_age: Duration,
    refresh_tolerance_bps: f64,
    mode: MidMode,
    policy: ReconcilePolicy,
    book_revision: u64,
    slot: &mut QuoteSlot,
    actions: &mut JoinSet<ActionCompletion>,
    order_sequence: &mut u64,
    cancel_sequence: &mut u64,
) -> Result<()> {
    if slot.busy || !slot.accepts_book_revision(book_revision) {
        return Ok(());
    }

    if let Some(live) = slot.live.as_mut() {
        let replace = desired.is_none_or(|(price, size)| {
            let price_moved = policy.replace_price
                && should_refresh_quote_price(QuoteRefreshCheck {
                    mode,
                    side,
                    current_price: live.price,
                    proposed_price: price,
                    tick_size,
                    tolerance_bps: refresh_tolerance_bps,
                    resting_age: live.submitted_at.elapsed(),
                    minimum_age: minimum_quote_age,
                });
            let size_difference = (live.size - size).abs();
            let size_changed = policy.replace_size
                && size_difference >= lot_size
                && size_difference * price >= min_notional;
            price_moved || size_changed
        });
        if replace && !live.cancel_requested {
            live.cancel_requested = true;
            slot.busy = true;
            *cancel_sequence = cancel_sequence.saturating_add(1);
            let sequence = *cancel_sequence;
            let order_id = live.order_id.clone();
            let plan = cancel_plan(parent, order_id.clone())?;
            let job_id = job_id.to_string();
            actions.spawn(async move {
                let result = crate::runtime::submit_bot_cancel(&job_id, sequence, &plan)
                    .await
                    .map_err(|error| format!("{error:#}"));
                ActionCompletion {
                    side,
                    kind: ActionKind::Cancel { order_id },
                    result,
                }
            });
        }
        return Ok(());
    }

    let Some((price, size)) = desired else {
        return Ok(());
    };
    slot.busy = true;
    *order_sequence = order_sequence.saturating_add(1);
    let sequence = *order_sequence;
    let plan = quote_plan(parent, side, size, price)?;
    let job_id = job_id.to_string();
    actions.spawn(async move {
        let result = crate::runtime::submit_bot_trade(&job_id, sequence, &plan)
            .await
            .map_err(|error| format!("{error:#}"));
        ActionCompletion {
            side,
            kind: ActionKind::Submit {
                price,
                size,
                book_revision,
            },
            result,
        }
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_action_completion(
    job_id: &str,
    bot: &str,
    mark_price: f64,
    completion: ActionCompletion,
    buy: &mut QuoteSlot,
    sell: &mut QuoteSlot,
    owned_orders: &mut HashSet<String>,
    pending_fills: &mut HashMap<String, Vec<ObservedFill>>,
    terminal_statuses: &mut HashMap<String, String>,
    ledger: &mut FillLedger,
) -> Result<()> {
    let slot = QuoteSlot::for_side(completion.side, buy, sell);
    slot.busy = false;
    match (completion.kind, completion.result) {
        (
            ActionKind::Submit {
                price,
                size,
                book_revision: _,
            },
            Ok(receipt),
        ) => {
            let order_id = receipt
                .order_id
                .context("mid-price quote receipt omitted its order id")?;
            owned_orders.insert(order_id.clone());
            let mut remaining_size = size;
            if let Some(fills) = pending_fills.remove(&order_id) {
                for fill in fills {
                    if record_live_fill(job_id, bot, mark_price, ledger, &order_id, &fill)? {
                        remaining_size = (remaining_size - fill.size).max(0.0);
                    }
                }
            }
            let terminal_status = terminal_statuses.remove(&order_id);
            let terminal = receipt.terminal || terminal_status.is_some();
            if !terminal && remaining_size > f64::EPSILON {
                slot.live = Some(WorkingQuote {
                    order_id: order_id.clone(),
                    price,
                    size: remaining_size,
                    submitted_at: Instant::now(),
                    cancel_requested: false,
                });
            }
            append_quote(
                job_id,
                bot,
                completion.side,
                terminal_status.as_deref().unwrap_or(&receipt.status),
                &order_id,
                price,
                size,
            )?;
        }
        (
            ActionKind::Submit {
                price,
                size,
                book_revision,
            },
            Err(error),
        ) => {
            let crossing = is_post_only_crossing_message(&error);
            append_quote(
                job_id,
                bot,
                completion.side,
                if crossing {
                    "rejectedCrossing"
                } else {
                    "rejected"
                },
                "-",
                price,
                size,
            )?;
            if crossing {
                slot.retry_after_book_revision = Some(book_revision);
            } else {
                bail!(
                    "{} quote submission failed: {error}",
                    completion.side.name()
                );
            }
        }
        (ActionKind::Cancel { order_id }, Ok(receipt)) => {
            if receipt.terminal
                && slot
                    .live
                    .as_ref()
                    .is_some_and(|quote| quote.order_id == order_id)
            {
                slot.live = None;
            }
        }
        (ActionKind::Cancel { order_id }, Err(error)) => {
            if is_order_gone_message(&error) {
                if slot
                    .live
                    .as_ref()
                    .is_some_and(|quote| quote.order_id == order_id)
                {
                    slot.live = None;
                }
                return Ok(());
            }
            if let Some(live) = slot.live.as_mut()
                && live.order_id == order_id
            {
                live.cancel_requested = false;
            }
            bail!(
                "{} quote cancellation failed: {error}",
                completion.side.name()
            );
        }
    }
    Ok(())
}

pub(super) fn quote_plan(
    parent: &TradePlan,
    side: QuoteSide,
    size: f64,
    price: f64,
) -> Result<TradePlan> {
    let exposure = size * price;
    Ok(TradePlan {
        created_at_ms: now_ms()?,
        venue: parent.venue,
        testnet: parent.testnet,
        account: parent.account.clone(),
        internal_symbol: parent.internal_symbol.clone(),
        venue_symbol: parent.venue_symbol.clone(),
        direction: side.direction(),
        side: side.order_side(),
        order_kind: OrderKind::Limit,
        time_in_force: Some(TimeInForce::Alo),
        requested_size: Some(size),
        size,
        price: Some(price),
        reference_price: price,
        requested_margin: None,
        estimated_margin: exposure / parent.leverage,
        estimated_exposure: exposure,
        projected_liquidation_price: None,
        leverage: parent.leverage,
        reduce_only: false,
        stop_loss_price: None,
        take_profit_price: None,
    })
}

pub(super) fn inventory_unwind_plan(
    parent: &TradePlan,
    direction: PositionDirection,
    size: f64,
    price: f64,
) -> Result<TradePlan> {
    let exposure = size * price;
    Ok(TradePlan {
        created_at_ms: now_ms()?,
        venue: parent.venue,
        testnet: parent.testnet,
        account: parent.account.clone(),
        internal_symbol: parent.internal_symbol.clone(),
        venue_symbol: parent.venue_symbol.clone(),
        direction,
        side: OrderSide::from(direction),
        order_kind: OrderKind::Market,
        time_in_force: None,
        requested_size: Some(size),
        size,
        price: None,
        reference_price: price,
        requested_margin: None,
        estimated_margin: exposure / parent.leverage,
        estimated_exposure: exposure,
        projected_liquidation_price: None,
        leverage: parent.leverage,
        // This closes the bot's virtual inventory, not necessarily the account's net position.
        // With another strategy on the opposite side, an unwind can increase the account net.
        reduce_only: false,
        stop_loss_price: None,
        take_profit_price: None,
    })
}

pub(super) fn cancel_plan(parent: &TradePlan, order_id: String) -> Result<CancelPlan> {
    Ok(CancelPlan {
        created_at_ms: now_ms()?,
        venue: parent.venue,
        testnet: parent.testnet,
        account: parent.account.clone(),
        internal_symbol: parent.internal_symbol.clone(),
        venue_symbol: parent.venue_symbol.clone(),
        order_id,
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_account_event(
    job_id: &str,
    bot: &str,
    mark_price: f64,
    value: Value,
    owned_orders: &HashSet<String>,
    pending_fills: &mut HashMap<String, Vec<ObservedFill>>,
    terminal_statuses: &mut HashMap<String, String>,
    buy: &mut QuoteSlot,
    sell: &mut QuoteSlot,
    ledger: &mut FillLedger,
) -> Result<()> {
    match value.get("type").and_then(Value::as_str) {
        Some("fill") => {
            let Some(order_id) = value.get("orderId").and_then(Value::as_str) else {
                return Ok(());
            };
            let timestamp = value
                .get("timestamp")
                .and_then(Value::as_u64)
                .or_else(|| value.get("ts").and_then(Value::as_u64))
                .unwrap_or_default();
            let fill = ObservedFill {
                timestamp: normalize_timestamp_ms(timestamp),
                recovered: false,
                buy: value.get("isBuy").and_then(Value::as_bool).unwrap_or(false),
                size: value.get("size").and_then(Value::as_f64).unwrap_or(0.0),
                price: value.get("price").and_then(Value::as_f64).unwrap_or(0.0),
                fee: value.get("fee").and_then(Value::as_f64),
            };
            if owned_orders.contains(order_id) {
                if record_live_fill(job_id, bot, mark_price, ledger, order_id, &fill)? {
                    apply_fill_to_working_quote(order_id, fill.size, buy, sell);
                }
            } else {
                pending_fills
                    .entry(order_id.to_string())
                    .or_default()
                    .push(fill);
            }
        }
        Some("orderUpdate") => {
            let Some(order_id) = value.get("oid").and_then(Value::as_str) else {
                return Ok(());
            };
            let Some(status) = value.get("status").and_then(Value::as_str) else {
                return Ok(());
            };
            let working_size = [&*buy, &*sell]
                .into_iter()
                .filter_map(|slot| slot.live.as_ref())
                .find(|quote| quote.order_id == order_id)
                .map(|quote| quote.size);
            if is_terminal_order_status(status) {
                terminal_statuses.insert(order_id.to_string(), status.to_string());
                clear_live_order(order_id, buy, sell);
            }
            if owned_orders.contains(order_id) && is_terminal_order_status(status) {
                let side = value
                    .get("isBuy")
                    .and_then(Value::as_bool)
                    .map(|buy| if buy { QuoteSide::Buy } else { QuoteSide::Sell })
                    .unwrap_or_else(|| {
                        value
                            .get("sz")
                            .and_then(Value::as_f64)
                            .map_or(QuoteSide::Buy, |size| {
                                if size < 0.0 {
                                    QuoteSide::Sell
                                } else {
                                    QuoteSide::Buy
                                }
                            })
                    });
                let original_size = value
                    .get("origSz")
                    .and_then(Value::as_f64)
                    .filter(|size| *size > 0.0)
                    .or(working_size)
                    .unwrap_or_default()
                    .abs();
                append_quote(
                    job_id,
                    bot,
                    side,
                    status,
                    order_id,
                    value.get("px").and_then(Value::as_f64).unwrap_or(0.0),
                    original_size,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct RecoveryContext<'a> {
    job_id: &'a str,
    bot: &'a str,
    mark_price: f64,
    open_orders: &'a [OpenOrder],
    owned_orders: &'a HashSet<String>,
}

fn reconcile_recovery(
    context: RecoveryContext<'_>,
    fills: Vec<Fill>,
    buy: &mut QuoteSlot,
    sell: &mut QuoteSlot,
    ledger: &mut FillLedger,
) -> Result<()> {
    let open_ids = context
        .open_orders
        .iter()
        .map(|order| order.order_id.as_str())
        .collect::<HashSet<_>>();
    for slot in [&mut *buy, &mut *sell] {
        if let Some(quote) = slot.live.as_mut() {
            if open_ids.contains(quote.order_id.as_str()) {
                quote.cancel_requested = false;
            } else {
                slot.live = None;
            }
        }
    }
    let mut response_counts = HashMap::<FillKey, usize>::new();
    for fill in fills {
        let Some(order_id) = fill.order_id.as_deref() else {
            continue;
        };
        if !context.owned_orders.contains(order_id) {
            continue;
        }
        let observed = ObservedFill {
            timestamp: fill.ts_ms,
            recovered: true,
            buy: fill.side == OrderSide::Buy,
            size: fill.amount,
            price: fill.price,
            fee: fill.fee,
        };
        let Some(key) = FillLedger::key(
            order_id,
            observed.timestamp,
            observed.buy,
            observed.size,
            observed.price,
        ) else {
            continue;
        };
        let occurrence = response_counts.entry(key).or_default();
        *occurrence += 1;
        if ledger.record_recovery_occurrence(order_id, &observed, *occurrence) {
            append_fill(
                context.job_id,
                context.bot,
                context.mark_price,
                ledger,
                order_id,
                &observed,
            )?;
            apply_fill_to_working_quote(order_id, observed.size, buy, sell);
        }
    }
    Ok(())
}

fn record_live_fill(
    job_id: &str,
    bot: &str,
    mark_price: f64,
    ledger: &mut FillLedger,
    order_id: &str,
    fill: &ObservedFill,
) -> Result<bool> {
    if !ledger.record_live(order_id, fill) {
        return Ok(false);
    }
    append_fill(job_id, bot, mark_price, ledger, order_id, fill)?;
    Ok(true)
}

fn apply_fill_to_working_quote(
    order_id: &str,
    filled_size: f64,
    buy: &mut QuoteSlot,
    sell: &mut QuoteSlot,
) {
    for slot in [buy, sell] {
        let Some(live) = slot.live.as_mut() else {
            continue;
        };
        if live.order_id != order_id {
            continue;
        }
        live.size = (live.size - filled_size).max(0.0);
        if live.size <= f64::EPSILON {
            slot.live = None;
        }
        return;
    }
}

pub(super) fn append_fill(
    job_id: &str,
    bot: &str,
    mark_price: f64,
    ledger: &FillLedger,
    order_id: &str,
    fill: &ObservedFill,
) -> Result<()> {
    let performance = ledger.performance(mark_price);
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.fill",
            "bot": bot,
            "jobId": job_id,
            "orderId": order_id,
            "venueTsMs": fill.timestamp,
            "recovered": fill.recovered,
            "side": if fill.buy { "BUY" } else { "SELL" },
            "size": fill.size,
            "price": fill.price,
            "boughtSize": ledger.bought_size,
            "soldSize": ledger.sold_size,
            "inventorySize": ledger.inventory(),
            "fee": fill.fee,
            "performance": performance,
        }),
    )
}

pub(super) fn current_mark(book: &watch::Receiver<BookFeedState>, fallback: f64) -> f64 {
    book.borrow()
        .top
        .as_ref()
        .and_then(|top| Some((top.best_bid?.price + top.best_ask?.price) / 2.0))
        .filter(|mark| mark.is_finite() && *mark > 0.0)
        .unwrap_or(fallback)
}

fn stop_loss_amount(definition: &MidPriceJobDefinition, allocated_margin: f64) -> Option<f64> {
    definition
        .stop_loss_pct
        .filter(|percent| *percent > 0.0)
        .map(|percent| allocated_margin * percent / 100.0)
}

pub(super) fn stop_loss_triggered(performance: &BotPerformance, max_loss: f64) -> bool {
    performance.trading_pnl.is_some_and(|pnl| pnl <= -max_loss)
}

pub(super) fn append_stop_loss(
    job_id: &str,
    bot: &str,
    stop_loss_pct: f64,
    max_loss: f64,
    mark_price: f64,
    performance: &BotPerformance,
) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.stop_loss",
            "bot": bot,
            "jobId": job_id,
            "stopLossPct": stop_loss_pct,
            "maxLoss": max_loss,
            "markPrice": mark_price,
            "performance": performance,
            "action": "cancel_and_flatten",
        }),
    )
}

fn clear_live_order(order_id: &str, buy: &mut QuoteSlot, sell: &mut QuoteSlot) {
    for slot in [buy, sell] {
        if slot
            .live
            .as_ref()
            .is_some_and(|quote| quote.order_id == order_id)
        {
            slot.live = None;
        }
    }
}

pub(super) fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status,
        "filled"
            | "cancelled"
            | "cancelledByUser"
            | "cancelledBySystem"
            | "siblingCancelled"
            | "rejected"
            | "rejectedCrossing"
            | "rejectedPostOnly"
            | "triggerFailed"
    ) || status.starts_with("cancelled")
        || status.starts_with("rejected")
}

fn append_quote(
    job_id: &str,
    bot: &str,
    side: QuoteSide,
    status: &str,
    order_id: &str,
    price: f64,
    size: f64,
) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.quote",
            "bot": bot,
            "jobId": job_id,
            "status": status,
            "side": side.name(),
            "orderId": order_id,
            "price": price,
            "size": size,
        }),
    )
}

pub(super) fn append_market_data(
    job_id: &str,
    bot: &str,
    feed: &str,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.market_data",
            "bot": bot,
            "jobId": job_id,
            "feed": feed,
            "status": status,
            "error": error,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
async fn cleanup(
    job_id: &str,
    bot: &str,
    mark_price: f64,
    definition: &MidPriceJobDefinition,
    parent: &TradePlan,
    adapter: &ExecutionAdapter,
    buy: &mut QuoteSlot,
    sell: &mut QuoteSlot,
    ledger: &mut FillLedger,
    owned_orders: &mut HashSet<String>,
) -> Result<()> {
    let cleanup_deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let (open_orders, fills) = tokio::join!(
            adapter.open_orders(&parent.account),
            adapter.fills(&parent.account),
        );
        let open_orders = open_orders?;
        reconcile_recovery(
            RecoveryContext {
                job_id,
                bot,
                mark_price,
                open_orders: &open_orders,
                owned_orders,
            },
            fills?,
            buy,
            sell,
            ledger,
        )?;
        let remaining = open_orders
            .into_iter()
            .filter(|order| owned_orders.contains(&order.order_id))
            .collect::<Vec<_>>();
        if remaining.is_empty() {
            break;
        }
        let cancellation_plans = remaining
            .into_iter()
            .map(|order| {
                let plan = cancel_plan(parent, order.order_id.clone())?;
                Ok((order, plan))
            })
            .collect::<Result<Vec<_>>>()?;
        let cancellation_results = join_all(
            cancellation_plans
                .iter()
                .map(|(_, plan)| adapter.cancel_order(plan)),
        )
        .await;
        for ((order, _), result) in cancellation_plans.into_iter().zip(cancellation_results) {
            match result {
                Ok(receipt) => append_quote(
                    job_id,
                    bot,
                    if order.side == OrderSide::Buy {
                        QuoteSide::Buy
                    } else {
                        QuoteSide::Sell
                    },
                    &receipt.status,
                    &order.order_id,
                    order.price,
                    order.remaining_size,
                )?,
                Err(error) if is_order_gone_error(&error) => {}
                Err(error) => return Err(error).context("failed to cancel a mid-price quote"),
            }
        }
        if Instant::now() >= cleanup_deadline {
            bail!("timed out waiting for mid-price quotes to cancel");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let market = execution_market(definition.venue, &definition.symbol)?;
    let rules = market.execution_rules()?;
    let inventory = ledger.inventory();
    let size = floor_to_step(inventory.abs(), rules.lot_size, rules.size_precision);
    if size < rules.lot_size / 2.0 {
        return Ok(());
    }
    if size * mark_price < rules.min_notional {
        bail!(
            "bot-owned residual {} inventory {} is below the venue minimum and could not be unwound automatically",
            definition.symbol,
            inventory
        );
    }

    let direction = if inventory > 0.0 {
        PositionDirection::Short
    } else {
        PositionDirection::Long
    };
    let plan = inventory_unwind_plan(parent, direction, size, mark_price)?;
    let receipt = adapter
        .submit_trade(&plan)
        .await
        .context("failed to unwind mid-price bot-owned inventory")?;
    let order_id = receipt
        .order_id
        .context("mid-price inventory unwind omitted its order id")?;
    owned_orders.insert(order_id);

    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        reconcile_recovery(
            RecoveryContext {
                job_id,
                bot,
                mark_price,
                open_orders: &[],
                owned_orders,
            },
            adapter.fills(&parent.account).await?,
            buy,
            sell,
            ledger,
        )?;
        if ledger.inventory().abs() < rules.lot_size / 2.0 {
            break;
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for bot-owned residual inventory to unwind; remaining={}",
                ledger.inventory()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

pub(super) fn is_order_gone_error(error: &anyhow::Error) -> bool {
    is_order_gone_message(&format!("{error:#}"))
}

pub(super) fn is_post_only_crossing_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("rejectedcrossing")
        || message.contains("post only order would have immediately matched")
        || message.contains("post-only order would have immediately matched")
}

pub(super) fn is_order_gone_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("not found")
        || message.contains("order was never placed")
        || message.contains("already canceled")
        || message.contains("already filled")
        || message.contains("already cancelled")
}

pub(super) fn floor_to_step(value: f64, step: f64, precision: u8) -> f64 {
    let scale = 10_f64.powi(i32::from(precision));
    (((value / step) + 1e-9).floor() * step * scale).round() / scale
}

fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("current timestamp does not fit in u64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_hyperliquid_cancel_rejection_is_not_fatal() {
        assert!(is_order_gone_message(
            "Hyperliquid rejected cancellation: Order was never placed, already canceled, or filled. asset=3"
        ));
        assert!(!is_order_gone_message(
            "Hyperliquid rejected cancellation: invalid signature"
        ));
    }

    #[test]
    fn hyperliquid_post_only_crossing_is_retryable() {
        assert!(is_post_only_crossing_message(
            "Hyperliquid rejected order: Post only order would have immediately matched, bbo was 66621@66642. asset=3"
        ));
        assert!(is_post_only_crossing_message(
            "BULK rejected order: rejectedCrossing"
        ));
        assert!(!is_post_only_crossing_message(
            "Hyperliquid rejected order: invalid signature"
        ));
    }

    #[test]
    fn fill_ledger_reconciles_recovery_counts_without_losing_identical_fills() {
        let mut ledger = FillLedger::default();
        let buy = ObservedFill {
            timestamp: 10,
            recovered: false,
            buy: true,
            size: 2.0,
            price: 100.0,
            fee: Some(-0.02),
        };
        let sell = ObservedFill {
            timestamp: 11,
            recovered: false,
            buy: false,
            size: 1.0,
            price: 101.0,
            fee: Some(-0.01),
        };
        assert!(ledger.record_live("one", &buy));
        assert!(!ledger.record_recovery_occurrence("one", &buy, 1));
        assert!(ledger.record_recovery_occurrence("one", &buy, 2));
        assert!(ledger.record_live("two", &sell));
        assert_eq!(ledger.bought_size, 4.0);
        assert_eq!(ledger.sold_size, 1.0);
        assert_eq!(ledger.inventory(), 3.0);
    }

    #[test]
    fn fill_ledger_accounts_for_realized_unrealized_fees_and_position_flips() {
        let mut ledger = FillLedger::with_allocated_margin(100.0);
        assert!(ledger.record_live(
            "buy",
            &ObservedFill {
                timestamp: 1,
                recovered: false,
                buy: true,
                size: 2.0,
                price: 100.0,
                fee: Some(-1.0),
            }
        ));
        assert!(ledger.record_live(
            "sell",
            &ObservedFill {
                timestamp: 2,
                recovered: false,
                buy: false,
                size: 3.0,
                price: 110.0,
                fee: Some(-1.0),
            }
        ));

        let performance = ledger.performance(105.0);
        assert_eq!(performance.bought_size, 2.0);
        assert_eq!(performance.sold_size, 3.0);
        assert_eq!(performance.matched_size, 2.0);
        assert_eq!(performance.average_buy_price, Some(100.0));
        assert_eq!(performance.average_sell_price, Some(110.0));
        assert_eq!(performance.inventory_size, -1.0);
        assert_eq!(performance.average_entry_price, Some(110.0));
        assert_eq!(performance.gross_realized_pnl, 20.0);
        assert_eq!(performance.unrealized_pnl, 5.0);
        assert_eq!(performance.fees, -2.0);
        assert_eq!(performance.trading_pnl, Some(23.0));
        assert_eq!(performance.return_on_margin_pct, Some(23.0));
    }

    #[test]
    fn recovered_fill_without_fee_keeps_gross_pnl_but_marks_net_pnl_unavailable() {
        let mut ledger = FillLedger::with_allocated_margin(100.0);
        assert!(ledger.record_recovery_occurrence(
            "recovered",
            &ObservedFill {
                timestamp: 1,
                recovered: true,
                buy: true,
                size: 1.0,
                price: 100.0,
                fee: None,
            },
            1,
        ));

        let performance = ledger.performance(101.0);
        assert!(!performance.fees_complete);
        assert_eq!(performance.unrealized_pnl, 1.0);
        assert_eq!(performance.trading_pnl, None);
        assert_eq!(performance.return_on_margin_pct, None);
    }

    #[test]
    fn crossing_retry_requires_a_newer_book_revision() {
        let mut slot = QuoteSlot {
            retry_after_book_revision: Some(7),
            ..QuoteSlot::default()
        };

        assert!(!slot.accepts_book_revision(7));
        assert!(!slot.accepts_book_revision(6));
        assert!(slot.accepts_book_revision(8));
        assert!(slot.accepts_book_revision(8));
    }

    #[test]
    fn desired_quote_drops_dust_below_minimum_notional() {
        assert_eq!(desired_quote(0.00001, 100.0, 0.00001, 5, 10.0), None);
        assert_eq!(desired_quote(0.2, 100.0, 0.01, 2, 10.0), Some((100.0, 0.2)));
    }

    #[test]
    fn refresh_only_chases_quotes_when_the_market_moves_away() {
        assert!(!quote_is_stale_away_from_market(
            QuoteSide::Buy,
            100.0,
            99.0,
            0.5
        ));
        assert!(quote_is_stale_away_from_market(
            QuoteSide::Buy,
            100.0,
            101.0,
            0.5
        ));
        assert!(!quote_is_stale_away_from_market(
            QuoteSide::Sell,
            100.0,
            101.0,
            0.5
        ));
        assert!(quote_is_stale_away_from_market(
            QuoteSide::Sell,
            100.0,
            99.0,
            0.5
        ));
        assert!(!quote_is_stale_away_from_market(
            QuoteSide::Buy,
            100.0,
            100.004,
            0.5
        ));
    }

    #[test]
    fn refresh_time_is_the_minimum_lifetime_of_each_quote() {
        let check = QuoteRefreshCheck {
            mode: MidMode::FillPriority,
            side: QuoteSide::Buy,
            current_price: 100.0,
            proposed_price: 101.0,
            tick_size: 0.01,
            tolerance_bps: 0.5,
            resting_age: Duration::from_millis(999),
            minimum_age: Duration::from_secs(1),
        };
        assert!(!should_refresh_quote_price(check));
        assert!(should_refresh_quote_price(QuoteRefreshCheck {
            resting_age: Duration::from_secs(1),
            ..check
        }));
    }

    #[test]
    fn quote_protection_reprices_in_both_directions_after_its_minimum_age() {
        for (side, proposed_price) in [
            (QuoteSide::Buy, 99.99),
            (QuoteSide::Buy, 100.01),
            (QuoteSide::Sell, 99.99),
            (QuoteSide::Sell, 100.01),
        ] {
            assert!(should_refresh_quote_price(QuoteRefreshCheck {
                mode: MidMode::QuoteProtection,
                side,
                current_price: 100.0,
                proposed_price,
                tick_size: 0.001,
                tolerance_bps: 999.0,
                resting_age: STRICT_MINIMUM_QUOTE_AGE,
                minimum_age: STRICT_MINIMUM_QUOTE_AGE,
            }));
        }
    }

    #[test]
    fn quote_protection_uses_the_larger_of_two_ticks_and_quarter_basis_point() {
        let check = QuoteRefreshCheck {
            mode: MidMode::QuoteProtection,
            side: QuoteSide::Buy,
            current_price: 100.0,
            proposed_price: 100.019,
            tick_size: 0.01,
            tolerance_bps: 999.0,
            resting_age: STRICT_MINIMUM_QUOTE_AGE,
            minimum_age: STRICT_MINIMUM_QUOTE_AGE,
        };
        assert!(!should_refresh_quote_price(check));
        assert!(should_refresh_quote_price(QuoteRefreshCheck {
            proposed_price: 100.026,
            tick_size: 0.001,
            ..check
        }));
    }

    #[test]
    fn hyperliquid_account_events_normalize_orders_fills_and_fee_signs() {
        let orders = normalize_hyperliquid_account_events(serde_json::json!({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "oid": 42,
                    "side": "A",
                    "limitPx": "101.25",
                    "sz": "0.5"
                },
                "status": "open",
                "statusTimestamp": 123
            }]
        }))
        .expect("order event normalizes");
        assert_eq!(orders[0]["oid"], "42");
        assert_eq!(orders[0]["status"], "resting");
        assert_eq!(orders[0]["sz"], -0.5);
        assert_eq!(orders[0]["origSz"], 0.5);
        assert_eq!(orders[0]["isBuy"], false);

        let filled_orders = normalize_hyperliquid_account_events(serde_json::json!({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "oid": 43,
                    "side": "A",
                    "limitPx": "101.25",
                    "sz": "0",
                    "origSz": "0.5"
                },
                "status": "filled",
                "statusTimestamp": 124
            }]
        }))
        .expect("filled order event normalizes");
        assert_eq!(filled_orders[0]["status"], "filled");
        assert_eq!(filled_orders[0]["origSz"], 0.5);
        assert_eq!(filled_orders[0]["isBuy"], false);

        let fills = normalize_hyperliquid_account_events(serde_json::json!({
            "channel": "user",
            "data": {"fills": [{
                "oid": 42,
                "side": "B",
                "px": "100.5",
                "sz": "0.25",
                "fee": "0.01",
                "time": 124
            }]}
        }))
        .expect("fill event normalizes");
        assert_eq!(fills[0]["orderId"], "42");
        assert_eq!(fills[0]["isBuy"], true);
        assert_eq!(fills[0]["fee"], -0.01);
    }

    #[test]
    fn stop_loss_uses_allocated_margin_and_requires_complete_net_pnl() {
        let definition = MidPriceJobDefinition {
            venue: ExecutionVenue::Bulk,
            testnet: false,
            symbol: "BTC/USDT".to_string(),
            max_inventory_size: 1.0,
            requested_margin: Some(200.0),
            max_inventory_margin: 200.0,
            max_inventory_exposure: 2_000.0,
            duration_seconds: 60,
            spread_bps: 6.0,
            refresh_seconds: 0.5,
            refresh_tolerance_bps: 0.25,
            directional_bias_percent: 0.0,
            leverage: 10.0,
            stop_loss_pct: Some(5.0),
        };
        assert_eq!(stop_loss_amount(&definition, 200.0), Some(10.0));

        let mut ledger = FillLedger::with_allocated_margin(200.0);
        assert!(ledger.record_live(
            "buy",
            &ObservedFill {
                timestamp: 1,
                recovered: false,
                buy: true,
                size: 1.0,
                price: 100.0,
                fee: Some(-1.0),
            },
        ));
        assert!(stop_loss_triggered(&ledger.performance(91.0), 10.0));

        let mut incomplete = FillLedger::with_allocated_margin(200.0);
        assert!(incomplete.record_recovery_occurrence(
            "buy",
            &ObservedFill {
                timestamp: 1,
                recovered: true,
                buy: true,
                size: 1.0,
                price: 100.0,
                fee: None,
            },
            1,
        ));
        assert!(!stop_loss_triggered(&incomplete.performance(80.0), 10.0));
    }

    #[test]
    fn inventory_unwind_is_not_reduce_only_or_account_position_dependent() {
        let parent = TradePlan {
            created_at_ms: 1,
            venue: ExecutionVenue::Bulk,
            testnet: false,
            account: "account".to_string(),
            internal_symbol: "BTC/USDT".to_string(),
            venue_symbol: "BTC-USD".to_string(),
            direction: PositionDirection::Long,
            side: OrderSide::Buy,
            order_kind: OrderKind::Market,
            time_in_force: None,
            requested_size: Some(1.0),
            size: 1.0,
            price: None,
            reference_price: 100.0,
            requested_margin: Some(10.0),
            estimated_margin: 10.0,
            estimated_exposure: 100.0,
            projected_liquidation_price: None,
            leverage: 10.0,
            reduce_only: false,
            stop_loss_price: None,
            take_profit_price: None,
        };

        let plan = inventory_unwind_plan(&parent, PositionDirection::Short, 0.25, 100.0)
            .expect("job inventory should produce an unwind plan");
        assert_eq!(plan.direction, PositionDirection::Short);
        assert_eq!(plan.side, OrderSide::Sell);
        assert_eq!(plan.size, 0.25);
        assert!(!plan.reduce_only);
    }

    #[test]
    fn fills_reduce_the_tracked_working_quantity_before_replenishment() {
        let mut buy = QuoteSlot {
            live: Some(WorkingQuote {
                order_id: "bid-1".to_string(),
                price: 100.0,
                size: 2.0,
                submitted_at: Instant::now(),
                cancel_requested: false,
            }),
            ..QuoteSlot::default()
        };
        let mut sell = QuoteSlot::default();

        apply_fill_to_working_quote("bid-1", 0.5, &mut buy, &mut sell);
        assert_eq!(buy.live.as_ref().map(|quote| quote.size), Some(1.5));

        apply_fill_to_working_quote("bid-1", 1.5, &mut buy, &mut sell);
        assert!(buy.live.is_none());
    }

    #[test]
    fn terminal_status_recognizes_post_only_rejections() {
        assert!(is_terminal_order_status("rejectedCrossing"));
        assert!(is_terminal_order_status("cancelledByUser"));
        assert!(!is_terminal_order_status("resting"));
    }
}
