use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;

use crate::bots::grid::{GridQuote, GridSpec, quote_grid};
use crate::bots::jobs::{
    BotJobDefinition, BotJobSubmission, BotPerformance, GridJobDefinition, MidPriceJobDefinition,
    OutcomeExecutionDefinition, OutcomeMakerPerformance,
};
use crate::bots::outcome::{OutcomeQuotes, gross_profit_per_pair, quote_prices};
use crate::cli::{OutputFormat, RunGridArgs, RunMidPriceArgs};
use crate::commands::bot::mid_price::{
    AccountFeedEvent, BookFeedState, BotStopped, append_market_data, confirm_live_execution,
    live_orderbook, render_submission, spawn_account_feed, spawn_book_feed,
};
use crate::domain::execution::{
    CancelPlan, ExecutionReceipt, ExecutionVenue, Fill, OpenOrder, OrderKind, OrderSide,
    PositionDirection, TimeInForce, TradePlan,
};
use crate::providers::execution::ExecutionAdapter;
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::exchange::{UserOutcomeAction, wire_number};
use crate::providers::hyperliquid::execution::{normalize_price_for, validate_price_for};
use crate::providers::hyperliquid::outcomes::{
    OUTCOME_MIN_NOTIONAL, OutcomeInstrument, outcome_execution_rules,
};

const HOLDING_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(20);
const EPSILON: f64 = 1e-9;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutcomePlanView<'a> {
    r#type: &'static str,
    bot: &'a str,
    venue: &'static str,
    network: &'static str,
    outcome_id: u32,
    question: &'a str,
    outcome: &'a str,
    quote_token: &'a str,
    normalized_symbol: &'a str,
    primary_name: &'a str,
    primary_symbol: &'a str,
    complement_name: &'a str,
    complement_symbol: &'a str,
    reference_price: f64,
    normalized_bid: f64,
    normalized_ask: f64,
    complement_sell_price: f64,
    primary_sell_price: f64,
    pair_size: f64,
    requested_margin: f64,
    gross_profit_per_pair: f64,
    estimated_gross_cycle_profit: f64,
    spread_bps: f64,
    refresh_seconds: f64,
    refresh_tolerance_bps: f64,
    stop_loss_pct: Option<f64>,
    duration_secs: u64,
    execution: &'static str,
    replenishment: &'static str,
    shutdown: &'static str,
    dry_run: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutcomeGridPlanView<'a> {
    r#type: &'static str,
    bot: &'static str,
    venue: &'static str,
    network: &'static str,
    symbol: &'a str,
    outcome_id: u32,
    question: &'a str,
    quote_token: &'a str,
    primary_name: &'a str,
    primary_symbol: &'a str,
    complement_name: &'a str,
    complement_symbol: &'a str,
    reference_price: f64,
    pair_size: f64,
    requested_margin: f64,
    levels_per_side: u16,
    step_bps: f64,
    levels: Vec<OutcomeGridPlanLevel>,
    stop_loss_pct: Option<f64>,
    duration_secs: u64,
    execution: &'static str,
    shutdown: &'static str,
    dry_run: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutcomeGridPlanLevel {
    level: u16,
    normalized_side: &'static str,
    normalized_price: f64,
    paired_price: f64,
    venue_symbol: String,
    venue_side: &'static str,
    venue_price: f64,
    size: f64,
}

#[derive(Clone)]
pub(super) struct OutcomePair {
    primary: OutcomeInstrument,
    complement: OutcomeInstrument,
}

#[derive(Clone)]
struct OutcomeRunDefinition {
    bot: &'static str,
    venue: ExecutionVenue,
    testnet: bool,
    symbol: String,
    outcome_id: u32,
    primary_symbol: String,
    complement_symbol: String,
    primary_market_fingerprint: String,
    complement_market_fingerprint: String,
    pair_size: f64,
    requested_margin: f64,
    duration_seconds: u64,
    spread_bps: f64,
    refresh_seconds: f64,
    refresh_tolerance_bps: f64,
    stop_loss_pct: Option<f64>,
}

impl OutcomeRunDefinition {
    fn from_mid(bot: &'static str, definition: &MidPriceJobDefinition) -> Result<Self> {
        let outcome = definition
            .outcome
            .as_ref()
            .context("outcome execution metadata is missing")?;
        Ok(Self {
            bot,
            venue: definition.venue,
            testnet: definition.testnet,
            symbol: definition.symbol.clone(),
            outcome_id: outcome.outcome_id,
            primary_symbol: outcome.primary_symbol.clone(),
            complement_symbol: outcome.complement_symbol.clone(),
            primary_market_fingerprint: outcome.primary_market_fingerprint.clone(),
            complement_market_fingerprint: outcome.complement_market_fingerprint.clone(),
            pair_size: outcome.pair_size,
            requested_margin: definition.max_inventory_margin,
            duration_seconds: definition.duration_seconds,
            spread_bps: definition.spread_bps,
            refresh_seconds: definition.refresh_seconds,
            refresh_tolerance_bps: definition.refresh_tolerance_bps,
            stop_loss_pct: definition.stop_loss_pct,
        })
    }

    fn from_grid(definition: &GridJobDefinition) -> Result<Self> {
        let outcome = definition
            .outcome
            .as_ref()
            .context("outcome execution metadata is missing")?;
        Ok(Self {
            bot: "grid",
            venue: definition.venue,
            testnet: definition.testnet,
            symbol: definition.symbol.clone(),
            outcome_id: outcome.outcome_id,
            primary_symbol: outcome.primary_symbol.clone(),
            complement_symbol: outcome.complement_symbol.clone(),
            primary_market_fingerprint: outcome.primary_market_fingerprint.clone(),
            complement_market_fingerprint: outcome.complement_market_fingerprint.clone(),
            pair_size: outcome.pair_size,
            requested_margin: definition.max_inventory_margin,
            duration_seconds: definition.duration_seconds,
            spread_bps: 0.0,
            refresh_seconds: 0.0,
            refresh_tolerance_bps: 0.0,
            stop_loss_pct: definition.stop_loss_pct,
        })
    }
}

impl OutcomePair {
    fn question(&self) -> &str {
        self.primary
            .question_name
            .as_deref()
            .unwrap_or(&self.primary.outcome_description)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum OutcomeSide {
    Primary,
    Complement,
}

impl OutcomeSide {
    const fn name(self) -> &'static str {
        match self {
            Self::Primary => "PRIMARY",
            Self::Complement => "COMPLEMENT",
        }
    }
}

#[derive(Clone, Debug)]
struct WorkingOrder {
    order_id: String,
    side: OutcomeSide,
    symbol: String,
    venue_symbol: String,
    price: f64,
    original_size: f64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct OutcomeGridKey {
    initial_buy: bool,
    level: u16,
}

#[derive(Clone, Debug)]
struct OutcomeGridSlot {
    initial: GridQuote,
    current_side: OrderSide,
    current_price: f64,
    filled_size: f64,
    working: Option<WorkingOrder>,
}

struct OutcomeGridFillState<'a> {
    ledger: &'a mut OutcomeLedger,
    order_slots: &'a HashMap<String, OutcomeGridKey>,
    order_sides: &'a HashMap<String, OutcomeSide>,
    slots: &'a mut HashMap<OutcomeGridKey, OutcomeGridSlot>,
}

impl OutcomeGridSlot {
    fn new(initial: GridQuote) -> Self {
        Self {
            initial,
            current_side: initial.side,
            current_price: initial.price,
            filled_size: 0.0,
            working: None,
        }
    }

    fn key(&self) -> OutcomeGridKey {
        OutcomeGridKey {
            initial_buy: self.initial.side == OrderSide::Buy,
            level: self.initial.level,
        }
    }

    fn remaining(&self) -> f64 {
        (self.initial.size - self.filled_size).max(0.0)
    }

    fn advance(&mut self) -> bool {
        let completed_cycle = self.current_side != self.initial.side;
        if completed_cycle {
            self.current_side = self.initial.side;
            self.current_price = self.initial.price;
        } else {
            self.current_side = opposite_side(self.initial.side);
            self.current_price = self.initial.paired_price;
        }
        self.filled_size = 0.0;
        self.working = None;
        completed_cycle
    }
}

fn opposite_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

#[derive(Default)]
struct OutcomeLedger {
    allocated_margin: f64,
    split_size: f64,
    merged_size: f64,
    primary_sold_size: f64,
    complement_sold_size: f64,
    primary_revenue: f64,
    complement_revenue: f64,
    primary_cost_basis: f64,
    complement_cost_basis: f64,
    gross_realized_pnl: f64,
    fees: f64,
    fees_complete: bool,
    completed_cycles: u64,
    seen_fills: HashSet<(String, u64, u64, u64)>,
}

impl OutcomeLedger {
    fn new(allocated_margin: f64) -> Self {
        Self {
            allocated_margin,
            fees_complete: true,
            ..Self::default()
        }
    }

    fn record_split(&mut self, size: f64, primary_reference: f64) {
        self.split_size += size;
        self.primary_cost_basis += size * primary_reference;
        self.complement_cost_basis += size * (1.0 - primary_reference);
    }

    fn record_merge(&mut self, size: f64) {
        let primary_inventory = self.primary_inventory();
        let complement_inventory = self.complement_inventory();
        let primary_cost = proportional_cost(self.primary_cost_basis, primary_inventory, size);
        let complement_cost =
            proportional_cost(self.complement_cost_basis, complement_inventory, size);
        self.primary_cost_basis = (self.primary_cost_basis - primary_cost).max(0.0);
        self.complement_cost_basis = (self.complement_cost_basis - complement_cost).max(0.0);
        self.gross_realized_pnl += size - primary_cost - complement_cost;
        self.merged_size += size;
    }

    fn record_fill(&mut self, side: OutcomeSide, fill: &Fill) -> bool {
        let Some(order_id) = fill.order_id.as_deref() else {
            return false;
        };
        let key = (
            order_id.to_string(),
            fill.ts_ms,
            fill.amount.to_bits(),
            fill.price.to_bits(),
        );
        if !self.seen_fills.insert(key) {
            return false;
        }
        match side {
            OutcomeSide::Primary => {
                let inventory = self.primary_inventory();
                let cost = proportional_cost(self.primary_cost_basis, inventory, fill.amount);
                self.primary_cost_basis = (self.primary_cost_basis - cost).max(0.0);
                self.primary_sold_size += fill.amount;
                self.primary_revenue += fill.amount * fill.price;
                self.gross_realized_pnl += fill.amount * fill.price - cost;
            }
            OutcomeSide::Complement => {
                let inventory = self.complement_inventory();
                let cost = proportional_cost(self.complement_cost_basis, inventory, fill.amount);
                self.complement_cost_basis = (self.complement_cost_basis - cost).max(0.0);
                self.complement_sold_size += fill.amount;
                self.complement_revenue += fill.amount * fill.price;
                self.gross_realized_pnl += fill.amount * fill.price - cost;
            }
        }
        match fill.fee {
            Some(fee) if fee.is_finite() => self.fees += fee,
            _ => self.fees_complete = false,
        }
        true
    }

    fn primary_inventory(&self) -> f64 {
        (self.split_size - self.merged_size - self.primary_sold_size).max(0.0)
    }

    fn complement_inventory(&self) -> f64 {
        (self.split_size - self.merged_size - self.complement_sold_size).max(0.0)
    }

    fn performance(&self, primary_mark: f64) -> BotPerformance {
        let primary_inventory = self.primary_inventory();
        let complement_inventory = self.complement_inventory();
        let inventory_value =
            primary_inventory * primary_mark + complement_inventory * (1.0 - primary_mark);
        let gross_realized_pnl = clean_zero(self.gross_realized_pnl);
        let unrealized_pnl =
            clean_zero(inventory_value - self.primary_cost_basis - self.complement_cost_basis);
        let trading_pnl = self
            .fees_complete
            .then_some(clean_zero(gross_realized_pnl + unrealized_pnl + self.fees));
        BotPerformance {
            allocated_margin: self.allocated_margin,
            bought_size: self.split_size * 2.0,
            sold_size: self.primary_sold_size + self.complement_sold_size,
            matched_size: self.primary_sold_size.min(self.complement_sold_size),
            average_buy_price: None,
            average_sell_price: ((self.primary_sold_size + self.complement_sold_size) > 0.0)
                .then_some(
                    (self.primary_revenue + self.complement_revenue)
                        / (self.primary_sold_size + self.complement_sold_size),
                ),
            inventory_size: primary_inventory - complement_inventory,
            average_entry_price: None,
            mark_price: primary_mark,
            gross_realized_pnl,
            unrealized_pnl,
            fees: self.fees,
            fees_complete: self.fees_complete,
            trading_pnl,
            return_on_margin_pct: trading_pnl.and_then(|pnl| {
                (self.allocated_margin > 0.0).then_some(pnl / self.allocated_margin * 100.0)
            }),
            outcome: Some(OutcomeMakerPerformance {
                split_size: self.split_size,
                merged_size: self.merged_size,
                primary_sold_size: self.primary_sold_size,
                complement_sold_size: self.complement_sold_size,
                primary_inventory,
                complement_inventory,
                completed_cycles: self.completed_cycles,
            }),
        }
    }
}

fn proportional_cost(cost_basis: f64, inventory: f64, amount: f64) -> f64 {
    if inventory <= EPSILON || cost_basis <= 0.0 {
        0.0
    } else {
        cost_basis * (amount / inventory).clamp(0.0, 1.0)
    }
}

fn clean_zero(value: f64) -> f64 {
    if value.abs() <= 1e-12 { 0.0 } else { value }
}

pub(super) async fn handle_mid(
    args: RunMidPriceArgs,
    bot: &'static str,
    refresh_seconds: f64,
    refresh_tolerance_bps: f64,
) -> Result<()> {
    args.validate()?;
    let network = HyperliquidNetwork::from_testnet(args.testnet);
    let selected = crate::providers::hyperliquid::outcomes::resolve(network, &args.symbol).await?;
    let pair = resolve_pair(network, selected.outcome_id, selected.side).await?;
    let rules = outcome_execution_rules();
    let pair_size = args
        .size
        .or(args.margin)
        .context("outcome bot size is missing")?;
    if (pair_size / rules.lot_size).fract().abs() > EPSILON {
        bail!(
            "outcome quote size {} must align to the share lot size {}",
            pair_size,
            rules.lot_size
        );
    }
    let book = live_orderbook(
        ExecutionVenue::HyperliquidOutcomes,
        &pair.primary.symbol,
        args.testnet,
    )
    .await?;
    let (best_bid, best_ask) = book_prices(&book)?;
    let quotes = executable_quote_prices(
        best_bid,
        best_ask,
        args.spread_bps,
        rules.tick_size,
        rules.price_precision,
    )?;
    validate_quote_notional(pair_size, quotes)?;
    let outcome = OutcomeExecutionDefinition {
        outcome_id: selected.outcome_id,
        primary_symbol: pair.primary.symbol.clone(),
        complement_symbol: pair.complement.symbol.clone(),
        primary_name: pair.primary.side_name.clone(),
        complement_name: pair.complement.side_name.clone(),
        primary_market_fingerprint: pair.primary.metadata_fingerprint.clone(),
        complement_market_fingerprint: pair.complement.metadata_fingerprint.clone(),
        quote_token: pair.primary.quote_token.clone(),
        pair_size,
    };
    let definition = MidPriceJobDefinition {
        venue: ExecutionVenue::HyperliquidOutcomes,
        testnet: args.testnet,
        symbol: pair.primary.symbol.clone(),
        max_inventory_size: pair_size * 2.0,
        requested_margin: Some(pair_size),
        max_inventory_margin: pair_size,
        max_inventory_exposure: pair_size,
        duration_seconds: args.duration,
        spread_bps: args.spread_bps,
        refresh_seconds,
        refresh_tolerance_bps,
        directional_bias_percent: 0.0,
        leverage: None,
        stop_loss_pct: args.stop_loss_pct.filter(|value| *value > 0.0),
        outcome: Some(outcome),
    };
    definition.validate()?;
    let run = OutcomeRunDefinition::from_mid(bot, &definition)?;
    let view = plan_view(&run, &pair, quotes, args.dry_run);
    if args.dry_run {
        return render_plan(&view, args.output);
    }
    let account = ExecutionAdapter::configured_account(ExecutionVenue::HyperliquidOutcomes)?;
    require_quote_balance(args.testnet, &account, &pair.primary.quote_token, pair_size).await?;
    if !args.yes && !matches!(args.output, OutputFormat::Terminal) {
        bail!("live bot execution with structured output requires --yes");
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_plan(&view, args.output)?;
        if !args.yes && !confirm_live_execution(ExecutionVenue::HyperliquidOutcomes, args.testnet)?
        {
            println!("cancelled; no bot job was submitted");
            return Ok(());
        }
    }
    let job = crate::runtime::submit_bot_job(BotJobSubmission {
        definition: if bot == "volume-mid" {
            BotJobDefinition::VolumeMid(definition)
        } else {
            BotJobDefinition::MidPrice(definition)
        },
    })
    .await?;
    render_submission(&job, args.output)
}

pub(super) async fn handle_grid(args: RunGridArgs) -> Result<()> {
    args.validate()?;
    let network = HyperliquidNetwork::from_testnet(args.testnet);
    let selected = crate::providers::hyperliquid::outcomes::resolve(network, &args.symbol).await?;
    let pair = resolve_pair(network, selected.outcome_id, selected.side).await?;
    let rules = outcome_execution_rules();
    let requested = args
        .size
        .or(args.margin)
        .context("outcome grid size is missing")?;
    let per_level =
        ((requested / f64::from(args.levels)) / rules.lot_size).floor() * rules.lot_size;
    if per_level < rules.lot_size / 2.0 {
        bail!(
            "outcome grid allocation is too small for {} levels",
            args.levels
        );
    }
    let pair_size = per_level * f64::from(args.levels);
    let book = live_orderbook(
        ExecutionVenue::HyperliquidOutcomes,
        &pair.primary.symbol,
        args.testnet,
    )
    .await?;
    let (best_bid, best_ask) = book_prices(&book)?;
    let center = (best_bid + best_ask) / 2.0;
    let quotes = outcome_grid_quotes(center, args.levels, args.step_bps, pair_size)?;
    validate_grid_quotes(&quotes, &pair)?;

    let outcome = OutcomeExecutionDefinition {
        outcome_id: selected.outcome_id,
        primary_symbol: pair.primary.symbol.clone(),
        complement_symbol: pair.complement.symbol.clone(),
        primary_name: pair.primary.side_name.clone(),
        complement_name: pair.complement.side_name.clone(),
        primary_market_fingerprint: pair.primary.metadata_fingerprint.clone(),
        complement_market_fingerprint: pair.complement.metadata_fingerprint.clone(),
        quote_token: pair.primary.quote_token.clone(),
        pair_size,
    };
    let definition = GridJobDefinition {
        venue: ExecutionVenue::HyperliquidOutcomes,
        testnet: args.testnet,
        symbol: pair.primary.symbol.clone(),
        // Half of each split side backs the initial ladder. The other half is
        // kept available for the paired orders created as initial legs fill.
        max_inventory_size: pair_size,
        requested_margin: Some(pair_size),
        max_inventory_margin: pair_size,
        max_inventory_exposure: pair_size,
        duration_seconds: args.duration,
        levels_per_side: args.levels,
        step_bps: args.step_bps,
        leverage: None,
        stop_loss_pct: args.stop_loss_pct.filter(|value| *value > 0.0),
        outcome: Some(outcome),
    };
    definition.validate()?;
    let view = outcome_grid_plan_view(&definition, &pair, center, &quotes, args.dry_run)?;
    if args.dry_run {
        return render_outcome_grid_plan(&view, args.output);
    }
    let account = ExecutionAdapter::configured_account(ExecutionVenue::HyperliquidOutcomes)?;
    require_quote_balance(args.testnet, &account, &pair.primary.quote_token, pair_size).await?;
    if !args.yes && !matches!(args.output, OutputFormat::Terminal) {
        bail!("live bot execution with structured output requires --yes");
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_outcome_grid_plan(&view, args.output)?;
        if !args.yes && !confirm_live_execution(ExecutionVenue::HyperliquidOutcomes, args.testnet)?
        {
            println!("cancelled; no bot job was submitted");
            return Ok(());
        }
    }
    let job = crate::runtime::submit_bot_job(BotJobSubmission {
        definition: BotJobDefinition::Grid(definition),
    })
    .await?;
    render_submission(&job, args.output)
}

pub(super) async fn run_mid_worker(
    job_id: &str,
    bot: &'static str,
    definition: &MidPriceJobDefinition,
) -> Result<()> {
    definition.validate()?;
    let definition = OutcomeRunDefinition::from_mid(bot, definition)?;
    let network = HyperliquidNetwork::from_testnet(definition.testnet);
    let (_, primary_side) =
        crate::providers::hyperliquid::outcomes::parse_symbol(&definition.primary_symbol)?;
    let pair = resolve_pair(network, definition.outcome_id, primary_side).await?;
    ensure_pair_identity(&definition, &pair)?;
    let adapter = ExecutionAdapter::new(definition.venue, definition.testnet).await?;
    let account = ExecutionAdapter::configured_account(definition.venue)?;
    let baseline = adapter.account_snapshot(&account).await?;
    let baseline_primary = holding_total(&baseline, &pair.primary.symbol);
    let baseline_complement = holding_total(&baseline, &pair.complement.symbol);
    let initial_book =
        live_orderbook(definition.venue, &pair.primary.symbol, definition.testnet).await?;
    let (initial_bid, initial_ask) = book_prices(&initial_book)?;
    let initial_quotes = executable_quote_prices(
        initial_bid,
        initial_ask,
        definition.spread_bps,
        outcome_execution_rules().tick_size,
        outcome_execution_rules().price_precision,
    )?;
    crate::runtime::append_bot_output(
        job_id,
        &plan_view(&definition, &pair, initial_quotes, false),
    )?;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(definition.duration_seconds);
    let mut book = spawn_book_feed(
        definition.venue,
        definition.testnet,
        pair.primary.symbol.clone(),
    );
    let mut account_events =
        spawn_account_feed(definition.venue, definition.testnet, account.clone());
    let mut ledger = OutcomeLedger::new(definition.requested_margin);
    let mut action_sequence = 0_u64;
    let mut order_sequence = 0_u64;
    let mut cancel_sequence = 0_u64;
    let mut working = HashMap::<OutcomeSide, WorkingOrder>::new();
    let mut order_sides = HashMap::<String, OutcomeSide>::new();
    let mut account_connected = false;
    let mut cycle_started = false;
    let mut cycle_primary_start = 0.0;
    let mut cycle_complement_start = 0.0;
    let mut cycle_quotes = initial_quotes;
    let mut cycle_quoted_at = Instant::now();
    let mut cycle_locked = false;
    let mut next_quote_retry = Instant::now();
    let mut last_mark = initial_quotes.reference_price;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut maintenance = tokio::time::interval(Duration::from_millis(250));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install outcome worker termination handler")?;

    let outcome: Result<&'static str> = async {
        let status = loop {
            tokio::select! {
                changed = book.changed() => {
                    if changed.is_err() {
                        bail!("outcome order-book task stopped");
                    }
                    let state = book.borrow().clone();
                    if let Some(ref error) = state.error {
                        append_market_data(job_id, definition.bot, "orderbook", "disconnected", Some(error))?;
                    }
                    if let Some(mark) = book_mark(&state) {
                        last_mark = mark;
                    }
                }
                event = account_events.recv() => {
                    match event.context("outcome account-event task stopped")? {
                        AccountFeedEvent::Connected => {
                            account_connected = true;
                            append_market_data(job_id, definition.bot, "account", "connected", None)?;
                        }
                        AccountFeedEvent::Disconnected(error) => {
                            account_connected = false;
                            append_market_data(job_id, definition.bot, "account", "disconnected", Some(&error))?;
                        }
                        AccountFeedEvent::Recovery { open_orders, fills } => {
                            reconcile_open_orders(&mut working, &open_orders);
                            for fill in fills {
                                apply_fill(job_id, definition.bot, &mut ledger, &order_sides, fill)?;
                            }
                        }
                        AccountFeedEvent::Data(value) => {
                            apply_account_value(
                                job_id,
                                definition.bot,
                                value,
                                &mut ledger,
                                &order_sides,
                                &mut working,
                            )?;
                        }
                    }
                }
                _ = maintenance.tick() => {
                    if !account_connected {
                        continue;
                    }
                    if !cycle_started {
                        if book.borrow().top.is_none() {
                            continue;
                        }
                        cycle_quotes = quotes_from_book(&definition, &book)?;
                        validate_quote_notional(definition.pair_size, cycle_quotes)?;
                        action_sequence = action_sequence.saturating_add(1);
                        split_cycle(job_id, action_sequence, &definition).await?;
                        ledger.record_split(definition.pair_size, cycle_quotes.reference_price);
                        wait_for_split_holdings(
                            &adapter,
                            &account,
                            &pair,
                            baseline_primary + ledger.primary_inventory(),
                            baseline_complement + ledger.complement_inventory(),
                        ).await?;
                        cycle_primary_start = ledger.primary_sold_size;
                        cycle_complement_start = ledger.complement_sold_size;
                        cycle_started = true;
                        if let Err(error) = place_quote_pair(
                            job_id,
                            &definition,
                            &pair,
                            &account,
                            cycle_quotes,
                            definition.pair_size,
                            &mut order_sequence,
                            &mut cancel_sequence,
                            &mut working,
                            &mut order_sides,
                        ).await {
                            if !is_post_only_crossing(&error) {
                                return Err(error);
                            }
                            append_quote_retry(job_id, definition.bot, &error)?;
                            next_quote_retry = Instant::now() + Duration::from_secs(1);
                        }
                        cycle_quoted_at = Instant::now();
                        cycle_locked = false;
                        continue;
                    }

                    let primary_cycle_filled = ledger.primary_sold_size - cycle_primary_start;
                    let complement_cycle_filled = ledger.complement_sold_size - cycle_complement_start;
                    cycle_locked |= primary_cycle_filled > EPSILON || complement_cycle_filled > EPSILON;
                    if primary_cycle_filled + EPSILON >= definition.pair_size
                        && complement_cycle_filled + EPSILON >= definition.pair_size
                    {
                        ledger.completed_cycles = ledger.completed_cycles.saturating_add(1);
                        crate::runtime::append_bot_output(job_id, &serde_json::json!({
                            "type": "bot.outcome.cycle",
                            "bot": definition.bot,
                            "jobId": job_id,
                            "status": "completed",
                            "cycle": ledger.completed_cycles,
                            "primarySold": primary_cycle_filled,
                            "complementSold": complement_cycle_filled,
                            "grossProfitPerPair": gross_profit_per_pair(cycle_quotes),
                        }))?;
                        working.clear();
                        cycle_started = false;
                        continue;
                    }

                    if Instant::now() >= next_quote_retry {
                        let remaining = [
                            (OutcomeSide::Complement, definition.pair_size - complement_cycle_filled),
                            (OutcomeSide::Primary, definition.pair_size - primary_cycle_filled),
                        ];
                        for (side, size) in remaining {
                            if size <= EPSILON || working.contains_key(&side) {
                                continue;
                            }
                            let (instrument, price) = match side {
                                OutcomeSide::Primary => (&pair.primary, cycle_quotes.primary_sell_price),
                                OutcomeSide::Complement => (&pair.complement, cycle_quotes.complement_sell_price),
                            };
                            if size * price + EPSILON < OUTCOME_MIN_NOTIONAL {
                                continue;
                            }
                            if let Err(error) = place_single_quote(
                                job_id,
                                &definition,
                                instrument,
                                side,
                                &account,
                                price,
                                size,
                                &mut order_sequence,
                                &mut working,
                                &mut order_sides,
                            ).await {
                                if !is_post_only_crossing(&error) {
                                    return Err(error);
                                }
                                append_quote_retry(job_id, definition.bot, &error)?;
                            }
                        }
                        next_quote_retry = Instant::now() + Duration::from_secs(1);
                    }

                    if !cycle_locked
                        && cycle_quoted_at.elapsed() >= Duration::from_secs_f64(definition.refresh_seconds)
                    {
                        let proposed = quotes_from_book(&definition, &book)?;
                        let drift = (proposed.reference_price - cycle_quotes.reference_price).abs()
                            / cycle_quotes.reference_price * 10_000.0;
                        if drift > definition.refresh_tolerance_bps {
                            cancel_all(
                                job_id,
                                &definition,
                                &mut cancel_sequence,
                                &mut working,
                            ).await?;
                            // A maker fill can race the cancellation. Reconcile
                            // before moving the pair so a newly one-sided cycle
                            // remains locked to its original exit prices.
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            recover_known_fills(
                                job_id,
                                definition.bot,
                                &adapter,
                                &account,
                                &order_sides,
                                &mut ledger,
                            ).await?;
                            let primary_after_cancel = ledger.primary_sold_size - cycle_primary_start;
                            let complement_after_cancel = ledger.complement_sold_size - cycle_complement_start;
                            if primary_after_cancel > EPSILON || complement_after_cancel > EPSILON {
                                cycle_locked = true;
                                next_quote_retry = Instant::now();
                                continue;
                            }
                            cycle_quotes = proposed;
                            if let Err(error) = place_quote_pair(
                                job_id,
                                &definition,
                                &pair,
                                &account,
                                cycle_quotes,
                                definition.pair_size,
                                &mut order_sequence,
                                &mut cancel_sequence,
                                &mut working,
                                &mut order_sides,
                            ).await {
                                if !is_post_only_crossing(&error) {
                                    return Err(error);
                                }
                                append_quote_retry(job_id, definition.bot, &error)?;
                                next_quote_retry = Instant::now() + Duration::from_secs(1);
                            }
                            cycle_quoted_at = Instant::now();
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let performance = ledger.performance(last_mark);
                    crate::runtime::bot_worker_heartbeat(job_id, std::process::id(), Some(&performance)).await?;
                    if let Some(percent) = definition.stop_loss_pct.filter(|value| *value > 0.0)
                        && performance.trading_pnl.is_some_and(|pnl| pnl <= -(definition.requested_margin * percent / 100.0))
                    {
                        crate::runtime::append_bot_output(job_id, &serde_json::json!({
                            "type": "bot.stop_loss",
                            "bot": definition.bot,
                            "jobId": job_id,
                            "pnl": performance.trading_pnl,
                            "limit": -(definition.requested_margin * percent / 100.0),
                            "mark": last_mark,
                        }))?;
                        break "stop_loss";
                    }
                }
                _ = &mut deadline_sleep => break "duration_elapsed",
                _ = terminate.recv() => break "stopped",
                _ = tokio::signal::ctrl_c() => break "stopped",
            }
        };
        Ok(status)
    }
    .await;

    let cleanup = cleanup(
        job_id,
        &definition,
        &pair,
        &adapter,
        &account,
        baseline_primary,
        baseline_complement,
        &mut action_sequence,
        &mut order_sequence,
        &mut cancel_sequence,
        &mut working,
        &mut order_sides,
        &mut ledger,
        last_mark,
    )
    .await;
    let status = match (outcome, cleanup) {
        (Ok(status), Ok(())) => status,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error),
        (Err(error), Err(cleanup)) => {
            return Err(error).context(format!("outcome cleanup also failed: {cleanup:#}"));
        }
    };
    let performance = ledger.performance(last_mark);
    crate::runtime::bot_worker_heartbeat(job_id, std::process::id(), Some(&performance)).await?;
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.run.finished",
            "bot": definition.bot,
            "jobId": job_id,
            "status": status,
            "performance": performance,
            "elapsedMs": started.elapsed().as_millis(),
        }),
    )?;
    if status == "stopped" {
        Err(BotStopped.into())
    } else {
        Ok(())
    }
}

pub(super) async fn run_grid_worker(job_id: &str, definition: &GridJobDefinition) -> Result<()> {
    definition.validate()?;
    let run = OutcomeRunDefinition::from_grid(definition)?;
    let network = HyperliquidNetwork::from_testnet(definition.testnet);
    let (_, primary_side) =
        crate::providers::hyperliquid::outcomes::parse_symbol(&run.primary_symbol)?;
    let pair = resolve_pair(network, run.outcome_id, primary_side).await?;
    ensure_pair_identity(&run, &pair)?;
    let adapter = ExecutionAdapter::new(definition.venue, definition.testnet).await?;
    let account = ExecutionAdapter::configured_account(definition.venue)?;
    let baseline = adapter.account_snapshot(&account).await?;
    let baseline_primary = holding_total(&baseline, &pair.primary.symbol);
    let baseline_complement = holding_total(&baseline, &pair.complement.symbol);
    let initial_book =
        live_orderbook(definition.venue, &pair.primary.symbol, definition.testnet).await?;
    let (best_bid, best_ask) = book_prices(&initial_book)?;
    let anchor = (best_bid + best_ask) / 2.0;
    let initial_quotes = outcome_grid_quotes(
        anchor,
        definition.levels_per_side,
        definition.step_bps,
        run.pair_size,
    )?;
    validate_grid_quotes(&initial_quotes, &pair)?;
    crate::runtime::append_bot_output(
        job_id,
        &outcome_grid_plan_view(definition, &pair, anchor, &initial_quotes, false)?,
    )?;

    let mut ledger = OutcomeLedger::new(definition.max_inventory_margin);
    let mut action_sequence = 1_u64;
    crate::runtime::submit_bot_outcome_action(
        job_id,
        action_sequence,
        &UserOutcomeAction::Split {
            outcome: run.outcome_id,
            amount: wire_number(run.pair_size),
        },
    )
    .await?;
    ledger.record_split(run.pair_size, anchor);
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.outcome.action",
            "bot": "grid",
            "jobId": job_id,
            "action": "split",
            "outcome": run.outcome_id,
            "size": run.pair_size,
        }),
    )?;
    wait_for_split_holdings(
        &adapter,
        &account,
        &pair,
        baseline_primary + run.pair_size,
        baseline_complement + run.pair_size,
    )
    .await?;

    let mut slots = initial_quotes
        .into_iter()
        .map(OutcomeGridSlot::new)
        .map(|slot| (slot.key(), slot))
        .collect::<HashMap<_, _>>();
    let mut order_slots = HashMap::<String, OutcomeGridKey>::new();
    let mut order_sides = HashMap::<String, OutcomeSide>::new();
    let mut order_sequence = 0_u64;
    let mut cancel_sequence = 0_u64;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(definition.duration_seconds);
    let mut book = spawn_book_feed(
        definition.venue,
        definition.testnet,
        pair.primary.symbol.clone(),
    );
    let mut last_mark = anchor;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut maintenance = tokio::time::interval(Duration::from_millis(500));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install outcome grid termination handler")?;

    let running: Result<&'static str> = async {
        loop {
            tokio::select! {
                changed = book.changed() => {
                    if changed.is_err() {
                        bail!("outcome grid order-book task stopped");
                    }
                    let state = book.borrow().clone();
                    if let Some(error) = state.error.as_deref() {
                        append_market_data(job_id, "grid", "orderbook", "disconnected", Some(error))?;
                    }
                    if let Some(mark) = book_mark(&state) {
                        last_mark = mark;
                    }
                }
                _ = maintenance.tick() => {
                    reconcile_outcome_grid_fills(
                        job_id,
                        &adapter,
                        &account,
                        last_mark,
                        &mut OutcomeGridFillState {
                            ledger: &mut ledger,
                            order_slots: &order_slots,
                            order_sides: &order_sides,
                            slots: &mut slots,
                        },
                    ).await?;
                    let open = adapter
                        .open_orders(&account)
                        .await?
                        .into_iter()
                        .map(|order| order.order_id)
                        .collect::<HashSet<_>>();
                    for slot in slots.values_mut() {
                        if let Some(order) = slot.working.as_ref()
                            && !open.contains(&order.order_id)
                        {
                            slot.working = None;
                        }
                    }

                    let completed = slots
                        .iter()
                        .filter_map(|(key, slot)| {
                            (slot.filled_size + EPSILON >= slot.initial.size).then_some(*key)
                        })
                        .collect::<Vec<_>>();
                    for key in completed {
                        let slot = slots.get_mut(&key).context("outcome grid slot disappeared")?;
                        let from = slot.current_side;
                        let completed_cycle = slot.advance();
                        if completed_cycle {
                            action_sequence = action_sequence.saturating_add(1);
                            crate::runtime::submit_bot_outcome_action(
                                job_id,
                                action_sequence,
                                &UserOutcomeAction::Split {
                                    outcome: run.outcome_id,
                                    amount: wire_number(slot.initial.size),
                                },
                            ).await?;
                            ledger.record_split(slot.initial.size, last_mark);
                            ledger.completed_cycles = ledger.completed_cycles.saturating_add(1);
                            wait_for_split_holdings(
                                &adapter,
                                &account,
                                &pair,
                                baseline_primary + ledger.primary_inventory(),
                                baseline_complement + ledger.complement_inventory(),
                            ).await?;
                        }
                        crate::runtime::append_bot_output(job_id, &serde_json::json!({
                            "type": "bot.grid.flip",
                            "bot": "grid",
                            "jobId": job_id,
                            "lane": if key.initial_buy { "BUY" } else { "SELL" },
                            "level": key.level,
                            "fromSide": normalized_side_name(from),
                            "toSide": normalized_side_name(slot.current_side),
                            "price": slot.current_price,
                            "size": slot.initial.size,
                            "outcomeExecution": true,
                        }))?;
                    }

                    let state = book.borrow().clone();
                    let top = state.top.as_ref();
                    for (key, slot) in &mut slots {
                        if slot.working.is_some() || slot.remaining() <= EPSILON {
                            continue;
                        }
                        let Some(top) = top else { continue; };
                        let (Some(best_bid), Some(best_ask)) = (top.best_bid, top.best_ask) else {
                            continue;
                        };
                        let maker_safe = match slot.current_side {
                            OrderSide::Buy => slot.current_price < best_ask.price,
                            OrderSide::Sell => slot.current_price > best_bid.price,
                        };
                        if !maker_safe {
                            continue;
                        }
                        let (instrument, venue_price) =
                            venue_quote(&pair, slot.current_side, slot.current_price)?;
                        let side = match slot.current_side {
                            OrderSide::Buy => OutcomeSide::Complement,
                            OrderSide::Sell => OutcomeSide::Primary,
                        };
                        order_sequence = order_sequence.saturating_add(1);
                        let plan = sell_plan(
                            instrument,
                            &account,
                            definition.testnet,
                            slot.remaining(),
                            Some(venue_price),
                            venue_price,
                        )?;
                        match crate::runtime::submit_bot_trade(job_id, order_sequence, &plan).await {
                            Ok(receipt) => {
                                let order = working_order(&plan, side, receipt)?;
                                append_outcome_grid_quote(job_id, "resting", *key, slot.current_side, &order)?;
                                order_slots.insert(order.order_id.clone(), *key);
                                order_sides.insert(order.order_id.clone(), side);
                                slot.working = Some(order);
                            }
                            Err(error) if is_post_only_crossing(&error) => {
                                crate::runtime::append_bot_output(job_id, &serde_json::json!({
                                    "type": "bot.quote",
                                    "bot": "grid",
                                    "jobId": job_id,
                                    "status": "rejectedCrossing",
                                    "side": normalized_side_name(slot.current_side),
                                    "level": key.level,
                                    "orderId": "-",
                                    "price": slot.current_price,
                                    "size": slot.remaining(),
                                }))?;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let performance = ledger.performance(last_mark);
                    crate::runtime::bot_worker_heartbeat(
                        job_id,
                        std::process::id(),
                        Some(&performance),
                    ).await?;
                    if let Some(percent) = definition.stop_loss_pct.filter(|value| *value > 0.0)
                        && performance.trading_pnl.is_some_and(|pnl| {
                            pnl <= -(definition.max_inventory_margin * percent / 100.0)
                        })
                    {
                        crate::runtime::append_bot_output(job_id, &serde_json::json!({
                            "type": "bot.stop_loss",
                            "bot": "grid",
                            "jobId": job_id,
                            "pnl": performance.trading_pnl,
                            "limit": -(definition.max_inventory_margin * percent / 100.0),
                            "mark": last_mark,
                        }))?;
                        break Ok("stop_loss");
                    }
                }
                _ = &mut deadline_sleep => break Ok("duration_elapsed"),
                _ = terminate.recv() => break Ok("stopped"),
                _ = tokio::signal::ctrl_c() => break Ok("stopped"),
            }
        }
    }.await;

    let cancel_result = cancel_outcome_grid_orders(
        job_id,
        definition,
        &account,
        &mut cancel_sequence,
        &mut slots,
    )
    .await;
    let _ = reconcile_outcome_grid_fills(
        job_id,
        &adapter,
        &account,
        last_mark,
        &mut OutcomeGridFillState {
            ledger: &mut ledger,
            order_slots: &order_slots,
            order_sides: &order_sides,
            slots: &mut slots,
        },
    )
    .await;
    let mut empty = HashMap::new();
    let cleanup_result = cleanup(
        job_id,
        &run,
        &pair,
        &adapter,
        &account,
        baseline_primary,
        baseline_complement,
        &mut action_sequence,
        &mut order_sequence,
        &mut cancel_sequence,
        &mut empty,
        &mut order_sides,
        &mut ledger,
        last_mark,
    )
    .await;
    cancel_result?;
    cleanup_result?;
    let status = running?;
    let performance = ledger.performance(last_mark);
    crate::runtime::bot_worker_heartbeat(job_id, std::process::id(), Some(&performance)).await?;
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.run.finished",
            "bot": "grid",
            "jobId": job_id,
            "status": status,
            "performance": performance,
            "elapsedMs": started.elapsed().as_millis(),
        }),
    )?;
    if status == "stopped" {
        Err(BotStopped.into())
    } else {
        Ok(())
    }
}

async fn reconcile_outcome_grid_fills(
    job_id: &str,
    adapter: &ExecutionAdapter,
    account: &str,
    primary_mark: f64,
    state: &mut OutcomeGridFillState<'_>,
) -> Result<()> {
    for fill in adapter.fills(account).await? {
        let Some(order_id) = fill.order_id.as_deref() else {
            continue;
        };
        let (Some(key), Some(side)) = (
            state.order_slots.get(order_id).copied(),
            state.order_sides.get(order_id).copied(),
        ) else {
            continue;
        };
        if !state.ledger.record_fill(side, &fill) {
            continue;
        }
        if let Some(slot) = state.slots.get_mut(&key) {
            slot.filled_size = (slot.filled_size + fill.amount).min(slot.initial.size);
        }
        crate::runtime::append_bot_output(
            job_id,
            &serde_json::json!({
                "type": "bot.fill",
                "bot": "grid",
                "jobId": job_id,
            "side": normalized_side_name(state.slots.get(&key).map_or(OrderSide::Sell, |slot| slot.current_side)),
                "outcomeSide": side.name(),
                "level": key.level,
                "orderId": order_id,
                "size": fill.amount,
                "price": fill.price,
                "fee": fill.fee,
            "primarySold": state.ledger.primary_sold_size,
            "complementSold": state.ledger.complement_sold_size,
            "primaryInventory": state.ledger.primary_inventory(),
            "complementInventory": state.ledger.complement_inventory(),
            "performance": state.ledger.performance(primary_mark),
            }),
        )?;
    }
    Ok(())
}

fn append_outcome_grid_quote(
    job_id: &str,
    status: &str,
    key: OutcomeGridKey,
    normalized_side: OrderSide,
    order: &WorkingOrder,
) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.quote",
            "bot": "grid",
            "jobId": job_id,
            "status": status,
            "side": normalized_side_name(normalized_side),
            "outcomeSide": order.side.name(),
            "level": key.level,
            "orderId": order.order_id,
            "price": order.price,
            "size": order.original_size,
        }),
    )
}

async fn cancel_outcome_grid_orders(
    job_id: &str,
    definition: &GridJobDefinition,
    account: &str,
    cancel_sequence: &mut u64,
    slots: &mut HashMap<OutcomeGridKey, OutcomeGridSlot>,
) -> Result<()> {
    for (key, slot) in slots {
        let Some(order) = slot.working.take() else {
            continue;
        };
        *cancel_sequence = cancel_sequence.saturating_add(1);
        let plan = CancelPlan {
            created_at_ms: now_ms()?,
            venue: definition.venue,
            testnet: definition.testnet,
            account: account.to_string(),
            internal_symbol: order.symbol.clone(),
            venue_symbol: order.venue_symbol.clone(),
            order_id: order.order_id.clone(),
        };
        match crate::runtime::submit_bot_cancel(job_id, *cancel_sequence, &plan).await {
            Ok(_) => {
                append_outcome_grid_quote(job_id, "cancelled", *key, slot.current_side, &order)?
            }
            Err(error) if is_order_gone(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn resolve_pair(
    network: HyperliquidNetwork,
    outcome_id: u32,
    primary_side: u8,
) -> Result<OutcomePair> {
    if primary_side > 1 {
        bail!("outcome {outcome_id} side must be 0 or 1");
    }
    let instruments = crate::providers::hyperliquid::outcomes::instruments(network).await?;
    let candidates = instruments
        .into_iter()
        .filter(|instrument| instrument.outcome_id == outcome_id)
        .collect::<Vec<_>>();
    pair_from_candidates(&candidates, outcome_id, primary_side)
}

fn pair_from_candidates(
    candidates: &[OutcomeInstrument],
    outcome_id: u32,
    primary_side: u8,
) -> Result<OutcomePair> {
    let primary = candidates
        .iter()
        .find(|instrument| instrument.side == primary_side)
        .cloned()
        .with_context(|| format!("outcome {outcome_id} has no active side {primary_side}"))?;
    let complement_side = 1 - primary_side;
    let complement = candidates
        .iter()
        .find(|instrument| instrument.side == complement_side)
        .cloned()
        .with_context(|| format!("outcome {outcome_id} has no active side {complement_side}"))?;
    if primary.settled || complement.settled {
        bail!("outcome {outcome_id} is settled");
    }
    if primary.quote_token != complement.quote_token {
        bail!("outcome {outcome_id} binary sides use different quote tokens");
    }
    Ok(OutcomePair {
        primary,
        complement,
    })
}

fn ensure_pair_identity(definition: &OutcomeRunDefinition, pair: &OutcomePair) -> Result<()> {
    if pair.primary.symbol != definition.primary_symbol
        || pair.complement.symbol != definition.complement_symbol
        || pair.primary.metadata_fingerprint != definition.primary_market_fingerprint
        || pair.complement.metadata_fingerprint != definition.complement_market_fingerprint
    {
        bail!("outcome metadata changed after the bot was planned");
    }
    Ok(())
}

fn plan_view<'a>(
    definition: &'a OutcomeRunDefinition,
    pair: &'a OutcomePair,
    quotes: OutcomeQuotes,
    dry_run: bool,
) -> OutcomePlanView<'a> {
    OutcomePlanView {
        r#type: "bot.plan",
        bot: definition.bot,
        venue: "hyperliquid-outcomes",
        network: HyperliquidNetwork::from_testnet(definition.testnet).label(),
        outcome_id: definition.outcome_id,
        question: pair.question(),
        outcome: &pair.primary.outcome_name,
        quote_token: &pair.primary.quote_token,
        normalized_symbol: &definition.symbol,
        primary_name: &pair.primary.side_name,
        primary_symbol: &pair.primary.symbol,
        complement_name: &pair.complement.side_name,
        complement_symbol: &pair.complement.symbol,
        reference_price: quotes.reference_price,
        normalized_bid: quotes.normalized_bid,
        normalized_ask: quotes.normalized_ask,
        complement_sell_price: quotes.complement_sell_price,
        primary_sell_price: quotes.primary_sell_price,
        pair_size: definition.pair_size,
        requested_margin: definition.requested_margin,
        gross_profit_per_pair: gross_profit_per_pair(quotes),
        estimated_gross_cycle_profit: gross_profit_per_pair(quotes) * definition.pair_size,
        spread_bps: definition.spread_bps,
        refresh_seconds: definition.refresh_seconds,
        refresh_tolerance_bps: definition.refresh_tolerance_bps,
        stop_loss_pct: definition.stop_loss_pct,
        duration_secs: definition.duration_seconds,
        execution: "split collateral, then maker-only complementary-side sells",
        replenishment: "split the next pair only after both current sides sell",
        shutdown: "cancel quotes, merge balanced shares, sell unmatched residual shares",
        dry_run,
    }
}

fn render_plan(plan: &OutcomePlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "{} outcome market maker{}",
                plan.bot,
                if plan.dry_run {
                    " (dry run — nothing will be submitted)"
                } else {
                    ""
                }
            );
            println!("  venue / network:   {} / {}", plan.venue, plan.network);
            println!(
                "  outcome:           {} ({})",
                plan.outcome_id, plan.outcome
            );
            println!("  question:          {}", plan.question);
            println!(
                "  collateral:        {:.8} {}",
                plan.requested_margin, plan.quote_token
            );
            println!(
                "  split pair size:   {} {} + {} {}",
                plan.pair_size, plan.primary_name, plan.pair_size, plan.complement_name
            );
            println!(
                "  primary side:      {} ({})",
                plan.primary_name, plan.primary_symbol
            );
            println!(
                "  complement side:   {} ({})",
                plan.complement_name, plan.complement_symbol
            );
            println!(
                "  normalized book:   bid {} / ask {}",
                plan.normalized_bid, plan.normalized_ask
            );
            println!(
                "  venue orders:      SELL {} @ {} / SELL {} @ {}",
                plan.complement_name,
                plan.complement_sell_price,
                plan.primary_name,
                plan.primary_sell_price
            );
            println!(
                "  gross per pair:    {:.8} {}",
                plan.gross_profit_per_pair, plan.quote_token
            );
            println!(
                "  gross per cycle:   {:.8} {} before fees",
                plan.estimated_gross_cycle_profit, plan.quote_token
            );
            println!("  spread:            {} bps", plan.spread_bps);
            println!(
                "  refresh:           {}s after {} bps midpoint drift, before any fill",
                plan.refresh_seconds, plan.refresh_tolerance_bps
            );
            if let Some(percent) = plan.stop_loss_pct {
                println!("  stop loss:         {percent}% of allocated collateral");
            }
            println!("  duration:          {}s", plan.duration_secs);
            println!("  execution:         {}", plan.execution);
            println!("  replenishment:     {}", plan.replenishment);
            println!("  shutdown:          {}", plan.shutdown);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn outcome_grid_plan_view<'a>(
    definition: &'a GridJobDefinition,
    pair: &'a OutcomePair,
    reference_price: f64,
    quotes: &[GridQuote],
    dry_run: bool,
) -> Result<OutcomeGridPlanView<'a>> {
    let outcome = definition
        .outcome
        .as_ref()
        .context("outcome execution metadata is missing")?;
    Ok(OutcomeGridPlanView {
        r#type: "bot.plan",
        bot: "grid",
        venue: "hyperliquid-outcomes",
        network: HyperliquidNetwork::from_testnet(definition.testnet).label(),
        symbol: &definition.symbol,
        outcome_id: outcome.outcome_id,
        question: pair.question(),
        quote_token: &outcome.quote_token,
        primary_name: &pair.primary.side_name,
        primary_symbol: &pair.primary.symbol,
        complement_name: &pair.complement.side_name,
        complement_symbol: &pair.complement.symbol,
        reference_price,
        pair_size: outcome.pair_size,
        requested_margin: definition.max_inventory_margin,
        levels_per_side: definition.levels_per_side,
        step_bps: definition.step_bps,
        levels: quotes
            .iter()
            .map(|quote| {
                let (instrument, price) = venue_quote(pair, quote.side, quote.price)?;
                Ok(OutcomeGridPlanLevel {
                    level: quote.level,
                    normalized_side: normalized_side_name(quote.side),
                    normalized_price: quote.price,
                    paired_price: quote.paired_price,
                    venue_symbol: instrument.symbol.clone(),
                    venue_side: "SELL",
                    venue_price: price,
                    size: quote.size,
                })
            })
            .collect::<Result<Vec<_>>>()?,
        stop_loss_pct: definition.stop_loss_pct,
        duration_secs: definition.duration_seconds,
        execution: "split collateral, then maker-only complementary-side grid sells",
        shutdown: "cancel quotes, merge balanced shares, sell unmatched residual shares",
        dry_run,
    })
}

fn render_outcome_grid_plan(plan: &OutcomeGridPlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "grid outcome market maker{}",
                if plan.dry_run {
                    " (dry run — nothing will be submitted)"
                } else {
                    ""
                }
            );
            println!("  venue / network:   {} / {}", plan.venue, plan.network);
            println!("  outcome:           {}", plan.outcome_id);
            println!("  question:          {}", plan.question);
            println!(
                "  collateral:        {:.8} {}",
                plan.requested_margin, plan.quote_token
            );
            println!(
                "  split pair size:   {} {} + {} {}",
                plan.pair_size, plan.primary_name, plan.pair_size, plan.complement_name
            );
            println!(
                "  primary side:      {} ({})",
                plan.primary_name, plan.primary_symbol
            );
            println!(
                "  complement side:   {} ({})",
                plan.complement_name, plan.complement_symbol
            );
            println!("  reference price:   {}", plan.reference_price);
            println!("  levels per side:   {}", plan.levels_per_side);
            println!("  grid step:         {} bps", plan.step_bps);
            for level in &plan.levels {
                println!(
                    "  {:<4} L{}:          {} -> {} as SELL {} @ {} size={}",
                    level.normalized_side,
                    level.level,
                    level.normalized_price,
                    level.paired_price,
                    level.venue_symbol,
                    level.venue_price,
                    level.size,
                );
            }
            if let Some(percent) = plan.stop_loss_pct {
                println!("  stop loss:         {percent}% of allocated collateral");
            }
            println!("  duration:          {}s", plan.duration_secs);
            println!("  execution:         {}", plan.execution);
            println!("  shutdown:          {}", plan.shutdown);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn normalized_side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn venue_quote(
    pair: &OutcomePair,
    normalized_side: OrderSide,
    normalized_price: f64,
) -> Result<(&OutcomeInstrument, f64)> {
    let (instrument, raw_price) = match normalized_side {
        OrderSide::Buy => (&pair.complement, 1.0 - normalized_price),
        OrderSide::Sell => (&pair.primary, normalized_price),
    };
    let rules = outcome_execution_rules();
    let price = normalize_price_for(raw_price, rules.size_precision, rules.price_precision, true);
    validate_price_for(price, rules.size_precision, rules.price_precision)?;
    Ok((instrument, price))
}

fn outcome_grid_quotes(
    center_price: f64,
    levels_per_side: u16,
    step_bps: f64,
    pair_size: f64,
) -> Result<Vec<GridQuote>> {
    let rules = outcome_execution_rules();
    quote_grid(GridSpec {
        center_price,
        levels_per_side,
        step_bps,
        // quote_grid splits this equally across its initial BUY and SELL
        // ladders. The remaining half of each split token stays available to
        // fund the paired order after an initial leg fills.
        max_inventory_size: pair_size,
        tick_size: rules.tick_size,
        price_precision: rules.price_precision,
    })
}

fn validate_grid_quotes(quotes: &[GridQuote], pair: &OutcomePair) -> Result<()> {
    for quote in quotes {
        let (_, venue_price) = venue_quote(pair, quote.side, quote.price)?;
        let (_, paired_venue_price) =
            venue_quote(pair, opposite_side(quote.side), quote.paired_price)?;
        if !(0.0..1.0).contains(&quote.price)
            || !(0.0..1.0).contains(&quote.paired_price)
            || !(0.0..1.0).contains(&venue_price)
            || !(0.0..1.0).contains(&paired_venue_price)
            || quote.size * venue_price + EPSILON < OUTCOME_MIN_NOTIONAL
            || quote.size * paired_venue_price + EPSILON < OUTCOME_MIN_NOTIONAL
        {
            bail!(
                "outcome grid level {} is outside probability bounds or below the venue minimum notional",
                quote.level
            );
        }
    }
    Ok(())
}

fn book_prices(book: &crate::domain::types::OrderBookSnapshot) -> Result<(f64, f64)> {
    Ok((
        book.bids
            .first()
            .context("outcome primary-side book has no bid")?
            .price,
        book.asks
            .first()
            .context("outcome primary-side book has no ask")?
            .price,
    ))
}

fn book_mark(state: &BookFeedState) -> Option<f64> {
    let top = state.top.as_ref()?;
    Some((top.best_bid?.price + top.best_ask?.price) / 2.0)
}

fn quotes_from_book(
    definition: &OutcomeRunDefinition,
    book: &tokio::sync::watch::Receiver<BookFeedState>,
) -> Result<OutcomeQuotes> {
    let state = book.borrow().clone();
    let top = state
        .top
        .context("outcome primary-side order book is not connected")?;
    executable_quote_prices(
        top.best_bid
            .context("outcome primary-side book has no bid")?
            .price,
        top.best_ask
            .context("outcome primary-side book has no ask")?
            .price,
        definition.spread_bps,
        outcome_execution_rules().tick_size,
        outcome_execution_rules().price_precision,
    )
}

fn executable_quote_prices(
    best_bid: f64,
    best_ask: f64,
    spread_bps: f64,
    tick_size: f64,
    price_precision: u8,
) -> Result<OutcomeQuotes> {
    let mut quotes = quote_prices(best_bid, best_ask, spread_bps, tick_size, price_precision)?;
    let rules = outcome_execution_rules();
    quotes.primary_sell_price = normalize_price_for(
        quotes.primary_sell_price,
        rules.size_precision,
        rules.price_precision,
        true,
    );
    quotes.complement_sell_price = normalize_price_for(
        quotes.complement_sell_price,
        rules.size_precision,
        rules.price_precision,
        true,
    );
    validate_price_for(
        quotes.primary_sell_price,
        rules.size_precision,
        rules.price_precision,
    )?;
    validate_price_for(
        quotes.complement_sell_price,
        rules.size_precision,
        rules.price_precision,
    )?;
    quotes.normalized_ask = quotes.primary_sell_price;
    quotes.normalized_bid =
        round_decimal(1.0 - quotes.complement_sell_price, rules.price_precision);
    if quotes.normalized_bid <= 0.0
        || quotes.normalized_ask >= 1.0
        || quotes.normalized_ask <= quotes.normalized_bid
        || gross_profit_per_pair(quotes) <= 0.0
    {
        bail!("outcome market maker could not construct executable Hyperliquid quotes");
    }
    Ok(quotes)
}

fn round_decimal(value: f64, precision: u8) -> f64 {
    let scale = 10_f64.powi(i32::from(precision));
    (value * scale).round() / scale
}

fn validate_quote_notional(size: f64, quotes: OutcomeQuotes) -> Result<()> {
    for (side, price) in [
        ("primary-side", quotes.primary_sell_price),
        ("complement-side", quotes.complement_sell_price),
    ] {
        if size * price + EPSILON < OUTCOME_MIN_NOTIONAL {
            bail!(
                "{side} quote notional {:.8} is below Hyperliquid outcome minimum {}; increase --margin",
                size * price,
                OUTCOME_MIN_NOTIONAL
            );
        }
    }
    Ok(())
}

async fn require_quote_balance(
    testnet: bool,
    account: &str,
    quote_token: &str,
    required: f64,
) -> Result<()> {
    let snapshot = ExecutionAdapter::new(ExecutionVenue::HyperliquidOutcomes, testnet)
        .await?
        .account_snapshot(account)
        .await?;
    let available = snapshot
        .spot_balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case(quote_token))
        .map_or(0.0, |balance| balance.available);
    if available + EPSILON < required {
        bail!(
            "insufficient Hyperliquid outcome {quote_token}: {available:.8} available, {required:.8} required"
        );
    }
    Ok(())
}

async fn split_cycle(job_id: &str, sequence: u64, definition: &OutcomeRunDefinition) -> Result<()> {
    crate::runtime::submit_bot_outcome_action(
        job_id,
        sequence,
        &UserOutcomeAction::Split {
            outcome: definition.outcome_id,
            amount: wire_number(definition.pair_size),
        },
    )
    .await?;
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.outcome.action",
            "bot": definition.bot,
            "jobId": job_id,
            "action": "split",
            "outcome": definition.outcome_id,
            "size": definition.pair_size,
        }),
    )
}

async fn wait_for_split_holdings(
    adapter: &ExecutionAdapter,
    account: &str,
    pair: &OutcomePair,
    expected_primary: f64,
    expected_complement: f64,
) -> Result<()> {
    let deadline = Instant::now() + HOLDING_SYNC_TIMEOUT;
    loop {
        let snapshot = adapter.account_snapshot(account).await?;
        if holding_total(&snapshot, &pair.primary.symbol) + EPSILON >= expected_primary
            && holding_total(&snapshot, &pair.complement.symbol) + EPSILON >= expected_complement
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for split outcome holdings to appear");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn holding_total(snapshot: &crate::domain::execution::AccountSnapshot, symbol: &str) -> f64 {
    snapshot
        .outcome_holdings
        .iter()
        .find(|holding| holding.symbol == symbol)
        .map_or(0.0, |holding| holding.total)
}

#[allow(clippy::too_many_arguments)]
async fn place_quote_pair(
    job_id: &str,
    definition: &OutcomeRunDefinition,
    pair: &OutcomePair,
    account: &str,
    quotes: OutcomeQuotes,
    size: f64,
    order_sequence: &mut u64,
    cancel_sequence: &mut u64,
    working: &mut HashMap<OutcomeSide, WorkingOrder>,
    order_sides: &mut HashMap<String, OutcomeSide>,
) -> Result<()> {
    let plans = vec![
        sell_plan(
            &pair.complement,
            account,
            definition.testnet,
            size,
            Some(quotes.complement_sell_price),
            quotes.complement_sell_price,
        )?,
        sell_plan(
            &pair.primary,
            account,
            definition.testnet,
            size,
            Some(quotes.primary_sell_price),
            quotes.primary_sell_price,
        )?,
    ];
    let mut items = Vec::with_capacity(2);
    for plan in plans {
        *order_sequence = order_sequence.saturating_add(1);
        items.push((*order_sequence, plan));
    }
    let outcomes = crate::runtime::submit_bot_trades(job_id, &items).await?;
    let mut placed = Vec::<WorkingOrder>::new();
    let mut errors = Vec::new();
    for (((_, plan), side), outcome) in items
        .iter()
        .zip([OutcomeSide::Complement, OutcomeSide::Primary])
        .zip(outcomes)
    {
        match outcome.into_result() {
            Ok(receipt) => {
                let order_id = receipt.order_id.clone();
                match working_order(plan, side, receipt) {
                    Ok(order) => placed.push(order),
                    Err(error) => {
                        if let Some(order_id) = order_id {
                            order_sides.insert(order_id, side);
                        }
                        errors.push(format!("{error:#}"));
                    }
                }
            }
            Err(error) => errors.push(error),
        }
    }
    if !errors.is_empty() || placed.len() != 2 {
        // Retain ownership before canceling the successful sibling. A fill can
        // race the cancellation after a mixed batch result; keeping its ID lets
        // the account stream attribute that fill to this cycle.
        for order in &placed {
            order_sides.insert(order.order_id.clone(), order.side);
            working.insert(order.side, order.clone());
        }
        for order in placed {
            *cancel_sequence = cancel_sequence.saturating_add(1);
            let plan = cancel_from_order(definition, account, &order)?;
            match crate::runtime::submit_bot_cancel(job_id, *cancel_sequence, &plan).await {
                Ok(_) => {
                    append_quote(job_id, definition.bot, "cancelled", &order)?;
                    working.remove(&order.side);
                }
                Err(error) if is_order_gone(&error) => {
                    working.remove(&order.side);
                }
                Err(error) => return Err(error),
            }
        }
        bail!(
            "outcome quote pair was not placed atomically: {}",
            errors.join("; ")
        );
    }
    for order in placed {
        append_quote(job_id, definition.bot, "resting", &order)?;
        order_sides.insert(order.order_id.clone(), order.side);
        working.insert(order.side, order);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn place_single_quote(
    job_id: &str,
    definition: &OutcomeRunDefinition,
    instrument: &OutcomeInstrument,
    side: OutcomeSide,
    account: &str,
    price: f64,
    size: f64,
    order_sequence: &mut u64,
    working: &mut HashMap<OutcomeSide, WorkingOrder>,
    order_sides: &mut HashMap<String, OutcomeSide>,
) -> Result<()> {
    *order_sequence = order_sequence.saturating_add(1);
    let plan = sell_plan(
        instrument,
        account,
        definition.testnet,
        size,
        Some(price),
        price,
    )?;
    let receipt = crate::runtime::submit_bot_trade(job_id, *order_sequence, &plan).await?;
    let order = working_order(&plan, side, receipt)?;
    append_quote(job_id, definition.bot, "resting", &order)?;
    order_sides.insert(order.order_id.clone(), side);
    working.insert(side, order);
    Ok(())
}

fn working_order(
    plan: &TradePlan,
    side: OutcomeSide,
    receipt: ExecutionReceipt,
) -> Result<WorkingOrder> {
    if receipt.terminal {
        bail!(
            "post-only outcome quote returned terminal status `{}`",
            receipt.status
        );
    }
    Ok(WorkingOrder {
        order_id: receipt.order_id.context("outcome quote omitted order id")?,
        side,
        symbol: plan.internal_symbol.clone(),
        venue_symbol: plan.venue_symbol.clone(),
        price: plan.price.context("outcome quote omitted price")?,
        original_size: plan.size,
    })
}

fn sell_plan(
    instrument: &OutcomeInstrument,
    account: &str,
    testnet: bool,
    size: f64,
    price: Option<f64>,
    reference_price: f64,
) -> Result<TradePlan> {
    Ok(TradePlan {
        created_at_ms: now_ms()?,
        venue: ExecutionVenue::HyperliquidOutcomes,
        testnet,
        account: account.to_string(),
        internal_symbol: instrument.symbol.clone(),
        venue_symbol: instrument.coin.clone(),
        direction: PositionDirection::Short,
        side: OrderSide::Sell,
        order_kind: if price.is_some() {
            OrderKind::Limit
        } else {
            OrderKind::Market
        },
        time_in_force: price.map(|_| TimeInForce::Alo),
        requested_size: Some(size),
        size,
        price,
        reference_price,
        requested_margin: None,
        estimated_margin: size * reference_price,
        estimated_exposure: size * reference_price,
        projected_liquidation_price: None,
        leverage: None,
        reduce_only: false,
        stop_loss_price: None,
        take_profit_price: None,
        market_fingerprint: Some(instrument.metadata_fingerprint.clone()),
    })
}

fn apply_account_value(
    job_id: &str,
    bot: &str,
    value: Value,
    ledger: &mut OutcomeLedger,
    order_sides: &HashMap<String, OutcomeSide>,
    working: &mut HashMap<OutcomeSide, WorkingOrder>,
) -> Result<()> {
    match value.get("type").and_then(Value::as_str) {
        Some("fill") => {
            let Some(order_id) = value.get("orderId").and_then(Value::as_str) else {
                return Ok(());
            };
            let Some(side) = order_sides.get(order_id).copied() else {
                return Ok(());
            };
            let fill = Fill {
                venue: ExecutionVenue::HyperliquidOutcomes,
                internal_symbol: working
                    .get(&side)
                    .map_or_else(String::new, |order| order.symbol.clone()),
                venue_symbol: String::new(),
                registry_supported: true,
                side: OrderSide::Sell,
                amount: value.get("size").and_then(Value::as_f64).unwrap_or(0.0),
                price: value.get("price").and_then(Value::as_f64).unwrap_or(0.0),
                reason: "maker".to_string(),
                order_id: Some(order_id.to_string()),
                maker: true,
                fee: value.get("fee").and_then(Value::as_f64),
                fee_asset: None,
                slot: 0,
                ts_ms: value
                    .get("timestamp")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
            };
            apply_fill(job_id, bot, ledger, order_sides, fill)?;
        }
        Some("orderUpdate") => {
            let Some(order_id) = value.get("oid").and_then(Value::as_str) else {
                return Ok(());
            };
            let terminal = value
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| {
                    status.eq_ignore_ascii_case("cancelled")
                        || status.eq_ignore_ascii_case("rejected")
                });
            if terminal
                && let Some(side) = order_sides.get(order_id).copied()
                && working
                    .get(&side)
                    .is_some_and(|order| order.order_id == order_id)
            {
                working.remove(&side);
            }
        }
        _ => {}
    }
    Ok(())
}

fn apply_fill(
    job_id: &str,
    bot: &str,
    ledger: &mut OutcomeLedger,
    order_sides: &HashMap<String, OutcomeSide>,
    fill: Fill,
) -> Result<()> {
    let Some(order_id) = fill.order_id.as_deref() else {
        return Ok(());
    };
    let Some(side) = order_sides.get(order_id).copied() else {
        return Ok(());
    };
    if !ledger.record_fill(side, &fill) {
        return Ok(());
    }
    // Keep the order registered after a fill update. Hyperliquid can publish the
    // fill before its terminal order update; removing it here would let the
    // worker submit a duplicate replacement during that gap. Cycle accounting
    // determines completion, while recovery removes canceled orders.
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.fill",
            "bot": bot,
            "jobId": job_id,
            "outcomeSide": side.name(),
            "orderId": order_id,
            "size": fill.amount,
            "price": fill.price,
            "fee": fill.fee,
            "primarySold": ledger.primary_sold_size,
            "complementSold": ledger.complement_sold_size,
            "primaryInventory": ledger.primary_inventory(),
            "complementInventory": ledger.complement_inventory(),
        }),
    )
}

fn reconcile_open_orders(
    working: &mut HashMap<OutcomeSide, WorkingOrder>,
    open_orders: &[OpenOrder],
) {
    let open = open_orders
        .iter()
        .map(|order| order.order_id.as_str())
        .collect::<HashSet<_>>();
    working.retain(|_, order| open.contains(order.order_id.as_str()));
}

async fn recover_known_fills(
    job_id: &str,
    bot: &str,
    adapter: &ExecutionAdapter,
    account: &str,
    order_sides: &HashMap<String, OutcomeSide>,
    ledger: &mut OutcomeLedger,
) -> Result<()> {
    for fill in adapter.fills(account).await? {
        apply_fill(job_id, bot, ledger, order_sides, fill)?;
    }
    Ok(())
}

async fn cancel_all(
    job_id: &str,
    definition: &OutcomeRunDefinition,
    cancel_sequence: &mut u64,
    working: &mut HashMap<OutcomeSide, WorkingOrder>,
) -> Result<()> {
    let account = ExecutionAdapter::configured_account(definition.venue)?;
    let orders = working.values().cloned().collect::<Vec<_>>();
    for order in orders {
        *cancel_sequence = cancel_sequence.saturating_add(1);
        let plan = cancel_from_order(definition, &account, &order)?;
        match crate::runtime::submit_bot_cancel(job_id, *cancel_sequence, &plan).await {
            Ok(_) => append_quote(job_id, definition.bot, "cancelled", &order)?,
            Err(error) if is_order_gone(&error) => {}
            Err(error) => return Err(error),
        }
    }
    working.clear();
    Ok(())
}

fn cancel_from_order(
    definition: &OutcomeRunDefinition,
    account: &str,
    order: &WorkingOrder,
) -> Result<CancelPlan> {
    Ok(CancelPlan {
        created_at_ms: now_ms()?,
        venue: definition.venue,
        testnet: definition.testnet,
        account: account.to_string(),
        internal_symbol: order.symbol.clone(),
        venue_symbol: order.venue_symbol.clone(),
        order_id: order.order_id.clone(),
    })
}

fn append_quote(job_id: &str, bot: &str, status: &str, order: &WorkingOrder) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.quote",
            "bot": bot,
            "jobId": job_id,
            "status": status,
            "side": format!("SELL_{}", order.side.name()),
            "orderId": order.order_id,
            "price": order.price,
            "size": order.original_size,
        }),
    )
}

fn is_order_gone(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("already canceled")
        || message.contains("already cancelled")
        || message.contains("already filled")
        || message.contains("order was never placed")
}

fn is_post_only_crossing(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("post only")
        || message.contains("post-only")
        || message.contains("immediately matched")
        || message.contains("crossing")
}

fn append_quote_retry(job_id: &str, bot: &str, error: &anyhow::Error) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.quote",
            "bot": bot,
            "jobId": job_id,
            "status": "rejectedCrossing",
            "side": "PAIR",
            "orderId": "-",
            "error": format!("{error:#}"),
        }),
    )
}

#[allow(clippy::too_many_arguments)]
async fn cleanup(
    job_id: &str,
    definition: &OutcomeRunDefinition,
    pair: &OutcomePair,
    adapter: &ExecutionAdapter,
    account: &str,
    baseline_primary: f64,
    baseline_complement: f64,
    action_sequence: &mut u64,
    order_sequence: &mut u64,
    cancel_sequence: &mut u64,
    working: &mut HashMap<OutcomeSide, WorkingOrder>,
    order_sides: &mut HashMap<String, OutcomeSide>,
    ledger: &mut OutcomeLedger,
    primary_mark: f64,
) -> Result<()> {
    cancel_all(job_id, definition, cancel_sequence, working).await?;
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let snapshot = adapter.account_snapshot(account).await?;
        let primary_total = holding_total(&snapshot, &pair.primary.symbol);
        let complement_total = holding_total(&snapshot, &pair.complement.symbol);
        // Outcome balances are fungible, so the startup snapshot is the only
        // durable boundary between user-owned inventory and this job's shares.
        // This also lets cleanup recover a split that reached the venue before
        // the worker received its acknowledgement.
        let bot_primary = (primary_total - baseline_primary).max(0.0);
        let bot_complement = (complement_total - baseline_complement).max(0.0);
        let untracked_pair = (bot_primary - ledger.primary_inventory())
            .max(0.0)
            .min((bot_complement - ledger.complement_inventory()).max(0.0));
        if untracked_pair > EPSILON {
            ledger.record_split(untracked_pair, primary_mark);
        }
        let merge = bot_primary.min(bot_complement).floor();
        if merge >= 1.0 {
            *action_sequence = action_sequence.saturating_add(1);
            crate::runtime::submit_bot_outcome_action(
                job_id,
                *action_sequence,
                &UserOutcomeAction::Merge {
                    outcome: definition.outcome_id,
                    amount: Some(wire_number(merge)),
                },
            )
            .await?;
            crate::runtime::append_bot_output(
                job_id,
                &serde_json::json!({
                    "type": "bot.outcome.action",
                    "bot": definition.bot,
                    "jobId": job_id,
                    "action": "merge",
                    "outcome": definition.outcome_id,
                    "size": merge,
                }),
            )?;
            ledger.record_merge(merge);
            wait_for_holding_reduction(
                adapter,
                account,
                pair,
                primary_total - merge,
                complement_total - merge,
                deadline,
            )
            .await?;
            continue;
        }
        let (instrument, side, excess, reference) = if bot_primary >= 1.0 {
            (
                &pair.primary,
                OutcomeSide::Primary,
                bot_primary.floor(),
                primary_mark,
            )
        } else if bot_complement >= 1.0 {
            (
                &pair.complement,
                OutcomeSide::Complement,
                bot_complement.floor(),
                1.0 - primary_mark,
            )
        } else {
            break;
        };
        if excess * reference + EPSILON < OUTCOME_MIN_NOTIONAL {
            crate::runtime::append_bot_output(
                job_id,
                &serde_json::json!({
                    "type": "bot.outcome.residual",
                    "bot": definition.bot,
                    "jobId": job_id,
                    "side": side.name(),
                    "size": excess,
                    "reason": "below venue minimum notional",
                }),
            )?;
            break;
        }
        *order_sequence = order_sequence.saturating_add(1);
        let plan = sell_plan(
            instrument,
            account,
            definition.testnet,
            excess,
            None,
            reference,
        )?;
        let receipt = crate::runtime::submit_bot_trade(job_id, *order_sequence, &plan).await?;
        if let Some(order_id) = receipt.order_id.clone() {
            order_sides.insert(order_id, side);
        }
        let filled_size = receipt
            .filled_size
            .filter(|size| size.is_finite() && *size > EPSILON)
            .context("outcome cleanup market order reported no filled size")?;
        let expected_sold = match side {
            OutcomeSide::Primary => ledger.primary_sold_size + filled_size,
            OutcomeSide::Complement => ledger.complement_sold_size + filled_size,
        };
        wait_for_recorded_fills(
            job_id,
            definition.bot,
            adapter,
            account,
            order_sides,
            ledger,
            side,
            expected_sold,
            deadline,
        )
        .await?;
        let (expected_primary, expected_complement) = match side {
            OutcomeSide::Primary => (primary_total - filled_size, complement_total),
            OutcomeSide::Complement => (primary_total, complement_total - filled_size),
        };
        wait_for_holding_reduction(
            adapter,
            account,
            pair,
            expected_primary,
            expected_complement,
            deadline,
        )
        .await?;
        if Instant::now() >= deadline {
            bail!("timed out flattening outcome market-maker residual inventory");
        }
    }
    Ok(())
}

async fn wait_for_holding_reduction(
    adapter: &ExecutionAdapter,
    account: &str,
    pair: &OutcomePair,
    expected_primary: f64,
    expected_complement: f64,
    deadline: Instant,
) -> Result<()> {
    loop {
        let snapshot = adapter.account_snapshot(account).await?;
        if holding_total(&snapshot, &pair.primary.symbol) <= expected_primary + EPSILON
            && holding_total(&snapshot, &pair.complement.symbol) <= expected_complement + EPSILON
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for outcome cleanup holdings to settle");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_recorded_fills(
    job_id: &str,
    bot: &str,
    adapter: &ExecutionAdapter,
    account: &str,
    order_sides: &HashMap<String, OutcomeSide>,
    ledger: &mut OutcomeLedger,
    side: OutcomeSide,
    expected_sold: f64,
    deadline: Instant,
) -> Result<()> {
    loop {
        recover_known_fills(job_id, bot, adapter, account, order_sides, ledger).await?;
        let sold = match side {
            OutcomeSide::Primary => ledger.primary_sold_size,
            OutcomeSide::Complement => ledger.complement_sold_size,
        };
        if sold + EPSILON >= expected_sold {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out reconciling outcome cleanup fills");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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

    fn outcome_instrument(side: u8, side_name: &str) -> OutcomeInstrument {
        OutcomeInstrument {
            exchange: "hyperliquid-outcomes".to_string(),
            network: "testnet".to_string(),
            symbol: format!("10225:{side}"),
            question_id: Some(1),
            question_name: Some("Will the reference value change?".to_string()),
            question_description: None,
            outcome_id: 10225,
            outcome_name: "Change market".to_string(),
            outcome_description: "Binary change outcome".to_string(),
            side,
            side_name: side_name.to_string(),
            quote_token: "USDC".to_string(),
            coin: format!("#{side}"),
            token_name: side_name.to_string(),
            asset_id: 100_000 + u32::from(side),
            settled: false,
            metadata_fingerprint: format!("fingerprint-{side}"),
        }
    }

    fn grid_quote(side: OrderSide) -> GridQuote {
        GridQuote {
            level: 1,
            side,
            price: if side == OrderSide::Buy { 0.4 } else { 0.6 },
            paired_price: if side == OrderSide::Buy { 0.6 } else { 0.4 },
            size: 10.0,
        }
    }

    fn fill(order_id: &str, amount: f64, price: f64, fee: f64) -> Fill {
        Fill {
            venue: ExecutionVenue::HyperliquidOutcomes,
            internal_symbol: String::new(),
            venue_symbol: String::new(),
            registry_supported: true,
            side: OrderSide::Sell,
            amount,
            price,
            reason: "maker".to_string(),
            order_id: Some(order_id.to_string()),
            maker: true,
            fee: Some(fee),
            fee_asset: Some("USDC".to_string()),
            slot: 1,
            ts_ms: 1,
        }
    }

    #[test]
    fn resolves_binary_pair_by_side_index_and_honors_selected_side() {
        let candidates = [
            outcome_instrument(0, "Change"),
            outcome_instrument(1, "No Change"),
        ];

        let side_zero = pair_from_candidates(&candidates, 10225, 0).expect("side 0 pair");
        assert_eq!(side_zero.primary.side_name, "Change");
        assert_eq!(side_zero.complement.side_name, "No Change");

        let side_one = pair_from_candidates(&candidates, 10225, 1).expect("side 1 pair");
        assert_eq!(side_one.primary.side_name, "No Change");
        assert_eq!(side_one.complement.side_name, "Change");
    }

    #[test]
    fn outcome_quotes_are_rounded_outward_to_hyperliquid_price_rules() {
        let quotes = executable_quote_prices(0.54, 0.55, 2.0, 0.00000001, 8)
            .expect("executable outcome quotes");

        assert_eq!(quotes.primary_sell_price, 0.54506);
        assert_eq!(quotes.complement_sell_price, 0.45506);
        assert_eq!(quotes.normalized_ask, 0.54506);
        assert_eq!(quotes.normalized_bid, 0.54494);
        validate_price_for(quotes.primary_sell_price, 0, 8).expect("valid primary price");
        validate_price_for(quotes.complement_sell_price, 0, 8).expect("valid complement price");
        assert!(gross_profit_per_pair(quotes) > 0.0);
    }

    #[test]
    fn outcome_grid_venue_prices_are_hyperliquid_executable() {
        let candidates = [
            outcome_instrument(0, "Change"),
            outcome_instrument(1, "No Change"),
        ];
        let pair = pair_from_candidates(&candidates, 10225, 0).expect("outcome pair");

        let (_, primary_price) =
            venue_quote(&pair, OrderSide::Sell, 0.54755475).expect("primary quote");
        let (_, complement_price) =
            venue_quote(&pair, OrderSide::Buy, 0.54744525).expect("complement quote");

        assert_eq!(primary_price, 0.54756);
        assert_eq!(complement_price, 0.45256);
        validate_price_for(primary_price, 0, 8).expect("valid primary price");
        validate_price_for(complement_price, 0, 8).expect("valid complement price");
    }

    #[test]
    fn ledger_values_unsold_yes_and_no_as_complements() {
        let mut ledger = OutcomeLedger::new(100.0);
        ledger.record_split(100.0, 0.7);
        let performance = ledger.performance(0.7);

        assert_eq!(performance.gross_realized_pnl, 0.0);
        assert_eq!(performance.trading_pnl, Some(0.0));
        let outcome = performance.outcome.expect("outcome metrics");
        assert_eq!(outcome.primary_inventory, 100.0);
        assert_eq!(outcome.complement_inventory, 100.0);
    }

    #[test]
    fn ledger_counts_paired_sales_and_balanced_merge_once() {
        let mut ledger = OutcomeLedger::new(10.0);
        ledger.record_split(10.0, 0.4);
        assert!(ledger.record_fill(OutcomeSide::Primary, &fill("yes", 4.0, 0.42, -0.04)));
        assert!(ledger.record_fill(OutcomeSide::Complement, &fill("no", 4.0, 0.62, -0.04)));
        assert!(!ledger.record_fill(OutcomeSide::Complement, &fill("no", 4.0, 0.62, -0.04)));
        ledger.record_merge(6.0);

        let performance = ledger.performance(0.4);
        assert!((performance.gross_realized_pnl - 0.16).abs() < 1e-12);
        assert!((performance.trading_pnl.expect("complete fees") - 0.08).abs() < 1e-12);
        let outcome = performance.outcome.expect("outcome metrics");
        assert_eq!(outcome.primary_inventory, 0.0);
        assert_eq!(outcome.complement_inventory, 0.0);
        assert_eq!(outcome.merged_size, 6.0);
    }

    #[test]
    fn ledger_separates_realized_sales_from_unmatched_inventory_risk() {
        let mut ledger = OutcomeLedger::new(10.0);
        ledger.record_split(10.0, 0.4);
        assert!(ledger.record_fill(OutcomeSide::Primary, &fill("yes", 10.0, 0.42, -0.1)));

        let performance = ledger.performance(0.5);
        assert!((performance.gross_realized_pnl - 0.2).abs() < 1e-12);
        assert!((performance.unrealized_pnl + 1.0).abs() < 1e-12);
        assert!((performance.trading_pnl.expect("complete fees") + 0.9).abs() < 1e-12);
    }

    #[test]
    fn outcome_grid_cells_flip_each_completed_leg_independently() {
        let mut buy = OutcomeGridSlot::new(grid_quote(OrderSide::Buy));
        assert_eq!(buy.current_side, OrderSide::Buy);
        assert!(!buy.advance());
        assert_eq!(buy.current_side, OrderSide::Sell);
        assert_eq!(buy.current_price, 0.6);
        assert!(buy.advance());
        assert_eq!(buy.current_side, OrderSide::Buy);
        assert_eq!(buy.current_price, 0.4);

        let mut sell = OutcomeGridSlot::new(grid_quote(OrderSide::Sell));
        assert!(!sell.advance());
        assert_eq!(sell.current_side, OrderSide::Buy);
        assert_eq!(sell.current_price, 0.4);
        assert!(sell.advance());
        assert_eq!(sell.current_side, OrderSide::Sell);
        assert_eq!(sell.current_price, 0.6);
    }

    #[test]
    fn outcome_grid_keeps_half_of_each_split_side_available_for_pairing() {
        let pair_size = 100.0;
        let quotes = outcome_grid_quotes(0.5, 5, 20.0, pair_size).expect("valid outcome grid");
        let initial_yes: f64 = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Sell)
            .map(|quote| quote.size)
            .sum();
        let initial_no: f64 = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Buy)
            .map(|quote| quote.size)
            .sum();

        assert!((initial_yes - pair_size / 2.0).abs() < EPSILON);
        assert!((initial_no - pair_size / 2.0).abs() < EPSILON);
    }
}
