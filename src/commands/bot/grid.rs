use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinSet;

use crate::bots::grid::{GridQuote, GridSpec, quote_grid};
use crate::bots::jobs::{BotJob, BotJobDefinition, BotJobSubmission, GridJobDefinition};
use crate::cli::{
    ExecutionVenueArg, OutputFormat, RunGridArgs, TradeArgs, TradeOrderKind, TradeTimeInForce,
};
use crate::commands::bot::mid_price::{
    AccountFeedEvent, BotStopped, FillKey, FillLedger, ObservedFill, QuoteSide,
    account_symbol_is_flat, append_fill, append_market_data, append_stop_loss, cancel_plan,
    confirm_live_execution, correlate_cleanup_fill_order_id, current_mark, execution_market,
    floor_to_step, inventory_unwind_plan, is_order_gone_error, is_order_gone_message,
    is_post_only_crossing_message, is_terminal_order_status, live_orderbook, quote_plan,
    record_position_reconciled_unwind, render_submission, spawn_account_feed, spawn_book_feed,
    stop_loss_triggered,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::{
    ExecutionReceipt, Fill, OpenOrder, OrderSide, PositionDirection, TradePlan,
};
use crate::providers::bulk::market_data::normalize_timestamp_ms;
use crate::providers::execution::ExecutionAdapter;

const BOT_NAME: &str = "grid";
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GridPlanLevel {
    level: u16,
    side: &'static str,
    price: f64,
    paired_price: f64,
    size: f64,
    exposure: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GridPlanView<'a> {
    r#type: &'static str,
    bot: &'static str,
    venue: String,
    symbol: &'a str,
    max_inventory_size: f64,
    requested_margin: Option<f64>,
    max_inventory_margin: f64,
    max_inventory_exposure: f64,
    reference_price: f64,
    levels_per_side: u16,
    levels: Vec<GridPlanLevel>,
    step_bps: f64,
    stop_loss_pct: Option<f64>,
    duration_secs: u64,
    leverage: f64,
    sizing: &'static str,
    execution: &'static str,
    shutdown: &'static str,
    dry_run: bool,
}

pub async fn handle(args: RunGridArgs) -> Result<()> {
    args.validate()?;
    if args.venue == ExecutionVenueArg::HyperliquidOutcomes {
        return super::outcome::handle_grid(args).await;
    }
    let parent = build_trade_plan(
        &trade_args(&args, args.size, args.margin),
        PositionDirection::Long,
    )
    .await?;
    let market = execution_market(parent.venue, &parent.internal_symbol)?;
    let rules = market.execution_rules()?;
    let book = live_orderbook(parent.venue, &parent.internal_symbol, parent.testnet).await?;
    let best_bid = book
        .bids
        .first()
        .copied()
        .with_context(|| format!("{} book has no bid", parent.venue.label()))?;
    let best_ask = book
        .asks
        .first()
        .copied()
        .with_context(|| format!("{} book has no ask", parent.venue.label()))?;
    let center = (best_bid.price + best_ask.price) / 2.0;
    let raw = quote_grid(GridSpec {
        center_price: center,
        levels_per_side: args.levels,
        step_bps: args.step_bps,
        max_inventory_size: parent.size,
        tick_size: rules.tick_size,
        price_precision: rules.price_precision,
    })?;
    let initial = executable_quotes(
        raw,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    );
    if initial.len() != usize::from(args.levels) * 2 {
        bail!(
            "grid amount is too small to create {} executable levels per side; increase --size/--margin or reduce --levels",
            args.levels
        );
    }

    let definition = GridJobDefinition {
        venue: parent.venue,
        testnet: parent.testnet,
        symbol: parent.internal_symbol.clone(),
        max_inventory_size: parent.size,
        requested_margin: parent.requested_margin,
        max_inventory_margin: parent.estimated_margin,
        max_inventory_exposure: parent.estimated_exposure,
        duration_seconds: args.duration,
        levels_per_side: args.levels,
        step_bps: args.step_bps,
        leverage: Some(args.leverage.unwrap_or(1.0)),
        stop_loss_pct: args.stop_loss_pct.filter(|percent| *percent > 0.0),
        outcome: None,
    };
    definition.validate()?;
    let view = plan_view(&parent, &definition, center, &initial, args.dry_run);

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

    let job = crate::runtime::submit_bot_job(BotJobSubmission {
        definition: BotJobDefinition::Grid(definition),
    })
    .await?;
    render_submission(&job, args.output)
}

pub async fn handle_worker_job(job_id: &str, job: BotJob) -> Result<()> {
    let BotJobDefinition::Grid(definition) = job.definition else {
        bail!("grid worker received a non-grid job");
    };
    let pid = std::process::id();
    crate::runtime::bot_worker_started(job_id, pid).await?;
    let result = if definition.outcome.is_some() {
        super::outcome::run_grid_worker(job_id, &definition).await
    } else {
        run_worker(job_id, &definition).await
    };
    let error = result
        .as_ref()
        .err()
        .and_then(|error| (!error.is::<BotStopped>()).then(|| format!("{error:#}")));
    if let Some(message) = &error {
        let _ = crate::runtime::append_bot_output(
            job_id,
            &serde_json::json!({
                "type": "bot.run.failed",
                "bot": BOT_NAME,
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

fn trade_args(args: &RunGridArgs, size: Option<f64>, margin: Option<f64>) -> TradeArgs {
    TradeArgs {
        symbol: args.symbol.clone(),
        symbol_flag: None,
        config: None,
        venue: args.venue,
        testnet: args.testnet,
        size,
        margin,
        order_kind: TradeOrderKind::Market,
        price: None,
        tif: TradeTimeInForce::Gtc,
        leverage: Some(args.leverage.unwrap_or(1.0)),
        reduce_only: false,
        sl: None,
        tp: None,
        dry_run: false,
        yes: true,
        output: args.output,
    }
}

fn worker_trade_args(definition: &GridJobDefinition) -> TradeArgs {
    TradeArgs {
        symbol: definition.symbol.clone(),
        symbol_flag: None,
        config: None,
        venue: definition.venue,
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
    parent: &'a TradePlan,
    definition: &GridJobDefinition,
    center: f64,
    quotes: &[GridQuote],
    dry_run: bool,
) -> GridPlanView<'a> {
    GridPlanView {
        r#type: "bot.plan",
        bot: BOT_NAME,
        venue: parent.venue.to_string(),
        symbol: &parent.internal_symbol,
        max_inventory_size: definition.max_inventory_size,
        requested_margin: definition.requested_margin,
        max_inventory_margin: definition.max_inventory_margin,
        max_inventory_exposure: definition.max_inventory_exposure,
        reference_price: center,
        levels_per_side: definition.levels_per_side,
        levels: quotes
            .iter()
            .map(|quote| GridPlanLevel {
                level: quote.level,
                side: side_name(quote.side),
                price: quote.price,
                paired_price: quote.paired_price,
                size: quote.size,
                exposure: quote.size * quote.price,
            })
            .collect(),
        step_bps: definition.step_bps,
        stop_loss_pct: definition.stop_loss_pct,
        duration_secs: definition.duration_seconds,
        leverage: definition.leverage.unwrap_or(1.0),
        sizing: "equal, fixed paired grid cells",
        execution: "maker-only post-only ALO paired grid orders",
        shutdown: "cancel owned quotes, then unwind bot-owned inventory",
        dry_run,
    }
}

fn render_plan(plan: &GridPlanView<'_>, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "classic grid{}",
                if plan.dry_run {
                    " (dry run — nothing will be submitted)"
                } else {
                    ""
                }
            );
            println!("  venue:              {}", plan.venue);
            println!("  symbol:             {}", plan.symbol);
            println!("  allocated grid size: {}", plan.max_inventory_size);
            if let Some(margin) = plan.requested_margin {
                println!("  requested margin:   {margin:.8}");
            }
            println!("  allocated margin:   {:.8}", plan.max_inventory_margin);
            println!("  working exposure:   {:.8}", plan.max_inventory_exposure);
            println!("  reference midpoint: {}", plan.reference_price);
            println!("  levels per side:    {}", plan.levels_per_side);
            println!(
                "  grid step:          {} bps between fixed prices",
                plan.step_bps
            );
            println!("  recentering:        disabled");
            for level in &plan.levels {
                println!(
                    "  {:<4} {:>2}:          {} -> {} size={} exposure={:.8}",
                    level.side,
                    level.level,
                    level.price,
                    level.paired_price,
                    level.size,
                    level.exposure
                );
            }
            if let Some(percent) = plan.stop_loss_pct {
                println!("  stop loss:          {percent}% of allocated margin");
            }
            println!("  cycle:              each fill flips one grid step to the opposite side");
            println!("  take profit:        uncapped");
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

fn executable_quotes(
    quotes: Vec<GridQuote>,
    lot_size: f64,
    size_precision: u8,
    min_notional: f64,
) -> Vec<GridQuote> {
    quotes
        .into_iter()
        .filter_map(|mut quote| {
            quote.size = floor_to_step(quote.size, lot_size, size_precision);
            (quote.size >= lot_size / 2.0 && quote.size * quote.price >= min_notional)
                .then_some(quote)
        })
        .collect()
}

fn side_name(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct GridKey {
    /// The side this cell starts on. It identifies the cell after the working
    /// order flips to its paired side.
    lane: QuoteSide,
    level: u16,
}

impl GridKey {
    fn from_quote(quote: GridQuote) -> Self {
        Self {
            lane: match quote.side {
                OrderSide::Buy => QuoteSide::Buy,
                OrderSide::Sell => QuoteSide::Sell,
            },
            level: quote.level,
        }
    }
}

#[derive(Clone, Debug)]
struct WorkingQuote {
    order_id: String,
    side: QuoteSide,
    price: f64,
    remaining_size: f64,
    cancel_requested: bool,
}

struct QuoteSlot {
    side: QuoteSide,
    buy_price: f64,
    sell_price: f64,
    cycle_size: f64,
    cycle_remaining: f64,
    live: Option<WorkingQuote>,
    pending_size: Option<f64>,
    busy: bool,
    retry_after_book_revision: Option<u64>,
}

impl QuoteSlot {
    fn from_quote(quote: GridQuote) -> Self {
        let (buy_price, sell_price) = match quote.side {
            OrderSide::Buy => (quote.price, quote.paired_price),
            OrderSide::Sell => (quote.paired_price, quote.price),
        };
        Self {
            side: match quote.side {
                OrderSide::Buy => QuoteSide::Buy,
                OrderSide::Sell => QuoteSide::Sell,
            },
            buy_price,
            sell_price,
            cycle_size: quote.size,
            cycle_remaining: quote.size,
            live: None,
            pending_size: None,
            busy: false,
            retry_after_book_revision: None,
        }
    }

    fn desired(&self) -> DesiredQuote {
        DesiredQuote {
            side: self.side,
            price: match self.side {
                QuoteSide::Buy => self.buy_price,
                QuoteSide::Sell => self.sell_price,
            },
            size: self.cycle_remaining,
        }
    }

    fn flip(&mut self) {
        self.side = match self.side {
            QuoteSide::Buy => QuoteSide::Sell,
            QuoteSide::Sell => QuoteSide::Buy,
        };
        self.cycle_remaining = self.cycle_size;
        self.live = None;
        self.retry_after_book_revision = None;
    }

    fn accepts_book_revision(&mut self, revision: u64) -> bool {
        match self.retry_after_book_revision {
            Some(rejected) if revision > rejected => {
                self.retry_after_book_revision = None;
                true
            }
            Some(_) => false,
            None => true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct DesiredQuote {
    side: QuoteSide,
    price: f64,
    size: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct MakerBook {
    best_bid: f64,
    best_ask: f64,
}

struct QuoteReconcileState<'a> {
    parent: &'a TradePlan,
    slots: &'a mut HashMap<GridKey, QuoteSlot>,
    actions: &'a mut JoinSet<ActionBatch>,
    order_sequence: &'a mut u64,
    cancel_sequence: &'a mut u64,
}

enum ActionKind {
    SubmitQuote {
        key: GridKey,
        side: QuoteSide,
        price: f64,
        size: f64,
        book_revision: u64,
    },
    CancelQuote {
        key: GridKey,
        side: QuoteSide,
        order_id: String,
    },
}

struct ActionCompletion {
    kind: ActionKind,
    result: std::result::Result<ExecutionReceipt, String>,
}

type ActionBatch = Vec<ActionCompletion>;

#[derive(Clone, Debug)]
enum OrderRole {
    Quote { key: GridKey, side: QuoteSide },
    Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct GridFlip {
    key: GridKey,
    from: QuoteSide,
    to: QuoteSide,
    price: f64,
    size: f64,
}

async fn run_worker(job_id: &str, definition: &GridJobDefinition) -> Result<()> {
    let parent = build_trade_plan(&worker_trade_args(definition), PositionDirection::Long).await?;
    let market = execution_market(definition.venue, &definition.symbol)?;
    let rules = market.execution_rules()?;
    let adapter = ExecutionAdapter::new(definition.venue, definition.testnet).await?;
    let initial_book =
        live_orderbook(definition.venue, &definition.symbol, definition.testnet).await?;
    let initial_bid = initial_book
        .bids
        .first()
        .copied()
        .with_context(|| format!("{} book has no bid", definition.venue.label()))?;
    let initial_ask = initial_book
        .asks
        .first()
        .copied()
        .with_context(|| format!("{} book has no ask", definition.venue.label()))?;
    let anchor_mid = (initial_bid.price + initial_ask.price) / 2.0;
    let initial_quotes = executable_quotes(
        quote_grid(GridSpec {
            center_price: anchor_mid,
            levels_per_side: definition.levels_per_side,
            step_bps: definition.step_bps,
            max_inventory_size: definition.max_inventory_size,
            tick_size: rules.tick_size,
            price_precision: rules.price_precision,
        })?,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    );
    crate::runtime::append_bot_output(
        job_id,
        &plan_view(&parent, definition, anchor_mid, &initial_quotes, false),
    )?;

    let keys = initial_quotes
        .iter()
        .copied()
        .map(GridKey::from_quote)
        .collect::<Vec<_>>();
    let mut slots = initial_quotes
        .iter()
        .copied()
        .map(|quote| (GridKey::from_quote(quote), QuoteSlot::from_quote(quote)))
        .collect::<HashMap<_, _>>();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(definition.duration_seconds);
    let mut book = spawn_book_feed(
        definition.venue,
        definition.testnet,
        definition.symbol.clone(),
    );
    let mut account_events = spawn_account_feed(
        definition.venue,
        definition.testnet,
        parent.account.clone(),
        parent.internal_symbol.clone(),
    );
    let mut account_connected = false;
    let allocated_margin = definition
        .requested_margin
        .unwrap_or(definition.max_inventory_margin);
    let mut ledger = FillLedger::with_allocated_margin(allocated_margin);
    let mut order_roles = HashMap::<String, OrderRole>::new();
    let mut pending_fills = HashMap::<String, Vec<ObservedFill>>::new();
    let mut terminal_statuses = HashMap::<String, String>::new();
    let mut actions = JoinSet::<ActionBatch>::new();
    let mut order_sequence = 0_u64;
    let mut cancel_sequence = 0_u64;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install bot worker termination handler")?;

    let outcome: Result<&'static str> = async {
        let outcome = loop {
            tokio::select! {
                changed = book.changed() => {
                    if changed.is_err() {
                        bail!("grid order-book task stopped");
                    }
                    let state = book.borrow().clone();
                    if let Some(error) = state.error {
                        append_market_data(
                            job_id,
                            BOT_NAME,
                            "orderbook",
                            "disconnected",
                            Some(&error),
                        )?;
                    }
                }
                event = account_events.recv() => {
                    match event.context("grid account-event task stopped")? {
                        AccountFeedEvent::Connected => {
                            account_connected = true;
                            append_market_data(job_id, BOT_NAME, "account", "connected", None)?;
                        }
                        AccountFeedEvent::Disconnected(error) => {
                            account_connected = false;
                            append_market_data(
                                job_id,
                                BOT_NAME,
                                "account",
                                "disconnected",
                                Some(&error),
                            )?;
                        }
                        AccountFeedEvent::Recovery { open_orders, fills } => {
                            reconcile_recovery(
                                job_id,
                                current_mark(&book, parent.reference_price),
                                &open_orders,
                                fills,
                                &order_roles,
                                &mut slots,
                                &mut ledger,
                            )?;
                        }
                        AccountFeedEvent::Data(value) => {
                            let accepts_pending = slots
                                .values()
                                .any(|slot| slot.busy && slot.live.is_none());
                            let mut account_state = AccountEventState {
                                order_roles: &order_roles,
                                pending_fills: &mut pending_fills,
                                terminal_statuses: &mut terminal_statuses,
                                slots: &mut slots,
                                ledger: &mut ledger,
                            };
                            apply_account_event(
                                job_id,
                                current_mark(&book, parent.reference_price),
                                value,
                                accepts_pending,
                                &mut account_state,
                            )?;
                        }
                    }
                }
                completion = actions.join_next(), if !actions.is_empty() => {
                    let completions = completion
                        .context("grid action set ended unexpectedly")?
                        .context("grid action task panicked")?;
                    for completion in completions {
                        apply_action_completion(
                            job_id,
                            current_mark(&book, parent.reference_price),
                            completion,
                            &mut slots,
                            &mut order_roles,
                            &mut pending_fills,
                            &mut terminal_statuses,
                            &mut ledger,
                        )?;
                    }
                }
                _ = heartbeat.tick() => {
                    let performance = ledger.performance(current_mark(&book, parent.reference_price));
                    crate::runtime::bot_worker_heartbeat(
                        job_id,
                        std::process::id(),
                        Some(&performance),
                    ).await?;
                }
                _ = &mut deadline_sleep => break "duration_elapsed",
                _ = terminate.recv() => break "stopped",
                _ = tokio::signal::ctrl_c() => break "stopped",
            }

            let mark_price = current_mark(&book, parent.reference_price);
            let performance = ledger.performance(mark_price);
            if let Some(percent) = definition.stop_loss_pct.filter(|percent| *percent > 0.0) {
                let max_loss = allocated_margin * percent / 100.0;
                if stop_loss_triggered(&performance, max_loss) {
                    crate::runtime::bot_worker_heartbeat(
                        job_id,
                        std::process::id(),
                        Some(&performance),
                    )
                    .await?;
                    append_stop_loss(job_id, BOT_NAME, percent, max_loss, mark_price, &performance)?;
                    break "stop_loss";
                }
            }

            let state = book.borrow().clone();
            let book_revision = state.revision;
            let maker_book = state.top.and_then(|top| {
                Some(MakerBook {
                    best_bid: top.best_bid?.price,
                    best_ask: top.best_ask?.price,
                })
            });
            let desired = if account_connected {
                desired_quotes(&slots, rules.lot_size, rules.min_notional)
            } else {
                HashMap::new()
            };
            reconcile_quotes(
                job_id,
                &keys,
                &desired,
                book_revision,
                maker_book,
                QuoteReconcileState {
                    parent: &parent,
                    slots: &mut slots,
                    actions: &mut actions,
                    order_sequence: &mut order_sequence,
                    cancel_sequence: &mut cancel_sequence,
                },
            )?;
        };
        Ok(outcome)
    }
    .await;

    let mut action_error = None;
    while let Some(completion) = actions.join_next().await {
        match completion {
            Ok(completions) => {
                for completion in completions {
                    if let Err(error) = apply_action_completion(
                        job_id,
                        current_mark(&book, parent.reference_price),
                        completion,
                        &mut slots,
                        &mut order_roles,
                        &mut pending_fills,
                        &mut terminal_statuses,
                        &mut ledger,
                    ) {
                        action_error.get_or_insert(error);
                    }
                }
            }
            Err(error) => {
                action_error.get_or_insert_with(|| anyhow::anyhow!("grid action failed: {error}"));
            }
        }
    }
    let outcome = match (outcome, action_error) {
        (Ok(outcome), None) => Ok(outcome),
        (Err(error), None) | (_, Some(error)) => Err(error),
    };
    let cleanup_result = cleanup(
        job_id,
        current_mark(&book, parent.reference_price),
        definition,
        &parent,
        &adapter,
        &mut slots,
        &mut ledger,
        &mut order_roles,
        &mut order_sequence,
        &mut cancel_sequence,
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
            return Err(error).context(format!("grid cleanup also failed: {cleanup:#}"));
        }
    };
    performance_update?;
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.run.finished",
            "bot": BOT_NAME,
            "jobId": job_id,
            "status": outcome,
            "boughtSize": performance.bought_size,
            "soldSize": performance.sold_size,
            "residualSize": performance.inventory_size,
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

fn desired_quotes(
    slots: &HashMap<GridKey, QuoteSlot>,
    lot_size: f64,
    min_notional: f64,
) -> HashMap<GridKey, DesiredQuote> {
    slots
        .iter()
        .filter_map(|(key, slot)| {
            let desired = slot.desired();
            (desired.size >= lot_size / 2.0 && desired.size * desired.price >= min_notional)
                .then_some((*key, desired))
        })
        .collect()
}

fn should_replace_quote(live: &WorkingQuote, desired: Option<DesiredQuote>) -> bool {
    let Some(desired) = desired else {
        return true;
    };
    desired.side != live.side || (desired.price - live.price).abs() > f64::EPSILON
}

fn maker_safe(quote: DesiredQuote, book: MakerBook) -> bool {
    match quote.side {
        QuoteSide::Buy => quote.price < book.best_ask,
        QuoteSide::Sell => quote.price > book.best_bid,
    }
}

fn reconcile_quotes(
    job_id: &str,
    keys: &[GridKey],
    desired: &HashMap<GridKey, DesiredQuote>,
    book_revision: u64,
    maker_book: Option<MakerBook>,
    state: QuoteReconcileState<'_>,
) -> Result<()> {
    if !state.actions.is_empty() {
        return Ok(());
    }

    let mut cancel_items = Vec::new();
    let mut cancel_kinds = Vec::new();
    for key in keys {
        let slot = state
            .slots
            .get_mut(key)
            .context("grid quote slot disappeared")?;
        if slot.busy || !slot.accepts_book_revision(book_revision) {
            continue;
        }
        if let Some(live) = slot.live.as_mut() {
            let replace = should_replace_quote(live, desired.get(key).copied());
            if !replace || live.cancel_requested {
                continue;
            }
            live.cancel_requested = true;
            slot.busy = true;
            *state.cancel_sequence = state.cancel_sequence.saturating_add(1);
            let order_id = live.order_id.clone();
            let plan = cancel_plan(state.parent, order_id.clone())?;
            cancel_items.push((*state.cancel_sequence, plan));
            cancel_kinds.push(ActionKind::CancelQuote {
                key: *key,
                side: live.side,
                order_id,
            });
        }
    }
    if !cancel_items.is_empty() {
        let job_id = job_id.to_string();
        state.actions.spawn(async move {
            let started = Instant::now();
            let completions = match crate::runtime::submit_bot_cancels(&job_id, &cancel_items).await
            {
                Ok(outcomes) if outcomes.len() == cancel_kinds.len() => cancel_kinds
                    .into_iter()
                    .zip(outcomes)
                    .map(|(kind, outcome)| ActionCompletion {
                        kind,
                        result: outcome.into_result(),
                    })
                    .collect(),
                Ok(outcomes) => batch_action_failures(
                    cancel_kinds,
                    format!(
                        "mlabd returned {} cancellation outcomes for {} grid quotes",
                        outcomes.len(),
                        cancel_items.len()
                    ),
                ),
                Err(error) => batch_action_failures(cancel_kinds, format!("{error:#}")),
            };
            if let Err(error) =
                append_grid_batch(&job_id, "cancel", started.elapsed(), &completions)
            {
                eprintln!("grid batch telemetry warning: {error:#}");
            }
            completions
        });
        return Ok(());
    }

    let mut submit_items = Vec::new();
    let mut submit_kinds = Vec::new();
    for key in keys {
        let Some(DesiredQuote { side, price, size }) = desired.get(key).copied() else {
            continue;
        };
        let Some(book) = maker_book else {
            continue;
        };
        if !maker_safe(DesiredQuote { side, price, size }, book) {
            continue;
        }
        let slot = state
            .slots
            .get_mut(key)
            .context("grid quote slot disappeared")?;
        if slot.busy || slot.live.is_some() || !slot.accepts_book_revision(book_revision) {
            continue;
        }
        slot.busy = true;
        slot.pending_size = Some(size);
        *state.order_sequence = state.order_sequence.saturating_add(1);
        submit_items.push((
            *state.order_sequence,
            quote_plan(state.parent, side, size, price)?,
        ));
        submit_kinds.push(ActionKind::SubmitQuote {
            key: *key,
            side,
            price,
            size,
            book_revision,
        });
    }
    if !submit_items.is_empty() {
        let job_id = job_id.to_string();
        state.actions.spawn(async move {
            let started = Instant::now();
            let completions = match crate::runtime::submit_bot_trades(&job_id, &submit_items).await
            {
                Ok(outcomes) if outcomes.len() == submit_kinds.len() => submit_kinds
                    .into_iter()
                    .zip(outcomes)
                    .map(|(kind, outcome)| ActionCompletion {
                        kind,
                        result: outcome.into_result(),
                    })
                    .collect(),
                Ok(outcomes) => batch_action_failures(
                    submit_kinds,
                    format!(
                        "mlabd returned {} execution outcomes for {} grid quotes",
                        outcomes.len(),
                        submit_items.len()
                    ),
                ),
                Err(error) => batch_action_failures(submit_kinds, format!("{error:#}")),
            };
            if let Err(error) = append_grid_batch(&job_id, "place", started.elapsed(), &completions)
            {
                eprintln!("grid batch telemetry warning: {error:#}");
            }
            completions
        });
    }
    Ok(())
}

fn batch_action_failures(kinds: Vec<ActionKind>, error: String) -> Vec<ActionCompletion> {
    kinds
        .into_iter()
        .map(|kind| ActionCompletion {
            kind,
            result: Err(error.clone()),
        })
        .collect()
}

fn append_grid_batch(
    job_id: &str,
    operation: &str,
    elapsed: Duration,
    completions: &[ActionCompletion],
) -> Result<()> {
    let succeeded = completions
        .iter()
        .filter(|completion| completion.result.is_ok())
        .count();
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.grid.batch",
            "bot": BOT_NAME,
            "jobId": job_id,
            "operation": operation,
            "orders": completions.len(),
            "succeeded": succeeded,
            "failed": completions.len() - succeeded,
            "latencyMs": elapsed.as_secs_f64() * 1_000.0,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_action_completion(
    job_id: &str,
    mark_price: f64,
    completion: ActionCompletion,
    slots: &mut HashMap<GridKey, QuoteSlot>,
    order_roles: &mut HashMap<String, OrderRole>,
    pending_fills: &mut HashMap<String, Vec<ObservedFill>>,
    terminal_statuses: &mut HashMap<String, String>,
    ledger: &mut FillLedger,
) -> Result<()> {
    match completion.kind {
        ActionKind::SubmitQuote {
            key,
            side,
            price,
            size,
            book_revision,
        } => {
            {
                let slot = slots.get_mut(&key).context("grid quote slot disappeared")?;
                slot.busy = false;
                slot.pending_size = None;
            }
            match completion.result {
                Ok(receipt) => {
                    let order_id = receipt
                        .order_id
                        .context("grid quote omitted its order id")?;
                    let role = OrderRole::Quote { key, side };
                    order_roles.insert(order_id.clone(), role.clone());
                    if let Some(fills) = pending_fills.remove(&order_id) {
                        for fill in fills {
                            record_fill(
                                job_id, mark_price, ledger, &order_id, &role, &fill, slots,
                            )?;
                        }
                    }
                    let remaining_size = slots.get(&key).map_or(0.0, |slot| {
                        if slot.side == side {
                            slot.cycle_remaining
                        } else {
                            0.0
                        }
                    });
                    let terminal_status = terminal_statuses.remove(&order_id);
                    let terminal = receipt.terminal || terminal_status.is_some();
                    let awaiting_fill = terminal_status
                        .as_deref()
                        .unwrap_or(&receipt.status)
                        .eq_ignore_ascii_case("filled")
                        && remaining_size > f64::EPSILON;
                    if (!terminal || awaiting_fill) && remaining_size > f64::EPSILON {
                        slots.get_mut(&key).expect("grid slot exists").live = Some(WorkingQuote {
                            order_id: order_id.clone(),
                            side,
                            price,
                            remaining_size,
                            cancel_requested: awaiting_fill,
                        });
                    }
                    append_grid_quote(
                        job_id,
                        key,
                        side,
                        terminal_status.as_deref().unwrap_or(&receipt.status),
                        &order_id,
                        price,
                        size,
                    )?;
                }
                Err(error) => {
                    let crossing = is_post_only_crossing_message(&error);
                    append_grid_quote(
                        job_id,
                        key,
                        side,
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
                        slots
                            .get_mut(&key)
                            .expect("grid slot exists")
                            .retry_after_book_revision = Some(book_revision);
                    } else {
                        bail!(
                            "{} level {} quote submission failed: {error}",
                            side.name(),
                            key.level
                        );
                    }
                }
            }
        }
        ActionKind::CancelQuote {
            key,
            side,
            order_id,
        } => {
            let slot = slots.get_mut(&key).context("grid quote slot disappeared")?;
            slot.busy = false;
            match completion.result {
                Ok(receipt) => {
                    if receipt.terminal
                        && slot
                            .live
                            .as_ref()
                            .is_some_and(|quote| quote.order_id == order_id)
                    {
                        slot.live = None;
                    }
                }
                Err(error) if is_order_gone_message(&error) => {
                    if slot
                        .live
                        .as_ref()
                        .is_some_and(|quote| quote.order_id == order_id)
                    {
                        slot.live = None;
                    }
                }
                Err(error) => {
                    if let Some(live) = slot.live.as_mut()
                        && live.order_id == order_id
                    {
                        live.cancel_requested = false;
                    }
                    bail!(
                        "{} level {} quote cancellation failed: {error}",
                        side.name(),
                        key.level
                    );
                }
            }
        }
    }
    Ok(())
}

struct AccountEventState<'a> {
    order_roles: &'a HashMap<String, OrderRole>,
    pending_fills: &'a mut HashMap<String, Vec<ObservedFill>>,
    terminal_statuses: &'a mut HashMap<String, String>,
    slots: &'a mut HashMap<GridKey, QuoteSlot>,
    ledger: &'a mut FillLedger,
}

fn apply_account_event(
    job_id: &str,
    mark_price: f64,
    value: Value,
    accepts_pending: bool,
    state: &mut AccountEventState<'_>,
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
            if let Some(role) = state.order_roles.get(order_id) {
                record_fill(
                    job_id,
                    mark_price,
                    state.ledger,
                    order_id,
                    role,
                    &fill,
                    state.slots,
                )?;
            } else if accepts_pending {
                state
                    .pending_fills
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
            if is_terminal_order_status(status) {
                if let Some(OrderRole::Quote { key, side }) = state.order_roles.get(order_id) {
                    let size = state
                        .slots
                        .get(key)
                        .and_then(|slot| slot.live.as_ref())
                        .map_or(0.0, |quote| quote.remaining_size);
                    if let Some(slot) = state.slots.get_mut(key)
                        && slot
                            .live
                            .as_ref()
                            .is_some_and(|quote| quote.order_id == order_id)
                    {
                        if status.eq_ignore_ascii_case("filled") {
                            // Account streams may deliver the terminal order update before
                            // its fill. Retain the slot until the fill updates inventory.
                            slot.live
                                .as_mut()
                                .expect("matching grid quote exists")
                                .cancel_requested = true;
                        } else {
                            slot.live = None;
                        }
                    }
                    append_grid_quote(
                        job_id,
                        *key,
                        *side,
                        status,
                        order_id,
                        value.get("px").and_then(Value::as_f64).unwrap_or(0.0),
                        value
                            .get("origSz")
                            .and_then(Value::as_f64)
                            .filter(|value| *value > 0.0)
                            .unwrap_or(size)
                            .abs(),
                    )?;
                } else if accepts_pending {
                    state
                        .terminal_statuses
                        .insert(order_id.to_string(), status.to_string());
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn record_fill(
    job_id: &str,
    mark_price: f64,
    ledger: &mut FillLedger,
    order_id: &str,
    role: &OrderRole,
    fill: &ObservedFill,
    slots: &mut HashMap<GridKey, QuoteSlot>,
) -> Result<bool> {
    if !ledger.record_live(order_id, fill) {
        return Ok(false);
    }
    append_fill(job_id, BOT_NAME, mark_price, ledger, order_id, fill)?;
    if let Some(flip) = apply_fill_role(order_id, role, fill, slots) {
        append_grid_flip(job_id, flip)?;
    }
    Ok(true)
}

fn apply_fill_role(
    order_id: &str,
    role: &OrderRole,
    fill: &ObservedFill,
    slots: &mut HashMap<GridKey, QuoteSlot>,
) -> Option<GridFlip> {
    match role {
        OrderRole::Quote { key, side } => {
            let slot = slots.get_mut(key)?;
            if let Some(live) = slot.live.as_mut()
                && live.order_id == order_id
            {
                live.remaining_size = (live.remaining_size - fill.size).max(0.0);
                if live.remaining_size <= 1e-12 {
                    slot.live = None;
                }
            }
            if slot.side != *side {
                return None;
            }
            slot.cycle_remaining = (slot.cycle_remaining - fill.size).max(0.0);
            if slot.cycle_remaining > 1e-12 {
                return None;
            }
            let flip = GridFlip {
                key: *key,
                from: *side,
                to: match side {
                    QuoteSide::Buy => QuoteSide::Sell,
                    QuoteSide::Sell => QuoteSide::Buy,
                },
                price: match side {
                    QuoteSide::Buy => slot.sell_price,
                    QuoteSide::Sell => slot.buy_price,
                },
                size: slot.cycle_size,
            };
            slot.flip();
            Some(flip)
        }
        OrderRole::Cleanup => None,
    }
}

fn reconcile_recovery(
    job_id: &str,
    mark_price: f64,
    open_orders: &[OpenOrder],
    fills: Vec<Fill>,
    order_roles: &HashMap<String, OrderRole>,
    slots: &mut HashMap<GridKey, QuoteSlot>,
    ledger: &mut FillLedger,
) -> Result<()> {
    let open_ids = open_orders
        .iter()
        .map(|order| order.order_id.as_str())
        .collect::<HashSet<_>>();
    for slot in slots.values_mut() {
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
        let Some(role) = order_roles.get(order_id) else {
            continue;
        };
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
            append_fill(job_id, BOT_NAME, mark_price, ledger, order_id, &observed)?;
            if let Some(flip) = apply_fill_role(order_id, role, &observed, slots) {
                append_grid_flip(job_id, flip)?;
            }
        }
    }
    Ok(())
}

fn append_grid_quote(
    job_id: &str,
    key: GridKey,
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
            "bot": BOT_NAME,
            "jobId": job_id,
            "status": status,
            "side": side.name(),
            "lane": key.lane.name(),
            "level": key.level,
            "orderId": order_id,
            "price": price,
            "size": size,
        }),
    )
}

fn append_grid_flip(job_id: &str, flip: GridFlip) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.grid.flip",
            "bot": BOT_NAME,
            "jobId": job_id,
            "lane": flip.key.lane.name(),
            "level": flip.key.level,
            "fromSide": flip.from.name(),
            "toSide": flip.to.name(),
            "price": flip.price,
            "size": flip.size,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
async fn cleanup(
    job_id: &str,
    mark_price: f64,
    definition: &GridJobDefinition,
    parent: &TradePlan,
    adapter: &ExecutionAdapter,
    slots: &mut HashMap<GridKey, QuoteSlot>,
    ledger: &mut FillLedger,
    order_roles: &mut HashMap<String, OrderRole>,
    order_sequence: &mut u64,
    cancel_sequence: &mut u64,
) -> Result<()> {
    let cleanup_deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let (open_orders, fills) = tokio::join!(
            adapter.open_orders_for_market(&parent.account, &parent.internal_symbol),
            adapter.fills(&parent.account),
        );
        let open_orders = open_orders?;
        reconcile_recovery(
            job_id,
            mark_price,
            &open_orders,
            fills?,
            order_roles,
            slots,
            ledger,
        )?;
        let remaining = open_orders
            .into_iter()
            .filter(|order| order_roles.contains_key(&order.order_id))
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
        let cancel_items = cancellation_plans
            .iter()
            .map(|(_, plan)| {
                *cancel_sequence = cancel_sequence.saturating_add(1);
                (*cancel_sequence, plan.clone())
            })
            .collect::<Vec<_>>();
        let cancellation_results = crate::runtime::submit_bot_cancels(job_id, &cancel_items)
            .await
            .context("failed to submit the grid cancellation batch")?;
        if cancellation_results.len() != cancellation_plans.len() {
            bail!(
                "venue returned {} outcomes for {} grid cleanup cancellations",
                cancellation_results.len(),
                cancellation_plans.len()
            );
        }
        for ((order, _), outcome) in cancellation_plans.into_iter().zip(cancellation_results) {
            let result = outcome.into_result().map_err(anyhow::Error::msg);
            match result {
                Ok(receipt) => {
                    if let Some(OrderRole::Quote { key, side }) = order_roles.get(&order.order_id) {
                        append_grid_quote(
                            job_id,
                            *key,
                            *side,
                            &receipt.status,
                            &order.order_id,
                            order.price,
                            order.remaining_size,
                        )?;
                    }
                }
                Err(error) if is_order_gone_error(&error) => {}
                Err(error) => return Err(error).context("failed to cancel a grid quote"),
            }
        }
        if Instant::now() >= cleanup_deadline {
            bail!("timed out waiting for grid quotes to cancel");
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
    *order_sequence = order_sequence.saturating_add(1);
    let receipt = crate::runtime::submit_bot_trade(job_id, *order_sequence, &plan)
        .await
        .context("failed to unwind grid bot-owned inventory")?;
    let order_id = receipt
        .order_id
        .clone()
        .context("grid inventory unwind omitted its order id")?;
    order_roles.insert(order_id.clone(), OrderRole::Cleanup);

    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let fills = adapter.fills(&parent.account).await?;
        if let Some(provider_order_id) = correlate_cleanup_fill_order_id(&plan, &receipt, &fills) {
            order_roles.insert(provider_order_id, OrderRole::Cleanup);
        }
        reconcile_recovery(job_id, mark_price, &[], fills, order_roles, slots, ledger)?;
        if ledger.inventory().abs() < rules.lot_size / 2.0 {
            break;
        }
        if Instant::now() >= deadline {
            if account_symbol_is_flat(
                adapter,
                &parent.account,
                &parent.internal_symbol,
                rules.lot_size,
            )
            .await?
            {
                record_position_reconciled_unwind(
                    job_id,
                    BOT_NAME,
                    mark_price,
                    &order_id,
                    receipt.average_fill_price,
                    ledger,
                )?;
                break;
            }
            bail!(
                "timed out waiting for grid bot-owned inventory to unwind; remaining={}",
                ledger.inventory()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(lane: QuoteSide) -> GridKey {
        GridKey { lane, level: 1 }
    }

    fn slot(side: QuoteSide) -> QuoteSlot {
        QuoteSlot {
            side,
            buy_price: 99.0,
            sell_price: 100.0,
            cycle_size: 2.0,
            cycle_remaining: 2.0,
            live: Some(WorkingQuote {
                order_id: "order-1".to_string(),
                side,
                price: match side {
                    QuoteSide::Buy => 99.0,
                    QuoteSide::Sell => 100.0,
                },
                remaining_size: 2.0,
                cancel_requested: false,
            }),
            pending_size: None,
            busy: false,
            retry_after_book_revision: None,
        }
    }

    fn fill(buy: bool, size: f64, price: f64) -> ObservedFill {
        ObservedFill {
            timestamp: 1,
            recovered: false,
            buy,
            size,
            price,
            fee: Some(0.0),
        }
    }

    #[test]
    fn partial_fill_keeps_the_same_cycle_and_remaining_quantity() {
        let key = key(QuoteSide::Buy);
        let mut slots = HashMap::from([(key, slot(QuoteSide::Buy))]);

        let flip = apply_fill_role(
            "order-1",
            &OrderRole::Quote {
                key,
                side: QuoteSide::Buy,
            },
            &fill(true, 0.75, 99.0),
            &mut slots,
        );

        assert_eq!(flip, None);
        assert_eq!(slots[&key].side, QuoteSide::Buy);
        assert_eq!(slots[&key].cycle_remaining, 1.25);
        assert_eq!(
            slots[&key]
                .live
                .as_ref()
                .expect("partial order remains live")
                .remaining_size,
            1.25
        );
    }

    #[test]
    fn completed_buy_flips_to_the_paired_sell() {
        let key = key(QuoteSide::Buy);
        let mut slots = HashMap::from([(key, slot(QuoteSide::Buy))]);

        let flip = apply_fill_role(
            "order-1",
            &OrderRole::Quote {
                key,
                side: QuoteSide::Buy,
            },
            &fill(true, 2.0, 99.0),
            &mut slots,
        )
        .expect("completed cycle should flip");

        assert_eq!(flip.from, QuoteSide::Buy);
        assert_eq!(flip.to, QuoteSide::Sell);
        assert_eq!(flip.price, 100.0);
        assert_eq!(slots[&key].side, QuoteSide::Sell);
        assert_eq!(slots[&key].cycle_remaining, 2.0);
        assert!(slots[&key].live.is_none());
    }

    #[test]
    fn completed_sell_flips_to_the_paired_buy() {
        let key = key(QuoteSide::Sell);
        let mut slots = HashMap::from([(key, slot(QuoteSide::Sell))]);

        let flip = apply_fill_role(
            "order-1",
            &OrderRole::Quote {
                key,
                side: QuoteSide::Sell,
            },
            &fill(false, 2.0, 100.0),
            &mut slots,
        )
        .expect("completed cycle should flip");

        assert_eq!(flip.from, QuoteSide::Sell);
        assert_eq!(flip.to, QuoteSide::Buy);
        assert_eq!(flip.price, 99.0);
        assert_eq!(slots[&key].side, QuoteSide::Buy);
    }

    #[test]
    fn canceled_partial_order_is_resubmitted_only_for_its_remainder() {
        let key = key(QuoteSide::Buy);
        let mut partial = slot(QuoteSide::Buy);
        partial.cycle_remaining = 0.6;
        partial.live = None;
        let slots = HashMap::from([(key, partial)]);

        let desired = desired_quotes(&slots, 0.1, 1.0);

        assert_eq!(
            desired[&key],
            DesiredQuote {
                side: QuoteSide::Buy,
                price: 99.0,
                size: 0.6,
            }
        );
    }

    #[test]
    fn fixed_quote_is_not_replaced_for_a_size_only_difference() {
        let live = WorkingQuote {
            order_id: "order-1".to_string(),
            side: QuoteSide::Buy,
            price: 99.0,
            remaining_size: 0.5,
            cancel_requested: false,
        };

        assert!(!should_replace_quote(
            &live,
            Some(DesiredQuote {
                side: QuoteSide::Buy,
                price: 99.0,
                size: 1.0,
            }),
        ));
        assert!(should_replace_quote(
            &live,
            Some(DesiredQuote {
                side: QuoteSide::Sell,
                price: 100.0,
                size: 1.0,
            }),
        ));
    }

    #[test]
    fn crossing_fixed_quote_waits_instead_of_being_submitted() {
        let book = MakerBook {
            best_bid: 100.0,
            best_ask: 101.0,
        };

        assert!(maker_safe(
            DesiredQuote {
                side: QuoteSide::Buy,
                price: 100.0,
                size: 1.0,
            },
            book,
        ));
        assert!(!maker_safe(
            DesiredQuote {
                side: QuoteSide::Buy,
                price: 101.0,
                size: 1.0,
            },
            book,
        ));
        assert!(maker_safe(
            DesiredQuote {
                side: QuoteSide::Sell,
                price: 101.0,
                size: 1.0,
            },
            book,
        ));
        assert!(!maker_safe(
            DesiredQuote {
                side: QuoteSide::Sell,
                price: 100.0,
                size: 1.0,
            },
            book,
        ));
    }
}
