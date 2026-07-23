use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::future::join_all;
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinSet;

use crate::bots::grid::{
    GridQuote, GridSpec, passive_mid_price, quote_grid, should_recenter, soft_reset_triggered,
};
use crate::bots::jobs::{BotJob, BotJobDefinition, BotJobSubmission, GridJobDefinition};
use crate::cli::{
    ExecutionVenueArg, OutputFormat, RunGridArgs, TradeArgs, TradeOrderKind, TradeTimeInForce,
};
use crate::commands::bot::mid_price::{
    AccountFeedEvent, BookFeedState, BotStopped, FillKey, FillLedger, ObservedFill, QuoteSide,
    append_fill, append_market_data, append_stop_loss, cancel_plan, confirm_live_execution,
    current_mark, execution_market, floor_to_step, inventory_unwind_plan, is_order_gone_error,
    is_order_gone_message, is_post_only_crossing_message, is_terminal_order_status, live_orderbook,
    quote_plan, render_submission, spawn_account_feed, spawn_book_feed, stop_loss_triggered,
    venue_key, venue_label,
};
use crate::commands::execution::build_trade_plan;
use crate::domain::execution::{
    ExecutionReceipt, ExecutionVenue, Fill, OpenOrder, OrderSide, PositionDirection, TradePlan,
};
use crate::domain::types::OrderBookLevel;
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
    size: f64,
    exposure: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GridPlanView<'a> {
    r#type: &'static str,
    bot: &'static str,
    venue: &'static str,
    symbol: &'a str,
    max_inventory_size: f64,
    requested_margin: Option<f64>,
    max_inventory_margin: f64,
    max_inventory_exposure: f64,
    reference_price: f64,
    levels_per_side: u16,
    levels: Vec<GridPlanLevel>,
    step_bps: f64,
    automatic_recenter_range_bps: f64,
    reset_threshold_pct: Option<f64>,
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
        .with_context(|| format!("{} book has no bid", venue_label(parent.venue)))?;
    let best_ask = book
        .asks
        .first()
        .copied()
        .with_context(|| format!("{} book has no ask", venue_label(parent.venue)))?;
    let center = (best_bid.price + best_ask.price) / 2.0;
    let raw = quote_grid(GridSpec {
        anchor_bid: best_bid,
        anchor_ask: best_ask,
        best_bid,
        best_ask,
        levels_per_side: args.levels,
        step_bps: args.step_bps,
        max_inventory_size: parent.size,
        inventory_size: 0.0,
        exposure_price: None,
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
        reset_threshold_pct: args.reset_threshold_pct.filter(|percent| *percent > 0.0),
        leverage: args.leverage,
        stop_loss_pct: args.stop_loss_pct.filter(|percent| *percent > 0.0),
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
    let result = run_worker(job_id, &definition).await;
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

fn worker_trade_args(definition: &GridJobDefinition) -> TradeArgs {
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
    parent: &'a TradePlan,
    definition: &GridJobDefinition,
    center: f64,
    quotes: &[GridQuote],
    dry_run: bool,
) -> GridPlanView<'a> {
    GridPlanView {
        r#type: "bot.plan",
        bot: BOT_NAME,
        venue: venue_key(parent.venue),
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
                size: quote.size,
                exposure: quote.size * quote.price,
            })
            .collect(),
        step_bps: definition.step_bps,
        automatic_recenter_range_bps: f64::from(definition.levels_per_side) * definition.step_bps,
        reset_threshold_pct: definition.reset_threshold_pct,
        stop_loss_pct: definition.stop_loss_pct,
        duration_secs: definition.duration_seconds,
        leverage: definition.leverage,
        sizing: "equal levels with automatic inventory skew",
        execution: "maker-only post-only ALO grid quotes",
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
                "grid market maker{}",
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
            println!("  max margin:         {:.8}", plan.max_inventory_margin);
            println!("  total exposure:     {:.8}", plan.max_inventory_exposure);
            println!("  reference midpoint: {}", plan.reference_price);
            println!("  levels per side:    {}", plan.levels_per_side);
            println!("  grid step:          {} bps behind touch", plan.step_bps);
            println!(
                "  flat recentering:   automatic after {} bps full-grid movement",
                plan.automatic_recenter_range_bps
            );
            for level in &plan.levels {
                println!(
                    "  {:<4} {:>2}:          {} size={} exposure={:.8}",
                    level.side, level.level, level.price, level.size, level.exposure
                );
            }
            if let Some(percent) = plan.stop_loss_pct {
                println!("  stop loss:          {percent}% of allocated margin");
            }
            if let Some(percent) = plan.reset_threshold_pct {
                println!("  soft reset:         {percent}% adverse move from average entry");
            }
            println!("  profit lock:        reducing levels stay beyond average entry");
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
    side: QuoteSide,
    level: u16,
}

impl GridKey {
    fn from_quote(quote: GridQuote) -> Self {
        Self {
            side: match quote.side {
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
    price: f64,
    remaining_size: f64,
    cancel_requested: bool,
}

#[derive(Default)]
struct QuoteSlot {
    live: Option<WorkingQuote>,
    /// Size reserved by an in-flight submission that has not returned its
    /// venue order id yet. It still counts against inventory headroom.
    pending_size: Option<f64>,
    busy: bool,
    retry_after_book_revision: Option<u64>,
}

impl QuoteSlot {
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

enum ActionKind {
    SubmitQuote {
        key: GridKey,
        price: f64,
        size: f64,
        book_revision: u64,
    },
    CancelQuote {
        key: GridKey,
        order_id: String,
    },
}

struct ActionCompletion {
    kind: ActionKind,
    result: std::result::Result<ExecutionReceipt, String>,
}

#[derive(Clone, Debug)]
enum OrderRole {
    Quote(GridKey),
    Cleanup,
}

#[derive(Clone, Copy, Debug)]
struct SoftResetState {
    inventory_sign: f64,
    exposure_price: f64,
}

fn soft_reset_inventory_rebalanced(
    previous_inventory_sign: f64,
    inventory_sign: f64,
    soft_reset_active: bool,
) -> bool {
    soft_reset_active && previous_inventory_sign != 0.0 && inventory_sign != previous_inventory_sign
}

fn all_grid_keys(levels_per_side: u16) -> Vec<GridKey> {
    let mut keys = Vec::with_capacity(usize::from(levels_per_side) * 2);
    for level in 1..=levels_per_side {
        keys.push(GridKey {
            side: QuoteSide::Buy,
            level,
        });
        keys.push(GridKey {
            side: QuoteSide::Sell,
            level,
        });
    }
    keys
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
        .with_context(|| format!("{} book has no bid", venue_label(definition.venue)))?;
    let initial_ask = initial_book
        .asks
        .first()
        .copied()
        .with_context(|| format!("{} book has no ask", venue_label(definition.venue)))?;
    let mut anchor_bid = initial_bid;
    let mut anchor_ask = initial_ask;
    let mut anchor_mid = (anchor_bid.price + anchor_ask.price) / 2.0;
    let initial_quotes = executable_quotes(
        quote_grid(GridSpec {
            anchor_bid,
            anchor_ask,
            best_bid: initial_bid,
            best_ask: initial_ask,
            levels_per_side: definition.levels_per_side,
            step_bps: definition.step_bps,
            max_inventory_size: definition.max_inventory_size,
            inventory_size: 0.0,
            exposure_price: None,
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

    let keys = all_grid_keys(definition.levels_per_side);
    let mut slots = keys
        .iter()
        .copied()
        .map(|key| (key, QuoteSlot::default()))
        .collect::<HashMap<_, _>>();
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
    let allocated_margin = definition
        .requested_margin
        .unwrap_or(definition.max_inventory_margin);
    let mut ledger = FillLedger::with_allocated_margin(allocated_margin);
    let mut order_roles = HashMap::<String, OrderRole>::new();
    let mut pending_fills = HashMap::<String, Vec<ObservedFill>>::new();
    let mut terminal_statuses = HashMap::<String, String>::new();
    let mut actions = JoinSet::<ActionCompletion>::new();
    let mut order_sequence = 0_u64;
    let mut cancel_sequence = 0_u64;
    let mut previous_inventory_sign = 0.0_f64;
    let mut soft_reset: Option<SoftResetState> = None;
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
                    let completion = completion
                        .context("grid action set ended unexpectedly")?
                        .context("grid action task panicked")?;
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
            let inventory = ledger.inventory();
            let inventory_sign = if inventory.abs() < rules.lot_size / 2.0 {
                0.0
            } else {
                inventory.signum()
            };

            if soft_reset_inventory_rebalanced(
                previous_inventory_sign,
                inventory_sign,
                soft_reset.is_some(),
            ) {
                let reset = soft_reset
                    .take()
                    .expect("an active grid soft reset must have state");
                append_soft_reset(
                    job_id,
                    "completed",
                    reset.inventory_sign,
                    reset.exposure_price,
                    mark_price,
                    definition.reset_threshold_pct,
                )?;
                if let Some(top) = state.top.as_ref()
                    && let (Some(best_bid), Some(best_ask)) = (top.best_bid, top.best_ask)
                {
                    let previous = anchor_mid;
                    anchor_bid = best_bid;
                    anchor_ask = best_ask;
                    anchor_mid = (best_bid.price + best_ask.price) / 2.0;
                    append_recenter(job_id, previous, anchor_mid, book_revision)?;
                }
            }

            if soft_reset.is_none()
                && inventory_sign != 0.0
                && let Some(threshold) = definition.reset_threshold_pct
                && let Some(exposure_price) = ledger.average_entry_price()
                && soft_reset_triggered(inventory, exposure_price, mark_price, threshold)?
            {
                soft_reset = Some(SoftResetState {
                    inventory_sign,
                    exposure_price,
                });
                append_soft_reset(
                    job_id,
                    "triggered",
                    inventory_sign,
                    exposure_price,
                    mark_price,
                    Some(threshold),
                )?;
            }

            if let Some(top) = state.top.as_ref()
                && let (Some(best_bid), Some(best_ask)) = (top.best_bid, top.best_ask)
            {
                let fair_price = (best_bid.price + best_ask.price) / 2.0;
                let recenter_flat_grid = inventory_sign == 0.0
                    && should_recenter(
                        anchor_mid,
                        fair_price,
                        definition.levels_per_side,
                        definition.step_bps,
                    )?;
                if recenter_flat_grid {
                    let previous = anchor_mid;
                    anchor_bid = best_bid;
                    anchor_ask = best_ask;
                    anchor_mid = fair_price;
                    append_recenter(job_id, previous, anchor_mid, book_revision)?;
                }
            }
            previous_inventory_sign = inventory_sign;

            let mut desired = if account_connected {
                if soft_reset.is_some() {
                    desired_soft_reset_quote(&state, inventory, rules)?
                } else {
                    desired_quotes(
                        definition,
                        anchor_bid,
                        anchor_ask,
                        &state,
                        inventory,
                        ledger.average_entry_price(),
                        rules,
                    )?
                }
            } else {
                HashMap::new()
            };
            cap_replenishment_to_inventory(
                &keys,
                &mut desired,
                &slots,
                inventory,
                definition.max_inventory_size,
                rules.lot_size,
                rules.size_precision,
                rules.min_notional,
            );
            for key in &keys {
                reconcile_quote(
                    job_id,
                    *key,
                    desired.get(key).copied(),
                    &parent,
                    rules.lot_size,
                    rules.min_notional,
                    book_revision,
                    soft_reset.is_some(),
                    slots.get_mut(key).expect("all grid slots are initialized"),
                    &mut actions,
                    &mut order_sequence,
                    &mut cancel_sequence,
                )?;
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
    definition: &GridJobDefinition,
    anchor_bid: OrderBookLevel,
    anchor_ask: OrderBookLevel,
    state: &BookFeedState,
    inventory_size: f64,
    exposure_price: Option<f64>,
    rules: &crate::markets::ExecutionRules,
) -> Result<HashMap<GridKey, (f64, f64)>> {
    let Some(top) = state.top.as_ref() else {
        return Ok(HashMap::new());
    };
    let (Some(best_bid), Some(best_ask)) = (top.best_bid, top.best_ask) else {
        return Ok(HashMap::new());
    };
    Ok(executable_quotes(
        quote_grid(GridSpec {
            anchor_bid,
            anchor_ask,
            best_bid,
            best_ask,
            levels_per_side: definition.levels_per_side,
            step_bps: definition.step_bps,
            max_inventory_size: definition.max_inventory_size,
            inventory_size,
            exposure_price,
            tick_size: rules.tick_size,
            price_precision: rules.price_precision,
        })?,
        rules.lot_size,
        rules.size_precision,
        rules.min_notional,
    )
    .into_iter()
    .map(|quote| (GridKey::from_quote(quote), (quote.price, quote.size)))
    .collect())
}

fn desired_soft_reset_quote(
    state: &BookFeedState,
    inventory_size: f64,
    rules: &crate::markets::ExecutionRules,
) -> Result<HashMap<GridKey, (f64, f64)>> {
    let Some(top) = state.top.as_ref() else {
        return Ok(HashMap::new());
    };
    let (Some(best_bid), Some(best_ask)) = (top.best_bid, top.best_ask) else {
        return Ok(HashMap::new());
    };
    let size = floor_to_step(inventory_size.abs(), rules.lot_size, rules.size_precision);
    if size < rules.lot_size / 2.0 {
        return Ok(HashMap::new());
    }
    let (side, price) = if inventory_size > 0.0 {
        (
            QuoteSide::Sell,
            passive_mid_price(
                OrderSide::Sell,
                best_bid,
                best_ask,
                rules.tick_size,
                rules.price_precision,
            )?,
        )
    } else {
        (
            QuoteSide::Buy,
            passive_mid_price(
                OrderSide::Buy,
                best_bid,
                best_ask,
                rules.tick_size,
                rules.price_precision,
            )?,
        )
    };
    if size * price < rules.min_notional {
        return Ok(HashMap::new());
    }

    Ok(HashMap::from([(GridKey { side, level: 1 }, (price, size))]))
}

#[allow(clippy::too_many_arguments)]
fn cap_replenishment_to_inventory(
    keys: &[GridKey],
    desired: &mut HashMap<GridKey, (f64, f64)>,
    slots: &HashMap<GridKey, QuoteSlot>,
    inventory_size: f64,
    max_inventory_size: f64,
    lot_size: f64,
    size_precision: u8,
    min_notional: f64,
) {
    let mut working_buy = 0.0;
    let mut working_sell = 0.0;
    for (key, slot) in slots {
        let size = slot.live.as_ref().map_or_else(
            || slot.pending_size.unwrap_or_default(),
            |quote| quote.remaining_size,
        );
        match key.side {
            QuoteSide::Buy => working_buy += size,
            QuoteSide::Sell => working_sell += size,
        }
    }

    let mut available_buy = (max_inventory_size - inventory_size - working_buy).max(0.0);
    let mut available_sell = (max_inventory_size + inventory_size - working_sell).max(0.0);
    for key in keys {
        let Some(slot) = slots.get(key) else {
            continue;
        };
        if slot.live.is_some() || slot.pending_size.is_some() {
            continue;
        }
        let Some((price, requested_size)) = desired.get(key).copied() else {
            continue;
        };
        let available = match key.side {
            QuoteSide::Buy => &mut available_buy,
            QuoteSide::Sell => &mut available_sell,
        };
        let size = floor_to_step(requested_size.min(*available), lot_size, size_precision);
        if size < lot_size / 2.0 || size * price < min_notional {
            desired.remove(key);
            continue;
        }
        desired.insert(*key, (price, size));
        *available = (*available - size).max(0.0);
    }
}

fn should_replace_quote(
    live: &WorkingQuote,
    desired: Option<(f64, f64)>,
    replace_size: bool,
    lot_size: f64,
    min_notional: f64,
) -> bool {
    let Some((price, size)) = desired else {
        return true;
    };
    if (price - live.price).abs() > f64::EPSILON {
        return true;
    }
    if !replace_size {
        return false;
    }
    let size_difference = (size - live.remaining_size).abs();
    size_difference >= lot_size && size_difference * price >= min_notional
}

#[allow(clippy::too_many_arguments)]
fn reconcile_quote(
    job_id: &str,
    key: GridKey,
    desired: Option<(f64, f64)>,
    parent: &TradePlan,
    lot_size: f64,
    min_notional: f64,
    book_revision: u64,
    replace_size: bool,
    slot: &mut QuoteSlot,
    actions: &mut JoinSet<ActionCompletion>,
    order_sequence: &mut u64,
    cancel_sequence: &mut u64,
) -> Result<()> {
    if slot.busy || !slot.accepts_book_revision(book_revision) {
        return Ok(());
    }
    if let Some(live) = slot.live.as_mut() {
        let replace = should_replace_quote(live, desired, replace_size, lot_size, min_notional);
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
                    kind: ActionKind::CancelQuote { key, order_id },
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
    slot.pending_size = Some(size);
    *order_sequence = order_sequence.saturating_add(1);
    let sequence = *order_sequence;
    let plan = quote_plan(parent, key.side, size, price)?;
    let job_id = job_id.to_string();
    actions.spawn(async move {
        let result = crate::runtime::submit_bot_trade(&job_id, sequence, &plan)
            .await
            .map_err(|error| format!("{error:#}"));
        ActionCompletion {
            kind: ActionKind::SubmitQuote {
                key,
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
            price,
            size,
            book_revision,
        } => {
            let slot = slots.get_mut(&key).context("grid quote slot disappeared")?;
            slot.busy = false;
            slot.pending_size = None;
            match completion.result {
                Ok(receipt) => {
                    let order_id = receipt
                        .order_id
                        .context("grid quote omitted its order id")?;
                    order_roles.insert(order_id.clone(), OrderRole::Quote(key));
                    let mut remaining_size = size;
                    if let Some(fills) = pending_fills.remove(&order_id) {
                        for fill in fills {
                            if record_fill(
                                job_id,
                                mark_price,
                                ledger,
                                &order_id,
                                &OrderRole::Quote(key),
                                &fill,
                                slots,
                            )? {
                                remaining_size = (remaining_size - fill.size).max(0.0);
                            }
                        }
                    }
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
                            price,
                            remaining_size,
                            // Do not replenish this level until its terminal fill event has
                            // updated inventory and consumed the remaining working quantity.
                            cancel_requested: awaiting_fill,
                        });
                    }
                    append_grid_quote(
                        job_id,
                        key,
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
                            key.side.name(),
                            key.level
                        );
                    }
                }
            }
        }
        ActionKind::CancelQuote { key, order_id } => {
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
                        key.side.name(),
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
                if let Some(OrderRole::Quote(key)) = state.order_roles.get(order_id) {
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
    apply_fill_role(role, fill, slots);
    Ok(true)
}

fn apply_fill_role(role: &OrderRole, fill: &ObservedFill, slots: &mut HashMap<GridKey, QuoteSlot>) {
    match role {
        OrderRole::Quote(key) => {
            if let Some(slot) = slots.get_mut(key)
                && let Some(live) = slot.live.as_mut()
            {
                live.remaining_size = (live.remaining_size - fill.size).max(0.0);
                if live.remaining_size <= f64::EPSILON {
                    slot.live = None;
                }
            }
        }
        OrderRole::Cleanup => {}
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
            apply_fill_role(role, &observed, slots);
        }
    }
    Ok(())
}

fn append_grid_quote(
    job_id: &str,
    key: GridKey,
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
            "side": key.side.name(),
            "level": key.level,
            "orderId": order_id,
            "price": price,
            "size": size,
        }),
    )
}

fn append_recenter(
    job_id: &str,
    previous_center: f64,
    center: f64,
    book_revision: u64,
) -> Result<()> {
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.grid.recenter",
            "bot": BOT_NAME,
            "jobId": job_id,
            "previousCenter": previous_center,
            "center": center,
            "bookRevision": book_revision,
        }),
    )
}

fn append_soft_reset(
    job_id: &str,
    status: &str,
    inventory_sign: f64,
    exposure_price: f64,
    mark_price: f64,
    reset_threshold_pct: Option<f64>,
) -> Result<()> {
    let threshold = reset_threshold_pct.unwrap_or_default() / 100.0;
    let trigger_price = if inventory_sign > 0.0 {
        exposure_price * (1.0 - threshold)
    } else {
        exposure_price * (1.0 + threshold)
    };
    crate::runtime::append_bot_output(
        job_id,
        &serde_json::json!({
            "type": "bot.grid.soft_reset",
            "bot": BOT_NAME,
            "jobId": job_id,
            "status": status,
            "inventory": if inventory_sign > 0.0 { "long" } else { "short" },
            "reducingSide": if inventory_sign > 0.0 { "SELL" } else { "BUY" },
            "exposurePrice": exposure_price,
            "markPrice": mark_price,
            "resetThresholdPct": reset_threshold_pct,
            "triggerPrice": trigger_price,
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
) -> Result<()> {
    let cleanup_deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let (open_orders, fills) = tokio::join!(
            adapter.open_orders(&parent.account),
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
        let cancellation_results = join_all(
            cancellation_plans
                .iter()
                .map(|(_, plan)| adapter.cancel_order(plan)),
        )
        .await;
        for ((order, _), result) in cancellation_plans.into_iter().zip(cancellation_results) {
            match result {
                Ok(receipt) => {
                    if let Some(OrderRole::Quote(key)) = order_roles.get(&order.order_id) {
                        append_grid_quote(
                            job_id,
                            *key,
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
    let receipt = adapter
        .submit_trade(&plan)
        .await
        .context("failed to unwind grid bot-owned inventory")?;
    let order_id = receipt
        .order_id
        .context("grid inventory unwind omitted its order id")?;
    order_roles.insert(order_id, OrderRole::Cleanup);

    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        reconcile_recovery(
            job_id,
            mark_price,
            &[],
            adapter.fills(&parent.account).await?,
            order_roles,
            slots,
            ledger,
        )?;
        if ledger.inventory().abs() < rules.lot_size / 2.0 {
            break;
        }
        if Instant::now() >= deadline {
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

    fn rules() -> crate::markets::ExecutionRules {
        crate::markets::ExecutionRules {
            price_precision: 1,
            size_precision: 1,
            tick_size: 0.1,
            lot_size: 0.1,
            min_notional: 1.0,
            max_leverage: 10,
            cross_margin: true,
            order_types: vec!["limit".to_string(), "market".to_string()],
            time_in_forces: vec!["alo".to_string()],
        }
    }

    fn book() -> BookFeedState {
        BookFeedState {
            revision: 1,
            top: Some(crate::domain::types::TopOfBook {
                timestamp_ms: 1,
                best_bid: Some(OrderBookLevel {
                    price: 99.0,
                    quantity: 1.0,
                }),
                best_ask: Some(OrderBookLevel {
                    price: 101.0,
                    quantity: 1.0,
                }),
            }),
            error: None,
        }
    }

    #[test]
    fn quote_fill_reduces_only_its_working_level() {
        let key = GridKey {
            side: QuoteSide::Buy,
            level: 2,
        };
        let mut slots = HashMap::from([(
            key,
            QuoteSlot {
                live: Some(WorkingQuote {
                    order_id: "order-1".to_string(),
                    price: 100.0,
                    remaining_size: 2.0,
                    cancel_requested: false,
                }),
                pending_size: None,
                busy: false,
                retry_after_book_revision: None,
            },
        )]);
        let fill = ObservedFill {
            timestamp: 1,
            recovered: false,
            buy: true,
            size: 0.75,
            price: 100.0,
            fee: Some(0.0),
        };

        apply_fill_role(&OrderRole::Quote(key), &fill, &mut slots);

        assert_eq!(
            slots[&key]
                .live
                .as_ref()
                .expect("partially filled quote remains live")
                .remaining_size,
            1.25
        );
    }

    #[test]
    fn soft_reset_long_quotes_only_the_reducing_side_at_mid() {
        let quotes = desired_soft_reset_quote(&book(), 0.5, &rules()).expect("valid reset quote");

        assert_eq!(quotes.len(), 1);
        assert_eq!(
            quotes[&GridKey {
                side: QuoteSide::Sell,
                level: 1,
            }],
            (100.0, 0.5)
        );
    }

    #[test]
    fn soft_reset_short_quotes_only_the_reducing_side_at_mid() {
        let quotes = desired_soft_reset_quote(&book(), -0.5, &rules()).expect("valid reset quote");

        assert_eq!(quotes.len(), 1);
        assert_eq!(
            quotes[&GridKey {
                side: QuoteSide::Buy,
                level: 1,
            }],
            (100.0, 0.5)
        );
    }

    #[test]
    fn normal_inventory_skew_does_not_resize_a_safe_resting_quote() {
        let live = WorkingQuote {
            order_id: "order-1".to_string(),
            price: 100.0,
            remaining_size: 1.0,
            cancel_requested: false,
        };

        assert!(!should_replace_quote(
            &live,
            Some((100.0, 0.5)),
            false,
            0.1,
            1.0,
        ));
        assert!(should_replace_quote(
            &live,
            Some((100.0, 0.5)),
            true,
            0.1,
            1.0,
        ));
        assert!(should_replace_quote(
            &live,
            Some((100.1, 1.0)),
            false,
            0.1,
            1.0,
        ));
    }

    #[test]
    fn replenishment_is_capped_by_live_and_pending_inventory_headroom() {
        let sell_one = GridKey {
            side: QuoteSide::Sell,
            level: 1,
        };
        let sell_two = GridKey {
            side: QuoteSide::Sell,
            level: 2,
        };
        let sell_three = GridKey {
            side: QuoteSide::Sell,
            level: 3,
        };
        let slots = HashMap::from([
            (
                sell_one,
                QuoteSlot {
                    live: Some(WorkingQuote {
                        order_id: "live".to_string(),
                        price: 100.0,
                        remaining_size: 0.05,
                        cancel_requested: false,
                    }),
                    pending_size: None,
                    busy: false,
                    retry_after_book_revision: None,
                },
            ),
            (
                sell_two,
                QuoteSlot {
                    live: None,
                    pending_size: Some(0.03),
                    busy: true,
                    retry_after_book_revision: None,
                },
            ),
            (sell_three, QuoteSlot::default()),
        ]);
        let mut desired = HashMap::from([
            (sell_one, (100.0, 0.05)),
            (sell_two, (100.0, 0.05)),
            (sell_three, (100.0, 0.05)),
        ]);

        cap_replenishment_to_inventory(
            &[sell_one, sell_two, sell_three],
            &mut desired,
            &slots,
            -0.9,
            1.0,
            0.01,
            2,
            1.0,
        );

        assert_eq!(desired[&sell_three], (100.0, 0.02));
    }

    #[test]
    fn normal_flattening_does_not_bypass_the_grid_recenter_threshold() {
        assert!(!soft_reset_inventory_rebalanced(1.0, 0.0, false));
        assert!(!soft_reset_inventory_rebalanced(-1.0, 1.0, false));
        assert!(soft_reset_inventory_rebalanced(1.0, 0.0, true));
        assert!(soft_reset_inventory_rebalanced(-1.0, 1.0, true));
    }
}
