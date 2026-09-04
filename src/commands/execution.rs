use std::io::{self, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{
    AccountQueryArgs, CancelOrderArgs, ClosePositionArgs, OutputFormat, TradeArgs, TradeOrderKind,
    TradeTimeInForce,
};
use crate::domain::execution::{
    CancelPlan, ExecutionReceipt, ExecutionVenue, OpenOrder, OutcomeHolding, Position,
    PositionDirection, PriceEncoding, SpotBalance, TimeInForce, TradePlan,
};
use crate::markets::Market;
use crate::providers::execution::ExecutionAdapter;
use crate::providers::hyperliquid::{HyperliquidNetwork, MARKET_ORDER_SLIPPAGE};
use crate::providers::market_data::MarketDataAdapter;
use crate::venues::{ExecutionBackend, NetworkPolicy, VenueMarket};

const HYPERLINK_RECONCILIATION_ATTEMPTS: usize = 3;
const HYPERLINK_RECONCILIATION_DELAY: Duration = Duration::from_millis(250);

pub async fn handle_trade(args: TradeArgs, direction: PositionDirection) -> Result<()> {
    handle_trade_with_position(args, direction, None).await
}

async fn handle_trade_with_position(
    mut args: TradeArgs,
    direction: PositionDirection,
    current_position: Option<Position>,
) -> Result<()> {
    args.apply_symbol_flag();
    args.validate_shape()?;
    if args.venue == ExecutionVenue::HyperliquidSpot && args.symbol.trim().is_empty() {
        args.symbol = crate::commands::markets::select_outcome_interactive(
            HyperliquidNetwork::from_testnet(args.testnet),
        )
        .await?
        .symbol;
    }
    let plan = build_trade_plan(&args, direction).await?;
    if args.dry_run {
        render_trade_plan(&plan, current_position.as_ref(), true, args.output)?;
        return Ok(());
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_trade_plan(&plan, current_position.as_ref(), false, args.output)?;
    }

    if !args.yes {
        if !matches!(args.output, OutputFormat::Terminal) {
            bail!("live execution with structured output requires --yes");
        }
        print!(
            "Submit this order to {}? [y/N]: ",
            plan.venue.network_label(plan.testnet)
        );
        io::stdout()
            .flush()
            .context("failed to flush confirmation prompt")?;
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .context("failed to read confirmation")?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("cancelled; no order was submitted");
            return Ok(());
        }
    }

    let receipt = crate::runtime::submit_trade(&plan).await?;
    let post_trade_state = reconcile_post_trade_state(&plan, &receipt).await;
    render_trade_result(&plan, &receipt, &post_trade_state, args.output)
}

#[derive(Default)]
struct PostTradeState {
    position: Option<Position>,
    position_closed: Option<bool>,
    reconciliation_error: Option<String>,
}

async fn reconcile_post_trade_state(
    plan: &TradePlan,
    receipt: &ExecutionReceipt,
) -> PostTradeState {
    if !matches!(receipt.status.as_str(), "filled" | "partiallyFilled") {
        return PostTradeState::default();
    }

    let adapter = match ExecutionAdapter::new_for_market(
        plan.venue,
        plan.testnet,
        "main",
        &plan.internal_symbol,
    )
    .await
    {
        Ok(adapter) => adapter,
        Err(error) => {
            return PostTradeState {
                reconciliation_error: Some(error.to_string()),
                ..PostTradeState::default()
            };
        }
    };
    let attempts = if plan.venue.execution_backend() == ExecutionBackend::Hyperlink {
        HYPERLINK_RECONCILIATION_ATTEMPTS
    } else {
        1
    };
    let mut latest_position = None;
    let mut latest_error = None;
    let mut successful_read = false;

    for attempt in 0..attempts {
        match adapter
            .account_snapshot_for_market(&plan.account, &plan.internal_symbol)
            .await
        {
            Ok(snapshot) => {
                successful_read = true;
                latest_error = None;
                let position = snapshot
                    .positions
                    .into_iter()
                    .find(|position| position.internal_symbol == plan.internal_symbol);

                if plan.reduce_only {
                    if position.is_none() {
                        return PostTradeState {
                            position_closed: Some(true),
                            ..PostTradeState::default()
                        };
                    }
                    latest_position = position;
                } else if let Some(position) = position {
                    let liquidation_ready = !plan.venue.is_perpetual()
                        || position.liquidation_price.is_finite()
                            && position.liquidation_price > 0.0;
                    latest_position = Some(position);
                    if liquidation_ready {
                        break;
                    }
                }
            }
            Err(error) => latest_error = Some(error.to_string()),
        }

        if attempt + 1 < attempts {
            tokio::time::sleep(HYPERLINK_RECONCILIATION_DELAY).await;
        }
    }

    PostTradeState {
        position_closed: (plan.reduce_only && successful_read).then_some(latest_position.is_none()),
        position: latest_position,
        reconciliation_error: (!successful_read).then(|| {
            latest_error.unwrap_or_else(|| "post-trade account state was unavailable".to_string())
        }),
    }
}

pub async fn handle_positions(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let venue = args.venue;
    let symbol = validate_optional_symbol(venue, args.symbol.as_deref())?;
    let account = ExecutionAdapter::configured_account(venue)?;
    let adapter = match symbol.as_deref() {
        Some(symbol) => ExecutionAdapter::new_for_market(venue, args.testnet, "main", symbol).await?,
        None => ExecutionAdapter::new(venue, args.testnet).await?,
    };
    let snapshot = match symbol.as_deref() {
        Some(symbol) => {
            adapter
                .account_snapshot_for_market(&account, symbol)
                .await?
        }
        None => adapter.account_snapshot(&account).await?,
    };
    let market_kind = symbol
        .as_deref()
        .map_or(Ok(venue.market()), |symbol| {
            crate::markets::execution_market(venue, symbol)
        })?;
    if market_kind == VenueMarket::Spot {
        let balances = snapshot
            .spot_balances
            .into_iter()
            .filter(|balance| {
                symbol.as_deref().is_none_or(|symbol| {
                    symbol
                        .split_once('/')
                        .is_some_and(|(base, _)| balance.asset == base)
                })
            })
            .collect::<Vec<_>>();
        return render_spot_balances(&balances, args.output);
    }
    if market_kind == VenueMarket::Outcome {
        let holdings = snapshot
            .outcome_holdings
            .into_iter()
            .filter(|holding| {
                symbol
                    .as_deref()
                    .is_none_or(|symbol| holding.symbol == symbol)
            })
            .collect::<Vec<_>>();
        return render_outcome_holdings(&holdings, args.output);
    }
    let positions = snapshot
        .positions
        .into_iter()
        .filter(|position| {
            symbol
                .as_deref()
                .is_none_or(|symbol| position.internal_symbol == symbol)
        })
        .collect::<Vec<_>>();
    render_positions(&positions, args.output)
}

pub async fn handle_orders(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let venue = args.venue;
    let symbol = validate_optional_symbol(venue, args.symbol.as_deref())?;
    let account = ExecutionAdapter::configured_account(venue)?;
    let adapter = match symbol.as_deref() {
        Some(symbol) => ExecutionAdapter::new_for_market(venue, args.testnet, "main", symbol).await?,
        None => ExecutionAdapter::new(venue, args.testnet).await?,
    };
    let orders = match symbol.as_deref() {
        Some(symbol) => adapter.open_orders_for_market(&account, symbol).await?,
        None => adapter.open_orders(&account).await?,
    }
    .into_iter()
    .filter(|order| {
        symbol
            .as_deref()
            .is_none_or(|symbol| order.internal_symbol == symbol)
    })
    .collect::<Vec<_>>();
    render_orders(&orders, args.output)
}

pub async fn handle_fills(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let venue = args.venue;
    let symbol = validate_optional_symbol(venue, args.symbol.as_deref())?;
    let account = ExecutionAdapter::configured_account(venue)?;
    let adapter = match symbol.as_deref() {
        Some(symbol) => ExecutionAdapter::new_for_market(venue, args.testnet, "main", symbol).await?,
        None => ExecutionAdapter::new(venue, args.testnet).await?,
    };
    let fills = adapter
        .fills(&account)
        .await?
        .into_iter()
        .filter(|fill| {
            symbol
                .as_deref()
                .is_none_or(|symbol| fill.internal_symbol == symbol)
        })
        .collect::<Vec<_>>();
    render_structured(&fills, args.output, || {
        if fills.is_empty() {
            println!("no fills");
            return;
        }
        println!(
            "{:<14} {:<12} {:>14} {:>14} {:<14} {:>13}",
            "SYMBOL", "SIDE", "AMOUNT", "PRICE", "REASON", "TS (MS)"
        );
        for fill in &fills {
            let amount = format_decimal(fill.amount, 8);
            let price = format_decimal(fill.price, 2);
            println!(
                "{:<14} {:<12?} {:>14} {:>14} {:<14} {:>13}",
                fill.internal_symbol, fill.side, amount, price, fill.reason, fill.ts_ms
            );
        }
    })
}

pub async fn handle_cancel(args: CancelOrderArgs) -> Result<()> {
    args.validate()?;
    let venue = args.venue;
    let market = execution_market_on(venue, args.testnet, &args.symbol).await?;
    ExecutionAdapter::new_for_market(venue, args.testnet, "main", &market.symbol)
        .await?
        .validate_order_id(&args.order_id)?;
    let account = ExecutionAdapter::configured_account(venue)?;
    let plan = CancelPlan {
        created_at_ms: now_ms()?,
        venue,
        testnet: args.testnet,
        account,
        internal_symbol: market.symbol.clone(),
        venue_symbol: execution_venue_symbol(venue, args.testnet, &market)?,
        order_id: args.order_id.clone(),
    };
    if args.dry_run {
        render_cancel_plan(&plan, true, args.output)?;
        return Ok(());
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_cancel_plan(&plan, false, args.output)?;
    }
    let prompt = format!("Cancel this {} order?", venue.network_label(args.testnet));
    if !args.yes && !confirm_live_action(args.output, &prompt)? {
        println!("cancelled; the order was not changed");
        return Ok(());
    }
    let receipt = crate::runtime::submit_cancel(&plan).await?;
    render_cancel_result(&plan, &receipt, args.output)
}

pub async fn handle_close(args: ClosePositionArgs) -> Result<()> {
    args.validate()?;
    let venue = args.venue;
    let requested_symbol = validate_optional_symbol(venue, args.symbol.as_deref())?;
    let account = ExecutionAdapter::configured_account(venue)?;
    let adapter = ExecutionAdapter::new(venue, args.testnet).await?;
    let snapshot = match requested_symbol.as_deref() {
        Some(symbol) => {
            adapter
                .account_snapshot_for_market(&account, symbol)
                .await?
        }
        None => adapter.account_snapshot(&account).await?,
    };
    let positions = snapshot
        .positions
        .into_iter()
        .filter(|position| {
            requested_symbol
                .as_deref()
                .is_none_or(|symbol| position.internal_symbol == symbol)
        })
        .collect::<Vec<_>>();
    let position = choose_position(positions, args.symbol.is_some(), args.output, args.yes)?;
    let direction = match position.direction {
        PositionDirection::Long => PositionDirection::Short,
        PositionDirection::Short => PositionDirection::Long,
    };
    handle_trade_with_position(
        TradeArgs {
            symbol: position.internal_symbol.clone(),
            symbol_flag: None,
            config: None,
            venue: args.venue,
            testnet: args.testnet,
            size: Some(position.size),
            margin: None,
            order_kind: TradeOrderKind::Market,
            price: None,
            tif: TradeTimeInForce::Gtc,
            leverage: Some(position.leverage.max(1.0)),
            reduce_only: true,
            sl: None,
            tp: None,
            dry_run: args.dry_run,
            yes: args.yes,
            output: args.output,
        },
        direction,
        Some(position),
    )
    .await
}

fn choose_position(
    positions: Vec<Position>,
    symbol_was_explicit: bool,
    output: OutputFormat,
    yes: bool,
) -> Result<Position> {
    match positions.len() {
        0 => bail!("no matching open position"),
        1 => {
            return positions
                .into_iter()
                .next()
                .context("selected position disappeared");
        }
        _ => {}
    }
    if symbol_was_explicit {
        bail!("the venue returned multiple open positions for the selected symbol");
    }
    if !matches!(output, OutputFormat::Terminal) || yes {
        bail!("multiple positions are open; pass the symbol to select one");
    }
    println!("select a position to close:");
    for (index, position) in positions.iter().enumerate() {
        println!(
            "  {}) {} {:?} size={} entry={} mark={}",
            index + 1,
            position.internal_symbol,
            position.direction,
            position.size,
            position.entry_price,
            position.mark_price
        );
    }
    print!("Position [1-{}]: ", positions.len());
    io::stdout()
        .flush()
        .context("failed to flush position prompt")?;
    let mut selection = String::new();
    io::stdin()
        .read_line(&mut selection)
        .context("failed to read position selection")?;
    let index = selection
        .trim()
        .parse::<usize>()
        .context("position selection must be a number")?;
    if index == 0 || index > positions.len() {
        bail!(
            "position selection must be between 1 and {}",
            positions.len()
        );
    }
    Ok(positions[index - 1].clone())
}

pub(crate) async fn build_trade_plan(
    args: &TradeArgs,
    direction: PositionDirection,
) -> Result<TradePlan> {
    build_trade_plan_with_price_normalization(args, direction, false, None, None, None).await
}

pub(crate) async fn build_script_trade_plan_for_account(
    args: &TradeArgs,
    direction: PositionDirection,
    account: &str,
    reference_price: Option<f64>,
    max_slippage: Option<f64>,
) -> Result<TradePlan> {
    build_trade_plan_with_price_normalization(
        args,
        direction,
        true,
        Some(account),
        reference_price,
        max_slippage,
    )
    .await
}

async fn build_trade_plan_with_price_normalization(
    args: &TradeArgs,
    direction: PositionDirection,
    normalize_prices: bool,
    account: Option<&str>,
    market_reference_price: Option<f64>,
    max_slippage: Option<f64>,
) -> Result<TradePlan> {
    let venue = args.venue;
    let market_kind = crate::markets::execution_market(venue, &args.symbol)?;
    let outcome = if market_kind == VenueMarket::Outcome {
        Some(
            crate::markets::outcomes::resolve(
                HyperliquidNetwork::from_testnet(args.testnet),
                &args.symbol,
            )
            .await?,
        )
    } else {
        None
    };
    let market = outcome.as_ref().map_or_else(
        || execution_market(venue, &args.symbol),
        |instrument| {
            Ok(crate::markets::outcomes::market_from_instrument(instrument))
        },
    )?;
    let normalized_args = normalize_prices
        .then(|| normalize_automated_prices(args, venue, &market, direction))
        .transpose()?;
    let args = normalized_args.as_ref().unwrap_or(args);
    validate_market_rules(venue, &market, args)?;
    let venue_spec = venue.spec()?;
    let leverage = market_kind
        .is_perpetual()
        .then(|| args.leverage.unwrap_or(1.0));
    let sizing_leverage = leverage.unwrap_or(1.0);
    let mut rules = execution_rules(venue, args.testnet, &market)?;
    if market_kind.is_perpetual() {
        let max_leverage = ExecutionAdapter::new_for_market(
            venue,
            args.testnet,
            "main",
            &market.symbol,
        )
            .await?
            .max_leverage(&market.symbol)
            .await?;
        rules.max_leverage = u16::try_from(max_leverage)
            .context("venue max leverage exceeds Market Lab's supported range")?;
        if leverage.is_some_and(|leverage| leverage > f64::from(rules.max_leverage)) {
            bail!(
                "{} leverage must be at most {} for {}",
                venue.label(),
                rules.max_leverage,
                market.symbol
            );
        }
    }
    let account = account.map_or_else(
        || ExecutionAdapter::configured_account(venue),
        |account| Ok(account.to_string()),
    )?;
    let reference_price = match args.order_kind {
        TradeOrderKind::Limit => args
            .price
            .context("--price is required with --type limit")?,
        TradeOrderKind::Market if market_reference_price.is_some() => {
            market_reference_price.expect("guarded script market reference")
        }
        TradeOrderKind::Market => {
            MarketDataAdapter::for_execution_market(venue, args.testnet, &market.symbol)?
                .ticker(&market.symbol)
                .await?
                .mark_price
        }
    };
    if !reference_price.is_finite() || reference_price <= 0.0 {
        bail!(
            "{} returned an invalid reference price for {}",
            venue.label(),
            market.venue_symbol
        );
    }
    validate_protection_prices(venue, &market, args, direction, reference_price)?;

    let market_slippage = max_slippage.unwrap_or(MARKET_ORDER_SLIPPAGE);
    let size = if let Some(size) = args.size {
        if !is_step_aligned(size, rules.lot_size) {
            bail!(
                "--size {size} is not aligned to {} lot size {} for {}",
                venue.label(),
                rules.lot_size,
                market.symbol
            );
        }
        round_to_precision(size, rules.size_precision)
    } else {
        let margin = args
            .margin
            .context("one of --size or --margin is required")?;
        let sizing_price = if !market_kind.is_perpetual()
            && direction == PositionDirection::Long
            && matches!(args.order_kind, TradeOrderKind::Market)
        {
            reference_price * (1.0 + market_slippage)
        } else {
            reference_price
        };
        let raw_size = exposure_from_margin(margin, sizing_leverage)? / sizing_price;
        floor_to_step(raw_size, rules.lot_size, rules.size_precision)
    };
    if size <= 0.0 {
        if !market_kind.is_perpetual() {
            bail!(
                "requested amount produces a size below {} spot lot size {} on {}",
                venue.label(),
                rules.lot_size,
                market.symbol
            );
        }
        bail!(
            "requested margin and leverage produce a size below {} lot size {} on {}",
            venue.label(),
            rules.lot_size,
            market.symbol
        );
    }
    let estimated_exposure = size * reference_price;
    let estimated_margin = estimated_exposure / sizing_leverage;
    if estimated_exposure + f64::EPSILON < rules.min_notional {
        bail!(
            "estimated exposure {estimated_exposure:.8} is below {} minimum notional {} for {}",
            venue.label(),
            rules.min_notional,
            market.symbol
        );
    }
    if market_kind == VenueMarket::Spot {
        validate_spot_funds(
            venue,
            args.testnet,
            &account,
            &market,
            direction,
            size,
            match args.order_kind {
                TradeOrderKind::Market if direction == PositionDirection::Long => {
                    reference_price * (1.0 + market_slippage)
                }
                TradeOrderKind::Market | TradeOrderKind::Limit => reference_price,
            },
        )
        .await?;
    }
    if market_kind == VenueMarket::Outcome {
        validate_outcome_funds(
            args.testnet,
            &account,
            outcome
                .as_ref()
                .context("outcome metadata was not resolved")?,
            direction,
            size,
            match args.order_kind {
                TradeOrderKind::Market if direction == PositionDirection::Long => {
                    reference_price * (1.0 + market_slippage)
                }
                TradeOrderKind::Market | TradeOrderKind::Limit => reference_price,
            },
        )
        .await?;
    }

    Ok(TradePlan {
        created_at_ms: now_ms()?,
        venue,
        testnet: args.testnet,
        account,
        internal_symbol: market.symbol.clone(),
        venue_symbol: execution_venue_symbol(venue, args.testnet, &market)?,
        direction,
        side: direction.into(),
        order_kind: args.order_kind.into(),
        time_in_force: matches!(args.order_kind, TradeOrderKind::Limit)
            .then(|| TimeInForce::from(args.tif)),
        requested_size: args.size,
        size,
        price: args.price,
        reference_price,
        max_slippage,
        requested_margin: args.margin,
        estimated_margin,
        estimated_exposure,
        projected_liquidation_price: None,
        leverage,
        reduce_only: args.reduce_only,
        stop_loss_price: args.sl,
        take_profit_price: args.tp,
        market_fingerprint: outcome.map(|instrument| instrument.metadata_fingerprint),
    })
}

#[derive(Clone, Copy)]
enum PriceRounding {
    Down,
    Up,
    Nearest,
}

fn normalize_automated_prices(
    args: &TradeArgs,
    venue: ExecutionVenue,
    market: &Market,
    direction: PositionDirection,
) -> Result<TradeArgs> {
    let rules = execution_rules(venue, args.testnet, market)?;
    let mut normalized = args.clone();
    if let Some(price) = args.price {
        normalized.price = Some(normalize_price_to_rules(
            venue,
            price,
            &rules,
            match direction {
                PositionDirection::Long => PriceRounding::Down,
                PositionDirection::Short => PriceRounding::Up,
            },
        )?);
    }
    if let Some(price) = args.sl {
        normalized.sl = Some(normalize_price_to_rules(
            venue,
            price,
            &rules,
            PriceRounding::Nearest,
        )?);
    }
    if let Some(price) = args.tp {
        normalized.tp = Some(normalize_price_to_rules(
            venue,
            price,
            &rules,
            PriceRounding::Nearest,
        )?);
    }
    Ok(normalized)
}

fn normalize_price_to_rules(
    venue: ExecutionVenue,
    price: f64,
    rules: &crate::markets::ExecutionRules,
    rounding: PriceRounding,
) -> Result<f64> {
    if !price.is_finite() || price <= 0.0 {
        bail!("automated order price must be finite and positive");
    }
    let spec = venue.spec()?;
    let normalized = match ExecutionAdapter::capabilities(venue).price_encoding {
        PriceEncoding::TickSize => {
            let units = price / rules.tick_size;
            let units = match rounding {
                PriceRounding::Down => (units + 1e-10).floor(),
                PriceRounding::Up => (units - 1e-10).ceil(),
                PriceRounding::Nearest => units.round(),
            };
            round_to_precision(units * rules.tick_size, rules.price_precision)
        }
        PriceEncoding::Hyperliquid => {
            let price_decimals = if spec.market.is_perpetual() {
                6
            } else {
                rules.price_precision
            };
            normalize_hyperliquid_price(price, rules.size_precision, price_decimals, rounding)
        }
    };
    if normalized <= 0.0 {
        bail!("automated order price is below the venue's minimum price increment");
    }
    Ok(normalized)
}

fn normalize_hyperliquid_price(
    price: f64,
    size_precision: u8,
    max_price_decimals: u8,
    rounding: PriceRounding,
) -> f64 {
    let down = crate::providers::hyperliquid::execution::normalize_price_for(
        price,
        size_precision,
        max_price_decimals,
        false,
    );
    let up = crate::providers::hyperliquid::execution::normalize_price_for(
        price,
        size_precision,
        max_price_decimals,
        true,
    );
    match rounding {
        PriceRounding::Down => down,
        PriceRounding::Up => up,
        PriceRounding::Nearest if price - down <= up - price => down,
        PriceRounding::Nearest => up,
    }
}

async fn validate_spot_funds(
    venue: ExecutionVenue,
    testnet: bool,
    account: &str,
    market: &Market,
    direction: PositionDirection,
    size: f64,
    execution_price: f64,
) -> Result<()> {
    let snapshot = ExecutionAdapter::new(venue, testnet)
        .await?
        .account_snapshot(account)
        .await?;
    let (asset, required, unit) = match direction {
        PositionDirection::Long => (market.quote_asset.as_str(), size * execution_price, "quote"),
        PositionDirection::Short => (market.base_asset.as_str(), size, "base"),
    };
    let balance = snapshot
        .spot_balances
        .iter()
        .find(|balance| balance.asset == asset);
    let available = balance.map_or(0.0, |balance| balance.available);
    let venue_asset = balance.map_or(asset, |balance| balance.venue_asset.as_str());
    let tolerance = 1e-12_f64.max(required.abs() * 1e-12);
    if available + tolerance < required {
        bail!(
            "insufficient {} {venue_asset} balance: {available:.8} available, {required:.8} {unit} amount required",
            venue.label()
        );
    }
    Ok(())
}

async fn validate_outcome_funds(
    testnet: bool,
    account: &str,
    instrument: &crate::markets::outcomes::OutcomeInstrument,
    direction: PositionDirection,
    size: f64,
    execution_price: f64,
) -> Result<()> {
    let snapshot = ExecutionAdapter::new_for_market(
        ExecutionVenue::HyperliquidSpot,
        testnet,
        "main",
        &instrument.symbol,
    )
        .await?
        .account_snapshot(account)
        .await?;
    let (available, required, asset) = match direction {
        PositionDirection::Long => {
            let available = snapshot
                .spot_balances
                .iter()
                .find(|balance| balance.asset.eq_ignore_ascii_case(&instrument.quote_token))
                .map_or(0.0, |balance| balance.available);
            (
                available,
                size * execution_price,
                instrument.quote_token.as_str(),
            )
        }
        PositionDirection::Short => {
            let available = snapshot
                .outcome_holdings
                .iter()
                .find(|holding| holding.symbol == instrument.symbol)
                .map_or(0.0, |holding| holding.available);
            (available, size, instrument.token_name.as_str())
        }
    };
    let tolerance = 1e-12_f64.max(required.abs() * 1e-12);
    if available + tolerance < required {
        bail!(
            "insufficient Hyperliquid outcome {asset} balance: {available:.8} available, {required:.8} required"
        );
    }
    Ok(())
}

fn validate_protection_prices(
    venue: ExecutionVenue,
    market: &Market,
    args: &TradeArgs,
    direction: PositionDirection,
    entry_price: f64,
) -> Result<()> {
    let rules = market.execution_rules()?;
    for (flag, price) in [("--sl", args.sl), ("--tp", args.tp)] {
        if let Some(price) = price
            && !is_price_aligned(venue, price, rules)
        {
            bail!(
                "{flag} {price} is not aligned to {} price rules for {}",
                venue.label(),
                market.symbol
            );
        }
    }
    match direction {
        PositionDirection::Long => {
            if args.sl.is_some_and(|price| price >= entry_price) {
                bail!("--sl must be below the long entry price {entry_price}");
            }
            if args.tp.is_some_and(|price| price <= entry_price) {
                bail!("--tp must be above the long entry price {entry_price}");
            }
        }
        PositionDirection::Short => {
            if args.sl.is_some_and(|price| price <= entry_price) {
                bail!("--sl must be above the short entry price {entry_price}");
            }
            if args.tp.is_some_and(|price| price >= entry_price) {
                bail!("--tp must be below the short entry price {entry_price}");
            }
        }
    }
    Ok(())
}

fn validate_market_rules(venue: ExecutionVenue, market: &Market, args: &TradeArgs) -> Result<()> {
    let capabilities = ExecutionAdapter::capabilities(venue);
    let rules = execution_rules(venue, args.testnet, market)?;
    if !capabilities.order_kinds.contains(&args.order_kind.into()) {
        bail!(
            "{} execution adapter does not support this order type",
            venue.label()
        );
    }
    if !market.is_available() {
        bail!(
            "{} market `{}` is not trading",
            venue.label(),
            market.venue_symbol
        );
    }
    let order_type = match args.order_kind {
        TradeOrderKind::Market => "MARKET",
        TradeOrderKind::Limit => "LIMIT",
    };
    if !market.supports_order_type(order_type) {
        bail!(
            "{} market `{}` does not support {order_type} orders",
            venue.label(),
            market.venue_symbol
        );
    }
    if !venue.is_perpetual() && args.leverage.is_some() {
        bail!("--leverage is not supported for {}", venue.label());
    }
    let leverage = args.leverage.unwrap_or(1.0);
    if leverage > f64::from(rules.max_leverage) {
        bail!(
            "--leverage {} exceeds {} maximum {}x for {}",
            leverage,
            venue.label(),
            rules.max_leverage,
            market.symbol
        );
    }
    if ExecutionAdapter::capabilities(venue).integer_leverage
        && leverage.fract().abs() > f64::EPSILON
    {
        bail!("{} leverage must be a whole number", venue.label());
    }
    if !venue.is_perpetual() {
        if args.reduce_only {
            bail!("{} orders do not support --reduce-only", venue.label());
        }
        if args.sl.is_some() || args.tp.is_some() {
            bail!(
                "{} does not support attached --sl or --tp orders",
                venue.label()
            );
        }
    }
    if let Some(price) = args.price
        && !is_price_aligned(venue, price, &rules)
    {
        bail!(
            "--price {price} is not aligned to {} price rules for {} (snapshot tick {})",
            venue.label(),
            market.symbol,
            rules.tick_size,
        );
    }
    if matches!(args.order_kind, TradeOrderKind::Limit) {
        let tif = match args.tif {
            TradeTimeInForce::Gtc => "GTC",
            TradeTimeInForce::Ioc => "IOC",
            TradeTimeInForce::Alo => "ALO",
        };
        if !rules
            .time_in_forces
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(tif))
        {
            bail!(
                "{} market `{}` does not support TIF {tif}",
                venue.label(),
                market.venue_symbol
            );
        }
    }
    Ok(())
}

fn is_price_aligned(
    venue: ExecutionVenue,
    price: f64,
    rules: &crate::markets::ExecutionRules,
) -> bool {
    let Ok(spec) = venue.spec() else {
        return false;
    };
    match ExecutionAdapter::capabilities(venue).price_encoding {
        PriceEncoding::TickSize => is_step_aligned(price, rules.tick_size),
        PriceEncoding::Hyperliquid => {
            let max_price_decimals = if spec.market.is_perpetual() { 6 } else { 8 };
            crate::providers::hyperliquid::execution::validate_price_for(
                price,
                rules.size_precision,
                max_price_decimals,
            )
            .is_ok()
        }
    }
}

fn validate_optional_symbol(venue: ExecutionVenue, symbol: Option<&str>) -> Result<Option<String>> {
    if symbol.is_some_and(|symbol| {
        crate::markets::execution_market(venue, symbol) == Ok(VenueMarket::Outcome)
    }) {
        return symbol
            .map(|symbol| {
                crate::markets::outcomes::parse_symbol(symbol).map(
                    |(outcome, side)| {
                        crate::markets::outcomes::canonical_symbol(outcome, side)
                    },
                )
            })
            .transpose();
    }
    symbol
        .map(|symbol| execution_market(venue, symbol).map(|market| market.symbol.clone()))
        .transpose()
}

fn execution_market(venue: ExecutionVenue, symbol: &str) -> Result<std::sync::Arc<Market>> {
    crate::markets::exchange_market(venue.spec()?.market_data_venue.as_str(), symbol)
}

async fn execution_market_on(
    venue: ExecutionVenue,
    testnet: bool,
    symbol: &str,
) -> Result<std::sync::Arc<Market>> {
    if crate::markets::execution_market(venue, symbol)? == VenueMarket::Outcome {
        let instrument = crate::markets::outcomes::resolve(
            HyperliquidNetwork::from_testnet(testnet),
            symbol,
        )
        .await?;
        return Ok(crate::markets::outcomes::market_from_instrument(&instrument));
    }
    execution_market(venue, symbol)
}

fn execution_rules(
    venue: ExecutionVenue,
    testnet: bool,
    market: &Market,
) -> Result<crate::markets::ExecutionRules> {
    match crate::markets::execution_market(venue, &market.symbol)? {
        VenueMarket::Spot => market
            .network_variant(HyperliquidNetwork::from_testnet(testnet).label())
            .map(|variant| variant.execution),
        VenueMarket::Perpetual if !market.network_variants.is_empty() => market
            .network_variant(HyperliquidNetwork::from_testnet(testnet).label())
            .map(|variant| variant.execution),
        VenueMarket::Outcome | VenueMarket::Perpetual => market.execution_rules().cloned(),
    }
}

fn execution_venue_symbol(venue: ExecutionVenue, testnet: bool, market: &Market) -> Result<String> {
    match crate::markets::execution_market(venue, &market.symbol)? {
        VenueMarket::Spot => market
            .network_variant(HyperliquidNetwork::from_testnet(testnet).label())
            .map(|variant| variant.venue_symbol),
        VenueMarket::Perpetual if !market.network_variants.is_empty() => market
            .network_variant(HyperliquidNetwork::from_testnet(testnet).label())
            .map(|variant| variant.venue_symbol),
        VenueMarket::Outcome | VenueMarket::Perpetual => Ok(market.venue_symbol.clone()),
    }
}

fn is_step_aligned(value: f64, step: f64) -> bool {
    let units = value / step;
    (units - units.round()).abs() <= 1e-8_f64.max(units.abs() * 1e-12)
}

fn exposure_from_margin(margin: f64, leverage: f64) -> Result<f64> {
    let exposure = margin * leverage;
    if !exposure.is_finite() {
        bail!("--margin multiplied by --leverage is too large");
    }
    Ok(exposure)
}

fn floor_to_step(value: f64, step: f64, precision: u8) -> f64 {
    let units = (value / step + 1e-10).floor();
    round_to_precision(units * step, precision)
}

fn round_to_precision(value: f64, precision: u8) -> f64 {
    let scale = 10_f64.powi(i32::from(precision));
    (value * scale).round() / scale
}

fn render_trade_plan(
    plan: &TradePlan,
    current_position: Option<&Position>,
    dry_run: bool,
    output: OutputFormat,
) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "{}",
                if dry_run {
                    "trade plan (dry run — nothing will be submitted)"
                } else {
                    "trade plan"
                }
            );
            println!("  venue:             {}", plan.venue);
            if plan.venue.spec()?.network != NetworkPolicy::TestnetOnly {
                println!(
                    "  network:           {}",
                    if plan.testnet { "testnet" } else { "mainnet" }
                );
            }
            println!("  account:           {}", plan.account);
            println!(
                "  symbol:            {} ({})",
                plan.internal_symbol, plan.venue_symbol
            );
            println!(
                "  direction / side:  {:?} / {:?}",
                plan.direction, plan.side
            );
            println!("  order:             {:?}", plan.order_kind);
            if let Some(tif) = plan.time_in_force {
                println!("  time in force:     {:?}", tif);
            }
            println!("  size:              {}", plan.size);
            if let Some(price) = plan.price {
                println!("  limit price:       {price}");
            }
            println!("  reference price:   {}", plan.reference_price);
            if let Some(margin) = plan.requested_margin {
                println!("  requested margin:  {margin:.8}");
            }
            println!("  est. margin:       {:.8}", plan.estimated_margin);
            println!("  est. exposure:     {:.8}", plan.estimated_exposure);
            if let Some(leverage) = plan.leverage {
                println!("  leverage:          {leverage}x");
            }
            if let Some(position) = current_position {
                if position.liquidation_price.is_finite() && position.liquidation_price > 0.0 {
                    println!(
                        "  liquidation price: {} (current position)",
                        position.liquidation_price
                    );
                } else {
                    println!(
                        "  liquidation price: not available from {} for the current position",
                        plan.venue.label()
                    );
                }
            } else if plan.venue.is_perpetual() && !plan.reduce_only {
                println!(
                    "  liquidation price: determined by {} after fill",
                    plan.venue.label()
                );
            }
            println!("  reduce only:       {}", plan.reduce_only);
            if let Some(price) = plan.stop_loss_price {
                println!("  stop loss:         {price} (native on-fill trigger)");
            }
            if let Some(price) = plan.take_profit_price {
                println!("  take profit:       {price} (native on-fill trigger)");
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_cancel_plan(plan: &CancelPlan, dry_run: bool, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(plan)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(plan)?),
        OutputFormat::Terminal => {
            println!(
                "{}",
                if dry_run {
                    "cancel plan (dry run — nothing will be submitted)"
                } else {
                    "cancel plan"
                }
            );
            println!("  venue:    {}", plan.venue);
            println!("  account:  {}", plan.account);
            println!(
                "  symbol:   {} ({})",
                plan.internal_symbol, plan.venue_symbol
            );
            println!("  order id: {}", plan.order_id);
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn confirm_live_action(output: OutputFormat, prompt: &str) -> Result<bool> {
    if !matches!(output, OutputFormat::Terminal) {
        bail!("live execution with structured output requires --yes");
    }
    print!("{prompt} [y/N]: ");
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

#[derive(Serialize)]
struct TradeExecutionOutput<'a> {
    plan: &'a TradePlan,
    receipt: &'a ExecutionReceipt,
    post_trade_position: Option<&'a Position>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position_closed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position_reconciliation_error: Option<&'a str>,
}

#[derive(Serialize)]
struct CancelExecutionOutput<'a> {
    plan: &'a CancelPlan,
    receipt: &'a ExecutionReceipt,
}

fn render_trade_result(
    plan: &TradePlan,
    receipt: &ExecutionReceipt,
    post_trade_state: &PostTradeState,
    output: OutputFormat,
) -> Result<()> {
    let result = TradeExecutionOutput {
        plan,
        receipt,
        post_trade_position: post_trade_state.position.as_ref(),
        position_closed: post_trade_state.position_closed,
        position_reconciliation_error: post_trade_state.reconciliation_error.as_deref(),
    };
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&result)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&result)?),
        OutputFormat::Terminal => {
            render_terminal_receipt(receipt);
            if post_trade_state.position_closed == Some(true) {
                println!("  position:          closed");
            } else if let Some(position) = post_trade_state.position.as_ref() {
                println!("  position:          open");
                if plan.reduce_only {
                    println!("  remaining size:    {}", position.size);
                }
                println!("  position leverage: {}x", position.leverage);
                if position.liquidation_price.is_finite() && position.liquidation_price > 0.0 {
                    println!("  liquidation price: {}", position.liquidation_price);
                } else if plan.venue.is_perpetual() {
                    println!(
                        "  liquidation price: not yet available from {}",
                        plan.venue.label()
                    );
                }
            } else if let Some(error) = post_trade_state.reconciliation_error.as_deref() {
                println!("  position state:    unavailable ({error})");
            } else if plan.venue.is_perpetual()
                && matches!(receipt.status.as_str(), "filled" | "partiallyFilled")
            {
                println!(
                    "  liquidation price: not yet available from {}",
                    plan.venue.label()
                );
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_cancel_result(
    plan: &CancelPlan,
    receipt: &ExecutionReceipt,
    output: OutputFormat,
) -> Result<()> {
    let result = CancelExecutionOutput { plan, receipt };
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&result)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&result)?),
        OutputFormat::Terminal => render_terminal_receipt(receipt),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn render_terminal_receipt(receipt: &ExecutionReceipt) {
    println!("{}: order {}", receipt.venue, receipt.status);
    if let Some(order_id) = &receipt.order_id {
        println!("  order id: {order_id}");
    }
    if let Some(requested_size) = receipt.requested_size {
        println!("  requested size: {requested_size}");
    }
    if let Some(filled_size) = receipt.filled_size {
        println!("  filled size:    {filled_size}");
    }
    if let Some(average_fill_price) = receipt.average_fill_price {
        println!("  average price:  {average_fill_price}");
    }
    println!("  terminal: {}", receipt.terminal);
}

fn render_positions(positions: &[Position], output: OutputFormat) -> Result<()> {
    render_structured(positions, output, || {
        if positions.is_empty() {
            println!("no open positions");
            return;
        }
        println!(
            "{:<14} {:<8} {:>12} {:>12} {:>12} {:>12} {:>10} {:>10}",
            "SYMBOL", "SIDE", "SIZE", "VALUE", "ENTRY", "MARK", "UPNL", "LEVERAGE"
        );
        for position in positions {
            let size = format_decimal(position.size, 8);
            let value = format_decimal(position.notional.abs(), 2);
            let entry = format_decimal(position.entry_price, 2);
            let mark = format_decimal(position.mark_price, 2);
            let unrealized_pnl = format_decimal(position.unrealized_pnl, 2);
            let leverage = format!("{}x", format_decimal(position.leverage, 2));
            println!(
                "{:<14} {:<8?} {:>12} {:>12} {:>12} {:>12} {:>10} {:>10}",
                position.internal_symbol,
                position.direction,
                size,
                value,
                entry,
                mark,
                unrealized_pnl,
                leverage
            );
        }
    })
}

fn render_spot_balances(balances: &[SpotBalance], output: OutputFormat) -> Result<()> {
    render_structured(balances, output, || {
        if balances.is_empty() {
            println!("no spot balances");
            return;
        }
        println!(
            "{:<12} {:<14} {:>16} {:>16} {:>16}",
            "ASSET", "VENUE ASSET", "TOTAL", "HELD", "AVAILABLE"
        );
        for balance in balances {
            println!(
                "{:<12} {:<14} {:>16} {:>16} {:>16}",
                balance.asset,
                balance.venue_asset,
                format_decimal(balance.total, 8),
                format_decimal(balance.held, 8),
                format_decimal(balance.available, 8),
            );
        }
    })
}

fn render_outcome_holdings(holdings: &[OutcomeHolding], output: OutputFormat) -> Result<()> {
    render_structured(holdings, output, || {
        if holdings.is_empty() {
            println!("no outcome holdings");
            return;
        }
        println!(
            "{:<12} {:<24} {:<16} {:>14} {:>14} {:>14}",
            "SYMBOL", "OUTCOME", "SIDE", "TOTAL", "HELD", "AVAILABLE"
        );
        for holding in holdings {
            println!(
                "{:<12} {:<24} {:<16} {:>14} {:>14} {:>14}",
                holding.symbol,
                truncate_terminal(&holding.outcome_name, 24),
                truncate_terminal(&holding.side_name, 16),
                format_decimal(holding.total, 8),
                format_decimal(holding.held, 8),
                format_decimal(holding.available, 8),
            );
        }
    })
}

fn truncate_terminal(value: &str, width: usize) -> String {
    let clean = clean_terminal_text(value);
    if clean.chars().count() <= width {
        clean
    } else {
        clean
            .chars()
            .take(width.saturating_sub(1))
            .collect::<String>()
            + "…"
    }
}

fn clean_terminal_text(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .collect()
}

fn render_orders(orders: &[OpenOrder], output: OutputFormat) -> Result<()> {
    render_structured(orders, output, || {
        if orders.is_empty() {
            println!("no open orders");
            return;
        }
        println!(
            "{:<14} {:<8} {:>14} {:>14} {:>14} {:<10} {:<10}",
            "SYMBOL", "SIDE", "PRICE", "REMAINING", "FILLED", "TIF", "STATUS"
        );
        for order in orders {
            let price = format_decimal(order.price, 2);
            let remaining = format_decimal(order.remaining_size, 8);
            let filled = format_decimal(order.filled_size, 8);
            println!(
                "{:<14} {:<8?} {:>14} {:>14} {:>14} {:<10} {:<10}",
                order.internal_symbol,
                order.side,
                price,
                remaining,
                filled,
                order.time_in_force,
                order.status
            );
        }
    })
}

fn render_structured<T: Serialize + ?Sized>(
    value: &T,
    output: OutputFormat,
    terminal: impl FnOnce(),
) -> Result<()> {
    match output {
        OutputFormat::Terminal => terminal(),
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(value)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(value)?),
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!(),
    }
    Ok(())
}

fn format_decimal(value: f64, max_decimals: usize) -> String {
    let formatted = format!("{value:.max_decimals$}");
    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
    if trimmed == "-0" {
        "0".to_string()
    } else {
        trimmed.to_string()
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

    #[test]
    fn step_alignment_handles_decimal_market_rules() {
        assert!(is_step_aligned(0.001, 0.000001));
        assert!(is_step_aligned(64_535.5, 0.1));
        assert!(!is_step_aligned(64_535.55, 0.1));
    }

    #[test]
    fn floors_exposure_size_to_lot_size() {
        assert_eq!(floor_to_step(0.0012349, 0.000001, 6), 0.001234);
    }

    #[test]
    fn margin_is_multiplied_by_leverage_to_create_exposure() {
        assert_eq!(
            exposure_from_margin(100.0, 10.0).expect("valid exposure"),
            1_000.0
        );
    }

    #[test]
    fn formats_terminal_numbers_without_noisy_zeroes() {
        assert_eq!(format_decimal(64_771.7, 2), "64771.7");
        assert_eq!(format_decimal(0.000154, 8), "0.000154");
        assert_eq!(format_decimal(-0.004928, 2), "0");
        assert_eq!(format_decimal(9.9699138, 2), "9.97");
    }

    #[test]
    fn normalizes_generated_hyperliquid_prices_without_exposing_wire_rules() {
        let rules = crate::markets::ExecutionRules {
            price_precision: 6,
            size_precision: 4,
            tick_size: 0.1,
            lot_size: 0.0001,
            min_notional: 10.0,
            max_leverage: 50,
            cross_margin: true,
            order_types: vec!["MARKET".to_string(), "LIMIT".to_string()],
            time_in_forces: vec!["GTC".to_string()],
        };

        assert_eq!(
            normalize_price_to_rules(
                ExecutionVenue::Hyperliquid,
                1911.94355,
                &rules,
                PriceRounding::Nearest,
            )
            .unwrap(),
            1911.9
        );
        assert_eq!(
            normalize_price_to_rules(
                ExecutionVenue::Hyperliquid,
                1911.94355,
                &rules,
                PriceRounding::Up,
            )
            .unwrap(),
            1912.0
        );
    }

    #[test]
    fn generated_limit_prices_round_away_from_crossing() {
        let rules = crate::markets::ExecutionRules {
            price_precision: 2,
            size_precision: 6,
            tick_size: 0.25,
            lot_size: 0.000001,
            min_notional: 1.0,
            max_leverage: 50,
            cross_margin: true,
            order_types: vec!["MARKET".to_string(), "LIMIT".to_string()],
            time_in_forces: vec!["GTC".to_string()],
        };

        assert_eq!(
            normalize_price_to_rules(ExecutionVenue::Bulk, 100.13, &rules, PriceRounding::Down,)
                .unwrap(),
            100.0
        );
        assert_eq!(
            normalize_price_to_rules(ExecutionVenue::Bulk, 100.13, &rules, PriceRounding::Up,)
                .unwrap(),
            100.25
        );
    }

    #[tokio::test]
    async fn script_plan_uses_cached_market_reference_and_slippage() {
        let args = TradeArgs {
            symbol: "BTC".to_string(),
            symbol_flag: None,
            config: None,
            venue: crate::cli::ExecutionVenueArg::Bulk,
            testnet: false,
            size: Some(0.001),
            margin: None,
            order_kind: TradeOrderKind::Market,
            price: None,
            tif: TradeTimeInForce::Gtc,
            leverage: Some(5.0),
            reduce_only: false,
            sl: None,
            tp: None,
            dry_run: false,
            yes: true,
            output: OutputFormat::Json,
        };

        let plan = build_script_trade_plan_for_account(
            &args,
            PositionDirection::Long,
            "account",
            Some(65_123.0),
            Some(0.0005),
        )
        .await
        .expect("cached reference should avoid a ticker lookup");

        assert_eq!(plan.reference_price, 65_123.0);
        assert_eq!(plan.max_slippage, Some(0.0005));
    }
}
