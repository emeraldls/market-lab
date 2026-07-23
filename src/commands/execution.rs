use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{
    AccountQueryArgs, CancelOrderArgs, ClosePositionArgs, OutputFormat, TradeArgs, TradeOrderKind,
    TradeTimeInForce,
};
use crate::domain::execution::{
    CancelPlan, ExecutionReceipt, ExecutionVenue, OpenOrder, Position, PositionDirection,
    TimeInForce, TradePlan,
};
use crate::markets::Market;
use crate::providers::bulk::market_data::BulkProvider;
use crate::providers::execution::ExecutionAdapter;
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::market_data::HyperliquidProvider;

pub async fn handle_trade(args: TradeArgs, direction: PositionDirection) -> Result<()> {
    args.validate_shape()?;
    let plan = build_trade_plan(&args, direction).await?;
    if args.dry_run {
        render_trade_plan(&plan, true, args.output)?;
        return Ok(());
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_trade_plan(&plan, false, args.output)?;
    }

    if !args.yes {
        if !matches!(args.output, OutputFormat::Terminal) {
            bail!("live execution with structured output requires --yes");
        }
        print!(
            "Submit this order to {}? [y/N]: ",
            venue_network_label(plan.venue, plan.testnet)
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
    let post_trade_position = if matches!(receipt.status.as_str(), "filled" | "partiallyFilled") {
        match ExecutionAdapter::new(plan.venue, plan.testnet).await {
            Ok(adapter) => {
                adapter
                    .account_snapshot(&plan.account)
                    .await
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .positions
                            .into_iter()
                            .find(|position| position.internal_symbol == plan.internal_symbol)
                    })
            }
            Err(_) => None,
        }
    } else {
        None
    };
    render_trade_result(&plan, &receipt, post_trade_position.as_ref(), args.output)
}

pub async fn handle_positions(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let venue = ExecutionVenue::from(args.venue);
    let account = ExecutionAdapter::configured_account(venue)?;
    let snapshot = ExecutionAdapter::new(venue, args.testnet)
        .await?
        .account_snapshot(&account)
        .await?;
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
    let symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let venue = ExecutionVenue::from(args.venue);
    let account = ExecutionAdapter::configured_account(venue)?;
    let orders = ExecutionAdapter::new(venue, args.testnet)
        .await?
        .open_orders(&account)
        .await?
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
    let symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let venue = ExecutionVenue::from(args.venue);
    let account = ExecutionAdapter::configured_account(venue)?;
    let fills = ExecutionAdapter::new(venue, args.testnet)
        .await?
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
    let venue = ExecutionVenue::from(args.venue);
    let market = execution_market(venue, &args.symbol)?;
    if venue == ExecutionVenue::Bulk {
        bulk_keychain::Hash::from_base58(&args.order_id).context("invalid BULK order id")?;
    } else {
        args.order_id
            .parse::<u64>()
            .context("Hyperliquid order id must be an unsigned integer")?;
    }
    let account = ExecutionAdapter::configured_account(venue)?;
    let plan = CancelPlan {
        created_at_ms: now_ms()?,
        venue,
        testnet: args.testnet,
        account,
        internal_symbol: market.symbol.clone(),
        venue_symbol: market.venue_symbol.clone(),
        order_id: args.order_id.clone(),
    };
    if args.dry_run {
        render_cancel_plan(&plan, true, args.output)?;
        return Ok(());
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_cancel_plan(&plan, false, args.output)?;
    }
    let prompt = format!(
        "Cancel this {} order?",
        venue_network_label(venue, args.testnet)
    );
    if !args.yes && !confirm_live_action(args.output, &prompt)? {
        println!("cancelled; the order was not changed");
        return Ok(());
    }
    let receipt = crate::runtime::submit_cancel(&plan).await?;
    render_cancel_result(&plan, &receipt, args.output)
}

pub async fn handle_close(args: ClosePositionArgs) -> Result<()> {
    args.validate()?;
    let requested_symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let venue = ExecutionVenue::from(args.venue);
    let account = ExecutionAdapter::configured_account(venue)?;
    let snapshot = ExecutionAdapter::new(venue, args.testnet)
        .await?
        .account_snapshot(&account)
        .await?;
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
    handle_trade(
        TradeArgs {
            symbol: position.internal_symbol,
            config: None,
            venue: args.venue,
            testnet: args.testnet,
            size: Some(position.size),
            margin: None,
            order_kind: TradeOrderKind::Market,
            price: None,
            tif: TradeTimeInForce::Gtc,
            leverage: position.leverage.max(1.0),
            reduce_only: true,
            sl: None,
            tp: None,
            dry_run: args.dry_run,
            yes: args.yes,
            output: args.output,
        },
        direction,
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
    let venue = ExecutionVenue::from(args.venue);
    let market = execution_market(venue, &args.symbol)?;
    validate_market_rules(venue, &market, args)?;
    let rules = market.execution_rules()?;
    let account = ExecutionAdapter::configured_account(venue)?;
    let reference_price = match args.order_kind {
        TradeOrderKind::Limit => args
            .price
            .context("--price is required with --type limit")?,
        TradeOrderKind::Market => match venue {
            ExecutionVenue::Bulk => BulkProvider::ticker(&market.symbol).await?.mark_price,
            ExecutionVenue::Hyperliquid => {
                HyperliquidProvider::ticker_on(
                    &market.symbol,
                    HyperliquidNetwork::from_testnet(args.testnet),
                )
                .await?
                .mark_price
            }
        },
    };
    if !reference_price.is_finite() || reference_price <= 0.0 {
        bail!(
            "{} returned an invalid reference price for {}",
            venue_label(venue),
            market.venue_symbol
        );
    }
    validate_protection_prices(venue, &market, args, direction, reference_price)?;

    let size = if let Some(size) = args.size {
        if !is_step_aligned(size, rules.lot_size) {
            bail!(
                "--size {size} is not aligned to {} lot size {} for {}",
                venue_label(venue),
                rules.lot_size,
                market.symbol
            );
        }
        round_to_precision(size, rules.size_precision)
    } else {
        let margin = args
            .margin
            .context("one of --size or --margin is required")?;
        let raw_size = exposure_from_margin(margin, args.leverage)? / reference_price;
        floor_to_step(raw_size, rules.lot_size, rules.size_precision)
    };
    if size <= 0.0 {
        bail!(
            "requested margin and leverage produce a size below {} lot size {} on {}",
            venue_label(venue),
            rules.lot_size,
            market.symbol
        );
    }
    let estimated_exposure = size * reference_price;
    let estimated_margin = estimated_exposure / args.leverage;
    if estimated_exposure + f64::EPSILON < rules.min_notional {
        bail!(
            "estimated exposure {estimated_exposure:.8} is below {} minimum notional {} for {}",
            venue_label(venue),
            rules.min_notional,
            market.symbol
        );
    }

    Ok(TradePlan {
        created_at_ms: now_ms()?,
        venue,
        testnet: args.testnet,
        account,
        internal_symbol: market.symbol.clone(),
        venue_symbol: market.venue_symbol.clone(),
        direction,
        side: direction.into(),
        order_kind: args.order_kind.into(),
        time_in_force: matches!(args.order_kind, TradeOrderKind::Limit)
            .then(|| TimeInForce::from(args.tif)),
        requested_size: args.size,
        size,
        price: args.price,
        reference_price,
        requested_margin: args.margin,
        estimated_margin,
        estimated_exposure,
        projected_liquidation_price: None,
        leverage: args.leverage,
        reduce_only: args.reduce_only,
        stop_loss_price: args.sl,
        take_profit_price: args.tp,
    })
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
                venue_label(venue),
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
    let rules = market.execution_rules()?;
    if !capabilities.order_kinds.contains(&args.order_kind.into()) {
        bail!(
            "{} execution adapter does not support this order type",
            venue_label(venue)
        );
    }
    if !market.is_available() {
        bail!(
            "{} market `{}` is not trading",
            venue_label(venue),
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
            venue_label(venue),
            market.venue_symbol
        );
    }
    if args.leverage > f64::from(rules.max_leverage) {
        bail!(
            "--leverage {} exceeds {} maximum {}x for {}",
            args.leverage,
            venue_label(venue),
            rules.max_leverage,
            market.symbol
        );
    }
    if venue == ExecutionVenue::Hyperliquid && args.leverage.fract().abs() > f64::EPSILON {
        bail!("Hyperliquid leverage must be a whole number");
    }
    if let Some(price) = args.price
        && !is_price_aligned(venue, price, rules)
    {
        bail!(
            "--price {price} is not aligned to {} price rules for {} (snapshot tick {})",
            venue_label(venue),
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
                venue_label(venue),
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
    match venue {
        ExecutionVenue::Bulk => is_step_aligned(price, rules.tick_size),
        ExecutionVenue::Hyperliquid => {
            crate::providers::hyperliquid::execution::validate_price(price, rules.size_precision)
                .is_ok()
        }
    }
}

fn validate_optional_symbol(symbol: Option<&str>) -> Result<Option<String>> {
    symbol.map(canonical_symbol).transpose()
}

fn execution_market(venue: ExecutionVenue, symbol: &str) -> Result<std::sync::Arc<Market>> {
    crate::markets::exchange_market(venue_key(venue), symbol)
}

fn venue_key(venue: ExecutionVenue) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "bulk",
        ExecutionVenue::Hyperliquid => "hyperliquid",
    }
}

fn venue_label(venue: ExecutionVenue) -> &'static str {
    match venue {
        ExecutionVenue::Bulk => "BULK",
        ExecutionVenue::Hyperliquid => "Hyperliquid",
    }
}

fn venue_network_label(venue: ExecutionVenue, testnet: bool) -> &'static str {
    match (venue, testnet) {
        (ExecutionVenue::Hyperliquid, true) => "Hyperliquid testnet",
        (ExecutionVenue::Hyperliquid, false) => "Hyperliquid mainnet",
        (ExecutionVenue::Bulk, _) => "BULK",
    }
}

fn canonical_symbol(symbol: &str) -> Result<String> {
    let normalized = symbol.trim().to_ascii_uppercase().replace('-', "/");
    let mut parts = normalized.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(base), Some(quote), None) if !base.is_empty() && !quote.is_empty() => {
            Ok(format!("{base}/{quote}"))
        }
        _ => bail!("symbol must look like BASE/QUOTE, e.g. BTC/USDT"),
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

fn render_trade_plan(plan: &TradePlan, dry_run: bool, output: OutputFormat) -> Result<()> {
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
            println!("  venue:             {}", venue_key(plan.venue));
            if plan.venue == ExecutionVenue::Hyperliquid {
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
            println!("  leverage:          {}x", plan.leverage);
            println!(
                "  liquidation price: determined by {} after fill",
                venue_label(plan.venue)
            );
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
            println!("  venue:    {}", venue_key(plan.venue));
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
}

#[derive(Serialize)]
struct CancelExecutionOutput<'a> {
    plan: &'a CancelPlan,
    receipt: &'a ExecutionReceipt,
}

fn render_trade_result(
    plan: &TradePlan,
    receipt: &ExecutionReceipt,
    post_trade_position: Option<&Position>,
    output: OutputFormat,
) -> Result<()> {
    let result = TradeExecutionOutput {
        plan,
        receipt,
        post_trade_position,
    };
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&result)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&result)?),
        OutputFormat::Terminal => {
            render_terminal_receipt(receipt);
            if let Some(position) = post_trade_position {
                println!("  position leverage: {}x", position.leverage);
                println!("  liquidation price: {}", position.liquidation_price);
            } else if matches!(receipt.status.as_str(), "filled" | "partiallyFilled") {
                println!(
                    "  liquidation price: not yet available from {}",
                    venue_label(plan.venue)
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
    println!("{}: order {}", venue_key(receipt.venue), receipt.status);
    if let Some(order_id) = &receipt.order_id {
        println!("  order id: {order_id}");
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
}
