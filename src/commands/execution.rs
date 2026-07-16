use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::cli::{
    AccountQueryArgs, CancelOrderArgs, ClosePositionArgs, ExecutionVenueArg, OutputFormat,
    TradeArgs, TradeOrderKind, TradeTimeInForce,
};
use crate::credentials;
use crate::domain::execution::{
    CancelPlan, ExecutionReceipt, ExecutionVenue, MarketRules, OpenOrder, Position,
    PositionDirection, TimeInForce, TradePlan,
};
use crate::providers::bulk::catalog::{self, BulkMarket};
use crate::providers::bulk::execution::BulkExecutionAdapter;
use crate::providers::bulk::market_data::BulkProvider;

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
        print!("Submit this order to BULK? [y/N]: ");
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
    render_trade_result(&plan, &receipt, args.output)
}

pub async fn handle_positions(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let account = credentials::bulk_account()?;
    let snapshot = match args.venue {
        ExecutionVenueArg::Bulk => {
            BulkExecutionAdapter::new()?
                .account_snapshot(&account)
                .await?
        }
    };
    let positions = snapshot
        .positions
        .into_iter()
        .filter(|position| symbol.is_none_or(|symbol| position.internal_symbol == symbol))
        .collect::<Vec<_>>();
    render_positions(&positions, args.output)
}

pub async fn handle_orders(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let account = credentials::bulk_account()?;
    let orders = match args.venue {
        ExecutionVenueArg::Bulk => BulkExecutionAdapter::new()?.open_orders(&account).await?,
    }
    .into_iter()
    .filter(|order| symbol.is_none_or(|symbol| order.internal_symbol == symbol))
    .collect::<Vec<_>>();
    render_orders(&orders, args.output)
}

pub async fn handle_fills(args: AccountQueryArgs) -> Result<()> {
    args.validate()?;
    let symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let account = credentials::bulk_account()?;
    let fills = match args.venue {
        ExecutionVenueArg::Bulk => BulkExecutionAdapter::new()?.fills(&account).await?,
    }
    .into_iter()
    .filter(|fill| symbol.is_none_or(|symbol| fill.internal_symbol == symbol))
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
    let market = catalog::market(&args.symbol)?;
    bulk_keychain::Hash::from_base58(&args.order_id).context("invalid BULK order id")?;
    let account = credentials::bulk_account()?;
    let plan = CancelPlan {
        created_at_ms: now_ms()?,
        venue: ExecutionVenue::Bulk,
        account,
        internal_symbol: market.internal_symbol.clone(),
        venue_symbol: market.symbol.clone(),
        order_id: args.order_id.clone(),
    };
    if args.dry_run {
        render_cancel_plan(&plan, true, args.output)?;
        return Ok(());
    }
    if matches!(args.output, OutputFormat::Terminal) {
        render_cancel_plan(&plan, false, args.output)?;
    }
    if !args.yes && !confirm_live_action(args.output, "Cancel this BULK order?")? {
        println!("cancelled; the order was not changed");
        return Ok(());
    }
    let receipt = crate::runtime::submit_cancel(&plan).await?;
    render_cancel_result(&plan, &receipt, args.output)
}

pub async fn handle_close(args: ClosePositionArgs) -> Result<()> {
    args.validate()?;
    let requested_symbol = validate_optional_symbol(args.symbol.as_deref())?;
    let account = credentials::bulk_account()?;
    let snapshot = match args.venue {
        ExecutionVenueArg::Bulk => {
            BulkExecutionAdapter::new()?
                .account_snapshot(&account)
                .await?
        }
    };
    let positions = snapshot
        .positions
        .into_iter()
        .filter(|position| requested_symbol.is_none_or(|symbol| position.internal_symbol == symbol))
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
            size: Some(position.size),
            notional: None,
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
        bail!("BULK returned multiple open positions for the selected symbol");
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
    let market = catalog::market(&args.symbol)?;
    validate_market_rules(market, args)?;
    let account = match args.venue {
        ExecutionVenueArg::Bulk => credentials::bulk_account()?,
    };
    let reference_price = match args.order_kind {
        TradeOrderKind::Limit => args
            .price
            .context("--price is required with --type limit")?,
        TradeOrderKind::Market => {
            BulkProvider::ticker(&market.internal_symbol)
                .await?
                .mark_price
        }
    };
    if !reference_price.is_finite() || reference_price <= 0.0 {
        bail!(
            "BULK returned an invalid reference price for {}",
            market.symbol
        );
    }
    validate_protection_prices(market, args, direction, reference_price)?;

    let size = if let Some(size) = args.size {
        if !is_step_aligned(size, market.lot_size) {
            bail!(
                "--size {size} is not aligned to BULK lot size {} for {}",
                market.lot_size,
                market.internal_symbol
            );
        }
        round_to_precision(size, market.size_precision)
    } else {
        let raw_size = args
            .notional
            .context("one of --size or --notional is required")?
            / reference_price;
        floor_to_step(raw_size, market.lot_size, market.size_precision)
    };
    if size <= 0.0 {
        bail!(
            "requested notional is too small for BULK lot size {} on {}",
            market.lot_size,
            market.internal_symbol
        );
    }
    let estimated_notional = size * reference_price;
    if estimated_notional + f64::EPSILON < market.min_notional {
        bail!(
            "estimated notional {estimated_notional:.8} is below BULK minimum {} for {}",
            market.min_notional,
            market.internal_symbol
        );
    }

    Ok(TradePlan {
        created_at_ms: now_ms()?,
        venue: ExecutionVenue::Bulk,
        account,
        internal_symbol: market.internal_symbol.clone(),
        venue_symbol: market.symbol.clone(),
        direction,
        side: direction.into(),
        order_kind: args.order_kind.into(),
        time_in_force: matches!(args.order_kind, TradeOrderKind::Limit)
            .then(|| TimeInForce::from(args.tif)),
        requested_size: args.size,
        requested_notional: args.notional,
        size,
        price: args.price,
        reference_price,
        estimated_notional,
        leverage: args.leverage,
        reduce_only: args.reduce_only,
        stop_loss_price: args.sl,
        take_profit_price: args.tp,
        rules: MarketRules {
            tick_size: market.tick_size,
            lot_size: market.lot_size,
            min_notional: market.min_notional,
            max_leverage: market.max_leverage,
        },
    })
}

fn validate_protection_prices(
    market: &BulkMarket,
    args: &TradeArgs,
    direction: PositionDirection,
    entry_price: f64,
) -> Result<()> {
    for (flag, price) in [("--sl", args.sl), ("--tp", args.tp)] {
        if let Some(price) = price
            && !is_step_aligned(price, market.tick_size)
        {
            bail!(
                "{flag} {price} is not aligned to BULK tick size {} for {}",
                market.tick_size,
                market.internal_symbol
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

fn validate_market_rules(market: &BulkMarket, args: &TradeArgs) -> Result<()> {
    let capabilities = BulkExecutionAdapter::capabilities();
    if !capabilities.order_kinds.contains(&args.order_kind.into()) {
        bail!("BULK execution adapter does not support this order type");
    }
    if !market.is_trading() {
        bail!("BULK market `{}` is not trading", market.symbol);
    }
    let order_type = match args.order_kind {
        TradeOrderKind::Market => "MARKET",
        TradeOrderKind::Limit => "LIMIT",
    };
    if !market.supports_order_type(order_type) {
        bail!(
            "BULK market `{}` does not support {order_type} orders",
            market.symbol
        );
    }
    if args.leverage > f64::from(market.max_leverage) {
        bail!(
            "--leverage {} exceeds BULK maximum {}x for {}",
            args.leverage,
            market.max_leverage,
            market.internal_symbol
        );
    }
    if let Some(price) = args.price
        && !is_step_aligned(price, market.tick_size)
    {
        bail!(
            "--price {price} is not aligned to BULK tick size {} for {}",
            market.tick_size,
            market.internal_symbol
        );
    }
    if matches!(args.order_kind, TradeOrderKind::Limit) {
        let tif = match args.tif {
            TradeTimeInForce::Gtc => "GTC",
            TradeTimeInForce::Ioc => "IOC",
            TradeTimeInForce::Alo => "ALO",
        };
        if !market
            .time_in_forces
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(tif))
        {
            bail!("BULK market `{}` does not support TIF {tif}", market.symbol);
        }
    }
    Ok(())
}

fn validate_optional_symbol(symbol: Option<&str>) -> Result<Option<&'static str>> {
    symbol
        .map(|symbol| catalog::market(symbol).map(|market| market.internal_symbol.as_str()))
        .transpose()
}

fn is_step_aligned(value: f64, step: f64) -> bool {
    let units = value / step;
    (units - units.round()).abs() <= 1e-8_f64.max(units.abs() * 1e-12)
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
            println!("  venue:             bulk");
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
            println!("  est. notional:     {:.8}", plan.estimated_notional);
            println!("  leverage:          {}x", plan.leverage);
            println!("  reduce only:       {}", plan.reduce_only);
            if let Some(price) = plan.stop_loss_price {
                println!("  stop loss:         {price} (native on-fill trigger)");
            }
            if let Some(price) = plan.take_profit_price {
                println!("  take profit:       {price} (native on-fill trigger)");
            }
            println!(
                "  rules:              tick={} lot={} min={} max_leverage={}x",
                plan.rules.tick_size,
                plan.rules.lot_size,
                plan.rules.min_notional,
                plan.rules.max_leverage
            );
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
            println!("  venue:    bulk");
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
}

#[derive(Serialize)]
struct CancelExecutionOutput<'a> {
    plan: &'a CancelPlan,
    receipt: &'a ExecutionReceipt,
}

fn render_trade_result(
    plan: &TradePlan,
    receipt: &ExecutionReceipt,
    output: OutputFormat,
) -> Result<()> {
    let result = TradeExecutionOutput { plan, receipt };
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&result)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&result)?),
        OutputFormat::Terminal => render_terminal_receipt(receipt),
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
    println!("bulk: order {}", receipt.status);
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
    fn floors_notional_size_to_lot_size() {
        assert_eq!(floor_to_step(0.0012349, 0.000001, 6), 0.001234);
    }

    #[test]
    fn formats_terminal_numbers_without_noisy_zeroes() {
        assert_eq!(format_decimal(64_771.7, 2), "64771.7");
        assert_eq!(format_decimal(0.000154, 8), "0.000154");
        assert_eq!(format_decimal(-0.004928, 2), "0");
        assert_eq!(format_decimal(9.9699138, 2), "9.97");
    }
}
