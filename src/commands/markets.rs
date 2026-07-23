use anyhow::Result;

use crate::cli::{CliDataProvider, MarketsArgs};
use crate::markets::{ExchangeMarkets, Market, MarketSnapshot};

pub async fn handle(args: MarketsArgs) -> Result<()> {
    args.validate()?;
    if args.refresh {
        let provider = args.provider.map(|provider| match provider {
            CliDataProvider::Mmt => "mmt",
        });
        crate::markets::refresh_route(provider, &args.exchange).await?;
        crate::runtime::reload_markets_if_running().await?;
    }

    let (snapshot, exchange) = match args.provider {
        Some(CliDataProvider::Mmt) => crate::markets::provider_exchange("mmt", &args.exchange)?,
        None => crate::markets::direct_exchange(&args.exchange)?,
    };

    if let Some(symbol) = &args.symbol {
        let market = match args.provider {
            Some(CliDataProvider::Mmt) => {
                crate::markets::provider_market("mmt", &args.exchange, symbol)?
            }
            None => crate::markets::exchange_market(&args.exchange, symbol)?,
        };
        return print_market(&snapshot, &exchange, &market, args.json);
    }

    print_exchange(&snapshot, &exchange, args.json)
}

fn print_market(
    snapshot: &MarketSnapshot,
    exchange: &ExchangeMarkets,
    market: &Market,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(market)?);
        return Ok(());
    }

    println!(
        "{} market ({} local snapshot)",
        exchange.exchange, snapshot.provider
    );
    println!("  market type:      {}", exchange.market_type.as_str());
    println!("  symbol:           {}", market.symbol);
    println!("  provider symbol:  {}", market.provider_symbol);
    println!("  venue symbol:     {}", market.venue_symbol);
    println!("  status:           {}", market.status);
    println!(
        "  base / quote:     {} / {}",
        market.base_asset, market.quote_asset
    );
    if let Some(increment) = market.price_increment {
        println!("  price increment:  {increment}");
    }
    if let Some(increment) = market.size_increment {
        println!("  size increment:   {increment}");
    }
    if let Some(rules) = &market.execution {
        println!("  execution:        yes");
        println!("  tick size:        {}", rules.tick_size);
        println!("  lot size:         {}", rules.lot_size);
        println!("  min notional:     {}", rules.min_notional);
        println!("  max leverage:     {}x", rules.max_leverage);
        println!("  price precision:  {}", rules.price_precision);
        println!("  size precision:   {}", rules.size_precision);
        println!("  order types:      {}", rules.order_types.join(", "));
        println!("  time in force:    {}", rules.time_in_forces.join(", "));
    } else {
        println!("  execution:        no (market data only)");
    }
    Ok(())
}

fn print_exchange(snapshot: &MarketSnapshot, exchange: &ExchangeMarkets, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(exchange)?);
        return Ok(());
    }

    let available = exchange
        .markets
        .iter()
        .filter(|market| market.is_available())
        .count();
    let executable = exchange
        .markets
        .iter()
        .filter(|market| market.execution.is_some())
        .count();
    println!(
        "{} markets ({} local snapshot)",
        exchange.exchange, snapshot.provider
    );
    println!("  fetched:    {}", snapshot.fetched_at);
    println!("  source:     {}", snapshot.source_url);
    println!("  type:       {}", exchange.market_type.as_str());
    println!(
        "  markets:    {} ({available} available, {executable} executable)",
        exchange.markets.len()
    );
    println!();
    println!(
        "{:<18} {:<20} {:<12} {:>14} {:>14}",
        "SYMBOL", "PROVIDER SYMBOL", "STATUS", "SIZE STEP", "EXECUTION"
    );
    for market in &exchange.markets {
        let size_step = market
            .size_increment
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<18} {:<20} {:<12} {:>14} {:>14}",
            market.symbol,
            market.provider_symbol,
            market.status,
            size_step,
            if market.execution.is_some() {
                "yes"
            } else {
                "no"
            }
        );
    }
    Ok(())
}
