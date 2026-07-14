use anyhow::Result;

use crate::cli::{MarketCatalogProvider, MarketsArgs};
use crate::providers::bulk::catalog;

pub fn handle(args: MarketsArgs) -> Result<()> {
    match args.provider {
        MarketCatalogProvider::Bulk => print_bulk_catalog(&args),
    }
}

fn print_bulk_catalog(args: &MarketsArgs) -> Result<()> {
    if let Some(symbol) = &args.symbol {
        let market = catalog::market(symbol)?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(market)?);
        } else {
            println!("bulk market (local snapshot)");
            println!("  venue symbol:    {}", market.symbol);
            println!("  internal symbol: {}", market.internal_symbol);
            println!("  status:          {}", market.status);
            println!("  base:            {}", market.base_asset);
            println!("  quote:           {}", market.quote_asset);
            println!("  tick size:       {}", market.tick_size);
            println!("  lot size:        {}", market.lot_size);
            println!("  min notional:    {}", market.min_notional);
            println!("  max leverage:    {}x", market.max_leverage);
            println!("  price precision: {}", market.price_precision);
            println!("  size precision:  {}", market.size_precision);
            println!(
                "  market orders:   {}",
                yes_no(market.supports_order_type("MARKET"))
            );
            println!("  order types:     {}", market.order_types.join(", "));
            println!("  time in force:   {}", market.time_in_forces.join(", "));
        }
        return Ok(());
    }

    let catalog = catalog::market_catalog()?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(catalog)?);
        return Ok(());
    }

    let trading = catalog
        .markets
        .iter()
        .filter(|market| market.is_trading())
        .count();
    println!("bulk markets (local snapshot)");
    println!("  fetched: {}", catalog.fetched_at);
    println!("  source:  {}", catalog.source_url);
    println!("  markets: {} ({trading} trading)", catalog.markets.len());
    println!();
    println!(
        "{:<16} {:<18} {:<10} {:>12} {:>14} {:>10}",
        "BULK SYMBOL", "INTERNAL SYMBOL", "STATUS", "LOT SIZE", "MIN NOTIONAL", "MAX LEV"
    );
    for market in &catalog.markets {
        println!(
            "{:<16} {:<18} {:<10} {:>12} {:>14.2} {:>9}x",
            market.symbol,
            market.internal_symbol,
            market.status,
            market.lot_size,
            market.min_notional,
            market.max_leverage
        );
    }
    Ok(())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
