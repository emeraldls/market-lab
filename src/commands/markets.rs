use std::io::{self, IsTerminal};

use anyhow::{Context, Result, bail};
use dialoguer::{FuzzySelect, Select, theme::ColorfulTheme};

use crate::cli::{CliDataProvider, MarketsArgs};
use crate::markets::outcomes::{OutcomeInstrument, parse_symbol};
use crate::markets::{ExchangeMarkets, Market, MarketSnapshot};
use crate::providers::hyperliquid::{HyperliquidNetwork, SPOT_EXCHANGE};

pub async fn handle(args: MarketsArgs) -> Result<()> {
    args.validate()?;
    let outcome_request = args.provider.is_none()
        && args.exchange.eq_ignore_ascii_case(SPOT_EXCHANGE)
        && args.hip == Some(4);
    if outcome_request {
        return handle_outcomes(args).await;
    }
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

async fn handle_outcomes(args: MarketsArgs) -> Result<()> {
    if args.refresh {
        anyhow::bail!(
            "Hyperliquid outcomes are discovered live; --refresh is unnecessary and no static snapshot is written"
        );
    }
    let network = HyperliquidNetwork::from_testnet(args.testnet);
    let mut instruments = crate::markets::outcomes::instruments(network).await?;
    if let Some(deployer) = args.deployer.as_deref() {
        let deployer = deployer.trim();
        instruments.retain(|instrument| {
            instrument.deployer.as_ref().is_some_and(|candidate| {
                candidate.venue.eq_ignore_ascii_case(deployer)
                    || candidate.address.eq_ignore_ascii_case(deployer)
            })
        });
    }
    if let Some(symbol) = args.symbol.as_deref() {
        let (outcome, side) = parse_symbol(symbol)?;
        let selected = instruments
            .iter()
            .find(|instrument| instrument.outcome_id == outcome && instrument.side == side)
            .with_context(|| {
                format!(
                    "Hyperliquid {} outcome instrument `{symbol}` does not match the requested filters",
                    network.label()
                )
            })?;
        return print_outcome_instrument(selected, args.json);
    }
    if let Some(search) = args.search.as_deref() {
        let needle = search.trim().to_ascii_lowercase();
        instruments.retain(|instrument| outcome_search_text(instrument).contains(&needle));
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&instruments)?);
        return Ok(());
    }

    println!(
        "{} instruments (Hyperliquid {} live outcomeMeta)",
        instruments.len(),
        network.label()
    );
    println!();
    println!(
        "{:<12} {:<10} {:<10} {:<46} {:<22} {:<16} {:<8}",
        "SYMBOL", "DEPLOYER", "QUESTION", "QUESTION NAME", "OUTCOME", "SIDE", "QUOTE"
    );
    for instrument in &instruments {
        println!(
            "{:<12} {:<10} {:<10} {:<46} {:<22} {:<16} {:<8}",
            instrument.symbol,
            instrument
                .deployer
                .as_ref()
                .map_or("-", |deployer| deployer.venue.as_str()),
            instrument
                .question_id
                .map_or_else(|| "-".to_string(), |id| id.to_string()),
            truncate(
                &clean_terminal_text(instrument.question_name.as_deref().unwrap_or("Standalone")),
                46
            ),
            truncate(&clean_terminal_text(&instrument.outcome_name), 22),
            truncate(&clean_terminal_text(&instrument.side_name), 16),
            instrument.quote_token,
        );
    }
    Ok(())
}

fn print_outcome_instrument(instrument: &OutcomeInstrument, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(instrument)?);
        return Ok(());
    }
    println!("Hyperliquid outcome instrument");
    println!("  network:       {}", instrument.network);
    println!("  symbol:        {}", instrument.symbol);
    if let Some(deployer) = &instrument.deployer {
        println!("  deployer:      {} ({})", deployer.venue, deployer.address);
    }
    if let Some(template) = &instrument.template {
        println!("  template:      {template}");
    }
    println!(
        "  question:      {}",
        instrument
            .question_id
            .map_or_else(|| "standalone".to_string(), |id| id.to_string())
    );
    if let Some(name) = &instrument.question_name {
        println!("  question name: {}", clean_terminal_text(name));
    }
    println!(
        "  outcome:       {} ({})",
        instrument.outcome_id,
        clean_terminal_text(&instrument.outcome_name)
    );
    println!(
        "  side:          {} ({})",
        instrument.side,
        clean_terminal_text(&instrument.side_name)
    );
    println!("  quote:         {}", instrument.quote_token);
    println!("  market coin:   {}", instrument.coin);
    println!("  action asset:  {}", instrument.asset_id);
    println!(
        "  status:        {}",
        if instrument.settled {
            "settled"
        } else {
            "available"
        }
    );
    Ok(())
}

fn outcome_search_text(instrument: &OutcomeInstrument) -> String {
    [
        instrument.symbol.clone(),
        instrument
            .deployer
            .as_ref()
            .map_or_else(String::new, |deployer| deployer.venue.clone()),
        instrument
            .deployer
            .as_ref()
            .map_or_else(String::new, |deployer| deployer.address.clone()),
        instrument
            .question_id
            .map_or_else(String::new, |id| id.to_string()),
        instrument.question_name.clone().unwrap_or_default(),
        instrument.outcome_name.clone(),
        instrument.template.clone().unwrap_or_default(),
        instrument.side_name.clone(),
    ]
    .join(" ")
    .to_ascii_lowercase()
}

pub async fn select_outcome_interactive(network: HyperliquidNetwork) -> Result<OutcomeInstrument> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        bail!("outcome selection needs an interactive terminal; pass a symbol such as `1001:0`");
    }
    let instruments = crate::markets::outcomes::instruments(network).await?;
    let mut outcomes = Vec::<OutcomeInstrument>::new();
    let mut seen = std::collections::HashSet::<u32>::new();
    for instrument in instruments.iter().filter(|instrument| !instrument.settled) {
        if seen.insert(instrument.outcome_id) {
            outcomes.push(instrument.clone());
        }
    }
    if outcomes.is_empty() {
        bail!(
            "Hyperliquid {} currently exposes no active outcomes",
            network.label()
        );
    }
    let labels = outcomes
        .iter()
        .map(|instrument| {
            let question = instrument.question_name.as_deref().unwrap_or("Standalone");
            format!(
                "{} — {} [outcome {}]",
                clean_terminal_text(question),
                clean_terminal_text(&instrument.outcome_name),
                instrument.outcome_id
            )
        })
        .collect::<Vec<_>>();
    let theme = ColorfulTheme::default();
    let selected = FuzzySelect::with_theme(&theme)
        .with_prompt("Search or select an outcome")
        .items(&labels)
        .default(0)
        .interact_opt()?
        .context("outcome selection was cancelled")?;
    let outcome = &outcomes[selected];
    let matching = instruments
        .into_iter()
        .filter(|instrument| instrument.outcome_id == outcome.outcome_id)
        .collect::<Vec<_>>();
    let labels = matching
        .iter()
        .map(|instrument| {
            format!(
                "{} (side {})",
                clean_terminal_text(&instrument.side_name),
                instrument.side
            )
        })
        .collect::<Vec<_>>();
    let selected = Select::with_theme(&theme)
        .with_prompt(format!(
            "Select a side for {}",
            clean_terminal_text(&outcome.outcome_name)
        ))
        .items(&labels)
        .default(0)
        .interact_opt()?
        .context("outcome side selection was cancelled")?;
    Ok(matching[selected].clone())
}

fn clean_terminal_text(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .collect()
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "…"
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
