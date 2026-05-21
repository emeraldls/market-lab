use anyhow::{Result, bail};

use crate::cli::{OutputFormat, VampArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::VampEstimate;
use crate::functions::vamp::estimate_vamp;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::realtime::{StreamRunConfig, run_mmt_realtime};

pub async fn handle(args: VampArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json");
        }

        eprintln!("note: MMT vamp stream uses live depth websocket; --at is ignored");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| estimate_vamp(snap, req.at, req.dollar_depth),
            |out| {
                format!(
                    "{} @ {} depth=${}: vamp={} (bid_vwap={}, ask_vwap={}) complete={} max_bid_quote={} max_ask_quote={}",
                    out.symbol,
                    out.at,
                    out.dollar_depth,
                    out.vamp,
                    out.bid_vwap,
                    out.ask_vwap,
                    out.complete,
                    out.max_reachable_quote_bid,
                    out.max_reachable_quote_ask,
                )
            },
        )
        .await;
    }

    let snapshot = match req.provider {
        ProviderKind::Mmt => {
            eprintln!("note: MMT vamp uses live /orderbook snapshot; --at is currently ignored");
            MmtProvider::live_orderbook(&req.exchange, &req.symbol, req.depth).await?
        }
        _ => {
            let client = ProviderClient::from_kind(req.provider);
            let inspect_req = InspectRequest {
                provider: req.provider,
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                at: req.at,
                depth: req.depth,
                book_mode: req.book_mode,
            };
            client.inspect(&inspect_req).await?
        }
    };

    let out = estimate_vamp(&snapshot, req.at, req.dollar_depth)?;
    render(&out, args.output)
}

fn render(out: &VampEstimate, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} @ {} depth=${}: vamp={} (bid_vwap={}, ask_vwap={}) complete={} max_bid_quote={} max_ask_quote={}",
                out.symbol,
                out.at,
                out.dollar_depth,
                out.vamp,
                out.bid_vwap,
                out.ask_vwap,
                out.complete,
                out.max_reachable_quote_bid,
                out.max_reachable_quote_ask,
            );
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(out)?),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO vamp export: {:?}", output);
        }
    }

    Ok(())
}
