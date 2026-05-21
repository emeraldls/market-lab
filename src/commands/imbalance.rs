use anyhow::{Result, bail};

use crate::cli::{ImbalanceArgs, OutputFormat};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::ImbalanceEstimate;
use crate::functions::imbalance::estimate_imbalance;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::realtime::{StreamRunConfig, run_mmt_realtime};

pub async fn handle(args: ImbalanceArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json");
        }

        eprintln!("note: MMT imbalance stream uses live depth websocket; --at is ignored");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| estimate_imbalance(snap, req.at, req.depth),
            |out| {
                format!(
                    "{} @ {} depth={} imbalance={:.6} bid_vol={} ask_vol={}",
                    out.symbol, out.at, out.depth, out.imbalance, out.bid_volume, out.ask_volume
                )
            },
        )
        .await;
    }

    let snapshot = match req.provider {
        ProviderKind::Mmt => {
            eprintln!(
                "note: MMT imbalance uses live /orderbook snapshot; --at is currently ignored"
            );
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

    let estimate = estimate_imbalance(&snapshot, req.at, req.depth)?;
    render(&estimate, args.output)
}

fn render(estimate: &ImbalanceEstimate, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} @ {} depth={} imbalance={:.6} bid_vol={} ask_vol={}",
                estimate.symbol,
                estimate.at,
                estimate.depth,
                estimate.imbalance,
                estimate.bid_volume,
                estimate.ask_volume
            );
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(estimate)?),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO imbalance export: {:?}", output);
        }
    }
    Ok(())
}
