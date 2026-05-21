use anyhow::{Result, bail};

use crate::cli::{OutputFormat, SlippageArgs};
use crate::domain::enums::ProviderKind;
use crate::domain::requests::InspectRequest;
use crate::domain::types::SlippageEstimate;
use crate::functions::slippage::estimate_slippage;
use crate::providers::mmt::MmtProvider;
use crate::providers::{MarketDataProvider, ProviderClient};

use super::realtime::{StreamRunConfig, run_mmt_realtime};

pub async fn handle(args: SlippageArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    if req.stream {
        if !matches!(req.provider, ProviderKind::Mmt) {
            bail!("--stream is currently supported only with --provider mmt");
        }
        if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
            bail!("stream mode currently supports only --output terminal|json");
        }

        eprintln!("note: MMT slippage stream uses live depth websocket; --at is ignored");
        return run_mmt_realtime(
            StreamRunConfig {
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                depth: req.depth,
                buffer_size: req.buffer_size,
                output: args.output,
            },
            move |snap| estimate_slippage(snap, req.notional, req.at, req.side),
            |out| {
                format!(
                    "{} {} @ {}: avg_fill={} slippage_bps={}",
                    out.symbol, out.side, out.at, out.avg_fill_price, out.slippage_bps
                )
            },
        )
        .await;
    }

    let snapshot = match req.provider {
        ProviderKind::Mmt => {
            eprintln!(
                "note: MMT slippage uses live /orderbook snapshot; --at is currently ignored"
            );
            MmtProvider::live_orderbook(&req.exchange, &req.symbol, req.depth).await?
        }
        _ => {
            let client = ProviderClient::from_kind(req.provider);
            let snapshot_req = InspectRequest {
                provider: req.provider,
                exchange: req.exchange.clone(),
                symbol: req.symbol.clone(),
                at: req.at,
                depth: req.depth,
                book_mode: req.book_mode,
            };
            client.inspect(&snapshot_req).await?
        }
    };

    let estimate = estimate_slippage(&snapshot, req.notional, req.at, req.side)?;

    render(&estimate, args.output)
}

fn render(estimate: &SlippageEstimate, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} {} @ {}: avg_fill={} slippage_bps={}",
                estimate.symbol,
                estimate.side,
                estimate.at,
                estimate.avg_fill_price,
                estimate.slippage_bps
            );
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(estimate)?),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO slippage export: {:?}", output);
        }
    }

    Ok(())
}
