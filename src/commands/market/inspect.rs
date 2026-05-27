use anyhow::Result;

use crate::cli::{InspectArgs, OutputFormat};
use crate::domain::types::OrderBookSnapshot;
use crate::providers::{MarketDataProvider, ProviderClient};

pub async fn handle(args: InspectArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    let client = ProviderClient::from_kind(req.provider);
    let snapshot = client.inspect(&req).await?;

    render(&snapshot, args.output)
}

fn render(snapshot: &OrderBookSnapshot, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "{} @ {} ({} bids / {} asks)",
                snapshot.symbol,
                snapshot.timestamp_ms,
                snapshot.bids.len(),
                snapshot.asks.len()
            );
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(snapshot)?);
        }
        OutputFormat::Jsonl => {
            println!("{}", serde_json::to_string(snapshot)?);
        }
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO inspect export: {:?}", output);
        }
    }

    Ok(())
}
