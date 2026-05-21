use anyhow::Result;

use crate::cli::{OutputFormat, ReplayArgs};
use crate::domain::types::TopOfBook;
use crate::providers::{MarketDataProvider, ProviderClient};

pub async fn handle(args: ReplayArgs) -> Result<()> {
    args.validate()?;
    let req = args.to_request();

    let client = ProviderClient::from_kind(req.provider);
    let events = client.replay(&req).await?;

    render(&events, args.output)
}

fn render(events: &[TopOfBook], output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            for event in events {
                println!(
                    "ts={} bid={:?} ask={:?}",
                    event.timestamp_ms, event.best_bid, event.best_ask
                );
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(events)?),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO replay export: {:?}", output);
        }
    }
    Ok(())
}
