use anyhow::Result;

use crate::cli::{HealthArgs, OutputFormat};
use crate::domain::types::ProviderHealth;
use crate::providers::{MarketDataProvider, ProviderClient};

pub async fn handle(args: HealthArgs) -> Result<()> {
    let provider_kind = args.provider.into();
    let client = ProviderClient::from_kind(provider_kind);
    let health = client.health().await?;

    render(&health, args.output)
}

fn render(health: &ProviderHealth, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!(
                "provider={} status={} details={}",
                health.provider, health.status, health.details
            );
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(health)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(health)?),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO health export: {:?}", output);
        }
    }

    Ok(())
}
