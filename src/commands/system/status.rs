use anyhow::Result;

use crate::cli::{OutputFormat, StatusArgs};
use crate::domain::types::{ProviderHealth, SystemStatus};
use crate::providers::{MarketDataProvider, ProviderClient};

pub async fn handle(args: StatusArgs) -> Result<()> {
    let provider_kind = args.provider.into();
    let client = ProviderClient::from_kind(provider_kind);
    let provider_health = client.health().await?;

    let status = build_status(provider_health);
    render(&status, args.output)
}

fn build_status(provider_health: ProviderHealth) -> SystemStatus {
    SystemStatus {
        app: "market-lab".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        provider: provider_health.provider.clone(),
        command_groups: vec![
            "inspect".to_string(),
            "replay".to_string(),
            "source".to_string(),
            "script".to_string(),
            "study".to_string(),
            "strategy".to_string(),
            "auth".to_string(),
            "health".to_string(),
            "status".to_string(),
        ],
        sources: vec![
            "orderbook".to_string(),
            "candles".to_string(),
            "vd".to_string(),
            "oi".to_string(),
            "volumes".to_string(),
        ],
        studies: vec![
            "spread".to_string(),
            "depth".to_string(),
            "imbalance".to_string(),
            "slippage".to_string(),
            "vamp".to_string(),
            "cvd".to_string(),
        ],
        strategies: vec![
            "run sma-crossover".to_string(),
            "backtest sma-crossover".to_string(),
        ],
        provider_health,
    }
}

fn render(status: &SystemStatus, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Terminal => {
            println!("app={} version={}", status.app, status.version);
            println!(
                "provider={} health={}",
                status.provider, status.provider_health.status
            );
            println!("command_groups={}", status.command_groups.join(","));
            println!("sources={}", status.sources.join(","));
            println!("studies={}", status.studies.join(","));
            println!("strategies={}", status.strategies.join(","));
        }
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(status)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(status)?),
        OutputFormat::Csv | OutputFormat::Parquet => {
            println!("TODO status export: {:?}", output);
        }
    }

    Ok(())
}
