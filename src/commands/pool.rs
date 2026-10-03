use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cli::{OutputFormat, PoolInspectArgs};
use crate::providers::elysium::PoolClient;

pub async fn handle_inspect(args: PoolInspectArgs) -> Result<()> {
    if matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("pool inspection supports --output terminal, json or jsonl");
    }
    let client = PoolClient::new(args.rpc_url)?;
    let pool = tokio::time::timeout(Duration::from_secs(60), client.inspect(args.address))
        .await
        .context("pool inspection timed out")??;
    match args.output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&pool)?),
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(&pool)?),
        OutputFormat::Terminal => {
            println!("pool {}", pool.address);
            println!("network: Elysium (chain {})", pool.chain_id);
            println!("block:   {} ({})", pool.block_number, pool.block_hash);
            println!("factory: {}", pool.factory);
            println!("\nreserves");
            for reserve in &pool.reserves {
                println!(
                    "  {} {} ({})",
                    reserve.amount.formatted,
                    reserve.symbol.escape_default(),
                    reserve.address
                );
            }
            println!(
                "\nfee:     {} bps ({}.{:02}%)",
                pool.fee_bps,
                pool.fee_bps / 100,
                pool.fee_bps % 100
            );
            println!("bounds:  {}-{} bps", pool.min_fee_bps, pool.max_fee_bps);
            println!("manager: {}", pool.manager);
            if let Some(pending) = pool.pending_manager {
                println!("pending manager: {pending}");
            }
            match pool.operator {
                Some(operator) => println!("operator: {operator}"),
                None => println!("operator: disabled"),
            }
            if let Some(balance) = &pool.operator_hype_balance {
                println!("operator gas balance: {} HYPE", balance.formatted);
            }
            println!(
                "LP supply: {} ({} base units)",
                pool.lp_supply.formatted, pool.lp_supply.raw
            );
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!("validated output"),
    }
    Ok(())
}
