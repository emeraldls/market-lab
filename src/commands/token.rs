use std::time::Duration;

use anyhow::{Context, Result, ensure};

use crate::cli::{OutputFormat, TokenCommands};
use crate::providers::elysium::PoolClient;

pub async fn handle(command: TokenCommands) -> Result<()> {
    let format = match &command {
        TokenCommands::Create(args) => args.wallet.format.output,
        TokenCommands::Created(args) => args.wallet.format.output,
    };
    ensure!(
        matches!(format, OutputFormat::Json | OutputFormat::Terminal),
        "token commands support terminal or json output"
    );
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        match command {
            TokenCommands::Create(args) => {
                PoolClient::new(args.wallet.rpc_url)?
                    .prepare_token(args.wallet.account, &args.name, &args.symbol, &args.supply)
                    .await
            }
            TokenCommands::Created(args) => {
                PoolClient::new(args.wallet.rpc_url)?
                    .created_token(args.transaction_hash, args.wallet.account)
                    .await
            }
        }
    })
    .await
    .context("token request timed out")??;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
