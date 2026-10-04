use alloy_primitives::B256;
use clap::{Args, Subcommand};

use super::pool::PoolWalletArgs;

#[derive(Debug, Subcommand)]
pub enum TokenCommands {
    /// Prepare a fixed-supply test-token deployment on Elysium. Never signs or broadcasts.
    Create(TokenCreateArgs),
    /// Verify a confirmed token deployment and read its address and supply.
    Created(TokenCreatedArgs),
}

#[derive(Debug, Args)]
pub struct TokenCreateArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub symbol: String,
    /// Total supply in whole-token units, with up to 18 decimal places.
    #[arg(long)]
    pub supply: String,
    #[command(flatten)]
    pub wallet: PoolWalletArgs,
}

#[derive(Debug, Args)]
pub struct TokenCreatedArgs {
    pub transaction_hash: B256,
    #[command(flatten)]
    pub wallet: PoolWalletArgs,
}
