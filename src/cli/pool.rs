use alloy_primitives::Address;
use clap::{Args, Subcommand};

use super::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum PoolCommands {
    /// Read a MarketLab pool's reserves, fees and permissions. No wallet required.
    Inspect(PoolInspectArgs),
}

#[derive(Debug, Args)]
pub struct PoolInspectArgs {
    pub address: Address,
    /// Override the RPC endpoint for the recorded Elysium deployment.
    #[arg(long)]
    pub rpc_url: Option<reqwest::Url>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}
