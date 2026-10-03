use alloy_primitives::Address;
use clap::{Args, Subcommand};

use super::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum PoolCommands {
    /// Read a MarketLab pool's reserves, fees and permissions. No wallet required.
    Inspect(PoolInspectArgs),
    /// Generate or inspect the dedicated fee operator. Never prints its private key.
    Operator {
        #[command(subcommand)]
        command: PoolOperatorCommands,
    },
    /// Run the adaptive-fee strategy in mlabd.
    Run(PoolRunArgs),
    /// List persistent pool jobs and any pending transaction.
    Jobs(PoolOutputArgs),
    /// Stop new fee decisions. An already prepared transaction may still settle.
    Stop {
        job_id: String,
        #[command(flatten)]
        format: PoolOutputArgs,
    },
    /// Show recent observations, transaction hashes and receipts.
    Logs {
        job_id: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[command(flatten)]
        format: PoolOutputArgs,
    },
}

#[derive(Debug, Subcommand)]
pub enum PoolOperatorCommands {
    Create,
    Address,
}

#[derive(Debug, Args)]
pub struct PoolOutputArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
}

#[derive(Debug, Args)]
pub struct PoolRunArgs {
    pub address: Address,
    #[arg(long)]
    pub rpc_url: Option<reqwest::Url>,
    #[command(flatten)]
    pub policy: crate::runtime::pools::FeePolicy,
    /// Inspect and print the policy without starting mlabd or signing.
    #[arg(long)]
    pub dry_run: bool,
    /// Skip the confirmation before starting automatic fee changes.
    #[arg(long)]
    pub yes: bool,
    #[command(flatten)]
    pub format: PoolOutputArgs,
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
