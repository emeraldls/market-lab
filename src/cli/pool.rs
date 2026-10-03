use alloy_primitives::{Address, B256, U256};
use clap::{Args, Subcommand};

use super::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum PoolCommands {
    /// Prepare an unsigned pool-creation transaction for the manager's wallet.
    Create(PoolCreateArgs),
    /// Resolve a pool address from its confirmed factory creation receipt.
    Created(PoolCreatedArgs),
    /// Prepare token approvals and a liquidity deposit. No signing or broadcasting.
    Deposit(PoolDepositArgs),
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

#[derive(Debug, Args)]
pub struct PoolWalletArgs {
    /// Wallet that will sign; never the restricted fee operator.
    #[arg(long)]
    pub account: Address,
    #[arg(long)]
    pub rpc_url: Option<reqwest::Url>,
    #[command(flatten)]
    pub format: PoolOutputArgs,
}

#[derive(Debug, Args)]
pub struct PoolCreateArgs {
    #[arg(long)]
    pub token_a: Address,
    #[arg(long)]
    pub token_b: Address,
    #[arg(long, default_value_t = 30)]
    pub fee_bps: u16,
    #[arg(long, default_value_t = 5)]
    pub min_fee_bps: u16,
    #[arg(long, default_value_t = 100)]
    pub max_fee_bps: u16,
    #[command(flatten)]
    pub wallet: PoolWalletArgs,
}

#[derive(Debug, Args)]
pub struct PoolCreatedArgs {
    pub transaction_hash: B256,
    #[command(flatten)]
    pub wallet: PoolWalletArgs,
}

#[derive(Debug, Args)]
pub struct PoolDepositArgs {
    pub address: Address,
    /// Maximum token0 amount in integer base units; read token order with pool inspect.
    #[arg(long)]
    pub amount0: U256,
    /// Maximum token1 amount in integer base units.
    #[arg(long)]
    pub amount1: U256,
    #[arg(long, default_value_t = 50)]
    pub slippage_bps: u16,
    #[command(flatten)]
    pub wallet: PoolWalletArgs,
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
