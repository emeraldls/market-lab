use alloy_primitives::{Address, B256, U256};
use clap::{Args, Subcommand};

use super::OutputFormat;

#[derive(Debug, Subcommand)]
pub enum PoolCommands {
    /// List pools registered with the factory, newest first.
    List(PoolListArgs),
    /// Prepare an unsigned pool-creation transaction for the manager's wallet.
    Create(PoolCreateArgs),
    /// Resolve a pool address from its confirmed factory creation receipt.
    Created(PoolCreatedArgs),
    /// Prepare token approvals and a liquidity deposit. No signing or broadcasting.
    Deposit(PoolDepositArgs),
    /// Read wallet balances and its share of a pool.
    Position(PoolAccountArgs),
    /// Prepare a swap and any required token approval. No signing or broadcasting.
    Swap(PoolSwapArgs),
    /// Prepare a withdrawal of LP shares. No signing or broadcasting.
    Withdraw(PoolWithdrawArgs),
    /// Prepare manager approval or revocation of the fee operator.
    Authorize(PoolAuthorizeArgs),
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
pub struct PoolListArgs {
    #[arg(long, default_value_t = 0)]
    pub offset: u64,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=50))]
    pub limit: u64,
    #[arg(long)]
    pub rpc_url: Option<reqwest::Url>,
    #[command(flatten)]
    pub format: PoolOutputArgs,
}

#[derive(Debug, Args)]
pub struct PoolAccountArgs {
    pub address: Address,
    #[command(flatten)]
    pub wallet: PoolWalletArgs,
}

#[derive(Debug, Args)]
pub struct PoolSwapArgs {
    #[command(flatten)]
    pub pool: PoolAccountArgs,
    #[arg(long)]
    pub token_in: Address,
    /// Input amount in integer token base units.
    #[arg(long)]
    pub amount_in: U256,
    #[arg(long, default_value_t = 50)]
    pub slippage_bps: u16,
}

#[derive(Debug, Args)]
pub struct PoolWithdrawArgs {
    #[command(flatten)]
    pub pool: PoolAccountArgs,
    /// LP shares in integer base units.
    #[arg(long)]
    pub shares: U256,
    #[arg(long, default_value_t = 50)]
    pub slippage_bps: u16,
}

#[derive(Debug, Args)]
pub struct PoolAuthorizeArgs {
    #[command(flatten)]
    pub pool: PoolAccountArgs,
    /// Fee operator address; the zero address revokes delegated access.
    #[arg(long)]
    pub operator: Address,
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
