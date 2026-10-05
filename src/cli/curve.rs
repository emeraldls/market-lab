use alloy_primitives::{Address, U256};
use clap::{Args, Subcommand, ValueEnum};
use reqwest::Url;

use super::OutputFormat;
use crate::runtime::pools::FeePolicy;

#[derive(Debug, Args)]
pub struct CurveArgs {
    /// Deployed Marketlab CurveFactory address. Required for on-chain commands.
    #[arg(long, global = true)]
    pub factory: Option<Address>,
    #[arg(long, global = true)]
    pub rpc_url: Option<Url>,
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Terminal)]
    pub output: OutputFormat,
    #[command(subcommand)]
    pub command: CurveCommands,
}

#[derive(Debug, Subcommand)]
pub enum CurveCommands {
    /// Prepare allocation approval and market creation. No signing or broadcasting.
    Create(CurveCreateArgs),
    /// List markets in the factory, newest first.
    List {
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=50))]
        limit: u64,
    },
    /// Read the curve, reserves, creator fees and permissions.
    Inspect { address: Address },
    /// Buy quotes take HYPE wei; sell quotes take token base units.
    Quote {
        address: Address,
        #[arg(long, value_enum)]
        side: CurveSide,
        #[arg(long)]
        amount: U256,
    },
    /// Prepare a native-HYPE buy; unspent HYPE is refunded. No signing or broadcasting.
    Buy(CurveTradeArgs),
    /// Prepare token approval and sell-back. No signing or broadcasting.
    Sell(CurveTradeArgs),
    /// Prepare a creator fee withdrawal. Seller backing cannot be withdrawn.
    ClaimFees(CurveWalletArgs),
    /// Prepare fee operator authorization; zero address revokes access.
    Authorize {
        #[command(flatten)]
        wallet: CurveWalletArgs,
        #[arg(long)]
        operator: Address,
    },
    /// Start automatic bounded fee updates using the shared Elysium fee operator.
    Run {
        address: Address,
        #[command(flatten)]
        policy: FeePolicy,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        yes: bool,
    },
    /// List fee jobs in the shared pool/curve signing queue.
    Jobs,
    /// Stop a fee job; a previously signed transaction may still settle.
    Stop { job_id: String },
    /// Read observations and fee transaction receipts.
    Logs {
        job_id: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Create or inspect the same dedicated operator used by pool fee jobs.
    Operator {
        #[command(subcommand)]
        command: super::PoolOperatorCommands,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CurveSide {
    Buy,
    Sell,
}

#[derive(Debug, Args)]
pub struct CurveWalletArgs {
    pub address: Address,
    #[arg(long)]
    pub account: Address,
}

#[derive(Debug, Args)]
pub struct CurveTradeArgs {
    #[command(flatten)]
    pub wallet: CurveWalletArgs,
    /// Buy: maximum native HYPE in wei. Sell: token amount in integer base units.
    #[arg(long)]
    pub amount: U256,
    #[arg(long, default_value_t = 50)]
    pub slippage_bps: u16,
}

#[derive(Debug, Args)]
pub struct CurveCreateArgs {
    #[arg(long)]
    pub token: Address,
    /// Tokens committed to this market, in integer token base units.
    #[arg(long)]
    pub allocation: U256,
    /// HYPE wei per whole token when no tokens have been sold.
    #[arg(long)]
    pub start_price: U256,
    /// HYPE wei per whole token when the entire allocation has been sold.
    #[arg(long)]
    pub end_price: U256,
    #[arg(long, default_value_t = 30)]
    pub fee_bps: u16,
    #[arg(long, default_value_t = 5)]
    pub min_fee_bps: u16,
    #[arg(long, default_value_t = 100)]
    pub max_fee_bps: u16,
    #[arg(long)]
    pub account: Address,
}
