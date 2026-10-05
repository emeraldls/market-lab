use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

use crate::cli::{CurveArgs, CurveCommands, CurveSide, OutputFormat, PoolOperatorCommands};
use crate::providers::elysium::PoolClient;
use crate::providers::elysium::curve::{CurveCreate, FeeMarket};
use crate::runtime::pools::{self, PoolRequest};

pub async fn handle(args: CurveArgs) -> Result<()> {
    ensure!(
        !matches!(args.output, OutputFormat::Csv | OutputFormat::Parquet),
        "curve commands support --output terminal, json or jsonl"
    );
    let output = args.output;
    let value = tokio::time::timeout(Duration::from_secs(120), execute(args))
        .await
        .context("curve command timed out")??;
    super::pool::print_result(&value, output)
}

async fn execute(args: CurveArgs) -> Result<Value> {
    let client = || -> Result<PoolClient> {
        PoolClient::for_market(
            args.rpc_url.clone(),
            FeeMarket::Curve {
                factory: args
                    .factory
                    .context("supply --factory with the deployed CurveFactory address")?,
            },
        )
    };
    match args.command {
        CurveCommands::Create(config) => {
            client()?
                .curve_create(
                    config.account,
                    CurveCreate {
                        token: config.token,
                        allocation: config.allocation,
                        start_price: config.start_price,
                        end_price: config.end_price,
                        fee_bps: config.fee_bps,
                        min_fee_bps: config.min_fee_bps,
                        max_fee_bps: config.max_fee_bps,
                    },
                )
                .await
        }
        CurveCommands::List { offset, limit } => client()?.curve_list(offset, limit).await,
        CurveCommands::Inspect { address } => client()?.curve_inspect(address).await,
        CurveCommands::Quote {
            address,
            side,
            amount,
        } => {
            client()?
                .curve_quote(address, amount, matches!(side, CurveSide::Sell))
                .await
        }
        CurveCommands::Buy(trade) => {
            client()?
                .curve_trade(
                    trade.wallet.address,
                    trade.wallet.account,
                    trade.amount,
                    false,
                    trade.slippage_bps,
                )
                .await
        }
        CurveCommands::Sell(trade) => {
            client()?
                .curve_trade(
                    trade.wallet.address,
                    trade.wallet.account,
                    trade.amount,
                    true,
                    trade.slippage_bps,
                )
                .await
        }
        CurveCommands::ClaimFees(wallet) => {
            client()?
                .curve_manage(wallet.address, wallet.account, None)
                .await
        }
        CurveCommands::Authorize { wallet, operator } => {
            client()?
                .curve_manage(wallet.address, wallet.account, Some(operator))
                .await
        }
        CurveCommands::Run {
            address,
            policy,
            dry_run,
            yes,
        } => {
            policy.validate()?;
            let observation = client()?.observe(address).await?;
            observation.log_price()?;
            ensure!(
                (observation.min_fee_bps..=observation.max_fee_bps).contains(&policy.base_fee_bps),
                "base fee is outside the curve bounds"
            );
            if dry_run {
                return Ok(json!({ "market": address, "dry_run": true,
                    "strategy": "adaptive_fee", "policy": policy, "snapshot": observation }));
            }
            ensure!(
                observation.operator == crate::credentials::pool::address()?,
                "the curve manager must authorize this fee operator first"
            );
            if !yes
                && !super::execution::confirm_live_action(
                    args.output,
                    "Start automatic curve fee changes using the operator's HYPE?",
                )?
            {
                return Ok(json!({ "cancelled": true }));
            }
            pools::request(PoolRequest::Start {
                pool: address,
                market: FeeMarket::Curve {
                    factory: args.factory.context("missing curve factory")?,
                },
                rpc_url: args.rpc_url,
                policy,
            })
            .await
        }
        CurveCommands::Jobs => pools::request(PoolRequest::Jobs).await,
        CurveCommands::Stop { job_id } => pools::request(PoolRequest::Stop { job_id }).await,
        CurveCommands::Logs { job_id, limit } => {
            pools::request(PoolRequest::Logs { job_id, limit }).await
        }
        CurveCommands::Operator { command } => {
            let address = match command {
                PoolOperatorCommands::Create => crate::credentials::pool::create()?,
                PoolOperatorCommands::Address => crate::credentials::pool::address()?,
            };
            Ok(json!({ "operator": address }))
        }
    }
}
