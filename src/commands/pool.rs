use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cli::{OutputFormat, PoolCommands, PoolInspectArgs, PoolOperatorCommands, PoolRunArgs};
use crate::providers::elysium::PoolClient;
use crate::runtime::pools::{self, PoolJob, PoolRequest};

pub async fn handle(command: PoolCommands) -> Result<()> {
    let (request, output) = match command {
        PoolCommands::List(args) => {
            let output = args.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.rpc_url)?;
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                client.list(args.offset, args.limit),
            )
            .await
            .context("pool listing timed out")??;
            return print_result(&result, output);
        }
        PoolCommands::Create(args) => {
            let output = args.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.wallet.rpc_url)?;
            let plan = tokio::time::timeout(
                Duration::from_secs(60),
                client.prepare_create(
                    args.wallet.account,
                    args.token_a,
                    args.token_b,
                    args.fee_bps,
                    args.min_fee_bps,
                    args.max_fee_bps,
                ),
            )
            .await
            .context("pool creation preparation timed out")??;
            return print_result(&serde_json::to_value(plan)?, output);
        }
        PoolCommands::Created(args) => {
            let output = args.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.wallet.rpc_url)?;
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                client.created_pool(args.transaction_hash, args.wallet.account),
            )
            .await
            .context("pool creation receipt lookup timed out")??;
            return print_result(&serde_json::to_value(result)?, output);
        }
        PoolCommands::Deposit(args) => {
            let output = args.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.wallet.rpc_url)?;
            let plan = tokio::time::timeout(
                Duration::from_secs(60),
                client.prepare_deposit(
                    args.address,
                    args.wallet.account,
                    args.amount0,
                    args.amount1,
                    args.slippage_bps,
                ),
            )
            .await
            .context("pool deposit preparation timed out")??;
            return print_result(&serde_json::to_value(plan)?, output);
        }
        PoolCommands::Inspect(args) => return handle_inspect(args).await,
        PoolCommands::Position(args) => {
            let output = args.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.wallet.rpc_url)?;
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                client.position(args.address, args.wallet.account),
            )
            .await
            .context("pool position lookup timed out")??;
            return print_result(&result, output);
        }
        PoolCommands::Swap(args) => {
            let output = args.pool.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.pool.wallet.rpc_url)?;
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                client.prepare_swap(
                    args.pool.address,
                    args.pool.wallet.account,
                    args.token_in,
                    args.amount_in,
                    args.slippage_bps,
                ),
            )
            .await
            .context("pool swap preparation timed out")??;
            return print_result(&result, output);
        }
        PoolCommands::Withdraw(args) => {
            let output = args.pool.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.pool.wallet.rpc_url)?;
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                client.prepare_withdraw(
                    args.pool.address,
                    args.pool.wallet.account,
                    args.shares,
                    args.slippage_bps,
                ),
            )
            .await
            .context("pool withdrawal preparation timed out")??;
            return print_result(&result, output);
        }
        PoolCommands::Authorize(args) => {
            let output = args.pool.wallet.format.output;
            validate_output(output)?;
            let client = PoolClient::new(args.pool.wallet.rpc_url)?;
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                client.prepare_authorize(
                    args.pool.address,
                    args.pool.wallet.account,
                    args.operator,
                ),
            )
            .await
            .context("pool operator approval preparation timed out")??;
            return print_result(&result, output);
        }
        PoolCommands::Operator { command } => {
            let address = match command {
                PoolOperatorCommands::Create => crate::credentials::pool::create()?,
                PoolOperatorCommands::Address => crate::credentials::pool::address()?,
            };
            println!("{address}");
            return Ok(());
        }
        PoolCommands::Run(args) => return handle_run(args).await,
        PoolCommands::Jobs(format) => (PoolRequest::Jobs, format.output),
        PoolCommands::Stop { job_id, format } => (PoolRequest::Stop { job_id }, format.output),
        PoolCommands::Logs {
            job_id,
            limit,
            format,
        } => (PoolRequest::Logs { job_id, limit }, format.output),
    };
    validate_output(output)?;
    let result = pools::request(request).await?;
    print_result(&result, output)
}

fn validate_output(output: OutputFormat) -> Result<()> {
    if matches!(output, OutputFormat::Csv | OutputFormat::Parquet) {
        bail!("pool commands support --output terminal, json or jsonl");
    }
    Ok(())
}

async fn handle_run(args: PoolRunArgs) -> Result<()> {
    let output = args.format.output;
    validate_output(output)?;
    args.policy.validate()?;
    let client = PoolClient::new(args.rpc_url.clone())?;
    let snapshot = tokio::time::timeout(Duration::from_secs(60), client.observe(args.address))
        .await
        .context("pool preview timed out")??;
    anyhow::ensure!(
        (snapshot.min_fee_bps..=snapshot.max_fee_bps).contains(&args.policy.base_fee_bps),
        "base fee is outside this pool's bounds"
    );
    if args.dry_run {
        let preview = serde_json::json!({ "pool": args.address, "strategy": "adaptive_fee", "policy": args.policy, "snapshot": snapshot, "dry_run": true });
        return print_result(&preview, output);
    }
    anyhow::ensure!(
        snapshot.operator == crate::credentials::pool::address()?,
        "the manager must grant this operator permission with setOperator first"
    );
    anyhow::ensure!(
        !snapshot.reserve0.is_zero() && !snapshot.reserve1.is_zero(),
        "pool has no liquidity"
    );
    if matches!(output, OutputFormat::Terminal) {
        println!(
            "pool {}\nstrategy: adaptive-fee\nfee bounds: {}-{} bps\nbase fee: {} bps\ninterval: {}s; cooldown: {}s\nmax gas per transaction: {} HYPE",
            args.address,
            snapshot.min_fee_bps,
            snapshot.max_fee_bps,
            args.policy.base_fee_bps,
            args.policy.interval,
            args.policy.cooldown,
            args.policy.max_tx_gas_hype
        );
    }
    if !args.yes
        && !super::execution::confirm_live_action(
            output,
            "Start automatic fee changes using the operator's HYPE?",
        )?
    {
        return Ok(());
    }
    let result = pools::request(PoolRequest::Start {
        pool: args.address,
        rpc_url: args.rpc_url,
        policy: args.policy,
    })
    .await?;
    print_result(&result, output)
}

fn print_job(job: &PoolJob) {
    let fee = |value: Option<u16>| {
        value
            .map(|value| format!("{value} bps"))
            .unwrap_or_else(|| "waiting".into())
    };
    println!(
        "{}  {:?}\n  pool: {}\n  fee: {}; target: {}",
        job.id,
        job.status,
        job.pool,
        fee(job.current_fee_bps),
        fee(job.target_fee_bps)
    );
    if let Some(hash) = job.last_transaction {
        println!("  transaction: {hash}");
    }
    if let Some(error) = &job.last_error {
        println!("  error: {error}");
    }
}

fn print_result(value: &serde_json::Value, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(value)?),
        OutputFormat::Jsonl => println!("{value}"),
        OutputFormat::Terminal => {
            if let Some(jobs) = value.get("jobs").and_then(serde_json::Value::as_array) {
                if jobs.is_empty() {
                    println!("No pool jobs.");
                }
                for job in jobs {
                    print_job(&serde_json::from_value(job.clone())?);
                }
                if let Some(pending) = value.get("pending").filter(|pending| !pending.is_null()) {
                    println!("pending transaction: {}", pending["transaction"]["hash"]);
                }
            } else if value.get("id").is_some() {
                print_job(&serde_json::from_value(value.clone())?);
            } else if let Some(events) = value.as_array() {
                for event in events {
                    let timestamp = event["ts_ms"]
                        .as_i64()
                        .and_then(chrono::DateTime::from_timestamp_millis)
                        .map(|timestamp| timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_default();
                    println!(
                        "{timestamp}  {}  {}",
                        event["event"].as_str().unwrap_or_default(),
                        event["data"]
                    );
                }
            } else {
                println!("{}", serde_json::to_string_pretty(value)?);
            }
        }
        OutputFormat::Csv | OutputFormat::Parquet => unreachable!("validated output"),
    }
    Ok(())
}

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
