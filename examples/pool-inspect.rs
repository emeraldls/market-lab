use alloy_primitives::{U256, utils::parse_units};
use anyhow::{Context, Result};
use market_lab::providers::elysium::{PoolClient, TokenAmount};

// Read-only integration example: cargo run --example pool-inspect -- <pool-address>
#[tokio::main]
async fn main() -> Result<()> {
    let address = std::env::args()
        .nth(1)
        .context("provide a pool address")?
        .parse()?;
    let pool = PoolClient::new(None)?.inspect(address).await?;
    assert!(pool.min_fee_bps <= pool.fee_bps && pool.fee_bps <= pool.max_fee_bps);
    assert_ne!(pool.reserves[0].address, pool.reserves[1].address);
    assert_eq!(
        pool.operator.is_some(),
        pool.operator_hype_balance.is_some()
    );
    for reserve in &pool.reserves {
        check_amount(&reserve.amount, reserve.decimals)?;
    }
    check_amount(&pool.lp_supply, pool.lp_decimals)?;
    println!("{}", serde_json::to_string_pretty(&pool)?);
    Ok(())
}

fn check_amount(amount: &TokenAmount, decimals: u8) -> Result<()> {
    let raw: U256 = amount.raw.parse()?;
    let formatted: U256 = parse_units(&amount.formatted, decimals)?.into();
    assert_eq!(
        raw, formatted,
        "display amount must preserve every base unit"
    );
    Ok(())
}
