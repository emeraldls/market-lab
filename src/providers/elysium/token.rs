//! Fixed-supply deployment plans and receipt verification. Never signs or broadcasts.
use alloy_sol_types::SolValue;

use super::*;

sol! {
    interface TokenName {
        function name() external view returns (string);
    }
}

#[derive(Deserialize)]
struct Artifact {
    bytecode: Bytes,
    runtime: Bytes,
}

fn artifact() -> Result<Artifact> {
    serde_json::from_str(include_str!("test_token.json")).context("invalid embedded token bytecode")
}

fn deployment_data(
    name: &str,
    symbol: &str,
    supply: &str,
    account: Address,
) -> Result<(Bytes, U256)> {
    ensure!(!account.is_zero(), "token recipient must not be zero");
    ensure!(
        !name.trim().is_empty() && name.len() <= 64,
        "token name must be 1-64 bytes"
    );
    ensure!(
        !symbol.trim().is_empty() && symbol.len() <= 12,
        "token symbol must be 1-12 bytes"
    );
    let parts: Vec<_> = supply.split('.').collect();
    ensure!(
        parts.len() <= 2
            && parts
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|c| c.is_ascii_digit()))
            && parts.get(1).is_none_or(|part| part.len() <= 18),
        "supply must be a positive decimal with at most 18 decimal places"
    );
    let amount: U256 = alloy_primitives::utils::parse_units(supply, 18)?.into();
    ensure!(amount > U256::ZERO, "supply must be positive");
    let mut data = artifact()?.bytecode.to_vec();
    data.extend((name.to_owned(), symbol.to_owned(), amount, account).abi_encode_params());
    Ok((data.into(), amount))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TokenReceipt {
    transaction_hash: B256,
    block_hash: B256,
    block_number: U64,
    status: U64,
    from: Address,
    to: Option<Address>,
    contract_address: Option<Address>,
}

impl PoolClient {
    pub async fn prepare_token(
        &self,
        account: Address,
        name: &str,
        symbol: &str,
        supply: &str,
    ) -> Result<Value> {
        let (data, amount) = deployment_data(name, symbol, supply, account)?;
        self.wallet_block().await?;
        let transaction = json!({"chainId": U64::from(self.chain_id), "from": account, "data": data, "value": "0x0"});
        let gas: U64 = self
            .rpc("eth_estimateGas", json!([transaction]))
            .await
            .context("token deployment simulation failed; check your HYPE balance")?;
        Ok(json!({
            "name": name, "symbol": symbol, "decimals": 18,
            "supply": TokenAmount::new(amount, 18)?, "recipient": account,
            "estimated_gas": gas, "transaction": transaction
        }))
    }

    pub async fn created_token(&self, hash: B256, account: Address) -> Result<Value> {
        let head = self.wallet_block().await?;
        let pending = json!({"status": "pending", "transaction_hash": hash});
        let receipt: Option<TokenReceipt> =
            self.rpc("eth_getTransactionReceipt", json!([hash])).await?;
        let Some(receipt) = receipt else {
            return Ok(pending);
        };
        ensure!(
            receipt.transaction_hash == hash && receipt.from == account && receipt.to.is_none(),
            "not a token deployment by this wallet"
        );
        ensure!(receipt.status == U64::from(1), "token deployment reverted");
        let block: Block = self
            .rpc("eth_getBlockByNumber", json!([receipt.block_number, false]))
            .await?;
        ensure!(
            block.hash == receipt.block_hash,
            "token receipt is no longer canonical"
        );
        if head.number.to::<u64>().saturating_sub(block.number.to()) < 2 {
            return Ok(pending);
        }
        let token = receipt
            .contract_address
            .context("receipt has no deployed token")?;
        let at = json!({"blockHash": block.hash, "requireCanonical": true});
        let code: Bytes = self.rpc("eth_getCode", json!([token, at])).await?;
        ensure!(
            code == artifact()?.runtime,
            "deployed contract is not the fixed-supply MarketLab test token"
        );
        let name = self.call(token, TokenName::nameCall {}, &at).await?;
        let symbol = self.call(token, Token::symbolCall {}, &at).await?;
        let supply = self.call(token, Token::totalSupplyCall {}, &at).await?;
        let balance = self
            .call(token, Token::balanceOfCall { account }, &at)
            .await?;
        ensure!(
            supply > U256::ZERO && balance == supply,
            "initial token supply was not allocated to this wallet"
        );
        Ok(
            json!({"status":"confirmed", "transaction_hash":hash, "block_hash": block.hash,
            "token": {"address": token, "name": name, "symbol": symbol, "decimals": 18},
            "supply": TokenAmount::new(supply,18)?, "recipient":account}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_supply_constructor() {
        let account = Address::repeat_byte(1);
        let (data, amount) = deployment_data("Example", "ABC", "1000000.25", account).unwrap();
        assert_eq!(amount.to_string(), "1000000250000000000000000");
        let artifact = artifact().unwrap();
        assert!(data.starts_with(&artifact.bytecode));
        let decoded = <(String, String, U256, Address)>::abi_decode_params_validate(
            &data[artifact.bytecode.len()..],
        )
        .unwrap();
        assert_eq!(decoded, ("Example".into(), "ABC".into(), amount, account));
        for supply in ["0", "-1", "1e6", "1.1234567890123456789", "", "1."] {
            assert!(deployment_data("ABC", "ABC", supply, account).is_err());
        }
        assert!(deployment_data("", "ABC", "1", account).is_err());
        assert!(deployment_data("ABC", "ABC", "1", Address::ZERO).is_err());
    }
}
