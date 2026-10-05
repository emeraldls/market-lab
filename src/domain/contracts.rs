//! Exact-amount contract operations. Quotes are reads; prepared calls require a signer.
use alloy_primitives::{Address, B256, Bytes, U64, U128, U256};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContractAction {
    Swap { token_in: String, amount: String },
    Deposit { amount0: String, amount1: String },
    Withdraw { shares: String },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContractRequest {
    pub market: Address,
    #[serde(default = "default_slippage")]
    pub slippage_bps: u16,
    #[serde(flatten)]
    pub action: ContractAction,
}

fn default_slippage() -> u16 {
    50
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletTransaction {
    pub chain_id: U64,
    pub from: Address,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContractStep {
    pub action: String,
    pub transaction: WalletTransaction,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContractPlan {
    pub market: Address,
    pub account: Address,
    pub chain_id: u64,
    pub quote: Value,
    pub transactions: Vec<ContractStep>,
}

/// Persist these exact bytes before broadcasting. Retry the same hash after an ambiguous result.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedTransaction {
    pub chain_id: u64,
    pub hash: B256,
    pub raw: Bytes,
    pub sender: Address,
    pub nonce: u64,
    pub to: Address,
    pub value: U256,
    pub max_gas_cost_wei: U256,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn requests_preserve_decimal_strings_and_reject_arbitrary_calls() {
        for action in [
            ContractAction::Swap {
                token_in: "native".into(),
                amount: "0.000000000000000001".into(),
            },
            ContractAction::Deposit {
                amount0: "1".into(),
                amount1: "2".into(),
            },
            ContractAction::Withdraw {
                shares: "0.01".into(),
            },
        ] {
            let request = ContractRequest {
                market: Address::repeat_byte(1),
                slippage_bps: 50,
                action,
            };
            let encoded = serde_json::to_value(&request).unwrap();
            assert_eq!(
                serde_json::from_value::<ContractRequest>(encoded.clone()).unwrap(),
                request
            );
            let mut arbitrary = encoded;
            arbitrary["recipient"] = json!(Address::repeat_byte(2));
            assert!(serde_json::from_value::<ContractRequest>(arbitrary).is_err());
        }
        let value = json!({"market": Address::repeat_byte(1), "action": "swap", "token_in": "native", "amount": 0.1});
        assert!(serde_json::from_value::<ContractRequest>(value).is_err());
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionReceipt {
    pub transaction_hash: B256,
    pub block_hash: B256,
    pub block_number: U64,
    pub status: U64,
    pub gas_used: U64,
    pub effective_gas_price: U128,
}
