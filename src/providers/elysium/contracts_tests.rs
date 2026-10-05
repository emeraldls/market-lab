use super::*;
use crate::domain::contracts::{ContractAction, ContractPlan, ContractRequest, SignedTransaction};
use crate::providers::elysium::contracts::ElysiumContracts;
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_signer_local::PrivateKeySigner;

// Invoked only by the isolated Anvil smoke test, never against configured public networks.
pub(super) async fn local_contract_operations(pool: Address, curve: Address, token: Address) {
    let signer: PrivateKeySigner = std::env::var("ELYSIUM_SMOKE_KEY").unwrap().parse().unwrap();
    let owner = signer.address();
    let pool_api = ElysiumContracts::connect(pool).await.unwrap();
    let curve_api = ElysiumContracts::connect(curve).await.unwrap();
    assert!(ElysiumContracts::connect(token).await.is_err());
    let shared = crate::providers::execution::contract_provider(
        crate::venues::VenueId::Elysium,
        false,
        pool,
    )
    .await
    .unwrap();
    assert!(!shared.state().await.unwrap().is_null());
    assert!(!pool_api.state().await.unwrap().is_null());
    assert!(!curve_api.state().await.unwrap().is_null());
    assert!(!pool_api.balances(owner).await.unwrap().is_null());
    assert!(!curve_api.balances(owner).await.unwrap()["hype_balance"].is_null());

    for (api, market, action) in [
        (
            &pool_api,
            pool,
            ContractAction::Swap {
                token_in: token.to_string(),
                amount: "1".into(),
            },
        ),
        (
            &pool_api,
            pool,
            ContractAction::Deposit {
                amount0: "1".into(),
                amount1: "1".into(),
            },
        ),
        (
            &pool_api,
            pool,
            ContractAction::Withdraw {
                shares: "0.1".into(),
            },
        ),
        (
            &curve_api,
            curve,
            ContractAction::Swap {
                token_in: "native".into(),
                amount: "0.01".into(),
            },
        ),
        (
            &curve_api,
            curve,
            ContractAction::Swap {
                token_in: token.to_string(),
                amount: "1".into(),
            },
        ),
    ] {
        let request = ContractRequest {
            market,
            slippage_bps: 50,
            action,
        };
        let quote = api.quote(&request).await.unwrap();
        assert_eq!(quote["market"], json!(market));
        if matches!(request.action, ContractAction::Swap { .. }) {
            assert!(!quote["quote"]["fee"]["amount"]["raw"].is_null());
            assert!(!quote["quote"]["input"]["amount"]["raw"].is_null());
        }
        let plan = api.prepare(owner, &request).await.unwrap();
        assert!(!plan.transactions.is_empty());
        let client = api.client();
        let nonce = client.operator_nonce(owner).await.unwrap();
        execute_plan(client, plan, &signer).await;
        assert!(client.operator_nonce(owner).await.unwrap() > nonce);
    }
    // A held token balance is not enough: sell-backs cannot exceed curve-issued inventory.
    let oversell = ContractRequest {
        market: curve,
        slippage_bps: 50,
        action: ContractAction::Swap {
            token_in: token.to_string(),
            amount: "500000".into(),
        },
    };
    assert!(curve_api.quote(&oversell).await.is_err());
    assert!(curve_api.prepare(owner, &oversell).await.is_err());
    let invalid = ContractRequest {
        market: curve,
        slippage_bps: 50,
        action: ContractAction::Deposit {
            amount0: "1".into(),
            amount1: "1".into(),
        },
    };
    assert!(curve_api.quote(&invalid).await.is_err());
    assert!(curve_api.prepare(owner, &invalid).await.is_err());
    let native_pool = ContractRequest {
        market: pool,
        slippage_bps: 50,
        action: ContractAction::Swap {
            token_in: "native".into(),
            amount: "1".into(),
        },
    };
    assert!(pool_api.quote(&native_pool).await.is_err());

    let request = ContractRequest {
        market: curve,
        slippage_bps: 50,
        action: ContractAction::Swap {
            token_in: "native".into(),
            amount: "0.01".into(),
        },
    };
    let plan = curve_api.prepare(owner, &request).await.unwrap();
    let call = &plan.transactions[0].transaction;
    let client = curve_api.client();
    let cap = U256::from(10u64.pow(16));
    assert!(
        client
            .prepare_transaction(call, &signer, U256::ZERO)
            .await
            .is_err()
    );
    let mut wrong = call.clone();
    wrong.chain_id = U64::from(1);
    assert!(
        client
            .prepare_transaction(&wrong, &signer, cap)
            .await
            .is_err()
    );
    wrong = call.clone();
    wrong.from = Address::repeat_byte(4);
    assert!(
        client
            .prepare_transaction(&wrong, &signer, cap)
            .await
            .is_err()
    );

    // Do not pick another nonce while an unknown/pending transaction exists.
    let signed = client
        .prepare_transaction(call, &signer, cap)
        .await
        .unwrap();
    let _: Value = client
        .rpc("anvil_setAutomine", json!([false]))
        .await
        .unwrap();
    client.broadcast_transaction(&signed).await.unwrap();
    let error = client
        .prepare_transaction(call, &signer, cap)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("untracked pending"));
    let _: Value = client.rpc("anvil_mine", json!(["0x1"])).await.unwrap();
    wait_for_inclusion(client, signed.hash).await;
    let _: Value = client.rpc("anvil_mine", json!(["0x2"])).await.unwrap();
    let _: Value = client
        .rpc("anvil_setAutomine", json!([true]))
        .await
        .unwrap();
    assert!(client.fee_receipt(signed.hash).await.unwrap().is_some());
}

async fn execute_plan(client: &PoolClient, plan: ContractPlan, signer: &PrivateKeySigner) {
    for step in plan.transactions {
        let signed = client
            .prepare_transaction(&step.transaction, signer, U256::from(10u64.pow(16)))
            .await
            .unwrap();
        let mut raw = signed.raw.as_ref();
        let decoded = TxEnvelope::decode_2718(&mut raw).unwrap();
        assert!(raw.is_empty());
        assert_eq!(decoded.chain_id(), Some(99801));
        assert_eq!(decoded.value(), step.transaction.value);
        assert_eq!(decoded.to(), Some(step.transaction.to));
        assert_eq!(decoded.input(), &step.transaction.data);
        assert_eq!(decoded.nonce(), signed.nonce);
        // Exercise the same serialization required for durable daemon recovery.
        let saved: SignedTransaction =
            serde_json::from_slice(&serde_json::to_vec(&signed).unwrap()).unwrap();
        client.broadcast_transaction(&saved).await.unwrap();
        wait_for_inclusion(client, saved.hash).await;
        assert!(client.fee_receipt(saved.hash).await.unwrap().is_none());
        let _: Value = client.rpc("anvil_mine", json!(["0x2"])).await.unwrap();
        let receipt = client.fee_receipt(saved.hash).await.unwrap().unwrap();
        assert_eq!(receipt.status, U64::from(1));
    }
}

async fn wait_for_inclusion(client: &PoolClient, hash: B256) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let receipt: Option<Value> = client
                .rpc("eth_getTransactionReceipt", json!([hash]))
                .await
                .unwrap();
            if receipt.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("local transaction was not mined");
}
