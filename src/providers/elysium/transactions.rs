use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{TxKind, U128, keccak256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;

use super::*;
use crate::domain::contracts::{SignedTransaction, WalletTransaction};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Observation {
    pub block: u64,
    pub block_hash: B256,
    pub timestamp: u64,
    #[serde(default)]
    pub spot_price: Option<U256>,
    pub reserve0: U256,
    pub reserve1: U256,
    pub operator: Address,
    pub fee_bps: u16,
    pub min_fee_bps: u16,
    pub max_fee_bps: u16,
}

impl Observation {
    pub fn log_price(&self) -> Result<f64> {
        if let Some(price) = self.spot_price {
            ensure!(!price.is_zero(), "market price must be positive");
            return Ok(price.to_string().parse::<f64>()?.ln());
        }
        ensure!(
            !self.reserve0.is_zero() && !self.reserve1.is_zero(),
            "pool has no liquidity"
        );
        Ok(self.reserve1.to_string().parse::<f64>()?.ln()
            - self.reserve0.to_string().parse::<f64>()?.ln())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignedFee {
    pub hash: B256,
    pub raw: Bytes,
    pub sender: Address,
    pub nonce: u64,
    pub pool: Address,
    pub fee_bps: u16,
    pub max_gas_cost_wei: U256,
}

pub use crate::domain::contracts::TransactionReceipt as FeeReceipt;

impl PoolClient {
    pub async fn observe(&self, pool: Address) -> Result<Observation> {
        if matches!(self.market, curve::FeeMarket::Curve { .. }) {
            return self.observe_curve(pool).await;
        }
        let chain: U64 = self.rpc("eth_chainId", json!([])).await?;
        ensure!(
            chain.to::<u64>() == self.chain_id,
            "incorrect Elysium chain ID"
        );
        let block: Block = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        ensure!(
            self.call(self.factory, PoolFactory::isPoolCall { pool }, &at)
                .await?,
            "pool is not registered with the MarketLab factory"
        );
        let reserves = self.call(pool, Pool::getReservesCall {}, &at).await?;
        Ok(Observation {
            block: block.number.to(),
            block_hash: block.hash,
            timestamp: block.timestamp.to(),
            spot_price: None,
            reserve0: reserves.reserve0,
            reserve1: reserves.reserve1,
            operator: self.call(pool, Pool::operatorCall {}, &at).await?,
            fee_bps: self.call(pool, Pool::feeBpsCall {}, &at).await?,
            min_fee_bps: self.call(pool, Pool::minFeeBpsCall {}, &at).await?,
            max_fee_bps: self.call(pool, Pool::maxFeeBpsCall {}, &at).await?,
        })
    }

    /// Prepare only. The daemon must durably save these exact bytes before broadcasting.
    pub async fn prepare_fee(
        &self,
        pool: Address,
        fee_bps: u16,
        signer: &PrivateKeySigner,
        max_gas_cost: U256,
    ) -> Result<SignedFee> {
        let observation = self.observe(pool).await?;
        ensure!(
            observation.operator == signer.address(),
            "pool operator is not authorized"
        );
        ensure!(
            (observation.min_fee_bps..=observation.max_fee_bps).contains(&fee_bps),
            "requested fee is outside the pool bounds"
        );
        let signed = self
            .prepare_transaction(
                &WalletTransaction::new(
                    self.chain_id,
                    signer.address(),
                    pool,
                    Pool::setFeeCall { feeBps: fee_bps },
                ),
                signer,
                max_gas_cost,
            )
            .await?;
        Ok(SignedFee {
            hash: signed.hash,
            raw: signed.raw,
            sender: signed.sender,
            nonce: signed.nonce,
            pool,
            fee_bps,
            max_gas_cost_wei: signed.max_gas_cost_wei,
        })
    }

    /// Sign a provider-prepared call. Callers serialize wallet access and persist before broadcast.
    pub async fn prepare_transaction(
        &self,
        call: &WalletTransaction,
        signer: &PrivateKeySigner,
        max_gas_cost: U256,
    ) -> Result<SignedTransaction> {
        ensure!(
            call.chain_id.to::<u64>() == self.chain_id,
            "transaction targets a different chain"
        );
        ensure!(
            call.from == signer.address(),
            "transaction sender does not match signing wallet"
        );
        let chain: U64 = self.rpc("eth_chainId", json!([])).await?;
        ensure!(
            chain.to::<u64>() == self.chain_id,
            "incorrect Elysium chain ID"
        );
        let sender = signer.address();
        let latest: U64 = self
            .rpc("eth_getTransactionCount", json!([sender, "latest"]))
            .await?;
        let pending: U64 = self
            .rpc("eth_getTransactionCount", json!([sender, "pending"]))
            .await?;
        ensure!(
            latest == pending,
            "wallet has an untracked pending transaction; wait for it to settle"
        );
        let estimate: U64 = self.rpc("eth_estimateGas", json!([call])).await?;
        let gas_limit = estimate
            .to::<u64>()
            .checked_mul(120)
            .context("gas estimate overflow")?
            / 100;
        let gas_price: U128 = self.rpc("eth_gasPrice", json!([])).await?;
        let priority: U128 = self.rpc("eth_maxPriorityFeePerGas", json!([])).await?;
        let max_fee_per_gas = gas_price
            .to::<u128>()
            .checked_mul(2)
            .context("gas price overflow")?;
        ensure!(
            priority.to::<u128>() <= max_fee_per_gas,
            "RPC returned inconsistent gas prices"
        );
        let max_gas_cost_wei = U256::from(gas_limit) * U256::from(max_fee_per_gas);
        ensure!(
            max_gas_cost_wei <= max_gas_cost,
            "transaction exceeds the configured HYPE gas cap"
        );
        let balance: U256 = self
            .rpc("eth_getBalance", json!([sender, "pending"]))
            .await?;
        let total = call
            .value
            .checked_add(max_gas_cost_wei)
            .context("transaction cost overflow")?;
        ensure!(
            balance >= total,
            "signing wallet needs more native HYPE for the payment and gas"
        );
        let transaction = TxEip1559 {
            chain_id: self.chain_id,
            nonce: pending.to(),
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas: priority.to(),
            to: TxKind::Call(call.to),
            input: call.data.clone(),
            value: call.value,
            ..Default::default()
        };
        let signature = signer.sign_hash_sync(&transaction.signature_hash())?;
        let envelope: TxEnvelope = transaction.into_signed(signature).into();
        let raw: Bytes = envelope.encoded_2718().into();
        Ok(SignedTransaction {
            chain_id: self.chain_id,
            hash: keccak256(&raw),
            raw,
            sender,
            nonce: pending.to(),
            to: call.to,
            value: call.value,
            max_gas_cost_wei,
        })
    }

    pub async fn broadcast_transaction(&self, signed: &SignedTransaction) -> Result<()> {
        ensure!(
            signed.chain_id == self.chain_id,
            "transaction targets a different chain"
        );
        self.broadcast_raw(signed.hash, &signed.raw).await
    }

    pub async fn broadcast_fee(&self, fee: &SignedFee) -> Result<()> {
        self.broadcast_raw(fee.hash, &fee.raw).await
    }

    async fn broadcast_raw(&self, expected_hash: B256, raw: &Bytes) -> Result<()> {
        ensure!(
            keccak256(raw) == expected_hash,
            "signed transaction hash mismatch"
        );
        let chain: U64 = self.rpc("eth_chainId", json!([])).await?;
        ensure!(
            chain.to::<u64>() == self.chain_id,
            "incorrect Elysium chain ID"
        );
        let hash: B256 = self.rpc("eth_sendRawTransaction", json!([raw])).await?;
        ensure!(
            hash == expected_hash,
            "RPC returned a different transaction hash"
        );
        Ok(())
    }

    pub async fn fee_receipt(&self, hash: B256) -> Result<Option<FeeReceipt>> {
        let receipt: Option<FeeReceipt> =
            self.rpc("eth_getTransactionReceipt", json!([hash])).await?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        ensure!(
            receipt.transaction_hash == hash,
            "RPC returned a different receipt"
        );
        let block: Block = self
            .rpc("eth_getBlockByNumber", json!([receipt.block_number, false]))
            .await?;
        ensure!(
            block.hash == receipt.block_hash,
            "fee receipt is no longer canonical"
        );
        let head: U64 = self.rpc("eth_blockNumber", json!([])).await?;
        // Require two successors before advancing the operator nonce. This is not finality.
        if head.to::<u64>().saturating_sub(block.number.to()) < 2 {
            return Ok(None);
        }
        Ok(Some(receipt))
    }

    pub async fn fee_known(&self, hash: B256) -> Result<bool> {
        let transaction: Option<Value> =
            self.rpc("eth_getTransactionByHash", json!([hash])).await?;
        Ok(transaction.is_some())
    }

    pub async fn operator_nonce(&self, sender: Address) -> Result<u64> {
        let nonce: U64 = self
            .rpc("eth_getTransactionCount", json!([sender, "latest"]))
            .await?;
        Ok(nonce.to())
    }
}
