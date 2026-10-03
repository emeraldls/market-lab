//! Unsigned browser-wallet calls. This module never loads credentials or broadcasts.
use alloy_sol_types::SolEvent;

use super::*;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletTransaction {
    pub chain_id: U64,
    pub from: Address,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
}

impl WalletTransaction {
    fn new<C: SolCall>(chain_id: u64, from: Address, to: Address, call: C) -> Self {
        Self {
            chain_id: U64::from(chain_id),
            from,
            to,
            data: call.abi_encode().into(),
            value: U256::ZERO,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WalletStep {
    pub action: &'static str,
    pub transaction: WalletTransaction,
}

#[derive(Debug, Serialize)]
pub struct CreatePlan {
    pub chain_id: u64,
    pub block_hash: B256,
    pub account: Address,
    pub tokens: [PoolReserve; 2],
    pub fee_bps: u16,
    pub min_fee_bps: u16,
    pub max_fee_bps: u16,
    pub transaction: WalletTransaction,
}

#[derive(Debug, Serialize)]
pub struct DepositPlan {
    pub chain_id: u64,
    pub block_hash: B256,
    pub pool: Address,
    pub account: Address,
    pub initial: bool,
    pub maximum_amounts: [PoolReserve; 2],
    pub expected_shares: TokenAmount,
    pub minimum_shares: TokenAmount,
    pub deadline: u64,
    pub transactions: Vec<WalletStep>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CreationStatus {
    Pending {
        transaction_hash: B256,
    },
    Confirmed {
        transaction_hash: B256,
        block_hash: B256,
        pool: Address,
        manager: Address,
        token0: Address,
        token1: Address,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreationReceipt {
    transaction_hash: B256,
    block_hash: B256,
    block_number: U64,
    status: U64,
    from: Address,
    to: Option<Address>,
    logs: Vec<ReceiptLog>,
}

#[derive(Deserialize)]
struct ReceiptLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
}

impl PoolClient {
    async fn wallet_block(&self) -> Result<Block> {
        let chain: U64 = self.rpc("eth_chainId", json!([])).await?;
        ensure!(
            chain.to::<u64>() == self.chain_id,
            "incorrect Elysium chain ID"
        );
        self.rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await
    }

    pub async fn prepare_create(
        &self,
        account: Address,
        token_a: Address,
        token_b: Address,
        fee_bps: u16,
        min_fee_bps: u16,
        max_fee_bps: u16,
    ) -> Result<CreatePlan> {
        ensure!(!account.is_zero(), "manager wallet must not be zero");
        ensure!(
            token_a != token_b && !token_a.is_zero() && !token_b.is_zero(),
            "choose two different ERC-20 tokens"
        );
        ensure!(
            min_fee_bps <= fee_bps && fee_bps <= max_fee_bps && max_fee_bps <= 1000,
            "fees must satisfy minimum <= initial <= maximum <= 1000 bps"
        );
        let block = self.wallet_block().await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let transaction = WalletTransaction::new(
            self.chain_id,
            account,
            self.factory,
            PoolFactory::createPoolCall {
                tokenA: token_a,
                tokenB: token_b,
                initialFeeBps: fee_bps,
                minFeeBps: min_fee_bps,
                maxFeeBps: max_fee_bps,
            },
        );
        let simulation: Bytes = self
            .rpc("eth_call", json!([transaction, at]))
            .await
            .context("pool creation simulation failed")?;
        PoolFactory::createPoolCall::abi_decode_returns_validate(&simulation)?;
        // The simulation address is not reserved: another creation can change it before signing.
        let (token0, token1) = if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        };
        Ok(CreatePlan {
            chain_id: self.chain_id,
            block_hash: block.hash,
            account,
            tokens: [
                self.reserve(token0, U256::ZERO, &at).await?,
                self.reserve(token1, U256::ZERO, &at).await?,
            ],
            fee_bps,
            min_fee_bps,
            max_fee_bps,
            transaction,
        })
    }

    pub async fn created_pool(&self, hash: B256, account: Address) -> Result<CreationStatus> {
        let head = self.wallet_block().await?;
        let receipt: Option<CreationReceipt> =
            self.rpc("eth_getTransactionReceipt", json!([hash])).await?;
        let Some(receipt) = receipt else {
            return Ok(CreationStatus::Pending {
                transaction_hash: hash,
            });
        };
        ensure!(
            receipt.transaction_hash == hash,
            "RPC returned a different receipt"
        );
        ensure!(
            receipt.from == account && receipt.to == Some(self.factory),
            "creation transaction does not belong to this wallet and factory"
        );
        ensure!(
            receipt.status == U64::from(1),
            "pool creation transaction reverted"
        );
        let block: Block = self
            .rpc("eth_getBlockByNumber", json!([receipt.block_number, false]))
            .await?;
        ensure!(
            block.hash == receipt.block_hash,
            "pool creation receipt is no longer canonical"
        );
        // Same confirmation policy as fee transactions, not a finality guarantee.
        if head.number.to::<u64>().saturating_sub(block.number.to()) < 2 {
            return Ok(CreationStatus::Pending {
                transaction_hash: hash,
            });
        }
        let log = receipt
            .logs
            .iter()
            .find(|log| {
                log.address == self.factory
                    && log.topics.first() == Some(&PoolFactory::PoolCreated::SIGNATURE_HASH)
            })
            .context("receipt has no MarketLab PoolCreated event")?;
        let event = PoolFactory::PoolCreated::decode_raw_log_validate(
            log.topics.iter().copied(),
            &log.data,
        )?;
        ensure!(
            event.manager == account,
            "pool manager does not match the signing wallet"
        );
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        ensure!(
            self.call(
                self.factory,
                PoolFactory::isPoolCall { pool: event.pool },
                &at
            )
            .await?,
            "created pool is not registered"
        );
        Ok(CreationStatus::Confirmed {
            transaction_hash: hash,
            block_hash: block.hash,
            pool: event.pool,
            manager: event.manager,
            token0: event.token0,
            token1: event.token1,
        })
    }

    pub async fn prepare_deposit(
        &self,
        pool: Address,
        account: Address,
        amount0: U256,
        amount1: U256,
        slippage_bps: u16,
    ) -> Result<DepositPlan> {
        ensure!(!account.is_zero(), "deposit wallet must not be zero");
        ensure!(slippage_bps < 10_000, "slippage must be below 10000 bps");
        let block = self.wallet_block().await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        ensure!(
            self.call(self.factory, PoolFactory::isPoolCall { pool }, &at)
                .await?,
            "pool is not registered with the MarketLab factory"
        );
        let initial = self
            .call(pool, Token::totalSupplyCall {}, &at)
            .await?
            .is_zero();
        if initial {
            ensure!(
                self.call(pool, Pool::ownerCall {}, &at).await? == account,
                "only the manager can seed this pool"
            );
        }
        let token0 = self.call(pool, Pool::token0Call {}, &at).await?;
        let token1 = self.call(pool, Pool::token1Call {}, &at).await?;
        let quote = self
            .call(
                pool,
                Pool::previewDepositCall {
                    max0: amount0,
                    max1: amount1,
                },
                &at,
            )
            .await?;
        let minimum = quote
            .shares
            .checked_mul(U256::from(10_000 - slippage_bps))
            .context("share amount overflow")?
            / U256::from(10_000);
        ensure!(
            !minimum.is_zero(),
            "deposit is too small for the selected slippage"
        );
        let decimals = self.call(pool, Token::decimalsCall {}, &at).await?;
        let mut transactions = Vec::new();
        for (token, amount) in [(token0, amount0), (token1, amount1)] {
            ensure!(
                self.call(token, Token::balanceOfCall { account }, &at)
                    .await?
                    >= amount,
                "wallet has insufficient balance for token {token}"
            );
            let allowance = self
                .call(
                    token,
                    Token::allowanceCall {
                        owner: account,
                        spender: pool,
                    },
                    &at,
                )
                .await?;
            if allowance < amount {
                // Some ERC-20s require clearing a nonzero approval before changing it.
                if !allowance.is_zero() {
                    transactions.push(WalletStep {
                        action: "reset_approval",
                        transaction: WalletTransaction::new(
                            self.chain_id,
                            account,
                            token,
                            Token::approveCall {
                                spender: pool,
                                amount: U256::ZERO,
                            },
                        ),
                    });
                }
                transactions.push(WalletStep {
                    action: "approve",
                    transaction: WalletTransaction::new(
                        self.chain_id,
                        account,
                        token,
                        Token::approveCall {
                            spender: pool,
                            amount,
                        },
                    ),
                });
            }
        }
        let deadline = block
            .timestamp
            .to::<u64>()
            .checked_add(1200)
            .context("deposit deadline overflow")?;
        transactions.push(WalletStep {
            action: "deposit",
            transaction: WalletTransaction::new(
                self.chain_id,
                account,
                pool,
                Pool::depositCall {
                    max0: amount0,
                    max1: amount1,
                    minShares: minimum,
                    recipient: account,
                    deadline: U256::from(deadline),
                },
            ),
        });
        Ok(DepositPlan {
            chain_id: self.chain_id,
            block_hash: block.hash,
            pool,
            account,
            initial,
            maximum_amounts: [
                self.reserve(token0, amount0, &at).await?,
                self.reserve(token1, amount1, &at).await?,
            ],
            expected_shares: TokenAmount::new(quote.shares, decimals)?,
            minimum_shares: TokenAmount::new(minimum, decimals)?,
            deadline,
            transactions,
        })
    }
}
