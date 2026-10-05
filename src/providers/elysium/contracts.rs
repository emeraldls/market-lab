//! Script-facing operations for Marketlab factory contracts. No arbitrary calldata or recipients.
use super::curve::{Curve, CurveFactory, FeeMarket};
use super::wallet::minimum_amount;
use super::*;
use crate::domain::contracts::{ContractAction, ContractPlan, ContractRequest, ContractStep};
use alloy_primitives::utils::parse_units;
use anyhow::bail;

pub struct ElysiumContracts {
    client: PoolClient,
    market: Address,
}

impl ElysiumContracts {
    pub async fn connect(market: Address) -> Result<Self> {
        let (mut client, curve_factory) = super::market_data::configured_client()?;
        let block = client.wallet_block().await?;
        let at = json!({"blockHash": block.hash, "requireCanonical": true});
        if !client
            .call(
                client.factory,
                PoolFactory::isPoolCall { pool: market },
                &at,
            )
            .await?
        {
            ensure!(
                client
                    .call(curve_factory, CurveFactory::isMarketCall { market }, &at)
                    .await?,
                "market is not registered with the configured Marketlab factories"
            );
            client.factory = curve_factory;
            client.market = FeeMarket::Curve {
                factory: curve_factory,
            };
        }
        Ok(Self { client, market })
    }

    pub fn client(&self) -> &PoolClient {
        &self.client
    }

    fn is_curve(&self) -> bool {
        matches!(self.client.market, FeeMarket::Curve { .. })
    }

    pub async fn state(&self) -> Result<Value> {
        if self.is_curve() {
            self.client.curve_inspect(self.market).await
        } else {
            Ok(serde_json::to_value(
                self.client.inspect(self.market).await?,
            )?)
        }
    }

    pub async fn balances(&self, account: Address) -> Result<Value> {
        if !self.is_curve() {
            return self.client.position(self.market, account).await;
        }
        let block = self.client.wallet_block().await?;
        let at = json!({"blockHash": block.hash, "requireCanonical": true});
        let token = self
            .client
            .call(self.market, Curve::tokenCall {}, &at)
            .await?;
        let amount = self
            .client
            .call(token, Token::balanceOfCall { account }, &at)
            .await?;
        let hype = self
            .client
            .rpc("eth_getBalance", json!([account, at]))
            .await?;
        Ok(
            json!({"chain_id": self.client.chain_id, "market": self.market, "account": account,
            "block_hash": block.hash, "block_number": block.number,
            "balances": [self.client.reserve(token, amount, &at).await?],
            "hype_balance": TokenAmount::new(hype, 18)?}),
        )
    }

    /// Hypothetical quotes do not require balances or approval from the calling wallet.
    pub async fn quote(&self, request: &ContractRequest) -> Result<Value> {
        self.validate_request(request)?;
        let block = self.client.wallet_block().await?;
        let at = json!({"blockHash": block.hash, "requireCanonical": true});
        let details = match &request.action {
            ContractAction::Swap { token_in, amount } => {
                let (input, decimals) = self.input_asset(token_in, &at).await?;
                let amount = amount_units(amount, decimals)?;
                if self.is_curve() {
                    let token = self
                        .client
                        .call(self.market, Curve::tokenCall {}, &at)
                        .await?;
                    if input.is_none() {
                        let quote = self
                            .client
                            .call(self.market, Curve::quoteBuyHypeCall { budget: amount }, &at)
                            .await?;
                        json!({"input": native_amount(quote.hype)?,
                            "expected_output": self.client.reserve(token, quote.tokens, &at).await?,
                            "minimum_output": self.client.reserve(token, minimum_amount(quote.tokens, request.slippage_bps)?, &at).await?,
                            "fee": native_amount(quote.fee)?})
                    } else {
                        let quote = self
                            .client
                            .call(self.market, Curve::quoteSellCall { tokens: amount }, &at)
                            .await?;
                        json!({"input": self.client.reserve(token, amount, &at).await?,
                            "expected_output": native_amount(quote.hype)?,
                            "minimum_output": native_amount(minimum_amount(quote.hype, request.slippage_bps)?)?,
                            "fee": native_amount(quote.fee)?})
                    }
                } else {
                    let input = input
                        .context("pool swaps require an ERC-20 input; wrap native HYPE first")?;
                    let token0 = self
                        .client
                        .call(self.market, Pool::token0Call {}, &at)
                        .await?;
                    let output = if input == token0 {
                        self.client
                            .call(self.market, Pool::token1Call {}, &at)
                            .await?
                    } else {
                        token0
                    };
                    let quote = self
                        .client
                        .call(
                            self.market,
                            Pool::quoteSwapCall {
                                tokenIn: input,
                                amountIn: amount,
                            },
                            &at,
                        )
                        .await?;
                    json!({"input": self.client.reserve(input, amount, &at).await?,
                        "expected_output": self.client.reserve(output, quote.amountOut, &at).await?,
                        "minimum_output": self.client.reserve(output, minimum_amount(quote.amountOut, request.slippage_bps)?, &at).await?,
                        "fee": self.client.reserve(input, quote.feeAmount, &at).await?})
                }
            }
            ContractAction::Deposit { amount0, amount1 } => {
                self.require_pool()?;
                let (amount0, amount1) = self.deposit_units(amount0, amount1, &at).await?;
                let quote = self
                    .client
                    .call(
                        self.market,
                        Pool::previewDepositCall {
                            max0: amount0,
                            max1: amount1,
                        },
                        &at,
                    )
                    .await?;
                let decimals = self
                    .client
                    .call(self.market, Token::decimalsCall {}, &at)
                    .await?;
                let token0 = self
                    .client
                    .call(self.market, Pool::token0Call {}, &at)
                    .await?;
                let token1 = self
                    .client
                    .call(self.market, Pool::token1Call {}, &at)
                    .await?;
                json!({"amount0": self.client.reserve(token0, quote.amount0, &at).await?,
                    "amount1": self.client.reserve(token1, quote.amount1, &at).await?,
                    "expected_shares": TokenAmount::new(quote.shares, decimals)?,
                    "minimum_shares": TokenAmount::new(minimum_amount(quote.shares, request.slippage_bps)?, decimals)?})
            }
            ContractAction::Withdraw { shares } => {
                self.require_pool()?;
                let decimals = self
                    .client
                    .call(self.market, Token::decimalsCall {}, &at)
                    .await?;
                let shares = amount_units(shares, decimals)?;
                let quote = self
                    .client
                    .call(self.market, Pool::previewWithdrawCall { shares }, &at)
                    .await?;
                let token0 = self
                    .client
                    .call(self.market, Pool::token0Call {}, &at)
                    .await?;
                let token1 = self
                    .client
                    .call(self.market, Pool::token1Call {}, &at)
                    .await?;
                json!({"shares": TokenAmount::new(shares, decimals)?,
                    "expected_output": [self.client.reserve(token0, quote.amount0, &at).await?, self.client.reserve(token1, quote.amount1, &at).await?],
                    "minimum_output": [self.client.reserve(token0, minimum_amount(quote.amount0, request.slippage_bps)?, &at).await?, self.client.reserve(token1, minimum_amount(quote.amount1, request.slippage_bps)?, &at).await?]})
            }
        };
        Ok(
            json!({"chain_id":self.client.chain_id, "market":self.market,
            "block_number":block.number, "block_hash":block.hash, "quote":details}),
        )
    }

    pub async fn prepare(
        &self,
        account: Address,
        request: &ContractRequest,
    ) -> Result<ContractPlan> {
        self.validate_request(request)?;
        let block = self.client.wallet_block().await?;
        let at = json!({"blockHash": block.hash, "requireCanonical": true});
        let mut plan = match &request.action {
            ContractAction::Swap { token_in, amount } => {
                let (input, decimals) = self.input_asset(token_in, &at).await?;
                let amount = amount_units(amount, decimals)?;
                if self.is_curve() {
                    self.client
                        .curve_trade(
                            self.market,
                            account,
                            amount,
                            input.is_some(),
                            request.slippage_bps,
                        )
                        .await?
                } else {
                    self.client
                        .prepare_swap(
                            self.market,
                            account,
                            input.context("pool swaps require an ERC-20 input")?,
                            amount,
                            request.slippage_bps,
                        )
                        .await?
                }
            }
            ContractAction::Deposit { amount0, amount1 } => {
                self.require_pool()?;
                let (amount0, amount1) = self.deposit_units(amount0, amount1, &at).await?;
                serde_json::to_value(
                    self.client
                        .prepare_deposit(
                            self.market,
                            account,
                            amount0,
                            amount1,
                            request.slippage_bps,
                        )
                        .await?,
                )?
            }
            ContractAction::Withdraw { shares } => {
                self.require_pool()?;
                let decimals = self
                    .client
                    .call(self.market, Token::decimalsCall {}, &at)
                    .await?;
                self.client
                    .prepare_withdraw(
                        self.market,
                        account,
                        amount_units(shares, decimals)?,
                        request.slippage_bps,
                    )
                    .await?
            }
        };
        let transactions = if let Some(steps) = plan
            .as_object_mut()
            .context("missing prepared plan")?
            .remove("transactions")
        {
            serde_json::from_value(steps)?
        } else {
            let transaction = plan
                .as_object_mut()
                .unwrap()
                .remove("transaction")
                .context("missing prepared transaction")?;
            vec![ContractStep {
                action: "withdraw".into(),
                transaction: serde_json::from_value(transaction)?,
            }]
        };
        Ok(ContractPlan {
            market: self.market,
            account,
            chain_id: self.client.chain_id,
            quote: plan,
            transactions,
        })
    }

    fn validate_request(&self, request: &ContractRequest) -> Result<()> {
        ensure!(
            request.market == self.market,
            "contract request targets a different market"
        );
        ensure!(
            request.slippage_bps < 10_000,
            "slippage must be below 10000 bps"
        );
        Ok(())
    }

    fn require_pool(&self) -> Result<()> {
        ensure!(
            !self.is_curve(),
            "bonding markets do not support LP deposits or withdrawals"
        );
        Ok(())
    }

    async fn input_asset(&self, input: &str, at: &Value) -> Result<(Option<Address>, u8)> {
        if input.eq_ignore_ascii_case("native") {
            ensure!(
                self.is_curve(),
                "pool swaps require an ERC-20 input; wrap native HYPE first"
            );
            return Ok((None, 18));
        }
        let input = super::market_data::market_address(input)
            .context("token_in must be a token address or native")?;
        if self.is_curve() {
            ensure!(
                self.client
                    .call(self.market, Curve::tokenCall {}, at)
                    .await?
                    == input,
                "input token does not belong to this bonding market"
            );
        } else {
            let token0 = self
                .client
                .call(self.market, Pool::token0Call {}, at)
                .await?;
            let token1 = self
                .client
                .call(self.market, Pool::token1Call {}, at)
                .await?;
            ensure!(
                input == token0 || input == token1,
                "input token does not belong to this pool"
            );
        }
        Ok((
            Some(input),
            self.client.call(input, Token::decimalsCall {}, at).await?,
        ))
    }

    async fn deposit_units(
        &self,
        amount0: &str,
        amount1: &str,
        at: &Value,
    ) -> Result<(U256, U256)> {
        let token0 = self
            .client
            .call(self.market, Pool::token0Call {}, at)
            .await?;
        let token1 = self
            .client
            .call(self.market, Pool::token1Call {}, at)
            .await?;
        Ok((
            amount_units(
                amount0,
                self.client.call(token0, Token::decimalsCall {}, at).await?,
            )?,
            amount_units(
                amount1,
                self.client.call(token1, Token::decimalsCall {}, at).await?,
            )?,
        ))
    }
}

fn native_amount(amount: U256) -> Result<Value> {
    Ok(
        json!({"address": null, "symbol": "HYPE", "decimals": 18, "amount": TokenAmount::new(amount, 18)?}),
    )
}

/// Reject silently rounded amounts. Scripts use human-unit decimal strings, never floats.
fn amount_units(amount: &str, decimals: u8) -> Result<U256> {
    ensure!(decimals <= 77, "unsupported token decimals: {decimals}");
    let mut parts = amount.split('.');
    let whole = parts.next().unwrap_or("");
    let fraction = parts.next();
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || parts.next().is_some()
        || fraction.is_some_and(|part| {
            part.is_empty()
                || part.len() > usize::from(decimals)
                || !part.bytes().all(|b| b.is_ascii_digit())
        })
    {
        bail!("amount must be a positive decimal string with at most {decimals} decimal places");
    }
    let value: U256 = parse_units(amount, decimals)?.into();
    ensure!(!value.is_zero(), "amount must be positive");
    Ok(value)
}

#[async_trait::async_trait]
impl crate::providers::execution::ContractProvider for ElysiumContracts {
    fn wallet_address(&self) -> Result<Address> {
        Ok(crate::credentials::elysium::load()?.address())
    }
    async fn state(&self) -> Result<Value> {
        Self::state(self).await
    }
    async fn balances(&self, account: Address) -> Result<Value> {
        Self::balances(self, account).await
    }
    async fn quote(&self, request: &ContractRequest) -> Result<Value> {
        Self::quote(self, request).await
    }
    async fn prepare(&self, account: Address, request: &ContractRequest) -> Result<ContractPlan> {
        Self::prepare(self, account, request).await
    }
    async fn sign(
        &self,
        call: &crate::domain::contracts::WalletTransaction,
        gas_cap: U256,
    ) -> Result<crate::domain::contracts::SignedTransaction> {
        let signer = crate::credentials::elysium::load()?;
        self.client
            .prepare_transaction(call, &signer, gas_cap)
            .await
    }
    async fn broadcast(&self, signed: &crate::domain::contracts::SignedTransaction) -> Result<()> {
        self.client.broadcast_transaction(signed).await
    }
    async fn receipt(
        &self,
        hash: B256,
    ) -> Result<Option<crate::domain::contracts::TransactionReceipt>> {
        self.client.fee_receipt(hash).await
    }
    async fn known(&self, hash: B256) -> Result<bool> {
        self.client.fee_known(hash).await
    }
    async fn nonce(&self, account: Address) -> Result<u64> {
        self.client.operator_nonce(account).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_amounts_reject_rounding_negative_and_exponent_notation() {
        assert_eq!(amount_units("1.000001", 6).unwrap(), U256::from(1_000_001));
        assert!(amount_units("1", 255).is_err());
        for value in ["1.0000001", "-1", "+1", "1e3", "NaN", "0", "1.", ".1", " 1"] {
            assert!(amount_units(value, 6).is_err(), "accepted {value}");
        }
    }
}
