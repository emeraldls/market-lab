//! Bonding-market reads and unsigned wallet transactions; pricing stays in Solidity.
use super::wallet::{WalletStep, WalletTransaction, minimum_amount};
use super::*;

sol! {
    interface CurveFactory {
        function marketCount() external view returns (uint256 count);
        function markets(uint256 index) external view returns (address market);
        function isMarket(address market) external view returns (bool registered);
        function createMarket(address token, uint256 allocation, uint256 startPrice, uint256 endPrice, uint16 feeBps, uint16 minFeeBps, uint16 maxFeeBps) external returns (address market);
        event MarketCreated(address indexed market, address indexed manager, address indexed token);
    }
    interface Curve {
        function token() external view returns (address asset);
        function allocation() external view returns (uint256 amount);
        function sold() external view returns (uint256 amount);
        function startPrice() external view returns (uint256 amount);
        function endPrice() external view returns (uint256 amount);
        function currentPrice() external view returns (uint256 amount);
        function backing() external view returns (uint256 amount);
        function accruedFees() external view returns (uint256 amount);
        function quoteBuy(uint256 tokens) external view returns (uint256 hype, uint256 fee);
        function quoteBuyHype(uint256 budget) external view returns (uint256 tokens, uint256 hype, uint256 fee);
        function quoteSell(uint256 tokens) external view returns (uint256 hype, uint256 fee);
        function buyHype(uint256 minimumTokens, address recipient, uint256 deadline) external payable;
        function sell(uint256 tokens, uint256 minimumHype, address recipient, uint256 deadline) external;
        function claimFees(address recipient) external;
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FeeMarket {
    #[default]
    Pool,
    Curve {
        factory: Address,
    },
}

#[derive(Debug)]
pub struct CurveCreate {
    pub token: Address,
    pub allocation: U256,
    pub start_price: U256,
    pub end_price: U256,
    pub fee_bps: u16,
    pub min_fee_bps: u16,
    pub max_fee_bps: u16,
}

impl PoolClient {
    pub fn for_market(rpc_url: Option<Url>, market: FeeMarket) -> Result<Self> {
        let mut client = Self::new(rpc_url)?;
        if let FeeMarket::Curve { factory } = market {
            ensure!(!factory.is_zero(), "curve factory must not be zero");
            client.factory = factory;
        }
        client.market = market;
        Ok(client)
    }

    pub(super) async fn observe_curve(&self, market: Address) -> Result<transactions::Observation> {
        let (block, at) = self.curve_block(market).await?;
        let values = self
            .calls(
                &[
                    (market, Curve::currentPriceCall {}.abi_encode()),
                    (market, Pool::operatorCall {}.abi_encode()),
                    (market, Pool::feeBpsCall {}.abi_encode()),
                    (market, Pool::minFeeBpsCall {}.abi_encode()),
                    (market, Pool::maxFeeBpsCall {}.abi_encode()),
                ],
                &at,
            )
            .await?;
        Ok(transactions::Observation {
            block: block.number.to(),
            block_hash: block.hash,
            timestamp: block.timestamp.to(),
            reserve0: U256::ZERO,
            reserve1: U256::ZERO,
            spot_price: Some(decode::<Curve::currentPriceCall>(&values[0])?),
            operator: decode::<Pool::operatorCall>(&values[1])?,
            fee_bps: decode::<Pool::feeBpsCall>(&values[2])?,
            min_fee_bps: decode::<Pool::minFeeBpsCall>(&values[3])?,
            max_fee_bps: decode::<Pool::maxFeeBpsCall>(&values[4])?,
        })
    }

    async fn curve_block(&self, market: Address) -> Result<(Block, Value)> {
        ensure!(
            matches!(self.market, FeeMarket::Curve { .. }),
            "expected a curve factory"
        );
        let block = self.wallet_block().await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        ensure!(
            self.call(self.factory, CurveFactory::isMarketCall { market }, &at)
                .await?,
            "market is not registered with this curve factory"
        );
        Ok((block, at))
    }

    pub async fn curve_inspect(&self, market: Address) -> Result<Value> {
        let (block, at) = self.curve_block(market).await?;
        self.curve_snapshot(market, &block, &at).await
    }

    pub(super) async fn curve_snapshot(
        &self,
        market: Address,
        block: &Block,
        at: &Value,
    ) -> Result<Value> {
        let values = self
            .calls(
                &[
                    (market, Curve::tokenCall {}.abi_encode()),
                    (market, Curve::allocationCall {}.abi_encode()),
                    (market, Curve::soldCall {}.abi_encode()),
                    (market, Curve::startPriceCall {}.abi_encode()),
                    (market, Curve::endPriceCall {}.abi_encode()),
                    (market, Curve::currentPriceCall {}.abi_encode()),
                    (market, Curve::backingCall {}.abi_encode()),
                    (market, Curve::accruedFeesCall {}.abi_encode()),
                    (market, Pool::ownerCall {}.abi_encode()),
                    (market, Pool::operatorCall {}.abi_encode()),
                    (market, Pool::feeBpsCall {}.abi_encode()),
                    (market, Pool::minFeeBpsCall {}.abi_encode()),
                    (market, Pool::maxFeeBpsCall {}.abi_encode()),
                ],
                at,
            )
            .await?;
        let token = decode::<Curve::tokenCall>(&values[0])?;
        let allocation = decode::<Curve::allocationCall>(&values[1])?;
        let asset = self.reserve(token, allocation, at).await?;
        Ok(json!({
            "chain_id": self.chain_id, "block_hash": block.hash, "block_number": block.number,
            "market": market, "factory": self.factory, "token": asset,
            "sold": TokenAmount::new(decode::<Curve::soldCall>(&values[2])?, asset.decimals)?,
            "start_price": TokenAmount::new(decode::<Curve::startPriceCall>(&values[3])?, 18)?,
            "end_price": TokenAmount::new(decode::<Curve::endPriceCall>(&values[4])?, 18)?,
            "current_price": TokenAmount::new(decode::<Curve::currentPriceCall>(&values[5])?, 18)?,
            "backing": TokenAmount::new(decode::<Curve::backingCall>(&values[6])?, 18)?,
            "accrued_fees": TokenAmount::new(decode::<Curve::accruedFeesCall>(&values[7])?, 18)?,
            "manager": decode::<Pool::ownerCall>(&values[8])?,
            "operator": decode::<Pool::operatorCall>(&values[9])?,
            "fee_bps": decode::<Pool::feeBpsCall>(&values[10])?,
            "min_fee_bps": decode::<Pool::minFeeBpsCall>(&values[11])?,
            "max_fee_bps": decode::<Pool::maxFeeBpsCall>(&values[12])?,
        }))
    }

    pub async fn curve_list(&self, offset: u64, limit: u64) -> Result<Value> {
        ensure!((1..=50).contains(&limit), "limit must be between 1 and 50");
        let block = self.wallet_block().await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let total = self
            .call(self.factory, CurveFactory::marketCountCall {}, &at)
            .await?;
        let total: u64 = total.try_into().context("curve count exceeds u64")?;
        let end = total.saturating_sub(offset);
        let start = end.saturating_sub(limit);
        let mut markets = Vec::new();
        for index in (start..end).rev() {
            let market = self
                .call(
                    self.factory,
                    CurveFactory::marketsCall {
                        index: U256::from(index),
                    },
                    &at,
                )
                .await?;
            markets.push(self.curve_snapshot(market, &block, &at).await?);
        }
        Ok(
            json!({ "markets": markets, "total": total, "offset": offset, "block_hash": block.hash }),
        )
    }

    pub async fn curve_create(&self, account: Address, config: CurveCreate) -> Result<Value> {
        ensure!(!account.is_zero(), "manager wallet must not be zero");
        ensure!(
            config.min_fee_bps <= config.fee_bps
                && config.fee_bps <= config.max_fee_bps
                && config.max_fee_bps <= 1000,
            "invalid fee bounds"
        );
        let max = U256::from((1_u128 << 96) - 1);
        ensure!(
            config.allocation > U256::ZERO
                && config.allocation <= max
                && config.start_price > U256::ZERO
                && config.end_price >= config.start_price
                && config.end_price <= max,
            "invalid allocation or price range"
        );
        let block = self.wallet_block().await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let _: U256 = self
            .call(self.factory, CurveFactory::marketCountCall {}, &at)
            .await?;
        let token = self.reserve(config.token, config.allocation, &at).await?;
        ensure!(
            token.decimals <= 18,
            "curve tokens must have at most 18 decimals"
        );
        let mut transactions = self
            .approvals(self.factory, account, config.token, config.allocation, &at)
            .await?;
        let transaction = WalletTransaction::new(
            self.chain_id,
            account,
            self.factory,
            CurveFactory::createMarketCall {
                token: config.token,
                allocation: config.allocation,
                startPrice: config.start_price,
                endPrice: config.end_price,
                feeBps: config.fee_bps,
                minFeeBps: config.min_fee_bps,
                maxFeeBps: config.max_fee_bps,
            },
        );
        // eth_call cannot simulate approvals in preceding steps. Simulate when allowance exists.
        if transactions.is_empty() {
            let _: Bytes = self.rpc("eth_call", json!([transaction, at])).await?;
        }
        transactions.push(WalletStep {
            action: "create_curve",
            transaction,
        });
        Ok(
            json!({ "chain_id": self.chain_id, "factory": self.factory, "account": account,
            "block_hash": block.hash, "token": token, "transactions": transactions }),
        )
    }

    pub async fn curve_quote(&self, market: Address, amount: U256, sell: bool) -> Result<Value> {
        let (block, at) = self.curve_block(market).await?;
        let token = self.call(market, Curve::tokenCall {}, &at).await?;
        let (tokens, hype, fee) = if sell {
            let quote = self
                .call(market, Curve::quoteSellCall { tokens: amount }, &at)
                .await?;
            (amount, quote.hype, quote.fee)
        } else {
            let quote = self
                .call(market, Curve::quoteBuyHypeCall { budget: amount }, &at)
                .await?;
            (quote.tokens, quote.hype, quote.fee)
        };
        Ok(
            json!({ "chain_id": self.chain_id, "block_hash": block.hash, "market": market,
            "side": if sell { "sell" } else { "buy" },
            "tokens": self.reserve(token, tokens, &at).await?,
            "hype": TokenAmount::new(hype, 18)?, "fee": TokenAmount::new(fee, 18)? }),
        )
    }

    pub async fn curve_trade(
        &self,
        market: Address,
        account: Address,
        amount: U256,
        sell: bool,
        slippage: u16,
    ) -> Result<Value> {
        ensure!(!account.is_zero(), "wallet must not be zero");
        let (block, at) = self.curve_block(market).await?;
        let token = self.call(market, Curve::tokenCall {}, &at).await?;
        let deadline = block
            .timestamp
            .to::<u64>()
            .checked_add(1200)
            .context("deadline overflow")?;
        let mut transactions;
        let details;
        if sell {
            let quote = self
                .call(market, Curve::quoteSellCall { tokens: amount }, &at)
                .await?;
            let minimum = minimum_amount(quote.hype, slippage)?;
            ensure!(minimum > U256::ZERO, "sell amount too small");
            transactions = self.approvals(market, account, token, amount, &at).await?;
            transactions.push(WalletStep {
                action: "sell",
                transaction: WalletTransaction::new(
                    self.chain_id,
                    account,
                    market,
                    Curve::sellCall {
                        tokens: amount,
                        minimumHype: minimum,
                        recipient: account,
                        deadline: U256::from(deadline),
                    },
                ),
            });
            details = json!({ "tokens": self.reserve(token, amount, &at).await?,
                "expected_hype": TokenAmount::new(quote.hype, 18)?,
                "minimum_hype": TokenAmount::new(minimum, 18)?, "fee": TokenAmount::new(quote.fee, 18)? });
        } else {
            let quote = self
                .call(market, Curve::quoteBuyHypeCall { budget: amount }, &at)
                .await?;
            let minimum = minimum_amount(quote.tokens, slippage)?;
            ensure!(minimum > U256::ZERO, "buy amount too small");
            let balance: U256 = self.rpc("eth_getBalance", json!([account, at])).await?;
            ensure!(
                balance > amount,
                "wallet needs enough HYPE for the payment plus gas"
            );
            let mut transaction = WalletTransaction::new(
                self.chain_id,
                account,
                market,
                Curve::buyHypeCall {
                    minimumTokens: minimum,
                    recipient: account,
                    deadline: U256::from(deadline),
                },
            );
            transaction.value = amount;
            let _: Bytes = self.rpc("eth_call", json!([transaction, at])).await?;
            transactions = vec![WalletStep {
                action: "buy",
                transaction,
            }];
            details = json!({ "minimum_tokens": self.reserve(token, minimum, &at).await?, "expected_tokens": self.reserve(token, quote.tokens, &at).await?,
                "maximum_hype": TokenAmount::new(amount, 18)?, "fee": TokenAmount::new(quote.fee, 18)?, "unused_hype_refunded": true });
        }
        Ok(
            json!({ "chain_id": self.chain_id, "market": market, "account": account,
            "block_hash": block.hash, "deadline": deadline, "quote": details, "transactions": transactions }),
        )
    }

    pub async fn curve_manage(
        &self,
        market: Address,
        account: Address,
        operator: Option<Address>,
    ) -> Result<Value> {
        let (block, at) = self.curve_block(market).await?;
        ensure!(
            !account.is_zero() && self.call(market, Pool::ownerCall {}, &at).await? == account,
            "only the curve manager can perform this action"
        );
        let (action, transaction) = match operator {
            Some(operator) => (
                "authorize",
                WalletTransaction::new(
                    self.chain_id,
                    account,
                    market,
                    Pool::setOperatorCall { operator },
                ),
            ),
            None => (
                "claim_fees",
                WalletTransaction::new(
                    self.chain_id,
                    account,
                    market,
                    Curve::claimFeesCall { recipient: account },
                ),
            ),
        };
        let _: Bytes = self.rpc("eth_call", json!([transaction, at])).await?;
        Ok(
            json!({ "chain_id": self.chain_id, "market": market, "block_hash": block.hash,
            "transactions": [WalletStep { action, transaction }] }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_targets_roundtrip_and_pool_stays_default() {
        let target = FeeMarket::Curve {
            factory: Address::repeat_byte(1),
        };
        assert_eq!(
            serde_json::from_value::<FeeMarket>(serde_json::to_value(&target).unwrap()).unwrap(),
            target
        );
        assert_eq!(FeeMarket::default(), FeeMarket::Pool);
    }

    #[test]
    fn curve_fee_calls_match_existing_signer_abi() {
        assert_eq!(Pool::setFeeCall::SIGNATURE, "setFee(uint16)");
        assert_eq!(Pool::setOperatorCall::SIGNATURE, "setOperator(address)");
        let buy = Curve::buyHypeCall {
            minimumTokens: U256::from(42),
            recipient: Address::repeat_byte(2),
            deadline: U256::from(123),
        };
        let decoded = Curve::buyHypeCall::abi_decode_validate(&buy.abi_encode()).unwrap();
        assert_eq!(decoded.minimumTokens, U256::from(42));
        assert_eq!(decoded.recipient, Address::repeat_byte(2));
    }

    #[test]
    fn old_pool_start_requests_remain_readable() {
        let policy = json!({ "base_fee_bps": 30, "sensitivity": 1.0, "window": 20,
            "interval": 30, "cooldown": 300, "min_change_bps": 2, "max_tx_gas_hype": "0.0001" });
        let request = json!({ "action": "start", "pool": Address::repeat_byte(3), "rpc_url": null, "policy": policy });
        let request: crate::runtime::pools::PoolRequest = serde_json::from_value(request).unwrap();
        assert!(matches!(
            request,
            crate::runtime::pools::PoolRequest::Start {
                market: FeeMarket::Pool,
                ..
            }
        ));
    }
}
