use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, U64, U256, utils::format_units};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, ensure};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

mod rpc;
pub mod token;
pub mod transactions;
pub mod wallet;

sol! {
    interface PoolFactory {
        function poolCount() external view returns (uint256 count);
        function pools(uint256 index) external view returns (address pool);
        function isPool(address pool) external view returns (bool registered);
        function createPool(address tokenA, address tokenB, uint16 initialFeeBps, uint16 minFeeBps, uint16 maxFeeBps) external returns (address pool);
        event PoolCreated(address indexed pool, address indexed manager, address token0, address token1, uint16 minFeeBps, uint16 maxFeeBps);
    }

    interface Pool {
        function token0() external view returns (address token);
        function token1() external view returns (address token);
        function getReserves() external view returns (uint256 reserve0, uint256 reserve1);
        function owner() external view returns (address manager);
        function pendingOwner() external view returns (address manager);
        function operator() external view returns (address account);
        function feeBps() external view returns (uint16 fee);
        function minFeeBps() external view returns (uint16 fee);
        function maxFeeBps() external view returns (uint16 fee);
        function setFee(uint16 feeBps) external;
        function setOperator(address operator) external;
        function quoteSwap(address tokenIn, uint256 amountIn) external view returns (uint256 amountOut, uint256 feeAmount);
        function swapExactInput(address tokenIn, uint256 amountIn, uint256 minAmountOut, address recipient, uint256 deadline) external returns (uint256 amountOut);
        function previewWithdraw(uint256 shares) external view returns (uint256 amount0, uint256 amount1);
        function withdraw(uint256 shares, uint256 min0, uint256 min1, address recipient, uint256 deadline) external returns (uint256 amount0, uint256 amount1);
        function previewDeposit(uint256 max0, uint256 max1) external view returns (uint256 amount0, uint256 amount1, uint256 shares);
        function deposit(uint256 max0, uint256 max1, uint256 minShares, address recipient, uint256 deadline) external returns (uint256 amount0, uint256 amount1, uint256 shares);
    }

    interface Token {
        function symbol() external view returns (string symbol);
        function decimals() external view returns (uint8 decimals);
        function totalSupply() external view returns (uint256 supply);
        function balanceOf(address account) external view returns (uint256 balance);
        function allowance(address owner, address spender) external view returns (uint256 amount);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Deployment {
    chain_id: u64,
    rpc_url: String,
    contracts: DeploymentContracts,
}

#[derive(Deserialize)]
struct DeploymentContracts {
    #[serde(rename = "PoolFactory")]
    factory: ContractAddress,
}

#[derive(Deserialize)]
struct ContractAddress {
    address: Address,
}

#[derive(Debug, Serialize)]
pub struct TokenAmount {
    pub raw: String,
    pub formatted: String,
}

impl TokenAmount {
    fn new(raw: U256, decimals: u8) -> Result<Self> {
        let mut formatted = format_units(raw, decimals).context("unsupported token decimals")?;
        if formatted.contains('.') {
            formatted = formatted
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_owned();
        }
        Ok(Self {
            raw: raw.to_string(),
            formatted,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct PoolReserve {
    pub address: Address,
    pub symbol: String,
    pub decimals: u8,
    pub amount: TokenAmount,
}

#[derive(Debug, Serialize)]
pub struct PoolSnapshot {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: B256,
    pub address: Address,
    pub factory: Address,
    pub manager: Address,
    pub pending_manager: Option<Address>,
    pub operator: Option<Address>,
    pub operator_hype_balance: Option<TokenAmount>,
    pub fee_bps: u16,
    pub min_fee_bps: u16,
    pub max_fee_bps: u16,
    pub reserves: [PoolReserve; 2],
    pub lp_decimals: u8,
    pub lp_supply: TokenAmount,
}

#[derive(Deserialize)]
struct Block {
    number: U64,
    hash: B256,
    timestamp: U64,
}

#[derive(Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    id: u64,
    #[serde(default)]
    result: Value,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

pub struct PoolClient {
    http: Client,
    rpc_url: Url,
    chain_id: u64,
    factory: Address,
}

impl PoolClient {
    pub fn new(rpc_url: Option<Url>) -> Result<Self> {
        let deployment: Deployment = serde_json::from_str(include_str!(
            "../../../contracts/pools/deployments/99801.json"
        ))
        .context("invalid embedded Elysium deployment")?;
        let rpc_url = match rpc_url {
            Some(url) => url,
            None => deployment
                .rpc_url
                .parse()
                .context("invalid Elysium RPC URL")?,
        };
        ensure!(
            matches!(rpc_url.scheme(), "http" | "https"),
            "RPC URL must use HTTP or HTTPS"
        );
        Ok(Self {
            http: Client::builder().timeout(Duration::from_secs(15)).build()?,
            rpc_url,
            chain_id: deployment.chain_id,
            factory: deployment.contracts.factory.address,
        })
    }

    pub async fn inspect(&self, pool: Address) -> Result<PoolSnapshot> {
        let chain: U64 = self.rpc("eth_chainId", json!([])).await?;
        ensure!(
            chain.to::<u64>() == self.chain_id,
            "RPC is on chain {chain}; expected Elysium chain {}",
            self.chain_id
        );
        let block: Block = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        self.inspect_at(pool, &block).await
    }

    async fn inspect_at(&self, pool: Address, block: &Block) -> Result<PoolSnapshot> {
        // Every value must come from this same canonical block, not successive moving heads.
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let registered = self
            .call(self.factory, PoolFactory::isPoolCall { pool }, &at)
            .await?;
        ensure!(
            registered,
            "{pool} is not registered with the MarketLab pool factory"
        );

        self.snapshot_at(pool, block).await
    }

    async fn snapshot_at(&self, pool: Address, block: &Block) -> Result<PoolSnapshot> {
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let values = self
            .calls(
                &[
                    (pool, Pool::token0Call {}.abi_encode()),
                    (pool, Pool::token1Call {}.abi_encode()),
                    (pool, Pool::getReservesCall {}.abi_encode()),
                    (pool, Pool::ownerCall {}.abi_encode()),
                    (pool, Pool::pendingOwnerCall {}.abi_encode()),
                    (pool, Pool::operatorCall {}.abi_encode()),
                    (pool, Pool::feeBpsCall {}.abi_encode()),
                    (pool, Pool::minFeeBpsCall {}.abi_encode()),
                    (pool, Pool::maxFeeBpsCall {}.abi_encode()),
                    (pool, Token::decimalsCall {}.abi_encode()),
                    (pool, Token::totalSupplyCall {}.abi_encode()),
                ],
                &at,
            )
            .await?;
        let token0 = decode::<Pool::token0Call>(&values[0])?;
        let token1 = decode::<Pool::token1Call>(&values[1])?;
        let reserves = decode::<Pool::getReservesCall>(&values[2])?;
        let manager = decode::<Pool::ownerCall>(&values[3])?;
        let pending = decode::<Pool::pendingOwnerCall>(&values[4])?;
        let operator = decode::<Pool::operatorCall>(&values[5])?;
        let fee_bps = decode::<Pool::feeBpsCall>(&values[6])?;
        let min_fee_bps = decode::<Pool::minFeeBpsCall>(&values[7])?;
        let max_fee_bps = decode::<Pool::maxFeeBpsCall>(&values[8])?;
        let lp_decimals = decode::<Token::decimalsCall>(&values[9])?;
        let supply = decode::<Token::totalSupplyCall>(&values[10])?;
        let tokens = self
            .calls(
                &[
                    (token0, Token::symbolCall {}.abi_encode()),
                    (token0, Token::decimalsCall {}.abi_encode()),
                    (token1, Token::symbolCall {}.abi_encode()),
                    (token1, Token::decimalsCall {}.abi_encode()),
                ],
                &at,
            )
            .await?;
        let reserve = |address, amount, symbol: &Bytes, decimals: &Bytes| -> Result<PoolReserve> {
            let decimals = decode::<Token::decimalsCall>(decimals)?;
            Ok(PoolReserve {
                address,
                symbol: decode::<Token::symbolCall>(symbol)?,
                decimals,
                amount: TokenAmount::new(amount, decimals)?,
            })
        };
        let first = reserve(token0, reserves.reserve0, &tokens[0], &tokens[1])?;
        let second = reserve(token1, reserves.reserve1, &tokens[2], &tokens[3])?;
        let operator_hype_balance = if operator.is_zero() {
            None
        } else {
            let balance = self.rpc("eth_getBalance", json!([operator, at])).await?;
            Some(TokenAmount::new(balance, 18)?)
        };

        Ok(PoolSnapshot {
            chain_id: self.chain_id,
            block_number: block.number.to(),
            block_hash: block.hash,
            address: pool,
            factory: self.factory,
            manager,
            pending_manager: (!pending.is_zero()).then_some(pending),
            operator: (!operator.is_zero()).then_some(operator),
            operator_hype_balance,
            fee_bps,
            min_fee_bps,
            max_fee_bps,
            reserves: [first, second],
            lp_decimals,
            lp_supply: TokenAmount::new(supply, lp_decimals)?,
        })
    }

    pub async fn list(&self, offset: u64, limit: u64) -> Result<Value> {
        ensure!(
            (1..=50).contains(&limit),
            "pool page limit must be between 1 and 50"
        );
        let block = self.wallet_block().await?;
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let total: u64 = self
            .call(self.factory, PoolFactory::poolCountCall {}, &at)
            .await?
            .try_into()
            .context("pool count exceeds supported range")?;
        let end = offset.saturating_add(limit).min(total);
        let requests: Vec<_> = (offset..end)
            .map(|index| {
                (
                    self.factory,
                    PoolFactory::poolsCall {
                        index: U256::from(total - index - 1),
                    }
                    .abi_encode(),
                )
            })
            .collect();
        let addresses = self.calls(&requests, &at).await?;
        let mut pools = Vec::with_capacity(addresses.len());
        // Factory enumeration already verifies membership. Keep snapshots sequential
        // so a cold list does not fan out into four competing RPC bursts.
        for address in addresses {
            let pool = decode::<PoolFactory::poolsCall>(&address)?;
            pools.push(self.snapshot_at(pool, &block).await?);
        }
        Ok(json!({
            "chain_id": self.chain_id, "factory": self.factory,
            "block_number": block.number.to::<u64>(), "block_hash": block.hash,
            "total": total, "offset": offset, "limit": limit, "pools": pools
        }))
    }

    async fn reserve(&self, address: Address, amount: U256, at: &Value) -> Result<PoolReserve> {
        let symbol = self.call(address, Token::symbolCall {}, at).await?;
        let decimals = self.call(address, Token::decimalsCall {}, at).await?;
        Ok(PoolReserve {
            address,
            symbol,
            decimals,
            amount: TokenAmount::new(amount, decimals)?,
        })
    }

    async fn call<C: SolCall>(&self, address: Address, call: C, at: &Value) -> Result<C::Return> {
        let data: Bytes = call.abi_encode().into();
        let result: Bytes = self
            .rpc("eth_call", json!([{ "to": address, "data": data }, at]))
            .await
            .with_context(|| format!("{} at {address} failed", C::SIGNATURE))?;
        C::abi_decode_returns_validate(&result)
            .with_context(|| format!("invalid {} response from {address}", C::SIGNATURE))
    }
}

fn decode<C: SolCall>(value: &Bytes) -> Result<C::Return> {
    C::abi_decode_returns_validate(value)
        .with_context(|| format!("invalid {} response", C::SIGNATURE))
}
