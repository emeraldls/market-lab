use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, U64, U256, utils::format_units};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub mod transactions;

sol! {
    interface PoolFactory {
        function isPool(address pool) external view returns (bool registered);
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
    }

    interface Token {
        function symbol() external view returns (string symbol);
        function decimals() external view returns (uint8 decimals);
        function totalSupply() external view returns (uint256 supply);
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
        // Every value must come from this same canonical block, not successive moving heads.
        let at = json!({ "blockHash": block.hash, "requireCanonical": true });
        let registered = self
            .call(self.factory, PoolFactory::isPoolCall { pool }, &at)
            .await?;
        ensure!(
            registered,
            "{pool} is not registered with the MarketLab pool factory"
        );

        let token0 = self.call(pool, Pool::token0Call {}, &at).await?;
        let token1 = self.call(pool, Pool::token1Call {}, &at).await?;
        let reserves = self.call(pool, Pool::getReservesCall {}, &at).await?;
        let first = self.reserve(token0, reserves.reserve0, &at).await?;
        let second = self.reserve(token1, reserves.reserve1, &at).await?;
        let manager = self.call(pool, Pool::ownerCall {}, &at).await?;
        let pending = self.call(pool, Pool::pendingOwnerCall {}, &at).await?;
        let operator = self.call(pool, Pool::operatorCall {}, &at).await?;
        let operator_hype_balance = if operator.is_zero() {
            None
        } else {
            let balance = self.rpc("eth_getBalance", json!([operator, at])).await?;
            Some(TokenAmount::new(balance, 18)?)
        };
        let fee_bps = self.call(pool, Pool::feeBpsCall {}, &at).await?;
        let min_fee_bps = self.call(pool, Pool::minFeeBpsCall {}, &at).await?;
        let max_fee_bps = self.call(pool, Pool::maxFeeBpsCall {}, &at).await?;
        let lp_decimals = self.call(pool, Token::decimalsCall {}, &at).await?;
        let supply = self.call(pool, Token::totalSupplyCall {}, &at).await?;

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

    async fn rpc<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let response: RpcResponse = self
            .http
            .post(self.rpc_url.clone())
            .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("Elysium {method} request failed"))?
            .error_for_status()
            .map_err(reqwest::Error::without_url)?
            .json()
            .await
            .with_context(|| format!("invalid Elysium {method} response"))?;
        ensure!(
            response.id == 1 && response.jsonrpc == "2.0",
            "invalid Elysium RPC response identity"
        );
        if let Some(error) = response.error {
            bail!(
                "Elysium {method} failed ({}): {}",
                error.code,
                error.message
            );
        }
        serde_json::from_value(response.result)
            .with_context(|| format!("Elysium {method} returned an invalid result"))
    }
}
