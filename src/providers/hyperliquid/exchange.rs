use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::HTTP_URL;
use super::signing::{HyperliquidWallet, WireSignature};
use super::ws::HyperliquidTradingClient;

static LAST_NONCE: AtomicU64 = AtomicU64::new(0);
pub const API_WALLET_NAME: &str = "marketlab";

#[derive(Clone)]
pub struct HyperliquidExchangeClient {
    trading: HyperliquidTradingClient,
    wallet: HyperliquidWallet,
}

impl HyperliquidExchangeClient {
    pub fn new(wallet: HyperliquidWallet) -> Result<Self> {
        Ok(Self {
            trading: HyperliquidTradingClient::shared(),
            wallet,
        })
    }

    pub async fn update_leverage(
        &self,
        asset: u32,
        leverage: u32,
        is_cross: bool,
    ) -> Result<ExchangeResponseStatus> {
        self.post_l1(Action::UpdateLeverage {
            asset,
            is_cross,
            leverage,
        })
        .await
    }

    pub async fn order(
        &self,
        orders: Vec<OrderRequest>,
        grouping: OrderGrouping,
    ) -> Result<ExchangeResponseStatus> {
        self.post_l1(Action::Order { orders, grouping }).await
    }

    pub async fn cancel(&self, asset: u32, oid: u64) -> Result<ExchangeResponseStatus> {
        self.post_l1(Action::Cancel {
            cancels: vec![CancelRequest { asset, oid }],
        })
        .await
    }

    async fn post_l1(&self, action: Action) -> Result<ExchangeResponseStatus> {
        let nonce = next_nonce()?;
        let signature = self.wallet.sign_l1_action(&action, nonce)?;
        let response = self
            .trading
            .post_action(&ExchangePayload {
                action,
                signature,
                nonce,
                vault_address: None,
            })
            .await?;
        serde_json::from_value(response).context("invalid Hyperliquid WebSocket exchange response")
    }
}

pub async fn approve_agent(
    master: &HyperliquidWallet,
) -> Result<(HyperliquidWallet, ExchangeResponseStatus)> {
    let agent = HyperliquidWallet::random();
    let nonce = next_nonce()?;
    let action = Action::ApproveAgent {
        signature_chain_id: "0x66eee".to_string(),
        hyperliquid_chain: "Testnet".to_string(),
        agent_address: agent.address(),
        agent_name: Some(API_WALLET_NAME.to_string()),
        nonce,
    };
    let signature = master.sign_approve_agent(agent.address_bytes(), API_WALLET_NAME, nonce)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to construct Hyperliquid authorization client")?;
    let response = post_exchange_http(&client, action, signature, nonce).await?;
    Ok((agent, response))
}

async fn post_exchange_http(
    client: &reqwest::Client,
    action: Action,
    signature: WireSignature,
    nonce: u64,
) -> Result<ExchangeResponseStatus> {
    let response = client
        .post(format!("{HTTP_URL}/exchange"))
        .json(&ExchangePayload {
            action,
            signature,
            nonce,
            vault_address: None,
        })
        .send()
        .await
        .context("failed to call Hyperliquid testnet exchange API")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read Hyperliquid exchange response")?;
    if !status.is_success() {
        bail!("Hyperliquid testnet exchange returned HTTP {status}: {body}");
    }
    serde_json::from_str(&body)
        .with_context(|| format!("invalid Hyperliquid exchange response: {body}"))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExchangePayload {
    action: Action,
    signature: WireSignature,
    nonce: u64,
    vault_address: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Action {
    UpdateLeverage {
        asset: u32,
        #[serde(rename = "isCross")]
        is_cross: bool,
        leverage: u32,
    },
    Order {
        orders: Vec<OrderRequest>,
        grouping: OrderGrouping,
    },
    Cancel {
        cancels: Vec<CancelRequest>,
    },
    ApproveAgent {
        #[serde(rename = "signatureChainId")]
        signature_chain_id: String,
        #[serde(rename = "hyperliquidChain")]
        hyperliquid_chain: String,
        #[serde(rename = "agentAddress")]
        agent_address: String,
        #[serde(rename = "agentName")]
        #[serde(skip_serializing_if = "Option::is_none")]
        agent_name: Option<String>,
        nonce: u64,
    },
}

#[derive(Clone, Copy, Debug, Serialize)]
pub enum OrderGrouping {
    #[serde(rename = "na")]
    None,
    #[serde(rename = "normalTpsl")]
    NormalTpSl,
}

#[derive(Clone, Debug, Serialize)]
pub struct OrderRequest {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "b")]
    pub is_buy: bool,
    #[serde(rename = "p")]
    pub limit_px: String,
    #[serde(rename = "s")]
    pub size: String,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "t")]
    pub order_type: WireOrder,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WireOrder {
    Limit {
        tif: String,
    },
    Trigger {
        #[serde(rename = "isMarket")]
        is_market: bool,
        #[serde(rename = "triggerPx")]
        trigger_px: String,
        tpsl: String,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct CancelRequest {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "o")]
    pub oid: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "status", content = "response")]
pub enum ExchangeResponseStatus {
    Ok(ExchangeResponse),
    Err(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExchangeResponse {
    #[serde(rename = "type")]
    pub response_type: String,
    pub data: Option<ExchangeDataStatuses>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExchangeDataStatuses {
    pub statuses: Vec<ExchangeDataStatus>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ExchangeDataStatus {
    Success,
    WaitingForFill,
    WaitingForTrigger,
    Error(String),
    Resting(RestingOrder),
    Filled(FilledOrder),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RestingOrder {
    pub oid: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FilledOrder {
    pub total_sz: String,
    pub avg_px: String,
    pub oid: u64,
}

pub fn wire_number(value: f64) -> String {
    let mut value = format!("{value:.8}");
    while value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    if value == "-0" {
        "0".to_string()
    } else {
        value
    }
}

fn next_nonce() -> Result<u64> {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let mut previous = LAST_NONCE.load(Ordering::Relaxed);
    loop {
        let next = now.max(previous.saturating_add(1));
        match LAST_NONCE.compare_exchange_weak(previous, next, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return Ok(next),
            Err(current) => previous = current,
        }
    }
}

pub fn response_error(response: &ExchangeResponseStatus) -> Option<String> {
    match response {
        ExchangeResponseStatus::Err(error) => Some(error.clone()),
        ExchangeResponseStatus::Ok(response) => response
            .data
            .as_ref()
            .into_iter()
            .flat_map(|data| &data.statuses)
            .find_map(|status| match status {
                ExchangeDataStatus::Error(error) => Some(error.clone()),
                _ => None,
            }),
    }
}

pub fn raw_response(response: &ExchangeResponseStatus) -> Value {
    serde_json::to_value(response).unwrap_or_else(|_| {
        serde_json::json!({
            "status": "serializationError"
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_action_matches_hyperliquid_wire_shape() {
        let action = Action::Order {
            orders: vec![OrderRequest {
                asset: 0,
                is_buy: true,
                limit_px: "65000".to_string(),
                size: "0.001".to_string(),
                reduce_only: false,
                order_type: WireOrder::Limit {
                    tif: "Alo".to_string(),
                },
            }],
            grouping: OrderGrouping::None,
        };
        let value = serde_json::to_value(action).expect("serializes");
        assert_eq!(value["type"], "order");
        assert_eq!(value["orders"][0]["a"], 0);
        assert_eq!(value["orders"][0]["t"]["limit"]["tif"], "Alo");
        assert_eq!(value["grouping"], "na");
    }

    #[test]
    fn normal_tpsl_and_named_agent_match_official_wire_shape() {
        assert_eq!(
            serde_json::to_value(OrderGrouping::NormalTpSl).expect("grouping serializes"),
            "normalTpsl"
        );
        let action = Action::ApproveAgent {
            signature_chain_id: "0x66eee".to_string(),
            hyperliquid_chain: "Testnet".to_string(),
            agent_address: "0x0000000000000000000000000000000000000001".to_string(),
            agent_name: Some(API_WALLET_NAME.to_string()),
            nonce: 1,
        };
        let value = serde_json::to_value(action).expect("agent serializes");
        assert_eq!(value["agentName"], API_WALLET_NAME);
        assert_eq!(value["signatureChainId"], "0x66eee");
        assert_eq!(value["hyperliquidChain"], "Testnet");
    }

    #[test]
    fn wire_numbers_are_canonical_and_bounded() {
        assert_eq!(wire_number(1.23000000), "1.23");
        assert_eq!(wire_number(-0.0), "0");
        assert_eq!(wire_number(0.00000001), "0.00000001");
    }
}
