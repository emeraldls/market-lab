use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::HyperliquidNetwork;
use super::client::HyperliquidClient;
use super::signing::{HyperliquidWallet, WireSignature};
use super::ws::HyperliquidTradingClient;

static LAST_NONCE: AtomicU64 = AtomicU64::new(0);
pub const MAINNET_API_WALLET_NAME: &str = "mlab-mainnet";
pub const TESTNET_API_WALLET_NAME: &str = "mlab-testnet";
pub const LEGACY_TESTNET_API_WALLET_NAME: &str = "marketlab";
pub const HYPERLINK_API_WALLET_NAME: &str = "mlab";
const HYPERLINK_SIGNATURE_CHAIN_ID: u64 = 42_161;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExchangeBackend {
    Hyperliquid,
    Hyperlink,
}

#[derive(Clone)]
pub struct HyperliquidExchangeClient {
    trading: HyperliquidTradingClient,
    wallet: HyperliquidWallet,
    network: HyperliquidNetwork,
    vault_address: Option<String>,
    backend: ExchangeBackend,
}

impl HyperliquidExchangeClient {
    pub fn new(wallet: HyperliquidWallet, network: HyperliquidNetwork) -> Result<Self> {
        Ok(Self {
            trading: HyperliquidTradingClient::shared(network),
            wallet,
            network,
            vault_address: None,
            backend: ExchangeBackend::Hyperliquid,
        })
    }

    pub fn for_subaccount(
        wallet: HyperliquidWallet,
        network: HyperliquidNetwork,
        vault_address: String,
    ) -> Result<Self> {
        let vault_address = super::signing::canonical_address(&vault_address)?;
        Ok(Self {
            trading: HyperliquidTradingClient::shared(network),
            wallet,
            network,
            vault_address: Some(vault_address),
            backend: ExchangeBackend::Hyperliquid,
        })
    }

    pub fn for_hyperlink(wallet: HyperliquidWallet) -> Result<Self> {
        Ok(Self {
            trading: HyperliquidTradingClient::shared_hyperlink(),
            wallet,
            network: HyperliquidNetwork::Mainnet,
            vault_address: None,
            backend: ExchangeBackend::Hyperlink,
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

    pub async fn user_outcome(&self, action: UserOutcomeAction) -> Result<ExchangeResponseStatus> {
        self.post_l1(user_outcome_request(action)).await
    }

    pub async fn cancel(&self, asset: u32, oid: u64) -> Result<ExchangeResponseStatus> {
        self.cancel_many(vec![CancelRequest { asset, oid }]).await
    }

    pub async fn cancel_fast(&self, asset: u32, oid: u64) -> Result<ExchangeResponseStatus> {
        self.cancel_many_fast(vec![CancelRequest { asset, oid }])
            .await
    }

    pub async fn cancel_many(&self, cancels: Vec<CancelRequest>) -> Result<ExchangeResponseStatus> {
        self.cancel_many_with_priority(cancels, false).await
    }

    pub async fn cancel_many_fast(
        &self,
        cancels: Vec<CancelRequest>,
    ) -> Result<ExchangeResponseStatus> {
        self.cancel_many_with_priority(cancels, true).await
    }

    pub async fn signed_read(&self, action: serde_json::Value) -> Result<serde_json::Value> {
        if self.backend != ExchangeBackend::Hyperlink {
            bail!("signed private reads are only available through HyperLink");
        }
        let response = self.post_l1_raw(action).await?;
        hyperlink_read_data(response)
    }

    async fn cancel_many_with_priority(
        &self,
        cancels: Vec<CancelRequest>,
        fast: bool,
    ) -> Result<ExchangeResponseStatus> {
        self.post_l1(Action::Cancel {
            cancels,
            fast: fast.then_some(true),
        })
        .await
    }

    async fn post_l1(&self, action: impl Serialize) -> Result<ExchangeResponseStatus> {
        let response = self.post_l1_raw(action).await?;
        serde_json::from_value(response).with_context(|| {
            format!(
                "invalid {} WebSocket exchange response",
                match self.backend {
                    ExchangeBackend::Hyperliquid => "Hyperliquid",
                    ExchangeBackend::Hyperlink => "HyperLink",
                }
            )
        })
    }

    async fn post_l1_raw(&self, action: impl Serialize) -> Result<serde_json::Value> {
        let nonce = next_nonce()?;
        let signature = self.wallet.sign_l1_action_for(
            &action,
            nonce,
            self.network,
            self.vault_address.as_deref(),
        )?;
        self.trading
            .post_action(&ExchangePayload {
                action,
                signature,
                nonce,
                vault_address: self.vault_address.clone(),
            })
            .await
    }
}

fn hyperlink_read_data(response: serde_json::Value) -> Result<serde_json::Value> {
    match response.get("status").and_then(serde_json::Value::as_str) {
        Some("ok") => {}
        Some("error") => {
            let error = response
                .pointer("/response/data")
                .or_else(|| response.get("response"))
                .unwrap_or(&response);
            bail!("HyperLink rejected private read: {error}");
        }
        Some(_) => bail!("HyperLink private read returned an unexpected payload: {response}"),
        None => {
            if response.get("type").and_then(serde_json::Value::as_str) == Some("error")
                || response.get("error").is_some()
            {
                bail!("HyperLink rejected private read: {response}");
            }
            if response.is_object() || response.is_array() {
                return Ok(response);
            }
            bail!("HyperLink private read returned an unexpected payload: {response}");
        }
    }
    let inner = response
        .get("response")
        .context("HyperLink private read omitted its response")?;
    if inner.get("type").and_then(serde_json::Value::as_str) == Some("error") {
        bail!("HyperLink rejected private read: {inner}");
    }
    Ok(inner.get("data").cloned().unwrap_or_else(|| inner.clone()))
}

fn user_outcome_request(action: UserOutcomeAction) -> UserOutcomeRequest {
    match action {
        UserOutcomeAction::Split { outcome, amount } => UserOutcomeRequest {
            action_type: "userOutcome",
            split_outcome: Some(OutcomeAmountRequest { outcome, amount }),
            merge_outcome: None,
            merge_question: None,
            negate_outcome: None,
        },
        UserOutcomeAction::Merge { outcome, amount } => UserOutcomeRequest {
            action_type: "userOutcome",
            split_outcome: None,
            merge_outcome: Some(OutcomeOptionalAmountRequest { outcome, amount }),
            merge_question: None,
            negate_outcome: None,
        },
        UserOutcomeAction::MergeQuestion { question, amount } => UserOutcomeRequest {
            action_type: "userOutcome",
            split_outcome: None,
            merge_outcome: None,
            merge_question: Some(QuestionAmountRequest { question, amount }),
            negate_outcome: None,
        },
        UserOutcomeAction::Negate {
            question,
            outcome,
            amount,
        } => UserOutcomeRequest {
            action_type: "userOutcome",
            split_outcome: None,
            merge_outcome: None,
            merge_question: None,
            negate_outcome: Some(NegateOutcomeRequest {
                question,
                outcome,
                amount,
            }),
        },
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserOutcomeAction {
    Split {
        outcome: u32,
        amount: String,
    },
    Merge {
        outcome: u32,
        amount: Option<String>,
    },
    MergeQuestion {
        question: u32,
        amount: Option<String>,
    },
    Negate {
        question: u32,
        outcome: u32,
        amount: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UserOutcomeRequest {
    #[serde(rename = "type")]
    action_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    split_outcome: Option<OutcomeAmountRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    merge_outcome: Option<OutcomeOptionalAmountRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    merge_question: Option<QuestionAmountRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    negate_outcome: Option<NegateOutcomeRequest>,
}

#[derive(Debug, Serialize)]
struct OutcomeAmountRequest {
    outcome: u32,
    amount: String,
}

#[derive(Debug, Serialize)]
struct OutcomeOptionalAmountRequest {
    outcome: u32,
    amount: Option<String>,
}

#[derive(Debug, Serialize)]
struct QuestionAmountRequest {
    question: u32,
    amount: Option<String>,
}

#[derive(Debug, Serialize)]
struct NegateOutcomeRequest {
    question: u32,
    outcome: u32,
    amount: String,
}

pub async fn approve_agent(
    master: &HyperliquidWallet,
    network: HyperliquidNetwork,
    agent_name: &str,
) -> Result<(HyperliquidWallet, ExchangeResponseStatus)> {
    let agent = HyperliquidWallet::random();
    let nonce = next_nonce()?;
    let action = Action::ApproveAgent {
        signature_chain_id: "0x66eee".to_string(),
        hyperliquid_chain: network.approval_chain().to_string(),
        agent_address: agent.address(),
        agent_name: Some(agent_name.to_string()),
        nonce,
    };
    let signature = master.sign_approve_agent(agent.address_bytes(), agent_name, nonce, network)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to construct Hyperliquid authorization client")?;
    let response = post_exchange_http(&client, network, action, signature, nonce).await?;
    Ok((agent, response))
}

pub async fn approve_hyperlink_agent(
    master: &HyperliquidWallet,
    agent: &HyperliquidWallet,
    agent_name: &str,
) -> Result<ExchangeResponseStatus> {
    let nonce = next_nonce()?;
    let action = Action::ApproveAgent {
        signature_chain_id: format!("0x{HYPERLINK_SIGNATURE_CHAIN_ID:x}"),
        hyperliquid_chain: HyperliquidNetwork::Mainnet.approval_chain().to_string(),
        agent_address: agent.address(),
        agent_name: Some(agent_name.to_string()),
        nonce,
    };
    let signature = master.sign_approve_agent_with_chain(
        agent.address_bytes(),
        agent_name,
        nonce,
        HyperliquidNetwork::Mainnet,
        HYPERLINK_SIGNATURE_CHAIN_ID,
    )?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to construct HyperLink authorization client")?;
    let response = post_exchange_http_to(
        &client,
        crate::providers::hyperlink::HTTP_URL,
        action,
        signature,
        nonce,
    )
    .await?;
    validate_hyperlink_agent_approval(&response, &agent.address())?;
    Ok(response)
}

fn validate_hyperlink_agent_approval(
    response: &ExchangeResponseStatus,
    expected_agent: &str,
) -> Result<()> {
    if let Some(error) = response_error(response) {
        bail!("HyperLink rejected API-wallet authorization: {error}");
    }
    let ExchangeResponseStatus::Ok(response) = response else {
        unreachable!("error responses are handled above");
    };
    let approval = response
        .data
        .as_ref()
        .and_then(|data| data.approve_agent.as_ref())
        .context("HyperLink authorization response omitted approveAgent confirmation")?;
    if !approval.success {
        bail!("HyperLink did not confirm API-wallet authorization");
    }
    if !approval.agent_address.eq_ignore_ascii_case(expected_agent) {
        bail!(
            "HyperLink confirmed API wallet {}, but Market Lab authorized {expected_agent}",
            approval.agent_address
        );
    }
    Ok(())
}

pub async fn create_subaccount(
    master: &HyperliquidWallet,
    network: HyperliquidNetwork,
    name: &str,
) -> Result<String> {
    let nonce = next_nonce()?;
    let action = Action::CreateSubAccount {
        name: name.to_string(),
    };
    let signature = master.sign_l1_action(&action, nonce, network)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("failed to construct Hyperliquid subaccount client")?;
    let response = post_exchange_http(&client, network, action, signature, nonce).await?;
    if let Some(error) = response_error(&response) {
        bail!("Hyperliquid rejected subaccount creation: {error}");
    }

    for attempt in 0..10 {
        let subaccounts: Vec<HyperliquidSubaccount> = HyperliquidClient::for_network(network)?
            .info(&serde_json::json!({
                "type": "subAccounts",
                "user": master.address(),
            }))
            .await
            .context("failed to query Hyperliquid subaccounts")?;
        if let Some(subaccount) = subaccounts.into_iter().find(|account| account.name == name) {
            return super::signing::canonical_address(&subaccount.sub_account_user)
                .context("Hyperliquid returned an invalid subaccount address");
        }
        if attempt < 9 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    bail!("Hyperliquid acknowledged subaccount creation but did not expose `{name}`")
}

pub async fn subaccounts(
    account: &str,
    network: HyperliquidNetwork,
) -> Result<Vec<(String, String)>> {
    let subaccounts: Vec<HyperliquidSubaccount> = HyperliquidClient::for_network(network)?
        .info(&serde_json::json!({ "type": "subAccounts", "user": account }))
        .await
        .context("failed to query Hyperliquid subaccounts")?;
    subaccounts
        .into_iter()
        .map(|subaccount| {
            Ok((
                subaccount.name,
                super::signing::canonical_address(&subaccount.sub_account_user)?,
            ))
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidSubaccount {
    name: String,
    sub_account_user: String,
}

async fn post_exchange_http(
    client: &reqwest::Client,
    network: HyperliquidNetwork,
    action: Action,
    signature: WireSignature,
    nonce: u64,
) -> Result<ExchangeResponseStatus> {
    post_exchange_http_to(client, network.http_url(), action, signature, nonce).await
}

async fn post_exchange_http_to(
    client: &reqwest::Client,
    base_url: &str,
    action: Action,
    signature: WireSignature,
    nonce: u64,
) -> Result<ExchangeResponseStatus> {
    let response = client
        .post(format!("{base_url}/exchange"))
        .json(&ExchangePayload {
            action,
            signature,
            nonce,
            vault_address: None,
        })
        .send()
        .await
        .with_context(|| format!("failed to call exchange API at {base_url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read Hyperliquid exchange response")?;
    if !status.is_success() {
        bail!("exchange API at {base_url} returned HTTP {status}: {body}");
    }
    serde_json::from_str(&body)
        .with_context(|| format!("invalid Hyperliquid exchange response: {body}"))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExchangePayload<T> {
    action: T,
    signature: WireSignature,
    nonce: u64,
    vault_address: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Action {
    CreateSubAccount {
        name: String,
    },
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
        #[serde(rename = "f", skip_serializing_if = "Option::is_none")]
        fast: Option<bool>,
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
    #[serde(rename = "c", skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
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
    #[serde(default)]
    pub statuses: Vec<ExchangeDataStatus>,
    #[serde(default, rename = "approveAgent")]
    pub approve_agent: Option<ApproveAgentResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApproveAgentResponse {
    pub success: bool,
    pub agent_address: String,
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

pub(crate) fn next_nonce() -> Result<u64> {
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
                client_order_id: None,
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
    fn hyperlink_cloid_uses_the_required_short_wire_field() {
        let action = Action::Order {
            orders: vec![OrderRequest {
                asset: 0,
                is_buy: true,
                limit_px: "65000".to_string(),
                size: "0.001".to_string(),
                reduce_only: false,
                client_order_id: Some("0x1234567890abcdef1234567890abcdef".to_string()),
                order_type: WireOrder::Limit {
                    tif: "Gtc".to_string(),
                },
            }],
            grouping: OrderGrouping::None,
        };
        let value = serde_json::to_value(action).expect("serializes");

        assert_eq!(
            value["orders"][0]["c"],
            "0x1234567890abcdef1234567890abcdef"
        );
    }

    #[test]
    fn hyperlink_cloid_signature_matches_official_sdk_vector() {
        let wallet = HyperliquidWallet::from_private_key(
            "0123456789012345678901234567890123456789012345678901234567890123",
        )
        .expect("wallet parses");
        let action = Action::Order {
            orders: vec![OrderRequest {
                asset: 1,
                is_buy: true,
                limit_px: "100".to_string(),
                size: "100".to_string(),
                reduce_only: false,
                order_type: WireOrder::Limit {
                    tif: "Gtc".to_string(),
                },
                client_order_id: Some("0x00000000000000000000000000000001".to_string()),
            }],
            grouping: OrderGrouping::None,
        };

        let signature = wallet
            .sign_l1_action(&action, 0, HyperliquidNetwork::Mainnet)
            .expect("action signs");

        assert_eq!(
            signature.r,
            // The Python SDK renders `r` as an integer and drops this leading
            // zero nibble. Market Lab keeps signature components at 32 bytes.
            "0x041ae18e8239a56cacbc5dad94d45d0b747e5da11ad564077fcac71277a946e3"
        );
        assert_eq!(
            signature.s,
            "0x3c61f667e747404fe7eea8f90ab0e76cc12ce60270438b2058324681a00116da"
        );
        assert_eq!(signature.v, 27);
    }

    #[test]
    fn hyperlink_approve_agent_confirmation_matches_the_live_wire_shape() {
        let expected = "0x0fcb98f0ea7b23f414be67c14bfab7fb2196d24b";
        let response: ExchangeResponseStatus = serde_json::from_value(serde_json::json!({
            "status": "ok",
            "response": {
                "type": "approveAgent",
                "data": {
                    "approveAgent": {
                        "success": true,
                        "agentAddress": expected,
                        "permission": "trade",
                        "ipWhitelist": [],
                        "validUntil": 1795391248185_u64
                    }
                }
            }
        }))
        .expect("live HyperLink approval response parses");

        validate_hyperlink_agent_approval(&response, expected)
            .expect("matching successful approval is accepted");
        assert!(
            validate_hyperlink_agent_approval(
                &response,
                "0x0000000000000000000000000000000000000001"
            )
            .expect_err("another agent must be rejected")
            .to_string()
            .contains("but Market Lab authorized")
        );
    }

    #[test]
    fn hyperlink_private_reads_accept_raw_objects_and_arrays() {
        let asset = serde_json::json!({
            "availableToTrade": ["0.0", "0.0"],
            "coin": "ETH",
            "leverage": { "type": "cross", "value": 15 },
            "markPx": "0.0",
            "maxTradeSzs": ["0.0", "0.0"],
            "user": "0xfdc6319fa33aa3b2178ca196963f7a5a06cd0852"
        });
        assert_eq!(
            hyperlink_read_data(asset.clone()).expect("raw asset data is accepted"),
            asset
        );

        let orders = serde_json::json!([]);
        assert_eq!(
            hyperlink_read_data(orders.clone()).expect("raw order arrays are accepted"),
            orders
        );
    }

    #[test]
    fn normal_tpsl_and_named_agent_match_official_wire_shape() {
        assert_eq!(
            serde_json::to_value(OrderGrouping::NormalTpSl).expect("grouping serializes"),
            "normalTpsl"
        );
        let action = Action::ApproveAgent {
            signature_chain_id: "0x66eee".to_string(),
            hyperliquid_chain: HyperliquidNetwork::Testnet.approval_chain().to_string(),
            agent_address: "0x0000000000000000000000000000000000000001".to_string(),
            agent_name: Some(TESTNET_API_WALLET_NAME.to_string()),
            nonce: 1,
        };
        let value = serde_json::to_value(action).expect("agent serializes");
        assert_eq!(value["agentName"], TESTNET_API_WALLET_NAME);
        assert_eq!(value["signatureChainId"], "0x66eee");
        assert_eq!(value["hyperliquidChain"], "Testnet");
    }

    #[test]
    fn fast_cancel_uses_the_hyperliquid_priority_flag() {
        let fast = serde_json::to_value(Action::Cancel {
            cancels: vec![CancelRequest { asset: 3, oid: 42 }],
            fast: Some(true),
        })
        .expect("fast cancel serializes");
        assert_eq!(
            fast,
            serde_json::json!({
                "type": "cancel",
                "cancels": [{ "a": 3, "o": 42 }],
                "f": true
            })
        );

        let normal = serde_json::to_value(Action::Cancel {
            cancels: vec![CancelRequest { asset: 3, oid: 42 }],
            fast: None,
        })
        .expect("normal cancel serializes");
        assert!(normal.get("f").is_none());
    }

    #[test]
    fn outcome_actions_match_the_hip4_wire_shapes() {
        let split = serde_json::to_value(user_outcome_request(UserOutcomeAction::Split {
            outcome: 1001,
            amount: "10".to_string(),
        }))
        .expect("split serializes");
        assert_eq!(
            split,
            serde_json::json!({
                "type": "userOutcome",
                "splitOutcome": { "outcome": 1001, "amount": "10" }
            })
        );

        let merge = serde_json::to_value(user_outcome_request(UserOutcomeAction::Merge {
            outcome: 1001,
            amount: None,
        }))
        .expect("merge serializes");
        assert_eq!(
            merge,
            serde_json::json!({
                "type": "userOutcome",
                "mergeOutcome": { "outcome": 1001, "amount": null }
            })
        );

        let merge_question =
            serde_json::to_value(user_outcome_request(UserOutcomeAction::MergeQuestion {
                question: 165,
                amount: Some("3".to_string()),
            }))
            .expect("question merge serializes");
        assert_eq!(
            merge_question,
            serde_json::json!({
                "type": "userOutcome",
                "mergeQuestion": { "question": 165, "amount": "3" }
            })
        );

        let negate = serde_json::to_value(user_outcome_request(UserOutcomeAction::Negate {
            question: 165,
            outcome: 1001,
            amount: "2.5".to_string(),
        }))
        .expect("negate serializes");
        assert_eq!(
            negate,
            serde_json::json!({
                "type": "userOutcome",
                "negateOutcome": { "question": 165, "outcome": 1001, "amount": "2.5" }
            })
        );
    }

    #[test]
    fn wire_numbers_are_canonical_and_bounded() {
        assert_eq!(wire_number(1.23000000), "1.23");
        assert_eq!(wire_number(-0.0), "0");
        assert_eq!(wire_number(0.00000001), "0.00000001");
    }
}
