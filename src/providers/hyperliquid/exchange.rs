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

#[derive(Clone)]
pub struct HyperliquidExchangeClient {
    trading: HyperliquidTradingClient,
    wallet: HyperliquidWallet,
    network: HyperliquidNetwork,
    vault_address: Option<String>,
}

impl HyperliquidExchangeClient {
    pub fn new(wallet: HyperliquidWallet, network: HyperliquidNetwork) -> Result<Self> {
        Ok(Self {
            trading: HyperliquidTradingClient::shared(network),
            wallet,
            network,
            vault_address: None,
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
        let nonce = next_nonce()?;
        let signature = self.wallet.sign_l1_action_for(
            &action,
            nonce,
            self.network,
            self.vault_address.as_deref(),
        )?;
        let response = self
            .trading
            .post_action(&ExchangePayload {
                action,
                signature,
                nonce,
                vault_address: self.vault_address.clone(),
            })
            .await?;
        serde_json::from_value(response).context("invalid Hyperliquid WebSocket exchange response")
    }
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
    let response = client
        .post(format!("{}/exchange", network.http_url()))
        .json(&ExchangePayload {
            action,
            signature,
            nonce,
            vault_address: None,
        })
        .send()
        .await
        .with_context(|| {
            format!(
                "failed to call Hyperliquid {} exchange API",
                network.label()
            )
        })?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read Hyperliquid exchange response")?;
    if !status.is_success() {
        bail!(
            "Hyperliquid {} exchange returned HTTP {status}: {body}",
            network.label()
        );
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
