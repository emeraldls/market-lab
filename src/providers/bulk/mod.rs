use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bulk_keychain::{
    CreateSubAccount, Keypair, Pubkey, SignatureDomain, SignedTransaction, Signer,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use self::client::BulkClient;
use self::ws::{BulkTradingClient, is_trading_acknowledgement};

pub mod client;
pub mod execution;
pub mod market_data;
pub mod markets;
pub mod ws;

const AGENT_CONFIRMATION_ATTEMPTS: usize = 10;
const AGENT_CONFIRMATION_DELAY: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkNetwork {
    Mainnet,
    Testnet,
}

impl BulkNetwork {
    pub const fn from_testnet(testnet: bool) -> Self {
        if testnet {
            Self::Testnet
        } else {
            Self::Mainnet
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }

    pub const fn api_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://mainnet-api1.bulk.trade/api/v1",
            Self::Testnet => "https://exchange-api.bulk.trade/api/v1",
        }
    }

    pub const fn websocket_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://mainnet-ws1.bulk.trade",
            Self::Testnet => "wss://exchange-ws1.bulk.trade",
        }
    }

    const fn signature_domain(self) -> SignatureDomain {
        match self {
            Self::Mainnet => SignatureDomain::Mainnet,
            Self::Testnet => SignatureDomain::Testnet,
        }
    }
}

pub(crate) fn signer(network: BulkNetwork, keypair: Keypair) -> Signer {
    Signer::new(keypair, network.signature_domain())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRegistration {
    pub account: String,
    pub agent_public_key: String,
}

pub async fn register_agent(
    network: BulkNetwork,
    master: Keypair,
    agent: Pubkey,
) -> Result<AgentRegistration> {
    set_agent_authorization(network, master, agent, false).await
}

pub async fn revoke_agent(
    network: BulkNetwork,
    master: Keypair,
    agent: Pubkey,
) -> Result<AgentRegistration> {
    set_agent_authorization(network, master, agent, true).await
}

pub async fn create_subaccount(
    network: BulkNetwork,
    master: Keypair,
    name: &str,
) -> Result<String> {
    let main_account = master.pubkey().to_base58();
    let mut signer = signer(network, master).without_order_id();
    let signed = signer
        .sign_create_sub_account(CreateSubAccount::new(name), Some(unique_nonce()?))
        .context("failed to sign BULK subaccount creation")?;
    let body = submit_transaction(network, &signed)
        .await
        .context("failed to submit BULK subaccount creation")?;
    if body.get("status").and_then(Value::as_str) != Some("ok") {
        bail!(
            "BULK rejected subaccount creation: {}",
            response_message(&body)
        );
    }
    extract_created_subaccount(&body, &main_account)
        .context("BULK confirmed subaccount creation without returning its public key")
}

fn extract_created_subaccount(body: &Value, main_account: &str) -> Result<String> {
    const ADDRESS_KEYS: &[&str] = &[
        "sub",
        "subAccount",
        "sub_account",
        "subAccountUser",
        "account",
        "pubkey",
        "address",
    ];
    if let Some(statuses) = body
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
    {
        for status in statuses {
            if let Some(failure) = status
                .get("createSubAccountFailed")
                .or_else(|| status.get("create_sub_account_failed"))
            {
                bail!(
                    "BULK rejected subaccount creation: {}",
                    response_message(failure)
                );
            }
        }
    }
    fn visit(value: &Value, main_account: &str) -> Option<String> {
        match value {
            Value::Object(object) => {
                for key in ADDRESS_KEYS {
                    if let Some(candidate) = object.get(*key).and_then(Value::as_str)
                        && candidate != main_account
                        && Pubkey::from_base58(candidate).is_ok()
                    {
                        return Some(candidate.to_string());
                    }
                }
                object.values().find_map(|value| visit(value, main_account))
            }
            Value::Array(values) => values.iter().find_map(|value| visit(value, main_account)),
            _ => None,
        }
    }
    visit(body, main_account).context("missing created subaccount public key")
}

async fn set_agent_authorization(
    network: BulkNetwork,
    master: Keypair,
    agent: Pubkey,
    delete: bool,
) -> Result<AgentRegistration> {
    let expected_agent = agent.to_base58();
    let signed = sign_agent_authorization(network, master, agent, delete)?;
    let account = signed.account.clone();
    let body = submit_transaction(network, &signed).await?;
    if is_trading_acknowledgement(&body) {
        confirm_agent_authorization(network, &account, &expected_agent, delete).await?;
    } else {
        validate_agent_response(&body, &expected_agent, delete)?;
    }

    Ok(AgentRegistration {
        account,
        agent_public_key: expected_agent,
    })
}

async fn confirm_agent_authorization(
    network: BulkNetwork,
    account: &str,
    agent: &str,
    delete: bool,
) -> Result<()> {
    let client = BulkClient::new(network)?;
    for attempt in 0..AGENT_CONFIRMATION_ATTEMPTS {
        let body: Value = client
            .post(
                "account",
                &serde_json::json!({ "type": "fullAccount", "user": account }),
            )
            .await
            .context("failed to verify BULK agent-wallet authorization")?;
        if agent_authorization_matches(&body, agent, delete)? {
            return Ok(());
        }
        if attempt + 1 < AGENT_CONFIRMATION_ATTEMPTS {
            tokio::time::sleep(AGENT_CONFIRMATION_DELAY).await;
        }
    }
    bail!(
        "BULK acknowledged the agent-wallet transaction but the account snapshot did not confirm that agent {agent} was {}",
        if delete { "removed" } else { "authorized" }
    )
}

fn agent_authorization_matches(body: &Value, agent: &str, delete: bool) -> Result<bool> {
    let wallets = body
        .as_array()
        .and_then(|entries| entries.iter().find_map(|entry| entry.get("fullAccount")))
        .and_then(|account| account.get("authorizedAgentWallets"))
        .and_then(Value::as_array)
        .context("BULK full-account response omitted authorizedAgentWallets")?;
    let authorized = wallets.iter().any(|wallet| {
        wallet.as_str() == Some(agent)
            || wallet.get("pubkey").and_then(Value::as_str) == Some(agent)
    });
    Ok(authorized != delete)
}

fn sign_agent_authorization(
    network: BulkNetwork,
    master: Keypair,
    agent: Pubkey,
    delete: bool,
) -> Result<SignedTransaction> {
    let mut signer = signer(network, master).without_order_id();
    signer
        .sign_agent_wallet(agent, delete, Some(unique_nonce()?))
        .context("failed to sign BULK agent-wallet authorization")
}

async fn submit_transaction(
    network: BulkNetwork,
    transaction: &SignedTransaction,
) -> Result<Value> {
    BulkTradingClient::new(network)
        .post(transaction)
        .await
        .context("failed to submit BULK agent-wallet authorization")
}

fn validate_agent_response(body: &Value, expected_agent: &str, delete: bool) -> Result<()> {
    if body.get("status").and_then(Value::as_str) != Some("ok") {
        bail!(
            "BULK rejected the agent-wallet authorization: {}",
            response_message(body)
        );
    }

    let statuses = body
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .context("BULK returned an agent-wallet response without statuses")?;

    for status in statuses {
        if let Some(success) = status.get("agentWallet") {
            let returned_agent = success
                .get("agent_wallet")
                .or_else(|| success.get("agentWallet"))
                .and_then(Value::as_str)
                .context("BULK agent-wallet success response omitted the agent public key")?;
            if returned_agent != expected_agent {
                bail!(
                    "BULK authorized unexpected agent {returned_agent}; expected {expected_agent}"
                );
            }
            return Ok(());
        }

        if let Some(failure) = status.get("agentWalletFailed") {
            bail!(
                "BULK failed to {} the agent wallet: {}",
                if delete { "remove" } else { "register" },
                response_message(failure)
            );
        }

        if let Some(error) = status.get("error") {
            bail!(
                "BULK rejected the agent-wallet authorization: {}",
                response_message(error)
            );
        }
    }

    bail!("BULK did not confirm the agent-wallet authorization")
}

fn response_message(body: &Value) -> String {
    body.get("message")
        .and_then(Value::as_str)
        .or_else(|| body.pointer("/error/message").and_then(Value::as_str))
        .or_else(|| body.pointer("/response/message").and_then(Value::as_str))
        .or_else(|| {
            body.pointer("/response/error/message")
                .and_then(Value::as_str)
        })
        .or_else(|| body.get("response").and_then(Value::as_str))
        .or_else(|| body.get("error").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| body.to_string())
}

fn unique_nonce() -> Result<u64> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos();
    u64::try_from(nanos).context("current timestamp does not fit in a BULK nonce")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bulk_network_selects_matching_endpoints_and_signature_domains() {
        let mainnet = signer(BulkNetwork::Mainnet, Keypair::generate());
        let testnet = signer(BulkNetwork::Testnet, Keypair::generate());

        assert_eq!(mainnet.signature_domain(), SignatureDomain::Mainnet);
        assert_eq!(testnet.signature_domain(), SignatureDomain::Testnet);
        assert!(BulkNetwork::Mainnet.api_url().contains("mainnet-api1"));
        assert!(BulkNetwork::Testnet.api_url().contains("exchange-api"));
    }

    #[test]
    fn signs_agent_authorization_with_master_as_account() {
        let master = Keypair::generate();
        let account = master.pubkey().to_base58();
        let agent = Keypair::generate().pubkey();
        let agent_public_key = agent.to_base58();

        let signed = sign_agent_authorization(BulkNetwork::Mainnet, master, agent, false)
            .expect("authorization signs");

        assert_eq!(signed.account, account);
        assert_eq!(signed.signer, account);
        assert_eq!(
            signed.actions,
            vec![json!({
                "agentWalletCreation": {
                    "a": agent_public_key,
                    "d": false
                }
            })]
        );
        assert!(!signed.signature.is_empty());
    }

    #[test]
    fn accepts_confirmed_agent_response() {
        let body = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{
                        "agentWallet": {"agent_wallet": "agent-public-key"}
                    }]
                }
            }
        });

        validate_agent_response(&body, "agent-public-key", false).expect("response is accepted");
    }

    #[test]
    fn rejects_failed_agent_response_even_when_envelope_status_is_ok() {
        let body = json!({
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [{
                        "agentWalletFailed": {"message": "Unauthorized"}
                    }]
                }
            }
        });

        let error = validate_agent_response(&body, "agent-public-key", false)
            .expect_err("failure status must be rejected");
        assert!(error.to_string().contains("Unauthorized"));
    }

    #[test]
    fn preserves_top_level_string_rejection_details() {
        let body = json!({
            "status": "error",
            "response": "bad signature"
        });

        let error = validate_agent_response(&body, "agent-public-key", false)
            .expect_err("rejection must fail");
        assert!(error.to_string().contains("bad signature"));
    }

    #[test]
    fn confirms_acknowledged_agent_state_from_account_snapshot() {
        let authorized = json!([{
            "fullAccount": {
                "authorizedAgentWallets": ["agent-public-key"]
            }
        }]);
        assert!(
            agent_authorization_matches(&authorized, "agent-public-key", false)
                .expect("authorization state parses")
        );
        assert!(
            !agent_authorization_matches(&authorized, "agent-public-key", true)
                .expect("revocation state parses")
        );

        let revoked = json!([{
            "fullAccount": {
                "authorizedAgentWallets": []
            }
        }]);
        assert!(
            agent_authorization_matches(&revoked, "agent-public-key", true)
                .expect("revocation state parses")
        );
    }

    #[test]
    fn extracts_created_subaccount_from_the_documented_status() {
        let master = Keypair::generate().pubkey().to_base58();
        let subaccount = Keypair::generate().pubkey().to_base58();
        let body = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{
                        "createSubAccount": {
                            "master": master,
                            "sub": subaccount,
                            "name": "trading-2",
                            "margin": 0.0
                        }
                    }]
                }
            }
        });

        assert_eq!(
            extract_created_subaccount(&body, &master).expect("subaccount parses"),
            subaccount
        );
    }

    #[test]
    fn surfaces_subaccount_creation_failure_status() {
        let master = Keypair::generate().pubkey().to_base58();
        let body = json!({
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [{
                        "createSubAccountFailed": {"message": "duplicate name"}
                    }]
                }
            }
        });

        let error = extract_created_subaccount(&body, &master)
            .expect_err("failure status must be rejected");
        assert!(error.to_string().contains("duplicate name"));
    }
}
