use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bulk_keychain::{Keypair, Pubkey, SignedTransaction, Signer};
use reqwest::Client;
use serde_json::Value;

const DEFAULT_BULK_API_URL: &str = "https://exchange-api.bulk.trade/api/v1";
const BULK_HTTP_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRegistration {
    pub account: String,
    pub agent_public_key: String,
}

pub async fn register_agent(master: Keypair, agent: Pubkey) -> Result<AgentRegistration> {
    set_agent_authorization(master, agent, false).await
}

pub async fn revoke_agent(master: Keypair, agent: Pubkey) -> Result<AgentRegistration> {
    set_agent_authorization(master, agent, true).await
}

async fn set_agent_authorization(
    master: Keypair,
    agent: Pubkey,
    delete: bool,
) -> Result<AgentRegistration> {
    let expected_agent = agent.to_base58();
    let signed = sign_agent_authorization(master, agent, delete)?;
    let account = signed.account.clone();
    let body = submit_transaction(&signed).await?;
    validate_agent_response(&body, &expected_agent, delete)?;

    Ok(AgentRegistration {
        account,
        agent_public_key: expected_agent,
    })
}

fn sign_agent_authorization(
    master: Keypair,
    agent: Pubkey,
    delete: bool,
) -> Result<SignedTransaction> {
    let mut signer = Signer::new(master).without_order_id();
    signer
        .sign_agent_wallet(agent, delete, Some(unique_nonce()?))
        .context("failed to sign BULK agent-wallet authorization")
}

async fn submit_transaction(transaction: &SignedTransaction) -> Result<Value> {
    let base_url = DEFAULT_BULK_API_URL;
    let url = format!("{}/order", base_url.trim_end_matches('/'));

    let response = Client::new()
        .post(url)
        .timeout(Duration::from_secs(BULK_HTTP_TIMEOUT_SECS))
        .json(transaction)
        .send()
        .await
        .context("failed to submit BULK agent-wallet authorization")?;

    let status = response.status();
    let body: Value = response
        .json()
        .await
        .context("failed to decode BULK agent-wallet response")?;

    if !status.is_success() {
        bail!(
            "BULK agent-wallet authorization returned HTTP {status}: {}",
            response_message(&body)
        );
    }

    Ok(body)
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
        .unwrap_or("unknown error")
        .to_string()
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
    fn signs_agent_authorization_with_master_as_account() {
        let master = Keypair::generate();
        let account = master.pubkey().to_base58();
        let agent = Keypair::generate().pubkey();
        let agent_public_key = agent.to_base58();

        let signed = sign_agent_authorization(master, agent, false).expect("authorization signs");

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
    fn rejects_failed_agent_response_even_when_http_body_status_is_ok() {
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
}
