use std::sync::OnceLock;

use anyhow::{Context, Result, bail};
use bulk_keychain::{Keypair, Pubkey};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use crate::cli::{AuthProvider, AuthProviderArgs, AuthSetArgs};
use crate::providers::bulk;

const MMT_API_KEY_ENV: &str = "MMT_API_KEY";
const KEYRING_SERVICE: &str = "market-lab";
const MMT_KEYRING_ACCOUNT: &str = "mmt-api-key";
const BULK_KEYRING_ACCOUNT: &str = "bulk-agent";
const BULK_CREDENTIAL_VERSION: u8 = 1;

static MMT_API_KEY: OnceLock<String> = OnceLock::new();

pub struct ActiveBulkCredential {
    pub account: Pubkey,
    pub agent: Keypair,
}

pub fn mmt_api_key() -> Result<String> {
    if let Some(key) = MMT_API_KEY.get() {
        return Ok(key.clone());
    }

    let key = if let Ok(key) = std::env::var(MMT_API_KEY_ENV) {
        validate_key(key, MMT_API_KEY_ENV)?
    } else {
        let key = mmt_entry()?
            .get_password()
            .context("MMT credentials are not configured; run `mlab auth set mmt`")?;
        validate_key(key, "stored MMT API key")?
    };

    let _ = MMT_API_KEY.set(key.clone());
    Ok(key)
}

pub fn mmt_is_configured() -> Result<bool> {
    if std::env::var(MMT_API_KEY_ENV).is_ok_and(|key| !key.trim().is_empty()) {
        return Ok(true);
    }
    match mmt_entry()?.get_password() {
        Ok(key) => Ok(!key.trim().is_empty()),
        Err(keyring::Error::NoEntry) => Ok(false),
        Err(error) => Err(error).context("failed to read MMT keychain status"),
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BulkCredentialStatus {
    Pending,
    Active,
}

#[derive(Debug, Deserialize, Serialize)]
struct BulkCredential {
    version: u8,
    status: BulkCredentialStatus,
    account: Option<String>,
    agent_public_key: String,
    agent_private_key: String,
}

impl BulkCredential {
    fn generate() -> Self {
        let agent = Keypair::generate();
        Self {
            version: BULK_CREDENTIAL_VERSION,
            status: BulkCredentialStatus::Pending,
            account: None,
            agent_public_key: agent.pubkey().to_base58(),
            agent_private_key: agent.to_base58(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.version != BULK_CREDENTIAL_VERSION {
            bail!(
                "unsupported stored BULK credential version {}",
                self.version
            );
        }

        let agent = self.agent_keypair()?;
        if agent.pubkey().to_base58() != self.agent_public_key {
            bail!("stored BULK agent public and private keys do not match");
        }

        if let Some(account) = &self.account {
            Pubkey::from_base58(account).context("stored BULK account public key is invalid")?;
        } else if self.status == BulkCredentialStatus::Active {
            bail!("stored active BULK credential is missing its account public key");
        }

        Ok(())
    }

    fn agent_keypair(&self) -> Result<Keypair> {
        Keypair::from_base58(&self.agent_private_key)
            .context("stored BULK agent private key is invalid")
    }
}

impl Drop for BulkCredential {
    fn drop(&mut self) {
        self.agent_private_key.zeroize();
    }
}

pub fn active_bulk_credential() -> Result<ActiveBulkCredential> {
    let credential = load_bulk_credential()?
        .context("BULK credentials are not configured; run `mlab auth set bulk`")?;
    if credential.status != BulkCredentialStatus::Active {
        bail!("BULK agent registration is pending; run `mlab auth set bulk` to finish it");
    }
    let account = credential
        .account
        .as_deref()
        .context("stored BULK credential is missing its account public key")?;
    Ok(ActiveBulkCredential {
        account: Pubkey::from_base58(account)
            .context("stored BULK account public key is invalid")?,
        agent: credential.agent_keypair()?,
    })
}

pub fn bulk_account() -> Result<String> {
    Ok(active_bulk_credential()?.account.to_base58())
}

pub async fn handle_set(args: AuthSetArgs) -> Result<()> {
    match args.provider {
        AuthProvider::Mmt => {
            if args.reauthorize {
                bail!("`--reauthorize` is only supported for BULK");
            }
            let key = rpassword::prompt_password("MMT API key: ")?;
            let key = validate_key(key, "MMT API key")?;
            mmt_entry()?
                .set_password(&key)
                .context("failed to store MMT API key in the OS keychain")?;
            crate::markets::refresh_mmt()
                .await
                .context("MMT was configured, but its market snapshot could not be initialized")?;
            crate::runtime::reload_markets_if_running().await?;
            println!("mmt: configured");
        }
        AuthProvider::Bulk => handle_set_bulk(args.reauthorize).await?,
    }
    Ok(())
}

pub fn handle_status() -> Result<()> {
    print_mmt_status()?;
    print_bulk_status()?;
    Ok(())
}

fn print_mmt_status() -> Result<()> {
    if std::env::var(MMT_API_KEY_ENV).is_ok_and(|key| !key.trim().is_empty()) {
        println!("mmt: configured via environment");
        return Ok(());
    }

    match mmt_entry()?.get_password() {
        Ok(key) if !key.trim().is_empty() => println!("mmt: configured in OS keychain"),
        Ok(_) | Err(keyring::Error::NoEntry) => println!("mmt: not configured"),
        Err(err) => return Err(err).context("failed to read MMT keychain status"),
    }
    Ok(())
}

fn print_bulk_status() -> Result<()> {
    match load_bulk_credential()? {
        Some(credential) => {
            let status = match credential.status {
                BulkCredentialStatus::Pending => "pending registration",
                BulkCredentialStatus::Active => "configured",
            };
            println!("bulk: {status} in OS keychain");
            if let Some(account) = &credential.account {
                println!("  account: {account}");
            }
            println!("  agent: {}", credential.agent_public_key);
        }
        None => println!("bulk: not configured"),
    }
    Ok(())
}

pub async fn handle_remove(args: AuthProviderArgs) -> Result<()> {
    match args.provider {
        AuthProvider::Mmt => match mmt_entry()?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => println!("mmt: removed"),
            Err(err) => return Err(err).context("failed to remove MMT API key from OS keychain"),
        },
        AuthProvider::Bulk => handle_remove_bulk().await?,
    }
    Ok(())
}

async fn handle_set_bulk(reauthorize: bool) -> Result<()> {
    let mut credential = match load_bulk_credential()? {
        Some(credential) if credential.status == BulkCredentialStatus::Active && !reauthorize => {
            println!("bulk: already configured");
            println!(
                "  account: {}",
                credential.account.as_deref().unwrap_or("unknown")
            );
            println!("  agent: {}", credential.agent_public_key);
            println!(
                "  use `mlab auth set bulk --reauthorize` if BULK rejects this agent as unauthorized"
            );
            return Ok(());
        }
        Some(credential) if credential.status == BulkCredentialStatus::Active => {
            println!("bulk: reauthorizing the existing agent");
            credential
        }
        Some(credential) => {
            println!("bulk: retrying registration for the pending agent");
            credential
        }
        None => {
            let credential = BulkCredential::generate();
            save_bulk_credential(&credential)?;
            println!("bulk: generated a new agent wallet and stored it as pending");
            credential
        }
    };

    let agent = credential.agent_keypair()?.pubkey();
    println!("  agent: {}", credential.agent_public_key);
    println!("The main wallet private key is used once for signing and is never stored.");

    let (master, account) = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "BULK main wallet private key (hidden): ",
        )?);
        let master = Keypair::from_base58(private_key.trim())
            .context("invalid BULK main wallet private key")?;
        let account = master.pubkey().to_base58();
        (master, account)
    };

    if let Some(expected_account) = &credential.account
        && expected_account != &account
    {
        bail!(
            "this BULK agent belongs to account {expected_account}, but the supplied key belongs to {account}"
        );
    }

    if credential.account.is_none() {
        credential.account = Some(account.clone());
        save_bulk_credential(&credential)?;
    }
    println!("bulk: authorizing the agent for account {account}");

    let registration = bulk::register_agent(master, agent).await.map_err(|error| {
        let recovery = if reauthorize {
            "BULK agent reauthorization was not confirmed; the existing local agent was preserved and `mlab auth set bulk --reauthorize` can safely retry it"
        } else {
            "BULK agent registration was not confirmed; the pending agent remains in the OS keychain and `mlab auth set bulk` can safely retry it"
        };
        error.context(recovery)
    })?;

    if registration.account != account
        || registration.agent_public_key != credential.agent_public_key
    {
        bail!("BULK registration confirmation did not match the requested account and agent");
    }

    credential.status = BulkCredentialStatus::Active;
    save_bulk_credential(&credential).with_context(|| {
        if reauthorize {
            "BULK reauthorized the agent, but Market Lab could not refresh it in the OS keychain; the existing credential was preserved"
        } else {
            "BULK registered the agent, but Market Lab could not mark it active in the OS keychain; the pending credential was preserved"
        }
    })?;

    println!(
        "bulk: {}",
        if reauthorize {
            "reauthorized"
        } else {
            "configured"
        }
    );
    println!("  account: {account}");
    println!("  agent: {}", credential.agent_public_key);
    Ok(())
}

async fn handle_remove_bulk() -> Result<()> {
    let Some(credential) = load_bulk_credential()? else {
        println!("bulk: not configured");
        return Ok(());
    };

    if credential.status == BulkCredentialStatus::Pending && credential.account.is_none() {
        delete_bulk_credential()?;
        println!("bulk: pending agent removed");
        return Ok(());
    }

    if credential.status == BulkCredentialStatus::Pending {
        bail!(
            "this BULK agent has an unconfirmed registration; retry `mlab auth set bulk` before removing it so Market Lab does not discard a potentially authorized key"
        );
    }

    let account = credential
        .account
        .as_deref()
        .context("stored BULK credential is missing its account public key")?;
    let agent = credential.agent_keypair()?.pubkey();

    println!("The main wallet private key is used once for revocation and is never stored.");
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "BULK main wallet private key (hidden): ",
        )?);
        let master = Keypair::from_base58(private_key.trim())
            .context("invalid BULK main wallet private key")?;
        let supplied_account = master.pubkey().to_base58();
        if supplied_account != account {
            bail!(
                "the supplied key belongs to BULK account {supplied_account}, but this agent belongs to {account}"
            );
        }
        master
    };

    println!("bulk: revoking agent {}", credential.agent_public_key);
    bulk::revoke_agent(master, agent)
        .await
        .context("BULK agent revocation was not confirmed; the agent remains in the OS keychain")?;

    delete_bulk_credential()?;
    println!("bulk: revoked and removed");
    Ok(())
}

fn delete_bulk_credential() -> Result<()> {
    match bulk_entry()?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(err) => Err(err).context("failed to remove BULK agent from OS keychain"),
    }
}

fn mmt_entry() -> Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, MMT_KEYRING_ACCOUNT)
        .context("failed to access the OS keychain")
}

fn bulk_entry() -> Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, BULK_KEYRING_ACCOUNT)
        .context("failed to access the OS keychain")
}

fn load_bulk_credential() -> Result<Option<BulkCredential>> {
    let encoded = match bulk_entry()?.get_password() {
        Ok(encoded) => Zeroizing::new(encoded),
        Err(keyring::Error::NoEntry) => return Ok(None),
        Err(err) => return Err(err).context("failed to read BULK agent from OS keychain"),
    };

    let credential: BulkCredential = serde_json::from_str(encoded.as_str())
        .context("stored BULK agent credential is malformed")?;
    credential.validate()?;
    Ok(Some(credential))
}

fn save_bulk_credential(credential: &BulkCredential) -> Result<()> {
    credential.validate()?;
    let encoded = Zeroizing::new(
        serde_json::to_string(credential).context("failed to encode BULK agent credential")?,
    );
    bulk_entry()?
        .set_password(encoded.as_str())
        .context("failed to store BULK agent in the OS keychain")
}

fn validate_key(key: String, name: &str) -> Result<String> {
    let key = key.trim().to_string();
    if key.is_empty() {
        bail!("{name} cannot be empty");
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_bulk_credential_contains_matching_agent_keys() {
        let credential = BulkCredential::generate();
        credential
            .validate()
            .expect("generated credential is valid");
        assert_eq!(credential.status, BulkCredentialStatus::Pending);
        assert!(credential.account.is_none());
        assert_eq!(
            credential
                .agent_keypair()
                .expect("agent key parses")
                .pubkey()
                .to_base58(),
            credential.agent_public_key
        );
    }

    #[test]
    fn active_bulk_credential_requires_an_account() {
        let mut credential = BulkCredential::generate();
        credential.status = BulkCredentialStatus::Active;
        let error = credential
            .validate()
            .expect_err("active credential without account must fail");
        assert!(error.to_string().contains("missing its account"));
    }
}
