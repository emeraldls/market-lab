use std::sync::OnceLock;

use anyhow::{Context, Result, bail};
use bulk_keychain::{Keypair, Pubkey};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use crate::cli::{AuthProvider, AuthProviderArgs, AuthSetArgs};
use crate::providers::bulk;
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::exchange::{
    LEGACY_TESTNET_API_WALLET_NAME, MAINNET_API_WALLET_NAME, TESTNET_API_WALLET_NAME,
    approve_agent, response_error,
};
use crate::providers::hyperliquid::signing::{HyperliquidWallet, canonical_address};

const MMT_API_KEY_ENV: &str = "MMT_API_KEY";
const KEYRING_SERVICE: &str = "market-lab";
const MMT_KEYRING_ACCOUNT: &str = "mmt-api-key";
const BULK_KEYRING_ACCOUNT: &str = "bulk-agent";
const HYPERLIQUID_KEYRING_ACCOUNT: &str = "hyperliquid-agents";
const LEGACY_HYPERLIQUID_KEYRING_ACCOUNT: &str = "hyperliquid-testnet-agent";
const BULK_CREDENTIAL_VERSION: u8 = 1;
const LEGACY_HYPERLIQUID_CREDENTIAL_VERSION: u8 = 1;
const HYPERLIQUID_CREDENTIAL_VERSION: u8 = 2;

static MMT_API_KEY: OnceLock<String> = OnceLock::new();

pub struct ActiveBulkCredential {
    pub account: Pubkey,
    pub agent: Keypair,
}

pub struct ActiveHyperliquidCredential {
    pub account: String,
    pub agent: HyperliquidWallet,
}

#[derive(Debug, Deserialize, Serialize)]
struct HyperliquidAgentCredential {
    name: String,
    address: String,
    private_key: String,
}

impl HyperliquidAgentCredential {
    fn from_wallet(name: &str, wallet: &HyperliquidWallet) -> Self {
        Self {
            name: name.to_string(),
            address: wallet.address(),
            private_key: wallet.private_key_hex(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.name.is_empty() || self.name.len() > 16 {
            bail!("stored Hyperliquid agent name must contain 1 to 16 characters");
        }
        let address = parse_hyperliquid_address(&self.address, "agent")?;
        let agent = self.wallet()?;
        let derived = agent.address();
        if address != self.address.to_ascii_lowercase() {
            bail!("stored Hyperliquid agent address is not canonical");
        }
        if derived != address {
            bail!("stored Hyperliquid agent public and private keys do not match");
        }
        Ok(())
    }

    fn wallet(&self) -> Result<HyperliquidWallet> {
        HyperliquidWallet::from_private_key(&self.private_key)
            .context("stored Hyperliquid agent private key is invalid")
    }
}

impl Drop for HyperliquidAgentCredential {
    fn drop(&mut self) {
        self.private_key.zeroize();
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct HyperliquidCredential {
    version: u8,
    account: String,
    mainnet_agent: Option<HyperliquidAgentCredential>,
    testnet_agent: Option<HyperliquidAgentCredential>,
}

impl HyperliquidCredential {
    fn validate(&self) -> Result<()> {
        if self.version != HYPERLIQUID_CREDENTIAL_VERSION {
            bail!(
                "unsupported stored Hyperliquid credential version {}",
                self.version
            );
        }
        let account = parse_hyperliquid_address(&self.account, "account")?;
        if account != self.account.to_ascii_lowercase() {
            bail!("stored Hyperliquid account is not canonical");
        }
        if self.mainnet_agent.is_none() && self.testnet_agent.is_none() {
            bail!("stored Hyperliquid credential contains no API wallets");
        }
        if let Some(agent) = &self.mainnet_agent {
            agent.validate()?;
        }
        if let Some(agent) = &self.testnet_agent {
            agent.validate()?;
        }
        if let (Some(mainnet), Some(testnet)) = (&self.mainnet_agent, &self.testnet_agent)
            && mainnet.name == testnet.name
        {
            bail!("stored Hyperliquid mainnet and testnet agents must have distinct names");
        }
        Ok(())
    }

    fn validate_complete(&self) -> Result<()> {
        self.validate()?;
        if self.mainnet_agent.is_none() || self.testnet_agent.is_none() {
            bail!("stored Hyperliquid credential is missing a network API wallet");
        }
        Ok(())
    }

    fn agent(&self, network: HyperliquidNetwork) -> Option<&HyperliquidAgentCredential> {
        match network {
            HyperliquidNetwork::Mainnet => self.mainnet_agent.as_ref(),
            HyperliquidNetwork::Testnet => self.testnet_agent.as_ref(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct LegacyHyperliquidCredential {
    version: u8,
    account: String,
    agent_address: String,
    agent_private_key: String,
}

#[derive(Deserialize)]
struct CredentialVersion {
    version: u8,
}

impl LegacyHyperliquidCredential {
    fn upgrade(mut self) -> Result<HyperliquidCredential> {
        if self.version != LEGACY_HYPERLIQUID_CREDENTIAL_VERSION {
            bail!(
                "unsupported stored Hyperliquid credential version {}",
                self.version
            );
        }
        let credential = HyperliquidCredential {
            version: HYPERLIQUID_CREDENTIAL_VERSION,
            account: std::mem::take(&mut self.account),
            mainnet_agent: None,
            testnet_agent: Some(HyperliquidAgentCredential {
                name: LEGACY_TESTNET_API_WALLET_NAME.to_string(),
                address: std::mem::take(&mut self.agent_address),
                private_key: std::mem::take(&mut self.agent_private_key),
            }),
        };
        credential.validate()?;
        Ok(credential)
    }
}

impl Drop for LegacyHyperliquidCredential {
    fn drop(&mut self) {
        self.agent_private_key.zeroize();
    }
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

pub fn active_hyperliquid_credential(
    network: HyperliquidNetwork,
) -> Result<ActiveHyperliquidCredential> {
    let credential = load_hyperliquid_credential()?
        .context("Hyperliquid credentials are not configured; run `mlab auth set hyperliquid`")?;
    let agent = credential.agent(network).with_context(|| {
        format!(
            "Hyperliquid {} API wallet is not configured; run `mlab auth set hyperliquid` to complete setup or add `--reauthorize` to replace all agents",
            network.label()
        )
    })?;
    Ok(ActiveHyperliquidCredential {
        account: credential.account.clone(),
        agent: agent.wallet()?,
    })
}

pub fn hyperliquid_account() -> Result<String> {
    let credential = load_hyperliquid_credential()?
        .context("Hyperliquid credentials are not configured; run `mlab auth set hyperliquid`")?;
    Ok(credential.account)
}

pub async fn handle_set(args: AuthSetArgs) -> Result<()> {
    match args.provider {
        AuthProvider::Mmt => {
            if args.reauthorize {
                bail!("`--reauthorize` is only supported for execution venues");
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
        AuthProvider::Hyperliquid => handle_set_hyperliquid(args.reauthorize).await?,
    }
    Ok(())
}

pub fn handle_status() -> Result<()> {
    print_mmt_status()?;
    print_bulk_status()?;
    print_hyperliquid_status()?;
    Ok(())
}

fn print_hyperliquid_status() -> Result<()> {
    match load_hyperliquid_credential()? {
        Some(credential) => {
            let status = if credential.mainnet_agent.is_some() && credential.testnet_agent.is_some()
            {
                "configured for mainnet and testnet"
            } else {
                "partially configured"
            };
            println!("hyperliquid: {status} in OS keychain");
            println!("  account: {}", credential.account);
            if let Some(agent) = &credential.mainnet_agent {
                println!("  mainnet agent: {} ({})", agent.address, agent.name);
            } else {
                println!("  mainnet agent: not configured");
            }
            if let Some(agent) = &credential.testnet_agent {
                println!("  testnet agent: {} ({})", agent.address, agent.name);
            } else {
                println!("  testnet agent: not configured");
            }
        }
        None => println!("hyperliquid: not configured"),
    }
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
        AuthProvider::Hyperliquid => handle_remove_hyperliquid().await?,
    }
    Ok(())
}

async fn handle_set_hyperliquid(reauthorize: bool) -> Result<()> {
    let mut existing = load_hyperliquid_credential()?;
    let replacing_existing = reauthorize && existing.is_some();
    if existing
        .as_ref()
        .is_some_and(|credential| credential.validate_complete().is_ok())
        && !reauthorize
    {
        let credential = existing.as_ref().expect("checked above");
        println!("hyperliquid: already configured for mainnet and testnet");
        println!("  account: {}", credential.account);
        print_hyperliquid_agents(credential);
        println!("  use `mlab auth set hyperliquid --reauthorize` to replace the API wallet");
        return Ok(());
    }

    println!("Hyperliquid mainnet and testnet API-wallet setup.");
    println!("The main wallet private key is used only for approval and is never stored.");
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "Hyperliquid main wallet private key (hidden): ",
        )?);
        HyperliquidWallet::from_private_key(private_key.trim())
            .context("invalid Hyperliquid main wallet private key")?
    };
    let account = master.address();
    if let Some(existing) = &existing
        && existing.account != account
    {
        bail!(
            "this Hyperliquid credential belongs to {}, but the supplied key belongs to {account}",
            existing.account
        );
    }

    let mainnet_name = existing
        .as_ref()
        .and_then(|credential| credential.mainnet_agent.as_ref())
        .map_or_else(
            || MAINNET_API_WALLET_NAME.to_string(),
            |agent| agent.name.clone(),
        );
    let testnet_name = existing
        .as_ref()
        .and_then(|credential| credential.testnet_agent.as_ref())
        .map_or_else(
            || TESTNET_API_WALLET_NAME.to_string(),
            |agent| agent.name.clone(),
        );

    let preserved_mainnet = if reauthorize {
        None
    } else {
        existing
            .as_mut()
            .and_then(|credential| credential.mainnet_agent.take())
    };
    let preserved_testnet = if reauthorize {
        None
    } else {
        existing
            .as_mut()
            .and_then(|credential| credential.testnet_agent.take())
    };

    let mainnet_agent = match preserved_mainnet {
        Some(agent) => agent,
        None => {
            authorize_hyperliquid_agent(
                &master,
                HyperliquidNetwork::Mainnet,
                &mainnet_name,
                replacing_existing,
            )
            .await?
        }
    };
    let testnet_agent = match preserved_testnet {
        Some(agent) => agent,
        None => {
            authorize_hyperliquid_agent(
                &master,
                HyperliquidNetwork::Testnet,
                &testnet_name,
                replacing_existing,
            )
            .await?
        }
    };

    let credential = HyperliquidCredential {
        version: HYPERLIQUID_CREDENTIAL_VERSION,
        account: account.clone(),
        mainnet_agent: Some(mainnet_agent),
        testnet_agent: Some(testnet_agent),
    };
    credential.validate_complete()?;
    save_hyperliquid_credential(&credential)?;
    crate::markets::refresh_hyperliquid()
        .await
        .context("Hyperliquid was configured, but its market snapshot could not be initialized")?;
    crate::runtime::reload_markets_if_running().await?;

    println!("hyperliquid: configured for mainnet and testnet");
    println!("  account: {account}");
    print_hyperliquid_agents(&credential);
    Ok(())
}

async fn handle_remove_hyperliquid() -> Result<()> {
    let Some(credential) = load_hyperliquid_credential()? else {
        println!("hyperliquid: not configured");
        return Ok(());
    };
    println!(
        "The main wallet private key is used once to replace the stored mainnet and testnet API wallets."
    );
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "Hyperliquid main wallet private key (hidden): ",
        )?);
        HyperliquidWallet::from_private_key(private_key.trim())
            .context("invalid Hyperliquid main wallet private key")?
    };
    let account = master.address();
    if account != credential.account {
        bail!(
            "the supplied key belongs to {account}, but the stored Hyperliquid agent belongs to {}",
            credential.account
        );
    }
    for network in [HyperliquidNetwork::Mainnet, HyperliquidNetwork::Testnet] {
        let Some(agent) = credential.agent(network) else {
            continue;
        };
        let (_replacement, response) = approve_agent(&master, network, &agent.name)
            .await
            .with_context(|| {
                format!(
                    "failed to replace the stored Hyperliquid {} API wallet",
                    network.label()
                )
            })?;
        ensure_hyperliquid_exchange_ok(
            &response,
            &format!("{} API-wallet replacement", network.label()),
        )?;
    }
    delete_hyperliquid_credential()?;
    println!("hyperliquid: revoked and removed");
    Ok(())
}

async fn authorize_hyperliquid_agent(
    master: &HyperliquidWallet,
    network: HyperliquidNetwork,
    name: &str,
    replacing: bool,
) -> Result<HyperliquidAgentCredential> {
    let action = if replacing {
        "replacing"
    } else {
        "authorizing"
    };
    println!(
        "hyperliquid: {action} {network} API wallet `{name}`",
        network = network.label()
    );
    let (wallet, response) = approve_agent(master, network, name)
        .await
        .with_context(|| {
            format!(
                "Hyperliquid {} API-wallet authorization failed",
                network.label()
            )
        })?;
    ensure_hyperliquid_exchange_ok(
        &response,
        &format!("{} API-wallet authorization", network.label()),
    )?;
    Ok(HyperliquidAgentCredential::from_wallet(name, &wallet))
}

fn print_hyperliquid_agents(credential: &HyperliquidCredential) {
    if let Some(agent) = &credential.mainnet_agent {
        println!("  mainnet agent: {} ({})", agent.address, agent.name);
    }
    if let Some(agent) = &credential.testnet_agent {
        println!("  testnet agent: {} ({})", agent.address, agent.name);
    }
}

fn ensure_hyperliquid_exchange_ok(
    response: &crate::providers::hyperliquid::exchange::ExchangeResponseStatus,
    operation: &str,
) -> Result<()> {
    if let Some(error) = response_error(response) {
        bail!("Hyperliquid rejected {operation}: {error}");
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

fn hyperliquid_entry() -> Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, HYPERLIQUID_KEYRING_ACCOUNT)
        .context("failed to access the OS keychain")
}

fn legacy_hyperliquid_entry() -> Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, LEGACY_HYPERLIQUID_KEYRING_ACCOUNT)
        .context("failed to access the legacy Hyperliquid OS keychain entry")
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

fn load_hyperliquid_credential() -> Result<Option<HyperliquidCredential>> {
    let encoded = match hyperliquid_entry()?.get_password() {
        Ok(encoded) => Zeroizing::new(encoded),
        Err(keyring::Error::NoEntry) => match legacy_hyperliquid_entry()?.get_password() {
            Ok(encoded) => Zeroizing::new(encoded),
            Err(keyring::Error::NoEntry) => return Ok(None),
            Err(error) => {
                return Err(error)
                    .context("failed to read legacy Hyperliquid agent from OS keychain");
            }
        },
        Err(error) => {
            return Err(error).context("failed to read Hyperliquid agent from OS keychain");
        }
    };
    let header: CredentialVersion = serde_json::from_str(encoded.as_str())
        .context("stored Hyperliquid agent credential is malformed")?;
    let credential = match header.version {
        LEGACY_HYPERLIQUID_CREDENTIAL_VERSION => {
            serde_json::from_str::<LegacyHyperliquidCredential>(encoded.as_str())
                .context("stored legacy Hyperliquid agent credential is malformed")?
                .upgrade()?
        }
        HYPERLIQUID_CREDENTIAL_VERSION => {
            serde_json::from_str::<HyperliquidCredential>(encoded.as_str())
                .context("stored Hyperliquid agent credential is malformed")?
        }
        version => bail!("unsupported stored Hyperliquid credential version {version}"),
    };
    credential.validate()?;
    Ok(Some(credential))
}

fn save_hyperliquid_credential(credential: &HyperliquidCredential) -> Result<()> {
    credential.validate_complete()?;
    let encoded = Zeroizing::new(
        serde_json::to_string(credential)
            .context("failed to encode Hyperliquid agent credential")?,
    );
    hyperliquid_entry()?
        .set_password(encoded.as_str())
        .context("failed to store Hyperliquid agent in the OS keychain")?;
    match legacy_hyperliquid_entry()?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) => {
            Err(error).context("failed to remove the migrated Hyperliquid keychain entry")
        }
    }
}

fn delete_hyperliquid_credential() -> Result<()> {
    for entry in [hyperliquid_entry()?, legacy_hyperliquid_entry()?] {
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => {}
            Err(error) => {
                return Err(error).context("failed to remove Hyperliquid agent from OS keychain");
            }
        }
    }
    Ok(())
}

fn parse_hyperliquid_address(address: &str, name: &str) -> Result<String> {
    canonical_address(address)
        .with_context(|| format!("stored Hyperliquid {name} address is invalid"))
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

    #[test]
    fn legacy_hyperliquid_credential_upgrades_to_testnet_only() {
        let master = HyperliquidWallet::random();
        let agent = HyperliquidWallet::random();
        let credential = LegacyHyperliquidCredential {
            version: LEGACY_HYPERLIQUID_CREDENTIAL_VERSION,
            account: master.address(),
            agent_address: agent.address(),
            agent_private_key: agent.private_key_hex(),
        }
        .upgrade()
        .expect("legacy credential upgrades");

        assert!(credential.mainnet_agent.is_none());
        let testnet = credential
            .testnet_agent
            .as_ref()
            .expect("legacy testnet agent is preserved");
        assert_eq!(testnet.name, LEGACY_TESTNET_API_WALLET_NAME);
        assert_eq!(testnet.address, agent.address());
    }

    #[test]
    fn complete_hyperliquid_credential_has_distinct_network_agents() {
        let master = HyperliquidWallet::random();
        let mainnet = HyperliquidWallet::random();
        let testnet = HyperliquidWallet::random();
        let credential = HyperliquidCredential {
            version: HYPERLIQUID_CREDENTIAL_VERSION,
            account: master.address(),
            mainnet_agent: Some(HyperliquidAgentCredential::from_wallet(
                MAINNET_API_WALLET_NAME,
                &mainnet,
            )),
            testnet_agent: Some(HyperliquidAgentCredential::from_wallet(
                TESTNET_API_WALLET_NAME,
                &testnet,
            )),
        };

        credential
            .validate_complete()
            .expect("dual-network credential is valid");
        assert_eq!(
            credential
                .agent(HyperliquidNetwork::Mainnet)
                .expect("mainnet agent")
                .address,
            mainnet.address()
        );
        assert_eq!(
            credential
                .agent(HyperliquidNetwork::Testnet)
                .expect("testnet agent")
                .address,
            testnet.address()
        );
    }
}
