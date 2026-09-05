use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use bulk_keychain::{Keypair, Pubkey};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

use crate::cli::{AuthProvider, AuthProviderArgs, AuthSetArgs};
use crate::providers::bulk::{self, BulkNetwork};
use crate::providers::hyperliquid::HyperliquidNetwork;
use crate::providers::hyperliquid::exchange::{
    HYPERLINK_API_WALLET_NAME, LEGACY_TESTNET_API_WALLET_NAME, MAINNET_API_WALLET_NAME,
    TESTNET_API_WALLET_NAME, approve_agent, approve_builder_fee, approve_hyperlink_agent,
    response_error,
};
use crate::providers::hyperliquid::signing::{HyperliquidWallet, canonical_address};

const MMT_API_KEY_ENV: &str = "MMT_API_KEY";
const CREDENTIAL_DIRECTORY_MODE: u32 = 0o700;
const CREDENTIAL_FILE_MODE: u32 = 0o600;
const MMT_CREDENTIAL_FILE: &str = "mmt-api-key";
const BULK_CREDENTIAL_FILE: &str = "bulk-agent.json";
const HYPERLIQUID_CREDENTIAL_FILE: &str = "hyperliquid-agents.json";
const HYPERLINK_CREDENTIAL_FILE: &str = "hyperlink-agent.json";
const LEGACY_BULK_CREDENTIAL_VERSION: u8 = 1;
const LEGACY_NETWORKED_BULK_CREDENTIAL_VERSION: u8 = 2;
const BULK_CREDENTIAL_VERSION: u8 = 3;
const LEGACY_HYPERLIQUID_CREDENTIAL_VERSION: u8 = 1;
const LEGACY_NETWORKED_HYPERLIQUID_CREDENTIAL_VERSION: u8 = 2;
const LEGACY_SUBACCOUNT_HYPERLIQUID_CREDENTIAL_VERSION: u8 = 3;
const HYPERLIQUID_CREDENTIAL_VERSION: u8 = 4;
const HYPERLINK_CREDENTIAL_VERSION: u8 = 1;

static MMT_API_KEY: OnceLock<String> = OnceLock::new();
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub struct ActiveBulkCredential {
    pub account: Pubkey,
    pub agent: Keypair,
}

pub struct ActiveHyperliquidCredential {
    pub account: String,
    pub agent: HyperliquidWallet,
    pub vault_address: Option<String>,
    pub builder: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NamedSubaccount {
    name: String,
    account: String,
}

impl NamedSubaccount {
    fn validate_hyperliquid(&self) -> Result<()> {
        validate_subaccount_name(&self.name)?;
        let account = parse_hyperliquid_address(&self.account, "subaccount")?;
        if account != self.account.to_ascii_lowercase() {
            bail!("stored Hyperliquid subaccount address is not canonical");
        }
        Ok(())
    }

    fn validate_bulk(&self) -> Result<()> {
        validate_subaccount_name(&self.name)?;
        Pubkey::from_base58(&self.account)
            .context("stored BULK subaccount public key is invalid")?;
        Ok(())
    }
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
    #[serde(default)]
    mainnet_subaccounts: Vec<NamedSubaccount>,
    #[serde(default)]
    testnet_subaccounts: Vec<NamedSubaccount>,
    #[serde(default)]
    builder: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HyperlinkCredential {
    version: u8,
    status: HyperlinkCredentialStatus,
    account: String,
    agent: HyperliquidAgentCredential,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HyperlinkCredentialStatus {
    Pending,
    Active,
}

impl HyperlinkCredential {
    fn validate(&self) -> Result<()> {
        if self.version != HYPERLINK_CREDENTIAL_VERSION {
            bail!(
                "unsupported stored HyperLink credential version {}",
                self.version
            );
        }
        let account = parse_hyperliquid_address(&self.account, "account")?;
        if account != self.account.to_ascii_lowercase() {
            bail!("stored HyperLink account address is not canonical");
        }
        self.agent.validate()
    }
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
        validate_named_subaccounts(
            &self.mainnet_subaccounts,
            NamedSubaccount::validate_hyperliquid,
        )?;
        validate_named_subaccounts(
            &self.testnet_subaccounts,
            NamedSubaccount::validate_hyperliquid,
        )?;
        if let Some(builder) = &self.builder {
            let canonical =
                canonical_address(builder).context("stored builder address is invalid")?;
            if canonical != *builder {
                bail!("stored Hyperliquid builder address is not canonical");
            }
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

    fn subaccounts(&self, network: HyperliquidNetwork) -> &[NamedSubaccount] {
        match network {
            HyperliquidNetwork::Mainnet => &self.mainnet_subaccounts,
            HyperliquidNetwork::Testnet => &self.testnet_subaccounts,
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
            mainnet_subaccounts: Vec::new(),
            testnet_subaccounts: Vec::new(),
            builder: None,
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
        let key = load_credential_file(MMT_CREDENTIAL_FILE, "MMT API key")?
            .context("MMT credentials are not configured; run `mlab auth set mmt`")?;
        validate_key(key.to_string(), "stored MMT API key")?
    };

    let _ = MMT_API_KEY.set(key.clone());
    Ok(key)
}

pub fn mmt_is_configured() -> Result<bool> {
    if std::env::var(MMT_API_KEY_ENV).is_ok_and(|key| !key.trim().is_empty()) {
        return Ok(true);
    }
    Ok(load_credential_file(MMT_CREDENTIAL_FILE, "MMT API key")?
        .is_some_and(|key| !key.trim().is_empty()))
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
    #[serde(default)]
    mainnet_authorized: bool,
    #[serde(default)]
    testnet_authorized: bool,
    #[serde(default)]
    mainnet_subaccounts: Vec<NamedSubaccount>,
    #[serde(default)]
    testnet_subaccounts: Vec<NamedSubaccount>,
    #[serde(default, rename = "subaccounts", skip_serializing)]
    legacy_subaccounts: Vec<NamedSubaccount>,
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
            mainnet_authorized: false,
            testnet_authorized: false,
            mainnet_subaccounts: Vec::new(),
            testnet_subaccounts: Vec::new(),
            legacy_subaccounts: Vec::new(),
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
        if self.status == BulkCredentialStatus::Active
            && !self.mainnet_authorized
            && !self.testnet_authorized
        {
            bail!("stored active BULK credential has no authorized network");
        }

        validate_named_subaccounts(&self.mainnet_subaccounts, NamedSubaccount::validate_bulk)?;
        validate_named_subaccounts(&self.testnet_subaccounts, NamedSubaccount::validate_bulk)?;

        Ok(())
    }

    fn agent_keypair(&self) -> Result<Keypair> {
        Keypair::from_base58(&self.agent_private_key)
            .context("stored BULK agent private key is invalid")
    }

    fn is_authorized(&self, network: BulkNetwork) -> bool {
        match network {
            BulkNetwork::Mainnet => self.mainnet_authorized,
            BulkNetwork::Testnet => self.testnet_authorized,
        }
    }

    fn set_authorized(&mut self, network: BulkNetwork, authorized: bool) {
        match network {
            BulkNetwork::Mainnet => self.mainnet_authorized = authorized,
            BulkNetwork::Testnet => self.testnet_authorized = authorized,
        }
    }

    fn subaccounts(&self, network: BulkNetwork) -> &[NamedSubaccount] {
        match network {
            BulkNetwork::Mainnet => &self.mainnet_subaccounts,
            BulkNetwork::Testnet => &self.testnet_subaccounts,
        }
    }

    fn subaccounts_mut(&mut self, network: BulkNetwork) -> &mut Vec<NamedSubaccount> {
        match network {
            BulkNetwork::Mainnet => &mut self.mainnet_subaccounts,
            BulkNetwork::Testnet => &mut self.testnet_subaccounts,
        }
    }

    fn upgrade(&mut self) {
        if matches!(
            self.version,
            LEGACY_BULK_CREDENTIAL_VERSION | LEGACY_NETWORKED_BULK_CREDENTIAL_VERSION
        ) {
            self.version = BULK_CREDENTIAL_VERSION;
            self.testnet_authorized = self.status == BulkCredentialStatus::Active;
            self.testnet_subaccounts = std::mem::take(&mut self.legacy_subaccounts);
        }
    }
}

impl Drop for BulkCredential {
    fn drop(&mut self) {
        self.agent_private_key.zeroize();
    }
}

pub fn active_bulk_credential() -> Result<ActiveBulkCredential> {
    active_bulk_credential_for(BulkNetwork::Mainnet, "main")
}

pub fn active_bulk_credential_for(
    network: BulkNetwork,
    name: &str,
) -> Result<ActiveBulkCredential> {
    let credential = load_bulk_credential()?
        .context("BULK credentials are not configured; run `mlab auth set bulk`")?;
    if !credential.is_authorized(network) {
        bail!(
            "BULK {} agent is not authorized; run `mlab auth set bulk{}`",
            network.label(),
            if network == BulkNetwork::Testnet {
                " --testnet"
            } else {
                ""
            }
        );
    }
    let main_account = credential
        .account
        .as_deref()
        .context("stored BULK credential is missing its account public key")?;
    let account =
        resolve_named_account(main_account, credential.subaccounts(network), name, "BULK")?;
    Ok(ActiveBulkCredential {
        account: Pubkey::from_base58(&account)
            .context("stored BULK account public key is invalid")?,
        agent: credential.agent_keypair()?,
    })
}

pub fn active_bulk_credential_for_account(
    network: BulkNetwork,
    account: &str,
) -> Result<ActiveBulkCredential> {
    let credential = load_bulk_credential()?
        .context("BULK credentials are not configured; run `mlab auth set bulk`")?;
    if !credential.is_authorized(network) {
        bail!("BULK {} agent is not authorized", network.label());
    }
    let main = credential
        .account
        .as_deref()
        .context("stored BULK credential is missing its account public key")?;
    let configured = main == account
        || credential
            .subaccounts(network)
            .iter()
            .any(|subaccount| subaccount.account == account);
    if !configured {
        bail!("BULK account {account} is not configured in MarketLab");
    }
    Ok(ActiveBulkCredential {
        account: Pubkey::from_base58(account).context("BULK account public key is invalid")?,
        agent: credential.agent_keypair()?,
    })
}

pub fn bulk_account() -> Result<String> {
    Ok(active_bulk_credential()?.account.to_base58())
}

pub fn bulk_account_for(network: BulkNetwork, name: &str) -> Result<String> {
    Ok(active_bulk_credential_for(network, name)?
        .account
        .to_base58())
}

pub fn bulk_accounts(network: BulkNetwork) -> Result<Vec<(String, String)>> {
    let credential = load_bulk_credential()?
        .context("BULK credentials are not configured; run `mlab auth set bulk`")?;
    let main = credential
        .account
        .clone()
        .context("stored BULK credential is missing its account public key")?;
    if !credential.is_authorized(network) {
        bail!("BULK {} agent is not authorized", network.label());
    }
    Ok(std::iter::once(("main".to_string(), main))
        .chain(
            credential
                .subaccounts(network)
                .iter()
                .map(|subaccount| (subaccount.name.clone(), subaccount.account.clone())),
        )
        .collect())
}

pub fn active_hyperliquid_credential(
    network: HyperliquidNetwork,
) -> Result<ActiveHyperliquidCredential> {
    active_hyperliquid_credential_for(network, "main")
}

pub fn active_hyperliquid_credential_for(
    network: HyperliquidNetwork,
    name: &str,
) -> Result<ActiveHyperliquidCredential> {
    let credential = load_hyperliquid_credential()?
        .context("Hyperliquid credentials are not configured; run `mlab auth set hyperliquid`")?;
    let agent = credential.agent(network).with_context(|| {
        format!(
            "Hyperliquid {} API wallet is not configured; run `mlab auth set hyperliquid` to complete setup or add `--reauthorize` to replace all agents",
            network.label()
        )
    })?;
    let account = resolve_named_account(
        &credential.account,
        credential.subaccounts(network),
        name,
        "Hyperliquid",
    )?;
    Ok(ActiveHyperliquidCredential {
        vault_address: (account != credential.account).then(|| account.clone()),
        account,
        agent: agent.wallet()?,
        builder: (network == HyperliquidNetwork::Mainnet)
            .then(|| credential.builder.clone())
            .flatten(),
    })
}

pub fn hyperliquid_account() -> Result<String> {
    let credential = load_hyperliquid_credential()?
        .context("Hyperliquid credentials are not configured; run `mlab auth set hyperliquid`")?;
    Ok(credential.account)
}

pub fn hyperliquid_account_for(network: HyperliquidNetwork, name: &str) -> Result<String> {
    Ok(active_hyperliquid_credential_for(network, name)?.account)
}

pub fn hyperliquid_accounts(network: HyperliquidNetwork) -> Result<Vec<(String, String)>> {
    let credential = load_hyperliquid_credential()?
        .context("Hyperliquid credentials are not configured; run `mlab auth set hyperliquid`")?;
    Ok(
        std::iter::once(("main".to_string(), credential.account.clone()))
            .chain(
                credential
                    .subaccounts(network)
                    .iter()
                    .map(|subaccount| (subaccount.name.clone(), subaccount.account.clone())),
            )
            .collect(),
    )
}

pub fn active_hyperlink_credential() -> Result<ActiveHyperliquidCredential> {
    let credential = load_hyperlink_credential()?
        .context("HyperLink credentials are not configured; run `mlab auth set hyperlink`")?;
    if credential.status != HyperlinkCredentialStatus::Active {
        bail!(
            "HyperLink API-wallet authorization is pending; run `mlab auth set hyperlink` to finish it"
        );
    }
    Ok(ActiveHyperliquidCredential {
        account: credential.account,
        agent: credential.agent.wallet()?,
        vault_address: None,
        builder: None,
    })
}

pub fn hyperlink_account_for(name: &str) -> Result<String> {
    if !name.trim().is_empty() && !name.eq_ignore_ascii_case("main") {
        bail!("HyperLink subaccounts are not supported; use account `main`");
    }
    Ok(active_hyperlink_credential()?.account)
}

pub fn hyperlink_accounts() -> Result<Vec<(String, String)>> {
    Ok(vec![("main".to_string(), hyperlink_account_for("main")?)])
}

pub async fn handle_set(args: AuthSetArgs) -> Result<()> {
    if args.reauthorize && args.subaccount.is_some() {
        bail!("`--reauthorize` and `--subaccount` cannot be used together");
    }
    if args.subaccount.is_some() && (args.builder.is_some() || args.clear_builder) {
        bail!("`--subaccount` cannot be used with `--builder` or `--clear-builder`");
    }
    if args.clear_builder && args.reauthorize {
        bail!("`--clear-builder` and `--reauthorize` cannot be used together");
    }
    if !matches!(args.provider, AuthProvider::Hyperliquid)
        && (args.builder.is_some() || args.clear_builder)
    {
        bail!("`--builder` and `--clear-builder` are available only for Hyperliquid");
    }
    if args.testnet && !matches!(args.provider, AuthProvider::Bulk) {
        bail!("`--testnet` is available here only for BULK");
    }
    match args.provider {
        AuthProvider::Mmt => {
            if args.subaccount.is_some() {
                bail!("MMT does not support execution subaccounts");
            }
            if args.reauthorize {
                bail!("`--reauthorize` is only supported for execution venues");
            }
            let key = rpassword::prompt_password("MMT API key: ")?;
            let key = validate_key(key, "MMT API key")?;
            save_credential_file(MMT_CREDENTIAL_FILE, key.as_bytes(), "MMT API key")?;
            crate::markets::refresh_mmt()
                .await
                .context("MMT was configured, but its market snapshot could not be initialized")?;
            crate::runtime::reload_markets_if_running().await?;
            println!("mmt: configured");
            print_credential_location(MMT_CREDENTIAL_FILE)?;
        }
        AuthProvider::Bulk => {
            handle_set_bulk(
                args.reauthorize,
                args.subaccount.as_deref(),
                BulkNetwork::from_testnet(args.testnet),
            )
            .await?
        }
        AuthProvider::Hyperliquid => {
            handle_set_hyperliquid(
                args.reauthorize,
                args.subaccount.as_deref(),
                args.builder.as_deref(),
                args.clear_builder,
            )
            .await?
        }
        AuthProvider::Hyperlink => {
            handle_set_hyperlink(args.reauthorize, args.subaccount.as_deref()).await?
        }
    }
    Ok(())
}

pub fn handle_status() -> Result<()> {
    print_mmt_status()?;
    print_bulk_status()?;
    print_hyperliquid_status()?;
    print_hyperlink_status()?;
    Ok(())
}

fn print_hyperlink_status() -> Result<()> {
    match load_hyperlink_credential()? {
        Some(credential) => {
            println!(
                "hyperlink: {} in local credential store",
                match credential.status {
                    HyperlinkCredentialStatus::Pending => "pending authorization",
                    HyperlinkCredentialStatus::Active => "configured",
                }
            );
            println!("  account: {}", credential.account);
            println!(
                "  agent: {} ({})",
                credential.agent.address, credential.agent.name
            );
        }
        None => println!("hyperlink: not configured"),
    }
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
            println!("hyperliquid: {status} in local credential store");
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
            print_subaccounts("mainnet subaccounts", &credential.mainnet_subaccounts);
            print_subaccounts("testnet subaccounts", &credential.testnet_subaccounts);
            match &credential.builder {
                Some(builder) => println!("  builder: {builder} (0 fee, mainnet)"),
                None => println!("  builder: not configured"),
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

    match load_credential_file(MMT_CREDENTIAL_FILE, "MMT API key")? {
        Some(key) if !key.trim().is_empty() => {
            println!("mmt: configured in local credential store");
        }
        Some(_) | None => println!("mmt: not configured"),
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
            println!("bulk: {status} in local credential store");
            if let Some(account) = &credential.account {
                println!("  account: {account}");
            }
            println!("  agent: {}", credential.agent_public_key);
            println!(
                "  mainnet: {}",
                if credential.mainnet_authorized {
                    "authorized"
                } else {
                    "not configured"
                }
            );
            println!(
                "  testnet: {}",
                if credential.testnet_authorized {
                    "authorized"
                } else {
                    "not configured"
                }
            );
            print_subaccounts("mainnet subaccounts", &credential.mainnet_subaccounts);
            print_subaccounts("testnet subaccounts", &credential.testnet_subaccounts);
        }
        None => println!("bulk: not configured"),
    }
    Ok(())
}

fn print_subaccounts(label: &str, subaccounts: &[NamedSubaccount]) {
    if subaccounts.is_empty() {
        return;
    }
    println!("  {label}:");
    for subaccount in subaccounts {
        println!("    {}: {}", subaccount.name, subaccount.account);
    }
}

fn validate_subaccount_name(name: &str) -> Result<&str> {
    let name = name.trim();
    if name.is_empty() || name.len() > 32 {
        bail!("subaccount name must contain 1 to 32 characters");
    }
    if name.eq_ignore_ascii_case("main") {
        bail!("`main` is reserved for the main account");
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("subaccount name may contain only letters, numbers, `-`, and `_`");
    }
    Ok(name)
}

fn validate_named_subaccounts(
    subaccounts: &[NamedSubaccount],
    validate: fn(&NamedSubaccount) -> Result<()>,
) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    let mut accounts = std::collections::HashSet::new();
    for subaccount in subaccounts {
        validate(subaccount)?;
        if !names.insert(subaccount.name.to_ascii_lowercase()) {
            bail!(
                "stored credential contains duplicate subaccount name `{}`",
                subaccount.name
            );
        }
        if !accounts.insert(subaccount.account.to_ascii_lowercase()) {
            bail!(
                "stored credential contains duplicate subaccount address `{}`",
                subaccount.account
            );
        }
    }
    Ok(())
}

fn resolve_named_account(
    main_account: &str,
    subaccounts: &[NamedSubaccount],
    name: &str,
    venue: &str,
) -> Result<String> {
    let name = name.trim();
    if name.is_empty() || name.eq_ignore_ascii_case("main") {
        return Ok(main_account.to_string());
    }
    subaccounts
        .iter()
        .find(|subaccount| subaccount.name.eq_ignore_ascii_case(name))
        .map(|subaccount| subaccount.account.clone())
        .with_context(|| {
            format!(
                "{venue} subaccount `{name}` is not configured; create it with `mlab auth set {} --subaccount {name}`",
                venue.to_ascii_lowercase()
            )
        })
}

async fn handle_create_hyperliquid_subaccount(name: &str) -> Result<()> {
    let name = validate_subaccount_name(name)?.to_string();
    let mut credential = load_hyperliquid_credential()?.context(
        "configure the Hyperliquid main account before creating a subaccount with `mlab auth set hyperliquid`",
    )?;
    credential.validate_complete()?;
    if credential
        .mainnet_subaccounts
        .iter()
        .chain(&credential.testnet_subaccounts)
        .any(|subaccount| subaccount.name.eq_ignore_ascii_case(&name))
    {
        bail!("Hyperliquid subaccount `{name}` already exists; use another name");
    }
    for network in [HyperliquidNetwork::Mainnet, HyperliquidNetwork::Testnet] {
        let remote =
            crate::providers::hyperliquid::exchange::subaccounts(&credential.account, network)
                .await?;
        if remote
            .iter()
            .any(|(remote_name, _)| remote_name.eq_ignore_ascii_case(&name))
        {
            bail!(
                "Hyperliquid {} subaccount `{name}` already exists; use another name",
                network.label()
            );
        }
    }

    println!(
        "The main wallet private key is used only to create the subaccount and is never stored."
    );
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "Hyperliquid main wallet private key (hidden): ",
        )?);
        HyperliquidWallet::from_private_key(private_key.trim())
            .context("invalid Hyperliquid main wallet private key")?
    };
    if master.address() != credential.account {
        bail!(
            "the supplied key belongs to {}, but the configured Hyperliquid main account is {}",
            master.address(),
            credential.account
        );
    }
    println!("hyperliquid: creating mainnet subaccount `{name}`");
    let mainnet = crate::providers::hyperliquid::exchange::create_subaccount(
        &master,
        HyperliquidNetwork::Mainnet,
        &name,
    )
    .await?;
    println!("hyperliquid: creating testnet subaccount `{name}`");
    let testnet = crate::providers::hyperliquid::exchange::create_subaccount(
        &master,
        HyperliquidNetwork::Testnet,
        &name,
    )
    .await?;
    credential.mainnet_subaccounts.push(NamedSubaccount {
        name: name.clone(),
        account: mainnet.clone(),
    });
    credential.testnet_subaccounts.push(NamedSubaccount {
        name: name.clone(),
        account: testnet.clone(),
    });
    save_hyperliquid_credential(&credential)?;
    println!("hyperliquid: subaccount created");
    println!("  name: {name}");
    println!("  mainnet: {mainnet}");
    println!("  testnet: {testnet}");
    Ok(())
}

async fn handle_create_bulk_subaccount(name: &str, network: BulkNetwork) -> Result<()> {
    let name = validate_subaccount_name(name)?.to_string();
    let mut credential = load_bulk_credential()?.context(
        "configure the BULK main account before creating a subaccount with `mlab auth set bulk`",
    )?;
    if !credential.is_authorized(network) {
        bail!(
            "configure BULK {} before creating a subaccount",
            network.label()
        );
    }
    if credential
        .subaccounts(network)
        .iter()
        .any(|subaccount| subaccount.name.eq_ignore_ascii_case(&name))
    {
        bail!("BULK subaccount `{name}` already exists; use another name");
    }
    println!(
        "The main wallet private key is used only to create the subaccount and is never stored."
    );
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "BULK main wallet private key (hidden): ",
        )?);
        Keypair::from_base58(private_key.trim()).context("invalid BULK main wallet private key")?
    };
    let main_account = credential
        .account
        .as_deref()
        .context("stored BULK credential is missing its main account")?;
    if master.pubkey().to_base58() != main_account {
        bail!(
            "the supplied key belongs to {}, but the configured BULK main account is {main_account}",
            master.pubkey().to_base58()
        );
    }
    println!("bulk: creating {} subaccount `{name}`", network.label());
    let account = bulk::create_subaccount(network, master, &name).await?;
    credential.subaccounts_mut(network).push(NamedSubaccount {
        name: name.clone(),
        account: account.clone(),
    });
    save_bulk_credential(&credential)?;
    println!("bulk: subaccount created");
    println!("  network: {}", network.label());
    println!("  name: {name}");
    println!("  account: {account}");
    Ok(())
}

pub async fn handle_remove(args: AuthProviderArgs) -> Result<()> {
    match args.provider {
        AuthProvider::Mmt => {
            delete_credential_file(MMT_CREDENTIAL_FILE, "MMT API key")?;
            println!("mmt: removed");
        }
        AuthProvider::Bulk => handle_remove_bulk().await?,
        AuthProvider::Hyperliquid => handle_remove_hyperliquid().await?,
        AuthProvider::Hyperlink => handle_remove_hyperlink().await?,
    }
    Ok(())
}

async fn handle_set_hyperlink(reauthorize: bool, subaccount: Option<&str>) -> Result<()> {
    if subaccount.is_some() {
        bail!("HyperLink subaccounts are not supported");
    }
    let existing = load_hyperlink_credential()?;
    if let Some(credential) = &existing
        && credential.status == HyperlinkCredentialStatus::Active
        && !reauthorize
    {
        println!("hyperlink: already configured");
        println!("  account: {}", credential.account);
        println!(
            "  agent: {} ({})",
            credential.agent.address, credential.agent.name
        );
        println!("  use `mlab auth set hyperlink --reauthorize` to replace the API wallet");
        return Ok(());
    }

    println!("HyperLink mainnet API-wallet setup.");
    println!("The main wallet private key is used only for approval and is never stored.");
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "HyperLink main wallet private key (hidden): ",
        )?);
        HyperliquidWallet::from_private_key(private_key.trim())
            .context("invalid HyperLink main wallet private key")?
    };
    let account = master.address();
    if let Some(existing) = &existing
        && existing.account != account
    {
        bail!(
            "this HyperLink credential belongs to {}, but the supplied key belongs to {account}",
            existing.account
        );
    }

    let mut credential = match existing {
        Some(credential) => {
            println!(
                "hyperlink: {} API wallet `{HYPERLINK_API_WALLET_NAME}`",
                if credential.status == HyperlinkCredentialStatus::Pending {
                    "retrying authorization for pending"
                } else {
                    "reauthorizing"
                }
            );
            credential
        }
        None => {
            let wallet = HyperliquidWallet::random();
            let credential = HyperlinkCredential {
                version: HYPERLINK_CREDENTIAL_VERSION,
                status: HyperlinkCredentialStatus::Pending,
                account: account.clone(),
                agent: HyperliquidAgentCredential::from_wallet(HYPERLINK_API_WALLET_NAME, &wallet),
            };
            save_hyperlink_credential(&credential)?;
            println!("hyperlink: generated and stored a pending API wallet");
            println!("  agent: {}", credential.agent.address);
            credential
        }
    };
    let wallet = credential.agent.wallet()?;
    let response = approve_hyperlink_agent(&master, &wallet, HYPERLINK_API_WALLET_NAME)
        .await
        .with_context(|| {
            if credential.status == HyperlinkCredentialStatus::Pending {
                "HyperLink API-wallet authorization was not confirmed; the pending API wallet remains stored and `mlab auth set hyperlink` can safely retry it"
            } else {
                "HyperLink API-wallet reauthorization was not confirmed; the existing API wallet remains stored"
            }
        })?;
    if let Some(error) = response_error(&response) {
        bail!("HyperLink rejected API-wallet authorization: {error}");
    }
    credential.status = HyperlinkCredentialStatus::Active;
    save_hyperlink_credential(&credential).context(
        "HyperLink authorized the API wallet, but Market Lab could not mark the stored credential active; rerun `mlab auth set hyperlink` to retry safely",
    )?;
    println!("hyperlink: configured");
    println!("  account: {account}");
    println!(
        "  agent: {} ({})",
        credential.agent.address, credential.agent.name
    );
    print_credential_location(HYPERLINK_CREDENTIAL_FILE)
}

async fn handle_remove_hyperlink() -> Result<()> {
    let Some(credential) = load_hyperlink_credential()? else {
        println!("hyperlink: not configured");
        return Ok(());
    };
    if credential.status == HyperlinkCredentialStatus::Pending {
        bail!(
            "this HyperLink API wallet has an unconfirmed authorization; retry `mlab auth set hyperlink` before removing it so Market Lab does not discard a potentially authorized key"
        );
    }
    println!(
        "The main wallet private key is used once to replace the stored HyperLink API wallet."
    );
    let master = {
        let private_key = Zeroizing::new(rpassword::prompt_password(
            "HyperLink main wallet private key (hidden): ",
        )?);
        HyperliquidWallet::from_private_key(private_key.trim())
            .context("invalid HyperLink main wallet private key")?
    };
    if master.address() != credential.account {
        bail!(
            "the supplied key belongs to {}, but the stored HyperLink agent belongs to {}",
            master.address(),
            credential.account
        );
    }
    let replacement = HyperliquidWallet::random();
    let response = approve_hyperlink_agent(&master, &replacement, &credential.agent.name)
        .await
        .context("failed to replace the stored HyperLink API wallet")?;
    if let Some(error) = response_error(&response) {
        bail!("HyperLink rejected API-wallet replacement: {error}");
    }
    delete_credential_file(HYPERLINK_CREDENTIAL_FILE, "HyperLink agent")?;
    println!("hyperlink: revoked and removed");
    Ok(())
}

async fn handle_set_hyperliquid(
    reauthorize: bool,
    subaccount: Option<&str>,
    builder: Option<&str>,
    clear_builder: bool,
) -> Result<()> {
    if let Some(name) = subaccount {
        return handle_create_hyperliquid_subaccount(name).await;
    }
    let mut existing = load_hyperliquid_credential()?;
    if clear_builder {
        let credential = existing.as_mut().context(
            "Hyperliquid credentials are not configured; run `mlab auth set hyperliquid`",
        )?;
        if credential.builder.take().is_none() {
            println!("hyperliquid: builder is already disabled");
            return Ok(());
        }
        save_hyperliquid_credential(credential)?;
        println!("hyperliquid: builder disabled");
        return Ok(());
    }

    let builder = builder
        .map(canonical_address)
        .transpose()
        .context("invalid Hyperliquid builder address")?;
    let agents_complete = existing
        .as_ref()
        .is_some_and(|credential| credential.validate_complete().is_ok());
    let replacing_existing = reauthorize && existing.is_some();
    if agents_complete && !reauthorize && builder.is_none() {
        let credential = existing.as_ref().expect("checked above");
        println!("hyperliquid: already configured for mainnet and testnet");
        println!("  account: {}", credential.account);
        print_hyperliquid_agents(credential);
        print_hyperliquid_builder(credential);
        println!("  use `mlab auth set hyperliquid --reauthorize` to replace the API wallet");
        return Ok(());
    }
    if agents_complete
        && !reauthorize
        && existing
            .as_ref()
            .and_then(|credential| credential.builder.as_ref())
            == builder.as_ref()
    {
        let credential = existing.as_ref().expect("checked above");
        println!("hyperliquid: builder already configured");
        print_hyperliquid_builder(credential);
        return Ok(());
    }

    if agents_complete && !reauthorize {
        println!("Hyperliquid mainnet builder setup.");
    } else {
        println!("Hyperliquid mainnet and testnet API-wallet setup.");
    }
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

    if let Some(builder) = &builder {
        println!("hyperliquid: approving mainnet builder `{builder}` with zero fee");
        let response = approve_builder_fee(&master, HyperliquidNetwork::Mainnet, builder, "0%")
            .await
            .context("Hyperliquid mainnet builder approval failed")?;
        ensure_hyperliquid_exchange_ok(&response, "mainnet builder approval")?;
    }

    let credential = HyperliquidCredential {
        version: HYPERLIQUID_CREDENTIAL_VERSION,
        account: account.clone(),
        mainnet_agent: Some(mainnet_agent),
        testnet_agent: Some(testnet_agent),
        mainnet_subaccounts: existing.as_mut().map_or_else(Vec::new, |credential| {
            std::mem::take(&mut credential.mainnet_subaccounts)
        }),
        testnet_subaccounts: existing.as_mut().map_or_else(Vec::new, |credential| {
            std::mem::take(&mut credential.testnet_subaccounts)
        }),
        builder: builder.or_else(|| {
            existing
                .as_mut()
                .and_then(|credential| credential.builder.take())
        }),
    };
    credential.validate_complete()?;
    save_hyperliquid_credential(&credential)?;
    if !agents_complete || reauthorize {
        crate::markets::refresh_hyperliquid().await.context(
            "Hyperliquid was configured, but its market snapshot could not be initialized",
        )?;
        crate::runtime::reload_markets_if_running().await?;
    }

    if agents_complete && !reauthorize {
        println!("hyperliquid: builder configured");
    } else {
        println!("hyperliquid: configured for mainnet and testnet");
    }
    println!("  account: {account}");
    print_hyperliquid_agents(&credential);
    print_hyperliquid_builder(&credential);
    print_credential_location(HYPERLIQUID_CREDENTIAL_FILE)?;
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

fn print_hyperliquid_builder(credential: &HyperliquidCredential) {
    match &credential.builder {
        Some(builder) => println!("  builder: {builder} (0 fee, mainnet)"),
        None => println!("  builder: not configured"),
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

async fn handle_set_bulk(
    reauthorize: bool,
    subaccount: Option<&str>,
    network: BulkNetwork,
) -> Result<()> {
    if let Some(name) = subaccount {
        return handle_create_bulk_subaccount(name, network).await;
    }
    let mut credential = match load_bulk_credential()? {
        Some(credential) if credential.is_authorized(network) && !reauthorize => {
            println!("bulk: {} already configured", network.label());
            println!(
                "  account: {}",
                credential.account.as_deref().unwrap_or("unknown")
            );
            println!("  agent: {}", credential.agent_public_key);
            println!(
                "  use `mlab auth set bulk{} --reauthorize` if BULK rejects this agent as unauthorized",
                if network == BulkNetwork::Testnet {
                    " --testnet"
                } else {
                    ""
                }
            );
            return Ok(());
        }
        Some(credential) if credential.is_authorized(network) => {
            println!("bulk: reauthorizing the existing {} agent", network.label());
            credential
        }
        Some(credential) => {
            println!(
                "bulk: authorizing the existing agent on {}",
                network.label()
            );
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
    println!(
        "bulk: authorizing the agent for account {account} on {}",
        network.label()
    );

    let registration = bulk::register_agent(network, master, agent)
        .await
        .with_context(|| {
            format!(
                "BULK {} agent authorization was not confirmed; the local agent was preserved and `mlab auth set bulk{}{}` can safely retry it",
                network.label(),
                if network == BulkNetwork::Testnet {
                    " --testnet"
                } else {
                    ""
                },
                if reauthorize { " --reauthorize" } else { "" }
            )
        })?;

    if registration.account != account
        || registration.agent_public_key != credential.agent_public_key
    {
        bail!("BULK registration confirmation did not match the requested account and agent");
    }

    credential.status = BulkCredentialStatus::Active;
    credential.set_authorized(network, true);
    save_bulk_credential(&credential).with_context(|| {
        if reauthorize {
            "BULK reauthorized the agent, but Market Lab could not refresh it in the local credential store; the existing credential was preserved"
        } else {
            "BULK registered the agent, but Market Lab could not mark it active in the local credential store; the pending credential was preserved"
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
    println!("  network: {}", network.label());
    print_credential_location(BULK_CREDENTIAL_FILE)?;
    Ok(())
}

async fn handle_remove_bulk() -> Result<()> {
    let Some(mut credential) = load_bulk_credential()? else {
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
        .clone()
        .context("stored BULK credential is missing its account public key")?;
    let agent = credential.agent_keypair()?.pubkey();

    println!("The main wallet private key is used once for revocation and is never stored.");
    let private_key = Zeroizing::new(rpassword::prompt_password(
        "BULK main wallet private key (hidden): ",
    )?);
    {
        let master = Keypair::from_base58(private_key.trim())
            .context("invalid BULK main wallet private key")?;
        let supplied_account = master.pubkey().to_base58();
        if supplied_account != account {
            bail!(
                "the supplied key belongs to BULK account {supplied_account}, but this agent belongs to {account}"
            );
        }
    }

    println!("bulk: revoking agent {}", credential.agent_public_key);
    for network in [BulkNetwork::Mainnet, BulkNetwork::Testnet] {
        if !credential.is_authorized(network) {
            continue;
        }
        let master = Keypair::from_base58(private_key.trim())
            .context("invalid BULK main wallet private key")?;
        bulk::revoke_agent(network, master, agent)
            .await
            .with_context(|| {
                format!(
                    "BULK {} agent revocation was not confirmed; the agent remains in the local credential store",
                    network.label()
                )
            })?;
        credential.set_authorized(network, false);
        if credential.mainnet_authorized || credential.testnet_authorized {
            save_bulk_credential(&credential)?;
        }
    }

    delete_bulk_credential()?;
    println!("bulk: revoked and removed");
    Ok(())
}

fn delete_bulk_credential() -> Result<()> {
    delete_credential_file(BULK_CREDENTIAL_FILE, "BULK agent")
}

fn load_bulk_credential() -> Result<Option<BulkCredential>> {
    let Some(encoded) = load_credential_file(BULK_CREDENTIAL_FILE, "BULK agent")? else {
        return Ok(None);
    };

    let mut credential: BulkCredential = serde_json::from_str(encoded.as_str())
        .context("stored BULK agent credential is malformed")?;
    credential.upgrade();
    credential.validate()?;
    Ok(Some(credential))
}

fn save_bulk_credential(credential: &BulkCredential) -> Result<()> {
    credential.validate()?;
    let encoded = Zeroizing::new(
        serde_json::to_string(credential).context("failed to encode BULK agent credential")?,
    );
    save_credential_file(BULK_CREDENTIAL_FILE, encoded.as_bytes(), "BULK agent")
}

fn load_hyperliquid_credential() -> Result<Option<HyperliquidCredential>> {
    let Some(encoded) = load_credential_file(HYPERLIQUID_CREDENTIAL_FILE, "Hyperliquid agent")?
    else {
        return Ok(None);
    };
    let header: CredentialVersion = serde_json::from_str(encoded.as_str())
        .context("stored Hyperliquid agent credential is malformed")?;
    let credential = match header.version {
        LEGACY_HYPERLIQUID_CREDENTIAL_VERSION => {
            serde_json::from_str::<LegacyHyperliquidCredential>(encoded.as_str())
                .context("stored legacy Hyperliquid agent credential is malformed")?
                .upgrade()?
        }
        LEGACY_NETWORKED_HYPERLIQUID_CREDENTIAL_VERSION
        | LEGACY_SUBACCOUNT_HYPERLIQUID_CREDENTIAL_VERSION
        | HYPERLIQUID_CREDENTIAL_VERSION => {
            let mut credential = serde_json::from_str::<HyperliquidCredential>(encoded.as_str())
                .context("stored Hyperliquid agent credential is malformed")?;
            credential.version = HYPERLIQUID_CREDENTIAL_VERSION;
            credential
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
    save_credential_file(
        HYPERLIQUID_CREDENTIAL_FILE,
        encoded.as_bytes(),
        "Hyperliquid agent",
    )
}

fn delete_hyperliquid_credential() -> Result<()> {
    delete_credential_file(HYPERLIQUID_CREDENTIAL_FILE, "Hyperliquid agent")
}

fn load_hyperlink_credential() -> Result<Option<HyperlinkCredential>> {
    let Some(encoded) = load_credential_file(HYPERLINK_CREDENTIAL_FILE, "HyperLink agent")? else {
        return Ok(None);
    };
    let credential: HyperlinkCredential = serde_json::from_str(encoded.as_str())
        .context("stored HyperLink agent credential is malformed")?;
    credential.validate()?;
    Ok(Some(credential))
}

fn save_hyperlink_credential(credential: &HyperlinkCredential) -> Result<()> {
    credential.validate()?;
    let encoded = Zeroizing::new(
        serde_json::to_string(credential).context("failed to encode HyperLink agent credential")?,
    );
    save_credential_file(
        HYPERLINK_CREDENTIAL_FILE,
        encoded.as_bytes(),
        "HyperLink agent",
    )
}

fn credential_directory() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is required for the credential directory")?;
    Ok(PathBuf::from(home).join(".market-lab").join("credentials"))
}

fn credential_path(file_name: &str) -> Result<PathBuf> {
    Ok(credential_directory()?.join(file_name))
}

fn print_credential_location(file_name: &str) -> Result<()> {
    println!("  stored: {}", credential_path(file_name)?.display());
    println!("  permissions: 0600");
    Ok(())
}

fn load_credential_file(file_name: &str, label: &str) -> Result<Option<Zeroizing<String>>> {
    read_credential_at(&credential_directory()?, file_name, label)
}

fn save_credential_file(file_name: &str, contents: &[u8], label: &str) -> Result<()> {
    write_credential_at(&credential_directory()?, file_name, contents, label)
}

fn delete_credential_file(file_name: &str, label: &str) -> Result<()> {
    delete_credential_at(&credential_directory()?, file_name, label)
}

fn ensure_credential_directory(directory: &Path) -> Result<()> {
    fs::create_dir_all(directory).with_context(|| {
        format!(
            "failed to create credential directory {}",
            directory.display()
        )
    })?;
    let metadata = fs::symlink_metadata(directory).with_context(|| {
        format!(
            "failed to inspect credential directory {}",
            directory.display()
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "credential directory {} must be a real directory, not a symlink",
            directory.display()
        );
    }
    ensure_current_user_owns(directory, &metadata, "credential directory")?;
    fs::set_permissions(
        directory,
        fs::Permissions::from_mode(CREDENTIAL_DIRECTORY_MODE),
    )
    .with_context(|| {
        format!(
            "failed to secure credential directory {}",
            directory.display()
        )
    })?;
    Ok(())
}

fn ensure_current_user_owns(path: &Path, metadata: &fs::Metadata, label: &str) -> Result<()> {
    // `geteuid` has no preconditions and only reads the process's effective uid.
    let current_user = unsafe { libc::geteuid() };
    if metadata.uid() != current_user {
        bail!(
            "{label} {} is owned by uid {}, not the current uid {current_user}",
            path.display(),
            metadata.uid()
        );
    }
    Ok(())
}

fn validate_credential_metadata(path: &Path, metadata: &fs::Metadata, label: &str) -> Result<()> {
    if !metadata.is_file() {
        bail!(
            "{label} credential {} must be a regular file",
            path.display()
        );
    }
    ensure_current_user_owns(path, metadata, &format!("{label} credential"))?;
    let mode = metadata.mode() & 0o777;
    if mode != CREDENTIAL_FILE_MODE {
        bail!(
            "{label} credential {} has permissions {mode:04o}; run `chmod 600 {}`",
            path.display(),
            path.display()
        );
    }
    Ok(())
}

fn read_credential_at(
    directory: &Path,
    file_name: &str,
    label: &str,
) -> Result<Option<Zeroizing<String>>> {
    ensure_credential_directory(directory)?;
    let path = directory.join(file_name);
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to open {label} credential {}", path.display()));
        }
    };
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {label} credential {}", path.display()))?;
    validate_credential_metadata(&path, &metadata, label)?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("failed to read {label} credential {}", path.display()))?;
    Ok(Some(Zeroizing::new(contents)))
}

fn write_credential_at(
    directory: &Path,
    file_name: &str,
    contents: &[u8],
    label: &str,
) -> Result<()> {
    ensure_credential_directory(directory)?;
    let path = directory.join(file_name);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => validate_credential_metadata(&path, &metadata, label)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to inspect {label} credential {}", path.display())
            });
        }
    }

    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp_path = directory.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(CREDENTIAL_FILE_MODE)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temp_path)
            .with_context(|| {
                format!(
                    "failed to create temporary {label} credential {}",
                    temp_path.display()
                )
            })?;
        file.write_all(contents).with_context(|| {
            format!(
                "failed to write temporary {label} credential {}",
                temp_path.display()
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "failed to sync temporary {label} credential {}",
                temp_path.display()
            )
        })?;
        drop(file);
        fs::rename(&temp_path, &path)
            .with_context(|| format!("failed to replace {label} credential {}", path.display()))?;
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .with_context(|| {
                format!(
                    "failed to sync credential directory {}",
                    directory.display()
                )
            })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn delete_credential_at(directory: &Path, file_name: &str, label: &str) -> Result<()> {
    ensure_credential_directory(directory)?;
    let path = directory.join(file_name);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to inspect {label} credential {}", path.display())
            });
        }
    };
    if metadata.file_type().is_symlink() {
        bail!(
            "refusing to remove symlinked {label} credential {}",
            path.display()
        );
    }
    validate_credential_metadata(&path, &metadata, label)?;
    fs::remove_file(&path)
        .with_context(|| format!("failed to remove {label} credential {}", path.display()))
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
    use std::os::unix::fs::symlink;

    fn test_credential_directory(name: &str) -> PathBuf {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "market-lab-credentials-{name}-{}-{sequence}",
            std::process::id()
        ))
    }

    #[test]
    fn credential_file_round_trip_is_private_and_atomic() {
        let directory = test_credential_directory("round-trip");
        write_credential_at(&directory, "secret", b"first", "test").expect("credential writes");

        let directory_mode = fs::metadata(&directory).expect("directory metadata").mode() & 0o777;
        let file_mode = fs::metadata(directory.join("secret"))
            .expect("file metadata")
            .mode()
            & 0o777;
        assert_eq!(directory_mode, CREDENTIAL_DIRECTORY_MODE);
        assert_eq!(file_mode, CREDENTIAL_FILE_MODE);
        assert_eq!(
            read_credential_at(&directory, "secret", "test")
                .expect("credential reads")
                .expect("credential exists")
                .as_str(),
            "first"
        );

        write_credential_at(&directory, "secret", b"second", "test")
            .expect("credential replaces atomically");
        assert_eq!(
            read_credential_at(&directory, "secret", "test")
                .expect("replacement reads")
                .expect("replacement exists")
                .as_str(),
            "second"
        );
        assert_eq!(
            fs::read_dir(&directory).expect("directory reads").count(),
            1
        );

        delete_credential_at(&directory, "secret", "test").expect("credential deletes");
        assert!(
            read_credential_at(&directory, "secret", "test")
                .expect("deleted credential check")
                .is_none()
        );
        fs::remove_dir_all(directory).expect("test directory cleans up");
    }

    #[test]
    fn credential_file_rejects_broad_permissions() {
        let directory = test_credential_directory("permissions");
        write_credential_at(&directory, "secret", b"value", "test").expect("credential writes");
        fs::set_permissions(directory.join("secret"), fs::Permissions::from_mode(0o644))
            .expect("permissions change");

        let error = read_credential_at(&directory, "secret", "test")
            .expect_err("broad permissions must fail");
        assert!(error.to_string().contains("chmod 600"));
        fs::remove_dir_all(directory).expect("test directory cleans up");
    }

    #[test]
    fn credential_file_rejects_symlinks() {
        let directory = test_credential_directory("symlink");
        ensure_credential_directory(&directory).expect("credential directory exists");
        let target = directory.with_extension("target");
        fs::write(&target, "outside").expect("target writes");
        symlink(&target, directory.join("secret")).expect("symlink creates");

        let error =
            read_credential_at(&directory, "secret", "test").expect_err("symlink must fail");
        assert!(error.to_string().contains("failed to open"));
        fs::remove_dir_all(directory).expect("test directory cleans up");
        fs::remove_file(target).expect("test target cleans up");
    }

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
    fn existing_bulk_credentials_remain_testnet_credentials() {
        let account = Keypair::generate().pubkey().to_base58();
        let agent = Keypair::generate();
        let subaccount = Keypair::generate().pubkey().to_base58();
        let encoded = serde_json::json!({
            "version": LEGACY_NETWORKED_BULK_CREDENTIAL_VERSION,
            "status": "active",
            "account": account,
            "agent_public_key": agent.pubkey().to_base58(),
            "agent_private_key": agent.to_base58(),
            "subaccounts": [{"name": "maker", "account": subaccount}],
        });
        let mut credential: BulkCredential =
            serde_json::from_value(encoded).expect("legacy credential decodes");

        credential.upgrade();

        credential.validate().expect("upgraded credential is valid");
        assert!(!credential.is_authorized(BulkNetwork::Mainnet));
        assert!(credential.is_authorized(BulkNetwork::Testnet));
        assert_eq!(
            credential.subaccounts(BulkNetwork::Testnet)[0].name,
            "maker"
        );
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
            mainnet_subaccounts: Vec::new(),
            testnet_subaccounts: Vec::new(),
            builder: None,
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

    #[test]
    fn hyperlink_credential_is_separate_and_self_consistent() {
        let master = HyperliquidWallet::random();
        let agent = HyperliquidWallet::random();
        let credential = HyperlinkCredential {
            version: HYPERLINK_CREDENTIAL_VERSION,
            status: HyperlinkCredentialStatus::Active,
            account: master.address(),
            agent: HyperliquidAgentCredential::from_wallet(HYPERLINK_API_WALLET_NAME, &agent),
        };

        credential
            .validate()
            .expect("HyperLink credential is valid");
        assert_eq!(credential.agent.address, agent.address());
        assert_eq!(credential.agent.name, HYPERLINK_API_WALLET_NAME);
    }

    #[test]
    fn named_account_resolution_keeps_main_explicit_and_rejects_unknown_names() {
        let subaccounts = vec![NamedSubaccount {
            name: "trading-2".to_string(),
            account: "0x1111111111111111111111111111111111111111".to_string(),
        }];
        assert_eq!(
            resolve_named_account(
                "0x2222222222222222222222222222222222222222",
                &subaccounts,
                "main",
                "Hyperliquid",
            )
            .expect("main resolves"),
            "0x2222222222222222222222222222222222222222"
        );
        assert_eq!(
            resolve_named_account("main", &subaccounts, "TRADING-2", "Hyperliquid")
                .expect("named account resolves"),
            "0x1111111111111111111111111111111111111111"
        );
        assert!(
            resolve_named_account("main", &subaccounts, "missing", "Hyperliquid")
                .expect_err("unknown account must fail")
                .to_string()
                .contains("not configured")
        );
    }
}
