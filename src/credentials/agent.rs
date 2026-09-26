use super::*;
use crate::providers::hyperliquid::client::HyperliquidClient;

pub(super) async fn import(args: AuthSetArgs) -> Result<()> {
    let provider = match args.provider {
        AuthProvider::Mmt => bail!("MMT uses an API key, not an agent wallet"),
        AuthProvider::Bulk => "bulk",
        AuthProvider::Hyperliquid => "hyperliquid",
        AuthProvider::Hyperlink => "hyperlink",
    };
    if args.testnet && matches!(args.provider, AuthProvider::Hyperlink) {
        bail!("HyperLink agent import supports mainnet only");
    }
    let account = args.account.as_deref().context("--account is required")?;
    let account = if matches!(args.provider, AuthProvider::Bulk) {
        Pubkey::from_base58(account)
            .context("invalid BULK account public key")?
            .to_base58()
    } else {
        canonical_address(account).context("invalid main account address")?
    };
    let private_key = read_key(args.agent_stdin)?;
    let address = match args.provider {
        AuthProvider::Bulk => import_bulk(&args, &account, &private_key).await?,
        AuthProvider::Hyperliquid | AuthProvider::Hyperlink => {
            import_evm(&args, &account, &private_key).await?
        }
        AuthProvider::Mmt => unreachable!(),
    };
    let network = if args.testnet { "testnet" } else { "mainnet" };
    if args.output.as_deref() == Some("json") {
        println!(
            "{}",
            serde_json::json!({
                "provider": provider, "account": account, "agent": address,
                "network": network, "status": "configured"
            })
        );
    } else {
        println!("{provider}: imported {network} agent\n  account: {account}\n  agent: {address}");
    }
    Ok(())
}

fn read_key(from_stdin: bool) -> Result<Zeroizing<String>> {
    let key = if from_stdin {
        let mut key = Zeroizing::new(String::new());
        std::io::stdin()
            .lock()
            .take(513)
            .read_to_string(&mut key)
            .context("failed to read agent private key from stdin")?;
        key
    } else {
        Zeroizing::new(rpassword::prompt_password("Agent private key (hidden): ")?)
    };
    if key.len() > 512 || key.trim().is_empty() {
        bail!("agent private key must contain 1 to 512 bytes");
    }
    Ok(key)
}

fn check_replacement(
    account: &str,
    address: &str,
    existing_account: Option<&str>,
    existing_agent: Option<&str>,
    replace: bool,
) -> Result<()> {
    if account == address {
        bail!("the supplied key is the main wallet key; provide an authorized agent key instead");
    }
    if existing_account.is_some_and(|stored| stored != account) {
        bail!("credentials belong to another account; use a separate MLAB_HOME");
    }
    if !replace && existing_agent.is_some_and(|stored| stored != address) {
        bail!("a different agent is already stored; use --replace to replace it locally");
    }
    Ok(())
}

async fn import_bulk(args: &AuthSetArgs, account: &str, key: &str) -> Result<String> {
    let wallet = Keypair::from_base58(key.trim()).map_err(|_| {
        anyhow::anyhow!("invalid BULK agent private key; expected a base58 keypair")
    })?;
    let address = wallet.pubkey().to_base58();
    let existing = load_bulk_credential()?;
    check_replacement(
        account,
        &address,
        existing.as_ref().and_then(|c| c.account.as_deref()),
        existing.as_ref().map(|c| c.agent_public_key.as_str()),
        args.replace,
    )?;
    let network = BulkNetwork::from_testnet(args.testnet);
    bulk::verify_agent(network, account, &address).await?;
    let mut credential = match existing {
        Some(mut credential) => {
            // BULK stores one key for both networks. A new key has no verified approval on the other network.
            if credential.agent_public_key != address {
                credential.mainnet_authorized = false;
                credential.testnet_authorized = false;
            }
            credential.agent_private_key.zeroize();
            credential.agent_private_key = wallet.to_base58();
            credential.agent_public_key = address.clone();
            credential
        }
        None => BulkCredential {
            version: BULK_CREDENTIAL_VERSION,
            status: BulkCredentialStatus::Pending,
            account: None,
            agent_public_key: address.clone(),
            agent_private_key: wallet.to_base58(),
            mainnet_authorized: false,
            testnet_authorized: false,
            mainnet_subaccounts: Vec::new(),
            testnet_subaccounts: Vec::new(),
            legacy_subaccounts: Vec::new(),
        },
    };
    credential.account = Some(account.to_string());
    credential.status = BulkCredentialStatus::Active;
    credential.set_authorized(network, true);
    save_bulk_credential(&credential)?;
    Ok(address)
}

async fn import_evm(args: &AuthSetArgs, account: &str, key: &str) -> Result<String> {
    let wallet = HyperliquidWallet::from_private_key(key.trim()).map_err(|_| {
        anyhow::anyhow!("invalid agent private key; expected a 32-byte hexadecimal key")
    })?;
    let address = wallet.address();
    if matches!(args.provider, AuthProvider::Hyperlink) {
        let existing = load_hyperlink_credential()?;
        check_replacement(
            account,
            &address,
            existing.as_ref().map(|c| c.account.as_str()),
            existing.as_ref().map(|c| c.agent.address.as_str()),
            args.replace,
        )?;
        let name = HyperliquidClient::with_base_url(crate::providers::hyperlink::HTTP_URL)?
            .verified_agent_name(account, &address)
            .await?;
        save_hyperlink_credential(&HyperlinkCredential {
            version: HYPERLINK_CREDENTIAL_VERSION,
            status: HyperlinkCredentialStatus::Active,
            account: account.to_string(),
            agent: HyperliquidAgentCredential::from_wallet(&name, &wallet),
        })?;
    } else {
        let network = HyperliquidNetwork::from_testnet(args.testnet);
        let existing = load_hyperliquid_credential()?;
        check_replacement(
            account,
            &address,
            existing.as_ref().map(|c| c.account.as_str()),
            existing
                .as_ref()
                .and_then(|c| c.agent(network))
                .map(|a| a.address.as_str()),
            args.replace,
        )?;
        let name = HyperliquidClient::for_network(network)?
            .verified_agent_name(account, &address)
            .await?;
        let mut credential = existing.unwrap_or_else(|| HyperliquidCredential {
            version: HYPERLIQUID_CREDENTIAL_VERSION,
            account: account.to_string(),
            mainnet_agent: None,
            testnet_agent: None,
            mainnet_subaccounts: Vec::new(),
            testnet_subaccounts: Vec::new(),
            builder: None,
        });
        let slot = match network {
            HyperliquidNetwork::Mainnet => &mut credential.mainnet_agent,
            HyperliquidNetwork::Testnet => &mut credential.testnet_agent,
        };
        *slot = Some(HyperliquidAgentCredential::from_wallet(&name, &wallet));
        save_hyperliquid_credential(&credential)?;
    }
    Ok(address)
}
