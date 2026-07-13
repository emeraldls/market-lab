use std::sync::OnceLock;

use anyhow::{Context, Result, bail};

use crate::cli::{AuthProvider, AuthProviderArgs};

const MMT_API_KEY_ENV: &str = "MMT_API_KEY";
const KEYRING_SERVICE: &str = "market-lab";
const MMT_KEYRING_ACCOUNT: &str = "mmt-api-key";

static MMT_API_KEY: OnceLock<String> = OnceLock::new();

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

pub fn handle_set(args: AuthProviderArgs) -> Result<()> {
    match args.provider {
        AuthProvider::Mmt => {
            let key = rpassword::prompt_password("MMT API key: ")?;
            let key = validate_key(key, "MMT API key")?;
            mmt_entry()?
                .set_password(&key)
                .context("failed to store MMT API key in the OS keychain")?;
            println!("mmt: configured");
        }
    }
    Ok(())
}

pub fn handle_status() -> Result<()> {
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

pub fn handle_remove(args: AuthProviderArgs) -> Result<()> {
    match args.provider {
        AuthProvider::Mmt => match mmt_entry()?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => println!("mmt: removed"),
            Err(err) => return Err(err).context("failed to remove MMT API key from OS keychain"),
        },
    }
    Ok(())
}

fn mmt_entry() -> Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, MMT_KEYRING_ACCOUNT)
        .context("failed to access the OS keychain")
}

fn validate_key(key: String, name: &str) -> Result<String> {
    let key = key.trim().to_string();
    if key.is_empty() {
        bail!("{name} cannot be empty");
    }
    Ok(key)
}
