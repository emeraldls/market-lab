use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result, ensure};
use zeroize::Zeroizing;

use super::*;

const FILE: &str = "pool-operator.key";

pub fn create() -> Result<Address> {
    let directory = credential_directory()?;
    ensure_credential_directory(&directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory.join("pool-operator.lock"))?;
    lock.lock()
        .context("failed to lock pool operator credentials")?;
    ensure!(
        load_credential_file(FILE, "pool operator")?.is_none(),
        "pool operator already exists; use `mlab pool operator address`"
    );
    let signer = PrivateKeySigner::random();
    let bytes = Zeroizing::new(signer.to_bytes().0);
    let encoded = Zeroizing::new(hex::encode(bytes.as_slice()));
    save_credential_file(FILE, encoded.as_bytes(), "pool operator")?;
    Ok(signer.address())
}

pub fn load() -> Result<PrivateKeySigner> {
    let source = load_credential_file(FILE, "pool operator")?
        .context("no pool operator configured; run `mlab pool operator create`")?;
    source.parse().context("invalid stored pool operator key")
}

pub fn address() -> Result<Address> {
    Ok(load()?.address())
}
