//! A funded trading wallet, separate from the restricted fee operator.
use super::*;
use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;

const FILE: &str = "elysium-trading.key";

pub fn setup() -> Result<Address> {
    setup_at(&credential_directory()?)
}

fn setup_at(directory: &Path) -> Result<Address> {
    ensure_credential_directory(directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory.join("elysium-trading.lock"))?;
    lock.lock()
        .context("failed to lock Elysium trading credentials")?;
    if let Some(signer) = load_at(directory)? {
        return Ok(signer.address());
    }
    let signer = PrivateKeySigner::random();
    let bytes = Zeroizing::new(signer.to_bytes().0);
    let encoded = Zeroizing::new(hex::encode(bytes.as_slice()));
    write_credential_at(
        directory,
        FILE,
        encoded.as_bytes(),
        "Elysium trading wallet",
    )?;
    Ok(signer.address())
}

fn load_optional() -> Result<Option<PrivateKeySigner>> {
    load_at(&credential_directory()?)
}

fn load_at(directory: &Path) -> Result<Option<PrivateKeySigner>> {
    read_credential_at(directory, FILE, "Elysium trading wallet")?
        .map(|source| source.parse().context("invalid stored Elysium trading key"))
        .transpose()
}

pub fn load() -> Result<PrivateKeySigner> {
    load_optional()?.context("no Elysium trading wallet configured; run `mlab auth set elysium`")
}

pub fn address() -> Result<Option<Address>> {
    Ok(load_optional()?.map(|signer| signer.address()))
}

pub fn account(name: &str) -> Result<String> {
    anyhow::ensure!(
        name.eq_ignore_ascii_case("main"),
        "Elysium supports account `main` only"
    );
    Ok(load()?.address().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn setup_reuses_the_funded_wallet_and_keeps_the_key_private() {
        let dir = super::super::tests::test_credential_directory("elysium");
        let first = setup_at(&dir).unwrap();
        assert_eq!(setup_at(&dir).unwrap(), first);
        assert_eq!(load_at(&dir).unwrap().unwrap().address(), first);
        assert_eq!(fs::metadata(dir.join(FILE)).unwrap().mode() & 0o777, 0o600);
        assert!(!dir.join("pool-operator.key").exists());
        fs::remove_dir_all(dir).unwrap();
    }
}
