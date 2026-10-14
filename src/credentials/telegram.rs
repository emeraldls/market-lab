use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

const FILE: &str = "telegram.json";

// Deliberately no Debug: request URLs and credentials contain the bot token.
#[derive(Deserialize, Serialize)]
pub struct TelegramCredential {
    pub token: String,
    pub chat_id: i64,
}

impl Drop for TelegramCredential {
    fn drop(&mut self) {
        self.token.zeroize();
    }
}

impl TelegramCredential {
    pub fn validate(&self) -> Result<()> {
        validate_token(&self.token)?;
        ensure!(self.chat_id != 0, "Telegram chat ID cannot be zero");
        Ok(())
    }
}

pub(crate) fn validate_token(token: &str) -> Result<()> {
    let (id, secret) = token.split_once(':').unwrap_or(("", ""));
    ensure!(
        !id.is_empty()
            && id.bytes().all(|b| b.is_ascii_digit())
            && (20..=128).contains(&secret.len())
            && secret
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')),
        "invalid Telegram bot token"
    );
    Ok(())
}

pub fn load() -> Result<Option<TelegramCredential>> {
    load_at(&directory()?)
}

pub(crate) fn directory() -> Result<std::path::PathBuf> {
    super::credential_directory()
}

pub(crate) fn load_at(directory: &std::path::Path) -> Result<Option<TelegramCredential>> {
    let Some(encoded) = super::read_credential_at(directory, FILE, "Telegram")? else {
        return Ok(None);
    };
    let credential: TelegramCredential = serde_json::from_str(&encoded).map_err(|_| {
        anyhow::anyhow!("stored Telegram configuration is invalid; run `mlab alert setup`")
    })?;
    credential.validate()?;
    Ok(Some(credential))
}

pub fn save(credential: &TelegramCredential) -> Result<()> {
    credential.validate()?;
    let encoded = Zeroizing::new(serde_json::to_string(credential)?);
    super::save_credential_file(FILE, encoded.as_bytes(), "Telegram")
}

pub fn remove() -> Result<()> {
    super::delete_credential_file(FILE, "Telegram")
}
