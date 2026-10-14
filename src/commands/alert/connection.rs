use anyhow::{Context, Result, ensure};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};
use zeroize::{Zeroize, Zeroizing};

use crate::alerts::TelegramClient;
use crate::cli::AlertConnectionCommands;
use crate::credentials::{
    self,
    telegram::{self, TelegramCredential},
};

const FILE: &str = "telegram-pairing.json";
const WEB_TEST_MESSAGE: &str = "Furge alerts are connected. Confirm this test message in Accounts.";

#[derive(Serialize, Deserialize)]
struct Pairing {
    id: String,
    token: String,
    code: String,
    username: String,
    created: u64,
    expires: u64,
    offset: i64,
    chat_id: Option<i64>,
    status: String,
}

impl Drop for Pairing {
    fn drop(&mut self) {
        self.token.zeroize();
    }
}

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn save(pairing: &Pairing) -> Result<()> {
    let data = Zeroizing::new(serde_json::to_vec(pairing)?);
    credentials::write_credential_at(&telegram::directory()?, FILE, &data, "Telegram pairing")
}

fn load(id: &str) -> Result<Pairing> {
    ensure!(
        id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
        "Invalid Telegram connection"
    );
    let data = credentials::read_credential_at(&telegram::directory()?, FILE, "Telegram pairing")?
        .context("Telegram connection expired. Connect again")?;
    let pairing: Pairing = serde_json::from_str(&data)
        .map_err(|_| anyhow::anyhow!("Telegram connection is invalid. Connect again"))?;
    if pairing.id == id && pairing.expires <= now()? {
        remove_pending()?;
    }
    ensure!(
        pairing.id == id && pairing.expires > now()?,
        "Telegram connection expired. Connect again"
    );
    Ok(pairing)
}

fn remove_pending() -> Result<()> {
    credentials::delete_credential_at(&telegram::directory()?, FILE, "Telegram pairing")
}

pub(super) async fn handle(command: AlertConnectionCommands) -> Result<()> {
    let client = TelegramClient::new()?;
    let result = match command {
        AlertConnectionCommands::Begin => {
            let mut token = Zeroizing::new(String::new());
            std::io::stdin().take(513).read_to_string(&mut token)?;
            ensure!(token.len() <= 512, "Telegram token is too long");
            let username = client.bot_username(token.trim()).await?;
            client.prepare_pairing(token.trim()).await?;
            let mut nonce = [0_u8; 16];
            OsRng.try_fill_bytes(&mut nonce)?;
            let id = hex::encode(nonce);
            let created = now()?;
            let pairing = Pairing {
                id: id.clone(),
                token: token.trim().into(),
                code: format!("mlab_{id}"),
                username,
                created,
                expires: created + 300,
                offset: 0,
                chat_id: None,
                status: "awaiting_code".into(),
            };
            save(&pairing)?;
            json!({"id":pairing.id,"status":pairing.status,"code":pairing.code,"username":pairing.username,"expires_at":pairing.expires,"url":format!("tg://resolve?domain={}&start={}",pairing.username,pairing.code)})
        }
        AlertConnectionCommands::Check { id } => {
            let mut pairing = load(&id)?;
            if pairing.status == "awaiting_code" {
                pairing.chat_id = client
                    .pairing_update(
                        &pairing.token,
                        &pairing.code,
                        pairing.created,
                        &mut pairing.offset,
                    )
                    .await?;
                if let Some(chat_id) = pairing.chat_id {
                    // An uncertain test send must never be repeated by polling.
                    pairing.status = "sending_test".into();
                    save(&pairing)?;
                    let credential = TelegramCredential {
                        token: pairing.token.clone(),
                        chat_id,
                    };
                    match client.send(&credential, WEB_TEST_MESSAGE).await {
                        Ok(_) => pairing.status = "awaiting_confirmation".into(),
                        Err(error) => {
                            pairing.status = "failed".into();
                            save(&pairing)?;
                            return Err(error);
                        }
                    }
                }
                save(&pairing)?;
            }
            json!({"id":pairing.id,"status":pairing.status,"expires_at":pairing.expires})
        }
        AlertConnectionCommands::Confirm { id } => {
            let pairing = load(&id)?;
            ensure!(
                pairing.status == "awaiting_confirmation",
                "Confirm the Telegram test message after it arrives"
            );
            telegram::save(&TelegramCredential {
                token: pairing.token.clone(),
                chat_id: pairing.chat_id.context("Telegram chat is not connected")?,
            })?;
            remove_pending()?;
            json!({"connected":true})
        }
        AlertConnectionCommands::Cancel { id } => {
            let _ = load(&id)?;
            remove_pending()?;
            json!({"cancelled":true})
        }
        AlertConnectionCommands::Status => {
            if let Some(data) =
                credentials::read_credential_at(&telegram::directory()?, FILE, "Telegram pairing")?
            {
                let pairing: Pairing = serde_json::from_str(&data).map_err(|_| {
                    anyhow::anyhow!("Telegram connection is invalid. Connect again")
                })?;
                if pairing.expires <= now()? {
                    remove_pending()?;
                }
            }
            json!({"connected":telegram::load()?.is_some()})
        }
        AlertConnectionCommands::Remove => {
            telegram::remove()?;
            remove_pending()?;
            json!({"connected":false})
        }
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
