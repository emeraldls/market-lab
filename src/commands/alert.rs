use anyhow::{Context, Result, ensure};
use dialoguer::Confirm;
use rand_core::{OsRng, RngCore};
use std::io::IsTerminal;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zeroize::Zeroizing;

use crate::alerts::TelegramClient;
use crate::cli::AlertCommands;
use crate::credentials::telegram::{self, TelegramCredential};
mod connection;

const TEST_MESSAGE: &str = "Market Lab: Telegram alerts are connected. Did you receive this message? Confirm in your terminal.";

pub async fn handle(command: AlertCommands) -> Result<()> {
    match command {
        AlertCommands::Connection { command } => connection::handle(command).await?,
        AlertCommands::Setup(args) => {
            ensure!(
                std::io::stdin().is_terminal() && !crate::remote::is_transport_child(),
                "Telegram setup requires a local interactive terminal. On the target host, run `mlab --remote local alert setup`"
            );
            ensure!(args.chat_id != Some(0), "Telegram chat ID cannot be zero");
            let token =
                Zeroizing::new(rpassword::prompt_password("Telegram bot token (hidden): ")?);
            let client = TelegramClient::new()?;
            let username = client.bot_username(token.trim()).await?;
            let chat_id = match args.chat_id {
                Some(chat_id) => chat_id,
                None => {
                    let mut nonce = [0_u8; 16];
                    OsRng
                        .try_fill_bytes(&mut nonce)
                        .context("could not generate a secure Telegram pairing code")?;
                    let code = format!("mlab_{}", hex::encode(nonce));
                    let since = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                    println!("Open in Telegram: tg://resolve?domain={username}&start={code}");
                    println!("Or send this code to @{username}: {code}");
                    println!("Waiting for your DM (5 minutes). Press Ctrl+C to cancel.");
                    tokio::select! {
                        result = client.wait_for_private_chat(token.trim(), &code, since, Duration::from_secs(300)) => result?,
                        _ = tokio::signal::ctrl_c() => anyhow::bail!("Telegram setup cancelled; existing credentials are unchanged"),
                    }
                }
            };
            let credential = TelegramCredential {
                token: token.trim().into(),
                chat_id,
            };
            credential.validate()?;
            client.send(&credential, TEST_MESSAGE).await?;
            ensure!(
                Confirm::new()
                    .with_prompt("Did you receive the Telegram test message?")
                    .default(false)
                    .interact()?,
                "Telegram configuration was not saved; existing credentials are unchanged"
            );
            telegram::save(&credential)?;
            println!(
                "Telegram alerts connected. Token saved privately in credentials/telegram.json."
            );
        }
        AlertCommands::Test => {
            let credential =
                telegram::load()?.context("Telegram is not configured; run `mlab alert setup`")?;
            let id = TelegramClient::new()?
                .send(&credential, TEST_MESSAGE)
                .await?;
            println!("Telegram test sent (message {id}).");
        }
        AlertCommands::Status => match telegram::load()? {
            Some(credential) => println!("Telegram connected · chat {}", credential.chat_id),
            None => println!("Telegram not configured."),
        },
        AlertCommands::Remove => {
            telegram::remove()?;
            println!("Telegram configuration removed.");
        }
    }
    Ok(())
}
