use super::*;
use crate::cli::AuthBuilderCommands;
use crate::providers::hyperliquid::{
    exchange::submit_builder_approval, signing::recover_builder_approval,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Approval {
    account: String,
    builder: String,
    nonce: u64,
    signature: String,
}

pub async fn handle(command: AuthBuilderCommands) -> Result<()> {
    let mut credential = load_hyperliquid_credential()?;
    let status = crate::runtime::status().await?;
    let active_work = crate::runtime::runtime_has_active_work(&status);
    if !matches!(command, AuthBuilderCommands::Status) {
        if active_work {
            bail!("stop active jobs and clear tracked orders before changing the builder");
        }
        let credential = credential
            .as_mut()
            .context("connect a Hyperliquid mainnet account before joining the program")?;
        credential
            .mainnet_agent
            .as_ref()
            .context("connect a Hyperliquid mainnet account before joining the program")?;
        match command {
            AuthBuilderCommands::Approve => {
                let mut input = String::new();
                std::io::stdin()
                    .lock()
                    .take(4097)
                    .read_to_string(&mut input)?;
                if input.len() > 4096 {
                    bail!("builder approval exceeds 4096 bytes");
                }
                let approval: Approval =
                    serde_json::from_str(&input).context("invalid signed builder approval")?;
                let builder = canonical_address(&approval.builder)?;
                let (signer, signature) =
                    recover_builder_approval(&builder, approval.nonce, &approval.signature)?;
                if signer != credential.account
                    || canonical_address(&approval.account)? != credential.account
                {
                    bail!("builder approval must be signed by the connected main account");
                }
                let response = submit_builder_approval(
                    HyperliquidNetwork::Mainnet,
                    &builder,
                    "0%",
                    approval.nonce,
                    signature,
                    42_161,
                )
                .await?;
                ensure_hyperliquid_exchange_ok(&response, "mainnet builder approval")?;
                credential.builder = Some(builder);
            }
            AuthBuilderCommands::Clear => credential.builder = None,
            AuthBuilderCommands::Status => unreachable!(),
        }
        save_hyperliquid_credential(credential)?;
    }
    println!(
        "{}",
        serde_json::json!({
            "account": credential.as_ref().map(|c| &c.account),
            "configured": credential.as_ref().is_some_and(|c| c.mainnet_agent.is_some()),
            "builder": credential.as_ref().and_then(|c| c.builder.as_ref()),
            "fee_rate": "0%",
            "active_work": active_work,
        })
    );
    Ok(())
}
