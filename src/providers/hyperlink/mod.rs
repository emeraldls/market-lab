pub mod ws;

pub const HTTP_URL: &str = "https://api.hyperlink.xyz";
pub const WS_URL: &str = "wss://api.hyperlink.xyz/ws";
pub const EXCHANGE: &str = "hyperlinkf";
pub const SPOT_EXCHANGE: &str = "hyperlink";

pub async fn verified_agent_name(
    account: &str,
    wallet: &super::hyperliquid::signing::HyperliquidWallet,
) -> anyhow::Result<String> {
    use super::hyperliquid::exchange::HyperliquidExchangeClient;
    use anyhow::Context;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Agent {
        name: String,
        address: String,
        valid_until: u64,
    }

    // Private queries resolve the account from the signer, not a `user` parameter.
    ws::verify_account(account, wallet).await?;
    let agents: Vec<Agent> = serde_json::from_value(
        HyperliquidExchangeClient::for_hyperlink(wallet.clone())?
            .signed_read(serde_json::json!({ "type": "extraAgents" }))
            .await?,
    )
    .context("HyperLink extraAgents returned an unexpected payload")?;
    let now = u64::try_from(chrono::Utc::now().timestamp_millis())?;
    let address = wallet.address();
    let approved = agents
        .into_iter()
        .find(|entry| entry.address.eq_ignore_ascii_case(&address) && entry.valid_until > now)
        .context("HyperLink agent approval is missing or expired")?;
    Ok(approved.name)
}
