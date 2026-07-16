use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("serve") | None => market_lab::runtime::serve().await,
        Some(command) => anyhow::bail!("unknown mlabd command `{command}` (expected `serve`)"),
    }
}
