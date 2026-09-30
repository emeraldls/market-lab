use anyhow::Result;

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref()
        == Some(std::ffi::OsStr::new(
            market_lab::scripting::sandbox::WORKER_COMMAND,
        ))
    {
        return market_lab::scripting::sandbox::worker(args);
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("serve") | None => market_lab::runtime::serve().await,
        Some("script-worker") => {
            let job_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("script-worker requires a job id"))?;
            if args.next().is_some() {
                anyhow::bail!("script-worker accepts exactly one job id");
            }
            market_lab::commands::script::run::handle_worker(&job_id).await
        }
        Some("strategy-worker") => {
            let job_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("strategy-worker requires a job id"))?;
            if args.next().is_some() {
                anyhow::bail!("strategy-worker accepts exactly one job id");
            }
            market_lab::commands::strategy::handle_worker(&job_id).await
        }
        Some("bot-worker") => {
            let job_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("bot-worker requires a job id"))?;
            if args.next().is_some() {
                anyhow::bail!("bot-worker accepts exactly one job id");
            }
            market_lab::commands::bot::handle_worker(&job_id).await
        }
        Some("healthcheck") => market_lab::runtime::healthcheck().await,
        Some(command) => {
            anyhow::bail!(
                "unknown mlabd command `{command}` (expected `serve|healthcheck|script-worker|strategy-worker|bot-worker`)"
            )
        }
    }
}
