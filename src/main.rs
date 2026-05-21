use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

mod cli;
mod commands;
mod core;
mod domain;
mod functions;
mod providers;

use cli::{Cli, Commands};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inspect(args) => commands::inspect::handle(args).await?,
        Commands::Replay(args) => commands::replay::handle(args).await?,
        Commands::Slippage(args) => commands::slippage::handle(args).await?,
        Commands::Imbalance(args) => commands::imbalance::handle(args).await?,
        Commands::Vamp(args) => commands::vamp::handle(args).await?,
        Commands::Health(args) => commands::health::handle(args).await?,
    }

    Ok(())
}
