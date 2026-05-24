use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

mod cli;
mod commands;
mod core;
mod domain;
mod functions;
mod providers;

use cli::{Cli, Commands, SourceCommands, StudyCommands};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inspect(args) => commands::inspect::handle(args).await?,
        Commands::Replay(args) => commands::replay::handle(args).await?,
        Commands::Source { command: source } => match source {
            SourceCommands::Orderbook(args) => commands::source::handle_orderbook(args).await?,
            SourceCommands::Vd(args) => commands::source::handle_vd(args).await?,
        },
        Commands::Study { command: study } => match study {
            StudyCommands::Slippage(args) => commands::slippage::handle(args).await?,
            StudyCommands::Imbalance(args) => commands::imbalance::handle(args).await?,
            StudyCommands::Vamp(args) => commands::vamp::handle(args).await?,
            StudyCommands::Cvd(args) => commands::cvd::handle(args).await?,
        },
        Commands::Health(args) => commands::health::handle(args).await?,
    }

    Ok(())
}
