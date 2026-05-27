use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

mod cli;
mod commands;
mod core;
mod domain;
mod providers;

use cli::{Cli, Commands, SourceCommands, StrategyCommands, StudyCommands};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inspect(args) => commands::market::inspect::handle(args).await?,
        Commands::Replay(args) => commands::market::replay::handle(args).await?,
        Commands::Source { command: source } => match source {
            SourceCommands::Orderbook(args) => commands::source::handle_orderbook(args).await?,
            SourceCommands::Vd(args) => commands::source::handle_vd(args).await?,
            SourceCommands::Candles(args) => commands::source::handle_candles(args).await?,
        },
        Commands::Study { command: study } => match study {
            StudyCommands::Slippage(args) => commands::study::slippage::handle(args).await?,
            StudyCommands::Imbalance(args) => commands::study::imbalance::handle(args).await?,
            StudyCommands::Spread(args) => commands::study::spread::handle(args).await?,
            StudyCommands::Depth(args) => commands::study::depth::handle(args).await?,
            StudyCommands::Vamp(args) => commands::study::vamp::handle(args).await?,
            StudyCommands::Cvd(args) => commands::study::cvd::handle(args).await?,
        },
        Commands::Strategy { command: strategy } => match strategy {
            StrategyCommands::SmaCrossover(args) => {
                commands::strategy::sma_crossover::handle(args).await?
            }
        },
        Commands::Health(args) => commands::system::health::handle(args).await?,
    }

    Ok(())
}
