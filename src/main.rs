use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

mod cli;
mod commands;
mod config;
mod core;
mod credentials;
mod domain;
mod functions;
mod providers;
mod scripting;

use cli::{
    AuthCommands, Cli, Commands, ScriptCommands, ScriptRunHistoryCommands, SourceCommands,
    StrategyBacktestCommands, StrategyCommands, StrategyRunCommands, StudyCommands,
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = config::expand_args(std::env::args_os())?;
    let cli = Cli::parse_from(args);

    match cli.command {
        Commands::Inspect(args) => commands::market::inspect::handle(args).await?,
        Commands::Replay(args) => commands::market::replay::handle(args).await?,
        Commands::Source { command: source } => match source {
            SourceCommands::Orderbook(args) => commands::source::handle_orderbook(args).await?,
            SourceCommands::Vd(args) => commands::source::handle_vd(args).await?,
            SourceCommands::Candles(args) => commands::source::handle_candles(args).await?,
            SourceCommands::Oi(args) => commands::source::handle_oi(args).await?,
            SourceCommands::Volumes(args) => commands::source::handle_volumes(args).await?,
        },
        Commands::Study { command: study } => match study {
            StudyCommands::Slippage(args) => commands::study::slippage::handle(args).await?,
            StudyCommands::Imbalance(args) => commands::study::imbalance::handle(args).await?,
            StudyCommands::Spread(args) => commands::study::spread::handle(args).await?,
            StudyCommands::Depth(args) => commands::study::depth::handle(args).await?,
            StudyCommands::Vamp(args) => commands::study::vamp::handle(args).await?,
            StudyCommands::Cvd(args) => commands::study::cvd::handle(args).await?,
        },
        Commands::Script { command: script } => match script {
            ScriptCommands::Run(args) => commands::script::run::handle(args).await?,
            ScriptCommands::Backtest(args) => commands::script::backtest::handle(args).await?,
            ScriptCommands::Runs { command } => match command {
                ScriptRunHistoryCommands::List(args) => commands::script::runs::handle_list(args)?,
                ScriptRunHistoryCommands::Show(args) => commands::script::runs::handle_show(args)?,
            },
        },
        Commands::Strategy { command: strategy } => match strategy {
            StrategyCommands::Run { command } => match command {
                StrategyRunCommands::SmaCrossover(args) => {
                    commands::strategy::sma_crossover::handle_run(args).await?
                }
            },
            StrategyCommands::Backtest { command } => match command {
                StrategyBacktestCommands::SmaCrossover(args) => {
                    commands::strategy::sma_crossover::handle_backtest(args).await?
                }
            },
        },
        Commands::Health(args) => commands::system::health::handle(args).await?,
        Commands::Status(args) => commands::system::status::handle(args).await?,
        Commands::Upgrade(args) => commands::system::upgrade::handle(args).await?,
        Commands::Auth { command } => match command {
            AuthCommands::Set(args) => credentials::handle_set(args).await?,
            AuthCommands::Status => credentials::handle_status()?,
            AuthCommands::Remove(args) => credentials::handle_remove(args).await?,
        },
    }

    Ok(())
}
