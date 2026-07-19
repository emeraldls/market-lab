use anyhow::Result;
use clap::Parser;
use mimalloc::MiMalloc;

use market_lab::cli::{
    AuthCommands, Cli, Commands, DaemonCommands, ScriptCommands, ScriptRunHistoryCommands,
    SourceCommands, StrategyCommands, StrategyRunCommands, StudyCommands, TradeCommands,
};
use market_lab::commands;
use market_lab::config;
use market_lab::credentials;
use market_lab::domain::execution::PositionDirection;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    let args = config::expand_args(std::env::args_os())?;
    let cli = Cli::parse_from(args);

    match cli.command {
        Commands::Markets(args) => commands::markets::handle(args).await?,
        Commands::Trade { command } => match command {
            TradeCommands::Long(args) => {
                commands::execution::handle_trade(args, PositionDirection::Long).await?
            }
            TradeCommands::Short(args) => {
                commands::execution::handle_trade(args, PositionDirection::Short).await?
            }
        },
        Commands::Positions(args) => commands::execution::handle_positions(args).await?,
        Commands::Orders(args) => commands::execution::handle_orders(args).await?,
        Commands::Fills(args) => commands::execution::handle_fills(args).await?,
        Commands::Cancel(args) => commands::execution::handle_cancel(args).await?,
        Commands::Close(args) => commands::execution::handle_close(args).await?,
        Commands::Daemon { command } => match command {
            DaemonCommands::Start(args) => commands::runtime::handle_start(args).await?,
            DaemonCommands::Status(args) => commands::runtime::handle_status(args).await?,
            DaemonCommands::Stop(args) => commands::runtime::handle_stop(args).await?,
            DaemonCommands::Events(args) => commands::runtime::handle_events(args)?,
        },
        Commands::Inspect(args) => commands::market::inspect::handle(args).await?,
        Commands::Replay(args) => commands::market::replay::handle(args).await?,
        Commands::Source { command: source } => match source {
            SourceCommands::Orderbook(args) => commands::source::handle_orderbook(args).await?,
            SourceCommands::Vd(args) => commands::source::handle_vd(args).await?,
            SourceCommands::Candles(args) => commands::source::handle_candles(args).await?,
            SourceCommands::Oi(args) => commands::source::handle_oi(args).await?,
            SourceCommands::Volumes(args) => commands::source::handle_volumes(args).await?,
            SourceCommands::Stats(args) => commands::source::handle_stats(args).await?,
            SourceCommands::Funding(args) => commands::source::handle_funding(args).await?,
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
            ScriptCommands::Jobs(args) => commands::script::jobs::handle_list(args).await?,
            ScriptCommands::Status(args) => commands::script::jobs::handle_status(args).await?,
            ScriptCommands::Logs(args) => commands::script::jobs::handle_logs(args).await?,
            ScriptCommands::Stop(args) => commands::script::jobs::handle_stop(args).await?,
            ScriptCommands::Restart(args) => commands::script::jobs::handle_restart(args).await?,
            ScriptCommands::Runs { command } => match command {
                ScriptRunHistoryCommands::List(args) => commands::script::runs::handle_list(args)?,
                ScriptRunHistoryCommands::Show(args) => commands::script::runs::handle_show(args)?,
            },
        },
        Commands::Strategy { command: strategy } => match strategy {
            StrategyCommands::Run { command } => match command {
                StrategyRunCommands::Twap(args) => commands::strategy::twap::handle(args).await?,
                StrategyRunCommands::Vwap(args) => commands::strategy::vwap::handle(args).await?,
            },
            StrategyCommands::Jobs(args) => commands::strategy::jobs::handle_list(args).await?,
            StrategyCommands::Status(args) => commands::strategy::jobs::handle_status(args).await?,
            StrategyCommands::Logs(args) => commands::strategy::jobs::handle_logs(args).await?,
            StrategyCommands::Stop(args) => commands::strategy::jobs::handle_stop(args).await?,
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
