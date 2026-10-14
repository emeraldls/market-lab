use clap::{Args, Subcommand};

#[derive(Debug, Subcommand)]
pub enum AlertCommands {
    /// Privately enter a Telegram bot token, send a test and confirm receipt.
    Setup(AlertSetupArgs),
    /// Send a test using the saved Telegram configuration.
    Test,
    /// Show configuration without exposing the token.
    Status,
    /// Remove the saved Telegram configuration.
    Remove,
}

#[derive(Debug, Args)]
pub struct AlertSetupArgs {
    /// Optional numeric chat ID for a group or existing setup. Omit to connect your DM automatically.
    #[arg(long, allow_hyphen_values = true)]
    pub chat_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use crate::cli::{Cli, Commands};
    use clap::Parser;

    #[test]
    fn alert_setup_accepts_group_chat_and_never_accepts_token_flag() {
        assert!(matches!(
            Cli::try_parse_from(["mlab", "alert", "setup"])
                .unwrap()
                .command,
            Commands::Alert {
                command: super::AlertCommands::Setup(super::AlertSetupArgs { chat_id: None })
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["mlab", "alert", "setup", "--chat-id", "-100123"])
                .unwrap()
                .command,
            Commands::Alert { .. }
        ));
        assert!(
            Cli::try_parse_from([
                "mlab",
                "alert",
                "setup",
                "--chat-id",
                "123",
                "--token",
                "secret"
            ])
            .is_err()
        );
    }
}
