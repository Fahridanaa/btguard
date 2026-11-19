//! Command line surface.

use clap::{Parser, Subcommand};

use crate::duration::parse_span;

#[derive(Debug, Parser)]
#[command(
    name = "btguard",
    version,
    about = "Observe Bluetooth audio incidents on PipeWire"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Show the live Bluetooth audio state and the recorded incident count
    Status,
    /// Watch PipeWire and the journal, and record incidents
    Watch,
    /// List recorded incidents, newest first
    Incidents {
        /// Time span to look back: 45s, 90m, 12h, 3d or 2w
        #[arg(long, default_value = "3d", value_parser = parse_span_arg)]
        since: i64,
        /// Maximum number of rows
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
}

fn parse_span_arg(text: &str) -> Result<i64, String> {
    parse_span(text).map_err(|error| format!("{error:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incidents_takes_a_span() {
        let cli = Cli::try_parse_from(["btguard", "incidents", "--since", "12h"]).unwrap();
        match cli.command {
            Command::Incidents { since, limit } => {
                assert_eq!(since, 43_200);
                assert_eq!(limit, 50);
            }
            Command::Status | Command::Watch => panic!("expected the incidents command"),
        }
    }

    #[test]
    fn incidents_defaults_to_three_days() {
        let cli = Cli::try_parse_from(["btguard", "incidents"]).unwrap();
        match cli.command {
            Command::Incidents { since, .. } => assert_eq!(since, 259_200),
            Command::Status | Command::Watch => panic!("expected the incidents command"),
        }
    }

    #[test]
    fn watch_takes_no_arguments() {
        let cli = Cli::try_parse_from(["btguard", "watch"]).unwrap();
        assert!(matches!(cli.command, Command::Watch));
        assert!(Cli::try_parse_from(["btguard", "watch", "--since", "3d"]).is_err());
    }

    #[test]
    fn a_span_without_a_unit_is_a_usage_error() {
        assert!(Cli::try_parse_from(["btguard", "incidents", "--since", "3"]).is_err());
    }
}
