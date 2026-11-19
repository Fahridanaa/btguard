//! btguard: observe Bluetooth audio on PipeWire and record incidents.
//!
//! `status` and `incidents` read the live graph and the store. `watch` is the
//! observer that writes rows. This version does not enforce policy. See
//! README.md.

mod cli;
mod duration;
mod paths;
mod pw;
mod store;
mod watch;

use std::process::ExitCode;

use anyhow::Result;
use chrono::{Local, TimeZone};
use clap::Parser;

use cli::{Cli, Command};
use pw::{Node, Snapshot};
use store::Store;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("btguard: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Status => status(),
        Command::Watch => watch::watch(),
        Command::Incidents { since, limit } => incidents(since, limit),
    }
}

fn status() -> Result<()> {
    let snapshot = Snapshot::load()?;
    let summary = Store::open_default()?.summary()?;

    println!("btguard {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!(
        "PipeWire      {}",
        snapshot.core_version.as_deref().unwrap_or("unknown")
    );
    println!(
        "Default sink  {}",
        snapshot.default_sink.as_deref().unwrap_or("(none)")
    );
    match summary.last_at {
        Some(at) => println!(
            "Incidents     {} recorded, last {}",
            summary.total,
            local_time(at)
        ),
        None => println!("Incidents     {} recorded", summary.total),
    }

    println!();
    println!("Bluetooth");
    if snapshot.cards.is_empty() {
        println!("  no device connected");
        return Ok(());
    }

    for card in &snapshot.cards {
        println!("  {} ({})", card.description, card.name);
        if let Some(connection) = &card.connection
            && connection != "connected"
        {
            println!("    state    {connection}");
        }
        match (&card.profile, &card.profile_description) {
            (Some(profile), Some(description)) => {
                println!("    profile  {profile} — {description}")
            }
            (Some(profile), None) => println!("    profile  {profile}"),
            (None, _) => println!("    profile  unknown"),
        }
        print_node("sink", snapshot.sink_for(card), &snapshot.default_sink);
        print_node("source", snapshot.source_for(card), &snapshot.default_sink);
    }

    Ok(())
}

fn print_node(label: &str, node: Option<&Node>, default_sink: &Option<String>) {
    let Some(node) = node else {
        println!("    {label:<8} none");
        return;
    };

    let mut parts = vec![node.name.clone()];
    if default_sink.as_deref() == Some(node.name.as_str()) {
        parts.push("[default]".to_owned());
    }
    if let Some(codec) = &node.codec {
        parts.push(format!("codec {codec}"));
    }
    println!("    {label:<8} {}", parts.join("  "));
}

fn incidents(since: i64, limit: usize) -> Result<()> {
    let store = Store::open_default()?;
    let cutoff = Local::now().timestamp() - since;
    let rows = store.since(cutoff, limit)?;
    let span = duration::format_span(since);

    if rows.is_empty() {
        println!("No incidents since {} ({span}).", local_time(cutoff));
        return Ok(());
    }

    println!(
        "{} incident{} since {} ({span})",
        rows.len(),
        if rows.len() == 1 { "" } else { "s" },
        local_time(cutoff)
    );

    for row in &rows {
        println!();
        println!("{}  {}", local_time(row.at_unix), row.kind);
        if let Some(device) = &row.device {
            println!("    device   {device}");
        }
        match (&row.profile_before, &row.profile_after) {
            (Some(before), Some(after)) => println!("    profile  {before} -> {after}"),
            (Some(before), None) => println!("    profile  {before}"),
            (None, Some(after)) => println!("    profile  -> {after}"),
            (None, None) => {}
        }
        if let Some(codec) = &row.codec {
            println!("    codec    {codec}");
        }
        if let Some(detail) = &row.detail {
            println!("    detail   {detail}");
        }
        if let Some(journal) = &row.journal {
            for line in journal.lines() {
                println!("    | {line}");
            }
        }
    }

    Ok(())
}

fn local_time(unix: i64) -> String {
    Local
        .timestamp_opt(unix, 0)
        .single()
        .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| unix.to_string())
}
