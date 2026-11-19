//! Where this tool keeps its state.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// `$XDG_STATE_HOME/bt-audio-guard`, or `~/.local/state/bt-audio-guard`.
pub fn app_state_dir() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(dir).join("bt-audio-guard"));
    }

    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".local/state/bt-audio-guard"))
}

pub fn db_path() -> Result<PathBuf> {
    Ok(app_state_dir()?.join("incidents.db"))
}

/// The last journal cursor the watcher processed. A restart resumes here.
pub fn journal_cursor_path() -> Result<PathBuf> {
    Ok(app_state_dir()?.join("journal.cursor"))
}

/// The single-instance lock for `btguard watch`.
pub fn watch_lock_path() -> Result<PathBuf> {
    Ok(app_state_dir()?.join("watch.lock"))
}
