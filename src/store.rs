//! SQLite incident store.
//!
//! One table holds every incident. A row is a time, a kind, the profile and
//! codec around it, and the journal lines from the same second.

use std::path::Path;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};

use crate::paths;

/// Bump this when the schema changes, and add the migration below.
pub const SCHEMA_VERSION: i64 = 1;

const SCHEMA_V1: &str = r#"
CREATE TABLE incidents (
    id             INTEGER PRIMARY KEY,
    at_unix        INTEGER NOT NULL,
    kind           TEXT    NOT NULL,
    device         TEXT,
    profile_before TEXT,
    profile_after  TEXT,
    codec          TEXT,
    detail         TEXT,
    journal        TEXT
);
CREATE INDEX incidents_at_unix ON incidents(at_unix);
"#;

/// The incident kinds this tool records. Keep the list in sync with the README.
///
/// The watcher, which is not written yet, is the only producer.
#[allow(dead_code)]
pub mod kind {
    pub const TRANSPORT_FAILURE: &str = "transport-failure";
    pub const SCO_ORPHAN: &str = "sco-orphan";
    pub const PROFILE_CHANGE: &str = "profile-change";
    pub const CODEC_CHANGE: &str = "codec-change";
    pub const DISCONNECT: &str = "disconnect";
    pub const BALANCE_DRIFT: &str = "balance-drift";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incident {
    /// Seconds since the Unix epoch, UTC.
    pub at_unix: i64,
    pub kind: String,
    pub device: Option<String>,
    pub profile_before: Option<String>,
    pub profile_after: Option<String>,
    pub codec: Option<String>,
    pub detail: Option<String>,
    pub journal: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub total: i64,
    pub last_at: Option<i64>,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open_default() -> Result<Self> {
        Self::open(&paths::db_path()?)
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }

        let conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;

        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            bail!(
                "{} uses schema version {version}, but this build understands only {SCHEMA_VERSION}",
                path.display()
            );
        }
        if version < 1 {
            conn.execute_batch(SCHEMA_V1)?;
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }

        Ok(Self { conn })
    }

    /// Record one incident. The watcher calls this; today only the tests do.
    #[allow(dead_code)]
    pub fn insert(&self, incident: &Incident) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO incidents
                 (at_unix, kind, device, profile_before, profile_after, codec, detail, journal)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                incident.at_unix,
                incident.kind,
                incident.device,
                incident.profile_before,
                incident.profile_after,
                incident.codec,
                incident.detail,
                incident.journal,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn summary(&self) -> Result<Summary> {
        let (total, last_at) =
            self.conn
                .query_row("SELECT COUNT(*), MAX(at_unix) FROM incidents", [], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
                })?;
        Ok(Summary { total, last_at })
    }

    /// Incidents at or after `cutoff`, newest first.
    pub fn since(&self, cutoff: i64, limit: usize) -> Result<Vec<Incident>> {
        let mut statement = self.conn.prepare(
            "SELECT at_unix, kind, device, profile_before, profile_after, codec, detail, journal
             FROM incidents
             WHERE at_unix >= ?1
             ORDER BY at_unix DESC, id DESC
             LIMIT ?2",
        )?;

        let rows = statement.query_map(params![cutoff, limit as i64], |row| {
            Ok(Incident {
                at_unix: row.get(0)?,
                kind: row.get(1)?,
                device: row.get(2)?,
                profile_before: row.get(3)?,
                profile_after: row.get(4)?,
                codec: row.get(5)?,
                detail: row.get(6)?,
                journal: row.get(7)?,
            })
        })?;

        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(at_unix: i64, kind: &str) -> Incident {
        Incident {
            at_unix,
            kind: kind.to_owned(),
            device: Some("bluez_card.1C_0A_E0_A3_E2_D0".to_owned()),
            profile_before: Some("a2dp-sink-sbc".to_owned()),
            profile_after: Some("headset-head-unit".to_owned()),
            codec: Some("sbc".to_owned()),
            detail: None,
            journal: Some(
                "spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0"
                    .to_owned(),
            ),
        }
    }

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(&dir.path().join("incidents.db")).expect("open the store");
        (dir, store)
    }

    #[test]
    fn a_new_store_is_empty() {
        let (_dir, store) = store();
        assert_eq!(
            store.summary().unwrap(),
            Summary {
                total: 0,
                last_at: None
            }
        );
        assert!(store.since(0, 10).unwrap().is_empty());
    }

    #[test]
    fn inserts_survive_a_reopen() {
        let (dir, store) = store();
        let id = store
            .insert(&sample(1_700_000_000, kind::TRANSPORT_FAILURE))
            .unwrap();
        assert!(id > 0);
        drop(store);

        let reopened = Store::open(&dir.path().join("incidents.db")).unwrap();
        assert_eq!(
            reopened.summary().unwrap(),
            Summary {
                total: 1,
                last_at: Some(1_700_000_000)
            }
        );
        let rows = reopened.since(0, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], sample(1_700_000_000, kind::TRANSPORT_FAILURE));
    }

    #[test]
    fn since_filters_and_returns_newest_first() {
        let (_dir, store) = store();
        store
            .insert(&sample(1_700_000_000, kind::TRANSPORT_FAILURE))
            .unwrap();
        store
            .insert(&sample(1_700_000_100, kind::SCO_ORPHAN))
            .unwrap();
        store
            .insert(&sample(1_700_000_200, kind::PROFILE_CHANGE))
            .unwrap();

        let rows = store.since(1_700_000_050, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].kind, kind::PROFILE_CHANGE);
        assert_eq!(rows[1].kind, kind::SCO_ORPHAN);

        let limited = store.since(1_700_000_050, 1).unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].kind, kind::PROFILE_CHANGE);
    }

    #[test]
    fn a_cutoff_includes_the_boundary() {
        let (_dir, store) = store();
        store
            .insert(&sample(1_700_000_000, kind::DISCONNECT))
            .unwrap();
        assert_eq!(store.since(1_700_000_000, 10).unwrap().len(), 1);
        assert!(store.since(1_700_000_001, 10).unwrap().is_empty());
    }
}
