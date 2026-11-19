//! SQLite incident store.
//!
//! One table holds every incident. A row is a time, a kind, the profile and
//! codec around it, and the journal lines from the same second.

use std::path::Path;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, ErrorCode, params};

use crate::paths;

/// Bump this when the schema changes, and add the migration below.
pub const SCHEMA_VERSION: i64 = 1;

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS incidents (
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
CREATE INDEX IF NOT EXISTS incidents_at_unix ON incidents(at_unix);
"#;

/// The incident kinds this tool records. Keep the list in sync with the README.
pub mod kind {
    pub const TRANSPORT_FAILURE: &str = "transport-failure";
    pub const SCO_ORPHAN: &str = "sco-orphan";
    pub const PROFILE_CHANGE: &str = "profile-change";
    pub const CODEC_CHANGE: &str = "codec-change";
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

/// `PRAGMA journal_mode=WAL` does not use SQLite's busy handler. Two first
/// opens can collide, so retry while another connection changes the mode.
fn enable_wal(conn: &Connection) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match conn.execute_batch("PRAGMA journal_mode=WAL;") {
            Ok(()) => return Ok(()),
            Err(error) => {
                let busy = matches!(
                    error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                );
                if busy && std::time::Instant::now() < deadline {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    continue;
                }
                return Err(error).context("enable WAL mode");
            }
        }
    }
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

        let mut conn =
            Connection::open(path).with_context(|| format!("open {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        enable_wal(&conn)?;

        let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version > SCHEMA_VERSION {
            bail!(
                "{} uses schema version {version}, but this build understands only {SCHEMA_VERSION}",
                path.display()
            );
        }
        if version < 1 {
            let transaction = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .context("start the schema transaction")?;
            transaction
                .execute_batch(SCHEMA_V1)
                .context("create the incident schema")?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction
                .commit()
                .context("commit the schema transaction")?;
        }

        Ok(Self { conn })
    }

    /// Record one incident. The watcher is the only producer.
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

    /// Add one journal line to an incident that is already recorded. The
    /// watcher uses this to attach lines that arrive after the event.
    pub fn append_journal(&self, id: i64, line: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE incidents
             SET journal = CASE
                 WHEN journal IS NULL OR journal = '' THEN ?2
                 ELSE journal || char(10) || ?2
             END
             WHERE id = ?1",
            params![id, line],
        )?;
        Ok(())
    }

    /// Replace the codec of a row when the codec update arrives after the
    /// profile change.
    pub fn update_codec(&self, id: i64, codec: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE incidents SET codec = ?2 WHERE id = ?1",
            params![id, codec],
        )?;
        Ok(())
    }

    /// Turn a codec row into the profile row for the same episode.
    pub fn promote_to_profile_change(
        &self,
        id: i64,
        profile_before: Option<&str>,
        profile_after: Option<&str>,
        codec: Option<&str>,
        journal: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE incidents
             SET kind = ?2,
                 profile_before = ?3,
                 profile_after = ?4,
                 codec = ?5,
                 detail = NULL,
                 journal = COALESCE(?6, journal)
             WHERE id = ?1",
            params![
                id,
                kind::PROFILE_CHANGE,
                profile_before,
                profile_after,
                codec,
                journal
            ],
        )?;
        Ok(())
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
            .insert(&sample(1_700_000_000, kind::BALANCE_DRIFT))
            .unwrap();
        assert_eq!(store.since(1_700_000_000, 10).unwrap().len(), 1);
        assert!(store.since(1_700_000_001, 10).unwrap().is_empty());
    }

    #[test]
    fn a_codec_update_replaces_only_the_codec() {
        let (_dir, store) = store();
        let mut incident = sample(1_700_000_000, kind::CODEC_CHANGE);
        incident.codec = Some("sbc".to_owned());
        incident.detail = Some("codec sbc -> aac".to_owned());
        let id = store.insert(&incident).unwrap();

        store.update_codec(id, "aac").unwrap();

        let rows = store.since(0, 10).unwrap();
        assert_eq!(rows[0].codec.as_deref(), Some("aac"));
        assert_eq!(rows[0].kind, kind::CODEC_CHANGE);
        assert_eq!(rows[0].detail.as_deref(), Some("codec sbc -> aac"));
    }

    #[test]
    fn a_codec_row_becomes_a_profile_row() {
        let (_dir, store) = store();
        let id = store
            .insert(&sample(1_700_000_000, kind::CODEC_CHANGE))
            .unwrap();

        store
            .promote_to_profile_change(
                id,
                Some("a2dp-sink-sbc"),
                Some("a2dp-sink-aac"),
                Some("aac"),
                Some("context"),
            )
            .unwrap();

        let rows = store.since(0, 10).unwrap();
        assert_eq!(rows[0].kind, kind::PROFILE_CHANGE);
        assert_eq!(rows[0].profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(rows[0].profile_after.as_deref(), Some("a2dp-sink-aac"));
        assert_eq!(rows[0].codec.as_deref(), Some("aac"));
        assert_eq!(rows[0].journal.as_deref(), Some("context"));
    }

    #[test]
    fn a_partial_schema_is_repaired() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("incidents.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(SCHEMA_V1).unwrap();
        drop(conn);

        let store = Store::open(&path).expect("repair the partial schema");
        assert_eq!(store.summary().unwrap().total, 0);

        let conn = Connection::open(&path).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn two_first_opens_succeed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = std::sync::Arc::new(dir.path().join("incidents.db"));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

        let handles: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    Store::open(&path).map(|_| ())
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("join").expect("open the store");
        }
    }

    #[test]
    fn journal_lines_append_to_an_existing_row() {
        let (_dir, store) = store();
        let mut incident = sample(1_700_000_000, kind::PROFILE_CHANGE);
        incident.journal = None;
        let id = store.insert(&incident).unwrap();

        store.append_journal(id, "first line").unwrap();
        store.append_journal(id, "second line").unwrap();

        let rows = store.since(0, 10).unwrap();
        assert_eq!(rows[0].journal.as_deref(), Some("first line\nsecond line"));
    }
}
