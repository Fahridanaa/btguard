//! Process-level tests for `btguard watch`.
//!
//! The watcher spawns `pw-dump` and `journalctl` by name. These tests put fake
//! scripts first on PATH, so they never touch the live audio graph or journal.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tempfile::TempDir;

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write the fake script");
    let mut permissions = fs::metadata(path).expect("stat").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod");
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pw-dump-bluez.json")
}

fn setup(pw_dump: &str, journalctl: &str) -> (TempDir, PathBuf, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = dir.path().join("bin");
    fs::create_dir_all(&bin).expect("fake bin");
    write_script(&bin.join("pw-dump"), pw_dump);
    write_script(&bin.join("journalctl"), journalctl);

    let state = dir.path().join("state");
    fs::create_dir_all(&state).expect("state dir");

    let fake = dir.path().to_path_buf();
    (dir, bin, state, fake)
}

fn watcher_command(bin: &Path, state: &Path, fake: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_btguard"));
    command
        .arg("watch")
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("XDG_STATE_HOME", state)
        .env("BTGUARD_FAKE_DIR", fake)
        .env("BTGUARD_FIXTURE", fixture())
        .env("HOME", fake)
        .stdin(Stdio::null());
    command
}

struct Watcher {
    child: Child,
    stopped: bool,
}

impl Watcher {
    fn stop(mut self) {
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGTERM);
        }
        let _ = self.child.wait();
        self.stopped = true;
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
        }
        let _ = self.child.wait();
    }
}

fn spawn_watcher(bin: &Path, state: &Path, fake: &Path) -> Watcher {
    let out = fs::File::create(fake.join("watcher.log")).expect("watcher log");
    let err = out.try_clone().expect("watcher log clone");
    // A new session lets the test stop the watcher and its fake children.
    let child = Command::new("/usr/bin/setsid")
        .arg(env!("CARGO_BIN_EXE_btguard"))
        .arg("watch")
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("XDG_STATE_HOME", state)
        .env("BTGUARD_FAKE_DIR", fake)
        .env("BTGUARD_FIXTURE", fixture())
        .env("HOME", fake)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("spawn the watcher");
    Watcher {
        child,
        stopped: false,
    }
}

fn stop(watcher: Watcher) {
    watcher.stop();
}

fn db_path(state: &Path) -> PathBuf {
    state.join("bt-audio-guard/incidents.db")
}

fn rows_now(state: &Path) -> Vec<(String, String)> {
    let path = db_path(state);
    if !path.exists() {
        return Vec::new();
    }
    let conn = match Connection::open(&path) {
        Ok(conn) => conn,
        Err(_) => return Vec::new(),
    };
    let _ = conn.busy_timeout(Duration::from_millis(500));
    let mut statement = match conn.prepare("SELECT kind, COALESCE(journal, '') FROM incidents") {
        Ok(statement) => statement,
        Err(_) => return Vec::new(),
    };
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)));
    match rows {
        Ok(rows) => rows.filter_map(Result::ok).collect(),
        Err(_) => Vec::new(),
    }
}

fn wait_for_message(state: &Path, needle: &str, timeout: Duration) -> Vec<(String, String)> {
    let deadline = Instant::now() + timeout;
    loop {
        let rows = rows_now(state);
        if rows.iter().any(|(_, journal)| journal.contains(needle)) {
            return rows;
        }
        if Instant::now() >= deadline {
            return rows;
        }
        sleep(Duration::from_millis(100));
    }
}

const MONITOR_THAT_DIES_ONCE: &str = r#"#!/bin/bash
dir="${BTGUARD_FAKE_DIR:?}"
echo "pw-dump $*" >> "$dir/pw-dump.args"
cat "$BTGUARD_FIXTURE"
if [ -f "$dir/pw-dump-once" ]; then
    sleep 60
else
    touch "$dir/pw-dump-once"
    sleep 0.2
    exit 0
fi
"#;

const MONITOR_THAT_STAYS: &str = r#"#!/bin/bash
dir="${BTGUARD_FAKE_DIR:?}"
echo "pw-dump $*" >> "$dir/pw-dump.args"
cat "$BTGUARD_FIXTURE"
sleep 60
"#;

const JOURNAL_ONE_LINE_THEN_STAYS: &str = r#"#!/bin/bash
dir="${BTGUARD_FAKE_DIR:?}"
echo "journalctl $*" >> "$dir/journalctl.args"
sleep 0.8
ts=$(date +%s%6N)
printf '{"__REALTIME_TIMESTAMP":"%s","SYSLOG_IDENTIFIER":"wireplumber","__CURSOR":"s=test;i=1","MESSAGE":"spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0 FIRST"}\n' "$ts"
sleep 60
"#;

const JOURNAL_THAT_DIES_AND_RESUMES: &str = r#"#!/bin/bash
dir="${BTGUARD_FAKE_DIR:?}"
echo "journalctl $*" >> "$dir/journalctl.args"
if printf '%s\n' "$@" | grep -q -- '--after-cursor=s=test;i=1'; then
    sleep 0.3
    ts=$(date +%s%6N)
    printf '{"__REALTIME_TIMESTAMP":"%s","SYSLOG_IDENTIFIER":"wireplumber","__CURSOR":"s=test;i=2","MESSAGE":"spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0 SECOND"}\n' "$ts"
    sleep 60
else
    sleep 0.3
    ts=$(date +%s%6N)
    printf '{"__REALTIME_TIMESTAMP":"%s","SYSLOG_IDENTIFIER":"wireplumber","__CURSOR":"s=test;i=1","MESSAGE":"spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0 FIRST"}\n' "$ts"
    exit 0
fi
"#;

const JOURNAL_THAT_STAYS: &str = r#"#!/bin/bash
sleep 60
"#;

#[test]
fn a_dying_pw_dump_is_restarted_and_the_transport_line_is_recorded() {
    let (dir, bin, state, fake) = setup(MONITOR_THAT_DIES_ONCE, JOURNAL_ONE_LINE_THEN_STAYS);
    let child = spawn_watcher(&bin, &state, &fake);

    let rows = wait_for_message(&state, "FIRST", Duration::from_secs(10));
    stop(child);

    if rows.len() != 1 {
        let log = fs::read_to_string(fake.join("watcher.log")).unwrap_or_default();
        panic!("rows: {rows:#?}; watcher log:\n{log}");
    }
    assert_eq!(rows[0].0, "transport-failure");
    let args = fs::read_to_string(fake.join("pw-dump.args")).expect("pw-dump args");
    assert!(
        args.lines().count() >= 2,
        "the watcher must restart pw-dump: {args}"
    );
    drop(dir);
}

#[test]
fn a_dying_journal_is_restarted_from_the_saved_cursor() {
    let (dir, bin, state, fake) = setup(MONITOR_THAT_STAYS, JOURNAL_THAT_DIES_AND_RESUMES);
    let child = spawn_watcher(&bin, &state, &fake);

    let rows = wait_for_message(&state, "SECOND", Duration::from_secs(10));
    stop(child);

    let all = rows
        .iter()
        .map(|(_, journal)| journal.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if !all.contains("FIRST") || !all.contains("SECOND") {
        let log = fs::read_to_string(fake.join("watcher.log")).unwrap_or_default();
        panic!("journal rows: {all:?}; watcher log:\n{log}");
    }

    let args = fs::read_to_string(fake.join("journalctl.args")).expect("journalctl args");
    assert!(
        args.contains("--after-cursor=s=test;i=1"),
        "the restart did not resume from the cursor: {args}"
    );

    let cursor =
        fs::read_to_string(state.join("bt-audio-guard/journal.cursor")).expect("the saved cursor");
    assert_eq!(cursor.trim(), "s=test;i=2");
    drop(dir);
}

#[test]
fn a_second_watcher_is_refused() {
    let (dir, bin, state, fake) = setup(MONITOR_THAT_STAYS, JOURNAL_THAT_STAYS);
    let child = spawn_watcher(&bin, &state, &fake);

    let lock = state.join("bt-audio-guard/watch.lock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !lock.exists() && Instant::now() < deadline {
        sleep(Duration::from_millis(100));
    }
    if !lock.exists() {
        let log = fs::read_to_string(fake.join("watcher.log")).unwrap_or_default();
        panic!("the first watcher did not take the lock; log:\n{log}");
    }
    sleep(Duration::from_millis(300));

    let output = watcher_command(&bin, &state, &fake)
        .output()
        .expect("run the second watcher");
    stop(child);

    assert!(!output.status.success(), "the second watcher must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("already running"), "stderr: {stderr}");
    drop(dir);
}
