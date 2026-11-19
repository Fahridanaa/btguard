//! Watch PipeWire and the journal, and record incidents.
//!
//! Two child processes are the only inputs: `pw-dump --monitor` for the graph
//! and `journalctl --follow` for Bluetooth lines. Nothing here changes audio
//! state. Every row comes from one of those two sources.
//!
//! The store is an incident log, not a transition log. A normal disconnect,
//! and the transport lines from a normal teardown, make no row.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::paths;
use crate::pw::{self, Card, Node};
use crate::store::{Incident, Store, kind};

/// Attach journal lines within this many seconds of a row.
const CONTEXT_SECONDS: i64 = 1;
/// How long the journal buffer keeps lines.
const CONTEXT_BUFFER_SECONDS: i64 = 10;
/// Cap on buffered journal lines, in case the journal is noisy.
const CONTEXT_LINES: usize = 500;
/// A pending episode waits until the end of the next second, so a teardown
/// that starts in the next second can cancel it.
const PENDING_SECONDS: i64 = 2;
/// A stream that is at least this far below the right channel is corrected.
const CORRECTED_DB: f64 = -6.0;
/// A correction that moves toward flat by this much is a deviation.
const BALANCE_DROP_DB: f64 = 3.0;
/// Wake this often to flush pending episodes and retry a failed child.
const RECV_TICK: Duration = Duration::from_millis(250);
/// Give up after this many short-lived children in a row.
const MAX_RESTART_FAILURES: usize = 5;

pub fn watch() -> Result<()> {
    let _lock = WatchLock::acquire(&paths::watch_lock_path()?)?;
    let store = Store::open_default()?;
    eprintln!("btguard watch: database {}", paths::db_path()?.display());

    let (sender, receiver) = mpsc::channel();
    let mut sources = Sources::new(
        sender.clone(),
        CursorFile::new(paths::journal_cursor_path()?),
    );
    sources.start_monitor();
    sources.start_journal();
    drop(sender);

    let mut observer = Observer::new(&store);
    let result = run_loop(&mut observer, &mut sources, receiver);
    sources.stop();
    result
}

fn run_loop(
    observer: &mut Observer<'_>,
    sources: &mut Sources,
    receiver: Receiver<Event>,
) -> Result<()> {
    loop {
        let now = now_unix();
        observer.flush_pending(now)?;
        if sources.exhausted() {
            bail!("could not keep the input processes alive");
        }

        match receiver.recv_timeout(RECV_TICK) {
            Ok(Event::Monitor { objects, full }) => {
                observer.on_monitor(&objects, now_unix(), full)?;
            }
            Ok(Event::Journal(line)) => {
                observer.on_journal(line.clone())?;
                if let Err(error) = sources.cursor.save_for(&line) {
                    eprintln!("btguard watch: could not save the journal cursor: {error:#}");
                }
            }
            Ok(Event::Stopped { source, error }) => {
                match error {
                    Some(error) => eprintln!(
                        "btguard watch: {} stopped: {error}; restarting",
                        source.name()
                    ),
                    None => eprintln!("btguard watch: {} stopped; restarting", source.name()),
                }
                sources.restart(source)?;
            }
            Err(RecvTimeoutError::Timeout) => sources.retry_due(now)?,
            Err(RecvTimeoutError::Disconnected) => {
                bail!("both input readers stopped");
            }
        }
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

fn backoff(failures: usize) -> i64 {
    match failures {
        0 | 1 => 1,
        2 => 2,
        3 => 4,
        4 => 8,
        _ => 16,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Monitor,
    Journal,
}

impl Source {
    fn name(self) -> &'static str {
        match self {
            Self::Monitor => "pw-dump",
            Self::Journal => "journalctl",
        }
    }
}

enum Event {
    Monitor {
        objects: Vec<Value>,
        full: bool,
    },
    Journal(JournalLine),
    Stopped {
        source: Source,
        error: Option<String>,
    },
}

struct Input {
    child: Option<Child>,
    started_at: i64,
    used_cursor: bool,
    retry_at: i64,
    failures: usize,
}

impl Input {
    fn new() -> Self {
        Self {
            child: None,
            started_at: 0,
            used_cursor: false,
            retry_at: 0,
            failures: 0,
        }
    }
}

struct Sources {
    sender: Sender<Event>,
    cursor: CursorFile,
    monitor: Input,
    journal: Input,
}

impl Sources {
    fn new(sender: Sender<Event>, cursor: CursorFile) -> Self {
        Self {
            sender,
            cursor,
            monitor: Input::new(),
            journal: Input::new(),
        }
    }

    fn start_monitor(&mut self) {
        if let Some(mut child) = self.monitor.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        match spawn_monitor(self.sender.clone()) {
            Ok(child) => {
                self.monitor.child = Some(child);
                self.monitor.started_at = now_unix();
                self.monitor.used_cursor = false;
            }
            Err(error) => {
                self.monitor.failures += 1;
                self.monitor.retry_at = now_unix() + backoff(self.monitor.failures);
                eprintln!("btguard watch: pw-dump failed to start: {error:#}");
            }
        }
    }

    fn start_journal(&mut self) {
        if let Some(mut child) = self.journal.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let cursor = match self.cursor.read() {
            Ok(cursor) => cursor,
            Err(error) => {
                eprintln!("btguard watch: could not read the journal cursor: {error:#}");
                None
            }
        };
        match spawn_journal(self.sender.clone(), cursor.clone()) {
            Ok(child) => {
                self.journal.child = Some(child);
                self.journal.started_at = now_unix();
                self.journal.used_cursor = cursor.is_some();
            }
            Err(error) => {
                self.journal.failures += 1;
                self.journal.retry_at = now_unix() + backoff(self.journal.failures);
                eprintln!("btguard watch: journalctl failed to start: {error:#}");
            }
        }
    }

    fn restart(&mut self, source: Source) -> Result<()> {
        let now = now_unix();
        let (lived, status, used_cursor) = match source {
            Source::Monitor => {
                let status = self
                    .monitor
                    .child
                    .take()
                    .and_then(|mut child| child.wait().ok());
                (
                    now - self.monitor.started_at,
                    status,
                    self.monitor.used_cursor,
                )
            }
            Source::Journal => {
                let status = self
                    .journal
                    .child
                    .take()
                    .and_then(|mut child| child.wait().ok());
                (
                    now - self.journal.started_at,
                    status,
                    self.journal.used_cursor,
                )
            }
        };

        let failures = {
            let input = match source {
                Source::Monitor => &mut self.monitor,
                Source::Journal => &mut self.journal,
            };
            if lived <= 2 {
                input.failures += 1;
            } else {
                input.failures = 0;
            }
            input.failures
        };
        if failures >= MAX_RESTART_FAILURES {
            bail!(
                "{} stopped {failures} times in a row; giving up",
                source.name()
            );
        }

        if source == Source::Journal
            && used_cursor
            && lived <= 2
            && status.map(|status| !status.success()).unwrap_or(false)
        {
            eprintln!("btguard watch: journal cursor was rejected; starting at the tail");
            self.cursor.clear()?;
        }

        match source {
            Source::Monitor => self.start_monitor(),
            Source::Journal => self.start_journal(),
        }
        Ok(())
    }

    fn retry_due(&mut self, now: i64) -> Result<()> {
        if self.monitor.child.is_none() && now >= self.monitor.retry_at {
            self.start_monitor();
        }
        if self.journal.child.is_none() && now >= self.journal.retry_at {
            self.start_journal();
        }
        Ok(())
    }

    fn exhausted(&self) -> bool {
        (self.monitor.child.is_none() && self.monitor.failures >= MAX_RESTART_FAILURES)
            || (self.journal.child.is_none() && self.journal.failures >= MAX_RESTART_FAILURES)
    }

    fn stop(&mut self) {
        for input in [&mut self.monitor, &mut self.journal] {
            if let Some(mut child) = input.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

impl Drop for Sources {
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_monitor(sender: Sender<Event>) -> Result<Child> {
    let mut child = Command::new("pw-dump")
        .arg("--monitor")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("start pw-dump --monitor; is PipeWire installed?")?;
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            bail!("pw-dump wrote no stdout");
        }
    };
    spawn_monitor_reader(stdout, sender);
    Ok(child)
}

fn spawn_journal(sender: Sender<Event>, cursor: Option<String>) -> Result<Child> {
    let mut command = Command::new("journalctl");
    command.args(journal_args(cursor.as_deref()));

    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("start journalctl --follow")?;
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            bail!("journalctl wrote no stdout");
        }
    };
    spawn_journal_reader(stdout, sender);
    Ok(child)
}

/// Resume after the saved cursor. A first run starts at the live tail.
fn journal_args(cursor: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "--follow".to_owned(),
        "--output=json".to_owned(),
        "--no-pager".to_owned(),
    ];
    match cursor {
        Some(cursor) => args.push(format!("--after-cursor={cursor}")),
        None => {
            args.push("-n".to_owned());
            args.push("0".to_owned());
        }
    }
    args
}

fn spawn_monitor_reader(stdout: ChildStdout, sender: Sender<Event>) {
    thread::spawn(move || {
        let mut reader = JsonArrayReader::new(stdout);
        let mut full = true;
        loop {
            match reader.next_array() {
                Ok(Some(objects)) => {
                    if sender.send(Event::Monitor { objects, full }).is_err() {
                        return;
                    }
                    full = false;
                }
                Ok(None) => {
                    let _ = sender.send(Event::Stopped {
                        source: Source::Monitor,
                        error: None,
                    });
                    return;
                }
                Err(error) => {
                    let _ = sender.send(Event::Stopped {
                        source: Source::Monitor,
                        error: Some(format!("{error:#}")),
                    });
                    return;
                }
            }
        }
    });
}

fn spawn_journal_reader(stdout: ChildStdout, sender: Sender<Event>) {
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let raw = match line {
                Ok(raw) => raw,
                Err(error) => {
                    let _ = sender.send(Event::Stopped {
                        source: Source::Journal,
                        error: Some(error.to_string()),
                    });
                    return;
                }
            };
            if raw.is_empty() {
                continue;
            }
            match JournalLine::parse(&raw) {
                Ok(line) => {
                    if sender.send(Event::Journal(line)).is_err() {
                        return;
                    }
                }
                Err(error) => eprintln!("btguard watch: skip a journal line: {error:#}"),
            }
        }

        let _ = sender.send(Event::Stopped {
            source: Source::Journal,
            error: None,
        });
    });
}

/// Read the sequence of JSON arrays that `pw-dump --monitor` writes.
///
/// The first array is the full graph. Each change is one more array. Every
/// array is complete, so a reader must follow braces and strings, not lines.
struct JsonArrayReader<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> JsonArrayReader<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    fn next_array(&mut self) -> Result<Option<Vec<Value>>> {
        let mut buffer = Vec::new();
        let mut depth = 0_i32;
        let mut in_string = false;
        let mut escaped = false;
        let mut started = false;

        loop {
            let Some(byte) = self.read_byte()? else {
                if started {
                    bail!("the stream ended in the middle of a JSON array");
                }
                return Ok(None);
            };

            if !started {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte != b'[' {
                    bail!("expected a JSON array, not {:?}", byte as char);
                }
                started = true;
                depth = 1;
                buffer.push(byte);
                continue;
            }

            buffer.push(byte);

            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => in_string = true,
                b'[' | b'{' => depth += 1,
                b']' | b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return serde_json::from_slice(&buffer)
                            .map(Some)
                            .context("parse a pw-dump monitor array");
                    }
                    if depth < 0 {
                        bail!("the JSON array closed twice");
                    }
                }
                _ => {}
            }
        }
    }

    fn read_byte(&mut self) -> io::Result<Option<u8>> {
        let mut byte = [0_u8; 1];
        match self.reader.read(&mut byte)? {
            0 => Ok(None),
            _ => Ok(Some(byte[0])),
        }
    }
}

/// One journal line from `journalctl --output=json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalLine {
    pub at_unix: i64,
    pub identifier: String,
    pub message: String,
    pub cursor: Option<String>,
    pub relevant: bool,
}

impl JournalLine {
    fn parse(raw: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(raw).context("journalctl did not write JSON")?;
        let message = message_text(&value);
        let identifier = identifier_text(&value);
        let relevant = is_bluetooth_relevant(&identifier, &message);

        Ok(Self {
            at_unix: timestamp_seconds(&value).unwrap_or_else(now_unix),
            identifier,
            message,
            cursor: value
                .get("__CURSOR")
                .and_then(Value::as_str)
                .map(str::to_owned),
            relevant,
        })
    }

    fn incident_kind(&self) -> Option<&'static str> {
        if self
            .message
            .contains("Failure in Bluetooth audio transport")
        {
            Some(kind::TRANSPORT_FAILURE)
        } else if self
            .message
            .contains("SCO packet for unknown connection handle")
        {
            Some(kind::SCO_ORPHAN)
        } else {
            None
        }
    }

    fn device(&self) -> Option<String> {
        bluetooth_device(&self.message)
    }
}

fn message_text(value: &Value) -> String {
    match value.get("MESSAGE") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(bytes)) => {
            let bytes: Vec<u8> = bytes
                .iter()
                .filter_map(Value::as_u64)
                .map(|b| b as u8)
                .collect();
            String::from_utf8_lossy(&bytes).into_owned()
        }
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn identifier_text(value: &Value) -> String {
    for key in ["SYSLOG_IDENTIFIER", "_COMM", "_TRANSPORT"] {
        if let Some(text) = value.get(key).and_then(Value::as_str) {
            return text.to_owned();
        }
    }
    String::new()
}

fn timestamp_seconds(value: &Value) -> Option<i64> {
    let raw = match value.get("__REALTIME_TIMESTAMP")? {
        Value::String(text) => text.parse::<i64>().ok()?,
        Value::Number(number) => number.as_i64()?,
        _ => return None,
    };
    // journalctl writes microseconds. Accept seconds too, for a synthetic log.
    Some(if raw > 1_000_000_000_000 {
        raw / 1_000_000
    } else {
        raw
    })
}

/// Keep the lines that give Bluetooth context. The transport failure itself
/// is a WirePlumber line (`spa.bluez5:`), not a bluetoothd line.
fn is_bluetooth_relevant(identifier: &str, message: &str) -> bool {
    match identifier {
        "bluetoothd" => true,
        "kernel" => {
            message.contains("Bluetooth") || message.contains("hci") || message.contains("SCO")
        }
        "wireplumber" | "pipewire" => {
            message.contains("bluez5")
                || message.contains("Bluetooth audio")
                || message.contains("bluez_output")
                || message.contains("bluez_input")
        }
        _ => false,
    }
}

/// `dev_1C_0A_E0_A3_E2_D0` in a BlueZ path becomes a card name.
fn bluetooth_device(message: &str) -> Option<String> {
    let rest = &message[message.find("dev_")? + 4..];
    let address = rest.get(..17)?;
    for (index, character) in address.char_indices() {
        if index % 3 == 2 {
            if character != '_' {
                return None;
            }
        } else if !character.is_ascii_hexdigit() {
            return None;
        }
    }
    Some(format!("bluez_card.{}", address.to_ascii_uppercase()))
}

struct CursorFile {
    path: PathBuf,
    last_saved: Option<String>,
    last_saved_at: i64,
}

impl CursorFile {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            last_saved: None,
            last_saved_at: 0,
        }
    }

    fn read(&mut self) -> Result<Option<String>> {
        match fs::read_to_string(&self.path) {
            Ok(text) => {
                let text = text.trim();
                if text.is_empty() {
                    self.last_saved = None;
                    Ok(None)
                } else {
                    self.last_saved = Some(text.to_owned());
                    Ok(Some(text.to_owned()))
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.last_saved = None;
                Ok(None)
            }
            Err(error) => Err(error).with_context(|| format!("read {}", self.path.display())),
        }
    }

    fn save_for(&mut self, line: &JournalLine) -> Result<()> {
        let Some(cursor) = &line.cursor else {
            return Ok(());
        };
        if self.last_saved.as_deref() == Some(cursor.as_str()) {
            return Ok(());
        }
        if line.relevant || self.last_saved.is_none() || line.at_unix - self.last_saved_at >= 1 {
            self.write(cursor)?;
            self.last_saved = Some(cursor.clone());
            self.last_saved_at = line.at_unix;
        }
        Ok(())
    }

    fn write(&self, cursor: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let temp = self.path.with_extension("cursor.tmp");
        fs::write(&temp, cursor).with_context(|| format!("write {}", temp.display()))?;
        fs::rename(&temp, &self.path)
            .with_context(|| format!("rename {} to {}", temp.display(), self.path.display()))?;
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        self.last_saved = None;
        self.last_saved_at = 0;
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("remove {}", self.path.display())),
        }
    }
}

/// One instance of `btguard watch` per state directory.
#[derive(Debug)]
pub struct WatchLock {
    _file: File,
}

impl WatchLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .with_context(|| format!("open {}", path.display()))?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                bail!(
                    "another btguard watch is already running (lock {})",
                    path.display()
                );
            }
            return Err(error).with_context(|| format!("lock {}", path.display()));
        }
        Ok(Self { _file: file })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Link {
    output_node: u64,
    input_node: u64,
}

impl Link {
    fn from_object(object: &Value) -> Option<Self> {
        Some(Self {
            output_node: object.pointer("/info/output-node-id")?.as_u64()?,
            input_node: object.pointer("/info/input-node-id")?.as_u64()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CardState {
    name: String,
    profile: Option<String>,
    codec: Option<String>,
    connection: Option<String>,
}

#[derive(Debug, Clone)]
struct RecentRow {
    at_unix: i64,
    id: i64,
    kind: String,
    from_journal: bool,
}

#[derive(Debug, Clone)]
struct RecentCardRow {
    card_id: u64,
    at_unix: i64,
    id: i64,
}

#[derive(Debug, Clone)]
struct Pending {
    card_id: u64,
    at_unix: i64,
    deadline: i64,
    incident: Incident,
    from_journal: bool,
}

#[derive(Debug, Clone)]
struct StreamLevels {
    volumes: Vec<f64>,
    map: Vec<String>,
}

pub struct Observer<'a> {
    store: &'a Store,
    cards: HashMap<u64, Card>,
    nodes: HashMap<u64, Node>,
    links: HashMap<u64, Link>,
    default_sink: Option<String>,
    /// Relevant journal lines, newest last, pruned to `CONTEXT_BUFFER_SECONDS`.
    journal: VecDeque<(i64, String)>,
    /// Rows written in the last few seconds, for journal backfill.
    recent: Vec<RecentRow>,
    /// One open episode per card, waiting for the teardown window.
    pending: Vec<Pending>,
    /// Card removals in the last few seconds, to classify teardown lines.
    disconnects: Vec<(u64, i64)>,
    /// Per-stream channel levels from the previous update.
    volumes: HashMap<u64, StreamLevels>,
    /// The last usable profile per card. `off` is teardown, not audio.
    last_profiles: HashMap<u64, String>,
    /// The last codec per card. A removed sink must not erase it.
    last_codecs: HashMap<u64, String>,
    /// Profile and codec rows that a later event can still fold into.
    profile_rows: Vec<RecentCardRow>,
    codec_rows: Vec<RecentCardRow>,
    initialized: bool,
}

impl<'a> Observer<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            cards: HashMap::new(),
            nodes: HashMap::new(),
            links: HashMap::new(),
            default_sink: None,
            journal: VecDeque::new(),
            recent: Vec::new(),
            pending: Vec::new(),
            disconnects: Vec::new(),
            volumes: HashMap::new(),
            last_profiles: HashMap::new(),
            last_codecs: HashMap::new(),
            profile_rows: Vec::new(),
            codec_rows: Vec::new(),
            initialized: false,
        }
    }

    /// Apply one array from `pw-dump --monitor`.
    ///
    /// The first array of each process is a full snapshot. The first snapshot
    /// of the watcher only sets the baseline. A later snapshot reconciles the
    /// graph, so a PipeWire restart is handled without a stale card.
    pub fn on_monitor(&mut self, objects: &[Value], at_unix: i64, full: bool) -> Result<()> {
        let before = self.card_states();
        let before_sink = self.default_sink.clone();
        let before_volumes = self.volumes.clone();
        let raw_before: HashMap<u64, Option<String>> = self
            .cards
            .iter()
            .map(|(id, card)| (*id, card.profile.clone()))
            .collect();

        if full && self.initialized {
            self.reconcile(objects);
        }
        for object in objects {
            self.apply(object);
        }

        let mut off_cards = Vec::new();
        let mut off_ended = Vec::new();
        for (id, card) in &self.cards {
            let was_off = raw_before.get(id).and_then(|profile| profile.as_deref()) == Some("off");
            let is_off = card.profile.as_deref() == Some("off");
            let disconnected = card.connection.as_deref() == Some("disconnected");
            if !was_off && is_off && !disconnected {
                off_cards.push(card.clone());
            } else if was_off && !is_off {
                off_ended.push(*id);
            }
        }
        for card in off_cards {
            self.start_off_pending(&card, at_unix);
        }
        for id in off_ended {
            self.cancel_pending(id, at_unix);
        }

        let after = self.card_states();
        let baseline = !self.initialized;

        self.compare_cards(&before, &after, at_unix, baseline)?;
        self.compare_streams(&before_volumes, at_unix, baseline)?;
        self.prune(at_unix);

        if baseline {
            self.initialized = true;
            let cards: Vec<&str> = self.cards.values().map(|card| card.name.as_str()).collect();
            let streams = self
                .nodes
                .values()
                .filter(|node| node.media_class == "Stream/Output/Audio")
                .count();
            eprintln!(
                "btguard watch: baseline: {} card(s), {} output stream(s){}",
                cards.len(),
                streams,
                if cards.is_empty() {
                    String::new()
                } else {
                    format!(": {}", cards.join(", "))
                }
            );
        } else if before_sink != self.default_sink {
            eprintln!(
                "btguard watch: default sink {} -> {}",
                before_sink.as_deref().unwrap_or("(none)"),
                self.default_sink.as_deref().unwrap_or("(none)")
            );
        }

        Ok(())
    }

    /// Apply one journal line. Only a deviation makes a row.
    pub fn on_journal(&mut self, line: JournalLine) -> Result<()> {
        if !line.relevant {
            return Ok(());
        }
        self.push_journal(&line);

        let Some(trigger) = line.incident_kind() else {
            self.append_to_recent(line.at_unix, &line.message, None)?;
            self.prune(line.at_unix);
            return Ok(());
        };

        let Some(card_id) = self.card_for_line(&line) else {
            // A teardown line after the card is gone, or an ambiguous line.
            self.append_to_recent(line.at_unix, &line.message, None)?;
            self.prune(line.at_unix);
            return Ok(());
        };

        if let Some(row) = self.recent_journal_row(trigger, line.at_unix) {
            self.store.append_journal(row.id, &line.message)?;
            self.append_to_recent(line.at_unix, &line.message, Some(row.id))?;
            self.prune(line.at_unix);
            return Ok(());
        }

        if let Some(row) = self.recent_profile_row(card_id, line.at_unix) {
            self.store.append_journal(row.id, &line.message)?;
            self.append_to_recent(line.at_unix, &line.message, Some(row.id))?;
            self.prune(line.at_unix);
            return Ok(());
        }

        if self.has_pending(card_id, line.at_unix) {
            // The pending episode already covers this line, and its journal is
            // rebuilt from the buffer when it is written.
            self.append_to_recent(line.at_unix, &line.message, None)?;
            self.prune(line.at_unix);
            return Ok(());
        }

        if self.recent_disconnect(card_id, line.at_unix) {
            // A normal teardown line is not an incident.
            self.append_to_recent(line.at_unix, &line.message, None)?;
            self.prune(line.at_unix);
            return Ok(());
        }

        self.start_transport_pending(card_id, trigger, line.at_unix);
        self.append_to_recent(line.at_unix, &line.message, None)?;
        self.prune(line.at_unix);
        Ok(())
    }

    /// Write every pending episode whose teardown window has closed.
    pub fn flush_pending(&mut self, now_unix: i64) -> Result<()> {
        let mut keep = Vec::new();
        let pending_episodes = std::mem::take(&mut self.pending);
        for mut pending in pending_episodes {
            if now_unix >= pending.deadline {
                pending.incident.journal = self.context(pending.incident.at_unix);
                let id = self.store.insert(&pending.incident)?;
                self.recent.push(RecentRow {
                    at_unix: pending.incident.at_unix,
                    id,
                    kind: pending.incident.kind.clone(),
                    from_journal: pending.from_journal,
                });
                if pending.incident.kind == kind::PROFILE_CHANGE {
                    self.profile_rows.push(RecentCardRow {
                        card_id: pending.card_id,
                        at_unix: pending.incident.at_unix,
                        id,
                    });
                }
                eprintln!(
                    "btguard watch: {} at {}",
                    pending.incident.kind,
                    local_time(pending.incident.at_unix)
                );
            } else {
                keep.push(pending);
            }
        }
        self.pending = keep;
        Ok(())
    }

    fn apply(&mut self, object: &Value) {
        let Some(id) = object.get("id").and_then(Value::as_u64) else {
            return;
        };

        // A removal is the only event without a type:
        //   {"id": 98, "info": null}
        if object.get("type").is_none() {
            self.cards.remove(&id);
            self.nodes.remove(&id);
            self.links.remove(&id);
            return;
        }

        match object.get("type").and_then(Value::as_str) {
            Some("PipeWire:Interface:Device") => {
                if let Some(card) = Card::from_object(object) {
                    if let Some(profile) = &card.profile
                        && profile.as_str() != "off"
                    {
                        self.last_profiles.insert(card.id, profile.clone());
                    }
                    self.cards.insert(card.id, card);
                }
            }
            Some("PipeWire:Interface:Node") => {
                if let Some(node) = Node::from_object(object) {
                    if let (Some(card_id), Some(codec)) = (node.device_id, node.codec.as_ref()) {
                        self.last_codecs.insert(card_id, codec.clone());
                    }
                    self.nodes.insert(node.id, node);
                }
            }
            Some("PipeWire:Interface:Link") => {
                if let Some(link) = Link::from_object(object) {
                    self.links.insert(id, link);
                }
            }
            Some("PipeWire:Interface:Metadata") => {
                if object
                    .pointer("/props/metadata.name")
                    .and_then(Value::as_str)
                    == Some("default")
                    && let Some(name) = pw::default_sink_from_metadata(object)
                {
                    self.default_sink = Some(name.to_owned());
                }
            }
            _ => {}
        }
    }

    fn reconcile(&mut self, objects: &[Value]) {
        let present: HashSet<u64> = objects
            .iter()
            .filter_map(|object| object.get("id").and_then(Value::as_u64))
            .collect();
        self.cards.retain(|id, _| present.contains(id));
        self.nodes.retain(|id, _| present.contains(id));
        self.links.retain(|id, _| present.contains(id));
    }

    fn card_states(&self) -> HashMap<u64, CardState> {
        self.cards
            .iter()
            .map(|(id, card)| {
                (
                    *id,
                    CardState {
                        name: card.name.clone(),
                        profile: self
                            .last_profiles
                            .get(id)
                            .cloned()
                            .or_else(|| card.profile.clone()),
                        codec: self
                            .last_codecs
                            .get(id)
                            .cloned()
                            .or_else(|| self.card_codec(card)),
                        connection: card
                            .connection
                            .clone()
                            .or_else(|| Some("connected".to_owned())),
                    },
                )
            })
            .collect()
    }

    fn card_codec(&self, card: &Card) -> Option<String> {
        let sink = self
            .nodes
            .values()
            .filter(|node| node.device_id == Some(card.id) && node.media_class == "Audio/Sink")
            .min_by_key(|node| node.id);
        let source = self
            .nodes
            .values()
            .filter(|node| node.device_id == Some(card.id) && node.media_class == "Audio/Source")
            .min_by_key(|node| node.id);

        sink.or(source).and_then(|node| node.codec.clone())
    }

    fn compare_cards(
        &mut self,
        before: &HashMap<u64, CardState>,
        after: &HashMap<u64, CardState>,
        at_unix: i64,
        baseline: bool,
    ) -> Result<()> {
        if baseline {
            return Ok(());
        }

        for (id, old) in before {
            let new = after.get(id);
            let disconnected = match new {
                Some(new) => {
                    old.connection.as_deref() != Some("disconnected")
                        && new.connection.as_deref() == Some("disconnected")
                }
                None => true,
            };

            if disconnected {
                // A normal disconnect is not an incident. It cancels a
                // teardown episode but keeps a real deviation row.
                self.cancel_pending(*id, at_unix);
                self.disconnects.push((*id, at_unix));
                eprintln!("btguard watch: disconnected {}", old.name);
                continue;
            }

            let Some(new) = new else {
                continue;
            };

            let profile_changed = old.profile != new.profile
                && old.profile.as_deref() != Some("off")
                && new.profile.as_deref() != Some("off");
            if profile_changed {
                self.record_profile_change(*id, old, new, at_unix)?;
                continue;
            }

            if old.codec.is_some() && new.codec.is_some() && old.codec != new.codec {
                self.record_codec_change(*id, old, new, at_unix)?;
            }
        }

        for (id, state) in after {
            if !before.contains_key(id) {
                eprintln!(
                    "btguard watch: connected {} profile {}",
                    state.name,
                    state.profile.as_deref().unwrap_or("unknown")
                );
            }
        }

        Ok(())
    }

    fn record_profile_change(
        &mut self,
        card_id: u64,
        old: &CardState,
        new: &CardState,
        at_unix: i64,
    ) -> Result<()> {
        self.cancel_pending(card_id, at_unix);
        let journal = self.context(at_unix);

        if let Some(codec_row) = self.recent_codec_row(card_id, at_unix).cloned() {
            self.store.promote_to_profile_change(
                codec_row.id,
                old.profile.as_deref(),
                new.profile.as_deref(),
                new.codec.as_deref(),
                journal.as_deref(),
            )?;
            self.codec_rows.retain(|row| row.id != codec_row.id);
            self.recent.retain(|row| row.id != codec_row.id);
            self.recent.push(RecentRow {
                at_unix: codec_row.at_unix,
                id: codec_row.id,
                kind: kind::PROFILE_CHANGE.to_owned(),
                from_journal: false,
            });
            self.profile_rows.push(RecentCardRow {
                card_id,
                at_unix: codec_row.at_unix,
                id: codec_row.id,
            });
            eprintln!(
                "btguard watch: profile {}: {} -> {} (folded with the codec row)",
                new.name,
                old.profile.as_deref().unwrap_or("unknown"),
                new.profile.as_deref().unwrap_or("unknown")
            );
            return Ok(());
        }

        let incident = Incident {
            at_unix,
            kind: kind::PROFILE_CHANGE.to_owned(),
            device: Some(new.name.clone()),
            profile_before: old.profile.clone(),
            profile_after: new.profile.clone(),
            codec: new.codec.clone(),
            detail: None,
            journal,
        };
        let id = self.store.insert(&incident)?;
        self.recent.push(RecentRow {
            at_unix,
            id,
            kind: kind::PROFILE_CHANGE.to_owned(),
            from_journal: false,
        });
        self.profile_rows.push(RecentCardRow {
            card_id,
            at_unix,
            id,
        });
        eprintln!(
            "btguard watch: profile {}: {} -> {}",
            new.name,
            old.profile.as_deref().unwrap_or("unknown"),
            new.profile.as_deref().unwrap_or("unknown")
        );
        Ok(())
    }

    fn record_codec_change(
        &mut self,
        card_id: u64,
        old: &CardState,
        new: &CardState,
        at_unix: i64,
    ) -> Result<()> {
        let Some(codec) = new.codec.as_deref() else {
            return Ok(());
        };

        if let Some(profile_row) = self.recent_profile_row(card_id, at_unix) {
            self.store.update_codec(profile_row.id, codec)?;
            return Ok(());
        }

        if let Some(pending) = self
            .pending
            .iter_mut()
            .find(|pending| pending.card_id == card_id && (pending.at_unix - at_unix).abs() <= 1)
        {
            pending.incident.codec = Some(codec.to_owned());
            return Ok(());
        }

        if let Some(codec_row) = self.recent_codec_row(card_id, at_unix).cloned() {
            self.store.update_codec(codec_row.id, codec)?;
            return Ok(());
        }

        let incident = Incident {
            at_unix,
            kind: kind::CODEC_CHANGE.to_owned(),
            device: Some(new.name.clone()),
            profile_before: None,
            profile_after: new.profile.clone(),
            codec: Some(codec.to_owned()),
            detail: Some(format!(
                "codec {} -> {}",
                old.codec.as_deref().unwrap_or("unknown"),
                codec
            )),
            journal: self.context(at_unix),
        };
        let id = self.store.insert(&incident)?;
        self.recent.push(RecentRow {
            at_unix,
            id,
            kind: kind::CODEC_CHANGE.to_owned(),
            from_journal: false,
        });
        self.codec_rows.push(RecentCardRow {
            card_id,
            at_unix,
            id,
        });
        eprintln!(
            "btguard watch: codec {}: {} -> {}",
            new.name,
            old.codec.as_deref().unwrap_or("unknown"),
            codec
        );
        Ok(())
    }

    fn compare_streams(
        &mut self,
        before: &HashMap<u64, StreamLevels>,
        at_unix: i64,
        baseline: bool,
    ) -> Result<()> {
        let nodes: Vec<Node> = self.nodes.values().cloned().collect();
        let mut next = HashMap::new();

        for node in &nodes {
            if node.media_class != "Stream/Output/Audio" {
                continue;
            }

            let levels = StreamLevels {
                volumes: node.channel_volumes.clone(),
                map: node.channel_map.clone(),
            };
            let sink = self.earbud_sink_for(node);

            let card_id =
                sink.and_then(|sink| self.nodes.get(&sink).and_then(|node| node.device_id));

            if !baseline
                && let Some(card_id) = card_id
                && !self
                    .pending
                    .iter()
                    .any(|pending| pending.card_id == card_id)
                && !self.recent_disconnect(card_id, at_unix)
                && let Some(old) = before.get(&node.id)
                && let (Some(old_db), Some(new_db)) =
                    (db_against_right(old), db_against_right(&levels))
                && old_db < CORRECTED_DB
                && new_db > old_db + BALANCE_DROP_DB
            {
                let card = self.cards.get(&card_id);
                let device = card.map(|card| card.name.clone());
                let profile = card.and_then(|card| card.profile.clone());
                let codec = card.and_then(|card| self.card_codec(card));
                let journal = self.context(at_unix);
                let lost = new_db > -1.0;
                self.record_incident(
                    Incident {
                        at_unix,
                        kind: kind::BALANCE_DRIFT.to_owned(),
                        device,
                        profile_before: None,
                        profile_after: profile,
                        codec,
                        detail: Some(format!(
                            "stream {}: left {:.3}, right {:.3} ({:.1} dB against right, was {:.1} dB); the balance correction {}",
                            node.name,
                            left_right(&levels).map(|(left, _)| left).unwrap_or(0.0),
                            left_right(&levels).map(|(_, right)| right).unwrap_or(0.0),
                            new_db,
                            old_db,
                            if lost { "was lost" } else { "weakened" }
                        )),
                        journal,
                    },
                    false,
                )?;
                eprintln!(
                    "btguard watch: balance drift on {}: {:.1} dB, was {:.1} dB",
                    node.name, new_db, old_db
                );
            }

            next.insert(node.id, levels);
        }

        self.volumes = next;
        Ok(())
    }

    /// The Bluetooth sink a stream feeds, if it is the earbuds.
    fn earbud_sink_for(&self, node: &Node) -> Option<u64> {
        if let Some(target) = &node.target_object
            && let Some(sink) = self.nodes.values().find(|candidate| {
                candidate.media_class == "Audio/Sink"
                    && candidate.is_bluez_audio()
                    && candidate.name == *target
            })
        {
            return Some(sink.id);
        }

        self.links
            .values()
            .filter(|link| link.output_node == node.id)
            .find_map(|link| {
                let sink = self.nodes.get(&link.input_node)?;
                (sink.media_class == "Audio/Sink" && sink.is_bluez_audio()).then_some(sink.id)
            })
    }

    fn start_off_pending(&mut self, card: &Card, at_unix: i64) {
        if let Some(pending) = self
            .pending
            .iter_mut()
            .find(|pending| pending.card_id == card.id && (pending.at_unix - at_unix).abs() <= 1)
        {
            pending.incident.profile_after = Some("off".to_owned());
            pending.incident.codec = self.last_codecs.get(&card.id).cloned();
            pending.incident.detail =
                Some("card is connected but silent on the off profile".to_owned());
            return;
        }

        let profile_before = self.last_profiles.get(&card.id).cloned();
        let codec = self.last_codecs.get(&card.id).cloned();
        let journal = self.context(at_unix);
        self.pending.push(Pending {
            card_id: card.id,
            at_unix,
            deadline: at_unix + PENDING_SECONDS,
            incident: Incident {
                at_unix,
                kind: kind::PROFILE_CHANGE.to_owned(),
                device: Some(card.name.clone()),
                profile_before,
                profile_after: Some("off".to_owned()),
                codec,
                detail: Some("card is connected but silent on the off profile".to_owned()),
                journal,
            },
            from_journal: false,
        });
    }

    fn start_transport_pending(&mut self, card_id: u64, trigger: &'static str, at_unix: i64) {
        let Some(card) = self.cards.get(&card_id) else {
            return;
        };
        let profile = self
            .last_profiles
            .get(&card_id)
            .cloned()
            .or_else(|| card.profile.clone());
        let codec = self.last_codecs.get(&card_id).cloned();
        let journal = self.context(at_unix);

        self.pending.push(Pending {
            card_id,
            at_unix,
            deadline: at_unix + PENDING_SECONDS,
            incident: Incident {
                at_unix,
                kind: trigger.to_owned(),
                device: Some(card.name.clone()),
                profile_before: None,
                profile_after: profile,
                codec,
                detail: None,
                journal,
            },
            from_journal: true,
        });
    }

    fn has_pending(&self, card_id: u64, at_unix: i64) -> bool {
        self.pending
            .iter()
            .any(|pending| pending.card_id == card_id && (pending.at_unix - at_unix).abs() <= 1)
    }

    fn cancel_pending(&mut self, card_id: u64, at_unix: i64) {
        self.pending.retain(|pending| {
            !(pending.card_id == card_id && (pending.at_unix - at_unix).abs() <= CONTEXT_SECONDS)
        });
    }

    fn recent_journal_row(&self, trigger: &str, at_unix: i64) -> Option<&RecentRow> {
        self.recent
            .iter()
            .filter(|row| {
                row.from_journal
                    && row.kind == trigger
                    && (row.at_unix - at_unix).abs() <= CONTEXT_SECONDS
            })
            .max_by_key(|row| row.at_unix)
    }

    fn recent_profile_row(&self, card_id: u64, at_unix: i64) -> Option<&RecentCardRow> {
        self.profile_rows
            .iter()
            .filter(|row| {
                row.card_id == card_id && (row.at_unix - at_unix).abs() <= CONTEXT_SECONDS
            })
            .max_by_key(|row| row.at_unix)
    }

    fn recent_codec_row(&self, card_id: u64, at_unix: i64) -> Option<&RecentCardRow> {
        self.codec_rows
            .iter()
            .filter(|row| {
                row.card_id == card_id && (row.at_unix - at_unix).abs() <= CONTEXT_SECONDS
            })
            .max_by_key(|row| row.at_unix)
    }

    fn recent_disconnect(&self, card_id: u64, at_unix: i64) -> bool {
        self.disconnects
            .iter()
            .any(|(id, at)| *id == card_id && (at - at_unix).abs() <= CONTEXT_SECONDS)
    }

    fn card_for_line(&self, line: &JournalLine) -> Option<u64> {
        let connected = |card: &&Card| card.connection.as_deref() != Some("disconnected");
        if let Some(name) = line.device() {
            return self
                .cards
                .values()
                .find(|card| card.name == name)
                .filter(connected)
                .map(|card| card.id);
        }

        let mut cards = self.cards.values().filter(connected);
        let first = cards.next();
        if cards.next().is_none() {
            first.map(|card| card.id)
        } else {
            None
        }
    }

    fn push_journal(&mut self, line: &JournalLine) {
        self.journal.push_back((line.at_unix, line.message.clone()));
        while let Some((second, _)) = self.journal.front() {
            if *second < line.at_unix - CONTEXT_BUFFER_SECONDS {
                self.journal.pop_front();
            } else {
                break;
            }
        }
        while self.journal.len() > CONTEXT_LINES {
            self.journal.pop_front();
        }
    }

    fn context(&self, at_unix: i64) -> Option<String> {
        let lines: Vec<&str> = self
            .journal
            .iter()
            .filter(|(second, _)| (second - at_unix).abs() <= CONTEXT_SECONDS)
            .map(|(_, line)| line.as_str())
            .collect();
        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    fn append_to_recent(&self, at_unix: i64, message: &str, skip: Option<i64>) -> Result<()> {
        for row in &self.recent {
            if Some(row.id) == skip {
                continue;
            }
            if (row.at_unix - at_unix).abs() <= CONTEXT_SECONDS {
                self.store.append_journal(row.id, message)?;
            }
        }
        Ok(())
    }

    fn record_incident(&mut self, incident: Incident, from_journal: bool) -> Result<i64> {
        let id = self.store.insert(&incident)?;
        self.recent.push(RecentRow {
            at_unix: incident.at_unix,
            id,
            kind: incident.kind.clone(),
            from_journal,
        });
        Ok(id)
    }

    fn prune(&mut self, at_unix: i64) {
        let cutoff = at_unix - CONTEXT_BUFFER_SECONDS;
        self.recent.retain(|row| row.at_unix >= cutoff);
        self.profile_rows.retain(|row| row.at_unix >= cutoff);
        self.codec_rows.retain(|row| row.at_unix >= cutoff);
        self.disconnects.retain(|(_, at)| *at >= cutoff);
    }
}

fn left_right(levels: &StreamLevels) -> Option<(f64, f64)> {
    let left_index = levels
        .map
        .iter()
        .position(|channel| channel == "FL")
        .unwrap_or(0);
    let right_index = levels
        .map
        .iter()
        .position(|channel| channel == "FR")
        .unwrap_or(1);
    if left_index == right_index
        || levels.volumes.len() <= left_index
        || levels.volumes.len() <= right_index
    {
        return None;
    }
    Some((levels.volumes[left_index], levels.volumes[right_index]))
}

fn db_against_right(levels: &StreamLevels) -> Option<f64> {
    let (left, right) = left_right(levels)?;
    let left = left.max(1e-9);
    let right = right.max(1e-9);
    Some(20.0 * (left / right).log10())
}

fn local_time(unix: i64) -> String {
    use chrono::{Local, TimeZone};
    Local
        .timestamp_opt(unix, 0)
        .single()
        .map(|time| time.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| unix.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Real captures from this machine. See tests/fixtures/README.md.
    const WITH_EARBUDS: &str = include_str!("../tests/fixtures/pw-dump-bluez.json");
    const WITHOUT_EARBUDS: &str = include_str!("../tests/fixtures/pw-dump-no-bluez.json");
    const WITH_STREAMS: &str = include_str!("../tests/fixtures/pw-dump-streams.json");
    const JOURNAL: &str = include_str!("../tests/fixtures/journal-bluetooth.jsonl");
    const LIVE: &str = include_str!("../tests/fixtures/pw-monitor-live.jsonl");

    const T: i64 = 1_700_000_000;

    fn dump(raw: &str) -> Vec<Value> {
        serde_json::from_str(raw).expect("a real pw-dump fixture")
    }

    fn object(objects: &[Value], id: u64) -> Value {
        objects
            .iter()
            .find(|object| object.get("id").and_then(Value::as_u64) == Some(id))
            .unwrap_or_else(|| panic!("no object {id} in the fixture"))
            .clone()
    }

    fn set(object: &mut Value, pointer: &str, value: Value) {
        *object
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("no {pointer}")) = value;
    }

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Store::open(&dir.path().join("incidents.db")).expect("open the store");
        (dir, store)
    }

    fn recorded(store: &Store) -> Vec<Incident> {
        store.since(0, 100).expect("read incidents")
    }

    fn only(rows: &[Incident], kind: &str) -> Incident {
        let matching: Vec<&Incident> = rows.iter().filter(|row| row.kind == kind).collect();
        assert_eq!(matching.len(), 1, "expected one {kind} row in {rows:#?}");
        matching[0].clone()
    }

    fn count(rows: &[Incident], kind: &str) -> usize {
        rows.iter().filter(|row| row.kind == kind).count()
    }

    fn all_journal() -> Vec<JournalLine> {
        JOURNAL
            .lines()
            .map(|raw| JournalLine::parse(raw).expect("parse a real journal line"))
            .collect()
    }

    fn relevant_journal() -> Vec<JournalLine> {
        all_journal()
            .into_iter()
            .filter(|line| line.relevant)
            .collect()
    }

    fn journal_line(needle: &str) -> JournalLine {
        relevant_journal()
            .into_iter()
            .find(|line| line.message.contains(needle))
            .unwrap_or_else(|| panic!("no relevant journal line with {needle:?}"))
    }

    fn journal_at(needle: &str, at_unix: i64) -> JournalLine {
        let mut line = journal_line(needle);
        line.at_unix = at_unix;
        line.cursor = Some(format!("cursor-{at_unix}"));
        line
    }

    fn baseline<'a>(store: &'a Store, raw: &str, at: i64) -> Observer<'a> {
        let mut observer = Observer::new(store);
        observer
            .on_monitor(&dump(raw), at, true)
            .expect("the baseline");
        observer
    }

    fn remove(observer: &mut Observer<'_>, id: u64, at: i64) {
        observer
            .on_monitor(&[json!({"id": id, "info": null})], at, false)
            .expect("apply the removal");
    }

    fn read_all(raw: &[u8]) -> Vec<Vec<Value>> {
        let mut reader = JsonArrayReader::new(raw);
        let mut arrays = Vec::new();
        while let Some(array) = reader.next_array().expect("read an array") {
            arrays.push(array);
        }
        arrays
    }

    struct Chunked<'a> {
        data: &'a [u8],
        chunk: usize,
    }

    impl Read for Chunked<'_> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let count = self.data.len().min(out.len()).min(self.chunk);
            out[..count].copy_from_slice(&self.data[..count]);
            self.data = &self.data[count..];
            Ok(count)
        }
    }

    #[test]
    fn reads_the_real_dump_as_one_array() {
        let arrays = read_all(WITH_EARBUDS.as_bytes());
        assert_eq!(arrays.len(), 1);
        assert_eq!(arrays[0].len(), 6);
    }

    #[test]
    fn reads_a_sequence_of_arrays_in_small_chunks() {
        let raw = format!("{WITH_EARBUDS}\n[{{\"id\":98,\"info\":null}}]\n");
        let mut reader = JsonArrayReader::new(Chunked {
            data: raw.as_bytes(),
            chunk: 3,
        });

        let first = reader.next_array().unwrap().expect("the full graph");
        let second = reader.next_array().unwrap().expect("the removal");
        assert_eq!(first.len(), 6);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0]["id"], json!(98));
        assert!(reader.next_array().unwrap().is_none());
    }

    #[test]
    fn brackets_inside_strings_do_not_end_an_array() {
        let raw = "[{\"id\":1,\"info\":{\"props\":{\"node.name\":\"a[}]\\\"b\"}}}]";
        let arrays = read_all(raw.as_bytes());
        assert_eq!(arrays.len(), 1);
        assert_eq!(arrays[0][0]["info"]["props"]["node.name"], json!("a[}]\"b"));
    }

    #[test]
    fn a_truncated_array_is_an_error() {
        let raw = &WITH_EARBUDS[..WITH_EARBUDS.len() - 3];
        let mut reader = JsonArrayReader::new(raw.as_bytes());
        assert!(reader.next_array().is_err());
    }

    #[test]
    fn a_value_that_is_not_an_array_is_an_error() {
        let mut reader = JsonArrayReader::new("{}".as_bytes());
        assert!(reader.next_array().is_err());
    }

    #[test]
    fn reads_real_journal_lines_and_their_kinds() {
        let all = all_journal();
        assert_eq!(all.len(), 9);
        assert_eq!(all.iter().filter(|line| line.relevant).count(), 8);

        let failure = all
            .iter()
            .find(|line| {
                line.at_unix == 1_794_830_708
                    && line
                        .message
                        .contains("Failure in Bluetooth audio transport")
            })
            .expect("the real transport failure");
        assert_eq!(failure.identifier, "wireplumber");
        assert_eq!(failure.incident_kind(), Some(kind::TRANSPORT_FAILURE));
        assert_eq!(
            failure.device().as_deref(),
            Some("bluez_card.1C_0A_E0_A3_E2_D0")
        );

        let sco = all
            .iter()
            .find(|line| line.message.contains("SCO packet"))
            .expect("a real SCO line");
        assert_eq!(sco.identifier, "kernel");
        assert_eq!(sco.incident_kind(), Some(kind::SCO_ORPHAN));
        assert_eq!(sco.device(), None);

        let ready = all
            .iter()
            .find(|line| line.message.contains("fd(29) ready"))
            .expect("a real bluetoothd line");
        assert_eq!(ready.identifier, "bluetoothd");
        assert_eq!(ready.incident_kind(), None);
        assert_eq!(
            ready.device().as_deref(),
            Some("bluez_card.1C_0A_E0_A3_E2_D0")
        );

        let usb = all
            .iter()
            .find(|line| line.message.contains("usb 3-1"))
            .expect("the kernel USB line");
        assert!(!usb.relevant);
    }

    #[test]
    fn a_non_json_journal_line_is_an_error() {
        assert!(JournalLine::parse("not json").is_err());
    }

    #[test]
    fn a_journal_message_may_be_a_byte_array() {
        // Synthetic: no real non-UTF-8 kernel line was available. journalctl
        // writes MESSAGE as an array of bytes when it is not valid UTF-8.
        let raw = concat!(
            "{\"__REALTIME_TIMESTAMP\":\"1794830708589459\",",
            "\"SYSLOG_IDENTIFIER\":\"kernel\",\"MESSAGE\":[",
            "66,108,117,101,116,111,111,116,104,58,32,104,99,105,48,58,32,",
            "83,67,79,32,112,97,99,107,101,116,32,102,111,114,32,117,110,107,110,111,119,110,32,",
            "99,111,110,110,101,99,116,105,111,110,32,104,97,110,100,108,101,32,50]}"
        );
        let line = JournalLine::parse(raw).expect("a relevant line");
        assert!(line.relevant);
        assert_eq!(
            line.message,
            "Bluetooth: hci0: SCO packet for unknown connection handle 2"
        );
        assert_eq!(line.incident_kind(), Some(kind::SCO_ORPHAN));
    }

    #[test]
    fn a_comm_fallback_keeps_a_wireplumber_line() {
        let raw = concat!(
            "{\"__REALTIME_TIMESTAMP\":\"1794830708589459\",",
            "\"_COMM\":\"wireplumber\",",
            "\"MESSAGE\":\"spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0\"}"
        );
        let line = JournalLine::parse(raw).expect("parse");
        assert_eq!(line.identifier, "wireplumber");
        assert!(line.relevant);
        assert_eq!(line.incident_kind(), Some(kind::TRANSPORT_FAILURE));
    }

    #[test]
    fn a_numeric_realtime_timestamp_is_accepted() {
        let raw = concat!(
            "{\"__REALTIME_TIMESTAMP\":1794830708589459,",
            "\"SYSLOG_IDENTIFIER\":\"kernel\",",
            "\"MESSAGE\":\"Bluetooth: hci0: SCO packet for unknown connection handle 2\"}"
        );
        let line = JournalLine::parse(raw).expect("parse");
        assert_eq!(line.at_unix, 1_794_830_708);
    }

    #[test]
    fn a_cursor_is_read_from_the_line() {
        let raw = concat!(
            "{\"__REALTIME_TIMESTAMP\":\"1794830708589459\",",
            "\"SYSLOG_IDENTIFIER\":\"kernel\",",
            "\"__CURSOR\":\"s=abc;i=42\",",
            "\"MESSAGE\":\"Bluetooth: hci0: SCO packet for unknown connection handle 2\"}"
        );
        let line = JournalLine::parse(raw).expect("parse");
        assert_eq!(line.cursor.as_deref(), Some("s=abc;i=42"));
    }

    #[test]
    fn the_first_dump_is_a_baseline_with_no_incidents() {
        let (_dir, store) = store();
        let mut observer = Observer::new(&store);
        observer.on_monitor(&dump(WITH_STREAMS), T, true).unwrap();

        assert!(recorded(&store).is_empty());
        assert_eq!(observer.cards.len(), 1);
        assert_eq!(
            observer.nodes.len(),
            4,
            "the sink, two output streams and one input stream"
        );
        assert_eq!(
            observer.default_sink.as_deref(),
            Some("bluez_output.1C_0A_E0_A3_E2_D0.1")
        );

        let levels = observer.volumes.get(&74).expect("Meiro");
        assert_eq!(levels.volumes, vec![0.199_520, 1.0]);
        assert_eq!(levels.map, ["FL", "FR"]);
        assert_eq!(
            db_against_right(levels),
            Some(20.0 * (0.199_520_f64).log10())
        );

        let meiro = observer.nodes.get(&74).expect("Meiro");
        assert_eq!(observer.earbud_sink_for(meiro), Some(94));
        let sober = observer.nodes.get(&95).expect("Sober");
        assert_eq!(observer.earbud_sink_for(sober), Some(94));
    }

    #[test]
    fn a_full_snapshot_after_a_restart_reconciles_the_graph() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        // A PipeWire restart delivers a full snapshot with no Bluetooth card.
        observer
            .on_monitor(&dump(WITHOUT_EARBUDS), T + 5, true)
            .unwrap();
        assert!(observer.cards.is_empty());
        assert!(recorded(&store).is_empty());

        // The next full snapshot has the card again.
        observer
            .on_monitor(&dump(WITH_EARBUDS), T + 10, true)
            .unwrap();
        assert_eq!(observer.cards.len(), 1);
        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_normal_disconnect_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_monitor(
                &[
                    json!({"id": 98, "info": null}),
                    json!({"id": 94, "info": null}),
                ],
                T + 5,
                false,
            )
            .unwrap();

        assert!(recorded(&store).is_empty());
        assert!(observer.cards.is_empty());
    }

    #[test]
    fn a_card_without_the_connection_prop_is_handled() {
        let (_dir, store) = store();
        let mut objects = dump(WITH_EARBUDS);
        let card = objects
            .iter_mut()
            .find(|object| object["id"] == json!(98))
            .expect("the card");
        card["info"]["props"]
            .as_object_mut()
            .expect("props")
            .remove("api.bluez5.connection");

        let mut observer = Observer::new(&store);
        observer.on_monitor(&objects, T, true).unwrap();
        assert_eq!(observer.cards.len(), 1);

        observer
            .on_monitor(&[json!({"id": 98, "info": null})], T + 5, false)
            .unwrap();

        assert!(observer.cards.is_empty());
        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_connection_change_then_a_removal_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/props/api.bluez5.connection",
            json!("disconnected"),
        );
        observer.on_monitor(&[card], T + 5, false).unwrap();
        remove(&mut observer, 98, T + 6);

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn replays_the_real_disconnect_and_reconnect() {
        let (_dir, store) = store();
        let mut observer = Observer::new(&store);

        for (index, line) in LIVE.lines().enumerate() {
            let array: Vec<Value> = serde_json::from_str(line).expect("a real monitor array");
            observer
                .on_monitor(&array, T + index as i64, index == 0)
                .expect("apply a real monitor array");
        }
        observer.flush_pending(T + 200).unwrap();

        assert!(
            recorded(&store).is_empty(),
            "a normal disconnect and reconnect must make no rows"
        );
    }

    #[test]
    fn a_teardown_transport_line_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 5))
            .unwrap();
        remove(&mut observer, 98, T + 6);
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_transport_line_after_the_card_is_gone_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        remove(&mut observer, 98, T + 5);

        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 6))
            .unwrap();
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_transport_line_before_a_normal_disconnect_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 5))
            .unwrap();
        remove(&mut observer, 98, T + 6);
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_transport_failure_while_connected_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 5))
            .unwrap();
        observer.flush_pending(T + 6).unwrap();
        assert!(recorded(&store).is_empty(), "the window is still open");

        observer.flush_pending(T + 7).unwrap();
        let rows = recorded(&store);
        let row = only(&rows, kind::TRANSPORT_FAILURE);
        assert_eq!(row.at_unix, T + 5);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.profile_after.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert!(
            row.journal
                .as_deref()
                .unwrap()
                .contains("Failure in Bluetooth audio transport")
        );
    }

    #[test]
    fn a_transport_burst_across_a_second_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 5))
            .unwrap();
        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 6))
            .unwrap();
        observer.flush_pending(T + 8).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::TRANSPORT_FAILURE);
        assert_eq!(
            row.journal
                .as_deref()
                .unwrap()
                .matches("Failure in Bluetooth audio transport")
                .count(),
            2
        );
    }

    #[test]
    fn a_sco_line_on_teardown_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer
            .on_journal(journal_at(
                "SCO packet for unknown connection handle",
                T + 5,
            ))
            .unwrap();
        remove(&mut observer, 98, T + 6);
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_sco_line_while_connected_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_journal(journal_at(
                "SCO packet for unknown connection handle",
                T + 5,
            ))
            .unwrap();
        observer.flush_pending(T + 7).unwrap();

        let row = only(&recorded(&store), kind::SCO_ORPHAN);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
    }

    #[test]
    fn journal_context_within_one_second_is_attached() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer
            .on_journal(journal_at("terminated unexpectedly", T + 6))
            .unwrap();

        let row = only(&recorded(&store), kind::PROFILE_CHANGE);
        assert!(
            row.journal
                .as_deref()
                .unwrap()
                .contains("terminated unexpectedly")
        );
    }

    #[test]
    fn journal_context_two_seconds_later_is_not_attached() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer
            .on_journal(journal_at("terminated unexpectedly", T + 7))
            .unwrap();

        let row = only(&recorded(&store), kind::PROFILE_CHANGE);
        assert_eq!(row.journal, None);
    }

    #[test]
    fn a_transport_line_after_a_profile_change_is_folded() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 6))
            .unwrap();
        observer.flush_pending(T + 8).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(count(&rows, kind::TRANSPORT_FAILURE), 0);
        assert!(
            row.journal
                .as_deref()
                .unwrap()
                .contains("Failure in Bluetooth audio transport")
        );
    }

    #[test]
    fn a_transport_line_before_a_profile_change_is_folded() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 5))
            .unwrap();

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        observer.on_monitor(&[card], T + 6, false).unwrap();
        observer.flush_pending(T + 8).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(count(&rows, kind::TRANSPORT_FAILURE), 0);
        assert!(
            row.journal
                .as_deref()
                .unwrap()
                .contains("Failure in Bluetooth audio transport")
        );
    }

    #[test]
    fn one_aac_selection_in_two_arrays_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("a2dp-sink-aac"),
        );
        observer.on_monitor(&[card], T + 5, false).unwrap();

        let mut sink = object(&objects, 94);
        set(&mut sink, "/info/props/api.bluez5.codec", json!("aac"));
        observer.on_monitor(&[sink], T + 6, false).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(rows.len(), 1, "rows: {rows:#?}");
        assert_eq!(row.profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.profile_after.as_deref(), Some("a2dp-sink-aac"));
        assert_eq!(row.codec.as_deref(), Some("aac"));
    }

    #[test]
    fn an_aac_codec_row_folds_into_the_profile_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut sink = object(&objects, 94);
        set(&mut sink, "/info/props/api.bluez5.codec", json!("aac"));
        observer.on_monitor(&[sink], T + 5, false).unwrap();

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("a2dp-sink-aac"),
        );
        observer.on_monitor(&[card], T + 6, false).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(rows.len(), 1, "rows: {rows:#?}");
        assert_eq!(row.profile_after.as_deref(), Some("a2dp-sink-aac"));
        assert_eq!(row.codec.as_deref(), Some("aac"));
    }

    #[test]
    fn a_standalone_codec_change_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut sink = object(&objects, 94);
        set(&mut sink, "/info/props/api.bluez5.codec", json!("aac"));
        observer.on_monitor(&[sink], T + 5, false).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::CODEC_CHANGE);
        assert_eq!(row.codec.as_deref(), Some("aac"));
        assert_eq!(row.profile_after.as_deref(), Some("a2dp-sink-sbc"));
    }

    #[test]
    fn a_profile_and_codec_in_one_array_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("a2dp-sink-aac"),
        );
        let mut sink = object(&objects, 94);
        set(&mut sink, "/info/props/api.bluez5.codec", json!("aac"));
        observer.on_monitor(&[card, sink], T + 5, false).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(count(&rows, kind::CODEC_CHANGE), 0);
        assert_eq!(row.codec.as_deref(), Some("aac"));
    }

    #[test]
    fn off_while_connected_is_a_row_after_the_window() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();
        assert!(recorded(&store).is_empty(), "the teardown window is open");

        observer.flush_pending(T + 7).unwrap();
        let row = only(&recorded(&store), kind::PROFILE_CHANGE);
        assert_eq!(row.profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.profile_after.as_deref(), Some("off"));
    }

    #[test]
    fn off_then_disconnect_is_not_a_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();
        remove(&mut observer, 98, T + 6);
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn off_then_back_on_is_not_a_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer
            .on_monitor(&[object(&objects, 98)], T + 6, false)
            .unwrap();
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn off_while_connected_without_the_connection_prop_is_a_row() {
        let (_dir, store) = store();
        let mut objects = dump(WITH_EARBUDS);
        let card = objects
            .iter_mut()
            .find(|object| object["id"] == json!(98))
            .expect("the card");
        card["info"]["props"]
            .as_object_mut()
            .expect("props")
            .remove("api.bluez5.connection");

        let mut observer = Observer::new(&store);
        observer.on_monitor(&objects, T, true).unwrap();

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();
        observer.flush_pending(T + 7).unwrap();

        let row = only(&recorded(&store), kind::PROFILE_CHANGE);
        assert_eq!(row.profile_after.as_deref(), Some("off"));
    }

    #[test]
    fn a_lost_balance_correction_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        let objects = dump(WITH_STREAMS);

        let mut meiro = object(&objects, 74);
        set(
            &mut meiro,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[meiro.clone()], T + 5, false).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::BALANCE_DRIFT);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert!(row.detail.as_deref().unwrap().contains("was lost"));

        observer.on_monitor(&[meiro], T + 6, false).unwrap();
        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 1);
    }

    #[test]
    fn a_balance_change_during_a_teardown_records_nothing() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        let objects = dump(WITH_STREAMS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer.on_monitor(&[card], T + 5, false).unwrap();

        let mut meiro = object(&objects, 74);
        set(
            &mut meiro,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[meiro], T + 5, false).unwrap();
        remove(&mut observer, 98, T + 6);
        observer.flush_pending(T + 8).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_partial_balance_loss_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        let objects = dump(WITH_STREAMS);

        let mut meiro = object(&objects, 74);
        set(
            &mut meiro,
            "/info/params/Props/0/channelVolumes",
            json!([0.708, 1.0]),
        );
        observer.on_monitor(&[meiro], T + 5, false).unwrap();

        let row = only(&recorded(&store), kind::BALANCE_DRIFT);
        assert!(row.detail.as_deref().unwrap().contains("weakened"));
    }

    #[test]
    fn a_small_balance_change_is_not_a_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        let objects = dump(WITH_STREAMS);

        let mut meiro = object(&objects, 74);
        set(
            &mut meiro,
            "/info/params/Props/0/channelVolumes",
            json!([0.251, 1.0]),
        );
        observer.on_monitor(&[meiro], T + 5, false).unwrap();

        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 0);
    }

    #[test]
    fn a_new_stream_that_starts_flat_is_not_a_drift() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        let objects = dump(WITH_STREAMS);

        let mut stream = object(&objects, 74);
        stream["id"] = json!(200);
        set(&mut stream, "/info/props/node.name", json!("new-app"));
        stream["info"]["props"]["target.object"] = json!("bluez_output.1C_0A_E0_A3_E2_D0.1");
        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([]),
        );
        observer
            .on_monitor(&[stream.clone()], T + 5, false)
            .unwrap();

        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer
            .on_monitor(&[stream.clone()], T + 6, false)
            .unwrap();

        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([0.19952, 1.0]),
        );
        observer.on_monitor(&[stream], T + 7, false).unwrap();

        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 0);
    }

    #[test]
    fn a_stream_on_another_sink_is_not_checked() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        let objects = dump(WITH_STREAMS);

        let mut stream = object(&objects, 74);
        stream["id"] = json!(201);
        set(&mut stream, "/info/props/node.name", json!("speakers-app"));
        stream["info"]["props"]["target.object"] =
            json!("alsa_output.pci-0000_00_1f.3.analog-stereo");
        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([0.19952, 1.0]),
        );
        observer
            .on_monitor(&[stream.clone()], T + 5, false)
            .unwrap();

        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[stream], T + 6, false).unwrap();

        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 0);
    }

    #[test]
    fn a_reversed_channel_map_is_handled() {
        let (_dir, store) = store();
        let mut objects = dump(WITH_STREAMS);
        let meiro = objects
            .iter_mut()
            .find(|object| object["id"] == json!(74))
            .expect("Meiro");
        set(
            meiro,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 0.19952]),
        );
        set(
            meiro,
            "/info/params/Props/0/channelMap",
            json!(["FR", "FL"]),
        );

        let mut observer = Observer::new(&store);
        observer.on_monitor(&objects, T, true).unwrap();

        let mut meiro = object(&objects, 74);
        set(
            &mut meiro,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[meiro], T + 5, false).unwrap();

        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 1);
    }

    #[test]
    fn a_baseline_with_a_flat_stream_records_nothing() {
        let (_dir, store) = store();
        let mut objects = dump(WITH_STREAMS);
        let meiro = objects
            .iter_mut()
            .find(|object| object["id"] == json!(74))
            .expect("Meiro");
        set(
            meiro,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );

        let mut observer = Observer::new(&store);
        observer.on_monitor(&objects, T, true).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn a_flushed_transport_survives_a_later_disconnect() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_journal(journal_at("Failure in Bluetooth audio transport", T + 5))
            .unwrap();
        observer.flush_pending(T + 7).unwrap();
        remove(&mut observer, 98, T + 8);

        let rows = recorded(&store);
        assert_eq!(rows.len(), 1, "rows: {rows:#?}");
        assert_eq!(count(&rows, kind::TRANSPORT_FAILURE), 1);
    }

    #[test]
    fn a_second_watch_lock_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("watch.lock");

        let first = WatchLock::acquire(&path).expect("the first lock");
        let error = WatchLock::acquire(&path).expect_err("the second lock");
        assert!(error.to_string().contains("already running"), "{error:#}");

        drop(first);
        WatchLock::acquire(&path).expect("the lock is free again");
    }

    #[test]
    fn a_journal_cursor_round_trips() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("journal.cursor");
        let mut cursor = CursorFile::new(path.clone());
        assert_eq!(cursor.read().unwrap(), None);

        let line = journal_at("Failure in Bluetooth audio transport", T + 5);
        cursor.save_for(&line).unwrap();
        assert_eq!(cursor.read().unwrap().as_deref(), Some("cursor-1700000005"));
        assert!(!path.with_extension("cursor.tmp").exists());
    }

    #[test]
    fn journal_args_resumes_from_a_cursor() {
        let tail = journal_args(None);
        assert!(tail.iter().any(|arg| arg == "-n"));
        assert!(tail.iter().any(|arg| arg == "0"));
        assert!(!tail.iter().any(|arg| arg.starts_with("--after-cursor=")));

        let resume = journal_args(Some("s=abc;i=42"));
        assert!(resume.iter().any(|arg| arg == "--after-cursor=s=abc;i=42"));
        assert!(!resume.iter().any(|arg| arg == "-n"));
    }
}
