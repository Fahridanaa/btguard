//! Watch PipeWire and the journal, and record incidents.
//!
//! Two child processes are the only inputs: `pw-dump --monitor` for the graph
//! and `journalctl --follow` for Bluetooth lines. Nothing here changes audio
//! state. Every row comes from one of those two sources.

use std::collections::{HashMap, VecDeque};
use std::io::{self, BufRead, BufReader, Read};
use std::process::{ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::paths;
use crate::pw::{self, Card, Node};
use crate::store::{Incident, Store, kind};

/// How long a journal line stays available for same-second correlation.
const CONTEXT_SECONDS: i64 = 10;
/// Cap on buffered journal lines, in case the journal is noisy.
const CONTEXT_LINES: usize = 500;
/// A stream that is at least this far below the right channel is corrected.
/// earbud-balance uses 14 dB; -6 dB is wide enough to catch any real fix.
const CORRECTED_DB: f64 = -6.0;
/// A stream that is above this level against the right channel is not
/// corrected. This covers a flat split and a left-dominant split.
const FLAT_DB: f64 = -1.0;

pub fn watch() -> Result<()> {
    let store = Store::open_default()?;
    eprintln!("btguard watch: database {}", paths::db_path()?.display());

    let (sender, receiver) = mpsc::channel();

    let mut monitor = Command::new("pw-dump")
        .arg("--monitor")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("start pw-dump --monitor; is PipeWire installed?")?;
    let monitor_out = monitor.stdout.take().context("pw-dump wrote no stdout")?;
    spawn_monitor_reader(monitor_out, sender.clone());

    let mut journal = match Command::new("journalctl")
        .args(["--follow", "--output=json", "--no-pager", "-n", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            let _ = monitor.kill();
            let _ = monitor.wait();
            return Err(error).context("start journalctl --follow");
        }
    };
    let journal_out = journal
        .stdout
        .take()
        .context("journalctl wrote no stdout")?;
    spawn_journal_reader(journal_out, sender.clone());
    drop(sender);

    eprintln!("btguard watch: watching pw-dump --monitor and journalctl -f");

    let mut observer = Observer::new(&store);
    let outcome = loop {
        match receiver.recv() {
            Ok(Event::Monitor(objects)) => {
                if let Err(error) = observer.on_monitor(&objects, now_unix()) {
                    break Err(error);
                }
            }
            Ok(Event::Journal(line)) => {
                if let Err(error) = observer.on_journal(line) {
                    break Err(error);
                }
            }
            Ok(Event::Stopped { source, error }) => {
                break Err(match error {
                    Some(error) => anyhow::anyhow!("{source} stopped: {error}"),
                    None => anyhow::anyhow!("{source} stopped"),
                });
            }
            Err(_) => break Err(anyhow::anyhow!("both input readers stopped")),
        }
    };

    // Stop the child that is still running. systemd also kills the cgroup.
    let _ = monitor.kill();
    let _ = monitor.wait();
    let _ = journal.kill();
    let _ = journal.wait();

    outcome
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

enum Event {
    Monitor(Vec<Value>),
    Journal(JournalLine),
    Stopped {
        source: &'static str,
        error: Option<String>,
    },
}

fn spawn_monitor_reader(stdout: ChildStdout, sender: Sender<Event>) {
    thread::spawn(move || {
        let mut reader = JsonArrayReader::new(stdout);
        loop {
            match reader.next_array() {
                Ok(Some(objects)) => {
                    if sender.send(Event::Monitor(objects)).is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    let _ = sender.send(Event::Stopped {
                        source: "pw-dump",
                        error: None,
                    });
                    return;
                }
                Err(error) => {
                    let _ = sender.send(Event::Stopped {
                        source: "pw-dump",
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
                        source: "journalctl",
                        error: Some(error.to_string()),
                    });
                    return;
                }
            };
            if raw.is_empty() {
                continue;
            }
            match JournalLine::parse(&raw) {
                Ok(Some(line)) => {
                    if sender.send(Event::Journal(line)).is_err() {
                        return;
                    }
                }
                Ok(None) => {}
                Err(error) => eprintln!("btguard watch: skip a journal line: {error:#}"),
            }
        }

        let _ = sender.send(Event::Stopped {
            source: "journalctl",
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

/// One Bluetooth-relevant line from `journalctl --output=json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalLine {
    pub at_unix: i64,
    pub identifier: String,
    pub message: String,
}

impl JournalLine {
    fn parse(raw: &str) -> Result<Option<Self>> {
        let value: Value = serde_json::from_str(raw).context("journalctl did not write JSON")?;
        let message = message_text(&value);
        let identifier = identifier_text(&value);

        if !is_bluetooth_relevant(&identifier, &message) {
            return Ok(None);
        }

        Ok(Some(Self {
            at_unix: timestamp_seconds(&value).unwrap_or_else(now_unix),
            identifier,
            message,
        }))
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
/// is a WirePlumber line (`spa.bluez5:`), not a bluetoothd line, so that
/// identifier must be included too.
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

struct CardContext {
    device: Option<String>,
    profile: Option<String>,
    codec: Option<String>,
}

pub struct Observer<'a> {
    store: &'a Store,
    cards: HashMap<u64, Card>,
    nodes: HashMap<u64, Node>,
    links: HashMap<u64, Link>,
    default_sink: Option<String>,
    /// Relevant journal lines, newest last, pruned to `CONTEXT_SECONDS`.
    journal: VecDeque<(i64, String)>,
    /// Incident ids recorded at each second, for journal backfill.
    recent: HashMap<i64, Vec<i64>>,
    /// One journal-driven row per kind per second, so a burst is one row.
    journal_rows: HashMap<(i64, &'static str), i64>,
    /// Per-stream channel volumes from the previous update.
    volumes: HashMap<u64, Vec<f64>>,
    /// The last usable profile per card. `off` is teardown, not audio.
    last_profiles: HashMap<u64, String>,
    /// The last codec per card. A removed sink must not erase it.
    last_codecs: HashMap<u64, String>,
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
            recent: HashMap::new(),
            journal_rows: HashMap::new(),
            volumes: HashMap::new(),
            last_profiles: HashMap::new(),
            last_codecs: HashMap::new(),
            initialized: false,
        }
    }

    /// Apply one array from `pw-dump --monitor`. The first array is the whole
    /// graph and only sets the baseline; it must not record incidents.
    pub fn on_monitor(&mut self, objects: &[Value], at_unix: i64) -> Result<()> {
        let before = self.card_states();
        let before_sink = self.default_sink.clone();
        let before_volumes = self.volumes.clone();

        for object in objects {
            self.apply(object);
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

    /// Apply one journal line. A transport failure or an orphan SCO packet
    /// records a row; every other relevant line is context for the same second.
    pub fn on_journal(&mut self, line: JournalLine) -> Result<()> {
        self.push_journal(&line);

        let Some(incident_kind) = line.incident_kind() else {
            self.append_to_recent(line.at_unix, &line.message, None)?;
            self.prune(line.at_unix);
            return Ok(());
        };

        let existing = self
            .journal_rows
            .get(&(line.at_unix, incident_kind))
            .copied();
        let row_id = match existing {
            Some(id) => {
                self.store.append_journal(id, &line.message)?;
                id
            }
            None => {
                let context = self.card_context(line.device());
                let journal = self.context(line.at_unix);
                let id = self.record(Incident {
                    at_unix: line.at_unix,
                    kind: incident_kind.to_owned(),
                    device: context.device,
                    profile_before: None,
                    profile_after: context.profile,
                    codec: context.codec,
                    detail: None,
                    journal,
                })?;
                self.journal_rows.insert((line.at_unix, incident_kind), id);
                eprintln!(
                    "btguard watch: {incident_kind} from {} at {}",
                    line.identifier,
                    local_time(line.at_unix)
                );
                id
            }
        };

        // The row above already has this line. Add it to the other incidents
        // of the same second, such as a profile change.
        self.append_to_recent(line.at_unix, &line.message, Some(row_id))?;
        self.prune(line.at_unix);
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
                        connection: card.connection.clone(),
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
            let name = new
                .map(|state| state.name.clone())
                .unwrap_or_else(|| old.name.clone());

            let disconnected = match new {
                Some(new) => {
                    old.connection.as_deref() == Some("connected")
                        && new.connection.as_deref() == Some("disconnected")
                }
                None => old.connection.as_deref() == Some("connected"),
            };

            if disconnected {
                let journal = self.context(at_unix);
                self.record(Incident {
                    at_unix,
                    kind: kind::DISCONNECT.to_owned(),
                    device: Some(old.name.clone()),
                    profile_before: old.profile.clone(),
                    profile_after: None,
                    codec: old.codec.clone(),
                    detail: Some("Bluetooth card disconnected".to_owned()),
                    journal,
                })?;
                eprintln!("btguard watch: disconnected {}", old.name);
                continue;
            }

            let Some(new) = new else {
                continue;
            };

            // `off` is the card's teardown state, not an audio profile. A
            // disconnect and a reconnect pass through it; the disconnect row
            // and the connect log already describe those transitions.
            if old.profile != new.profile
                && old.profile.as_deref() != Some("off")
                && new.profile.as_deref() != Some("off")
            {
                let journal = self.context(at_unix);
                self.record(Incident {
                    at_unix,
                    kind: kind::PROFILE_CHANGE.to_owned(),
                    device: Some(name.clone()),
                    profile_before: old.profile.clone(),
                    profile_after: new.profile.clone(),
                    codec: new.codec.clone(),
                    detail: None,
                    journal,
                })?;
                eprintln!(
                    "btguard watch: profile {}: {} -> {}",
                    name,
                    old.profile.as_deref().unwrap_or("unknown"),
                    new.profile.as_deref().unwrap_or("unknown")
                );
                continue;
            }

            if old.codec.is_some() && new.codec.is_some() && old.codec != new.codec {
                let journal = self.context(at_unix);
                self.record(Incident {
                    at_unix,
                    kind: kind::CODEC_CHANGE.to_owned(),
                    device: Some(name.clone()),
                    profile_before: None,
                    profile_after: new.profile.clone(),
                    codec: new.codec.clone(),
                    detail: Some(format!(
                        "codec {} -> {}",
                        old.codec.as_deref().unwrap_or("unknown"),
                        new.codec.as_deref().unwrap_or("unknown")
                    )),
                    journal,
                })?;
                eprintln!(
                    "btguard watch: codec {}: {} -> {}",
                    name,
                    old.codec.as_deref().unwrap_or("unknown"),
                    new.codec.as_deref().unwrap_or("unknown")
                );
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

    fn compare_streams(
        &mut self,
        before: &HashMap<u64, Vec<f64>>,
        at_unix: i64,
        baseline: bool,
    ) -> Result<()> {
        let nodes: Vec<Node> = self.nodes.values().cloned().collect();
        let mut next = HashMap::new();

        for node in &nodes {
            if node.media_class != "Stream/Output/Audio" {
                continue;
            }

            let volumes = node.channel_volumes.clone();
            let sink = self.earbud_sink_for(node);

            if !baseline
                && let Some(sink) = sink
                && let Some(old) = before.get(&node.id)
                && old.len() >= 2
                && volumes.len() >= 2
                && db_against_right(old) < CORRECTED_DB
                && db_against_right(&volumes) > FLAT_DB
            {
                let card = self
                    .nodes
                    .get(&sink)
                    .and_then(|node| node.device_id)
                    .and_then(|card_id| self.cards.get(&card_id));
                let device = card.map(|card| card.name.clone());
                let profile = card.and_then(|card| card.profile.clone());
                let codec = card.and_then(|card| self.card_codec(card));
                let journal = self.context(at_unix);
                self.record(Incident {
                    at_unix,
                    kind: kind::BALANCE_DRIFT.to_owned(),
                    device,
                    profile_before: None,
                    profile_after: profile,
                    codec,
                    detail: Some(format!(
                        "stream {}: left {:.3}, right {:.3} ({:.1} dB against right); the balance correction was lost",
                        node.name,
                        volumes[0],
                        volumes[1],
                        db_against_right(&volumes)
                    )),
                    journal,
                })?;
                eprintln!(
                    "btguard watch: balance drift on {}: left {:.3}, right {:.3}",
                    node.name, volumes[0], volumes[1]
                );
            }

            next.insert(node.id, volumes);
        }

        self.volumes = next;
        Ok(())
    }

    /// The Bluetooth card that owns the sink a stream feeds, if it is the
    /// earbuds. A stream names its target directly or is connected by a Link.
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

    fn card_context(&self, device: Option<String>) -> CardContext {
        let card = device
            .as_deref()
            .and_then(|name| self.cards.values().find(|card| card.name == name))
            .or_else(|| {
                let mut cards = self.cards.values();
                let only = cards.next();
                if cards.next().is_none() { only } else { None }
            });

        CardContext {
            device: device.or_else(|| card.map(|card| card.name.clone())),
            profile: card.and_then(|card| card.profile.clone()),
            codec: card.and_then(|card| self.card_codec(card)),
        }
    }

    fn push_journal(&mut self, line: &JournalLine) {
        self.journal.push_back((line.at_unix, line.message.clone()));
        while let Some((second, _)) = self.journal.front() {
            if *second < line.at_unix - CONTEXT_SECONDS {
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
            .filter(|(second, _)| *second == at_unix)
            .map(|(_, line)| line.as_str())
            .collect();
        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    }

    fn append_to_recent(&self, at_unix: i64, message: &str, skip: Option<i64>) -> Result<()> {
        for id in self.recent.get(&at_unix).cloned().unwrap_or_default() {
            if Some(id) == skip {
                continue;
            }
            self.store.append_journal(id, message)?;
        }
        Ok(())
    }

    fn record(&mut self, incident: Incident) -> Result<i64> {
        let id = self.store.insert(&incident)?;
        self.recent.entry(incident.at_unix).or_default().push(id);
        Ok(id)
    }

    fn prune(&mut self, at_unix: i64) {
        let cutoff = at_unix - CONTEXT_SECONDS;
        self.recent.retain(|second, _| *second >= cutoff);
        self.journal_rows.retain(|(second, _), _| *second >= cutoff);
    }
}

fn db_against_right(volumes: &[f64]) -> f64 {
    let left = volumes[0].max(1e-9);
    let right = volumes[1].max(1e-9);
    20.0 * (left / right).log10()
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

    fn only<'a>(rows: &'a [Incident], kind: &str) -> &'a Incident {
        let matching: Vec<&Incident> = rows.iter().filter(|row| row.kind == kind).collect();
        assert_eq!(matching.len(), 1, "expected one {kind} row in {rows:#?}");
        matching[0]
    }

    fn count(rows: &[Incident], kind: &str) -> usize {
        rows.iter().filter(|row| row.kind == kind).count()
    }

    fn relevant_journal() -> Vec<JournalLine> {
        JOURNAL
            .lines()
            .filter_map(|raw| JournalLine::parse(raw).expect("parse a real journal line"))
            .collect()
    }

    fn journal_line(needle: &str) -> JournalLine {
        relevant_journal()
            .into_iter()
            .find(|line| line.message.contains(needle))
            .unwrap_or_else(|| panic!("no journal line with {needle:?}"))
    }

    fn baseline<'a>(store: &'a Store, raw: &str, at: i64) -> Observer<'a> {
        let mut observer = Observer::new(store);
        observer.on_monitor(&dump(raw), at).expect("the baseline");
        observer
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
        let lines = relevant_journal();
        assert_eq!(lines.len(), 8, "the USB line is not Bluetooth");

        let failure = lines
            .iter()
            .find(|line| {
                line.message
                    .contains("Failure in Bluetooth audio transport")
                    && line.at_unix == 1_794_830_708
            })
            .expect("a real transport failure");
        assert_eq!(failure.identifier, "wireplumber");
        assert_eq!(failure.at_unix, 1_794_830_708);
        assert_eq!(failure.incident_kind(), Some(kind::TRANSPORT_FAILURE));
        assert_eq!(
            failure.device().as_deref(),
            Some("bluez_card.1C_0A_E0_A3_E2_D0")
        );

        let sco = lines
            .iter()
            .find(|line| line.message.contains("SCO packet"))
            .expect("a real SCO line");
        assert_eq!(sco.identifier, "kernel");
        assert_eq!(sco.incident_kind(), Some(kind::SCO_ORPHAN));
        assert_eq!(sco.device(), None);

        let ready = lines
            .iter()
            .find(|line| line.message.contains("fd(29) ready"))
            .expect("a real bluetoothd line");
        assert_eq!(ready.identifier, "bluetoothd");
        assert_eq!(ready.incident_kind(), None);
        assert_eq!(
            ready.device().as_deref(),
            Some("bluez_card.1C_0A_E0_A3_E2_D0")
        );

        let usb = JOURNAL
            .lines()
            .find(|raw| raw.contains("usb 3-1"))
            .expect("the kernel USB line");
        assert_eq!(JournalLine::parse(usb).unwrap(), None);
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
        let line = JournalLine::parse(raw).unwrap().expect("a relevant line");
        assert_eq!(
            line.message,
            "Bluetooth: hci0: SCO packet for unknown connection handle 2"
        );
        assert_eq!(line.incident_kind(), Some(kind::SCO_ORPHAN));
    }

    #[test]
    fn the_first_dump_is_a_baseline_with_no_incidents() {
        let (_dir, store) = store();
        let mut observer = Observer::new(&store);
        observer.on_monitor(&dump(WITH_STREAMS), T).unwrap();

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
        assert_eq!(
            observer.volumes.get(&74).map(Vec::as_slice),
            Some(&[0.199520, 1.0][..])
        );
        assert_eq!(
            observer.volumes.get(&95).map(Vec::as_slice),
            Some(&[0.199520, 1.0][..])
        );
        assert!(
            !observer.volumes.contains_key(&88),
            "an input stream has no playback balance"
        );

        let meiro = observer.nodes.get(&74).expect("Meiro, mapped by a Link");
        assert_eq!(observer.earbud_sink_for(meiro), Some(94));
        let sober = observer
            .nodes
            .get(&95)
            .expect("Sober, mapped by target.object");
        assert_eq!(observer.earbud_sink_for(sober), Some(94));
    }

    #[test]
    fn a_profile_change_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        set(
            &mut card,
            "/info/params/Profile/0/description",
            json!("Headset Head Unit (HFP)"),
        );

        observer.on_monitor(&[card], T + 5).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(row.at_unix, T + 5);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.profile_after.as_deref(), Some("headset-head-unit"));
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert_eq!(count(&rows, kind::CODEC_CHANGE), 0);
    }

    #[test]
    fn a_profile_change_and_its_codec_change_are_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        let mut sink = object(&objects, 94);
        set(&mut sink, "/info/props/api.bluez5.codec", json!("cvsd"));

        observer.on_monitor(&[card, sink], T + 5).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(row.profile_after.as_deref(), Some("headset-head-unit"));
        assert_eq!(row.codec.as_deref(), Some("cvsd"));
        assert_eq!(count(&rows, kind::CODEC_CHANGE), 0);
    }

    #[test]
    fn a_codec_change_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut sink = object(&objects, 94);
        set(&mut sink, "/info/props/api.bluez5.codec", json!("ldac"));

        observer.on_monitor(&[sink], T + 5).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::CODEC_CHANGE);
        assert_eq!(row.at_unix, T + 5);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.profile_after.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.codec.as_deref(), Some("ldac"));
        assert_eq!(row.detail.as_deref(), Some("codec sbc -> ldac"));
    }

    #[test]
    fn a_sink_that_disappears_is_not_a_codec_change() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_monitor(&[json!({"id": 94, "info": null})], T + 5)
            .unwrap();

        assert_eq!(count(&recorded(&store), kind::CODEC_CHANGE), 0);
        assert_eq!(count(&recorded(&store), kind::PROFILE_CHANGE), 0);
    }

    #[test]
    fn a_teardown_reports_the_last_working_profile() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(&mut card, "/info/params/Profile/0/name", json!("off"));
        observer
            .on_monitor(&[json!({"id": 94, "info": null}), card], T + 5)
            .unwrap();
        assert_eq!(
            recorded(&store).len(),
            0,
            "the sink removal and the off profile are teardown, not incidents"
        );

        observer
            .on_monitor(&[json!({"id": 98, "info": null})], T + 6)
            .unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::DISCONNECT);
        assert_eq!(row.profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert_eq!(count(&rows, kind::PROFILE_CHANGE), 0);
        assert_eq!(count(&rows, kind::CODEC_CHANGE), 0);
    }

    #[test]
    fn a_reconnect_does_not_invent_rows() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        // The real sequence: sink removed, card profile off, card removed.
        let mut card_off = object(&objects, 98);
        set(&mut card_off, "/info/params/Profile/0/name", json!("off"));
        observer
            .on_monitor(&[json!({"id": 94, "info": null}), card_off.clone()], T + 5)
            .unwrap();
        observer
            .on_monitor(&[json!({"id": 98, "info": null})], T + 6)
            .unwrap();

        // Then the card comes back off, and settles on A2DP with a new sink.
        observer.on_monitor(&[card_off], T + 7).unwrap();
        observer
            .on_monitor(&[object(&objects, 98), object(&objects, 94)], T + 8)
            .unwrap();

        let rows = recorded(&store);
        assert_eq!(count(&rows, kind::DISCONNECT), 1);
        assert_eq!(count(&rows, kind::PROFILE_CHANGE), 0);
        assert_eq!(count(&rows, kind::CODEC_CHANGE), 0);
    }

    #[test]
    fn a_removed_card_is_one_disconnect_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        observer
            .on_monitor(
                &[
                    json!({"id": 98, "info": null}),
                    json!({"id": 94, "info": null}),
                ],
                T + 5,
            )
            .unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::DISCONNECT);
        assert_eq!(row.at_unix, T + 5);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.profile_after, None);
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert!(observer.cards.is_empty());

        observer
            .on_monitor(&[json!({"id": 98, "info": null})], T + 6)
            .unwrap();
        assert_eq!(count(&recorded(&store), kind::DISCONNECT), 1);
    }

    #[test]
    fn a_connection_change_then_a_removal_is_one_disconnect_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/props/api.bluez5.connection",
            json!("disconnected"),
        );
        observer.on_monitor(&[card], T + 5).unwrap();
        assert_eq!(count(&recorded(&store), kind::DISCONNECT), 1);

        observer
            .on_monitor(&[json!({"id": 98, "info": null})], T + 6)
            .unwrap();
        assert_eq!(count(&recorded(&store), kind::DISCONNECT), 1);
    }

    #[test]
    fn a_disconnect_carries_the_journal_from_its_second() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let failure = journal_line("Failure in Bluetooth audio transport");

        observer.on_journal(failure.clone()).unwrap();
        observer
            .on_monitor(&[json!({"id": 98, "info": null})], failure.at_unix)
            .unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::DISCONNECT);
        assert!(
            row.journal
                .as_deref()
                .unwrap()
                .contains("Failure in Bluetooth audio transport")
        );
    }

    #[test]
    fn a_transport_failure_and_its_same_second_context_are_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);

        let terminated = journal_line("terminated unexpectedly");
        let failure = relevant_journal()
            .into_iter()
            .find(|line| {
                line.message
                    .contains("Failure in Bluetooth audio transport")
                    && line.at_unix == terminated.at_unix
            })
            .expect("the real pair is in one second");

        observer.on_journal(terminated).unwrap();
        observer.on_journal(failure.clone()).unwrap();
        observer.on_journal(failure).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::TRANSPORT_FAILURE);
        let journal = row.journal.as_deref().expect("the journal lines");
        assert!(journal.contains("terminated unexpectedly"), "{journal}");
        assert_eq!(
            journal
                .matches("Failure in Bluetooth audio transport")
                .count(),
            2
        );
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.profile_after.as_deref(), Some("a2dp-sink-sbc"));
    }

    #[test]
    fn a_journal_line_backfills_a_profile_change_from_the_same_second() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);
        let terminated = journal_line("terminated unexpectedly");

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        observer.on_monitor(&[card], terminated.at_unix).unwrap();
        observer.on_journal(terminated).unwrap();

        let rows = recorded(&store);
        let change = only(&rows, kind::PROFILE_CHANGE);
        assert!(
            change
                .journal
                .as_deref()
                .unwrap()
                .contains("terminated unexpectedly")
        );
        assert_eq!(
            count(&rows, kind::TRANSPORT_FAILURE),
            0,
            "the terminated line is context, not a separate trigger"
        );
    }

    #[test]
    fn a_journal_line_from_another_second_is_not_attached() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_EARBUDS, T);
        let objects = dump(WITH_EARBUDS);
        let ready = journal_line("fd(29) ready");

        observer.on_journal(ready.clone()).unwrap();

        let mut card = object(&objects, 98);
        set(
            &mut card,
            "/info/params/Profile/0/name",
            json!("headset-head-unit"),
        );
        observer.on_monitor(&[card], ready.at_unix + 1).unwrap();

        let rows = recorded(&store);
        let change = only(&rows, kind::PROFILE_CHANGE);
        assert_eq!(change.journal, None);
    }

    #[test]
    fn a_lost_balance_correction_is_one_row() {
        let (_dir, store) = store();
        let mut observer = baseline(&store, WITH_STREAMS, T);
        assert!(recorded(&store).is_empty());
        let objects = dump(WITH_STREAMS);

        let mut meiro = object(&objects, 74);
        set(
            &mut meiro,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[meiro.clone()], T + 5).unwrap();

        let rows = recorded(&store);
        let row = only(&rows, kind::BALANCE_DRIFT);
        assert_eq!(row.at_unix, T + 5);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert!(row.detail.as_deref().unwrap().contains("Meiro"));
        assert!(row.detail.as_deref().unwrap().contains("lost"));

        observer.on_monitor(&[meiro], T + 6).unwrap();
        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 1);

        let mut fixed = object(&objects, 74);
        set(
            &mut fixed,
            "/info/params/Props/0/channelVolumes",
            json!([0.19952, 1.0]),
        );
        observer.on_monitor(&[fixed], T + 7).unwrap();
        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 1);
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
        observer.on_monitor(&[stream.clone()], T + 5).unwrap();

        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[stream.clone()], T + 6).unwrap();

        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([0.19952, 1.0]),
        );
        observer.on_monitor(&[stream], T + 7).unwrap();

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
        observer.on_monitor(&[stream.clone()], T + 5).unwrap();

        set(
            &mut stream,
            "/info/params/Props/0/channelVolumes",
            json!([1.0, 1.0]),
        );
        observer.on_monitor(&[stream], T + 6).unwrap();

        assert_eq!(count(&recorded(&store), kind::BALANCE_DRIFT), 0);
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
        observer.on_monitor(&objects, T).unwrap();

        assert!(recorded(&store).is_empty());
    }

    #[test]
    fn replays_the_real_disconnect_and_reconnect() {
        let (_dir, store) = store();
        let mut observer = Observer::new(&store);

        for (index, line) in LIVE.lines().enumerate() {
            let array: Vec<Value> = serde_json::from_str(line).expect("a real monitor array");
            observer
                .on_monitor(&array, T + index as i64)
                .expect("apply a real monitor array");
        }

        let rows = recorded(&store);
        let row = only(&rows, kind::DISCONNECT);
        assert_eq!(row.device.as_deref(), Some("bluez_card.1C_0A_E0_A3_E2_D0"));
        assert_eq!(row.profile_before.as_deref(), Some("a2dp-sink-sbc"));
        assert_eq!(row.codec.as_deref(), Some("sbc"));
        assert_eq!(count(&rows, kind::PROFILE_CHANGE), 0);
        assert_eq!(count(&rows, kind::CODEC_CHANGE), 0);
    }
}
