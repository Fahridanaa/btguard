//! Read the live PipeWire graph from `pw-dump`.

use std::process::Command;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

/// A Bluetooth audio card as PipeWire shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Card {
    pub id: u64,
    pub name: String,
    pub description: String,
    pub address: Option<String>,
    pub connection: Option<String>,
    pub profile: Option<String>,
    pub profile_description: Option<String>,
}

/// A PipeWire audio node: a sink, a source or an application stream.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub id: u64,
    pub name: String,
    pub description: Option<String>,
    pub media_class: String,
    pub codec: Option<String>,
    pub profile: Option<String>,
    pub device_id: Option<u64>,
    /// Linear channel volumes from `info.params.Props[0].channelVolumes`.
    pub channel_volumes: Vec<f64>,
    pub channel_map: Vec<String>,
    /// `target.object` when the stream names its sink or source.
    pub target_object: Option<String>,
}

impl Node {
    /// True for a node that belongs to a Bluetooth card. `bluez_midi.server`
    /// is a `Midi/Bridge`, so it is never in `Snapshot.nodes`.
    pub fn is_bluez_audio(&self) -> bool {
        self.name.starts_with("bluez_output.")
            || self.name.starts_with("bluez_input.")
            || self.codec.is_some()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    pub core_version: Option<String>,
    pub default_sink: Option<String>,
    pub cards: Vec<Card>,
    pub nodes: Vec<Node>,
}

impl Snapshot {
    pub fn load() -> Result<Self> {
        let output = Command::new("pw-dump")
            .output()
            .context("run pw-dump; is PipeWire installed?")?;
        if !output.status.success() {
            bail!(
                "pw-dump failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let json = String::from_utf8(output.stdout).context("pw-dump wrote invalid UTF-8")?;
        Self::parse(&json)
    }

    pub fn parse(json: &str) -> Result<Self> {
        let objects: Vec<Value> =
            serde_json::from_str(json).context("pw-dump did not write a JSON array")?;

        let mut snapshot = Self::default();

        for object in &objects {
            match object.get("type").and_then(Value::as_str) {
                Some("PipeWire:Interface:Core") => {
                    snapshot.core_version = object
                        .pointer("/info/version")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                Some("PipeWire:Interface:Metadata") => {
                    if object
                        .pointer("/props/metadata.name")
                        .and_then(Value::as_str)
                        == Some("default")
                    {
                        snapshot.read_default_sink(object);
                    }
                }
                Some("PipeWire:Interface:Device") => {
                    if let Some(card) = Card::from_object(object) {
                        snapshot.cards.push(card);
                    }
                }
                Some("PipeWire:Interface:Node") => {
                    if let Some(node) = Node::from_object(object)
                        && node.is_bluez_audio()
                    {
                        snapshot.nodes.push(node);
                    }
                }
                _ => {}
            }
        }

        Ok(snapshot)
    }

    /// `default.audio.sink` holds the sink in use. `default.configured.*` is
    /// only what WirePlumber saved, and can name a device that is not present.
    fn read_default_sink(&mut self, object: &Value) {
        if let Some(name) = default_sink_from_metadata(object) {
            self.default_sink = Some(name.to_owned());
        }
    }

    pub fn sink_for<'a>(&'a self, card: &Card) -> Option<&'a Node> {
        self.node_for(card, "Audio/Sink")
    }

    pub fn source_for<'a>(&'a self, card: &Card) -> Option<&'a Node> {
        self.node_for(card, "Audio/Source")
    }

    fn node_for<'a>(&'a self, card: &Card, media_class: &str) -> Option<&'a Node> {
        self.nodes
            .iter()
            .find(|node| node.device_id == Some(card.id) && node.media_class == media_class)
    }
}

impl Card {
    pub(crate) fn from_object(object: &Value) -> Option<Self> {
        let props = props(object)?;
        if text(props, "device.api") != Some("bluez5") {
            return None;
        }

        // Do not read `bluez5.profile`: on a live A2DP card it can say "off".
        // The active profile is the Profile param.
        let (profile, profile_description) = active_profile(object);

        Some(Self {
            id: object.get("id")?.as_u64()?,
            name: text(props, "device.name")?.to_owned(),
            description: text(props, "device.description")
                .or_else(|| text(props, "device.alias"))
                .unwrap_or_default()
                .to_owned(),
            address: text(props, "api.bluez5.address").map(str::to_owned),
            connection: text(props, "api.bluez5.connection").map(str::to_owned),
            profile,
            profile_description,
        })
    }
}

impl Node {
    pub(crate) fn from_object(object: &Value) -> Option<Self> {
        let props = props(object)?;
        let name = text(props, "node.name")?;
        let media_class = text(props, "media.class").unwrap_or_default();

        // Keep the audio graph only. This drops `bluez_midi.server`, which is
        // a Midi/Bridge node with `api.bluez5.*` properties.
        if !matches!(
            media_class,
            "Audio/Sink" | "Audio/Source" | "Stream/Output/Audio" | "Stream/Input/Audio"
        ) {
            return None;
        }

        let (channel_volumes, channel_map) = node_volumes(object);

        Some(Self {
            id: object.get("id")?.as_u64()?,
            name: name.to_owned(),
            description: text(props, "node.description").map(str::to_owned),
            media_class: media_class.to_owned(),
            codec: text(props, "api.bluez5.codec").map(str::to_owned),
            profile: text(props, "api.bluez5.profile").map(str::to_owned),
            device_id: number(props, "device.id"),
            channel_volumes,
            channel_map,
            target_object: text(props, "target.object").map(str::to_owned),
        })
    }
}

fn node_volumes(object: &Value) -> (Vec<f64>, Vec<String>) {
    let Some(props) = object
        .pointer("/info/params/Props")
        .and_then(Value::as_array)
        .and_then(|list| list.first())
    else {
        return (Vec::new(), Vec::new());
    };

    let volumes = props
        .get("channelVolumes")
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(Value::as_f64).collect())
        .unwrap_or_default();
    let map = props
        .get("channelMap")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();

    (volumes, map)
}

fn props(object: &Value) -> Option<&Map<String, Value>> {
    object.get("info")?.get("props")?.as_object()
}

/// The sink in use from a `default` Metadata object. `default.configured.*`
/// is only what WirePlumber saved, so it is not read here.
pub(crate) fn default_sink_from_metadata(object: &Value) -> Option<&str> {
    object
        .get("metadata")?
        .as_array()?
        .iter()
        .find(|entry| entry.get("key").and_then(Value::as_str) == Some("default.audio.sink"))
        .and_then(|entry| entry.pointer("/value/name"))
        .and_then(Value::as_str)
}

fn text<'a>(props: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    props.get(key)?.as_str()
}

fn number(props: &Map<String, Value>, key: &str) -> Option<u64> {
    let value = props.get(key)?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn active_profile(object: &Value) -> (Option<String>, Option<String>) {
    let Some(active) = object
        .pointer("/info/params/Profile")
        .and_then(Value::as_array)
        .and_then(|list| list.first())
    else {
        return (None, None);
    };

    (
        active
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned),
        active
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real captures from this machine. See tests/fixtures/README.md.
    const WITH_EARBUDS: &str = include_str!("../tests/fixtures/pw-dump-bluez.json");
    const WITHOUT_EARBUDS: &str = include_str!("../tests/fixtures/pw-dump-no-bluez.json");

    #[test]
    fn reads_the_connected_earbuds() {
        let snapshot = Snapshot::parse(WITH_EARBUDS).expect("parse the real capture");

        assert_eq!(snapshot.core_version.as_deref(), Some("1.6.11"));
        assert_eq!(snapshot.cards.len(), 1, "one Bluetooth card");

        let card = &snapshot.cards[0];
        assert_eq!(card.name, "bluez_card.1C_0A_E0_A3_E2_D0");
        assert_eq!(card.description, "soundcore R50i");
        assert_eq!(card.address.as_deref(), Some("1C:0A:E0:A3:E2:D0"));
        assert_eq!(card.connection.as_deref(), Some("connected"));

        // This capture has bluez5.profile = "off" on the card while it plays on
        // A2DP. The status command must show the Profile param instead.
        assert_eq!(card.profile.as_deref(), Some("a2dp-sink-sbc"));

        assert_eq!(
            snapshot.default_sink.as_deref(),
            Some("bluez_output.1C_0A_E0_A3_E2_D0.1")
        );

        let sink = snapshot.sink_for(card).expect("a sink for the card");
        assert_eq!(sink.name, "bluez_output.1C_0A_E0_A3_E2_D0.1");
        assert_eq!(sink.codec.as_deref(), Some("sbc"));
        assert_eq!(sink.media_class, "Audio/Sink");
        // Linear channel volumes from the real capture. The sink volume is not
        // the per-stream balance correction; streams carry their own volumes.
        assert_eq!(sink.channel_volumes, vec![0.140608, 0.140608]);
        assert_eq!(sink.channel_map, ["FL", "FR"]);
        assert!(snapshot.source_for(card).is_none(), "no HFP source");
    }

    #[test]
    fn ignores_bluetooth_when_no_device_is_connected() {
        let snapshot = Snapshot::parse(WITHOUT_EARBUDS).expect("parse the real capture");

        assert!(snapshot.cards.is_empty());
        assert!(snapshot.nodes.is_empty());
        assert_eq!(
            snapshot.default_sink.as_deref(),
            Some("alsa_output.pci-0000_00_1f.3.analog-stereo")
        );
    }

    #[test]
    fn a_dump_that_is_not_json_is_an_error() {
        assert!(Snapshot::parse("not json").is_err());
    }

    #[test]
    fn a_bluetooth_midi_node_is_not_audio() {
        let snapshot = Snapshot::parse(WITH_EARBUDS).expect("parse the real capture");
        assert!(
            snapshot
                .nodes
                .iter()
                .all(|node| node.name != "bluez_midi.server"),
            "the BLE MIDI node must not count as audio"
        );
    }
}
