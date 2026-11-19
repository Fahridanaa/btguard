# bt-audio-guard

An observer for Bluetooth audio on PipeWire. It records what goes wrong with
the soundcore R50i earbuds on this laptop, so a failure becomes a row in a
table instead of a silent game and a crashed shell.

**Status: observer.** `btguard status`, `btguard incidents` and `btguard watch`
work. The watcher records incidents; it does not enforce policy. The
WirePlumber policy and the DMS widget do not exist yet.

## The problem

The earbuds are a soundcore R50i at address `1C:0A:E0:A3:E2:D0`. PipeWire shows
them as `bluez_card.1C_0A_E0_A3_E2_D0`.

Three failures:

1. **The earbud mic killed playback.** Opening the mic switched the card to the
   HFP profile `headset-head-unit`. That tore down the A2DP transport and
   removed both `bluez_output` and `bluez_input`. The game went silent.
   quickshell also crashed with `QAudioContext SIGSEGV` at 19:24:23 and
   19:42:42.
2. **AAC played silence.** SBC plays sound. `pactl` shows the codec only inside
   the profile description; the node has `api.bluez5.codec`.
3. **The right bud was up to 14 dB quieter than the left.**

The drop-in that fixed this records 90 `Failure in Bluetooth audio transport`
lines in 3 days before the fix. Each one landed in the same second as a kernel
line: `Bluetooth: hci0: SCO packet for unknown connection handle N`. SCO is the
headset voice link.

## The hand fix that is already in place

| File | What it does |
|---|---|
| `~/.config/wireplumber/wireplumber.conf.d/52-bluez-stability.conf` | `bluez5.roles = [ "a2dp_source" ]` removes HFP, so no SCO link can open. Also disables mSBC, SBC-XQ, FastStream and A2DP-duplex, turns on hardware volume, and turns off route autoswitch. |
| `~/.local/state/wireplumber/default-profile` | Saves `bluez_card.1C_0A_E0_A3_E2_D0=a2dp-sink-sbc`. |
| `~/.config/wireplumber/wireplumber.conf.d/51-nvidia-hdmi-deprioritize.conf` | Drops the dead NVIDIA HDMI nodes to priority 1, so they cannot win default-node selection. |
| `~/.local/bin/earbud-balance` and `~/.config/systemd/user/earbud-balance.service` | Attenuates the left channel of each stream by 14 dB, and removes the split from streams on other sinks. |

Measured on 2026-11-19, before the live disconnect test:

```
$ journalctl --since "7 days ago" | grep -c "Failure in Bluetooth audio transport"
2
```

The two lines are from Nov 14 and Nov 16. The rate is lower than 90 per 3 days,
but it is not zero. The live disconnect test added one more at 17:38.

This repository does not change those files. It watches the stack they run on.

## What this project adds

The hand fix is policy without a record. This project adds the record first.

- **Observe.** Watch the graph and the journal, and store each incident with
  the journal lines from the same second.
- **Decide later.** After one week of data, decide what to enforce. Do not
  guess now.
- **Enforce in the right place.** Profile and balance policy belongs in a
  WirePlumber Lua script, inside the session manager. It does not race streams.
- **Show.** A DMS bar widget shows the codec, the battery and the count for
  today.

Why the split:

| Option | Good | Bad |
|---|---|---|
| WirePlumber Lua policy | Acts inside the session manager, so no race with streams. The correct place for policy. | Hard to debug. No SQLite. Can break audio for the whole session. |
| External Rust observer | Easy to test. Gets the journal and D-Bus easily. Safe to stop. | Acts a little after each event, so it can lose races. |

The observer must never make audio worse. Policy goes in Lua.

## What works today

```
$ btguard status
btguard 0.1.0

PipeWire      1.6.11
Default sink  bluez_output.1C_0A_E0_A3_E2_D0.1
Incidents     0 recorded

Bluetooth
  soundcore R50i (bluez_card.1C_0A_E0_A3_E2_D0)
    profile  a2dp-sink-sbc — High Fidelity Playback (A2DP Sink, codec SBC)
    sink     bluez_output.1C_0A_E0_A3_E2_D0.1  [default]  codec sbc
    source   none
```

```
$ btguard incidents --since 3d
No incidents since 2026-11-16 16:48:10 (3d).
```

`incidents` reads the table that `watch` writes.

## The watcher

```
$ btguard watch
btguard watch: database /home/fahridanaa/.local/state/bt-audio-guard/incidents.db
btguard watch: watching pw-dump --monitor and journalctl -f
btguard watch: baseline: 1 card(s), 2 output stream(s): bluez_card.1C_0A_E0_A3_E2_D0
```

It starts two child processes and never polls:

| Input | What it reads |
|---|---|
| `pw-dump --monitor` | The full graph first, then one JSON array per change: card profile, codec, default sink, nodes, streams and links. |
| `journalctl --follow --output=json -n 0` | New journal lines from `bluetoothd`, the kernel, and WirePlumber's `spa.bluez5` lines. |

A row is recorded for these transitions:

| Kind | When |
|---|---|
| `profile-change` | A Bluetooth card's `info.params.Profile[0].name` changes. The profile param is live; `bluez5.profile` can read `off`. |
| `codec-change` | The codec of a card's sink or source changes while the profile stays the same. A sink that disappears is not a codec change. |
| `disconnect` | A connected card says `disconnected` or is removed from the graph. The removal after a disconnect is not a second row. |
| `transport-failure` | A WirePlumber line contains `Failure in Bluetooth audio transport`. A burst in one second is one row. |
| `sco-orphan` | A kernel line contains `SCO packet for unknown connection handle`. A burst in one second is one row. |
| `balance-drift` | A stream on the earbud sink was corrected (left at least 6 dB below right) and then lost that attenuation (left within 1 dB of right, or louder). The row names the stream and both volumes. |

The first `pw-dump --monitor` array sets the baseline. It records no incidents,
so a restart does not invent a profile change for every card.

**Same-second journal.** The watcher keeps the last 10 seconds of relevant
journal lines. A line that arrives before a graph event is attached when the
row is written. A line that arrives after it is appended to the rows of that
second. A profile change and a transport failure in the same second therefore
share the same journal text.

**Balance drift has a limit.** A stream that arrives flat and never receives
the correction is not a row. That condition needs a timer, and this observer
does not infer incidents from timers. Recorded drift means a correction that
was present and then disappeared.

The watcher only reads. It does not change the card profile, the volumes, the
WirePlumber files or `earbud-balance`.

A live disconnect on 2026-11-19 produced two rows, both with the journal lines
from the same second:

```
2026-11-19 17:38:11  transport-failure
    device   bluez_card.1C_0A_E0_A3_E2_D0
    | spa.bluez5.sink.media: 0x562ec5afd2e8: connection (/org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0) terminated unexpectedly
    | pw.node: (bluez_output.1C_0A_E0_A3_E2_D0.1-81) running -> error (Received error event)
    | spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0
    | pw.node: (bluez_output.1C_0A_E0_A3_E2_D0.1-81) running -> error (Received error event)

2026-11-19 17:38:11  disconnect
    device   bluez_card.1C_0A_E0_A3_E2_D0
    profile  a2dp-sink-sbc
    codec    sbc
    detail   Bluetooth card disconnected
    | spa.bluez5.sink.media: 0x562ec5afd2e8: connection (/org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0) terminated unexpectedly
    | pw.node: (bluez_output.1C_0A_E0_A3_E2_D0.1-81) running -> error (Received error event)
    | spa.bluez5: Failure in Bluetooth audio transport /org/bluez/hci0/dev_1C_0A_E0_A3_E2_D0/sep1/fd0
    | pw.node: (bluez_output.1C_0A_E0_A3_E2_D0.1-81) running -> error (Received error event)
```

The reconnect added no row. The card came back as `off`, then settled on
`a2dp-sink-sbc`; neither transition is an incident.

## Data sources

| Source | Status | What it gives |
|---|---|---|
| `pw-dump --monitor` | Used by `watch` | Card profile, codec, default sink, nodes, streams and links. |
| `journalctl -f` | Used by `watch` | `bluetoothd`, kernel Bluetooth lines and WirePlumber `spa.bluez5` lines. The transport failure itself is a WirePlumber line, not a `bluetoothd` line. |
| BlueZ D-Bus | Not used yet | `org.bluez.Battery1`. This unit does not expose it (`No such interface 'org.bluez.Battery1'`), so the widget must handle a missing battery. |
| `pactl subscribe` | Not used | `earbud-balance` uses it today. `pw-dump --monitor` already reports stream volumes. |

## The incident table

`src/store.rs` owns the schema. Version 1 is one table:

| Column | Meaning |
|---|---|
| `id` | Row id. |
| `at_unix` | Seconds since the Unix epoch, UTC. |
| `kind` | `transport-failure`, `sco-orphan`, `profile-change`, `codec-change`, `disconnect` or `balance-drift`. |
| `device` | The card or node name, such as `bluez_card.1C_0A_E0_A3_E2_D0`. |
| `profile_before`, `profile_after` | The card profile around the incident. |
| `codec` | The codec in use. |
| `detail` | Free text from the observer. |
| `journal` | The journal lines from the same second, one per line. |

The database is `$XDG_STATE_HOME/bt-audio-guard/incidents.db`, or
`~/.local/state/bt-audio-guard/incidents.db`. It uses WAL mode.

## Field notes

These are measured on this machine, not assumed.

- **`bluez5.profile` on the card lies.** With the card live on `a2dp-sink-sbc`,
  `info.props["bluez5.profile"]` reads `off`. The live value is
  `info.params.Profile[0].name`. The test in `src/pw.rs` guards this.
- **`default.configured.audio.sink` can name a device that is not connected.**
  Read `default.audio.sink` for the sink in use.
- **`bluez_midi.server` is not audio.** It is a `Midi/Bridge` node with
  `api.bluez5.*` properties. Filter on `media.class` or the `bluez_output.` and
  `bluez_input.` prefixes.
- **`pw-dump --monitor` is a sequence of JSON arrays, not line-delimited
  JSON.** PipeWire 1.6.11 writes the full graph as one pretty-printed array,
  then one more array for each change. A removal is a small array with no
  `type`: `[{"id": 98, "info": null}]`. It is not one endless array either: the
  initial array closes with `]` before the events start. The reader in
  `src/watch.rs` follows braces and strings; a line reader fails.
- **The transport failure is a WirePlumber line.** It is
  `spa.bluez5: Failure in Bluetooth audio transport ...` with
  `SYSLOG_IDENTIFIER=wireplumber`, not a `bluetoothd` line. A watcher that only
  follows `bluetoothd` and the kernel cannot see it. The SCO line is a kernel
  line: `Bluetooth: hci0: SCO packet for unknown connection handle N`.
- **Stream volumes are linear, not decibels.** A corrected stream has
  `channelVolumes: [0.199520, 1.000000]`, which is 14 dB below the right
  channel. A flat stream has `[1.0, 1.0]`. The sink has its own volumes
  (`[0.140608, 0.140608]` in the capture); the balance correction lives on the
  stream nodes.
- **A stream's sink comes from two places.** `target.object` names it when the
  application sets one (`Sober`). When it is absent (`Meiro`), the Link objects
  connect the stream's output node to the sink's node.
- **The per-codec switches were written against PipeWire 1.6.9.** The machine
  now runs 1.6.11. Recheck `bluez5.codecs` before you rely on those switches.
- **The earbud battery is not on D-Bus today.** `org.bluez.Battery1` is absent
  on this unit, even when connected.

## Layout

```
src/main.rs      CLI glue and the read-only views
src/cli.rs       clap surface
src/pw.rs        pw-dump parser and live snapshot
src/watch.rs     the streaming monitor, journal and observer
src/store.rs     SQLite schema and queries
src/duration.rs  --since spans
src/paths.rs     state directory
systemd/         the user unit, not installed by this repo
tests/fixtures/  real pw-dump and journal captures
policy/          WirePlumber Lua policy, later
widget/          DMS bar widget, later
```

## Build and run

```
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
./target/debug/btguard status
./target/debug/btguard watch
./target/debug/btguard incidents --since 3d
```

## Run it as a user service

The unit is `systemd/btguard-watch.service`. This repository does not install
or enable it.

```
cargo build --release
install -Dm755 target/release/btguard ~/.local/bin/btguard
install -Dm644 systemd/btguard-watch.service ~/.config/systemd/user/btguard-watch.service
systemctl --user daemon-reload
systemctl --user enable --now btguard-watch.service
```

The unit runs `Restart=always` with `RestartSec=3`, like
`earbud-balance.service`, and uses `KillMode=control-group` so a stop also stops
`pw-dump` and `journalctl`.

## Next

1. The Lua policy in `policy/`.
2. The DMS widget in `widget/`.

## Open questions

- **Is the 14 dB imbalance the earbuds or the laptop?** Test the earbuds on a
  phone. If the right bud is quiet there too, it is a hardware fault and a
  warranty claim is the correct fix.
- **Is AAC silence a PipeWire bug?** Probably. A recorded incident with the
  exact journal lines makes a good upstream report. A guard that only forces
  SBC hides the bug.
- **One machine or many?** The device MAC is fixed today. Multi-device support
  needs discovery and per-device settings, and is about 3 times the work.
