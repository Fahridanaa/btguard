# bt-audio-guard

An observer for Bluetooth audio on PipeWire. It records what goes wrong with
the soundcore R50i earbuds on this laptop, so a failure becomes a row in a
table instead of a silent game and a crashed shell.

**Status: skeleton.** `btguard status` and `btguard incidents` work. The
watcher, the WirePlumber policy and the DMS widget do not exist yet.

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

Measured on 2026-11-19:

```
$ journalctl --since "7 days ago" | grep -c "Failure in Bluetooth audio transport"
2
```

The two lines are from Nov 14 and Nov 16. The rate is lower than 90 per 3 days,
but it is not zero.

This repository does not change those files. It watches the stack they run on.

## What this project adds

The hand fix is policy without a record. This project adds the record first.

- **Observe.** Watch the graph, the journal and BlueZ. Store each incident with
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

`incidents` reads an empty table today, because nothing writes to it yet.

## Planned data sources

| Source | What it gives |
|---|---|
| `pw-dump --monitor` | Card profile, codec, default sink, nodes and streams. |
| `journalctl -f` | bluetoothd and kernel lines, such as the transport failure and the SCO line. |
| BlueZ D-Bus | `org.bluez.Battery1`. This unit does not expose it today (`No such interface 'org.bluez.Battery1'`), so the widget must handle a missing battery. |
| `pactl subscribe` | Stream volume changes. `earbud-balance` uses this today. |

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
- **`pw-dump --monitor` is not line-delimited JSON.** PipeWire 1.6.11 writes a
  pretty-printed array: `[`, objects separated by `,`, then `]`. It sends the
  full initial state first. The watcher needs a brace-depth reader, not a line
  reader.
- **The per-codec switches were written against PipeWire 1.6.9.** The machine
  now runs 1.6.11. Recheck `bluez5.codecs` before you rely on those switches.
- **The earbud battery is not on D-Bus today.** `org.bluez.Battery1` is absent
  on this unit, even when connected.

## Layout

```
src/main.rs      CLI glue and the two views
src/cli.rs       clap surface
src/pw.rs        pw-dump parser and live snapshot
src/store.rs     SQLite schema and queries
src/duration.rs  --since spans
src/paths.rs     state directory
tests/fixtures/  real pw-dump captures
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
./target/debug/btguard incidents --since 3d
```

## Next

1. `btguard watch`: read `pw-dump --monitor` and `journalctl -f`, then write
   rows.
2. A systemd user unit with `Restart=always` and `RestartSec=3`, like
   `earbud-balance.service`.
3. The Lua policy in `policy/`.
4. The DMS widget in `widget/`.

## Open questions

- **Is the 14 dB imbalance the earbuds or the laptop?** Test the earbuds on a
  phone. If the right bud is quiet there too, it is a hardware fault and a
  warranty claim is the correct fix.
- **Is AAC silence a PipeWire bug?** Probably. A recorded incident with the
  exact journal lines makes a good upstream report. A guard that only forces
  SBC hides the bug.
- **One machine or many?** The device MAC is fixed today. Multi-device support
  needs discovery and per-device settings, and is about 3 times the work.
