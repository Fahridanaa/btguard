# Fixtures

Real captures from this machine. The tests in `src/pw.rs` and `src/watch.rs`
parse them.

## pw-dump-bluez.json

Captured 2026-11-19 with the soundcore R50i connected and `a2dp-sink-sbc`
active.

```
pw-dump | jq '[.[] | select(.id==0 or .id==42 or .id==51 or .id==58 or .id==94 or .id==98)]'
```

| id | Object |
|---|---|
| 0 | Core, `info.version` 1.6.11 |
| 42 | Metadata `default` |
| 51 | Built-in ALSA device |
| 58 | Built-in ALSA sink |
| 94 | `bluez_output.1C_0A_E0_A3_E2_D0.1`, codec `sbc` |
| 98 | `bluez_card.1C_0A_E0_A3_E2_D0` |

Two traps are in this capture:

- `info.params.Profile[0].name` is `a2dp-sink-sbc`.
- `info.props["bluez5.profile"]` is `off`.

The card is live, so the parser must read the param, not the prop.

## pw-dump-no-bluez.json

Captured 2026-11-19 before the earbuds connected.

```
pw-dump | jq '[.[] | select(.id==0 or .id==42 or .id==51 or .id==58)]'
```

The metadata still has `default.configured.audio.sink` on the earbuds. Only
`default.audio.sink` moved to the ALSA sink.

## pw-dump-streams.json

Captured 2026-11-19 with the earbuds connected and two application streams
(`Meiro` and `Sober`) on the earbud sink. Both streams have
`channelVolumes: [0.199520, 1.000000]`, the 14 dB left attenuation that
`earbud-balance` applies. `Meiro` has no `target.object` and is mapped to the
sink by Link objects; `Sober` has a `target.object` and links.

```
pw-dump | jq '[.[] | select(.id==0 or .id==42 or .id==74 or .id==76 or .id==78 or .id==83 or .id==86 or .id==88 or .id==92 or .id==94 or .id==95 or .id==98 or .id==106)]'
```

| id | Object |
|---|---|
| 0 | Core |
| 42 | Metadata `default`, `default.audio.sink` is the earbuds |
| 74 | `Meiro`, `Stream/Output/Audio`, volumes `[0.199520, 1.0]` |
| 88 | `cava`, `Stream/Input/Audio`, volumes `[1.0, 1.0]` |
| 94 | `bluez_output.1C_0A_E0_A3_E2_D0.1`, codec `sbc` |
| 95 | `Sober`, `Stream/Output/Audio`, volumes `[0.199520, 1.0]` |
| 98 | `bluez_card.1C_0A_E0_A3_E2_D0` |
| 76, 78 | Links `74 -> 94` |
| 83, 86 | Links `94 -> 88` |
| 92, 106 | Links `95 -> 94` |

## journal-bluetooth.jsonl

Nine real `journalctl --output=json` lines from 2026-10-20 to 2026-11-19,
trimmed to the fields the watcher reads. The file is JSON Lines: one journal
object per line, not one JSON array.

```
journalctl -o json --no-pager | jq -c '
  select(
    .__REALTIME_TIMESTAMP == "1794830708564402"
    or .__REALTIME_TIMESTAMP == "1794830708589459"
    or .__REALTIME_TIMESTAMP == "1794830743835355"
    or .__REALTIME_TIMESTAMP == "1794670578722714"
    or .__REALTIME_TIMESTAMP == "1792067006151330"
    or .__REALTIME_TIMESTAMP == "1792067373659734"
    or .__REALTIME_TIMESTAMP == "1792070112470769"
    or .__REALTIME_TIMESTAMP == "1795071866540822"
    or .__REALTIME_TIMESTAMP == "1795082283865377"
  ) | {__REALTIME_TIMESTAMP, SYSLOG_IDENTIFIER, _TRANSPORT, MESSAGE}'
```

The selected lines cover:

- `spa.bluez5.sink.media: ... connection (...) terminated unexpectedly` and
  the `spa.bluez5: Failure in Bluetooth audio transport ...` line in the same
  second. Both are real `wireplumber` lines, not `bluetoothd` lines.
- `Bluetooth: hci0: SCO packet for unknown connection handle 2` and `... 4`,
  real kernel lines from 2026-10-20, before the WirePlumber fix.
- `/org/bluez/hci0/dev_.../sep1/fd0: fd(29) ready`, a real `bluetoothd` line.
- `Bluetooth: SCO socket layer initialized`, a real kernel line that is
  relevant but is not an incident.
- `usb 3-1: New USB device strings: ...`, a real kernel line that is not
  Bluetooth and must be filtered out.

The SCO lines are older than the transport failures. The journal has no SCO
line after the `bluez5.roles = [ "a2dp_source" ]` fix.

## pw-monitor-live.jsonl

A real `pw-dump --monitor` session from 2026-11-19, 17:34 to 17:35. The earbuds
were connected, then `bluetoothctl disconnect 1C:0A:E0:A3:E2:D0` ran, then the
earbuds reconnected on their own. The file is a sequence of JSON arrays, one
per line, not one array and not JSON Lines of objects.

The raw capture used this filter; the full graph is trimmed to the objects the
watcher reads:

```
pw-dump --monitor | jq --unbuffered -c '
  map(select(
    (.id == 0)
    or (.id == 42)
    or (.type == "PipeWire:Interface:Device" and (.info.props["device.api"] == "bluez5"))
    or (.type == "PipeWire:Interface:Node" and (
         ((.info.props["node.name"] // "") | startswith("bluez_"))
         or ((.info.props["media.class"] // "") | startswith("Stream/"))
       ))
    or (.type == "PipeWire:Interface:Link")
    or ((.type == null) and (.info == null))
  ))
  | select(length > 0)'
```

The raw capture had 167 arrays because it also kept every stream and link
change. The fixture keeps the card lifecycle:

```
jq -c '
  map(select(
    (.id == 42)
    or (.type == "PipeWire:Interface:Device" and (.info.props["device.api"] == "bluez5"))
    or (.type == "PipeWire:Interface:Node" and (
         ((.info.props["node.name"] // "") | startswith("bluez_output."))
         or ((.info.props["node.name"] // "") | startswith("bluez_input."))
       ))
    or ((.type == null) and (.info == null) and ((.id == 98) or (.id == 87) or (.id == 94) or (.id == 81)))
  )) | select(length > 0)
' monitor-raw.jsonl
```

What it contains:

- Card `98` on `a2dp-sink-sbc`, then profile `off`, then removal. Sink node
  `94` is removed first.
- Metadata `default.audio.sink` moves to the ALSA sink and back.
- Card `87` and sink node `81` are the reconnect. PipeWire reuses ids, so the
  card and sink get new ids.
- The updated `codec` and `profile` values are real; they are not edited.

The test `replays_the_real_disconnect_and_reconnect` feeds every line to the
observer and asserts one `disconnect` row with the last working profile
(`a2dp-sink-sbc`), one codec (`sbc`), and no `profile-change` or `codec-change`
rows for the `off` transitions.

## Rules

- Trim with `jq`. Do not edit the values.
- Record the date, the device state and the command.
- A state that you cannot capture goes in a file with `-synthetic` in the name.
