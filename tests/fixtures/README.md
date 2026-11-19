# Fixtures

Real `pw-dump` captures from this machine, trimmed to the objects the parser
reads. The tests in `src/pw.rs` parse them.

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

## Rules

- Trim with `jq`. Do not edit the values.
- Record the date, the device state and the command.
- A state that you cannot capture goes in a file with `-synthetic` in the name.
