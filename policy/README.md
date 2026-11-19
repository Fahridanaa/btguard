# policy/

Empty on purpose. The profile and balance policy will live here as a
WirePlumber Lua script.

## Why here

A Lua script runs inside WirePlumber. It acts with the event, not after it. An
external daemon loses races: by the time it sees a profile change, the stream
may be gone.

## Contract

- Put the card back on `a2dp-sink-sbc` if it leaves that profile.
- Keep the 14 dB left attenuation on streams that play on the earbuds.
- Flatten a leaked split on streams that play on other sinks.
- Log each action to the journal, with the profile and the stream.
- Do not use SQLite and do not read the journal. The observer does that.

## Not written yet

The live fix is still the hand-written set:

- `~/.config/wireplumber/wireplumber.conf.d/52-bluez-stability.conf`
- `~/.local/bin/earbud-balance`

Do not copy a script from this directory into `~/.config/wireplumber` until it
is tested on this machine. A bad policy can break audio for the whole session.
