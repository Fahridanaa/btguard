# AGENTS.md

Rules for an AI that works in this repo.

## What this is

`bt-audio-guard` observes Bluetooth audio on PipeWire and records incidents.
Read `README.md` first. It is an observer: it must not change audio state.

## Commands

    cargo build
    cargo test
    cargo clippy --all-targets -- -D warnings
    cargo fmt --check

## Do not touch the live audio stack

The running fix lives outside this repo:

- `~/.config/wireplumber/wireplumber.conf.d/`
- `~/.local/state/wireplumber/`
- `~/.local/bin/earbud-balance`
- `~/.config/systemd/user/earbud-balance.service`

Do not edit or restart them. Propose the change and let the user apply it.
Enforcement code goes in `policy/`, not in the Rust binary.

## Fixtures are real captures

`tests/fixtures/*.json` come from this machine. Do not write a fixture by hand
and call it real. If you need a state that you cannot capture, put `-synthetic`
in the name and say so in `tests/fixtures/README.md`.

Recapture with `pw-dump`, select the objects the parser reads, and record the
command in `tests/fixtures/README.md`.

## Schema

One migration at a time. Bump `SCHEMA_VERSION` and add a step. Do not edit
`SCHEMA_V1` after the first release.

## Keep the observer honest

A row must come from a real source: `pw-dump`, the journal or D-Bus. Do not
infer an incident from a timer.
