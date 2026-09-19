# Event Stream Replay Engine

A Python-based event replay and state reconstruction engine designed to record application events and deterministically rebuild historical application state for debugging, auditing, and failure reproduction.

## Features

* Append-only event storage
* SHA-256 hash-chain integrity
* Deterministic event replay
* Historical state reconstruction
* Time-travel replay
* Event history inspection
* State snapshots
* Historical state comparison
* Event integrity verification
* JSON report generation
* CLI interface
* Built-in demo workflow

## Tech Stack

**Python | Event Sourcing | SHA-256 | JSONL | CLI**

## DSA Used

**Dictionary | deque | List | Hashing | Sequential Event Processing**

## Usage

Run the demo:

```bash
python event_stream_replay_engine_fixed.py demo
```

Verify event integrity:

```bash
python event_stream_replay_engine_fixed.py verify --store events.jsonl
```

Replay application state:

```bash
python event_stream_replay_engine_fixed.py replay --store events.jsonl
```

Replay up to a specific event:

```bash
python event_stream_replay_engine_fixed.py replay --store events.jsonl --end 4
```

View event history:

```bash
python event_stream_replay_engine_fixed.py history --store events.jsonl
```

Create a snapshot:

```bash
python event_stream_replay_engine_fixed.py snapshot --store events.jsonl --end 4
```

Compare historical states:

```bash
python event_stream_replay_engine_fixed.py compare --store events.jsonl --first 2 --second 6
```

Generate a JSON report:

```bash
python event_stream_replay_engine_fixed.py demo --json report.json
```

## Architecture

```text
Application Event
       ↓
Append-Only Event Store
       ↓
SHA-256 Hash Chain
       ↓
Event History
       ↓
Replay Engine
       ↓
State Reconstruction
       ↓
Snapshot / Time Travel
       ↓
Debugging & Analysis
```

## Example Event

```json
{
  "type": "MoneyDeposited",
  "aggregate": "acct-1",
  "payload": {
    "amount": 500
  }
}
```

## Purpose

Built to demonstrate event sourcing, deterministic replay, hash-chain integrity, historical state reconstruction, debugging through time travel, and reliable event-driven system design.


