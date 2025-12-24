# Coherence Task — Event Ingestion Pipeline

A modular event ingestion system with real-time streaming support.

## Overview

This project implements a two-part event processing pipeline:

- **Part 1:** Batch ingestion with validation, classification, and routing
- **Part 2:** Real-time streaming with buffering, backpressure, and retry

Both parts share the same core boundary, ensuring consistent behavior regardless of input method.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         main.py                                 │
│                    (batch or stream mode)                       │
└─────────────────────────┬───────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ▼                               ▼
   ┌─────────────┐               ┌─────────────────┐
   │ Batch Mode  │               │  Stream Mode    │
   │ (direct)    │               │  (WebSocket)    │
   └──────┬──────┘               └────────┬────────┘
          │                               │
          │                     ┌─────────▼─────────┐
          │                     │ streamListener.py │
          │                     │ • buffer          │
          │                     │ • backpressure    │
          │                     │ • retry           │
          │                     │ • hooks           │
          │                     └─────────┬─────────┘
          │                               │
          └───────────────┬───────────────┘
                          │
                          ▼
            ┌─────────────────────────┐
            │ integrationBoundary.py  │
            │ • validate raw events   │
            │ • enforce invariants    │
            │ • validate normalized   │
            │ • route to handler      │
            └────────────┬────────────┘
                         │
                         ▼
            ┌─────────────────────────┐
            │   eventsIngestion.py    │
            │ • sort by timestamp     │
            │ • classify event type   │
            │ • normalize schema      │
            └─────────────────────────┘
```

## Quick Start

### Setup

```bash
cd coherence-task
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Tests

```bash
pytest test.py -v
```

### Batch Mode (Part 1)

```bash
python main.py
```

Processes hardcoded test events through the pipeline.

### Stream Mode (Part 2)

**Terminal 1 — Start server:**
```bash
python main.py stream
```

**Terminal 2 — Send events:**
```bash
python test_ws_client.py         # Run all test scenarios once
python test_ws_client.py loop    # Continuous loop (Ctrl+C to stop)
```

Both server and client support **graceful shutdown** — press `Ctrl+C` to stop cleanly. The server will flush any remaining buffered events before exiting.

Or send events manually:
```python
import asyncio, json, websockets

async def send():
    async with websockets.connect('ws://localhost:8765') as ws:
        event = {"timestamp": "2025-12-24T10:00:00Z", "source": "slack", "text": "Hello"}
        await ws.send(json.dumps(event))

asyncio.run(send())
```

## Project Structure

```
coherence-task/
├── main.py                 # Entry point (batch + stream modes)
├── eventsIngestion.py      # Core pipeline: sort, classify, normalize
├── integrationBoundary.py  # Hard boundary with validation + invariants
├── streamListener.py       # Real-time: buffer, backpressure, retry, hooks
├── events_types.json       # Classification rules
├── test.py                 # 13 unit tests (Part 1 + Part 2)
├── test_ws_client.py       # WebSocket test client with 10 scenarios
├── requirements.txt        # Dependencies
└── documentations/
    └── SPEC.md             # Original specification
```

## Event Schema

### Raw Event (Input)

```json
{
  "timestamp": "2025-12-24T10:00:00Z",
  "source": "slack",
  "text": "Team standup notes"
}
```

### Normalized Event (Output)

```json
{
  "t": "2025-12-24T10:00:00Z",
  "source": "slack",
  "type": "team_update",
  "text": "Team standup notes"
}
```

## Event Classification

Defined in `events_types.json`:

| Type | Sources | Keywords |
|------|---------|----------|
| `team_update` | — | team update, deadline, sprint, review, standup |
| `external_comm` | email | client, customer, partner |
| `system_alert` | — | error, alert, timeout, failed |

## Boundary Invariants

The `IngestionRoutingBoundary` guarantees:

1. **Count preserved** — No events dropped or duplicated
2. **Chronological order** — Events sorted by timestamp
3. **Trace consistency** — All events in a batch share one `trace_id`
4. **Schema validated** — Both raw and normalized events checked

## Stream Listener Features

| Feature | Description |
|---------|-------------|
| **Buffering** | Events accumulate until `flush_threshold` (default: 10) |
| **Timeout** | Partial buffer flushes after `flush_timeout` (default: 1s) |
| **Backpressure** | Queue blocks when full (`max_queue_size`: 100) |
| **Retry** | Retries `max_retries` times (default: 2) before dropping |
| **Hooks** | Extension points for future modules |
| **Graceful Shutdown** | `Ctrl+C` flushes remaining buffer before exit |

## Extension Points

For future drift-detection and coherence scoring:

```python
listener = StreamListener(boundary)

# Called when each event arrives
listener.on_event = lambda e: drift_detector.observe(e)

# Called before/after batch ingestion
listener.on_pre_flush = lambda batch: analyze(batch)
listener.on_post_flush = lambda batch: summarize(batch)

# Called when batch dropped after max retries
listener.on_error = lambda batch, err: log_failure(batch, err)
```

## WebSocket Test Scenarios

The `test_ws_client.py` includes 10 test scenarios that exercise different aspects of the system:

| Scenario | What it Tests |
|----------|---------------|
| Happy Path - Mixed Sources | Valid events from slack, email, system |
| All Event Types | One of each classification type |
| Burst - Trigger Buffer Flush | 12 events to hit flush_threshold |
| Team Updates Only | Multiple team_update keywords |
| External Communications | Email-based external_comm events |
| System Alerts | Error/alert/timeout/failed keywords |
| Batch Send | JSON array (tests batch parsing) |
| Rapid Fire | 20 events at 50ms (backpressure test) |
| Misc/Unclassified | Events that fall through to misc |
| Large Text Payload | Longer text content |

## Linting

```bash
ruff check .        # Full lint
ruff check . --fix  # Auto-fix
ruff format .       # Format code
```

## Tests

| Test | Validates |
|------|-----------|
| `test_ingest_routes_all_events_in_order` | Happy path |
| `test_ingestion_fails_on_missing_fields` | Raw validation |
| `test_ingestion_fails_on_invalid_timestamp` | Timestamp validation |
| `test_normalized_schema_failure` | Normalized validation |
| `test_invariant_failure_on_wrong_order` | Order invariant |
| `test_routing_failure` | Router errors propagate |
| `test_all_events_share_same_trace_id` | Trace consistency |
| `test_buffer_flushes_at_threshold` | Buffer threshold |
| `test_buffer_flushes_on_timeout` | Timeout flush |
| `test_backpressure_rejects_when_full` | Backpressure |
| `test_retry_on_boundary_failure` | Retry logic |
| `test_drop_batch_after_max_retries` | Drop after retries |
| `test_extension_hooks_called` | Hooks invoked |

