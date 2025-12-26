# Coherence — Event Ingestion Pipeline

A production-ready event ingestion system with real-time streaming support.

## Overview

This project implements a two-part event processing pipeline:

- **Part 1:** Batch ingestion with validation, classification, and routing
- **Part 2:** Real-time streaming with buffering, backpressure, and retry

Both parts share the same core boundary (`IngestionRoutingBoundary`), ensuring consistent behavior regardless of input method. The boundary serves as the **Axis interface point** — a clean, stable contract for downstream consumers.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         main.py                                 │
│                   (WebSocket server by default)                 │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
              ┌─────────────────────┐
              │  WebSocket Input    │
              │  (test_ws_client)   │
              └──────────┬──────────┘
                         │
               ┌─────────▼─────────┐
               │ streamListener.py │
               │ • buffer          │
               │ • backpressure    │
               │ • retry           │
               │ • hooks           │
               └─────────┬─────────┘
                         │
                         ▼
            ┌─────────────────────────┐
            │ integrationBoundary.py  │  ← AXIS INTERFACE
            │ • validate raw events   │
            │ • enforce invariants    │
            │ • timeout protection    │
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
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Tests

```bash
pytest test.py -v
```

### Start Server (default)

```bash
python main.py
```

This starts the WebSocket server on `ws://localhost:8765`.

### Send Events

In another terminal:

```bash
python test_ws_client.py         # Run all 10 test scenarios once
python test_ws_client.py loop    # Continuous loop (Ctrl+C to stop)
```

Both server and client support **graceful shutdown** — press `Ctrl+C` to stop cleanly. The server flushes any remaining buffered events before exiting.

### Batch Demo (optional)

For quick testing without starting a server:

```bash
python main.py batch
```

This runs hardcoded test events directly through the boundary.

## Boundary Contract

The `IngestionRoutingBoundary` is the Axis interface point. Import what you need:

```python
from integrationBoundary import (
    IngestionRoutingBoundary,
    IngestionValidationError,
    BoundaryInvariantError,
    RoutingError,
    RoutingTimeoutError,
    MAX_TEXT_LENGTH,
    MAX_BATCH_SIZE,
    DEFAULT_ROUTER_TIMEOUT,
)
```

### Guarantees

On successful return from `ingest()`:

| Guarantee | Description |
|-----------|-------------|
| **Count preserved** | No events dropped or duplicated |
| **Chronological order** | Events sorted by timestamp |
| **Trace consistency** | All events in a batch share one `trace_id` |
| **Schema validated** | Both raw and normalized events checked |

### Not Guaranteed

- Router success (errors are raised)
- Global ordering across multiple `ingest()` calls
- Deduplication of logically duplicate events

## Error Handling

Structured exceptions for downstream error handling:

| Exception | Meaning | Action |
|-----------|---------|--------|
| `IngestionValidationError` | Bad input (missing fields, invalid format) | Do not retry — fix input |
| `BoundaryInvariantError` | Internal invariant violated | Alert — indicates pipeline bug |
| `RoutingError` | Router failed to process event | May retry depending on cause |
| `RoutingTimeoutError` | Router exceeded timeout | Retry with backoff |

Example:

```python
from integrationBoundary import (
    IngestionRoutingBoundary,
    IngestionValidationError,
    RoutingError,
    RoutingTimeoutError,
)

boundary = IngestionRoutingBoundary(router, router_timeout=30.0)

try:
    boundary.ingest(events)
except IngestionValidationError as e:
    # Bad input, log and reject
    log.error(f"Invalid event: {e}")
except RoutingTimeoutError as e:
    # Router hung, retry with backoff
    log.warning(f"Timeout on {e.event_id}, retrying...")
except RoutingError as e:
    # Router failed, check cause
    log.error(f"Routing failed: {e.cause}")
```

## Input Limits

| Limit | Value | Configurable |
|-------|-------|--------------|
| Max text length | 10,000 chars | `MAX_TEXT_LENGTH` |
| Max batch size | 1,000 events | `MAX_BATCH_SIZE` |
| Router timeout | 30 seconds | `router_timeout` param |

## Logging

All logs are structured JSON with levels:

| Level | When |
|-------|------|
| `INFO` | Normal operation flow |
| `WARNING` | Retriable failures, retries, degraded state |
| `ERROR` | Non-retriable failures, dropped data |

Example log:

```json
{
  "ts": "2025-12-26T10:00:00.000000+00:00",
  "level": "INFO",
  "stage": "ingestion_exit",
  "trace_id": "trace_abc123",
  "meta": {"count": 5}
}
```

Flush operations include timing:

```json
{
  "level": "INFO",
  "stage": "stream_flush_success",
  "meta": {
    "count": 10,
    "attempt": 1,
    "duration_ms": 45.23
  }
}
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
| `misc` | — | (fallback for unmatched events) |

## Stream Listener Features

| Feature | Description | Default |
|---------|-------------|---------|
| **Buffering** | Events accumulate until threshold | `flush_threshold=10` |
| **Timeout** | Partial buffer flushes on timeout | `flush_timeout=1.0s` |
| **Backpressure** | Queue blocks when full | `max_queue_size=100` |
| **Retry** | Retries before dropping | `max_retries=2` |
| **Timing** | Flush duration logged in ms | — |
| **Graceful Shutdown** | `Ctrl+C` flushes buffer before exit | — |

## Project Structure

```
coherence/
├── main.py                 # Entry point (stream server default, batch optional)
├── eventsIngestion.py      # Core pipeline: sort, classify, normalize
├── integrationBoundary.py  # Hard boundary with validation + invariants
├── streamListener.py       # Real-time: buffer, backpressure, retry
├── events_types.json       # Classification rules
├── test.py                 # 17 unit tests
├── test_ws_client.py       # WebSocket test client (10 scenarios)
├── requirements.txt        # Dependencies
├── archived/               # Previous implementation (reference only)
└── documentations/
    └── SPEC.md             # Original specification
```

## WebSocket Test Scenarios

The `test_ws_client.py` includes 10 scenarios:

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

## Tests

17 unit tests covering:

| Category | Tests |
|----------|-------|
| **Happy Path** | `test_ingest_routes_all_events_in_order` |
| **Input Validation** | `test_ingestion_fails_on_missing_fields`, `test_ingestion_fails_on_invalid_timestamp`, `test_input_text_too_long`, `test_input_batch_too_large`, `test_input_empty_source` |
| **Schema Validation** | `test_normalized_schema_failure` |
| **Invariants** | `test_invariant_failure_on_wrong_order`, `test_all_events_share_same_trace_id` |
| **Routing** | `test_routing_failure`, `test_routing_timeout` |
| **Edge Cases** | `test_empty_batch_succeeds` |
| **Stream Listener** | `test_buffer_flushes_at_threshold`, `test_buffer_flushes_on_timeout`, `test_backpressure_rejects_when_full`, `test_retry_on_boundary_failure`, `test_drop_batch_after_max_retries` |

Run all tests:

```bash
pytest test.py -v
```

## Linting

```bash
ruff check .        # Lint
ruff check . --fix  # Auto-fix
ruff format .       # Format
```
