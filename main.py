import sys
import json
import asyncio
from datetime import datetime, timezone
from integrationBoundary import IngestionRoutingBoundary
from streamListener import StreamListener, websocket_adapter


def router(event: dict, trace_id: str, event_id: str):
    """Route normalized events. Pluggable for future coherence scoring."""
    print("\n" + "-" * 40)
    print("[ROUTER_DISPATCH]")
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "stage": "router_dispatch",
                "trace_id": trace_id,
                "event_id": event_id,
                "route": event["type"],
            },
            indent=2,
            sort_keys=True,
        )
    )


# --- Batch Mode (Part 1) ---


def run_batch():
    """Run with hardcoded test events (original Part 1 behavior)."""
    test_events = [
        {
            "timestamp": "2025-12-05T08:10:00Z",
            "source": "slack",
            "text": "Team update: sprint retrospective notes",
        },
        {
            "timestamp": "2025-12-05T08:09:30Z",
            "source": "email",
            "text": "Client escalated an issue with the API",
        },
        {
            "timestamp": "2025-12-03T08:12:45Z",
            "source": "system",
            "text": "Error: connection timeout",
        },
    ]
    boundary = IngestionRoutingBoundary(router)
    boundary.ingest(test_events)


# --- Stream Mode (Part 2) ---


async def run_stream():
    """Run real-time stream listener with WebSocket input."""
    try:
        import websockets
    except ImportError:
        print("WebSocket mode requires 'websockets' package: pip install websockets")
        sys.exit(1)

    boundary = IngestionRoutingBoundary(router)
    listener = StreamListener(
        boundary,
        max_queue_size=100,
        flush_threshold=10,
        flush_timeout=1.0,
        max_retries=2,
    )

    async def handle_client(websocket):
        await websocket_adapter(websocket, listener)

    async with websockets.serve(handle_client, "localhost", 8765):
        print("WebSocket server started on ws://localhost:8765")
        await listener.run()


# --- Entry Point ---

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"

    if mode == "stream":
        asyncio.run(run_stream())
    else:
        run_batch()


# Expected Output

"""----------------------------------------
[INGESTION_RECEIVED]
{
  "meta": {
    "count": 3
  },
  "stage": "ingestion_received",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235231+00:00"
}

----------------------------------------
[INGESTION_PROCESSED]
{
  "meta": {
    "count": 3
  },
  "stage": "ingestion_processed",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235369+00:00"
}

----------------------------------------
[ROUTING_ENTER]
{
  "meta": {
    "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:0",
    "type": "system_alert"
  },
  "stage": "routing_enter",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235386+00:00"
}

----------------------------------------
[ROUTER_DISPATCH]
{
  "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:0",
  "route": "system_alert",
  "stage": "router_dispatch",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235400Z"
}

----------------------------------------
[ROUTING_EXIT]
{
  "meta": {
    "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:0"
  },
  "stage": "routing_exit",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235412+00:00"
}

----------------------------------------
[ROUTING_ENTER]
{
  "meta": {
    "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:1",
    "type": "external_comm"
  },
  "stage": "routing_enter",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235424+00:00"
}

----------------------------------------
[ROUTER_DISPATCH]
{
  "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:1",
  "route": "external_comm",
  "stage": "router_dispatch",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235434Z"
}

----------------------------------------
[ROUTING_EXIT]
{
  "meta": {
    "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:1"
  },
  "stage": "routing_exit",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235444+00:00"
}

----------------------------------------
[ROUTING_ENTER]
{
  "meta": {
    "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:2",
    "type": "team_update"
  },
  "stage": "routing_enter",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235458+00:00"
}

----------------------------------------
[ROUTER_DISPATCH]
{
  "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:2",
  "route": "team_update",
  "stage": "router_dispatch",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235469Z"
}

----------------------------------------
[ROUTING_EXIT]
{
  "meta": {
    "event_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8:2"
  },
  "stage": "routing_exit",
  "trace_id": "trace_33e10abe-05f2-4b1e-a077-130b214d4ee8",
  "ts": "2025-12-14T06:15:52.235480+00:00"
}"""
