import asyncio
import json
from datetime import datetime, timezone, timedelta
import websockets


def make_event(ts, source, text):
    return {
        "timestamp": ts.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": source,
        "text": text,
    }


async def main():
    # Base timestamp for streaming events
    base = datetime.now(timezone.utc)

    # Initial batch (simulates buffered ingestion)
    events = [
        make_event(base, "email", "Client escalated an issue"),
        make_event(base + timedelta(seconds=1), "system", "Error: timeout"),
        make_event(base + timedelta(seconds=2), "slack", "Sprint update"),
    ]

    print("Connecting to ws://localhost:8765")

    async with websockets.connect("ws://localhost:8765") as ws:
        print("Connected")

        # Sending initial batch
        await ws.send(json.dumps(events))
        print(f"Sent initial batch of {len(events)} events")

        # Sending bursty real-time traffic
        for i in range(30):
            event = make_event(
                base + timedelta(milliseconds=i * 15), "slack", f"Team update {i}"
            )
            await ws.send(json.dumps(event))
            await asyncio.sleep(0.5)

        print("Sent burst stream")


if __name__ == "__main__":
    asyncio.run(main())


# Expected Output

"""
================================================================================
[WS_CONNECTED]
{
  "meta": {},
  "stage": "ws_connected",
  "ts": "2025-12-23T21:03:21.919972+00:00"
}

================================================================================
[WS_BATCH_ATTEMPT]
{
  "meta": {
    "attempt": 1,
    "count": 33
  },
  "stage": "ws_batch_attempt",
  "ts": "2025-12-23T21:03:36.970893+00:00"
}
{"meta": {"count": 33}, "stage": "ingestion_enter", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "ts": "2025-12-23T21:03:36.971125+00:00"}
{"meta": {"count": 33}, "stage": "ingestion_exit", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "ts": "2025-12-23T21:03:36.971389+00:00"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:0", "type": "external_comm", "source": "email"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:1", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:2", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:3", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:4", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:5", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:6", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:7", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:8", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:9", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:10", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:11", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:12", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:13", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:14", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:15", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:16", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:17", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:18", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:19", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:20", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:21", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:22", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:23", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:24", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:25", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:26", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:27", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:28", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:29", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:30", "type": "team_update", "source": "slack"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:31", "type": "system_alert", "source": "system"}
{"stage": "router_dispatch", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "event_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2:32", "type": "team_update", "source": "slack"}
{"meta": {"count": 33}, "stage": "routing_complete", "trace_id": "trace_c80f0eec-1185-4163-bba5-ec0df36d27e2", "ts": "2025-12-23T21:03:36.971794+00:00"}

================================================================================
[WS_BATCH_INGESTED]
{
  "meta": {
    "attempt": 1,
    "count": 33
  },
  "stage": "ws_batch_ingested",
  "ts": "2025-12-23T21:03:36.971828+00:00"
  """
