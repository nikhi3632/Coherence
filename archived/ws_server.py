import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import websockets

from integrationBoundary import IngestionRoutingBoundary


def log(stage: str, meta: Optional[Dict[str, Any]] = None):
    print("\n" + "=" * 80)
    print(f"[{stage.upper()}]")
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                "meta": meta or {},
            },
            indent=2,
            sort_keys=True,
        )
    )


class WebSocketIngestionServer:
    """
    Real-time ingestion server.
    Receives raw events over WebSocket, buffers them, and feeds batches
    into the existing ingestion → routing boundary.
    """

    def __init__(
        self,
        boundary: IngestionRoutingBoundary,
        buffer_size: int = 50,
        flush_interval_sec: float = 0.25,
    ):
        self.boundary = boundary
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_size = buffer_size
        self.flush_interval_sec = flush_interval_sec
        self.lock = asyncio.Lock()

    async def handler(self, websocket):
        log("ws_connected")

        try:
            async for message in websocket:
                try:
                    event = json.loads(message)
                except json.JSONDecodeError:
                    log("ws_invalid_json", {"raw": message})
                    continue

                await self._add_event(event)

        except websockets.ConnectionClosed:
            log("ws_disconnected")
        finally:
            await self._flush()

    async def _add_event(self, payload):
        async with self.lock:
            # Normalize payload shape
            if isinstance(payload, list):
                self.buffer.extend(payload)
            elif isinstance(payload, dict):
                self.buffer.append(payload)
            else:
                log("ws_invalid_payload", {"type": str(type(payload))})
                return

            if len(self.buffer) >= self.buffer_size:
                await self._flush()

    async def _flush(self):
        async with self.lock:
            if not self.buffer:
                return

            batch = self.buffer
            self.buffer = []

        max_retries = 1
        attempt = 0

        while True:
            try:
                attempt += 1
                log("ws_batch_attempt", {"count": len(batch), "attempt": attempt})

                self.boundary.ingest(batch)

                log("ws_batch_ingested", {"count": len(batch), "attempt": attempt})
                return

            except Exception as e:
                log(
                    "ws_batch_failed",
                    {
                        "count": len(batch),
                        "attempt": attempt,
                        "error": type(e).__name__,
                    },
                )

                if attempt > max_retries:
                    log(
                        "ws_batch_dropped",
                        {"count": len(batch), "final_error": type(e).__name__},
                    )
                    return

                await asyncio.sleep(0.1)


async def main():
    def router(event, trace_id, event_id):
        print(
            json.dumps(
                {
                    "stage": "router_dispatch",
                    "trace_id": trace_id,
                    "event_id": event_id,
                    "type": event["type"],
                    "source": event["source"],
                }
            )
        )

    boundary = IngestionRoutingBoundary(router)
    server = WebSocketIngestionServer(boundary)

    async with websockets.serve(server.handler, "localhost", 8765):
        log("ws_server_started", {"port": 8765})
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
