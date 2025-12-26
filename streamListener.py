"""
Stream Listener Module

Provides real-time event ingestion with:
- Async queue-based input (WebSocket adapter optional)
- Buffering with flush threshold and timeout
- Backpressure via bounded queue
- Retry on failure with graceful degradation
- Graceful shutdown with buffer flush

This module feeds into IngestionRoutingBoundary — it is not a parallel path.
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional


def _log(stage: str, meta: Optional[Dict[str, Any]] = None, level: str = "INFO"):
    """
    Structured logging with levels.

    Levels:
        INFO: Normal operation flow
        WARNING: Retriable failures, degraded state
        ERROR: Non-retriable failures, dropped data
    """
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "stage": stage,
                "meta": meta or {},
            },
            sort_keys=True,
        )
    )


class StreamListener:
    """
    Real-time event listener with buffering and backpressure.

    Events flow: accept() → queue → buffer → flush → boundary.ingest()
    """

    def __init__(
        self,
        boundary,
        max_queue_size: int = 100,
        flush_threshold: int = 10,
        flush_timeout: float = 1.0,
        max_retries: int = 2,
    ):
        self.boundary = boundary
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self.buffer: List[Dict[str, Any]] = []
        self.flush_threshold = flush_threshold
        self.flush_timeout = flush_timeout
        self.max_retries = max_retries
        self._running = False

    async def accept(self, event: Dict[str, Any]):
        """
        Submit an event to the listener.

        Blocks if queue is full (backpressure).
        Any input source (WebSocket, HTTP, test code) calls this.
        """
        await self.queue.put(event)

    def accept_nowait(self, event: Dict[str, Any]):
        """
        Submit an event without waiting.

        Raises asyncio.QueueFull if queue is at capacity.
        Use for strict backpressure where blocking is not acceptable.
        """
        self.queue.put_nowait(event)

    async def run(self):
        """
        Main processing loop.

        Reads from queue, buffers events, flushes on threshold or timeout.
        Call stop() to exit gracefully.
        """
        self._running = True
        _log(
            "stream_listener_started",
            {
                "flush_threshold": self.flush_threshold,
                "flush_timeout": self.flush_timeout,
                "max_queue_size": self.queue.maxsize,
            },
        )

        while self._running:
            try:
                event = await asyncio.wait_for(
                    self.queue.get(), timeout=self.flush_timeout
                )
                self.buffer.append(event)

                if len(self.buffer) >= self.flush_threshold:
                    await self._flush()

            except asyncio.TimeoutError:
                # Flush whatever we have on timeout
                if self.buffer:
                    await self._flush()

        # Final flush on shutdown
        if self.buffer:
            await self._flush()
        _log("stream_listener_stopped")

    def stop(self):
        """Signal the listener to stop after current iteration."""
        self._running = False

    async def _flush(self):
        """Flush buffer to boundary with retry logic and timing."""
        if not self.buffer:
            return

        batch = self.buffer
        self.buffer = []

        _log("stream_flush_attempt", {"count": len(batch)})
        flush_start = time.monotonic()

        for attempt in range(1, self.max_retries + 2):  # +2 because range is exclusive
            try:
                attempt_start = time.monotonic()
                self.boundary.ingest(batch)
                attempt_duration_ms = (time.monotonic() - attempt_start) * 1000

                total_duration_ms = (time.monotonic() - flush_start) * 1000
                _log(
                    "stream_flush_success",
                    {
                        "count": len(batch),
                        "attempt": attempt,
                        "duration_ms": round(total_duration_ms, 2),
                        "attempt_duration_ms": round(attempt_duration_ms, 2),
                    },
                )
                return

            except Exception as e:
                attempt_duration_ms = (time.monotonic() - flush_start) * 1000
                _log(
                    "stream_flush_failed",
                    {
                        "count": len(batch),
                        "attempt": attempt,
                        "error": type(e).__name__,
                        "duration_ms": round(attempt_duration_ms, 2),
                    },
                    level="WARNING",
                )

                if attempt <= self.max_retries:
                    await asyncio.sleep(0.1 * attempt)  # backoff
                    continue

                # Max retries exhausted - drop batch
                total_duration_ms = (time.monotonic() - flush_start) * 1000
                _log(
                    "stream_batch_dropped",
                    {
                        "count": len(batch),
                        "error": str(e),
                        "total_duration_ms": round(total_duration_ms, 2),
                    },
                    level="ERROR",
                )


# --- Optional WebSocket Adapter ---


async def websocket_adapter(websocket, listener: StreamListener):
    """
    Thin adapter that feeds WebSocket messages into the listener.

    Usage:
        async with websockets.serve(
            lambda ws: websocket_adapter(ws, listener),
            "localhost", 8765
        ):
            await listener.run()
    """
    _log("ws_client_connected")
    try:
        async for message in websocket:
            try:
                event = json.loads(message)
                if isinstance(event, list):
                    for e in event:
                        await listener.accept(e)
                else:
                    await listener.accept(event)
            except json.JSONDecodeError:
                _log(
                    "ws_invalid_json",
                    {"raw": message[:100]},
                    level="WARNING",
                )
    except Exception as e:
        _log(
            "ws_client_disconnected",
            {"reason": type(e).__name__},
            level="WARNING",
        )
