"""
Stream Listener Module

Provides real-time event ingestion with:
- Async queue-based input (WebSocket adapter optional)
- Buffering with flush threshold and timeout
- Backpressure via bounded queue
- Retry on failure with graceful degradation
- Extension hooks for future modules (drift-detection, coherence scoring)
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Callable


def _log(stage: str, meta: Optional[Dict[str, Any]] = None):
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
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

    Extension points (register callbacks for future modules):
        - on_event: called when event arrives (drift-detection)
        - on_pre_flush: called before batch ingestion
        - on_post_flush: called after successful ingestion
        - on_error: called when batch dropped after max retries
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

        # Extension hooks (None by default, future modules register here)
        self.on_event: Optional[Callable[[Dict[str, Any]], None]] = None
        self.on_pre_flush: Optional[Callable[[List[Dict[str, Any]]], None]] = None
        self.on_post_flush: Optional[Callable[[List[Dict[str, Any]]], None]] = None
        self.on_error: Optional[Callable[[List[Dict[str, Any]], Exception], None]] = (
            None
        )

    async def accept(self, event: Dict[str, Any]):
        """
        Submit an event to the listener.

        Blocks if queue is full (backpressure).
        Any input source (WebSocket, HTTP, test code) calls this.
        """
        if self.on_event:
            self.on_event(event)
        await self.queue.put(event)

    def accept_nowait(self, event: Dict[str, Any]):
        """
        Submit an event without waiting.

        Raises asyncio.QueueFull if queue is at capacity.
        Use for strict backpressure where blocking is not acceptable.
        """
        if self.on_event:
            self.on_event(event)
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
        """Flush buffer to boundary with retry logic."""
        if not self.buffer:
            return

        batch = self.buffer
        self.buffer = []

        if self.on_pre_flush:
            self.on_pre_flush(batch)

        _log("stream_flush_attempt", {"count": len(batch)})

        for attempt in range(1, self.max_retries + 2):  # +2 because range is exclusive
            try:
                self.boundary.ingest(batch)
                _log("stream_flush_success", {"count": len(batch), "attempt": attempt})

                if self.on_post_flush:
                    self.on_post_flush(batch)
                return

            except Exception as e:
                _log(
                    "stream_flush_failed",
                    {
                        "count": len(batch),
                        "attempt": attempt,
                        "error": type(e).__name__,
                    },
                )

                if attempt <= self.max_retries:
                    await asyncio.sleep(0.1 * attempt)  # backoff
                    continue

                # Max retries exhausted - drop batch
                _log("stream_batch_dropped", {"count": len(batch), "error": str(e)})

                if self.on_error:
                    self.on_error(batch, e)


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
                _log("ws_invalid_json", {"raw": message[:100]})
    except Exception as e:
        _log("ws_client_disconnected", {"reason": type(e).__name__})
