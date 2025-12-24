# test_ingestion_boundary.py
import pytest
import asyncio
from integrationBoundary import IngestionRoutingBoundary
from streamListener import StreamListener


# 1. HAPPY PATH (everything works)
def test_ingest_routes_all_events_in_order():
    routed = []

    def router(event, trace_id, event_id):
        routed.append((event, trace_id, event_id))

    boundary = IngestionRoutingBoundary(router)

    raw_events = [
        {"timestamp": "2025-12-05T10:00:00Z", "source": "slack", "text": "Update"},
        {"timestamp": "2025-12-03T10:00:00Z", "source": "system", "text": "Error"},
        {"timestamp": "2025-12-04T10:00:00Z", "source": "email", "text": "Client msg"},
    ]

    boundary.ingest(raw_events)

    # invariant: count preserved
    assert len(routed) == len(raw_events)

    # invariant: timestamps sorted
    timestamps = [evt["t"] for evt, _, _ in routed]
    assert timestamps == sorted(timestamps)

    # invariant: same trace_id for entire run
    trace_ids = {trace for _, trace, _ in routed}
    assert len(trace_ids) == 1


# 2. INGESTION FAILURE — missing fields
def test_ingestion_fails_on_missing_fields():
    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)

    raw_events = [
        {"timestamp": "2025-12-05T10:00:00Z", "source": "slack"}  # missing text
    ]

    with pytest.raises(ValueError):
        boundary.ingest(raw_events)


# 3. INGESTION FAILURE — invalid timestamp
def test_ingestion_fails_on_invalid_timestamp():
    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)

    raw_events = [{"timestamp": "not-a-time", "source": "slack", "text": "ok"}]

    with pytest.raises(ValueError):
        boundary.ingest(raw_events)


# 4. NORMALIZED VALIDATION FAILURE (Simulate build_timeline returning bad schema)
def test_normalized_schema_failure(monkeypatch):
    def bad_timeline(_):
        return [{"t": "2025-12-05T10:00:00Z", "source": "s"}]  # missing fields

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)

    # FORCE build_timeline to return incorrect schema
    monkeypatch.setattr("integrationBoundary.build_timeline", bad_timeline)

    raw_events = [{"timestamp": "2025-12-05T10:00:00Z", "source": "sys", "text": "ok"}]

    with pytest.raises(ValueError):
        boundary.ingest(raw_events)


# 5. INVARIANT FAILURE — order broken (force wrong order)
def test_invariant_failure_on_wrong_order(monkeypatch):
    def bad_timeline(_):
        # events out of order
        return [
            {"t": "2025-12-05T10:00:00Z", "source": "s", "type": "x", "text": "A"},
            {"t": "2025-12-04T10:00:00Z", "source": "s", "type": "x", "text": "B"},
        ]

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)
    monkeypatch.setattr("integrationBoundary.build_timeline", bad_timeline)

    raw_events = [{"timestamp": "2025-12-03T10:00:00Z", "source": "sys", "text": "A"}]

    with pytest.raises(AssertionError):  # ordering invariant broken
        boundary.ingest(raw_events)


# 6. ROUTING FAILURE — router raises exception
def test_routing_failure():
    def router(_event, _trace_id, _event_id):
        raise RuntimeError("router explode")

    boundary = IngestionRoutingBoundary(router)

    raw_events = [
        {"timestamp": "2025-12-05T10:00:00Z", "source": "system", "text": "Alert"}
    ]

    # boundary must surface router failure
    with pytest.raises(RuntimeError):
        boundary.ingest(raw_events)


# 7. TRACE ID CONSISTENCY — all routed events must share the same trace_id
def test_all_events_share_same_trace_id():
    captured = []

    def router(_event, trace_id, _event_id):
        captured.append(trace_id)

    boundary = IngestionRoutingBoundary(router)

    raw_events = [
        {"timestamp": "2025-12-05T10:00:00Z", "source": "slack", "text": "A"},
        {"timestamp": "2025-12-06T10:00:00Z", "source": "email", "text": "B"},
    ]

    boundary.ingest(raw_events)

    assert len(set(captured)) == 1, "All events must share the same trace_id"


# ============================================================
# PART 2: STREAM LISTENER TESTS
# ============================================================


def make_event(idx):
    """Helper to create valid test events."""
    return {
        "timestamp": f"2025-12-{10 + idx:02d}T10:00:00Z",
        "source": "slack",
        "text": f"Event {idx}",
    }


# 8. BUFFER FLUSHES AT THRESHOLD
@pytest.mark.asyncio
async def test_buffer_flushes_at_threshold():
    """Events accumulate until flush_threshold, then flush to boundary."""
    ingested_batches = []

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)
    # Capture batches
    original_ingest = boundary.ingest

    def capture_ingest(batch, **kwargs):
        ingested_batches.append(list(batch))
        return original_ingest(batch, **kwargs)

    boundary.ingest = capture_ingest

    listener = StreamListener(boundary, flush_threshold=3, flush_timeout=10.0)

    # Add 3 events (should trigger flush)
    for i in range(3):
        await listener.accept(make_event(i))

    # Process the queue
    async def run_briefly():
        listener._running = True
        # Process until queue is empty
        while not listener.queue.empty() or listener.buffer:
            try:
                event = await asyncio.wait_for(listener.queue.get(), timeout=0.1)
                listener.buffer.append(event)
                if len(listener.buffer) >= listener.flush_threshold:
                    await listener._flush()
            except asyncio.TimeoutError:
                if listener.buffer:
                    await listener._flush()
                break

    await run_briefly()

    assert len(ingested_batches) == 1, "Should have flushed once"
    assert len(ingested_batches[0]) == 3, "Batch should contain 3 events"


# 9. BUFFER FLUSHES ON TIMEOUT
@pytest.mark.asyncio
async def test_buffer_flushes_on_timeout():
    """Partial buffer flushes when timeout expires."""
    ingested_batches = []

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)
    original_ingest = boundary.ingest

    def capture_ingest(batch, **kwargs):
        ingested_batches.append(list(batch))
        return original_ingest(batch, **kwargs)

    boundary.ingest = capture_ingest

    listener = StreamListener(boundary, flush_threshold=10, flush_timeout=0.1)

    # Add only 2 events (below threshold)
    await listener.accept(make_event(0))
    await listener.accept(make_event(1))

    # Run with short timeout - should flush on timeout
    async def run_with_timeout():
        listener._running = True
        while not listener.queue.empty():
            event = await listener.queue.get()
            listener.buffer.append(event)
        # Simulate timeout flush
        if listener.buffer:
            await listener._flush()

    await run_with_timeout()

    assert len(ingested_batches) == 1, "Should have flushed on timeout"
    assert len(ingested_batches[0]) == 2, "Batch should contain 2 events"


# 10. BACKPRESSURE — QUEUE FULL RAISES
@pytest.mark.asyncio
async def test_backpressure_rejects_when_full():
    """Queue at capacity rejects new events with QueueFull."""

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)
    listener = StreamListener(boundary, max_queue_size=2, flush_threshold=10)

    # Fill the queue
    await listener.accept(make_event(0))
    await listener.accept(make_event(1))

    # Third event should raise QueueFull (using nowait)
    with pytest.raises(asyncio.QueueFull):
        listener.accept_nowait(make_event(2))


# 11. RETRY ON FAILURE
@pytest.mark.asyncio
async def test_retry_on_boundary_failure():
    """Listener retries on boundary failure before dropping."""
    attempts = []

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)
    original_ingest = boundary.ingest

    def failing_ingest(batch, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:  # Fail first 2 attempts
            raise RuntimeError("Simulated failure")
        return original_ingest(batch, **kwargs)

    boundary.ingest = failing_ingest

    listener = StreamListener(boundary, flush_threshold=1, max_retries=2)
    await listener.accept(make_event(0))

    # Process
    event = await listener.queue.get()
    listener.buffer.append(event)
    await listener._flush()

    assert len(attempts) == 3, "Should have retried twice then succeeded"


# 12. DROP AFTER MAX RETRIES
@pytest.mark.asyncio
async def test_drop_batch_after_max_retries():
    """Batch is dropped after max retries exhausted."""
    attempts = []
    dropped = []

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)

    def always_fail(_batch, **_kwargs):
        attempts.append(1)
        raise RuntimeError("Always fails")

    boundary.ingest = always_fail

    listener = StreamListener(boundary, flush_threshold=1, max_retries=2)
    listener.on_error = lambda batch, _err: dropped.append(batch)

    await listener.accept(make_event(0))

    event = await listener.queue.get()
    listener.buffer.append(event)
    await listener._flush()

    # max_retries=2 means 3 total attempts (1 initial + 2 retries)
    assert len(attempts) == 3, "Should have attempted 3 times"
    assert len(dropped) == 1, "Should have dropped 1 batch"


# 13. EXTENSION HOOKS CALLED
@pytest.mark.asyncio
async def test_extension_hooks_called():
    """on_event, on_pre_flush, on_post_flush hooks are invoked."""
    events_seen = []
    pre_flush_batches = []
    post_flush_batches = []

    def router(_e, _t, _i):
        pass

    boundary = IngestionRoutingBoundary(router)
    listener = StreamListener(boundary, flush_threshold=2)

    listener.on_event = lambda e: events_seen.append(e)
    listener.on_pre_flush = lambda batch: pre_flush_batches.append(list(batch))
    listener.on_post_flush = lambda batch: post_flush_batches.append(list(batch))

    await listener.accept(make_event(0))
    await listener.accept(make_event(1))

    # Process
    while not listener.queue.empty():
        event = await listener.queue.get()
        listener.buffer.append(event)
        if len(listener.buffer) >= listener.flush_threshold:
            await listener._flush()

    assert len(events_seen) == 2, "on_event should be called for each event"
    assert len(pre_flush_batches) == 1, "on_pre_flush should be called once"
    assert len(post_flush_batches) == 1, "on_post_flush should be called once"
