# Alignment assignment

This is structured it as a two-week assignment with two clean parts.

Part 1 builds directly on the pipeline work you just completed, and Part 2 extends it into the real-time system so we can start connecting it to live signals.

**Part 1 (Week 1): Modular Ingestion Integration**

Goal: connect the ingestion pipeline you built into the broader event-flow architecture.

Scope:

- Create a clean interface layer between your ingestion module and the system’s event router.
- Add predictable data contracts (schemas, field guarantees, validation).
- Implement minimal logging hooks so we can trace ingestion → routing in a deterministic way.
- Ensure the module can accept multiple event types without changing core logic.

Deliverable:

A PR with the ingestion engine running end-to-end through the routing boundary, with logs that make each step traceable.

**Part 2 (Week 2): Real-Time Stream Extension**

Goal: extend the ingestion engine to support real-time inputs so we can eventually feed it into the coherence layer.

Scope:

- Add a lightweight stream listener (WebSocket or async queue).
- Implement buffering + backpressure handling for bursty inputs.
- Add graceful error recovery and retry semantics.
- Keep the structure modular so we can later attach drift-detection and coherence scoring.

Deliverable:

A working stream-enabled ingestion path that handles events in real time and exposes clean extension points for future modules.

Let me know if anything here needs clarification before you begin.