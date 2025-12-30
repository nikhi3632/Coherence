# Unified Routing Picture

*Router System Diagnosis Overview Synthesis*

**Source Repositories:**
- [Signal-Router-Service](https://github.com/mattmarietta/Signal-Router-Service)
- [router_skeleton_fastapi](https://github.com/ashish-kd/router_skeleton_fastapi/tree/JWT/RBAC-implementation)

---

## The Problem

**People's words often don't match their physiological state.** Someone says "I'm fine" while their HRV shows stress. The text says one thing; the body says another.

**Coherence Protocol** detects this mismatch by fusing linguistic signals with biometric signals, routing them reliably, and acting on what's detected.

**How scoring works:** Signal-Router assigns drift scores to linguistic markers — `"whatever"` → 0.84, `"per my last message"` → 0.70 — then amplifies when HRV is low, tracking patterns over time to output a coherence score.

## Current State: Two Implementations

| Aspect              | router_skeleton_fastapi          | Signal-Router-Service         |
|---------------------|----------------------------------|-------------------------------|
| **Role**            | Routing infrastructure           | Coherence detection engine    |
| **Classification**  | Keyword matching                 | Linguistic + HRV fusion       |
| **AXIS**            | HTTP call to external mock       | IS the analysis engine        |
| **Storage**         | Plaintext PostgreSQL             | Encrypted JSON files          |
| **Reliability**     | Circuit breakers, retries, DLQ   | None                          |
| **Observability**   | Prometheus + Grafana             | Print statements              |
| **Auth**            | JWT + API key                    | None                          |
| **Security**        | SQL injection risk               | Key stored on disk            |

> **Summary:** router_skeleton has infrastructure but no intelligence. Signal-Router has intelligence but no infrastructure.

## Critical Issues

### P0 — Security

| Issue                       | Source         | Impact                                                                 |
|-----------------------------|----------------|------------------------------------------------------------------------|
| **SQL injection**           | router_skeleton| `user_id` flows into SQL via string interpolation. Database compromise possible. |
| **Encryption key on disk**  | Signal-Router  | `secret.key` in plaintext. Filesystem access exposes all PHI.          |

### P1 — Architectural

| Issue                              | Source | Impact                                                                                           |
|------------------------------------|--------|--------------------------------------------------------------------------------------------------|
| **Inverted data flow**             | Both   | Classification happens *before* analysis. Example: "I'm fine" + low HRV has no keywords → routed to DLQ → Signal-Router never sees it. |
| **No shared contract**             | Both   | Different schemas. Integration requires translation layer.                                        |
| **Stateful vs stateless mismatch** | Both   | Signal-Router maintains per-user state. DLQ replay breaks temporal patterns.                      |

### P2 — Operational

| Issue                       | Source         | Impact                                              |
|-----------------------------|----------------|-----------------------------------------------------|
| **Classification brittleness** | router_skeleton | Typos/synonyms fall to DLQ silently.             |
| **No feedback loop**        | Both           | Circuit breaker trips but Signal-Router unaware.    |
| **Detection without action**| Both           | Coherence score hits 0.16 → nothing happens.        |

## Strong Foundations

| What                      | Why                                                                      |
|---------------------------|--------------------------------------------------------------------------|
| **Canonical hashing**     | `SHA256(tenant_id, event_id, payload_version, payload)` → exactly-once   |
| **DLQ with replay**       | Clear failure boundary, message preservation. Replay checks logs table before reprocessing → idempotent.          |
| **Observability separation** | Prometheus/Grafana cleanly separated from routing logic               |
| **Encrypted state**       | Signal-Router's approach to PHI is correct; router_skeleton should adopt |

## Target Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                             INGRESS                                   │
│           router_skeleton_fastapi (auth, dedupe, rate limit)         │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        ANALYSIS (first)                               │
│                     Signal-Router-Service                             │
│        • Drift detection • HRV fusion • Coherence scoring            │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼  coherence_score + classification
┌──────────────────────────────────────────────────────────────────────┐
│                     ROUTING (second, informed)                        │
│                     router_skeleton_fastapi                           │
│        • Route based on analysis output • Fan-out to agents          │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                   ┌─────────────┼─────────────┐
                   ▼             ▼             ▼
                 AXIS            M           DLQ
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          ACTION LAYER                                 │
│        • Threshold alerts • Webhooks • Agent throttling              │
└──────────────────────────────────────────────────────────────────────┘
```

> **Key inversion:** Route ALL signals to Signal-Router-Service *first*, then route based on its output.

## Service Boundaries

| Service                    | Owns                                                                     |
|----------------------------|--------------------------------------------------------------------------|
| **router_skeleton_fastapi**| Ingress, auth, deduplication, rate limiting, retries, DLQ, fan-out, observability |
| **Signal-Router-Service**  | Drift detection, biometric fusion, coherence scoring, pattern analysis   |

Neither service should cross into the other's domain.

## Implementation Roadmap

### Phase 1: Security

| Task                                                  | Owner           | Notes                              |
|-------------------------------------------------------|-----------------|-----------------------------------|
| Replace SQL string interpolation with parameterized queries | router_skeleton | Highest impact                    |
| Validate `user_id` (length, character set)            | router_skeleton | Defense in depth                  |
| Move `secret.key` to environment or secrets manager   | Signal-Router   | Plan rotation                     |
| Add field-level encryption before PostgreSQL          | router_skeleton | Match Signal-Router's PHI handling|

### Phase 2: Integration

| Task                                                  | Owner           | Notes                                    |
|-------------------------------------------------------|-----------------|------------------------------------------|
| Define shared Pydantic contract                       | Both            | Common request/response models           |
| Invert flow: router_skeleton calls Signal-Router first| router_skeleton | Before keyword classification            |
| Add sequence numbers for message ordering             | router_skeleton | Preserve temporal patterns               |
| Implement backpressure signal                         | Both            | Buffer or shed if Signal-Router unhealthy|

### Phase 3: Action Layer

| Task                          | Owner           | Notes                              |
|-------------------------------|-----------------|-----------------------------------|
| Add threshold-based alerts    | New             | Webhook/PagerDuty on low coherence|
| Add agent throttling          | router_skeleton | Reduce load to unhealthy agents   |
| Optional: fuzzy matching      | router_skeleton | Reduce DLQ volume from typos      |

## Verification Checklist

- [ ] No raw SQL interpolation anywhere
- [ ] `user_id` validated at ingress
- [ ] Encryption key not on filesystem
- [ ] PHI encrypted before PostgreSQL
- [ ] All signals pass through Signal-Router before routing decision
- [ ] Shared contract defined and versioned
- [ ] Message ordering preserved for stateful analysis
- [ ] Backpressure mechanism tested
- [ ] Threshold alerts firing correctly
- [ ] Rate-limiter cleanup does not leak memory
- [ ] Circuit-breaker state behaves correctly across async contexts

## Summary

| Layer        | Before                          | After                              |
|--------------|---------------------------------|------------------------------------|
| **Security** | SQL injection, key on disk      | Parameterized queries, secrets mgr |
| **Flow**     | Classify → Route → Analyze      | Analyze → Classify → Route         |
| **Contract** | Two incompatible schemas        | Shared Pydantic models             |
| **State**    | Stateless routing breaks patterns | Sequence numbers preserve order  |
| **Action**   | Detect and log                  | Detect, alert, throttle            |

> **The job:** Wire Signal-Router-Service (intelligence) into router_skeleton_fastapi (infrastructure), with analysis informing routing, encryption throughout, and an action layer that responds to what's detected.
