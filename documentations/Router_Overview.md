# Router System Diagnosis

## System Overview

### Signal-Router-Service (PoC)

Ingests biometric and chat signals → routes to an AXIS agent stub → persists encrypted history in a file. Designed for emotional coherence detection from HRV and text data. State is file-based with encryption using a persistent key (`secret.key`).

### router_skeleton_fastapi (production-oriented)

Multi-tenant message router. Requests flow through:

```
canonicalization → deduplication → classification → parallel agent fan-out → DLQ on failure
```

Uses PostgreSQL for persistence, Prometheus/Grafana for observability, and includes rate limiting, circuit breakers, and DLQ auto-replay.

---

## Key Invariants

| Invariant | Implementation |
|-----------|----------------|
| **Exactly-once semantics** | Deterministic `message_id = SHA256(tenant_id, event_id, payload_version, canonical_payload)` with `PRIMARY KEY` on `logs.log_id`. Duplicate requests return the original routing result. |
| **Replay safety** | DLQ replay checks the logs table before reprocessing, preserving idempotency. |
| **Classification boundary** | Pure keyword-based routing: `assist → Axis`, `policy → M`, `emergency → both`, `unknown → DLQ`. No fuzzy or semantic fallback. |
| **State management** | Signal-Router uses file-based encrypted state; router_skeleton uses PostgreSQL with indexed lookups and a DLQ table. |

---

## Critical Findings

### 1. CRITICAL — SQL Injection via User-Controlled Fields

`user_id` flows into SQL inserts as `sender_id_for_db` using string interpolation rather than parameterized queries. Some JSON fields attempt quote escaping, but `sender_id_for_db` is inserted raw. A comment noting avoidance of parameter binding suggests a misunderstanding of its security guarantees.

**Impact:** Crafted input can lead to database compromise or data loss. Similar patterns appear in routing and DLQ replay paths.

### 2. HIGH — Encryption Key Stored on Disk (Signal-Router)

The encryption key is loaded from a plaintext `secret.key` file. Filesystem access would expose all historical biometric and chat data. No rotation or access control.

### 3. MEDIUM — Classification Brittleness

Strict keyword matching causes typos and synonyms to fall into DLQ silently, reducing routing accuracy and observability.

---

## Boundary and Correctness Analysis

### Strong Boundaries

- Canonical hashing + database constraints correctly enforce idempotency
- DLQ provides a clear failure boundary and preserves messages
- Observability concerns are cleanly separated from routing logic

### Weak Boundaries

- SQL layer breaks trust boundaries by embedding user input directly
- Classification logic is tightly coupled to routing, limiting testability and extensibility
- No input validation on `user_id` (length or character set)

### Verification Points

- Confirm rate-limiter cleanup does not leak memory
- Confirm circuit-breaker state behaves correctly across async contexts

---

## Minimal Improvements (Prioritized)

| Priority | Improvement |
|----------|-------------|
| **1** | Replace all raw SQL string interpolation with parameterized queries (highest impact) |
| **2** | Validate `user_id` (length and allowed characters) as defense in depth |
| **3** | Move encryption key to environment or a secrets manager and plan basic rotation |
| **4** | Optional: add light fuzzy matching or stemming to reduce DLQ volume from typos |

---

## Summary

**router_skeleton_fastapi** is architecturally sound with strong deduplication and DLQ semantics, but correctness and security could be undermined by raw SQL construction on user input.

**Signal-Router-Service** is a clean PoC that needs basic production hardening, especially around key management.

---

**Immediate priority:** Parameterized SQL and input validation.

**Next:** Secure key handling and modest classification robustness improvements.
