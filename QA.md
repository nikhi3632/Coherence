# Interview Q&A Prep

## 1. What is the smallest concrete change you would make to invert the flow so Signal-Router-Service sees every signal first — without rewriting the system?

**Add AXIS as a required first hop in `route_to_agents()` before the switch on `kind`.**

Location: `router_skeleton_fastapi/app/router.py`

```python
# In route_to_agents(), after line 272 (ingress metric), before agents_for(kind):

# === Always call AXIS first for coherence analysis ===
axis_response = await call_agent("Axis", {**payload, "trace_id": trace_id}, trace_id)
if axis_response:
    coherence_score = axis_response.get("coherence_score", 1.0)
    payload["coherence_score"] = coherence_score
    # Override kind based on coherence score
    if coherence_score < 0.3:
        kind = "emergency"  # Low coherence = escalate
    elif kind == "unknown" and coherence_score > 0.5:
        kind = "assist"     # Unknown but coherent = probably assist
# === END ===

# Existing code continues: agents = await agents_for(kind)
```

This ensures **Signal-Router-Service** (AXIS) sees every signal and can influence routing decisions based on coherence score, without restructuring the existing fan-out logic.

---

## 2. You called out ordering as critical for drift detection. If Kafka ordering isn't available, what's your fallback — and what correctness do we lose?

**Fallback:** Include `(user_id, sequence_number)` in every payload. **Signal-Router-Service** buffers incoming messages per user for a short window (e.g., 200ms), sorts by sequence number, then processes in order.

### How to generate sequence_number

**Client-generated (preserves logical order)**

The **client** (source of the signal) maintains a counter per session:

```python
# Client side
class SignalClient:
    def __init__(self, user_id):
        self.user_id = user_id
        self.seq = 0
    
    def send(self, text, hrv):
        self.seq += 1
        return {
            "user_id": self.user_id,
            "sequence_number": self.seq,
            "text": text,
            "hrv": hrv
        }
```

**Why client-generated for Coherence:** Drift detection cares about *when the user actually said something*, not when the server received it. A message like "I'm fine" followed by "actually, I'm not okay" has meaning in that order — network delays shouldn't flip them.

### What we lose without Kafka

- Real-time detection becomes near-real-time (200ms delay)
- If buffer window is too short, late messages still arrive out of order → "sudden shift" detection has false positives/negatives
- Memory pressure: need to hold messages per user in memory

**Acceptable tradeoff:** For coherence scoring, 200ms latency is fine. We're not doing trading — we're detecting drift over conversations that span minutes/hours. Slightly degraded ordering is acceptable; silent message loss is not.

---

## 3. Name one invariant you would enforce in code, not documentation — and where would you enforce it?

**Invariant:** *"No message can be routed to DLQ without a coherence score."*

**Where:** This is already enforced by the Q1 change. The AXIS-first pattern guarantees `coherence_score` is attached before any routing decision:

```python
# From Q1 — AXIS runs first, always attaches score
axis_response = await call_agent("Axis", {**payload, "trace_id": trace_id}, trace_id)
if axis_response:
    coherence_score = axis_response.get("coherence_score", 1.0)
    payload["coherence_score"] = coherence_score  # ← invariant enforced here
```

**Add a guard** for the edge case where AXIS fails (circuit breaker open, timeout):

```python
# After the AXIS call block
if "coherence_score" not in payload:
    payload["coherence_score"] = None  # Explicit marker
    kind = "dlq_pending"  # New status: "needs retry, not failed"

# Also add "dlq_pending" to KIND_MAP:
# KIND_MAP["dlq_pending"] = ["DLQ"]  # Route to DLQ for retry
```

This turns "AXIS is down" into a retryable state, not a silent drop. The `dlq_pending` status is distinct from `unknown` — it means "we couldn't analyze" rather than "we analyzed and don't know."

**Why:** This invariant is the difference between "lost signal" and "analyzed but unroutable signal."

---

## 4. You suggested field-level encryption before Postgres. What would you intentionally leave unencrypted, and why?

**Problem: No shared schema.** The two implementations have incompatible contracts:

| router_skeleton_fastapi | Signal-Router-Service |
|-------------------------|----------------------|
| `tenant_id` | `user_id` |
| `event_id` | `session_id` |
| `payload_version` | — |
| — | `hrv_data` |
| `payload` (generic) | `text_context` |

**Encryption must happen at each boundary, not shared:**

### router_skeleton_fastapi (cleartext for operations)

| Field | Encrypt? | Reason |
|-------|----------|--------|
| `tenant_id` | No | Sharding, access control, query filtering |
| `event_id` / `message_id` | No | Deduplication index lookups |
| `ts` | No | Ordering, TTL, time-range queries |
| `kind` | No | Routing logic, metrics aggregation |
| `payload` | **Yes** | Contains user content |

### Signal-Router-Service (cleartext for analysis)

| Field | Encrypt? | Reason |
|-------|----------|--------|
| `user_id` | No | User state lookups, drift history |
| `session_id` | No | Conversation grouping |
| `text_context` | **Yes** | PHI — actual user words |
| `hrv_data` | **Yes** | PHI — biometric data |

### Why leave these fields unencrypted?

If you encrypt everything:

1. **No indexing** — Postgres can't build B-tree indexes on encrypted columns. Query "find all messages for tenant X" becomes a full table scan + decrypt every row.

2. **No sharding** — Multi-tenant systems shard by `tenant_id`. If it's encrypted, the router can't route to the right shard without decrypting first — defeating the point.

3. **No deduplication** — `message_id` must be comparable in cleartext. If encrypted, two identical messages produce different ciphertexts (due to IV/nonce), breaking exactly-once.

4. **No time-based queries** — "Show me signals from the last hour" requires `ts` in cleartext. Encrypted timestamps mean decrypting entire table to filter.

5. **No metrics** — Prometheus/Grafana can't aggregate by `kind` if it's encrypted. You lose "emergency signals per minute" dashboards.

**The tradeoff:** These fields are *identifiers*, not *content*. Knowing "tenant_123 sent a message at 10:42 classified as emergency" is operational metadata. Knowing *what they said* and *their heart rate* is PHI. Encrypt the content, not the envelope.

### Translation layer handles schema boundary

```python
# At router → AXIS boundary
def translate_for_axis(router_payload: dict) -> dict:
    """Decrypt router payload, map to AXIS schema.
    AXIS receives cleartext — it needs to analyze content.
    Encryption happens at rest in each service's database.
    """
    decrypted = decrypt(router_payload["payload"])
    return {
        "user_id": router_payload["tenant_id"],  # Mapping
        "session_id": router_payload["event_id"],
        "text_context": decrypted["text"],       # Cleartext for analysis
        "hrv_data": decrypted.get("biometrics", {}).get("hrv")
    }
```

**Why this design:**
- **Router** stores `payload` encrypted at rest — never reads PHI, only routes by `kind`
- **AXIS** receives cleartext to analyze, stores results encrypted at rest
- **Translation layer** is the only place decryption happens — single audit point
- Each service owns its encryption keys — compromise of one doesn't expose the other

---

## 5. What part of this system do you think we should not touch for the next 4–6 weeks?

**Don't touch: Canonical message_id generation** (`generate_canonical_message_id()` in `router_skeleton_fastapi/app/router.py`)

**Why:**
- It's the foundation of exactly-once processing
- It has comprehensive test coverage (`test_canonicalization_comprehensive.py`)
- Any change risks silent duplicate processing or false rejections
- It's stable and working — there's no burning need to change it

**What to focus on instead:** The integration layer between the two services. That's where the value is, and it doesn't require touching the proven deduplication logic.

