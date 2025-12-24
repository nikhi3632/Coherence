# Router Implementations: One-pager

## The Problem Statement (from what I gather)

**People's words often don't match their physiological state.** Someone says "I'm fine" while their HRV shows stress. A team member sends a dismissive message while their biometrics indicate frustration. The text says one thing; the body says another.

**Coherence Protocol detects this mismatch** by fusing linguistic signals (what someone says) with biometric signals (what their body shows), routing those mixed signals to the right handlers, and ensuring nothing gets lost.

The goal is to build infrastructure that can ingest mixed signals at scale, analyze them intelligently, route them reliably, and act on what's detected while keeping sensitive health data secure.

---

## What Each Does

**[Signal-Router-Service](https://github.com/mattmarietta/Signal-Router-Service)** is a **coherence detection engine**. It takes text + biometrics (HRV) and determines if someone's words match their physiological state. Scores linguistic drift markers ("whatever" → 0.84, "per my last message" → 0.70), amplifies when HRV is low, tracks patterns over time, outputs a coherence score. Stores everything encrypted. Single-tenant, no reliability patterns, no scale.

**[router_skeleton_fastapi](https://github.com/ashish-kd/router_skeleton_fastapi/tree/JWT/RBAC-implementation)** is **routing infrastructure**. It accepts signals from multiple tenants, deduplicates via canonical message_id (SHA256 of tenant + event + payload), classifies by keyword matching, fans out to downstream agents (AXIS, M, or DLQ), handles failures with circuit breakers and retries, replays from DLQ automatically. Full Prometheus/Grafana observability. Stores plaintext in Postgres.

---

## Where They Overlap

- Accept text + biometric signals
- Classify signals
- Route to AXIS
- Log everything

## Where They Differ

| Aspect         | router_skeleton_fastapi        | Signal-Router-Service      |
| -------------- | ------------------------------ | -------------------------- |
| Classification | Keyword matching               | Linguistic + HRV fusion    |
| AXIS role      | HTTP call to external mock     | IS the analysis engine     |
| Storage        | Plaintext Postgres             | Encrypted JSON             |
| Schema         | Multi-tenant, flexible         | Single-tenant, HRV-focused |
| Reliability    | Circuit breakers, retries, DLQ | None                       |
| Observability  | Prometheus + Grafana           | Print statements           |
| Auth           | JWT + API key                  | None                       |

---

## System-Level Concerns

**1. Data flow: Classification happens before analysis**

**router_skeleton_fastapi** makes routing decisions (keyword match → agent selection) before **Signal-Router-Service** analyzes the signal. This inverts the dependency: the routing layer decides where data goes without input from the intelligence layer. A signal like "I'm fine" + low HRV has no keywords → routed to DLQ → **Signal-Router-Service** never sees it. The system loses the very signals it's designed to catch.

**2. State management: Stateful analysis vs stateless routing**

**Signal-Router-Service** maintains per-user state (sliding window of drift scores, session history) to detect patterns over time. **router_skeleton_fastapi** is stateless per-request. If messages fail and replay from DLQ out of order, temporal patterns break. The drift detector's "sudden shift" detection (score went from 0.2 → 0.7) becomes unreliable when message 5 replays before message 3.

**3. Contract boundary: No shared schema**

Each implementation defines its own request/response contracts. **router_skeleton_fastapi** expects `tenant_id`, `event_id`, `payload_version`. **Signal-Router-Service** expects `user_id`, `session_id`, `hrv_data`, `text_context`. No shared Pydantic models, no versioned API contract. Integration requires translation layer or schema migration.

**4. Security: Encryption asymmetry**

**Signal-Router-Service** encrypts PHI at rest (Fernet/AES). **router_skeleton_fastapi** stores plaintext JSONB in Postgres. For healthcare/biometric data, the "less production-ready" implementation got security more right. Field-level encryption needed before Postgres, or encrypt the entire payload column.

**5. Failure handling: No feedback loop**

**router_skeleton_fastapi** has DLQ + replay, but no mechanism to notify **Signal-Router-Service** of upstream failures. If an agent is unhealthy, circuit breaker trips but coherence scoring continues unaware. No backpressure signal, no degraded mode coordination between services.

**6. Observability gap: Detection without action**

Both implementations detect and log. Neither acts. Coherence score hits 0.16 → nothing happens. **router_skeleton_fastapi** has Prometheus metrics but no alerting rules. No webhook, no PagerDuty integration, no threshold-based circuit breaker. Observability is present but without enforcement or response.

---

## How I would approach

```
Signal → router_skeleton_fastapi (dedupe, auth, reliability) → Signal-Router-Service (analysis)
                                                                        ↓
                                                                coherence_score + classification
                                                                        ↓
                                                 router_skeleton_fastapi makes secondary routing decision
                                                                        ↓
                                                              Action based on thresholds
```

**Service boundaries:**
- **router_skeleton_fastapi** owns: ingress, deduplication, rate limiting, retries, DLQ, fan-out, observability
- **Signal-Router-Service** owns: drift detection, biometric fusion, coherence scoring, pattern analysis

**Key changes:**
- Invert the flow: route ALL signals to **Signal-Router-Service** first, then route based on its output
- Define shared contract: common Pydantic models for request/response between services
- Add message ordering: include sequence numbers or use Kafka partitions to preserve temporal order for stateful analysis
- Encrypt before Postgres: field-level encryption for PHI, or encrypt payload column
- Add action layer: threshold-based alerts (webhook/PagerDuty), agent throttling on low coherence scores
- Backpressure: if **Signal-Router-Service** is unhealthy, **router_skeleton_fastapi** should buffer or shed load gracefully

---

## Conclusion

> **router_skeleton_fastapi has reliability but no intelligence. Signal-Router-Service has intelligence but no infrastructure. The job is to wire them together; with Signal-Router-Service informing routing decisions, encryption throughout and an action layer that responds to what's detected.**
