# Implementation sources and modifications

Overview

This document lists files placed into Implementation/ and their origin. For each file we state:
- Source repository
- Purpose / functionality (mapped to ROUTING_SYNTHESIS features)
- Modifications applied (if any)

1) signal_router/classifier.py
- Source: Signal-Router-Service/classifier.py
- Purpose / mapping to synthesis: Classify incoming signals into categories ("emotional_signal", "physiological_signal", "mixed_signal", "invalid_signal"). This component implements the synthesis requirement that the analysis engine must deterministically classify signal types as part of the coherence scoring pipeline.
- Modifications: Ported into Implementation/signal_router; adapted to use Pydantic-compatible input shapes and to be invoked by the /analyze wrapper. Removed any filesystem side-effects.

2) signal_router/biometric.py
- Source: Signal-Router-Service/biometric.py (extracted)
- Purpose / mapping to synthesis: HRV fusion and biometric helper utilities used by Signal-Router to amplify linguistic drift when physiological signals indicate stress. Implements the synthesis requirement that coherence scoring fuses linguistic markers with biometric signals to compute the final coherence score.
- Modifications: Namespaced under Implementation.signal_router; removed disk-based secret handling and made secret access env-driven.

3) signal_router/drift_detector.py
- Source: Signal-Router-Service/drift_detector.py
- Purpose / mapping to synthesis: Compute linguistic drift scores (per-token / phrase drift), combine them with biometric amplifiers, and emit time-windowed flags used by the coherence scoring policy. Implements synthesis requirements for drift scoring and temporal pattern detection.
- Modifications: Cleaned up to remove direct filesystem writes and reworked to accept programmatic inputs from the analysis API.

4) signal_router/history_storage.py
- Source: Signal-Router-Service/history_storage.py
- Purpose / mapping to synthesis: Provide per-user historical state (recent coherence scores, drift history, amplification factors) necessary to produce sequence-aware coherence scores and to prevent DLQ replay from breaking temporal patterns. Supports in-memory for tests and a pluggable DB backend for production.
- Modifications: Made pluggable backend; no on-disk secret storage; backend credentials read from environment.

5) signal_router/api.py
- Source: New wrapper around Signal-Router-Service main.py/router
- Purpose / mapping to synthesis: Expose a deterministic analysis endpoint (/analyze) that returns a canonical sequence_id, a coherence_score, and a classification. This is the single analysis contract that the router must call before any routing decision is made.
- Modifications: Implemented fresh in Implementation to standardize the response schema and to avoid disk key usage; added a /health endpoint for readiness and simple backpressure signalling.

6) router/app.py
- Source: Adapted from router_skeleton_fastapi/app/router.py and app/main.py
- Purpose / mapping to synthesis: Implement the ingress flow required by synthesis: authenticate, validate input, compute canonical message id for ordering and idempotency, call Signal-Router /analyze and use that response as the sole source of truth to decide routing, throttling, alerting, or DLQ. Guarantees no bypass paths and outlines where DLQ & persistence occur.
- Modifications: Removed local keyword-based classification; enforced analyze-first call; encrypts PHI before any persistent storage using utils/encryption; DB operations are prepared to use parameterized queries (placeholders in this minimal impl).

7) utils/canonical_hash.py
- Source: Adapted from router_skeleton_fastapi/app/router.py generate_canonical_message_id
- Purpose / mapping to synthesis: Implement deterministic canonical hashing (SHA256 over tenant_id, event_id/user_id+ts, payload_version, canonical_payload) used by the synthesis to ensure deterministic message ids for idempotency and ordering during DLQ replay and routing.
- Modifications: Factored into a utility with deterministic JSON canonicalization and exported as a simple function.

8) utils/encryption.py
- Source: Adapted from signal_service_trial/logger.py and integrity_service
- Purpose / mapping to synthesis: Provide field-level encryption helpers so PHI is encrypted before persistent storage under processes described by synthesis. Keys are read from environment variables (no disk keys).
- Modifications: Implemented as an env-driven helper using Fernet; raised an error if ENCRYPTION_KEY is not set.

Files intentionally not copied
- Any file that writes secret keys to disk (e.g., Signal-Router-Service/secret.key logic) was not copied. Instead we implemented env-driven key handling.
- Duplicated classifier or router implementations were consolidated; router_skeleton_fastapi's keyword classifier was disabled in favor of analysis-first.

Next steps
- Complete signal_router modules (biometric, drift_detector, history_storage) integration so /analyze returns canonical sequence_id plus per-user, time-aware coherence score
- Add DLQ persistent storage and idempotent replay logic in router
- Add tests covering analyze-first flow, ordering guarantees, and DLQ idempotency
