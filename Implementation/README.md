Implementation README

This folder contains the minimal, synthesis-aligned implementation assembled from three repositories:
- router_skeleton_fastapi (ingress, routing skeleton)
- Signal-Router-Service (analysis, classifier, history)
- signal_service_trial (encryption & integrity helpers)

Files and origins (summary):
- signal_router/api.py — new wrapper implementing /analyze; adapts classifier from Signal-Router-Service 
- signal_router/classifier.py — adapted from Signal-Router-Service/classifier.py, classification logic
- router/app.py — adapted from router_skeleton_fastapi; ingress pipeline that calls /analyze first and routes based on its response
- utils/canonical_hash.py — adapted canonical id generation from router_skeleton_fastapi
- utils/encryption.py — adapted from signal_service_trial to provide env-driven field-level encryption

Modifications applied
- Removed any file-based secret storage and replaced with env-driven keys (`ENCRYPTION_KEY`).
- Removed committed `__pycache__` artifacts and updated `.gitignore` to ignore `__pycache__/` and `*.pyc`.
- Fixed `IngestRequest.payload` to use `Field(default_factory=dict)` to avoid mutable default arguments.
- Implemented minimal file-backed DLQ persistence at `Implementation/router/dlq_store.py` that writes encrypted blobs under `Implementation/router/dlq/` and optional metadata files.
- Centralized routing logic in `Implementation/router/routing.py` and updated both `ingest` and `replay_dlq` to reuse `pick_destination(...)` so live routing and replay share the same decision rules.
- Added integration smoke tests at `Implementation/tests/test_ingest_integration.py` to exercise analyze→ingest→route common flows (normal routing, low-score DLQ, analyzer error, validation error).

How to run (development)
- Start the analyzer:

```powershell
# from Coherence root
uvicorn Implementation.signal_router.api:app --port 8001 --reload
```

- Start the router:

```powershell
uvicorn Implementation.router.app:app --port 8000 --reload
```

- Test ingest (example):

```powershell
curl -X POST "http://localhost:8000/ingest" -H "Content-Type: application/json" -d '{"tenant_id":"t1","user_id":"u1","payload_version":1,"ts":"2025-12-05T08:10:00Z","payload": {"text":"I am fine","hrv": {"hrv_ms": 30}} }'
```

Run the smoke tests

- Ensure the Python environment has the required test packages (pytest, httpx, fastapi, cryptography). Add them to `requirements.txt` if needed.
- From repository root run:

```powershell
pytest -q Implementation/tests/test_ingest_integration.py
```
Next steps
- Implement persistence for history_storage and canonical sequence ids.
- Decide DLQ architecture: adapt `replay_dlq.py` to read file-based DLQ, or implement migration/import into a DB-backed DLQ table for production.
- Expand tests to cover ordering, idempotency, and replay semantics.

Notes
- The file-backed DLQ is intentionally minimal; it is useful for local testing and smoke replay. For production, prefer a durable DB or object store and a migration path.
- No changes were pushed to remote from this session; commits are local.
- No changes were pushed to remote from this session; commits are local.

