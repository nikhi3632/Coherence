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
- Removed any file-based secret storage and replaced with env-driven keys (ENCRYPTION_KEY)
- Disabled local keyword-based routing in the router; routing is decisioned by analysis output only
- DB access points left as placeholders; any SQL must use parameterized queries

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

Next steps
- Implement persistence for history_storage and canonical sequence ids
- Add DLQ storage and idempotent replay logic
- Add tests covering analyze-first flow and ordering
