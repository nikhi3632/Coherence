"""
Ingress router adapted from router_skeleton_fastapi. Implements analyze-first flow:
1. Auth (placeholder)
2. Validate and canonical_hash
3. Call Signal-Router /analyze
4. Route based on analysis output (agents or DLQ)

This is a minimal implementation focusing on control flow and contracts.
"""
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import httpx
import os
from typing import Dict, Any

from ..utils.canonical_hash import generate_canonical_message_id
from ..utils.encryption import encrypt_payload

app = FastAPI(title="Router - Implementation")

ANALYZER_URL = os.getenv("ANALYZER_URL", "http://localhost:8001/analyze")


class IngestRequest(BaseModel):
    tenant_id: str
    event_id: str | None = None
    user_id: str | None = None
    payload_version: int = 1
    ts: str | None = None
    payload: Dict[str, Any] = {}


@app.post("/ingest")
async def ingest(req: IngestRequest):
    # Basic validation
    if not req.tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    ts = req.ts or "1970-01-01T00:00:00Z"

    # Generate canonical id
    message_id = generate_canonical_message_id(req.tenant_id, req.event_id, req.user_id, ts, req.payload_version, req.payload)

    # Call analyzer (Signal-Router) - mandatory
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(ANALYZER_URL, json={
                "tenant_id": req.tenant_id,
                "event_id": req.event_id,
                "user_id": req.user_id,
                "payload_version": req.payload_version,
                "ts": ts,
                **req.payload,
            })
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Analyzer unavailable: {e}")

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Analyzer returned error")

    analysis = resp.json()

    # Analysis must contain coherence_score and classification
    if "coherence_score" not in analysis or "classification" not in analysis:
        raise HTTPException(status_code=500, detail="Invalid analyzer response")

    # Based on classification decide agents
    classification = analysis["classification"]
    coherence = analysis["coherence_score"]

    # Simplified routing decision
    if classification == "invalid_signal":
        # DLQ path
        # Here we encrypt payload before storing in DLQ
        encrypted = encrypt_payload({"message_id": message_id, "payload": req.payload})
        # In this minimal impl we just return DLQ status
        return {"status": "dlq", "message_id": message_id}

    # Otherwise pretend to route to Axis agent
    # Return the analysis along with routing decision
    return {
        "status": "routed",
        "message_id": message_id,
        "classification": classification,
        "coherence_score": coherence,
        "routed_agents": ["Axis"],
    }
