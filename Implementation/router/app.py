"""
Ingress router adapted from router_skeleton_fastapi. Implements analyze-first flow:
1. Auth (placeholder)
2. Validate and canonical_hash
3. Call Signal-Router /analyze
4. Route based on analysis output (agents or DLQ)

This is a minimal implementation focusing on control flow and contracts.
"""
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
import httpx
import os
from typing import Dict, Any

from ..utils.canonical_hash import generate_canonical_message_id
from ..utils.encryption import encrypt_payload
from .dlq_store import persist_to_dlq
from .routing import pick_destination

app = FastAPI(title="Router - Implementation")

ANALYZER_URL = os.getenv("ANALYZER_URL", "http://localhost:8001/analyze")


class IngestRequest(BaseModel):
    tenant_id: str
    event_id: str | None = None
    user_id: str | None = None
    payload_version: int = 1
    ts: str | None = None
    payload: Dict[str, Any] = Field(default_factory=dict)


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

    # 使用 synthesis-aligned mapping
    destinations, decision_trace = pick_destination(classification, analysis.get("coherence_score"), req.tenant_id)
    if "DLQ" in destinations:
        # DLQ path: encrypt and persist
        encrypted = encrypt_payload({"message_id": message_id, "payload": req.payload})
        dlq_path = persist_to_dlq(
            encrypted,
            {
                "message_id": message_id,
                "tenant_id": req.tenant_id,
                "event_id": req.event_id,
                "decision_trace": decision_trace,
            },
        )
        return {
            "status": "dlq_stored",
            "message_id": message_id,
            "dlq_path": dlq_path,
            "classification": classification,
            "coherence_score": coherence,
            "decision_trace": decision_trace,
        }

    # 普通路由，支持多 agent
    return {
        "status": "routed",
        "message_id": message_id,
        "classification": classification,
        "coherence_score": coherence,
        "routed_agents": destinations,
        "decision_trace": decision_trace,
    }
