"""
Minimal Signal-Router API wrapper exposing /analyze and /health.
Wraps classifier logic and returns standardized response for router to consume.
"""
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional, Dict, Any
import os

from .classifier import SignalClassifier

app = FastAPI(title="Signal-Router - Implementation")

classifier = SignalClassifier()


class AnalyzeRequest(BaseModel):
    tenant_id: str
    event_id: Optional[str] = None
    user_id: Optional[str] = None
    payload_version: int = 1
    ts: Optional[str] = None
    text: Optional[str] = None
    hrv: Optional[Dict[str, Any]] = None


class AnalyzeResponse(BaseModel):
    sequence_id: str
    coherence_score: float
    classification: str


@app.post("/analyze")
def analyze(req: AnalyzeRequest):
    # Minimal deterministic response using classifier
    classification = classifier.classify(req)

    # Simple coherence scoring heuristic
    if classification == "mixed_signal":
        score = 0.2
    elif classification == "emotional_signal":
        score = 0.6
    elif classification == "physiological_signal":
        score = 0.7
    else:
        score = 0.1

    # sequence_id placeholder - in production use canonical hash or incremental id
    sequence_id = f"seq-{req.user_id or 'anon'}-{req.event_id or 'noid'}"

    return AnalyzeResponse(sequence_id=sequence_id, coherence_score=score, classification=classification).model_dump()


@app.get("/health")
def health():
    return {"status": "ok"}
