"""Simple routing map and accessor used by router and DLQ replay.

This keeps classification->destination rules in one place so replay and live
ingest use the same decision logic.
"""
from typing import Optional


# Synthesis-aligned mapping: classification → destinations
CLASS_TO_DEST = {
    "assist": ["Axis"],
    "policy": ["M"],
    "emergency": ["M", "Axis"],  # parallel
    "unknown": ["DLQ"],
}

DEFAULT_THRESHOLD = 0.3

def pick_destination(classification: str | None, coherence_score: float | None, tenant_id: str | None = None):
    """
    Returns (destinations: list[str], decision_trace: list[str])
    - If score exists and < DEFAULT_THRESHOLD => DLQ
    - Else map classification -> destinations (fallback to ["Axis"])
    """
    decision_trace = []

    # score-based DLQ
    if coherence_score is not None:
        decision_trace.append(f"coherence_score={coherence_score}")
        try:
            score = float(coherence_score)
            if score < DEFAULT_THRESHOLD:
                decision_trace.append(f"score<{DEFAULT_THRESHOLD} -> DLQ")
                return ["DLQ"], decision_trace
        except Exception:
            decision_trace.append("score=parse_error")

    # classification mapping
    if classification:
        decision_trace.append(f"classification={classification}")
        dests = CLASS_TO_DEST.get(classification, None)
        if dests:
            decision_trace.append(f"mapped_to={dests}")
            return list(dests), decision_trace

    # fallback
    decision_trace.append("fallback=Axis")
    return ["Axis"], decision_trace
