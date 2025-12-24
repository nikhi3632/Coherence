import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from eventsIngestion import build_timeline


# --------- HARD BOUNDARY INVARIANTS ---------
REQUIRED_RAW_FIELDS = {"timestamp", "source", "text"}
REQUIRED_NORMALIZED_FIELDS = {"t", "source", "type", "text"}


def log_error(stage: str, details: dict, error: Exception):
    print("\n" + "-" * 40)
    print(f"[{stage.upper()} ERROR]")
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                "details": details,
                "error": repr(error),
            },
            indent=2,
        )
    )


def _log(stage: str, trace_id: str, meta: Dict[str, Any] = None):
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                "trace_id": trace_id,
                "meta": meta or {},
            },
            sort_keys=True,
        )
    )


def validate_raw_event(event: Dict[str, Any]):
    # Guarantee: all raw events must contain required fields and valid timestamp.
    missing = REQUIRED_RAW_FIELDS - set(event.keys())
    if missing:
        raise ValueError(f"Missing raw fields: {missing}")

    # Timestamp must be valid ISO-8601
    datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

    if not isinstance(event["text"], str) or not event["text"].strip():
        raise ValueError("Invalid text field")


def validate_normalized_event(event: Dict[str, Any]):
    # Guarantee: normalized schema is stable and complete.
    missing = REQUIRED_NORMALIZED_FIELDS - set(event.keys())
    if missing:
        raise ValueError(f"Missing normalized fields: {missing}")


class IngestionRoutingBoundary:
    """
    *HARD BOUNDARY: ingestion → routing*

    On successful return from ingest():
        - All events routed exactly once.
        - Routing order matches chronological order.
        - All routed events share a single trace_id.
        - Schema of routed events is stable and validated.

    NOT guaranteed:
        - Router success.
        - Global ordering across multiple ingest() calls.
        - Deduplication of logically duplicate events.
    """

    def __init__(self, router):
        self.router = router

    def ingest(self, raw_events: List[Dict[str, Any]], trace_id: Optional[str] = None):
        trace_id = trace_id or f"trace_{uuid.uuid4()}"
        _log("ingestion_enter", trace_id, {"count": len(raw_events)})

        # Validate raw input
        for e in raw_events:
            try:
                validate_raw_event(e)
            except Exception:
                log_error("ingestion_validation", {"raw_event": e}, e)
                raise

        # Ingestion + normalization pipeline
        timeline = build_timeline(raw_events)

        #  Boundary Invariant 1: Event count preserved
        assert len(timeline) == len(raw_events), (
            "Ingestion dropped or duplicated events"
        )

        # Boundary Invariant 2: Ordered timeline
        ts = [evt["t"] for evt in timeline]
        assert ts == sorted(ts), "Timeline not ordered at boundary"

        _log("ingestion_exit", trace_id, {"count": len(timeline)})

        # Handoff to routing
        for idx, event in enumerate(timeline):
            validate_normalized_event(event)
            event_id = f"{trace_id}:{idx}"

            try:
                self.router(event, trace_id, event_id)
            except Exception as e:
                _log(
                    "routing_failed",
                    trace_id,
                    {"event_id": event_id, "error": type(e).__name__},
                )
                raise

        _log("routing_complete", trace_id, {"count": len(timeline)})
