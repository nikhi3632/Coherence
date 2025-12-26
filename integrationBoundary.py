"""
Integration Boundary Module

The hard boundary between event ingestion and routing.
This is the Axis interface point — a clean, stable contract.

BOUNDARY CONTRACT (what Axis can rely on):
    - All events validated before routing
    - Events routed in chronological order
    - Single trace_id per batch
    - Structured errors for failure handling
    - Timeout protection on router calls

ERROR CATEGORIES:
    - IngestionValidationError: Bad input, do not retry
    - BoundaryInvariantError: Internal invariant violated, alert
    - RoutingError: Router failed, may retry
    - RoutingTimeoutError: Router hung, may retry with backoff
"""

__all__ = [
    # Core boundary
    "IngestionRoutingBoundary",
    # Exceptions (for downstream error handling)
    "IngestionValidationError",
    "BoundaryInvariantError",
    "RoutingError",
    "RoutingTimeoutError",
    # Configuration constants
    "MAX_TEXT_LENGTH",
    "MAX_BATCH_SIZE",
    "DEFAULT_ROUTER_TIMEOUT",
]

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional


from eventsIngestion import build_timeline


# --------- STRUCTURED EXCEPTIONS ---------


class IngestionValidationError(ValueError):
    """
    Raised when input event fails validation.
    Non-retriable: the input is malformed.
    """

    pass


class BoundaryInvariantError(AssertionError):
    """
    Raised when an internal invariant is violated.
    Indicates a bug in the ingestion pipeline.
    """

    pass


class RoutingError(Exception):
    """
    Raised when the router fails to process an event.
    May be retriable depending on the underlying cause.
    """

    def __init__(self, message: str, event_id: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.event_id = event_id
        self.cause = cause


class RoutingTimeoutError(RoutingError):
    """
    Raised when the router exceeds the configured timeout.
    Retriable with backoff.
    """

    pass


# --------- CONFIGURATION ---------

REQUIRED_RAW_FIELDS = {"timestamp", "source", "text"}
REQUIRED_NORMALIZED_FIELDS = {"t", "source", "type", "text"}
MAX_TEXT_LENGTH = 10_000  # 10KB max text length
MAX_BATCH_SIZE = 1000  # Max events per batch
DEFAULT_ROUTER_TIMEOUT = 30.0  # seconds


def _log(
    stage: str,
    trace_id: str,
    meta: Optional[Dict[str, Any]] = None,
    level: str = "INFO",
):
    """
    Structured logging with levels.

    Levels:
        INFO: Normal operation flow
        WARNING: Retriable failures, degraded state
        ERROR: Non-retriable failures, data loss
    """
    print(
        json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "stage": stage,
                "trace_id": trace_id,
                "meta": meta or {},
            },
            sort_keys=True,
        )
    )


def validate_raw_event(event: Dict[str, Any]):
    """
    Validate raw event structure and content.

    Raises:
        IngestionValidationError: If validation fails (non-retriable)
    """
    # Required fields
    missing = REQUIRED_RAW_FIELDS - set(event.keys())
    if missing:
        raise IngestionValidationError(f"Missing required fields: {missing}")

    # Timestamp must be valid ISO-8601
    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except (ValueError, TypeError) as e:
        raise IngestionValidationError(f"Invalid timestamp format: {e}")

    # Text field validation
    if not isinstance(event["text"], str):
        raise IngestionValidationError("Field 'text' must be a string")
    if not event["text"].strip():
        raise IngestionValidationError("Field 'text' cannot be empty")
    if len(event["text"]) > MAX_TEXT_LENGTH:
        raise IngestionValidationError(
            f"Field 'text' exceeds max length ({len(event['text'])} > {MAX_TEXT_LENGTH})"
        )

    # Source field validation
    if not isinstance(event["source"], str) or not event["source"].strip():
        raise IngestionValidationError("Field 'source' must be a non-empty string")


def validate_normalized_event(event: Dict[str, Any]):
    """
    Validate normalized event schema.

    Raises:
        BoundaryInvariantError: If schema is invalid (indicates pipeline bug)
    """
    missing = REQUIRED_NORMALIZED_FIELDS - set(event.keys())
    if missing:
        raise BoundaryInvariantError(f"Normalized event missing fields: {missing}")


class IngestionRoutingBoundary:
    """
    HARD BOUNDARY: ingestion → routing

    This is the Axis interface point. All events pass through this boundary
    for validation, normalization, and routing.

    GUARANTEES (on successful return from ingest()):
        - All events routed exactly once
        - Routing order matches chronological order
        - All routed events share a single trace_id
        - Schema of routed events is stable and validated

    NOT GUARANTEED:
        - Router success (errors are raised)
        - Global ordering across multiple ingest() calls
        - Deduplication of logically duplicate events

    ERROR BEHAVIOR:
        - IngestionValidationError: Bad input, batch rejected, do not retry
        - BoundaryInvariantError: Pipeline bug, batch rejected, alert
        - RoutingError: Router failed, partial progress possible
        - RoutingTimeoutError: Router hung, partial progress possible
    """

    def __init__(self, router, router_timeout: float = DEFAULT_ROUTER_TIMEOUT):
        """
        Initialize the boundary.

        Args:
            router: Callable(event, trace_id, event_id) to route normalized events
            router_timeout: Max seconds to wait for router per event (default: 30s)
        """
        self.router = router
        self.router_timeout = router_timeout
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="router")

    def ingest(self, raw_events: List[Dict[str, Any]], trace_id: Optional[str] = None):
        """
        Ingest a batch of raw events through the boundary.

        Args:
            raw_events: List of raw event dicts with timestamp, source, text
            trace_id: Optional trace ID (generated if not provided)

        Raises:
            IngestionValidationError: If any event fails validation
            BoundaryInvariantError: If pipeline invariants are violated
            RoutingError: If router fails for any event
            RoutingTimeoutError: If router exceeds timeout
        """
        trace_id = trace_id or f"trace_{uuid.uuid4()}"
        _log("ingestion_enter", trace_id, {"count": len(raw_events)})

        # Batch size limit
        if len(raw_events) > MAX_BATCH_SIZE:
            raise IngestionValidationError(
                f"Batch size {len(raw_events)} exceeds maximum {MAX_BATCH_SIZE}"
            )

        # Validate raw input
        for idx, event in enumerate(raw_events):
            try:
                validate_raw_event(event)
            except IngestionValidationError as e:
                _log(
                    "validation_failed",
                    trace_id,
                    {"event_index": idx, "error": str(e)},
                    level="ERROR",
                )
                raise

        # Ingestion + normalization pipeline
        timeline = build_timeline(raw_events)

        # Boundary Invariant 1: Event count preserved
        if len(timeline) != len(raw_events):
            raise BoundaryInvariantError(
                f"Event count mismatch: input={len(raw_events)}, output={len(timeline)}"
            )

        # Boundary Invariant 2: Ordered timeline
        timestamps = [evt["t"] for evt in timeline]
        if timestamps != sorted(timestamps):
            raise BoundaryInvariantError("Timeline not in chronological order")

        _log("ingestion_exit", trace_id, {"count": len(timeline)})

        # Handoff to routing
        for idx, event in enumerate(timeline):
            validate_normalized_event(event)
            event_id = f"{trace_id}:{idx}"

            try:
                self._route_with_timeout(event, trace_id, event_id)
            except RoutingError:
                raise
            except Exception as e:
                _log(
                    "routing_failed",
                    trace_id,
                    {"event_id": event_id, "error": type(e).__name__},
                    level="ERROR",
                )
                raise RoutingError(
                    f"Router failed: {e}", event_id=event_id, cause=e
                ) from e

        _log("routing_complete", trace_id, {"count": len(timeline)})

    def _route_with_timeout(self, event: Dict, trace_id: str, event_id: str):
        """Route a single event with timeout protection."""
        future = self._executor.submit(self.router, event, trace_id, event_id)
        try:
            future.result(timeout=self.router_timeout)
        except FuturesTimeoutError:
            _log(
                "routing_timeout",
                trace_id,
                {"event_id": event_id, "timeout": self.router_timeout},
                level="WARNING",
            )
            raise RoutingTimeoutError(
                f"Router exceeded {self.router_timeout}s timeout",
                event_id=event_id,
            )
