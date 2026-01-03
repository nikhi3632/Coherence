import hashlib
import json
from typing import Optional, Dict, Any


def generate_canonical_message_id(
    tenant_id: str,
    event_id: Optional[str],
    user_id: Optional[str],
    ts_iso: str,
    payload_version: int,
    payload: Dict[str, Any],
) -> str:
    """
    Deterministic message id for exactly-once processing.
    Matches ROUTING_SYNTHESIS: SHA256(tenant_id, event_id, payload_version, payload)
    """
    canonical_payload = {k: v for k, v in payload.items() if k not in ["trace_id", "timestamp", "ts"]}
    canonical_json = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))

    if event_id:
        identifier = event_id
    elif user_id:
        identifier = f"{user_id}:{ts_iso}"
    else:
        identifier = hashlib.sha256(canonical_json.encode()).hexdigest()[:16]

    hash_components = f"{tenant_id}:{identifier}:{payload_version}:{canonical_json}"
    message_hash = hashlib.sha256(hash_components.encode()).hexdigest()[:32]
    return message_hash
