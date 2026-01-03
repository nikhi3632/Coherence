import os
import json
from typing import Dict, Any


DLQ_DIR = os.path.join(os.path.dirname(__file__), "dlq")
os.makedirs(DLQ_DIR, exist_ok=True)


def persist_to_dlq(encrypted_bytes: bytes, meta: Dict[str, Any]) -> str:
    """Persist encrypted payload to a file in the router/dlq directory.

    Returns the absolute path to the stored file.
    """
    # message_id is the canonical id; fallback to timestamp-based name
    message_id = meta.get("message_id") or meta.get("log_id") or str(int(meta.get("ts_epoch", 0)))
    fname = f"{message_id}.enc"
    path = os.path.join(DLQ_DIR, fname)

    # write encrypted payload
    with open(path, "wb") as f:
        f.write(encrypted_bytes)

    # write metadata alongside for replay/inspection
    meta_path = path + ".meta.json"
    try:
        with open(meta_path, "w", encoding="utf-8") as mf:
            json.dump(meta, mf, ensure_ascii=False, indent=2)
    except Exception:
        # metadata is best-effort; payload is primary
        pass

    return path
