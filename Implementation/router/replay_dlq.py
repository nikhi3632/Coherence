"""
DLQ replay adapted from router_skeleton_fastapi/db/replay_dlq.py
This version uses parameterized SQL (SQLAlchemy text) and accepts an AsyncSession.
"""
import json
import time
import logging
from sqlalchemy import text
from typing import Any
from .routing import pick_destination

logger = logging.getLogger("implementation.replay")

async def replay_item(db, id_: int, log_id: str, payload_text: str) -> bool:
    try:
        payload = json.loads(payload_text)
        sender_id = payload.get("user_id") or payload.get("userId") or "unknown"

        # Pre-replay deduplication
        result = await db.execute(text("SELECT 1 FROM logs WHERE log_id = :log_id"), {"log_id": log_id})
        if result.fetchone():
            logger.info(f"Skipping replay - already processed: {log_id}")
            await db.execute(text("DELETE FROM dlq WHERE id = :id"), {"id": id_})
            await db.commit()
            return True

        # Simple heuristic for kind (legacy)
        msg = json.dumps(payload).lower()
        kind = "assist"
        if any(k in msg for k in ["emergency", "urgent", "crisis"]):
            kind = "emergency"
        elif any(k in msg for k in ["policy", "compliance"]):
            kind = "policy"

        # Use centralized routing logic (now returns list and trace)
        classification = payload.get("classification") or kind
        coherence_score = payload.get("coherence_score")
        destinations, decision_trace = pick_destination(classification, coherence_score)
        routed_agents = json.dumps(destinations)
        response_obj = {"status": "replayed", "source": "dlq_replay"}
        metadata = {
            "replayed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "original_dlq_id": id_,
            "decision_trace": decision_trace,
        }

        # Insert into logs using parameter binding and jsonb casts
        await db.execute(
            text(
                """
                INSERT INTO logs (log_id, ts, sender_id, kind, routed_agents, response, metadata)
                VALUES (:log_id, :ts, :sender_id, :kind, :routed_agents::jsonb, :response::jsonb, :metadata::jsonb)
                ON CONFLICT (log_id) DO UPDATE SET
                    response = :response::jsonb,
                    metadata = logs.metadata || :metadata::jsonb
                """
            ),
            {
                "log_id": log_id,
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "sender_id": sender_id,
                "kind": kind,
                "routed_agents": routed_agents,
                "response": json.dumps(response_obj),
                "metadata": json.dumps(metadata),
            }
        )

        # Delete from DLQ
        await db.execute(text("DELETE FROM dlq WHERE id = :id"), {"id": id_})
        await db.commit()

        logger.info(f"Replayed DLQ item id={id_}, log_id={log_id}")
        return True
    except Exception as e:
        await db.rollback()
        logger.error(f"Error replaying DLQ item id={id_}: {e}")
        try:
            await db.execute(text("UPDATE dlq SET attempts = attempts + 1 WHERE id = :id"), {"id": id_})
            await db.commit()
        except Exception:
            pass
        return False


async def replay(db, limit: int = 100, dry_run: bool = False):
    logger.info(f"Starting DLQ replay (limit={limit}, dry_run={dry_run})")
    result = await db.execute(text("SELECT id, log_id, payload::text FROM dlq ORDER BY ts ASC, attempts ASC LIMIT :limit"), {"limit": limit})
    rows = result.fetchall()
    if not rows:
        logger.info("No DLQ items to replay")
        return

    if dry_run:
        for (id_, log_id, payload_text) in rows:
            logger.info(f"Would replay: id={id_}, log_id={log_id}")
        return

    success = 0
    error = 0
    for (id_, log_id, payload_text) in rows:
        ok = await replay_item(db, id_, log_id, payload_text)
        if ok:
            success += 1
        else:
            error += 1

    logger.info(f"DLQ replay complete: total={len(rows)}, success={success}, error={error}")
