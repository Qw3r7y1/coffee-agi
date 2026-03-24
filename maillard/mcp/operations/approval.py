"""
Execution Control Center for Maillard Coffee Roasters.

Centralized action queue: generate -> approve/reject -> execute/fail.
Persisted to data/action_queue.json. No auto-execution. Synchronous only.

Statuses: pending -> approved -> executed
                  -> rejected
          approved -> failed
"""
from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger

_QUEUE_FILE = Path(__file__).resolve().parent.parent.parent.parent / "data" / "action_queue.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> list[dict]:
    try:
        if _QUEUE_FILE.exists():
            return json.loads(_QUEUE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def _save(entries: list[dict]) -> None:
    _QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _QUEUE_FILE.write_text(json.dumps(entries, indent=2), encoding="utf-8")


def _make_id(text: str) -> str:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return hashlib.sha256(f"{date}:{text}".encode()).hexdigest()[:12]


# =============================================================================
# ADD
# =============================================================================


def add_action(
    action: str,
    department: str = "",
    title: str = "",
    reason: str = "",
    item: str | None = None,
    quantity: str | None = None,
    supplier: str | None = None,
    urgency: str = "medium",
    **extra,
) -> dict:
    """Add an action to the queue. Deduplicates by id per day."""
    aid = _make_id(action)
    entry = {
        "id": aid,
        "department": department,
        "action": action,
        "title": title or action[:50],
        "reason": reason,
        "item": item,
        "quantity": quantity,
        "supplier": supplier,
        "urgency": urgency,
        "status": "pending",
        "timestamp": _now(),
    }

    entries = _load()
    if any(e["id"] == aid for e in entries):
        return next(e for e in entries if e["id"] == aid)

    entries.append(entry)
    _save(entries)
    logger.info(f"[QUEUE] added: {action[:50]}")
    return entry


def add_from_dict(raw: dict | str) -> dict:
    """Add from a decision engine action dict or plain string."""
    if isinstance(raw, str):
        return add_action(action=raw)

    return add_action(
        action=raw.get("action", str(raw)),
        department=raw.get("department") or raw.get("category", ""),
        title=raw.get("title") or raw.get("action", "")[:50],
        reason=raw.get("reason") or raw.get("why", ""),
        item=raw.get("item") or raw.get("sku"),
        quantity=str(raw.get("quantity") or raw.get("recommended_qty", "")),
        supplier=raw.get("supplier"),
        urgency=raw.get("urgency") or raw.get("priority", "medium"),
    )


# =============================================================================
# STATUS TRANSITIONS
# =============================================================================


def _transition(action_id: str, from_status: str | list, to_status: str, **fields) -> dict:
    """Generic status transition with validation."""
    if isinstance(from_status, str):
        from_status = [from_status]

    entries = _load()
    for e in entries:
        if e["id"] == action_id:
            if e["status"] not in from_status:
                return {"error": f"Cannot {to_status}: current status is '{e['status']}'"}
            e["status"] = to_status
            e[f"{to_status}_at"] = _now()
            e.update(fields)
            _save(entries)
            logger.info(f"[QUEUE] {action_id}: -> {to_status}")
            return e
    return {"error": f"Action {action_id} not found"}


def approve_action(action_id: str, approved_by: str = "owner") -> dict:
    return _transition(action_id, "pending", "approved", approved_by=approved_by)


def reject_action(action_id: str, reason: str = "") -> dict:
    return _transition(action_id, "pending", "rejected", reject_reason=reason)


def mark_executed(action_id: str) -> dict:
    return _transition(action_id, "approved", "executed")


def mark_failed(action_id: str, reason: str = "") -> dict:
    return _transition(action_id, ["approved", "executed"], "failed", fail_reason=reason)


# Aliases for backward compat
execute_action = mark_executed
request_approval = add_from_dict
standardize_action = add_from_dict


# =============================================================================
# QUERIES
# =============================================================================


def list_actions(status: str | None = None) -> list[dict]:
    entries = _load()
    if status:
        return [e for e in entries if e["status"] == status]
    return entries


def get_pending_actions() -> list[dict]:
    return list_actions("pending")


def get_all_actions(status: str | None = None) -> list[dict]:
    return list_actions(status)


def get_queue_summary() -> dict:
    entries = _load()
    by_status: dict[str, int] = {}
    for e in entries:
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1
    return {
        "total": len(entries),
        "pending": by_status.get("pending", 0),
        "approved": by_status.get("approved", 0),
        "rejected": by_status.get("rejected", 0),
        "executed": by_status.get("executed", 0),
        "failed": by_status.get("failed", 0),
    }


def clear_queue() -> int:
    entries = _load()
    count = len(entries)
    _save([])
    return count
