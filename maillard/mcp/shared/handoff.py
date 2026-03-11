"""
Inter-department handoff protocol.
Defines valid handoff routes and the HandoffRequest/HandoffResult models.
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel


# ── Valid handoff routes (from → to) ─────────────────────────────────────────
HANDOFF_ROUTES: dict[str, list[str]] = {
    "sales":        ["accounting", "operations", "marketing", "executive"],
    "marketing":    ["designer", "sales", "analyst", "executive"],
    "designer":     ["marketing", "executive"],
    "operations":   ["procurement", "hr", "accounting", "executive"],
    "procurement":  ["accounting", "operations", "legal"],
    "accounting":   ["executive", "legal"],
    "legal":        ["executive", "hr", "accounting"],
    "hr":           ["legal", "accounting", "executive"],
    "analyst":      ["executive", "marketing", "sales", "operations"],
    "executive":    ["*"],   # executive can route to anyone
    "orchestrator": ["*"],
}


class HandoffRequest(BaseModel):
    from_dept: str
    to_dept: str
    task: str
    priority: str = "normal"   # normal | urgent | low
    context: dict[str, Any] = {}
    requires_response: bool = False
    timestamp: str = ""

    def model_post_init(self, _):
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


class HandoffResult(BaseModel):
    accepted: bool
    from_dept: str
    to_dept: str
    task: str
    message: str = ""
    result: dict[str, Any] = {}


def validate_handoff(from_dept: str, to_dept: str) -> bool:
    allowed = HANDOFF_ROUTES.get(from_dept, [])
    return "*" in allowed or to_dept in allowed


def build_handoff(
    from_dept: str,
    to_dept: str,
    task: str,
    context: dict,
    priority: str = "normal",
) -> HandoffRequest:
    if not validate_handoff(from_dept, to_dept):
        raise ValueError(f"Handoff from '{from_dept}' to '{to_dept}' is not permitted.")
    return HandoffRequest(
        from_dept=from_dept,
        to_dept=to_dept,
        task=task,
        context=context,
        priority=priority,
    )
