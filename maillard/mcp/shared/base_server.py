"""
Base MCP server class — all department MCPs inherit from this.
"""
from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from loguru import logger


class BaseMCPServer(ABC):
    """
    Abstract base for every Maillard department MCP server.

    Subclasses must implement:
      - department: str  (e.g. "designer", "accounting")
      - tools: list[dict]  — MCP tool manifests
      - handle_tool(name, args) -> dict
    """

    department: str = "base"

    def __init__(self):
        self._audit = AuditLog(self.department)
        logger.info(f"[{self.department.upper()}-MCP] server initialised")

    # ── Tool manifest ─────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def tools(self) -> list[dict]:
        """Return list of MCP tool definitions (name, description, inputSchema)."""

    # ── Tool dispatch ─────────────────────────────────────────────────────────

    @abstractmethod
    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        """Execute a tool by name and return result dict."""

    # ── Handoff ───────────────────────────────────────────────────────────────

    async def handoff(self, target: str, task: str, context: dict) -> dict:
        """
        Emit a structured handoff request to another department.
        The orchestrator picks this up and routes it.
        """
        payload = {
            "from": self.department,
            "to": target,
            "task": task,
            "context": context,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._audit.log("handoff_emitted", payload)
        logger.info(f"[{self.department.upper()}-MCP] handoff → {target}: {task}")
        return payload

    # ── Helpers ───────────────────────────────────────────────────────────────

    def data_path(self, *parts: str) -> str:
        base = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", self.department, *parts
        )
        os.makedirs(os.path.dirname(base) if "." in os.path.basename(base) else base, exist_ok=True)
        return os.path.normpath(base)

    def ok(self, data: Any) -> dict:
        return {"status": "ok", "department": self.department, "data": data}

    def err(self, message: str) -> dict:
        return {"status": "error", "department": self.department, "error": message}


class AuditLog:
    def __init__(self, department: str):
        self.department = department
        log_dir = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", department, "audit"
        )
        os.makedirs(log_dir, exist_ok=True)
        self.path = os.path.join(log_dir, "audit.jsonl")

    def log(self, event: str, payload: dict) -> None:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "department": self.department,
            "event": event,
            "payload": payload,
        }
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.warning(f"Audit log write failed: {e}")
