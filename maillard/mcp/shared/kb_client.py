"""
Thin wrapper around the existing KnowledgeBase for use by all MCP servers.
"""
from __future__ import annotations
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from ingestion.knowledge_base import KnowledgeBase

_kb: KnowledgeBase | None = None


def get_kb() -> KnowledgeBase:
    global _kb
    if _kb is None:
        _kb = KnowledgeBase()
    return _kb


def search(query: str, n_results: int = 4, topic_filter: str | None = None) -> list[dict]:
    kb = get_kb()
    results = kb.search(query, n_results=n_results)
    if topic_filter:
        results = [r for r in results if r.get("topic") == topic_filter]
    return results


async def ingest(filename: str, content: bytes, topic: str, difficulty: str = "foundation") -> dict:
    kb = get_kb()
    return await kb.ingest_file(filename, content, topic, difficulty)
