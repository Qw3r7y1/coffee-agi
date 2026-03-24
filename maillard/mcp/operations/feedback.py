"""
Execution Feedback Engine for Maillard Coffee Roasters.

Compares what was recommended vs what was done vs what went wrong.
Outputs short, actionable adjustments for the next day.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from loguru import logger

_LOG_FILE = Path(__file__).resolve().parent.parent.parent.parent / "data" / "daily_log.json"


def load_daily_log() -> dict:
    """Load the daily execution log from data/daily_log.json."""
    try:
        if _LOG_FILE.exists():
            return json.loads(_LOG_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[FEEDBACK] Could not load daily log: {e}")
    return {"date": "", "actions_taken": [], "issues": [], "notes": "", "previous_brief_actions": []}


def analyze_execution_feedback(log: dict | None = None, previous_actions: list[str] | None = None) -> dict:
    """Compare recommended actions vs executed actions vs issues.

    Args:
        log: Daily log dict (loaded from file if None)
        previous_actions: List of action strings from previous brief (from log if None)

    Returns:
        {
            "missed_actions": [actions recommended but not executed],
            "unexpected_issues": [issues that no action covered],
            "partial_execution": [actions done but with wrong quantities],
            "adjustments": [short fixes for tomorrow],
        }
    """
    if log is None:
        log = load_daily_log()

    taken = log.get("actions_taken", [])
    issues = log.get("issues", [])
    notes = log.get("notes", "")
    recommended = previous_actions or log.get("previous_brief_actions", [])

    taken_lower = [a.lower() for a in taken]
    result = {
        "missed_actions": [],
        "unexpected_issues": [],
        "partial_execution": [],
        "adjustments": [],
    }

    # ── Missed actions: recommended but not done ─────────────────────────
    for rec in recommended:
        rec_lower = rec.lower()
        # Check if any taken action covers this recommendation
        covered = False
        partial = False

        for t in taken_lower:
            # Extract key items from both strings
            rec_items = _extract_items(rec_lower)
            taken_items = _extract_items(t)

            if rec_items & taken_items:
                # Same item was acted on -- check if quantity matches
                rec_qty = _extract_qty(rec_lower)
                taken_qty = _extract_qty(t)

                if rec_qty and taken_qty and taken_qty < rec_qty * 0.8:
                    partial = True
                    result["partial_execution"].append({
                        "recommended": rec,
                        "executed": next(a for a in taken if _extract_items(a.lower()) & rec_items),
                        "gap": f"Recommended {rec_qty}, did {taken_qty}",
                    })
                covered = True
                break

        if not covered and not partial:
            result["missed_actions"].append(rec)

    # ── Unexpected issues: problems not covered by any action ────────────
    for issue in issues:
        issue_lower = issue.lower()
        was_covered = False
        for rec in recommended:
            # Check if the recommended actions would have prevented this issue
            rec_items = _extract_items(rec.lower())
            issue_items = _extract_items(issue_lower)
            if rec_items & issue_items:
                was_covered = True
                break

        if not was_covered:
            result["unexpected_issues"].append(issue)

    # ── Generate adjustments ─────────────────────────────────────────────
    # From missed actions
    for missed in result["missed_actions"]:
        result["adjustments"].append(f"Execute tomorrow: {missed}")

    # From partial execution
    for partial in result["partial_execution"]:
        result["adjustments"].append(f"Increase quantity: {partial['gap']}")

    # From unexpected issues
    for issue in result["unexpected_issues"]:
        items = _extract_items(issue.lower())
        for item in items:
            result["adjustments"].append(f"Add {item} to daily monitoring")
        if not items:
            result["adjustments"].append(f"Address: {issue}")

    # From notes
    if "higher than expected" in notes.lower() or "demand" in notes.lower():
        result["adjustments"].append("Increase order quantities by 20% to cover demand surge")

    return result


def _extract_items(text: str) -> set[str]:
    """Extract product/ingredient keywords from a text string."""
    keywords = {
        "milk", "whole milk", "oat milk", "oat", "coffee", "espresso", "beans",
        "ethiopia", "yirgacheffe", "brazil", "cups", "cream",
    }
    found = set()
    for kw in keywords:
        if kw in text:
            found.add(kw)
    return found


def _extract_qty(text: str) -> float | None:
    """Extract the first number from a text string."""
    m = re.search(r"(\d+\.?\d*)\s*(l|liters?|kg|units?)", text)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+\.?\d*)", text)
    if m:
        return float(m.group(1))
    return None
