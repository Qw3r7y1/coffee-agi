"""
Daily Task Generator for Maillard Coffee Roasters.

Converts decision engine actions into a plain text task list.
Max 5 tasks. Ordered by priority. Exact wording preserved.
"""
from __future__ import annotations


def generate_daily_tasks(actions: list[dict] | None = None) -> str:
    """Generate a plain text task list from decision engine actions.

    Args:
        actions: List of decision dicts with "action" key.
                 If None, pulls from the morning brief automatically.

    Returns:
        Plain text numbered list, max 5 tasks.
    """
    if actions is None:
        from maillard.mcp.operations.decision_engine import generate_morning_brief
        brief = generate_morning_brief()
        actions = brief.get("decisions", [])

    if not actions:
        return "1. No tasks today"

    lines = []
    for i, a in enumerate(actions[:5], 1):
        text = a.get("action", a) if isinstance(a, dict) else str(a)
        lines.append(f"{i}. {text}")

    return "\n".join(lines)
