"""
Shrinkage Tracking for Maillard Coffee Roasters.

Tracks difference between expected inventory (system) and actual counted inventory.
Flags normal, warning, and investigate thresholds.
V1: key items only, no audit system, no attribution.
"""
from __future__ import annotations

from maillard.mcp.operations.state_loader import get_operational_snapshot


# Thresholds
NORMAL_MAX = 3.0      # under 3% = normal
WARNING_MAX = 8.0     # 3-8% = warning
                      # above 8% = investigate


def calculate_shrinkage(expected: float, actual: float) -> dict:
    """Calculate shrinkage between expected and actual stock.

    Returns:
        {
            "expected": float,
            "actual": float,
            "shrinkage": float (units lost),
            "shrinkage_pct": float,
            "status": "normal" | "warning" | "investigate",
        }
    """
    shrinkage = round(expected - actual, 2)
    pct = round(shrinkage / expected * 100, 1) if expected > 0 else 0.0

    if pct <= NORMAL_MAX:
        status = "normal"
    elif pct <= WARNING_MAX:
        status = "warning"
    else:
        status = "investigate"

    return {
        "expected": expected,
        "actual": actual,
        "shrinkage": shrinkage,
        "shrinkage_pct": pct,
        "status": status,
    }


def run_shrinkage_check(actual_counts: dict[str, float]) -> dict:
    """Compare actual physical counts against system-expected stock.

    Args:
        actual_counts: {"ethiopia_yirgacheffe": 11.2, "whole_milk": 13.5, ...}
                       Only items you counted — missing items are skipped.

    Returns:
        {
            "items": {sku: shrinkage_result},
            "total_shrinkage_cost": float,
            "alerts": [items needing attention],
            "formatted": str,
        }
    """
    snap = get_operational_snapshot()
    updated_inv = snap.get("updated_inventory", {})

    items = {}
    alerts = []
    total_cost = 0.0

    for sku, actual in actual_counts.items():
        inv = updated_inv.get(sku)
        if not inv:
            continue

        expected = inv.get("stock", 0)
        result = calculate_shrinkage(expected, actual)
        result["unit"] = inv.get("unit", "")
        result["cost_per_unit"] = inv.get("cost_per_unit", 0)
        result["shrinkage_cost"] = round(result["shrinkage"] * result["cost_per_unit"], 2)

        items[sku] = result
        total_cost += max(0, result["shrinkage_cost"])

        if result["status"] != "normal":
            alerts.append({
                "item": sku,
                "status": result["status"],
                "shrinkage": result["shrinkage"],
                "shrinkage_pct": result["shrinkage_pct"],
                "cost_lost": result["shrinkage_cost"],
                "unit": result["unit"],
            })

    # Format
    lines = ["Shrinkage Report", "=" * 35, ""]
    for sku, r in items.items():
        flag = "" if r["status"] == "normal" else f" [{r['status'].upper()}]"
        lines.append(
            f"  {sku.replace('_',' '):25s} "
            f"expected={r['expected']:6.1f} actual={r['actual']:6.1f} "
            f"shrink={r['shrinkage']:5.1f}{r['unit']} ({r['shrinkage_pct']}%){flag}"
        )
    if alerts:
        lines.append("")
        lines.append("Alerts:")
        for a in alerts:
            lines.append(f"  {a['item'].replace('_',' ')}: {a['shrinkage_pct']}% ({a['shrinkage']} {a['unit']}) = EUR {a['cost_lost']}")
    lines.append("")
    lines.append(f"Total shrinkage cost: EUR {total_cost:.2f}")
    lines.append("=" * 35)

    return {
        "items": items,
        "total_shrinkage_cost": round(total_cost, 2),
        "alerts": alerts,
        "formatted": "\n".join(lines),
    }


def get_margin_shrinkage_impact(shrinkage_report: dict | None = None) -> dict:
    """Return a simple margin pressure note from shrinkage data.

    Can be injected into cost engine or morning brief.
    """
    if not shrinkage_report:
        return {"impact": None, "note": "No shrinkage data available"}

    cost = shrinkage_report.get("total_shrinkage_cost", 0)
    alerts = shrinkage_report.get("alerts", [])

    if cost <= 0 and not alerts:
        return {"impact": None, "note": "Shrinkage within normal range"}

    notes = []
    if cost > 0:
        notes.append(f"EUR {cost:.2f} lost to shrinkage")
    for a in alerts[:2]:
        notes.append(f"{a['item'].replace('_',' ')}: {a['shrinkage_pct']}% loss")

    return {
        "impact": "margin_pressure",
        "cost_eur": cost,
        "note": ". ".join(notes),
    }
