"""
Inventory Intelligence System for Maillard Coffee Roasters.

This is the BRAIN layer on top of the CRUD inventory system.
It analyzes usage patterns, predicts stockouts, detects waste anomalies,
and generates actionable reorder recommendations.

Functions:
  - get_daily_usage_rate()       -- avg consumption per day per item
  - predict_stockout()           -- days until zero for each item
  - get_reorder_recommendations() -- what to order, how much, when
  - detect_waste_anomalies()     -- abnormal waste patterns
  - get_inventory_health_report() -- full operational intelligence
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger

from maillard.models.database import SessionLocal
from maillard.models.operations import InventoryItem, UsageLog, WasteLog


# ── Maillard operational constants ───────────────────────────────────────────
# These reflect a real specialty coffee shop + roastery.

LEAD_TIMES = {
    # Days from order to delivery, by category
    "green_coffee": 14,    # international shipping
    "roasted_coffee": 2,   # in-house roasting turnaround
    "milk": 1,             # daily dairy delivery
    "consumables": 3,      # cups, lids, napkins
    "packaging": 5,        # bags, labels, boxes
    "equipment": 21,       # parts, machines
}

# Safety stock multiplier: how many extra days of buffer beyond lead time
SAFETY_STOCK_DAYS = {
    "green_coffee": 7,
    "roasted_coffee": 3,
    "milk": 1,
    "consumables": 5,
    "packaging": 5,
    "equipment": 0,
}

# Waste thresholds (% of usage that's acceptable as waste)
WASTE_THRESHOLD_PCT = {
    "green_coffee": 2.0,     # very little waste expected
    "roasted_coffee": 3.0,   # QC rejects, stale bags
    "milk": 8.0,             # expiry is common
    "consumables": 5.0,
    "packaging": 2.0,
    "equipment": 0,
}

# Default analysis window
DEFAULT_ANALYSIS_DAYS = 30


# =============================================================================
# DAILY USAGE RATE
# =============================================================================


def get_daily_usage_rate(sku: str | None = None, days: int = DEFAULT_ANALYSIS_DAYS) -> list[dict]:
    """Calculate average daily consumption per item from usage logs.

    Returns a list of items with their daily burn rate.
    If sku is provided, returns only that item.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with SessionLocal() as session:
            # Get all active items (or just one)
            q = session.query(InventoryItem).filter(InventoryItem.is_active == True)
            if sku:
                q = q.filter(InventoryItem.sku == sku)
            items = q.all()

            results = []
            for item in items:
                # Sum usage in the window
                total_used = (
                    session.query(func.coalesce(func.sum(UsageLog.quantity_used), 0))
                    .filter(
                        UsageLog.item_id == item.id,
                        UsageLog.logged_at >= cutoff,
                    )
                    .scalar()
                )
                total_used = float(total_used)

                # Count distinct days with usage
                usage_days = (
                    session.query(func.count(func.distinct(func.date(UsageLog.logged_at))))
                    .filter(
                        UsageLog.item_id == item.id,
                        UsageLog.logged_at >= cutoff,
                    )
                    .scalar()
                ) or 0

                daily_rate = round(total_used / days, 3) if days > 0 else 0
                # Also compute rate based on actual active days (more accurate for intermittent items)
                active_day_rate = round(total_used / usage_days, 3) if usage_days > 0 else 0

                # Usage breakdown by type
                type_breakdown = {}
                type_rows = (
                    session.query(UsageLog.usage_type, func.sum(UsageLog.quantity_used))
                    .filter(UsageLog.item_id == item.id, UsageLog.logged_at >= cutoff)
                    .group_by(UsageLog.usage_type)
                    .all()
                )
                for utype, qty in type_rows:
                    type_breakdown[utype] = round(float(qty), 2)

                results.append({
                    "sku": item.sku,
                    "name": item.name,
                    "category": item.category,
                    "unit": item.unit,
                    "current_stock": item.quantity,
                    "period_days": days,
                    "total_used": round(total_used, 2),
                    "active_usage_days": usage_days,
                    "daily_rate": daily_rate,
                    "active_day_rate": active_day_rate,
                    "usage_by_type": type_breakdown,
                })

            return results
    except Exception as e:
        logger.error(f"[INV-INTEL] daily_usage_rate failed: {e}")
        return []


# Need the func import for aggregation
from sqlalchemy import func


# =============================================================================
# STOCKOUT PREDICTION
# =============================================================================


def predict_stockout(days: int = DEFAULT_ANALYSIS_DAYS) -> list[dict]:
    """Predict when each item will run out based on recent usage rate.

    Returns items sorted by urgency (fewest days to stockout first).
    Items with zero usage are excluded.
    """
    rates = get_daily_usage_rate(days=days)
    predictions = []

    for r in rates:
        daily = r["daily_rate"]
        if daily <= 0:
            continue  # no usage = no stockout

        stock = r["current_stock"]
        days_remaining = round(stock / daily, 1) if daily > 0 else float("inf")

        lead_time = LEAD_TIMES.get(r["category"], 7)
        safety_days = SAFETY_STOCK_DAYS.get(r["category"], 3)

        # Urgency: how many days BEFORE stockout we need to reorder
        reorder_point_days = lead_time + safety_days
        days_until_critical = round(days_remaining - reorder_point_days, 1)

        if days_until_critical <= 0:
            urgency = "CRITICAL"
        elif days_until_critical <= 3:
            urgency = "URGENT"
        elif days_until_critical <= 7:
            urgency = "SOON"
        else:
            urgency = "OK"

        predictions.append({
            "sku": r["sku"],
            "name": r["name"],
            "category": r["category"],
            "unit": r["unit"],
            "current_stock": stock,
            "daily_rate": daily,
            "days_remaining": days_remaining,
            "lead_time_days": lead_time,
            "safety_stock_days": safety_days,
            "reorder_point_days": reorder_point_days,
            "days_until_critical": days_until_critical,
            "urgency": urgency,
        })

    # Sort: CRITICAL first, then URGENT, then by days_remaining
    urgency_order = {"CRITICAL": 0, "URGENT": 1, "SOON": 2, "OK": 3}
    predictions.sort(key=lambda p: (urgency_order.get(p["urgency"], 9), p["days_remaining"]))

    return predictions


# =============================================================================
# REORDER RECOMMENDATIONS
# =============================================================================


def get_reorder_recommendations(days: int = DEFAULT_ANALYSIS_DAYS) -> dict:
    """Generate specific reorder recommendations.

    For each item that needs reordering, calculates:
      - how much to order (covers lead_time + safety_stock + buffer)
      - estimated cost
      - deadline to place order
    """
    predictions = predict_stockout(days=days)
    recommendations = []
    total_cost = 0.0

    for p in predictions:
        if p["urgency"] == "OK":
            continue  # doesn't need reorder yet

        daily = p["daily_rate"]
        lead = p["lead_time_days"]
        safety = p["safety_stock_days"]

        # Order enough for lead_time + safety + 14 days buffer (2 weeks operating stock)
        buffer_days = 14
        target_stock = daily * (lead + safety + buffer_days)
        order_qty = round(max(0, target_stock - p["current_stock"]), 1)

        if order_qty <= 0:
            continue

        # Get cost
        try:
            with SessionLocal() as session:
                item = session.query(InventoryItem).filter_by(sku=p["sku"]).first()
                cost_per_unit = item.cost_per_unit if item else 0
                supplier = item.supplier if item else None
        except Exception:
            cost_per_unit = 0
            supplier = None

        est_cost = round(order_qty * cost_per_unit, 2)
        total_cost += est_cost

        # When to order: now minus lead time from stockout
        days_to_order = max(0, p["days_remaining"] - lead)

        recommendations.append({
            "sku": p["sku"],
            "name": p["name"],
            "category": p["category"],
            "urgency": p["urgency"],
            "order_quantity": order_qty,
            "unit": p["unit"],
            "estimated_cost_eur": est_cost,
            "supplier": supplier,
            "days_until_stockout": p["days_remaining"],
            "days_to_place_order": round(days_to_order, 1),
            "action": _reorder_action(p["urgency"], p["name"], order_qty, p["unit"], days_to_order),
        })

    return {
        "recommendations": recommendations,
        "total_items": len(recommendations),
        "total_estimated_cost_eur": round(total_cost, 2),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _reorder_action(urgency: str, name: str, qty: float, unit: str, days_to_order: float) -> str:
    if urgency == "CRITICAL":
        return f"ORDER NOW: {qty} {unit} of {name}. Stock will run out before delivery arrives."
    if urgency == "URGENT":
        return f"Order {qty} {unit} of {name} within 1-2 days."
    return f"Order {qty} {unit} of {name} within {int(days_to_order)} days."


# =============================================================================
# WASTE ANOMALY DETECTION
# =============================================================================


def detect_waste_anomalies(days: int = DEFAULT_ANALYSIS_DAYS) -> dict:
    """Detect abnormal waste patterns by comparing waste rate to usage rate.

    An anomaly is when waste exceeds the acceptable threshold for the category.
    Also detects sudden waste spikes (single-day waste > 3x daily average).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    anomalies = []
    summary = {"total_waste_cost_eur": 0, "total_usage_cost_eur": 0}

    try:
        with SessionLocal() as session:
            items = session.query(InventoryItem).filter(InventoryItem.is_active == True).all()

            for item in items:
                # Total usage
                total_used = float(
                    session.query(func.coalesce(func.sum(UsageLog.quantity_used), 0))
                    .filter(UsageLog.item_id == item.id, UsageLog.logged_at >= cutoff)
                    .scalar()
                )

                # Total waste
                total_wasted = float(
                    session.query(func.coalesce(func.sum(WasteLog.quantity_wasted), 0))
                    .filter(WasteLog.item_id == item.id, WasteLog.logged_at >= cutoff)
                    .scalar()
                )

                total_waste_cost = float(
                    session.query(func.coalesce(func.sum(WasteLog.cost_lost), 0))
                    .filter(WasteLog.item_id == item.id, WasteLog.logged_at >= cutoff)
                    .scalar()
                )

                summary["total_waste_cost_eur"] += total_waste_cost
                summary["total_usage_cost_eur"] += total_used * item.cost_per_unit

                if total_used <= 0 and total_wasted <= 0:
                    continue

                # Waste as % of total throughput (usage + waste)
                throughput = total_used + total_wasted
                waste_pct = round(total_wasted / throughput * 100, 1) if throughput > 0 else 0
                threshold = WASTE_THRESHOLD_PCT.get(item.category, 5.0)

                is_anomaly = waste_pct > threshold

                # Check for spike: any single waste log > 3x the daily waste average
                daily_waste_avg = total_wasted / days if days > 0 else 0
                spike_logs = []
                if daily_waste_avg > 0:
                    all_waste = (
                        session.query(WasteLog)
                        .filter(WasteLog.item_id == item.id, WasteLog.logged_at >= cutoff)
                        .all()
                    )
                    for wl in all_waste:
                        if wl.quantity_wasted > daily_waste_avg * 3:
                            spike_logs.append({
                                "date": wl.logged_at.isoformat() if wl.logged_at else None,
                                "quantity": wl.quantity_wasted,
                                "reason": wl.reason,
                                "cost_eur": wl.cost_lost,
                            })

                has_spike = len(spike_logs) > 0

                if is_anomaly or has_spike:
                    anomalies.append({
                        "sku": item.sku,
                        "name": item.name,
                        "category": item.category,
                        "total_used": round(total_used, 2),
                        "total_wasted": round(total_wasted, 2),
                        "waste_pct": waste_pct,
                        "threshold_pct": threshold,
                        "is_above_threshold": is_anomaly,
                        "waste_cost_eur": round(total_waste_cost, 2),
                        "spikes": spike_logs if has_spike else None,
                        "action": _waste_action(item.name, waste_pct, threshold, has_spike),
                    })

        summary["total_waste_cost_eur"] = round(summary["total_waste_cost_eur"], 2)
        summary["total_usage_cost_eur"] = round(summary["total_usage_cost_eur"], 2)
        if summary["total_usage_cost_eur"] > 0:
            summary["overall_waste_pct"] = round(
                summary["total_waste_cost_eur"] / summary["total_usage_cost_eur"] * 100, 1
            )
        else:
            summary["overall_waste_pct"] = 0

        return {
            "period_days": days,
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "summary": summary,
        }
    except Exception as e:
        logger.error(f"[INV-INTEL] waste anomaly detection failed: {e}")
        return {"error": str(e)}


def _waste_action(name: str, waste_pct: float, threshold: float, has_spike: bool) -> str:
    parts = []
    if waste_pct > threshold:
        parts.append(f"Waste rate {waste_pct}% exceeds {threshold}% threshold for {name}.")
    if has_spike:
        parts.append("Waste spike detected -- investigate root cause.")
    parts.append("Review storage conditions, FIFO compliance, and expiry dates.")
    return " ".join(parts)


# =============================================================================
# FULL HEALTH REPORT
# =============================================================================


def get_inventory_health_report(days: int = DEFAULT_ANALYSIS_DAYS) -> dict:
    """Comprehensive inventory intelligence report.

    Combines all intelligence into a single actionable output:
      1. Stock status overview (by category)
      2. Stockout predictions (sorted by urgency)
      3. Reorder recommendations (with costs)
      4. Waste analysis (anomalies + trends)
      5. Operational metrics
    """
    from maillard.mcp.operations.inventory import list_items, get_stock_value, get_reorder_alerts

    # 1. Current stock
    items = list_items()
    stock_value = get_stock_value()
    basic_alerts = get_reorder_alerts()

    # Category summary
    by_category: dict[str, dict] = {}
    for item in items:
        cat = item["category"]
        if cat not in by_category:
            by_category[cat] = {"items": 0, "total_qty": 0, "low_stock": 0}
        by_category[cat]["items"] += 1
        by_category[cat]["total_qty"] += item["quantity"]
        if item.get("needs_reorder"):
            by_category[cat]["low_stock"] += 1

    # 2. Stockout predictions
    predictions = predict_stockout(days=days)
    critical_items = [p for p in predictions if p["urgency"] in ("CRITICAL", "URGENT")]

    # 3. Reorder recommendations
    reorders = get_reorder_recommendations(days=days)

    # 4. Waste analysis
    waste = detect_waste_anomalies(days=days)

    # 5. Key metrics
    total_items = len(items)
    low_stock_count = len(basic_alerts)
    critical_count = len(critical_items)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period_days": days,

        "overview": {
            "total_items": total_items,
            "low_stock_items": low_stock_count,
            "critical_items": critical_count,
            "stock_value_eur": stock_value.get("total_value_eur", 0),
            "by_category": by_category,
        },

        "stockout_predictions": predictions,
        "reorder_recommendations": reorders,
        "waste_analysis": waste,

        "action_summary": _build_action_summary(critical_items, reorders, waste),
    }


def _build_action_summary(critical: list, reorders: dict, waste: dict) -> list[str]:
    """Build a prioritized list of actions."""
    actions = []

    # Critical reorders first
    for item in critical:
        if item["urgency"] == "CRITICAL":
            actions.append(
                f"[CRITICAL] {item['name']}: {item['days_remaining']:.0f} days of stock left. "
                f"Order immediately -- lead time is {item['lead_time_days']} days."
            )
        elif item["urgency"] == "URGENT":
            actions.append(
                f"[URGENT] {item['name']}: {item['days_remaining']:.0f} days of stock left. "
                f"Place order within 1-2 days."
            )

    # Reorder cost
    recs = reorders.get("recommendations", [])
    if recs:
        total = reorders.get("total_estimated_cost_eur", 0)
        actions.append(
            f"[REORDER] {len(recs)} items need ordering. Estimated cost: EUR {total:.2f}"
        )

    # Waste anomalies
    anomalies = waste.get("anomalies", [])
    if anomalies:
        waste_cost = waste.get("summary", {}).get("total_waste_cost_eur", 0)
        actions.append(
            f"[WASTE] {len(anomalies)} items with abnormal waste. "
            f"Total waste cost: EUR {waste_cost:.2f}. Investigate."
        )

    if not actions:
        actions.append("[OK] All inventory levels healthy. No immediate actions needed.")

    return actions
