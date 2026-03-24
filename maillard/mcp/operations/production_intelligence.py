"""
Coffee Production Intelligence for Maillard Coffee Roasters.

Analyzes roast history to optimize:
  - Roast loss tracking and anomaly detection
  - Batch size optimization
  - Capacity planning and scheduling
  - Green-to-roasted yield analysis
  - Production vs demand gap detection
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger
from sqlalchemy import func

from maillard.models.database import SessionLocal
from maillard.models.operations import RoastBatch, InventoryItem


# ── Maillard roasting constants ──────────────────────────────────────────────
TYPICAL_LOSS_PCT = 18.0          # expected weight loss
LOSS_TOLERANCE_LOW = 14.0        # below this -> under-roasted or scale error
LOSS_TOLERANCE_HIGH = 22.0       # above this -> over-roasted or fire risk
MAX_ROASTER_CAPACITY_KG = 25.0   # max per batch (typical shop roaster)
MIN_BATCH_KG = 5.0               # minimum efficient batch
ROASTS_PER_DAY_CAPACITY = 4      # max batches per day (cooling + QC time)

DEFAULT_DAYS = 30


# =============================================================================
# ROAST LOSS ANALYSIS
# =============================================================================


def analyze_roast_losses(days: int = DEFAULT_DAYS) -> dict:
    """Analyze roast loss patterns and detect anomalies.

    Returns:
      - average loss %, trend direction
      - per-origin and per-roast-level breakdown
      - batches with abnormal loss (outside tolerance)
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with SessionLocal() as session:
            batches = (
                session.query(RoastBatch)
                .filter(
                    RoastBatch.status == "completed",
                    RoastBatch.completed_at >= cutoff,
                    RoastBatch.roast_loss_percent.isnot(None),
                )
                .order_by(RoastBatch.completed_at)
                .all()
            )

            if not batches:
                return {"period_days": days, "batches_analyzed": 0, "message": "No completed batches in period."}

            losses = [b.roast_loss_percent for b in batches]
            avg_loss = round(sum(losses) / len(losses), 1)

            # Trend: compare first half vs second half
            mid = len(losses) // 2
            if mid > 0:
                older_avg = sum(losses[:mid]) / mid
                newer_avg = sum(losses[mid:]) / (len(losses) - mid)
                trend = "improving" if newer_avg < older_avg - 0.5 else (
                    "worsening" if newer_avg > older_avg + 0.5 else "stable"
                )
            else:
                trend = "insufficient_data"

            # By roast level
            by_level: dict[str, list] = {}
            for b in batches:
                lv = b.roast_level or "unknown"
                by_level.setdefault(lv, []).append(b.roast_loss_percent)

            level_stats = {}
            for lv, vals in by_level.items():
                level_stats[lv] = {
                    "avg_loss_pct": round(sum(vals) / len(vals), 1),
                    "batch_count": len(vals),
                    "min": round(min(vals), 1),
                    "max": round(max(vals), 1),
                }

            # By green coffee origin
            by_origin: dict[str, list] = {}
            for b in batches:
                origin = b.green_coffee_name or b.green_coffee_sku
                by_origin.setdefault(origin, []).append(b.roast_loss_percent)

            origin_stats = {}
            for origin, vals in by_origin.items():
                origin_stats[origin] = {
                    "avg_loss_pct": round(sum(vals) / len(vals), 1),
                    "batch_count": len(vals),
                }

            # Anomalies
            anomalies = []
            for b in batches:
                loss = b.roast_loss_percent
                if loss < LOSS_TOLERANCE_LOW:
                    anomalies.append({
                        "batch_code": b.batch_code,
                        "loss_pct": loss,
                        "issue": "under_roasted",
                        "detail": f"Loss {loss}% is below {LOSS_TOLERANCE_LOW}% -- possible under-roast or scale error",
                    })
                elif loss > LOSS_TOLERANCE_HIGH:
                    anomalies.append({
                        "batch_code": b.batch_code,
                        "loss_pct": loss,
                        "issue": "over_roasted",
                        "detail": f"Loss {loss}% exceeds {LOSS_TOLERANCE_HIGH}% -- possible over-roast or fire damage",
                    })

            # Total green consumed and roasted produced
            total_green = sum(b.green_weight_kg for b in batches)
            total_roasted = sum(b.roasted_weight_kg or 0 for b in batches)

            return {
                "period_days": days,
                "batches_analyzed": len(batches),
                "avg_loss_pct": avg_loss,
                "expected_loss_pct": TYPICAL_LOSS_PCT,
                "loss_trend": trend,
                "total_green_kg": round(total_green, 1),
                "total_roasted_kg": round(total_roasted, 1),
                "overall_yield_pct": round(total_roasted / total_green * 100, 1) if total_green else 0,
                "by_roast_level": level_stats,
                "by_origin": origin_stats,
                "anomalies": anomalies,
                "anomaly_count": len(anomalies),
            }
    except Exception as e:
        logger.error(f"[PROD-INTEL] roast loss analysis failed: {e}")
        return {"error": str(e)}


# =============================================================================
# CAPACITY & SCHEDULING
# =============================================================================


def get_capacity_analysis(days: int = DEFAULT_DAYS) -> dict:
    """Analyze production capacity utilization.

    Compares actual output to theoretical maximum.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with SessionLocal() as session:
            batches = (
                session.query(RoastBatch)
                .filter(RoastBatch.created_at >= cutoff)
                .all()
            )

            completed = [b for b in batches if b.status == "completed"]
            failed = [b for b in batches if b.status == "failed"]
            scheduled = [b for b in batches if b.status == "scheduled"]

            total_batches = len(completed)
            total_green = sum(b.green_weight_kg for b in completed)
            total_roasted = sum(b.roasted_weight_kg or 0 for b in completed)

            # Theoretical max
            max_daily_kg = MAX_ROASTER_CAPACITY_KG * ROASTS_PER_DAY_CAPACITY
            max_period_kg = max_daily_kg * days
            utilization_pct = round(total_green / max_period_kg * 100, 1) if max_period_kg else 0

            # Average batch size
            avg_batch_kg = round(total_green / total_batches, 1) if total_batches else 0
            batch_efficiency = round(avg_batch_kg / MAX_ROASTER_CAPACITY_KG * 100, 1) if MAX_ROASTER_CAPACITY_KG else 0

            # Batches per day (operating days)
            unique_days = set()
            for b in completed:
                if b.completed_at:
                    unique_days.add(b.completed_at.date())
            operating_days = len(unique_days) or 1
            batches_per_day = round(total_batches / operating_days, 1)

            # Failure rate
            total_attempted = len(completed) + len(failed)
            failure_rate = round(len(failed) / total_attempted * 100, 1) if total_attempted else 0

            # Recommendations
            recommendations = []
            if batch_efficiency < 60:
                recommendations.append(
                    f"Batch size averaging {avg_batch_kg}kg ({batch_efficiency}% of {MAX_ROASTER_CAPACITY_KG}kg capacity). "
                    f"Consider consolidating smaller batches to improve efficiency."
                )
            if utilization_pct < 30:
                recommendations.append(
                    f"Roaster utilization at {utilization_pct}%. Significant spare capacity available."
                )
            if utilization_pct > 85:
                recommendations.append(
                    f"Roaster utilization at {utilization_pct}%. Approaching capacity limits. "
                    f"Consider scheduling evening roasts or additional equipment."
                )
            if failure_rate > 5:
                recommendations.append(
                    f"Roast failure rate {failure_rate}%. Investigate causes -- "
                    f"check temperature calibration and green coffee moisture."
                )

            return {
                "period_days": days,
                "total_batches": total_batches,
                "failed_batches": len(failed),
                "scheduled_batches": len(scheduled),
                "failure_rate_pct": failure_rate,
                "total_green_kg": round(total_green, 1),
                "total_roasted_kg": round(total_roasted, 1),
                "avg_batch_kg": avg_batch_kg,
                "max_batch_capacity_kg": MAX_ROASTER_CAPACITY_KG,
                "batch_efficiency_pct": batch_efficiency,
                "batches_per_day": batches_per_day,
                "max_batches_per_day": ROASTS_PER_DAY_CAPACITY,
                "operating_days": operating_days,
                "capacity_utilization_pct": utilization_pct,
                "max_daily_capacity_kg": max_daily_kg,
                "recommendations": recommendations,
            }
    except Exception as e:
        logger.error(f"[PROD-INTEL] capacity analysis failed: {e}")
        return {"error": str(e)}


# =============================================================================
# BATCH PLANNING
# =============================================================================


def recommend_next_batches() -> dict:
    """Recommend what to roast next based on inventory and demand.

    Produces batch-level planning:
      1. Calculates total need per roasted product (wholesale + retail deficit)
      2. Splits large needs into physical batches (max 25kg each)
      3. Groups batches by green coffee origin into roast sessions
      4. Sequences sessions to minimize setup changes
      5. Respects daily roaster capacity (4 batches/day max)
    """
    try:
        with SessionLocal() as session:
            roasted_items = (
                session.query(InventoryItem)
                .filter(InventoryItem.category == "roasted_coffee", InventoryItem.is_active == True)
                .all()
            )

            green_items = (
                session.query(InventoryItem)
                .filter(InventoryItem.category == "green_coffee", InventoryItem.is_active == True,
                        InventoryItem.quantity > 0)
                .all()
            )
            green_by_sku = {g.sku: g for g in green_items}
            # Track remaining green as we allocate (don't double-book)
            green_remaining = {g.sku: g.quantity for g in green_items}

            from maillard.models.operations import OrderLine, WholesaleOrder
            pending_lines = (
                session.query(OrderLine).join(WholesaleOrder)
                .filter(OrderLine.fulfilled == False,
                        WholesaleOrder.status.in_(["pending", "confirmed", "roasting"]))
                .all()
            )
            wholesale_demand: dict[str, float] = {}
            for line in pending_lines:
                wholesale_demand[line.product_sku] = wholesale_demand.get(line.product_sku, 0) + line.quantity_kg

            # ── Step 1: Calculate needs per roasted product ──────────
            needs: list[dict] = []
            for item in roasted_items:
                deficit = max(0, item.min_quantity - item.quantity)
                ws_need = wholesale_demand.get(item.sku, 0)
                total_need = deficit + ws_need
                if total_need <= 0:
                    continue

                green_needed = round(total_need / (1 - TYPICAL_LOSS_PCT / 100), 1)
                priority = "HIGH" if ws_need > 0 else ("MEDIUM" if deficit > 0 else "LOW")

                # Find matching green coffee
                matched_green = None
                for gsku, g in green_by_sku.items():
                    if green_remaining.get(gsku, 0) >= MIN_BATCH_KG:
                        matched_green = g
                        break

                if not matched_green:
                    continue

                # Cap by available green
                allocatable = min(green_needed, green_remaining[matched_green.sku])
                if allocatable < MIN_BATCH_KG:
                    continue

                needs.append({
                    "roasted_sku": item.sku,
                    "roasted_name": item.name,
                    "current_stock_kg": item.quantity,
                    "min_stock_kg": item.min_quantity,
                    "wholesale_demand_kg": round(ws_need, 1),
                    "total_need_kg": round(total_need, 1),
                    "green_coffee_sku": matched_green.sku,
                    "green_coffee_name": matched_green.name,
                    "green_needed_kg": round(allocatable, 1),
                    "priority": priority,
                })

                # Reserve the green coffee
                green_remaining[matched_green.sku] -= allocatable

            # Sort: HIGH first
            needs.sort(key=lambda n: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(n["priority"], 9))

            # ── Step 2: Split into physical batches (max 25kg) ───────
            batches: list[dict] = []
            for need in needs:
                remaining = need["green_needed_kg"]
                batch_num = 0
                while remaining >= MIN_BATCH_KG:
                    batch_num += 1
                    batch_kg = min(remaining, MAX_ROASTER_CAPACITY_KG)
                    # Prefer full batches; don't create tiny remnants
                    if remaining - batch_kg < MIN_BATCH_KG and remaining <= MAX_ROASTER_CAPACITY_KG:
                        batch_kg = remaining
                    expected_output = round(batch_kg * (1 - TYPICAL_LOSS_PCT / 100), 1)

                    batches.append({
                        "batch_sequence": 0,  # filled later
                        "roasted_sku": need["roasted_sku"],
                        "roasted_name": need["roasted_name"],
                        "green_coffee_sku": need["green_coffee_sku"],
                        "green_coffee_name": need["green_coffee_name"],
                        "batch_kg": round(batch_kg, 1),
                        "expected_output_kg": expected_output,
                        "efficiency_pct": round(batch_kg / MAX_ROASTER_CAPACITY_KG * 100, 0),
                        "priority": need["priority"],
                        "wholesale_demand_kg": need["wholesale_demand_kg"],
                        "total_need_kg": need["total_need_kg"],
                    })
                    remaining -= batch_kg

            # ── Step 3: Group into sessions by green coffee origin ────
            # A session = consecutive batches of the same green coffee.
            # This minimizes setup changes (hopper swap, profile change).
            sessions: list[dict] = []
            by_green: dict[str, list] = {}
            for b in batches:
                by_green.setdefault(b["green_coffee_sku"], []).append(b)

            session_num = 0
            for green_sku, session_batches in by_green.items():
                session_num += 1
                green_info = green_by_sku.get(green_sku)
                total_green = sum(b["batch_kg"] for b in session_batches)
                total_output = sum(b["expected_output_kg"] for b in session_batches)
                n = len(session_batches)

                # Number the batches within the session
                for i, b in enumerate(session_batches, 1):
                    b["batch_sequence"] = i

                # Time estimate: setup once + roast time per batch
                setup_min = 15
                roast_min_per_batch = 90  # preheat + roast + cool + QC
                # Same origin back-to-back saves ~20 min per subsequent batch (no hopper change)
                total_time = setup_min + (roast_min_per_batch * n) - (20 * max(0, n - 1))

                sessions.append({
                    "session": session_num,
                    "green_coffee_sku": green_sku,
                    "green_coffee_name": green_info.name if green_info else green_sku,
                    "total_green_kg": round(total_green, 1),
                    "total_output_kg": round(total_output, 1),
                    "batch_count": n,
                    "batches": session_batches,
                    "estimated_time_min": total_time,
                    "setup_changes": 0,  # no change within session
                    "priority": "HIGH" if any(b["priority"] == "HIGH" for b in session_batches) else "MEDIUM",
                })

            # ── Step 4: Capacity check ───────────────────────────────
            total_batches = sum(s["batch_count"] for s in sessions)
            fits_in_day = total_batches <= ROASTS_PER_DAY_CAPACITY
            days_needed = -(-total_batches // ROASTS_PER_DAY_CAPACITY)  # ceiling division
            total_time_min = sum(s["estimated_time_min"] for s in sessions)
            # Add 10 min for each setup change between sessions
            setup_changes = max(0, len(sessions) - 1)
            total_time_min += setup_changes * 10

            return {
                "sessions": sessions,
                "total_sessions": len(sessions),
                "total_batches": total_batches,
                "total_green_kg": round(sum(s["total_green_kg"] for s in sessions), 1),
                "total_output_kg": round(sum(s["total_output_kg"] for s in sessions), 1),
                "total_time_min": total_time_min,
                "setup_changes_between_sessions": setup_changes,
                "fits_in_one_day": fits_in_day,
                "days_needed": days_needed,
                "max_batches_per_day": ROASTS_PER_DAY_CAPACITY,
                "green_coffee_available": [
                    {"sku": g.sku, "name": g.name, "available_kg": g.quantity}
                    for g in green_items
                ],
                # Backward compat: flatten sessions into recommendations list
                "recommendations": [
                    {
                        "roasted_sku": b["roasted_sku"],
                        "roasted_name": b["roasted_name"],
                        "green_coffee_sku": b["green_coffee_sku"],
                        "green_coffee_name": b["green_coffee_name"],
                        "recommended_batch_kg": b["batch_kg"],
                        "expected_output_kg": b["expected_output_kg"],
                        "wholesale_demand_kg": b["wholesale_demand_kg"],
                        "total_need_kg": b["total_need_kg"],
                        "priority": b["priority"],
                    }
                    for s in sessions for b in s["batches"]
                ],
            }
    except Exception as e:
        logger.error(f"[PROD-INTEL] batch planning failed: {e}")
        return {"error": str(e)}


# =============================================================================
# PRODUCTION HEALTH REPORT
# =============================================================================


def get_production_health_report(days: int = DEFAULT_DAYS) -> dict:
    """Full production intelligence report."""
    from maillard.mcp.operations.production import get_production_summary

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period_days": days,
        "production_summary": get_production_summary(days=days),
        "loss_analysis": analyze_roast_losses(days=days),
        "capacity": get_capacity_analysis(days=days),
        "batch_plan": recommend_next_batches(),
    }
