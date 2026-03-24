"""
Coffee Production System for Maillard Coffee Roasters.

Handles:
  - Roast batch creation and tracking (green -> roasted)
  - Roast scheduling
  - Roast loss calculation (typical 15-20% weight loss)
  - Wholesale vs retail allocation of roasted output
  - Integration with inventory (deducts green, adds roasted)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger

from maillard.models.database import SessionLocal
from maillard.models.operations import RoastBatch, InventoryItem
from maillard.mcp.operations.inventory import log_usage, update_stock, receive_stock


# ── Constants ────────────────────────────────────────────────────────────────
TYPICAL_ROAST_LOSS_PCT = 18.0  # Maillard reference: 18% weight loss
MIN_ROAST_LOSS_PCT = 10.0
MAX_ROAST_LOSS_PCT = 25.0


def _generate_batch_code() -> str:
    """Generate a unique batch code: RB-YYYYMMDD-NNN."""
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y%m%d")
    try:
        with SessionLocal() as session:
            count = (
                session.query(RoastBatch)
                .filter(RoastBatch.batch_code.like(f"RB-{date_part}-%"))
                .count()
            )
            return f"RB-{date_part}-{count + 1:03d}"
    except Exception:
        return f"RB-{date_part}-001"


# ═══════════════════════════════════════════════════════════════════════════════
# ROAST BATCH MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════


def schedule_roast(
    green_coffee_sku: str,
    green_weight_kg: float,
    roast_level: str = "medium",
    scheduled_date: str | None = None,
    roaster: str | None = None,
    notes: str | None = None,
) -> dict:
    """Schedule a new roast batch. Validates green stock is available."""
    try:
        with SessionLocal() as session:
            # Validate green coffee exists and has stock
            green_item = session.query(InventoryItem).filter_by(sku=green_coffee_sku).first()
            if not green_item:
                return {"error": f"Green coffee SKU '{green_coffee_sku}' not found in inventory"}

            if green_item.quantity < green_weight_kg:
                return {
                    "error": f"Insufficient green coffee: {green_item.quantity} kg available, "
                             f"{green_weight_kg} kg requested"
                }

            batch_code = _generate_batch_code()
            sched = None
            if scheduled_date:
                try:
                    sched = datetime.fromisoformat(scheduled_date)
                except ValueError:
                    return {"error": f"Invalid date format: {scheduled_date}. Use ISO format."}

            batch = RoastBatch(
                batch_code=batch_code,
                green_coffee_sku=green_coffee_sku,
                green_coffee_name=green_item.name,
                green_weight_kg=green_weight_kg,
                roast_level=roast_level,
                status="scheduled",
                scheduled_date=sched or datetime.now(timezone.utc),
                roaster=roaster,
                notes=notes,
            )
            session.add(batch)
            session.commit()
            session.refresh(batch)
            logger.info(f"[PRODUCTION] scheduled {batch_code}: {green_weight_kg}kg of {green_coffee_sku}")
            return batch.to_dict()
    except Exception as e:
        logger.error(f"[PRODUCTION] schedule_roast failed: {e}")
        return {"error": str(e)}


def start_roast(batch_code: str, roast_temp_c: float | None = None) -> dict:
    """Mark a roast batch as in progress. Deducts green coffee from inventory."""
    try:
        with SessionLocal() as session:
            batch = session.query(RoastBatch).filter_by(batch_code=batch_code).first()
            if not batch:
                return {"error": f"Batch '{batch_code}' not found"}
            if batch.status != "scheduled":
                return {"error": f"Batch is '{batch.status}', can only start 'scheduled' batches"}

            batch.status = "in_progress"
            if roast_temp_c:
                batch.roast_temp_c = roast_temp_c
            session.commit()

        # Deduct green coffee from inventory
        usage = log_usage(
            sku=batch.green_coffee_sku,
            quantity_used=batch.green_weight_kg,
            usage_type="roasting",
            reference=batch_code,
            staff=batch.roaster,
        )
        if "error" in usage:
            return {"error": f"Started roast but inventory deduction failed: {usage['error']}"}

        logger.info(f"[PRODUCTION] started {batch_code}")
        return {"batch": batch.to_dict(), "inventory_deducted": True}
    except Exception as e:
        logger.error(f"[PRODUCTION] start_roast failed: {e}")
        return {"error": str(e)}


def complete_roast(
    batch_code: str,
    roasted_weight_kg: float,
    roasted_sku: str | None = None,
    roast_duration_min: float | None = None,
    retail_allocation_kg: float = 0,
    wholesale_allocation_kg: float = 0,
    notes: str | None = None,
) -> dict:
    """Complete a roast batch. Calculates loss and adds roasted coffee to inventory."""
    try:
        with SessionLocal() as session:
            batch = session.query(RoastBatch).filter_by(batch_code=batch_code).first()
            if not batch:
                return {"error": f"Batch '{batch_code}' not found"}
            if batch.status != "in_progress":
                return {"error": f"Batch is '{batch.status}', can only complete 'in_progress' batches"}

            batch.roasted_weight_kg = roasted_weight_kg
            batch.calculate_loss()
            batch.status = "completed"
            batch.completed_at = datetime.now(timezone.utc)
            if roasted_sku:
                batch.roasted_sku = roasted_sku
            if roast_duration_min:
                batch.roast_duration_min = roast_duration_min
            if notes:
                batch.notes = (batch.notes or "") + f"\n{notes}"

            # Validate allocations
            total_alloc = retail_allocation_kg + wholesale_allocation_kg
            if total_alloc > roasted_weight_kg:
                return {"error": f"Allocations ({total_alloc}kg) exceed roasted output ({roasted_weight_kg}kg)"}
            batch.retail_allocation_kg = retail_allocation_kg
            batch.wholesale_allocation_kg = wholesale_allocation_kg

            session.commit()
            session.refresh(batch)

        # Validate roast loss
        warnings = []
        if batch.roast_loss_percent and batch.roast_loss_percent < MIN_ROAST_LOSS_PCT:
            warnings.append(f"Roast loss {batch.roast_loss_percent}% unusually low (expected {TYPICAL_ROAST_LOSS_PCT}%)")
        if batch.roast_loss_percent and batch.roast_loss_percent > MAX_ROAST_LOSS_PCT:
            warnings.append(f"Roast loss {batch.roast_loss_percent}% unusually high (expected {TYPICAL_ROAST_LOSS_PCT}%)")

        # Add roasted coffee to inventory if SKU provided
        inventory_added = False
        if roasted_sku:
            result = receive_stock(roasted_sku, roasted_weight_kg)
            if "error" not in result:
                inventory_added = True
            else:
                warnings.append(f"Could not add to inventory: {result['error']}")

        logger.info(
            f"[PRODUCTION] completed {batch_code}: {batch.green_weight_kg}kg -> "
            f"{roasted_weight_kg}kg (loss: {batch.roast_loss_percent}%)"
        )
        return {
            "batch": batch.to_dict(),
            "inventory_added": inventory_added,
            "warnings": warnings if warnings else None,
        }
    except Exception as e:
        logger.error(f"[PRODUCTION] complete_roast failed: {e}")
        return {"error": str(e)}


def fail_roast(batch_code: str, reason: str, staff: str | None = None) -> dict:
    """Mark a roast as failed and log waste."""
    try:
        with SessionLocal() as session:
            batch = session.query(RoastBatch).filter_by(batch_code=batch_code).first()
            if not batch:
                return {"error": f"Batch '{batch_code}' not found"}

            batch.status = "failed"
            batch.completed_at = datetime.now(timezone.utc)
            batch.notes = (batch.notes or "") + f"\nFAILED: {reason}"
            session.commit()

        # Log waste for the green coffee used
        from maillard.mcp.operations.inventory import log_waste
        log_waste(
            sku=batch.green_coffee_sku,
            quantity_wasted=0,  # already deducted at start
            reason="failed_roast",
            staff=staff,
            notes=f"Batch {batch_code}: {reason}",
        )

        logger.info(f"[PRODUCTION] FAILED {batch_code}: {reason}")
        return {"batch": batch.to_dict(), "waste_logged": True}
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# QUERIES
# ═══════════════════════════════════════════════════════════════════════════════


def get_batch(batch_code: str) -> dict:
    """Get a single roast batch."""
    try:
        with SessionLocal() as session:
            batch = session.query(RoastBatch).filter_by(batch_code=batch_code).first()
            if not batch:
                return {"error": f"Batch '{batch_code}' not found"}
            return batch.to_dict()
    except Exception as e:
        return {"error": str(e)}


def list_batches(
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List roast batches, optionally filtered by status."""
    try:
        with SessionLocal() as session:
            q = session.query(RoastBatch)
            if status:
                q = q.filter(RoastBatch.status == status)
            batches = q.order_by(RoastBatch.created_at.desc()).limit(limit).all()
            return [b.to_dict() for b in batches]
    except Exception as e:
        return []


def get_production_summary(days: int = 30) -> dict:
    """Production summary for the last N days."""
    try:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        with SessionLocal() as session:
            batches = (
                session.query(RoastBatch)
                .filter(RoastBatch.created_at >= cutoff)
                .all()
            )
            completed = [b for b in batches if b.status == "completed"]
            failed = [b for b in batches if b.status == "failed"]
            scheduled = [b for b in batches if b.status == "scheduled"]

            total_green = sum(b.green_weight_kg for b in completed)
            total_roasted = sum(b.roasted_weight_kg or 0 for b in completed)
            avg_loss = (
                sum(b.roast_loss_percent or 0 for b in completed) / len(completed)
                if completed else 0
            )
            total_wholesale = sum(b.wholesale_allocation_kg or 0 for b in completed)
            total_retail = sum(b.retail_allocation_kg or 0 for b in completed)

            return {
                "period_days": days,
                "total_batches": len(batches),
                "completed": len(completed),
                "failed": len(failed),
                "scheduled": len(scheduled),
                "total_green_kg": round(total_green, 2),
                "total_roasted_kg": round(total_roasted, 2),
                "avg_roast_loss_pct": round(avg_loss, 1),
                "wholesale_allocated_kg": round(total_wholesale, 2),
                "retail_allocated_kg": round(total_retail, 2),
                "unallocated_kg": round(total_roasted - total_wholesale - total_retail, 2),
            }
    except Exception as e:
        return {"error": str(e)}


def get_scheduled_roasts() -> list[dict]:
    """Get upcoming scheduled roasts."""
    return list_batches(status="scheduled")
