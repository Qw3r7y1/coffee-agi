"""
Inventory Management System for Maillard Coffee Roasters.

Handles:
  - Stock tracking (add, update, query)
  - Usage logging (consumption per purpose)
  - Waste tracking (spoilage, expired, failed roasts)
  - Reorder alerts (items below min threshold)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger
from sqlalchemy import func

from maillard.models.database import SessionLocal
from maillard.models.operations import InventoryItem, UsageLog, WasteLog


# ═══════════════════════════════════════════════════════════════════════════════
# STOCK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════


def add_item(
    sku: str,
    name: str,
    category: str,
    unit: str,
    quantity: float = 0,
    min_quantity: float = 0,
    cost_per_unit: float = 0,
    supplier: str | None = None,
    location: str = "main_store",
    notes: str | None = None,
) -> dict:
    """Add a new inventory item."""
    try:
        with SessionLocal() as session:
            existing = session.query(InventoryItem).filter_by(sku=sku).first()
            if existing:
                return {"error": f"SKU '{sku}' already exists: {existing.name}"}

            item = InventoryItem(
                sku=sku, name=name, category=category, unit=unit,
                quantity=quantity, min_quantity=min_quantity,
                cost_per_unit=cost_per_unit, supplier=supplier,
                location=location, notes=notes,
            )
            session.add(item)
            session.commit()
            session.refresh(item)
            logger.info(f"[INVENTORY] added {sku}: {name} ({quantity} {unit})")
            return item.to_dict()
    except Exception as e:
        logger.error(f"[INVENTORY] add_item failed: {e}")
        return {"error": str(e)}


def update_stock(sku: str, quantity: float, mode: str = "set") -> dict:
    """Update stock level. mode: 'set' (absolute) or 'adjust' (delta)."""
    try:
        with SessionLocal() as session:
            item = session.query(InventoryItem).filter_by(sku=sku).first()
            if not item:
                return {"error": f"SKU '{sku}' not found"}

            old_qty = item.quantity
            if mode == "set":
                item.quantity = quantity
            elif mode == "adjust":
                item.quantity += quantity
            else:
                return {"error": f"Invalid mode '{mode}'. Use 'set' or 'adjust'."}

            item.updated_at = datetime.now(timezone.utc)
            session.commit()
            logger.info(f"[INVENTORY] {sku}: {old_qty} -> {item.quantity} {item.unit} (mode={mode})")
            session.refresh(item)
            return item.to_dict()
    except Exception as e:
        logger.error(f"[INVENTORY] update_stock failed: {e}")
        return {"error": str(e)}


def receive_stock(sku: str, quantity: float, cost_per_unit: float | None = None, notes: str | None = None) -> dict:
    """Receive new stock (e.g., delivery from supplier). Adds to current quantity."""
    try:
        with SessionLocal() as session:
            item = session.query(InventoryItem).filter_by(sku=sku).first()
            if not item:
                return {"error": f"SKU '{sku}' not found"}

            item.quantity += quantity
            if cost_per_unit is not None:
                item.cost_per_unit = cost_per_unit
            item.updated_at = datetime.now(timezone.utc)
            session.commit()
            session.refresh(item)
            logger.info(f"[INVENTORY] received {quantity} {item.unit} of {sku} (new qty: {item.quantity})")
            return {"received": quantity, "item": item.to_dict()}
    except Exception as e:
        logger.error(f"[INVENTORY] receive_stock failed: {e}")
        return {"error": str(e)}


def get_item(sku: str) -> dict:
    """Get a single inventory item by SKU."""
    try:
        with SessionLocal() as session:
            item = session.query(InventoryItem).filter_by(sku=sku).first()
            if not item:
                return {"error": f"SKU '{sku}' not found"}
            return item.to_dict()
    except Exception as e:
        return {"error": str(e)}


def list_items(
    category: str | None = None,
    location: str | None = None,
    active_only: bool = True,
) -> list[dict]:
    """List inventory items with optional filters."""
    try:
        with SessionLocal() as session:
            q = session.query(InventoryItem)
            if active_only:
                q = q.filter(InventoryItem.is_active == True)
            if category:
                q = q.filter(InventoryItem.category == category)
            if location:
                q = q.filter(InventoryItem.location == location)
            items = q.order_by(InventoryItem.category, InventoryItem.name).all()
            return [i.to_dict() for i in items]
    except Exception as e:
        logger.error(f"[INVENTORY] list_items failed: {e}")
        return []


def get_reorder_alerts() -> list[dict]:
    """Get all items at or below their reorder threshold."""
    try:
        with SessionLocal() as session:
            items = (
                session.query(InventoryItem)
                .filter(
                    InventoryItem.is_active == True,
                    InventoryItem.quantity <= InventoryItem.min_quantity,
                )
                .order_by(InventoryItem.category)
                .all()
            )
            alerts = []
            for item in items:
                alerts.append({
                    **item.to_dict(),
                    "deficit": round(item.min_quantity - item.quantity, 2),
                    "alert": f"LOW STOCK: {item.name} at {item.quantity} {item.unit} (min: {item.min_quantity})",
                })
            logger.info(f"[INVENTORY] {len(alerts)} reorder alerts")
            return alerts
    except Exception as e:
        logger.error(f"[INVENTORY] get_reorder_alerts failed: {e}")
        return []


def get_stock_value(category: str | None = None) -> dict:
    """Calculate total stock value, optionally by category."""
    try:
        with SessionLocal() as session:
            q = session.query(InventoryItem).filter(InventoryItem.is_active == True)
            if category:
                q = q.filter(InventoryItem.category == category)
            items = q.all()
            total = sum(i.quantity * i.cost_per_unit for i in items)
            by_cat: dict[str, float] = {}
            for i in items:
                by_cat[i.category] = by_cat.get(i.category, 0) + (i.quantity * i.cost_per_unit)
            return {
                "total_value_eur": round(total, 2),
                "by_category": {k: round(v, 2) for k, v in by_cat.items()},
                "item_count": len(items),
            }
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# USAGE TRACKING
# ═══════════════════════════════════════════════════════════════════════════════


def log_usage(
    sku: str,
    quantity_used: float,
    usage_type: str,
    reference: str | None = None,
    staff: str | None = None,
    notes: str | None = None,
) -> dict:
    """Log stock consumption and deduct from inventory."""
    try:
        with SessionLocal() as session:
            item = session.query(InventoryItem).filter_by(sku=sku).first()
            if not item:
                return {"error": f"SKU '{sku}' not found"}

            if item.quantity < quantity_used:
                return {
                    "error": f"Insufficient stock: {item.quantity} {item.unit} available, "
                             f"{quantity_used} requested"
                }

            # Deduct from inventory
            item.quantity -= quantity_used
            item.updated_at = datetime.now(timezone.utc)

            # Create log entry
            log = UsageLog(
                item_id=item.id,
                quantity_used=quantity_used,
                usage_type=usage_type,
                reference=reference,
                staff=staff,
                notes=notes,
            )
            session.add(log)
            session.commit()
            logger.info(f"[INVENTORY] used {quantity_used} {item.unit} of {sku} ({usage_type})")
            return {
                "logged": True,
                "usage": log.to_dict(),
                "remaining": item.quantity,
                "needs_reorder": item.needs_reorder,
            }
    except Exception as e:
        logger.error(f"[INVENTORY] log_usage failed: {e}")
        return {"error": str(e)}


def get_usage_history(
    sku: str | None = None,
    usage_type: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Get usage history, optionally filtered."""
    try:
        with SessionLocal() as session:
            q = session.query(UsageLog)
            if sku:
                item = session.query(InventoryItem).filter_by(sku=sku).first()
                if item:
                    q = q.filter(UsageLog.item_id == item.id)
            if usage_type:
                q = q.filter(UsageLog.usage_type == usage_type)
            logs = q.order_by(UsageLog.logged_at.desc()).limit(limit).all()
            return [l.to_dict() for l in logs]
    except Exception as e:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# WASTE TRACKING
# ═══════════════════════════════════════════════════════════════════════════════


def log_waste(
    sku: str,
    quantity_wasted: float,
    reason: str,
    staff: str | None = None,
    notes: str | None = None,
) -> dict:
    """Log waste/spoilage and deduct from inventory."""
    try:
        with SessionLocal() as session:
            item = session.query(InventoryItem).filter_by(sku=sku).first()
            if not item:
                return {"error": f"SKU '{sku}' not found"}

            # Deduct from inventory
            item.quantity = max(0, item.quantity - quantity_wasted)
            item.updated_at = datetime.now(timezone.utc)

            cost_lost = round(quantity_wasted * item.cost_per_unit, 2)

            log = WasteLog(
                item_id=item.id,
                quantity_wasted=quantity_wasted,
                reason=reason,
                cost_lost=cost_lost,
                staff=staff,
                notes=notes,
            )
            session.add(log)
            session.commit()
            logger.info(f"[INVENTORY] WASTE: {quantity_wasted} {item.unit} of {sku} ({reason}) = EUR {cost_lost}")
            return {
                "logged": True,
                "waste": log.to_dict(),
                "cost_lost_eur": cost_lost,
                "remaining": item.quantity,
            }
    except Exception as e:
        logger.error(f"[INVENTORY] log_waste failed: {e}")
        return {"error": str(e)}


def get_waste_summary(days: int = 30) -> dict:
    """Get waste summary for the last N days."""
    try:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        with SessionLocal() as session:
            logs = (
                session.query(WasteLog)
                .filter(WasteLog.logged_at >= cutoff)
                .all()
            )
            total_cost = sum(l.cost_lost for l in logs)
            by_reason: dict[str, float] = {}
            for l in logs:
                by_reason[l.reason] = by_reason.get(l.reason, 0) + l.cost_lost
            return {
                "period_days": days,
                "total_entries": len(logs),
                "total_cost_lost_eur": round(total_cost, 2),
                "by_reason": {k: round(v, 2) for k, v in by_reason.items()},
            }
    except Exception as e:
        return {"error": str(e)}
