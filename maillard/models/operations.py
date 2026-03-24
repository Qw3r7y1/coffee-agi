"""
SQLAlchemy models for Operations: Inventory, Production, and Wholesale.

Three systems in one module:
  1. Inventory — stock items, usage logs, waste logs, reorder alerts
  2. Production — roast batches (green → roasted), scheduling
  3. Wholesale — customers, orders, order lines, deliveries
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Column, DateTime, Float, Integer, String, Text, JSON,
    Boolean, ForeignKey, Enum,
)
from sqlalchemy.orm import relationship
from maillard.models.database import Base


def _utcnow():
    return datetime.now(timezone.utc)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════


class InventoryItem(Base):
    """A trackable stock item (green coffee, roasted coffee, milk, cups, etc.)."""
    __tablename__ = "inventory_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sku = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    category = Column(String(50), nullable=False, index=True)
    # categories: green_coffee, roasted_coffee, milk, consumables, packaging, equipment
    unit = Column(String(20), nullable=False)  # kg, liters, units, bags
    quantity = Column(Float, nullable=False, default=0)
    min_quantity = Column(Float, nullable=False, default=0)  # reorder threshold
    cost_per_unit = Column(Float, default=0)  # EUR
    supplier = Column(String(200))
    location = Column(String(100), default="main_store")  # main_store, bar, roastery
    notes = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    usage_logs = relationship("UsageLog", back_populates="item", lazy="dynamic")
    waste_logs = relationship("WasteLog", back_populates="item", lazy="dynamic")

    def __repr__(self):
        return f"<InventoryItem {self.sku}: {self.name} qty={self.quantity}{self.unit}>"

    @property
    def needs_reorder(self) -> bool:
        return self.quantity <= self.min_quantity and self.is_active

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "sku": self.sku,
            "name": self.name,
            "category": self.category,
            "unit": self.unit,
            "quantity": self.quantity,
            "min_quantity": self.min_quantity,
            "cost_per_unit": self.cost_per_unit,
            "supplier": self.supplier,
            "location": self.location,
            "needs_reorder": self.needs_reorder,
            "is_active": self.is_active,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class UsageLog(Base):
    """Records stock consumption (e.g., 2kg of Brazil beans used for roasting)."""
    __tablename__ = "usage_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer, ForeignKey("inventory_items.id"), nullable=False, index=True)
    quantity_used = Column(Float, nullable=False)
    usage_type = Column(String(50), nullable=False)
    # usage_types: roasting, bar_service, wholesale_order, transfer, other
    reference = Column(String(200))  # e.g., "roast_batch_42" or "wholesale_order_17"
    staff = Column(String(100))
    notes = Column(Text)
    logged_at = Column(DateTime, default=_utcnow)

    item = relationship("InventoryItem", back_populates="usage_logs")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "item_id": self.item_id,
            "item_name": self.item.name if self.item else None,
            "quantity_used": self.quantity_used,
            "usage_type": self.usage_type,
            "reference": self.reference,
            "staff": self.staff,
            "logged_at": self.logged_at.isoformat() if self.logged_at else None,
        }


class WasteLog(Base):
    """Records waste/spoilage (e.g., expired milk, failed roast batch)."""
    __tablename__ = "waste_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer, ForeignKey("inventory_items.id"), nullable=False, index=True)
    quantity_wasted = Column(Float, nullable=False)
    reason = Column(String(50), nullable=False)
    # reasons: expired, spoiled, spilled, defective, failed_roast, other
    cost_lost = Column(Float, default=0)  # EUR
    staff = Column(String(100))
    notes = Column(Text)
    logged_at = Column(DateTime, default=_utcnow)

    item = relationship("InventoryItem", back_populates="waste_logs")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "item_id": self.item_id,
            "item_name": self.item.name if self.item else None,
            "quantity_wasted": self.quantity_wasted,
            "reason": self.reason,
            "cost_lost": self.cost_lost,
            "staff": self.staff,
            "logged_at": self.logged_at.isoformat() if self.logged_at else None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. PRODUCTION (Coffee Roasting)
# ═══════════════════════════════════════════════════════════════════════════════


class RoastBatch(Base):
    """A single roasting batch: green input → roasted output."""
    __tablename__ = "roast_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_code = Column(String(50), unique=True, nullable=False, index=True)
    # Input
    green_coffee_sku = Column(String(50), nullable=False)  # FK-like reference to inventory
    green_coffee_name = Column(String(200))
    green_weight_kg = Column(Float, nullable=False)
    # Output
    roasted_weight_kg = Column(Float)
    roast_loss_percent = Column(Float)  # calculated: (green - roasted) / green * 100
    # Roast profile
    roast_level = Column(String(30))  # light, medium, medium-dark, dark
    roast_temp_c = Column(Float)
    roast_duration_min = Column(Float)
    # Status
    status = Column(String(20), nullable=False, default="scheduled")
    # statuses: scheduled, in_progress, completed, failed
    scheduled_date = Column(DateTime)
    completed_at = Column(DateTime)
    # Allocation
    roasted_sku = Column(String(50))  # output SKU in inventory
    retail_allocation_kg = Column(Float, default=0)
    wholesale_allocation_kg = Column(Float, default=0)
    # Meta
    roaster = Column(String(100))  # staff who roasted
    notes = Column(Text)
    created_at = Column(DateTime, default=_utcnow)

    def __repr__(self):
        return f"<RoastBatch {self.batch_code}: {self.green_weight_kg}kg → {self.roasted_weight_kg}kg>"

    def calculate_loss(self):
        """Calculate and set roast loss percentage."""
        if self.green_weight_kg and self.roasted_weight_kg:
            self.roast_loss_percent = round(
                (self.green_weight_kg - self.roasted_weight_kg) / self.green_weight_kg * 100, 1
            )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "batch_code": self.batch_code,
            "green_coffee_sku": self.green_coffee_sku,
            "green_coffee_name": self.green_coffee_name,
            "green_weight_kg": self.green_weight_kg,
            "roasted_weight_kg": self.roasted_weight_kg,
            "roast_loss_percent": self.roast_loss_percent,
            "roast_level": self.roast_level,
            "roast_temp_c": self.roast_temp_c,
            "roast_duration_min": self.roast_duration_min,
            "status": self.status,
            "scheduled_date": self.scheduled_date.isoformat() if self.scheduled_date else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "roasted_sku": self.roasted_sku,
            "retail_allocation_kg": self.retail_allocation_kg,
            "wholesale_allocation_kg": self.wholesale_allocation_kg,
            "roaster": self.roaster,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. WHOLESALE
# ═══════════════════════════════════════════════════════════════════════════════


class WholesaleCustomer(Base):
    """A wholesale customer (café, hotel, restaurant, reseller)."""
    __tablename__ = "wholesale_customers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    contact_person = Column(String(200))
    email = Column(String(200))
    phone = Column(String(50))
    address = Column(Text)
    customer_type = Column(String(50), default="cafe")
    # types: cafe, hotel, restaurant, office, reseller
    default_products = Column(JSON)  # list of SKUs they typically order
    payment_terms = Column(String(50), default="net_30")
    notes = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utcnow)

    orders = relationship("WholesaleOrder", back_populates="customer", lazy="dynamic")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "contact_person": self.contact_person,
            "email": self.email,
            "phone": self.phone,
            "customer_type": self.customer_type,
            "default_products": self.default_products,
            "payment_terms": self.payment_terms,
            "is_active": self.is_active,
        }


class WholesaleOrder(Base):
    """A wholesale order from a customer."""
    __tablename__ = "wholesale_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_number = Column(String(50), unique=True, nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("wholesale_customers.id"), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending")
    # statuses: pending, confirmed, roasting, ready, shipped, delivered, cancelled
    order_date = Column(DateTime, default=_utcnow)
    requested_delivery = Column(DateTime)
    actual_delivery = Column(DateTime)
    total_kg = Column(Float, default=0)
    total_eur = Column(Float, default=0)
    delivery_address = Column(Text)
    notes = Column(Text)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    customer = relationship("WholesaleCustomer", back_populates="orders")
    lines = relationship("OrderLine", back_populates="order", lazy="joined")

    def __repr__(self):
        return f"<WholesaleOrder {self.order_number}: {self.status}>"

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "order_number": self.order_number,
            "customer_id": self.customer_id,
            "customer_name": self.customer.name if self.customer else None,
            "status": self.status,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "requested_delivery": self.requested_delivery.isoformat() if self.requested_delivery else None,
            "actual_delivery": self.actual_delivery.isoformat() if self.actual_delivery else None,
            "total_kg": self.total_kg,
            "total_eur": self.total_eur,
            "lines": [l.to_dict() for l in self.lines] if self.lines else [],
            "notes": self.notes,
        }


class OrderLine(Base):
    """A line item within a wholesale order."""
    __tablename__ = "order_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("wholesale_orders.id"), nullable=False, index=True)
    product_sku = Column(String(50), nullable=False)
    product_name = Column(String(200))
    quantity_kg = Column(Float, nullable=False)
    price_per_kg = Column(Float, nullable=False)
    line_total = Column(Float)  # quantity_kg * price_per_kg
    roast_batch_code = Column(String(50))  # linked roast batch, if allocated
    fulfilled = Column(Boolean, default=False)

    order = relationship("WholesaleOrder", back_populates="lines")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "product_sku": self.product_sku,
            "product_name": self.product_name,
            "quantity_kg": self.quantity_kg,
            "price_per_kg": self.price_per_kg,
            "line_total": self.line_total,
            "roast_batch_code": self.roast_batch_code,
            "fulfilled": self.fulfilled,
        }


# =============================================================================
# 4. ACTION LOG (execution tracking)
# =============================================================================


class ActionLog(Base):
    """Tracks execution state of daily plan actions."""
    __tablename__ = "action_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    action_id = Column(String(100), nullable=False, index=True)  # unique key per action per day
    plan_date = Column(String(10), nullable=False, index=True)   # YYYY-MM-DD
    action_text = Column(Text, nullable=False)
    category = Column(String(30))       # inventory, production, wholesale
    sub_type = Column(String(30))       # order, roast_session, fulfill, waste
    priority = Column(String(10))
    status = Column(String(20), nullable=False, default="pending")
    # statuses: pending, in_progress, completed, skipped
    assigned_to = Column(String(100))
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    notes = Column(Text)
    action_data = Column(JSON)          # full action dict snapshot
    created_at = Column(DateTime, default=_utcnow)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "action_id": self.action_id,
            "plan_date": self.plan_date,
            "action": self.action_text,
            "category": self.category,
            "sub_type": self.sub_type,
            "priority": self.priority,
            "status": self.status,
            "assigned_to": self.assigned_to,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "notes": self.notes,
        }
