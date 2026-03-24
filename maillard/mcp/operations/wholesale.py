"""
Wholesale Fulfillment System for Maillard Coffee Roasters.

Handles:
  - Customer management
  - Order tracking (pending -> confirmed -> roasting -> ready -> shipped -> delivered)
  - Production matching (link orders to roast batches)
  - Delivery planning
  - Customer demand tracking
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger

from maillard.models.database import SessionLocal
from maillard.models.operations import (
    WholesaleCustomer, WholesaleOrder, OrderLine, RoastBatch,
)


def _generate_order_number() -> str:
    """Generate a unique order number: WO-YYYYMMDD-NNN."""
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y%m%d")
    try:
        with SessionLocal() as session:
            count = (
                session.query(WholesaleOrder)
                .filter(WholesaleOrder.order_number.like(f"WO-{date_part}-%"))
                .count()
            )
            return f"WO-{date_part}-{count + 1:03d}"
    except Exception:
        return f"WO-{date_part}-001"


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOMER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════


def add_customer(
    name: str,
    contact_person: str | None = None,
    email: str | None = None,
    phone: str | None = None,
    address: str | None = None,
    customer_type: str = "cafe",
    payment_terms: str = "net_30",
    notes: str | None = None,
) -> dict:
    """Add a new wholesale customer."""
    try:
        with SessionLocal() as session:
            cust = WholesaleCustomer(
                name=name, contact_person=contact_person, email=email,
                phone=phone, address=address, customer_type=customer_type,
                payment_terms=payment_terms, notes=notes,
            )
            session.add(cust)
            session.commit()
            session.refresh(cust)
            logger.info(f"[WHOLESALE] added customer: {name}")
            return cust.to_dict()
    except Exception as e:
        logger.error(f"[WHOLESALE] add_customer failed: {e}")
        return {"error": str(e)}


def list_customers(active_only: bool = True) -> list[dict]:
    """List wholesale customers."""
    try:
        with SessionLocal() as session:
            q = session.query(WholesaleCustomer)
            if active_only:
                q = q.filter(WholesaleCustomer.is_active == True)
            return [c.to_dict() for c in q.order_by(WholesaleCustomer.name).all()]
    except Exception as e:
        return []


def get_customer(customer_id: int) -> dict:
    """Get a customer by ID."""
    try:
        with SessionLocal() as session:
            cust = session.get(WholesaleCustomer, customer_id)
            if not cust:
                return {"error": f"Customer {customer_id} not found"}
            return cust.to_dict()
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════


def create_order(
    customer_id: int,
    lines: list[dict],
    requested_delivery: str | None = None,
    delivery_address: str | None = None,
    notes: str | None = None,
) -> dict:
    """Create a new wholesale order.

    lines: [{"product_sku": "...", "product_name": "...", "quantity_kg": N, "price_per_kg": N}, ...]
    """
    try:
        with SessionLocal() as session:
            cust = session.get(WholesaleCustomer, customer_id)
            if not cust:
                return {"error": f"Customer {customer_id} not found"}

            order_number = _generate_order_number()
            delivery_dt = None
            if requested_delivery:
                try:
                    delivery_dt = datetime.fromisoformat(requested_delivery)
                except ValueError:
                    return {"error": f"Invalid date: {requested_delivery}"}

            order = WholesaleOrder(
                order_number=order_number,
                customer_id=customer_id,
                status="pending",
                requested_delivery=delivery_dt,
                delivery_address=delivery_address or cust.address,
                notes=notes,
            )
            session.add(order)
            session.flush()  # get order.id

            total_kg = 0
            total_eur = 0
            for line_data in lines:
                qty = line_data.get("quantity_kg", 0)
                price = line_data.get("price_per_kg", 0)
                line_total = round(qty * price, 2)
                line = OrderLine(
                    order_id=order.id,
                    product_sku=line_data.get("product_sku", ""),
                    product_name=line_data.get("product_name", ""),
                    quantity_kg=qty,
                    price_per_kg=price,
                    line_total=line_total,
                )
                session.add(line)
                total_kg += qty
                total_eur += line_total

            order.total_kg = round(total_kg, 2)
            order.total_eur = round(total_eur, 2)
            session.commit()
            session.refresh(order)
            logger.info(f"[WHOLESALE] created {order_number}: {total_kg}kg, EUR {total_eur}")
            return order.to_dict()
    except Exception as e:
        logger.error(f"[WHOLESALE] create_order failed: {e}")
        return {"error": str(e)}


def update_order_status(order_number: str, new_status: str, notes: str | None = None) -> dict:
    """Update an order's status."""
    valid_statuses = ["pending", "confirmed", "roasting", "ready", "shipped", "delivered", "cancelled"]
    if new_status not in valid_statuses:
        return {"error": f"Invalid status '{new_status}'. Valid: {valid_statuses}"}

    try:
        with SessionLocal() as session:
            order = session.query(WholesaleOrder).filter_by(order_number=order_number).first()
            if not order:
                return {"error": f"Order '{order_number}' not found"}

            old_status = order.status
            order.status = new_status
            if notes:
                order.notes = (order.notes or "") + f"\n[{new_status}] {notes}"
            if new_status == "delivered":
                order.actual_delivery = datetime.now(timezone.utc)
            order.updated_at = datetime.now(timezone.utc)
            session.commit()
            session.refresh(order)
            logger.info(f"[WHOLESALE] {order_number}: {old_status} -> {new_status}")
            return order.to_dict()
    except Exception as e:
        return {"error": str(e)}


def link_batch_to_order(order_number: str, product_sku: str, batch_code: str) -> dict:
    """Link a roast batch to an order line for fulfillment tracking."""
    try:
        with SessionLocal() as session:
            order = session.query(WholesaleOrder).filter_by(order_number=order_number).first()
            if not order:
                return {"error": f"Order '{order_number}' not found"}

            line = (
                session.query(OrderLine)
                .filter(
                    OrderLine.order_id == order.id,
                    OrderLine.product_sku == product_sku,
                    OrderLine.fulfilled == False,
                )
                .first()
            )
            if not line:
                return {"error": f"No unfulfilled line for SKU '{product_sku}' in {order_number}"}

            line.roast_batch_code = batch_code
            line.fulfilled = True
            session.commit()

            # Check if all lines fulfilled
            all_fulfilled = all(l.fulfilled for l in order.lines)
            if all_fulfilled and order.status == "roasting":
                order.status = "ready"
                order.updated_at = datetime.now(timezone.utc)
                session.commit()

            logger.info(f"[WHOLESALE] linked batch {batch_code} -> {order_number}/{product_sku}")
            return {
                "linked": True,
                "order": order.to_dict(),
                "all_lines_fulfilled": all_fulfilled,
            }
    except Exception as e:
        return {"error": str(e)}


def get_order(order_number: str) -> dict:
    """Get a single order with lines."""
    try:
        with SessionLocal() as session:
            order = session.query(WholesaleOrder).filter_by(order_number=order_number).first()
            if not order:
                return {"error": f"Order '{order_number}' not found"}
            return order.to_dict()
    except Exception as e:
        return {"error": str(e)}


def list_orders(
    status: str | None = None,
    customer_id: int | None = None,
    limit: int = 50,
) -> list[dict]:
    """List orders with optional filters."""
    try:
        with SessionLocal() as session:
            q = session.query(WholesaleOrder)
            if status:
                q = q.filter(WholesaleOrder.status == status)
            if customer_id:
                q = q.filter(WholesaleOrder.customer_id == customer_id)
            orders = q.order_by(WholesaleOrder.created_at.desc()).limit(limit).all()
            return [o.to_dict() for o in orders]
    except Exception as e:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# PLANNING & DEMAND
# ═══════════════════════════════════════════════════════════════════════════════


def get_pending_demand() -> dict:
    """Calculate total unfulfilled wholesale demand by product."""
    try:
        with SessionLocal() as session:
            unfulfilled = (
                session.query(OrderLine)
                .join(WholesaleOrder)
                .filter(
                    OrderLine.fulfilled == False,
                    WholesaleOrder.status.in_(["pending", "confirmed", "roasting"]),
                )
                .all()
            )
            demand: dict[str, float] = {}
            for line in unfulfilled:
                key = line.product_sku
                demand[key] = demand.get(key, 0) + line.quantity_kg

            return {
                "total_pending_kg": round(sum(demand.values()), 2),
                "by_product": {k: round(v, 2) for k, v in demand.items()},
                "unfulfilled_lines": len(unfulfilled),
            }
    except Exception as e:
        return {"error": str(e)}


def get_delivery_schedule(days_ahead: int = 14) -> list[dict]:
    """Get orders with deliveries in the next N days."""
    try:
        cutoff = datetime.now(timezone.utc) + timedelta(days=days_ahead)
        with SessionLocal() as session:
            orders = (
                session.query(WholesaleOrder)
                .filter(
                    WholesaleOrder.requested_delivery <= cutoff,
                    WholesaleOrder.status.in_(["confirmed", "roasting", "ready"]),
                )
                .order_by(WholesaleOrder.requested_delivery)
                .all()
            )
            return [o.to_dict() for o in orders]
    except Exception as e:
        return []


def get_customer_demand_history(customer_id: int, months: int = 6) -> dict:
    """Track a customer's ordering history over time."""
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)
        with SessionLocal() as session:
            cust = session.get(WholesaleCustomer, customer_id)
            if not cust:
                return {"error": f"Customer {customer_id} not found"}

            orders = (
                session.query(WholesaleOrder)
                .filter(
                    WholesaleOrder.customer_id == customer_id,
                    WholesaleOrder.order_date >= cutoff,
                    WholesaleOrder.status != "cancelled",
                )
                .all()
            )
            total_kg = sum(o.total_kg for o in orders)
            total_eur = sum(o.total_eur for o in orders)
            return {
                "customer": cust.name,
                "period_months": months,
                "order_count": len(orders),
                "total_kg": round(total_kg, 2),
                "total_eur": round(total_eur, 2),
                "avg_order_kg": round(total_kg / len(orders), 2) if orders else 0,
                "avg_order_eur": round(total_eur / len(orders), 2) if orders else 0,
            }
    except Exception as e:
        return {"error": str(e)}
