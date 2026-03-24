"""
Wholesale Fulfillment Intelligence for Maillard Coffee Roasters.

Analyzes wholesale operations to provide:
  - Demand forecasting (per customer, per product)
  - Production-to-order gap analysis
  - Delivery risk scoring
  - Customer value ranking
  - Fulfillment rate tracking
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any
from collections import defaultdict

from loguru import logger
from sqlalchemy import func

from maillard.models.database import SessionLocal
from maillard.models.operations import (
    WholesaleCustomer, WholesaleOrder, OrderLine, RoastBatch, InventoryItem,
)

DEFAULT_DAYS = 90  # longer window for wholesale trends


# =============================================================================
# DEMAND FORECASTING
# =============================================================================


def forecast_demand(days_history: int = DEFAULT_DAYS, forecast_weeks: int = 4) -> dict:
    """Forecast wholesale demand based on historical order patterns.

    Uses simple weekly averaging -- the right approach for a small roastery
    where order patterns are regular but volumes vary.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_history)
    try:
        with SessionLocal() as session:
            orders = (
                session.query(WholesaleOrder)
                .filter(
                    WholesaleOrder.order_date >= cutoff,
                    WholesaleOrder.status != "cancelled",
                )
                .all()
            )

            if not orders:
                return {"forecast_weeks": forecast_weeks, "message": "No order history to forecast from."}

            weeks = days_history / 7
            total_kg = sum(o.total_kg for o in orders)
            total_eur = sum(o.total_eur for o in orders)
            weekly_avg_kg = round(total_kg / weeks, 1) if weeks else 0
            weekly_avg_eur = round(total_eur / weeks, 2) if weeks else 0

            # Per-product demand
            all_lines = (
                session.query(OrderLine)
                .join(WholesaleOrder)
                .filter(
                    WholesaleOrder.order_date >= cutoff,
                    WholesaleOrder.status != "cancelled",
                )
                .all()
            )

            product_demand: dict[str, float] = defaultdict(float)
            for line in all_lines:
                product_demand[line.product_sku] += line.quantity_kg

            product_forecast = {}
            for sku, total in product_demand.items():
                weekly = round(total / weeks, 1) if weeks else 0
                product_forecast[sku] = {
                    "weekly_avg_kg": weekly,
                    "forecast_total_kg": round(weekly * forecast_weeks, 1),
                    "history_total_kg": round(total, 1),
                }

            # Per-customer demand
            customer_demand: dict[int, dict] = {}
            for o in orders:
                cid = o.customer_id
                if cid not in customer_demand:
                    cust = session.get(WholesaleCustomer, cid)
                    customer_demand[cid] = {
                        "customer_name": cust.name if cust else f"#{cid}",
                        "total_kg": 0, "total_eur": 0, "order_count": 0,
                    }
                customer_demand[cid]["total_kg"] += o.total_kg
                customer_demand[cid]["total_eur"] += o.total_eur
                customer_demand[cid]["order_count"] += 1

            customer_forecast = {}
            for cid, data in customer_demand.items():
                weekly_kg = round(data["total_kg"] / weeks, 1) if weeks else 0
                customer_forecast[data["customer_name"]] = {
                    "weekly_avg_kg": weekly_kg,
                    "forecast_total_kg": round(weekly_kg * forecast_weeks, 1),
                    "order_frequency_per_week": round(data["order_count"] / weeks, 2),
                    "avg_order_value_eur": round(data["total_eur"] / data["order_count"], 2) if data["order_count"] else 0,
                }

            return {
                "history_days": days_history,
                "forecast_weeks": forecast_weeks,
                "weekly_avg_kg": weekly_avg_kg,
                "weekly_avg_eur": weekly_avg_eur,
                "forecast_total_kg": round(weekly_avg_kg * forecast_weeks, 1),
                "forecast_total_eur": round(weekly_avg_eur * forecast_weeks, 2),
                "by_product": product_forecast,
                "by_customer": customer_forecast,
            }
    except Exception as e:
        logger.error(f"[WS-INTEL] demand forecast failed: {e}")
        return {"error": str(e)}


# =============================================================================
# PRODUCTION GAP ANALYSIS
# =============================================================================


def analyze_production_gap() -> dict:
    """Compare pending wholesale demand against available roasted stock and scheduled production.

    Identifies:
      - Which products have enough stock
      - Which need additional roasting
      - Total production deficit
    """
    try:
        with SessionLocal() as session:
            # Pending demand
            pending_lines = (
                session.query(OrderLine)
                .join(WholesaleOrder)
                .filter(
                    OrderLine.fulfilled == False,
                    WholesaleOrder.status.in_(["pending", "confirmed", "roasting"]),
                )
                .all()
            )

            demand_by_sku: dict[str, float] = defaultdict(float)
            for line in pending_lines:
                demand_by_sku[line.product_sku] += line.quantity_kg

            # Available roasted stock
            roasted_items = (
                session.query(InventoryItem)
                .filter(InventoryItem.category == "roasted_coffee", InventoryItem.is_active == True)
                .all()
            )
            stock_by_sku = {i.sku: i.quantity for i in roasted_items}

            # Scheduled/in-progress production
            active_batches = (
                session.query(RoastBatch)
                .filter(RoastBatch.status.in_(["scheduled", "in_progress"]))
                .all()
            )
            incoming_by_sku: dict[str, float] = defaultdict(float)
            for b in active_batches:
                if b.roasted_sku:
                    # Estimate output using typical loss
                    expected = b.green_weight_kg * 0.82
                    incoming_by_sku[b.roasted_sku] += expected

            # Gap analysis per product
            gap_items = []
            total_demand = 0
            total_available = 0
            total_deficit = 0

            all_skus = set(demand_by_sku.keys()) | set(stock_by_sku.keys())
            for sku in sorted(all_skus):
                demand = demand_by_sku.get(sku, 0)
                stock = stock_by_sku.get(sku, 0)
                incoming = incoming_by_sku.get(sku, 0)
                available = stock + incoming
                gap = demand - available

                total_demand += demand
                total_available += available
                if gap > 0:
                    total_deficit += gap

                # Find name
                item = next((i for i in roasted_items if i.sku == sku), None)
                name = item.name if item else sku

                status = "COVERED" if gap <= 0 else ("PARTIAL" if stock > 0 or incoming > 0 else "DEFICIT")

                gap_items.append({
                    "sku": sku,
                    "name": name,
                    "demand_kg": round(demand, 1),
                    "in_stock_kg": round(stock, 1),
                    "in_production_kg": round(incoming, 1),
                    "total_available_kg": round(available, 1),
                    "gap_kg": round(max(0, gap), 1),
                    "status": status,
                })

            gap_items.sort(key=lambda g: -g["gap_kg"])

            return {
                "total_pending_demand_kg": round(total_demand, 1),
                "total_available_kg": round(total_available, 1),
                "total_deficit_kg": round(total_deficit, 1),
                "coverage_pct": round(total_available / total_demand * 100, 1) if total_demand else 100,
                "items": gap_items,
                "needs_production": [g for g in gap_items if g["status"] != "COVERED"],
            }
    except Exception as e:
        logger.error(f"[WS-INTEL] production gap analysis failed: {e}")
        return {"error": str(e)}


# =============================================================================
# DELIVERY RISK SCORING
# =============================================================================


def score_delivery_risk() -> list[dict]:
    """Score each pending order for delivery risk.

    Risk factors:
      - Product not in stock (needs roasting)
      - Delivery deadline approaching
      - Large order volume
      - Customer history (new vs established)
    """
    try:
        with SessionLocal() as session:
            orders = (
                session.query(WholesaleOrder)
                .filter(WholesaleOrder.status.in_(["pending", "confirmed", "roasting", "ready"]))
                .all()
            )

            results = []
            now = datetime.now(timezone.utc)

            for order in orders:
                risk_score = 0
                risk_factors = []

                # Check if all lines are fulfilled
                unfulfilled = [l for l in order.lines if not l.fulfilled]
                if unfulfilled:
                    risk_score += 30
                    unfulfilled_kg = sum(l.quantity_kg for l in unfulfilled)
                    risk_factors.append(f"{len(unfulfilled)} lines unfulfilled ({unfulfilled_kg:.1f}kg)")

                # Deadline proximity
                if order.requested_delivery:
                    days_left = (order.requested_delivery - now).total_seconds() / 86400
                    if days_left < 0:
                        risk_score += 40
                        risk_factors.append(f"OVERDUE by {abs(days_left):.0f} days")
                    elif days_left < 2:
                        risk_score += 25
                        risk_factors.append(f"Due in {days_left:.0f} days")
                    elif days_left < 5:
                        risk_score += 10
                        risk_factors.append(f"Due in {days_left:.0f} days")

                # Order size
                if order.total_kg > 20:
                    risk_score += 10
                    risk_factors.append(f"Large order ({order.total_kg}kg)")

                # Status progression
                if order.status == "pending":
                    risk_score += 15
                    risk_factors.append("Still pending (not confirmed)")

                risk_score = min(risk_score, 100)
                risk_level = (
                    "CRITICAL" if risk_score >= 60 else
                    "HIGH" if risk_score >= 40 else
                    "MEDIUM" if risk_score >= 20 else "LOW"
                )

                results.append({
                    "order_number": order.order_number,
                    "customer": order.customer.name if order.customer else f"#{order.customer_id}",
                    "status": order.status,
                    "total_kg": order.total_kg,
                    "total_eur": order.total_eur,
                    "requested_delivery": order.requested_delivery.isoformat() if order.requested_delivery else None,
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "risk_factors": risk_factors,
                })

            results.sort(key=lambda r: -r["risk_score"])
            return results
    except Exception as e:
        logger.error(f"[WS-INTEL] delivery risk scoring failed: {e}")
        return []


# =============================================================================
# CUSTOMER VALUE RANKING
# =============================================================================


def rank_customers(days: int = DEFAULT_DAYS) -> list[dict]:
    """Rank wholesale customers by value and reliability.

    Metrics:
      - Total revenue
      - Order frequency
      - Average order size
      - Growth trend
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with SessionLocal() as session:
            customers = session.query(WholesaleCustomer).filter(WholesaleCustomer.is_active == True).all()
            rankings = []

            for cust in customers:
                orders = (
                    session.query(WholesaleOrder)
                    .filter(
                        WholesaleOrder.customer_id == cust.id,
                        WholesaleOrder.order_date >= cutoff,
                        WholesaleOrder.status != "cancelled",
                    )
                    .order_by(WholesaleOrder.order_date)
                    .all()
                )

                if not orders:
                    continue

                total_kg = sum(o.total_kg for o in orders)
                total_eur = sum(o.total_eur for o in orders)
                weeks = days / 7

                # Growth: compare first half vs second half
                mid = len(orders) // 2
                if mid > 0:
                    older_rev = sum(o.total_eur for o in orders[:mid])
                    newer_rev = sum(o.total_eur for o in orders[mid:])
                    growth = "growing" if newer_rev > older_rev * 1.1 else (
                        "declining" if newer_rev < older_rev * 0.9 else "stable"
                    )
                else:
                    growth = "new"

                rankings.append({
                    "customer_id": cust.id,
                    "name": cust.name,
                    "type": cust.customer_type,
                    "total_revenue_eur": round(total_eur, 2),
                    "total_kg": round(total_kg, 1),
                    "order_count": len(orders),
                    "orders_per_week": round(len(orders) / weeks, 2) if weeks else 0,
                    "avg_order_eur": round(total_eur / len(orders), 2),
                    "avg_order_kg": round(total_kg / len(orders), 1),
                    "growth_trend": growth,
                })

            rankings.sort(key=lambda r: -r["total_revenue_eur"])

            # Assign tier
            for i, r in enumerate(rankings):
                if i == 0 or r["total_revenue_eur"] > 1000:
                    r["tier"] = "GOLD"
                elif r["total_revenue_eur"] > 500:
                    r["tier"] = "SILVER"
                else:
                    r["tier"] = "BRONZE"

            return rankings
    except Exception as e:
        logger.error(f"[WS-INTEL] customer ranking failed: {e}")
        return []


# =============================================================================
# FULFILLMENT RATE
# =============================================================================


def get_fulfillment_metrics(days: int = DEFAULT_DAYS) -> dict:
    """Track how well we're fulfilling wholesale orders.

    Key metrics:
      - On-time delivery rate
      - Complete fulfillment rate
      - Average fulfillment time
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        with SessionLocal() as session:
            delivered = (
                session.query(WholesaleOrder)
                .filter(
                    WholesaleOrder.status == "delivered",
                    WholesaleOrder.actual_delivery >= cutoff,
                )
                .all()
            )

            if not delivered:
                return {"period_days": days, "delivered_orders": 0, "message": "No deliveries in period."}

            on_time = 0
            late = 0
            lead_times = []

            for o in delivered:
                if o.requested_delivery and o.actual_delivery:
                    if o.actual_delivery <= o.requested_delivery + timedelta(days=1):
                        on_time += 1
                    else:
                        late += 1
                    lt = (o.actual_delivery - o.order_date).total_seconds() / 86400
                    lead_times.append(lt)

            total = on_time + late
            on_time_pct = round(on_time / total * 100, 1) if total else 0
            avg_lead = round(sum(lead_times) / len(lead_times), 1) if lead_times else 0

            total_revenue = sum(o.total_eur for o in delivered)
            total_kg = sum(o.total_kg for o in delivered)

            return {
                "period_days": days,
                "delivered_orders": len(delivered),
                "on_time_count": on_time,
                "late_count": late,
                "on_time_rate_pct": on_time_pct,
                "avg_lead_time_days": avg_lead,
                "total_revenue_eur": round(total_revenue, 2),
                "total_kg_delivered": round(total_kg, 1),
            }
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# WHOLESALE HEALTH REPORT
# =============================================================================


def get_wholesale_health_report(days: int = DEFAULT_DAYS) -> dict:
    """Full wholesale intelligence report."""
    from maillard.mcp.operations.wholesale import get_pending_demand

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period_days": days,
        "demand_forecast": forecast_demand(days_history=days),
        "production_gap": analyze_production_gap(),
        "delivery_risk": score_delivery_risk(),
        "customer_rankings": rank_customers(days=days),
        "fulfillment_metrics": get_fulfillment_metrics(days=days),
        "pending_demand": get_pending_demand(),
    }
