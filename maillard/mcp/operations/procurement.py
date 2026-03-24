"""
Procurement Engine for Maillard Coffee Roasters.

Simple purchasing decision logic:
  1. Calculate daily average usage from 7-day sales + recipes
  2. Calculate days of stock remaining
  3. Generate purchase recommendations with supplier info

No APIs. No automation. Just decision logic from JSON files.
"""
from __future__ import annotations

from loguru import logger

from maillard.mcp.operations.state_loader import (
    load_current_state,
    load_recipes,
    get_operational_snapshot,
    calculate_inventory_usage,
    detect_low_inventory,
)


# =============================================================================
# DAILY AVERAGE USAGE
# =============================================================================


def calculate_daily_avg(sales_last_7_days: dict, recipes: dict) -> dict:
    """Convert 7-day product sales into average daily ingredient usage.

    Returns:
        {ingredient: avg_daily_amount} e.g. {"espresso_beans_kg": 2.3, "whole_milk_liters": 12.1}
    """
    avg_daily_sales = {product: round(qty / 7, 2) for product, qty in sales_last_7_days.items()}
    daily_usage = calculate_inventory_usage(avg_daily_sales, recipes)
    return daily_usage


# =============================================================================
# DAYS OF STOCK
# =============================================================================


def calculate_days_of_stock(updated_inventory: dict, avg_usage: dict, ingredient_map: dict | None = None) -> dict:
    """Calculate days remaining for each inventory item.

    Args:
        updated_inventory: {sku: {stock, unit, ...}} after today's consumption
        avg_usage: {ingredient: daily_amount} from calculate_daily_avg
        ingredient_map: optional override for ingredient->SKU mapping

    Returns:
        {sku: {"days_left": float, "stock": float, "daily_usage": float, "unit": str}}
    """
    # Build reverse map: which ingredient draws from which SKU
    sku_usage: dict[str, float] = {}

    # Map ingredient names to SKUs by category/name matching
    for ingredient, daily_amount in avg_usage.items():
        matched = False
        for sku, info in updated_inventory.items():
            lower_sku = sku.lower()
            lower_ing = ingredient.lower().replace("_kg", "").replace("_liters", "")

            # Match by name overlap
            if (lower_ing in lower_sku or lower_sku in lower_ing
                    or _category_match(ingredient, info.get("category", ""))):
                sku_usage[sku] = sku_usage.get(sku, 0) + daily_amount
                matched = True
                break

        # Fallback: try category match for generic ingredients
        if not matched:
            cat = _ingredient_category(ingredient)
            for sku, info in updated_inventory.items():
                if info.get("category") == cat:
                    sku_usage[sku] = sku_usage.get(sku, 0) + daily_amount
                    break

    result = {}
    for sku, info in updated_inventory.items():
        stock = info.get("stock", 0)
        daily = sku_usage.get(sku, 0)

        if daily > 0:
            days = round(stock / daily, 1)
        elif stock > 0:
            days = None  # no usage = infinite
        else:
            days = 0  # no stock, no usage

        result[sku] = {
            "days_left": days,
            "stock": stock,
            "daily_usage": round(daily, 3),
            "unit": info.get("unit", ""),
            "min_stock": info.get("min_stock", 0),
            "category": info.get("category", ""),
        }

    return result


def _category_match(ingredient: str, category: str) -> bool:
    ing = ingredient.lower()
    if "milk" in ing and category == "milk":
        return True
    if ("espresso" in ing or "beans" in ing or "coffee" in ing) and category == "roasted_coffee":
        return True
    return False


def _ingredient_category(ingredient: str) -> str:
    ing = ingredient.lower()
    if "milk" in ing:
        return "milk"
    if "espresso" in ing or "beans" in ing or "coffee" in ing:
        return "roasted_coffee"
    return ""


# =============================================================================
# PURCHASE RECOMMENDATIONS
# =============================================================================


def generate_purchase_recommendations(
    updated_inventory: dict,
    avg_usage: dict,
    suppliers: dict,
) -> list[dict]:
    """Generate purchase recommendations based on stock, usage, and supplier lead times.

    Rules:
      reorder_point = lead_time_days + 2 (safety buffer)
      IF days_left <= reorder_point -> recommend purchase
      purchase_qty = avg_usage * (lead_time_days + 5), respecting min_order_qty

    Returns:
        [{item, status, urgency, days_left, recommended_qty, supplier, lead_time, reason}]
    """
    stock_days = calculate_days_of_stock(updated_inventory, avg_usage)
    recommendations = []

    for sku, info in stock_days.items():
        days_left = info["days_left"]
        daily = info["daily_usage"]
        stock = info["stock"]
        unit = info["unit"]
        category = info["category"]

        if daily <= 0:
            continue  # no consumption = no need to order

        # Find matching supplier
        supplier_info = _find_supplier(sku, category, suppliers)
        lead_time = supplier_info.get("lead_time_days", 7)
        min_order = supplier_info.get("min_order_qty", 0)
        supplier_name = supplier_info.get("supplier", "Unknown")

        reorder_point = lead_time + 2

        if days_left is not None and days_left <= reorder_point:
            # Calculate order quantity
            cover_days = lead_time + 5  # cover lead time + 5 day buffer
            raw_qty = round(daily * cover_days, 1)
            qty = max(raw_qty, min_order)  # respect minimum order

            # Urgency
            if days_left <= 0:
                urgency = "CRITICAL"
                status = "BUY NOW"
            elif days_left <= lead_time:
                urgency = "HIGH"
                status = "BUY"
            else:
                urgency = "MEDIUM"
                status = "BUY"

            # Reason
            reason_parts = []
            if days_left <= 1:
                reason_parts.append("Stockout imminent")
            elif days_left <= lead_time:
                reason_parts.append(f"Only {days_left}d left, lead time is {lead_time}d")
            else:
                reason_parts.append(f"{days_left}d left, reorder point reached")

            if daily > info.get("min_stock", 0) * 0.1:
                reason_parts.append(f"consuming {daily:.1f} {unit}/day")

            recommendations.append({
                "item": sku,
                "status": status,
                "urgency": urgency,
                "days_left": days_left,
                "stock": stock,
                "daily_usage": daily,
                "recommended_qty": qty,
                "unit": unit,
                "supplier": supplier_name,
                "lead_time_days": lead_time,
                "min_order_qty": min_order,
                "estimated_cost": round(qty * updated_inventory.get(sku, {}).get("cost_per_unit", 0), 2),
                "reason": ". ".join(reason_parts),
            })

    # Sort: CRITICAL first, then HIGH, then MEDIUM
    urgency_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
    recommendations.sort(key=lambda r: (urgency_order.get(r["urgency"], 9), r.get("days_left", 99)))

    return recommendations


def _find_supplier(sku: str, category: str, suppliers: dict) -> dict:
    """Find the best matching supplier for an inventory item."""
    lower = sku.lower()

    # Direct match by ingredient name in suppliers
    for key, info in suppliers.items():
        if key.lower() in lower or lower in key.lower():
            return info

    # Category match
    if category == "milk":
        if "oat" in lower:
            return suppliers.get("oat_milk", {})
        return suppliers.get("whole_milk", {})
    if category in ("roasted_coffee", "green_coffee"):
        return suppliers.get("espresso_beans", suppliers.get("coffee", {}))

    return {}


# =============================================================================
# FULL PROCUREMENT REPORT
# =============================================================================


def get_procurement_report() -> dict:
    """Generate the complete procurement report from current state.

    Loads everything from JSON files, calculates usage, and returns recommendations.
    """
    snap = get_operational_snapshot()
    state = snap.get("state", {})
    recipes = snap.get("recipes", {})
    updated_inv = snap.get("updated_inventory", {})
    suppliers = state.get("suppliers", {})
    sales_7d = state.get("sales_last_7_days", {})

    avg_usage = calculate_daily_avg(sales_7d, recipes)
    stock_days = calculate_days_of_stock(updated_inv, avg_usage)
    recommendations = generate_purchase_recommendations(updated_inv, avg_usage, suppliers)
    risks = detect_low_inventory(updated_inv)

    return {
        "avg_daily_usage": avg_usage,
        "stock_days": stock_days,
        "recommendations": recommendations,
        "inventory_risks": risks,
        "total_items_to_order": len(recommendations),
        "total_estimated_cost": round(sum(r["estimated_cost"] for r in recommendations), 2),
    }
