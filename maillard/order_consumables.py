"""
Order Consumables — automatic cost additions for multi-item orders.

Business rule: when an order has 2+ items, add bag/carrier costs.

Configurable rules in data/order_consumables.json.
"""
from __future__ import annotations

import json
from pathlib import Path

from maillard.mcp.operations.cost_engine import get_ingredient_cost

_DATA = Path(__file__).resolve().parent.parent / "data"
_CONFIG_FILE = _DATA / "order_consumables.json"

# Default rules if config file doesn't exist
_DEFAULT_RULES = {
    "rules": [
        {
            "name": "bag",
            "description": "Paper/plastic bag for multi-item orders",
            "condition": "item_count >= 2",
            "ingredient_key": "kraft_pastry_dry_wax_bag_6_5x8_2_000",
            "quantity": 1,
            "unit": "ea",
            "enabled": True,
        },
        {
            "name": "cup_carrier",
            "description": "Cup carrier for orders with 3+ drinks",
            "condition": "drink_count >= 3",
            "ingredient_key": "2_coffee_cup_carriers_w_handle_200",
            "quantity": 1,
            "unit": "ea",
            "enabled": True,
        },
    ]
}

# Drink categories — items that count toward drink_count
_DRINK_SLUGS = {
    "espresso", "double_espresso", "americano", "latte", "cappuccino",
    "flat_white", "mocha", "macchiato", "cortado", "freddo_espresso",
    "freddo_cappuccino", "cold_brew", "iced_latte", "filter_coffee",
    "pour_over", "matcha", "frape", "hot_chocolate", "iced_chocolate",
    "chai", "japanese_style_iced_coffee", "oj",
}


def _load_rules() -> list[dict]:
    if _CONFIG_FILE.exists():
        try:
            data = json.loads(_CONFIG_FILE.read_text(encoding="utf-8"))
            return data.get("rules", [])
        except Exception:
            pass
    # Create default config
    _CONFIG_FILE.write_text(json.dumps(_DEFAULT_RULES, indent=2), encoding="utf-8")
    return _DEFAULT_RULES["rules"]


def save_rules(rules: list[dict]) -> None:
    _CONFIG_FILE.write_text(json.dumps({"rules": rules}, indent=2), encoding="utf-8")


def get_rules() -> list[dict]:
    return _load_rules()


def calculate_order_consumables(order_items: list[dict]) -> list[dict]:
    """Calculate additional consumable costs for an order.

    Args:
        order_items: list of items in the order, each with at least:
            {"product_slug": str, "quantity": int, "product_category": str}

    Returns:
        list of consumable line items to add:
            [{"name": str, "ingredient_key": str, "quantity": int,
              "unit_cost": float, "line_cost": float, "reason": str}]
    """
    rules = _load_rules()

    item_count = sum(i.get("quantity", 1) for i in order_items)
    drink_count = sum(
        i.get("quantity", 1) for i in order_items
        if i.get("product_slug", "") in _DRINK_SLUGS
        or i.get("product_category", "") == "drink"
    )

    consumables = []
    for rule in rules:
        if not rule.get("enabled", True):
            continue

        condition = rule.get("condition", "")
        triggered = False

        if condition == "item_count >= 2" and item_count >= 2:
            triggered = True
        elif condition == "item_count >= 3" and item_count >= 3:
            triggered = True
        elif condition == "drink_count >= 2" and drink_count >= 2:
            triggered = True
        elif condition == "drink_count >= 3" and drink_count >= 3:
            triggered = True
        elif condition == "drink_count >= 4" and drink_count >= 4:
            triggered = True
        elif condition == "always":
            triggered = True

        if triggered:
            ing_key = rule.get("ingredient_key", "")
            qty = rule.get("quantity", 1)
            cost_info = get_ingredient_cost(ing_key)
            unit_cost = cost_info.get("unit_cost", 0)

            consumables.append({
                "name": rule.get("name", ing_key),
                "description": rule.get("description", ""),
                "ingredient_key": ing_key,
                "quantity": qty,
                "unit": rule.get("unit", "ea"),
                "unit_cost": unit_cost,
                "line_cost": round(qty * unit_cost, 5),
                "cost_source": cost_info.get("source", "none"),
                "reason": f"{rule['name']}: {condition}",
            })

    return consumables


def calculate_daily_consumables_cost(business_date: str) -> dict:
    """Calculate total consumable costs for all multi-item orders on a given date.

    Returns:
        {"date": str, "orders_with_consumables": int, "total_cost": float,
         "breakdown": [{"name", "count", "unit_cost", "total_cost"}]}
    """
    import sqlite3
    db_path = _DATA / "sales.db"
    if not db_path.exists():
        return {"date": business_date, "orders_with_consumables": 0, "total_cost": 0, "breakdown": []}

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Get all orders with their items for this date
    orders = conn.execute("""
        SELECT order_id, product_slug, product_category, SUM(quantity) as qty
        FROM order_items
        WHERE business_date = ?
        GROUP BY order_id, product_slug, product_category
    """, (business_date,)).fetchall()
    conn.close()

    # Group by order
    orders_map: dict[str, list] = {}
    for r in orders:
        orders_map.setdefault(r["order_id"], []).append(dict(r))

    total_cost = 0
    orders_with_consumables = 0
    consumable_counts: dict[str, dict] = {}

    for order_id, items in orders_map.items():
        consumables = calculate_order_consumables(items)
        if consumables:
            orders_with_consumables += 1
            for c in consumables:
                total_cost += c["line_cost"]
                name = c["name"]
                if name not in consumable_counts:
                    consumable_counts[name] = {"count": 0, "unit_cost": c["unit_cost"], "total_cost": 0}
                consumable_counts[name]["count"] += c["quantity"]
                consumable_counts[name]["total_cost"] += c["line_cost"]

    breakdown = [
        {"name": k, "count": v["count"], "unit_cost": round(v["unit_cost"], 5),
         "total_cost": round(v["total_cost"], 4)}
        for k, v in consumable_counts.items()
    ]

    return {
        "date": business_date,
        "orders_with_consumables": orders_with_consumables,
        "total_orders": len(orders_map),
        "total_cost": round(total_cost, 4),
        "breakdown": breakdown,
    }
