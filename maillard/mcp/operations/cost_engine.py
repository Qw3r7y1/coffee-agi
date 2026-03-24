"""
Cost Intelligence Engine for Maillard Coffee Roasters.

Ingredient cost only. No overhead, no labor.
Calculates cost, margin, and margin % for every product.
"""
from __future__ import annotations

import json
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent.parent.parent / "data"


def _load(name: str) -> dict:
    p = _DATA / name
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def calculate_product_costs(
    recipes: dict | None = None,
    costs: dict | None = None,
    prices: dict | None = None,
) -> dict:
    """Calculate ingredient cost, price, margin, and margin % per product.

    Args:
        recipes: {product: {ingredient: amount}} from recipes.json
        costs: {ingredient: cost_per_unit} from costs.json
        prices: {product: sell_price} from prices.json

    Returns:
        {product: {"cost": X, "price": Y, "margin": Z, "margin_pct": %}}
    """
    recipes = recipes or _load("recipes.json")
    costs = costs or _load("costs.json")
    prices = prices or _load("prices.json")

    result = {}
    for product, ingredients in recipes.items():
        total_cost = 0
        for ingredient, amount in ingredients.items():
            unit_cost = costs.get(ingredient, 0)
            total_cost += amount * unit_cost

        total_cost = round(total_cost, 2)
        price = round(prices.get(product, 0), 2)

        # Skip products with no price (not on menu)
        if price <= 0:
            continue

        margin = round(price - total_cost, 2)
        margin_pct = round(margin / price * 100, 1)

        # Sanity: skip unrealistic margins
        if margin_pct > 95 or margin_pct < 0:
            continue

        if margin_pct < 50:
            grade = "CRITICAL"
            action = "RAISE PRICE or REDUCE COST"
        elif margin_pct < 65:
            grade = "LOW"
            action = "REVIEW PRICING"
        elif margin_pct < 75:
            grade = "GOOD"
            action = "OK"
        else:
            grade = "STRONG"
            action = "OK"

        result[product] = {
            "cost": total_cost,
            "price": price,
            "margin": margin,
            "margin_pct": margin_pct,
            "grade": grade,
            "action": action,
        }

    return result
