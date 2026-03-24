"""
Product Execution Layer for Maillard Coffee Roasters.

Decides which products to push (high margin + available stock)
and which to avoid (low margin or stockout risk).
"""
from __future__ import annotations

from maillard.mcp.operations.cost_engine import calculate_product_costs
from maillard.mcp.operations.state_loader import get_operational_snapshot


def generate_product_actions(strategy: dict | None = None) -> dict:
    """Generate push/avoid product lists from cost + inventory data.

    Args:
        strategy: Optional override. If None, auto-generates from live data.

    Returns:
        {"push": [max 2], "avoid": [max 2]}
    """
    if strategy and "push" in strategy:
        return {
            "push": strategy["push"][:2],
            "avoid": strategy.get("avoid", [])[:2],
        }

    costs = calculate_product_costs()
    snap = get_operational_snapshot()
    inv_risks = snap.get("inventory_risks", {})
    demand = snap.get("demand_signals", {})

    # Items at risk (stockout/critical inventory)
    risk_ingredients = set()
    for sku, r in inv_risks.items():
        risk_ingredients.add(sku.lower())

    # Score each drink: margin + demand trend + stock availability
    scored: list[tuple[str, float, dict]] = []
    for product, data in costs.items():
        if data["price"] == 0:
            continue

        score = data["margin_pct"]

        # Boost if demand is rising
        sig = demand.get(product, {})
        if sig.get("trend") == "rising":
            score += 10

        # Penalize if key ingredient is at risk
        # Milk drinks penalized if milk is out
        if any(risk in product for risk in ["bag", "1kg"]):
            score -= 5  # retail bags tie up roasted stock
        if "milk" in " ".join(risk_ingredients):
            if data["cost"] > 0.6:  # milk-heavy drinks
                score -= 30

        scored.append((product, score, data))

    scored.sort(key=lambda x: -x[1])

    # Push: top 2 that are profitable AND stock is available
    push = []
    for product, score, data in scored:
        if score > 50 and len(push) < 2:
            push.append(f"{product.replace('_', ' ').title()} -- EUR {data['margin']:.2f} margin ({data['margin_pct']:.0f}%)")

    # Avoid: bottom 2 that are low margin OR stock-constrained
    avoid = []
    for product, score, data in reversed(scored):
        if len(avoid) >= 2:
            break
        if score < 50 or data["margin_pct"] < 50:
            avoid.append(f"{product.replace('_', ' ').title()} -- {data['margin_pct']:.0f}% margin")

    return {"push": push[:2], "avoid": avoid[:2]}
