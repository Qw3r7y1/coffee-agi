"""
Sales Intelligence for Maillard Coffee Roasters.

Combines sales, demand, margin, and inventory into daily revenue decisions.

STRICT: Refuses to produce analysis without verified live Square data.
Never fabricates products, margins, or summaries.
"""
from __future__ import annotations

from maillard.mcp.operations.cost_engine import calculate_product_costs
from maillard.mcp.operations.state_loader import get_operational_snapshot, get_state_meta


# ---------------------------------------------------------------------------
# Live data gate
# ---------------------------------------------------------------------------

_NO_DATA = {
    "status": "no_live_data",
    "message": "Sales data not connected. No analysis available.",
    "top_sellers": [],
    "best_margin_movers": [],
    "push": [],
    "deprioritize": [],
    "formatted": "Sales data not connected. No analysis available.",
    "warnings": [],
}


def _validate_live_data() -> dict | None:
    """Check that real Square data is loaded. Returns error dict or None if OK."""
    meta = get_state_meta()

    if not meta.get("exists"):
        return {**_NO_DATA, "reason": "current_state.json does not exist"}

    if not meta.get("has_live_sales"):
        return {**_NO_DATA, "reason": "No sales_today in state file"}

    if meta.get("raw_order_count", 0) == 0:
        return {**_NO_DATA, "reason": "raw_order_count is 0 — no real orders loaded"}

    source = meta.get("source", "")
    if source not in ("sqlite", "postgres", "live_square"):
        return {**_NO_DATA, "reason": f"Unknown data source: {source!r}"}

    if not meta.get("last_square_sync_at") and source != "live_square":
        return {**_NO_DATA, "reason": "No last_square_sync_at — sync may not have run"}

    return None


def _validate_costs(costs: dict) -> tuple[dict, list[str]]:
    """Filter out products with invalid margins. Returns (clean_costs, warnings)."""
    clean = {}
    warnings = []
    for product, data in costs.items():
        pct = data.get("margin_pct", 0)
        if pct > 95:
            warnings.append(f"{product}: margin {pct:.0f}% unrealistic (>95%), excluded")
            continue
        if pct < 0:
            warnings.append(f"{product}: margin {pct:.0f}% negative (<0%), excluded")
            continue
        clean[product] = data
    return clean, warnings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_sales_intelligence(
    sales: dict | None = None,
    demand: dict | None = None,
    costs: dict | None = None,
    inventory_risks: dict | None = None,
) -> dict:
    """Produce daily revenue decisions from live sales + demand + margin + inventory.

    Returns no_live_data response if Square data is not verified.
    """
    # Gate: refuse to run without live data
    gate_error = _validate_live_data()
    if gate_error:
        return gate_error

    # Load live data
    if any(x is None for x in [sales, demand, costs, inventory_risks]):
        snap = get_operational_snapshot()
        state = snap.get("state", {})
        sales = sales or state.get("sales_today", {})
        demand = demand or snap.get("demand_signals", {})
        inventory_risks = inventory_risks or snap.get("inventory_risks", {})
    costs = costs or calculate_product_costs()

    # Validate costs — remove unrealistic margins
    costs, cost_warnings = _validate_costs(costs)

    # Double-check: sales must have real entries
    if not sales or sum(sales.values()) == 0:
        return {**_NO_DATA, "reason": "sales_today loaded but empty or all zeros"}

    risk_items = set(inventory_risks.keys()) if inventory_risks else set()
    warnings = list(cost_warnings)

    # ── 1. Top sellers (by units today) ──
    top_sellers = []
    for product, qty in sorted(sales.items(), key=lambda x: -x[1])[:3]:
        cost_data = costs.get(product, {})
        margin = cost_data.get("margin_pct", 0)
        top_sellers.append({
            "product": product,
            "units": qty,
            "margin_pct": margin,
            "reason": f"{qty} sold, {margin:.0f}% margin" if margin else f"{qty} sold",
        })

    # ── 2. Best margin movers (strong margin + real demand) ──
    best_margin = []
    for product, data in costs.items():
        if data.get("grade") not in ("STRONG", "GOOD"):
            continue
        qty = sales.get(product, 0)
        if qty < 1:
            continue
        best_margin.append({
            "product": product,
            "units": qty,
            "margin": data["margin"],
            "margin_pct": data["margin_pct"],
        })
    best_margin.sort(key=lambda x: (-x["margin_pct"], -x["units"]))
    best_margin_movers = []
    for item in best_margin[:3]:
        best_margin_movers.append({
            **item,
            "reason": f"${item['margin']:.2f}/unit at {item['margin_pct']:.0f}%, {item['units']} sold",
        })

    # ── 3. Push (high margin + stock OK + demand stable/rising) ──
    push = []
    for product, data in costs.items():
        if data.get("grade") not in ("STRONG", "GOOD"):
            continue
        has_risk = any(product in r or r in product for r in risk_items)
        if has_risk:
            continue
        sig = demand.get(product, {})
        trend = sig.get("trend", "stable")
        if trend == "dropping":
            continue
        qty = sales.get(product, 0)
        reason = f"{data['margin_pct']:.0f}% margin, stock OK"
        if trend == "rising":
            reason += ", demand rising"
        if qty > 0:
            reason += f", {qty} sold today"
        push.append({"product": product, "margin_pct": data["margin_pct"], "reason": reason})
    push.sort(key=lambda x: -x["margin_pct"])
    push = push[:2]

    # ── 4. Deprioritize (low stock OR weak margin OR no movement) ──
    deprioritize = []
    for product, data in costs.items():
        reasons = []
        if data.get("grade") in ("CRITICAL", "LOW"):
            reasons.append(f"{data['margin_pct']:.0f}% margin")
        has_risk = any(product in r or r in product for r in risk_items)
        if has_risk:
            reasons.append("stock constrained")
        qty = sales.get(product, 0)
        if qty == 0 and data.get("price", 0) > 0:
            reasons.append("zero sales today")
        if reasons:
            deprioritize.append({"product": product, "reason": ", ".join(reasons)})
    deprioritize.sort(key=lambda x: len(x["reason"]), reverse=True)
    deprioritize = deprioritize[:2]

    formatted = _format(top_sellers, best_margin_movers, push, deprioritize)

    return {
        "status": "live",
        "top_sellers": top_sellers,
        "best_margin_movers": best_margin_movers,
        "push": push,
        "deprioritize": deprioritize,
        "formatted": formatted,
        "warnings": warnings,
    }


def _format(top: list, margin: list, push: list, depri: list) -> str:
    L = ["Sales Intelligence (live Square data)", "=" * 40, ""]

    L.append("Top Sellers:")
    for i, t in enumerate(top, 1):
        L.append(f"  {i}. {t['product'].replace('_',' ')} -- {t['reason']}")

    L.append("")
    L.append("Best Margin Movers:")
    if margin:
        for i, m in enumerate(margin, 1):
            L.append(f"  {i}. {m['product'].replace('_',' ')} -- {m['reason']}")
    else:
        L.append("  (no margin data matched today's sales)")

    if push:
        L.append("")
        L.append("Push:")
        for p in push:
            L.append(f"  + {p['product'].replace('_',' ')} -- {p['reason']}")

    if depri:
        L.append("")
        L.append("Deprioritize:")
        for d in depri:
            L.append(f"  - {d['product'].replace('_',' ')} -- {d['reason']}")

    L.append("")
    L.append("=" * 40)
    return "\n".join(L)
