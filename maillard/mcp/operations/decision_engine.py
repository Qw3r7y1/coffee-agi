"""
Operations Decision Engine for Maillard Coffee Roasters.

Full operations intelligence: connects inventory -> production -> sales.
Answers: "What do I produce, what do I order, and what do I risk losing today?"

Priority tiers:
  CRITICAL — must be done immediately (business stops if ignored)
  HIGH     — same day (significant revenue/service impact)
  MEDIUM   — within 2-3 days (degrades to HIGH if delayed)

Every action links to its demand source:
  retail (bar service), wholesale (customer order), or blend (component).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger

from maillard.mcp.operations import inventory_intelligence, production_intelligence, wholesale_intelligence
from maillard.mcp.operations import inventory, production, wholesale
from maillard.mcp.operations.state_loader import get_operational_snapshot, detect_low_inventory
from maillard.mcp.operations.execution import (
    persist_plan_actions, get_completed_action_ids, get_failed_action_ids,
    get_action_stats, _make_action_id,
)
from maillard.models.database import SessionLocal
from maillard.models.operations import InventoryItem


# ── Maillard constants ───────────────────────────────────────────────────────
ROAST_LOSS_PCT = 18.0
SHOTS_PER_KG = 45
GREEN_TO_ROASTED = 0.82

WHOLESALE_ALLOC = 0.6
RETAIL_ALLOC = 0.4

# Time estimates (minutes)
TIME_ROAST_PER_BATCH = 90
TIME_ROAST_SETUP = 15
TIME_ORDER_CALL = 5
TIME_PACK_PER_KG = 3
TIME_WASTE_REVIEW = 15

# Time slots
SLOT_MORNING = "morning"
SLOT_MIDDAY = "mid-day"
SLOT_EVENING = "end-of-day"

# Revenue per unit (EUR)
AVG_ESPRESSO_REV = 3.50       # per espresso-based drink
AVG_MILK_DRINK_REV = 4.50     # per latte/capp/flat white
WHOLESALE_REV_PER_KG = 20.00  # per kg roasted wholesale
DRINKS_PER_LITER_MILK = 4     # ~250ml per milk drink
DRINKS_PER_KG_ROASTED = SHOTS_PER_KG  # 45 doubles per kg

# Cost per unit (EUR) — used when item cost_per_unit is 0
COST_DEFAULTS = {
    "green_coffee": 7.00,    # per kg
    "roasted_coffee": 0,     # derived from green + roast cost
    "milk": 1.50,            # per liter
    "consumables": 0.12,     # per cup
    "packaging": 0.30,       # per bag/box
}

# Roasting cost (EUR per kg green input — energy, labor, depreciation)
ROAST_COST_PER_KG = 2.50

# Stock-days thresholds for production triggering
STOCK_CRITICAL_DAYS = 1
STOCK_HIGH_DAYS = 2
STOCK_MEDIUM_DAYS = 5


# =============================================================================
# DAILY EXECUTION PLAN
# =============================================================================


def generate_daily_operations_plan() -> dict:
    now = datetime.now(timezone.utc)

    # ── Load real-world state from JSON ─────────────────────────────────
    real_state = get_operational_snapshot()

    # ── Fetch external signals ───────────────────────────────────────────
    market_signal = _get_market_signal()
    demand_signals = _get_demand_signals()

    # Merge real-world demand signals (from JSON) into demand_signals
    for product, sig in real_state.get("demand_signals", {}).items():
        if sig.get("trend") != "stable":
            demand_signals.setdefault("products", {}).setdefault(product, sig)

    # ── Build the full demand picture ────────────────────────────────────
    stock_status = _build_stock_status(demand_signals)

    raw_actions: list[dict] = []
    raw_actions.extend(_inventory_orders(stock_status))
    raw_actions.extend(_green_coffee_purchasing(stock_status, market_signal))
    raw_actions.extend(_production_decisions(stock_status))
    raw_actions.extend(_wholesale_decisions())
    raw_actions.extend(_waste_actions())

    actions = _consolidate(raw_actions)

    for a in actions:
        _assign_schedule(a)

    # ── Assign IDs and filter by execution state ───────────────────────
    plan_date = now.strftime("%Y-%m-%d")
    done_ids = get_completed_action_ids(plan_date)      # success + skipped
    failed_ids = get_failed_action_ids(plan_date)       # need re-attention

    for a in actions:
        a["action_id"] = _make_action_id(a, plan_date)
        if a["action_id"] in done_ids:
            a["status"] = "completed_success"
        elif a["action_id"] in failed_ids:
            a["status"] = "completed_failed"
            # Elevate failed actions to at least HIGH
            if a["priority"] not in ("CRITICAL",):
                a["priority"] = "HIGH"
            a["reason"] = f"[RETRY] Previous attempt failed. {a.get('reason', '')}"
        else:
            a["status"] = "pending"

    # Separate done-success from active (failed stays active)
    completed_actions = [a for a in actions if a["status"] == "completed_success"]
    actions = [a for a in actions if a["status"] != "completed_success"]

    # ── Score and rank active actions ────────────────────────────────────
    for a in actions:
        score_result = score_action_priority(a)
        a["priority_score"] = score_result["priority_score"]
        new_band = score_result["priority_band"]
        band_rank = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        if band_rank.get(new_band, 9) < band_rank.get(a["priority"], 9):
            a["priority"] = new_band

    risk_alerts = _risk_alerts(stock_status)

    # ── Sort: priority band first, then score descending ─────────────────
    band_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    actions.sort(key=lambda a: (
        band_order.get(a["priority"], 9),
        -a.get("priority_score", 0),
    ))

    for i, a in enumerate(actions, 1):
        a["step"] = i

    # ── Persist actions for tracking ─────────────────────────────────────
    persist_plan_actions(actions, plan_date)

    # Financial summary
    total_cost = sum(a.get("financials", {}).get("action_cost_eur", 0) for a in actions)
    total_rev_protected = sum(a.get("financials", {}).get("revenue_protected_eur", 0) for a in actions)
    total_loss_if_ignored = sum(a.get("financials", {}).get("loss_if_ignored_eur", 0) for a in actions)

    # Top 3 by score
    top3 = sorted(actions, key=lambda a: -a.get("priority_score", 0))[:3]
    top3_summary = [
        {"step": a.get("step"), "action": a["action"], "score": a.get("priority_score", 0),
         "revenue_protected": a.get("financials", {}).get("revenue_protected_eur", 0),
         "loss_if_ignored": a.get("financials", {}).get("loss_if_ignored_eur", 0)}
        for a in top3
    ]

    plan = {
        "generated_at": now.isoformat(),
        "plan_date": now.strftime("%Y-%m-%d"),
        "market_signal": market_signal,
        "demand_signals": demand_signals,
        "real_state": {
            "usage_today": real_state.get("usage_today", {}),
            "updated_inventory": real_state.get("updated_inventory", {}),
            "inventory_risks": real_state.get("inventory_risks", {}),
            "stock_days": real_state.get("stock_days", {}),
        },
        "summary": {
            "total_actions": len(actions),
            "critical": sum(1 for a in actions if a["priority"] == "CRITICAL"),
            "high_priority": sum(1 for a in actions if a["priority"] == "HIGH"),
            "medium_priority": sum(1 for a in actions if a["priority"] == "MEDIUM"),
            "total_time_min": sum(a.get("time_minutes", 0) for a in actions),
            "risk_alerts": len(risk_alerts),
            "financials": {
                "total_action_cost_eur": round(total_cost, 2),
                "total_revenue_protected_eur": round(total_rev_protected, 0),
                "total_loss_if_ignored_eur": round(total_loss_if_ignored, 0),
                "net_value_eur": round(total_rev_protected - total_cost, 0),
            },
            "top_3": top3_summary,
            "execution": get_action_stats(plan_date),
        },
        "actions": actions,
        "completed_actions": completed_actions,
        "risk_alerts": risk_alerts,
        "stock_status": stock_status,
        "morning": [a for a in actions if a.get("when") == SLOT_MORNING],
        "midday": [a for a in actions if a.get("when") == SLOT_MIDDAY],
        "evening": [a for a in actions if a.get("when") == SLOT_EVENING],
        "next_24h": [a for a in actions if a.get("deadline_hours", 999) <= 24],
        "next_3_days": [a for a in actions if a.get("deadline_hours", 999) <= 72],
        "next_7_days": actions,
        "formatted_plan": _format_plan(actions, completed_actions, risk_alerts, stock_status, now),
    }

    logger.info(
        f"[DECISION-ENGINE] plan: {len(actions)} steps "
        f"({plan['summary']['critical']}C/{plan['summary']['high_priority']}H/"
        f"{plan['summary']['medium_priority']}M), {len(risk_alerts)} risks"
    )
    return plan


# =============================================================================
# PRIORITY SCORING
# =============================================================================


def score_action_priority(action: dict) -> dict:
    """Compute a numeric priority score (0-100) combining urgency and financial impact.

    Scoring components (max 100):
      Urgency base     (0-35):  CRITICAL=35, HIGH=25, MEDIUM=10, LOW=0
      Loss if ignored  (0-25):  scaled by magnitude (EUR 0-500+)
      Revenue protected(0-15):  scaled by magnitude
      ROI efficiency   (0-10):  high-protection / low-cost actions rise
      Time pressure    (0-10):  deadline_hours inverse
      Quick win bonus  (0-5):   fast actions (<15 min) get a bump

    Returns:
        {"priority_score": int, "priority_band": str, "scoring_breakdown": dict}
    """
    fin = action.get("financials", {})
    priority = action.get("priority", "MEDIUM")

    # ── 1. Urgency base (0-35) ───────────────────────────────────────────
    urgency_base = {"CRITICAL": 35, "HIGH": 25, "MEDIUM": 10, "LOW": 0}.get(priority, 0)

    # ── 2. Loss if ignored (0-25) ────────────────────────────────────────
    loss = fin.get("loss_if_ignored_eur", 0)
    # Scale: EUR 0=0, EUR 50=5, EUR 200=15, EUR 500+=25
    if loss >= 500:
        loss_score = 25
    elif loss > 0:
        loss_score = round(min(25, loss / 500 * 25))
    else:
        loss_score = 0

    # ── 3. Revenue protected (0-15) ──────────────────────────────────────
    rev = fin.get("revenue_protected_eur", 0)
    if rev >= 500:
        rev_score = 15
    elif rev > 0:
        rev_score = round(min(15, rev / 500 * 15))
    else:
        rev_score = 0

    # ── 4. ROI efficiency (0-10) ─────────────────────────────────────────
    # low-cost / high-protection = high ROI score
    roi = fin.get("roi")
    if roi is not None and roi > 0:
        roi_score = round(min(10, roi * 2.5))  # ROI 4x = max 10
    elif fin.get("action_cost_eur", 0) == 0 and rev > 0:
        roi_score = 10  # free action with revenue protection = perfect ROI
    else:
        roi_score = 0

    # ── 5. Time pressure (0-10) ──────────────────────────────────────────
    deadline_h = action.get("deadline_hours", 168)
    if deadline_h <= 4:
        time_score = 10
    elif deadline_h <= 12:
        time_score = 8
    elif deadline_h <= 24:
        time_score = 5
    elif deadline_h <= 48:
        time_score = 2
    else:
        time_score = 0

    # ── 6. Quick win bonus (0-5) ─────────────────────────────────────────
    mins = action.get("time_minutes", 60)
    if mins <= 5:
        quick_score = 5
    elif mins <= 15:
        quick_score = 3
    elif mins <= 30:
        quick_score = 1
    else:
        quick_score = 0

    total = urgency_base + loss_score + rev_score + roi_score + time_score + quick_score
    total = min(100, total)

    # Determine band from score (can upgrade, never downgrade in caller)
    if total >= 70:
        band = "CRITICAL"
    elif total >= 45:
        band = "HIGH"
    elif total >= 20:
        band = "MEDIUM"
    else:
        band = "LOW"

    return {
        "priority_score": total,
        "priority_band": band,
        "scoring_breakdown": {
            "urgency_base": urgency_base,
            "loss_if_ignored": loss_score,
            "revenue_protected": rev_score,
            "roi_efficiency": roi_score,
            "time_pressure": time_score,
            "quick_win": quick_score,
        },
    }


# =============================================================================
# DEMAND SIGNALS (from sales normalization layer)
# =============================================================================

# Maps sales products to inventory SKU patterns for demand linkage.
# "latte" demand drives milk + roasted coffee consumption.
# "ethiopia_yirgacheffe" (bag) demand drives roasted coffee + packaging.
_DEMAND_TO_INVENTORY = {
    # Drink products -> what they consume from inventory
    "latte":             {"milk": 0.25, "roasted_coffee": 0.022},  # 250ml milk, 22g coffee per drink
    "cappuccino":        {"milk": 0.20, "roasted_coffee": 0.022},
    "flat_white":        {"milk": 0.18, "roasted_coffee": 0.022},
    "mocha":             {"milk": 0.22, "roasted_coffee": 0.022},
    "iced_latte":        {"milk": 0.25, "roasted_coffee": 0.022},
    "freddo_cappuccino": {"milk": 0.15, "roasted_coffee": 0.022},
    "espresso":          {"roasted_coffee": 0.022},
    "double_espresso":   {"roasted_coffee": 0.022},
    "americano":         {"roasted_coffee": 0.022},
    "freddo_espresso":   {"roasted_coffee": 0.022},
    "cortado":           {"milk": 0.06, "roasted_coffee": 0.022},
    "cold_brew":         {"roasted_coffee": 0.060},  # 60g per serving (batch brew)
    "filter_coffee":     {"roasted_coffee": 0.015},
    "pour_over":         {"roasted_coffee": 0.018},
}

# Threshold for "rising" demand: today vs average
_DEMAND_RISE_PCT = 20   # 20% above average = rising
_DEMAND_DROP_PCT = -20   # 20% below average = dropping


def _get_demand_signals() -> dict:
    """Fetch sales demand signals from the normalization layer.

    Returns:
        {
            "products": {
                "latte": {"daily_avg": 15, "today": 20, "trend": "rising", "change_pct": 33},
                ...
            },
            "inventory_impact": {
                "milk": {"extra_daily_liters": 1.2, "trend": "rising", "driven_by": ["latte", "cappuccino"]},
                "roasted_coffee": {"extra_daily_kg": 0.5, "trend": "rising", "driven_by": [...]},
            },
            "available": bool,
        }
    """
    try:
        from maillard.mcp.sales.normalization import get_demand_summary
        from maillard.mcp.operations.inventory import get_usage_history

        # Get recent usage as a proxy for sales (usage logs ARE the sales record in this system)
        usage = get_usage_history(usage_type="bar_service", limit=200)
        if not usage:
            return {"products": {}, "inventory_impact": {}, "available": False}

        # Convert usage logs into sales-like records for the demand aggregator
        sales_proxy = []
        for u in usage:
            sales_proxy.append({
                "timestamp": u.get("logged_at", ""),
                "product": u.get("item_name", "unknown"),
                "quantity": u.get("quantity_used", 0),
                "channel": "pos",
                "revenue": 0,
                "category": "drink",
                "product_display": u.get("item_name", ""),
            })

        # If we have actual normalized sales data, use that instead
        # For now, derive signals from usage patterns in inventory_intelligence
        rates = inventory_intelligence.get_daily_usage_rate()

        product_signals = {}
        inv_impact: dict[str, dict] = {}

        for r in rates:
            sku = r["sku"]
            name = r["name"]
            daily = r["daily_rate"]
            active_rate = r["active_day_rate"]

            # Trend: compare daily_rate (avg over full period) vs active_day_rate (avg on active days)
            # If active_day_rate > daily_rate * 1.2 -> demand is rising (recent days are busier)
            if daily > 0 and active_rate > 0:
                change = round((active_rate - daily) / daily * 100, 0)
                if change >= _DEMAND_RISE_PCT:
                    trend = "rising"
                elif change <= _DEMAND_DROP_PCT:
                    trend = "dropping"
                else:
                    trend = "stable"
            else:
                trend = "stable"
                change = 0

            product_signals[sku] = {
                "name": name,
                "daily_avg": daily,
                "active_day_rate": active_rate,
                "trend": trend,
                "change_pct": change,
            }

            # Map product demand to inventory impact
            cat = r.get("category", "")
            if cat == "milk":
                inv_impact.setdefault("milk", {"extra_daily": 0, "trend": "stable", "driven_by": []})
                if trend == "rising":
                    inv_impact["milk"]["extra_daily"] += round((active_rate - daily), 3)
                    inv_impact["milk"]["driven_by"].append(name)
                    inv_impact["milk"]["trend"] = "rising"
            elif cat == "roasted_coffee":
                inv_impact.setdefault("roasted_coffee", {"extra_daily": 0, "trend": "stable", "driven_by": []})
                if trend == "rising":
                    inv_impact["roasted_coffee"]["extra_daily"] += round((active_rate - daily), 3)
                    inv_impact["roasted_coffee"]["driven_by"].append(name)
                    inv_impact["roasted_coffee"]["trend"] = "rising"

        return {"products": product_signals, "inventory_impact": inv_impact, "available": True}

    except Exception as e:
        logger.warning(f"[DECISION-ENGINE] demand signals unavailable: {e}")
        return {"products": {}, "inventory_impact": {}, "available": False}


# =============================================================================
# STOCK STATUS — unified view with demand signals
# =============================================================================


def _build_stock_status(demand_signals: dict | None = None) -> list[dict]:
    """Build a unified stock picture: current level, daily rate, days left, demand trend."""
    predictions = inventory_intelligence.predict_stockout()

    ws_demand = wholesale_intelligence.analyze_production_gap()
    ws_items = {g["sku"]: g for g in ws_demand.get("items", [])}

    rates = inventory_intelligence.get_daily_usage_rate()
    rate_map = {r["sku"]: r for r in rates}

    ds = demand_signals or {}
    product_signals = ds.get("products", {})
    inv_impact = ds.get("inventory_impact", {})

    result = []
    for p in predictions:
        sku = p["sku"]
        r = rate_map.get(sku, {})
        ws = ws_items.get(sku, {})

        # Demand sources
        demand_sources = []
        usage_by_type = r.get("usage_by_type", {})
        if usage_by_type.get("bar_service", 0) > 0:
            demand_sources.append(f"retail: {usage_by_type['bar_service']:.1f} {p['unit']}/period")
        if usage_by_type.get("roasting", 0) > 0:
            demand_sources.append(f"roasting: {usage_by_type['roasting']:.1f} {p['unit']}/period")
        if usage_by_type.get("wholesale_order", 0) > 0:
            demand_sources.append(f"wholesale: {usage_by_type['wholesale_order']:.1f} {p['unit']}/period")
        if ws.get("demand_kg", 0) > 0:
            demand_sources.append(f"pending WS orders: {ws['demand_kg']}kg")

        # Demand signal for this item
        sig = product_signals.get(sku, {})
        demand_trend = sig.get("trend", "stable")
        demand_change = sig.get("change_pct", 0)

        # Check if this item's category has rising demand from other products
        cat = p.get("category", "")
        cat_impact = inv_impact.get(cat, {})
        if cat_impact.get("trend") == "rising" and demand_trend == "stable":
            demand_trend = "rising_indirect"
            driven_by = cat_impact.get("driven_by", [])
            if driven_by:
                demand_sources.append(f"demand rising from: {', '.join(driven_by[:3])}")

        # Adjust urgency based on demand trend
        urgency = p["urgency"]
        if demand_trend in ("rising", "rising_indirect"):
            # Rising demand + already urgent -> escalate
            if urgency == "URGENT":
                urgency = "CRITICAL"
            elif urgency == "SOON":
                urgency = "URGENT"

        result.append({
            **p,
            "urgency": urgency,  # possibly upgraded
            "demand_sources": demand_sources,
            "demand_trend": demand_trend,
            "demand_change_pct": demand_change,
            "ws_pending_kg": ws.get("demand_kg", 0),
            "ws_coverage": ws.get("status", "n/a"),
        })

    return result


# =============================================================================
# INVENTORY ORDERS (perishables + consumables)
# =============================================================================


def _inventory_orders(stock_status: list[dict]) -> list[dict]:
    actions = []
    # Load costs in one query
    cost_map = _get_cost_map()

    for s in stock_status:
        if s["category"] in ("green_coffee",):
            continue
        if s["daily_rate"] <= 0:
            continue

        sku, name, cat = s["sku"], s["name"], s["category"]
        days_left = s["days_remaining"]
        daily = s["daily_rate"]
        lead = s["lead_time_days"]
        unit = s["unit"]
        sources = ", ".join(s["demand_sources"]) or "general use"
        cost_per = cost_map.get(sku, COST_DEFAULTS.get(cat, 0))
        demand_trend = s.get("demand_trend", "stable")
        trend_note = f" Demand {demand_trend}." if demand_trend != "stable" else ""

        if s["urgency"] == "CRITICAL":
            qty = round(daily * (lead + 7), 1)
            fin = _financials_order(cat, qty, cost_per, daily, lead)
            actions.append(_action(
                cat="inventory", sub="order", priority="CRITICAL",
                action=f"Order {qty} {unit} {name} -- OUT OF STOCK",
                sku=sku, quantity=qty, unit=unit,
                deadline_hours=2, demand=sources,
                reason=f"Stock: {s['current_stock']} {unit}. Burns {daily}/{unit}/day. Lead: {lead}d.{trend_note}",
                impact=f"Protects EUR {fin['revenue_protected_eur']:.0f} revenue. Cost: EUR {fin['action_cost_eur']:.2f}. {_consequence(cat)}",
                if_ignored=f"EUR {fin['loss_if_ignored_eur']:.0f} lost. {_consequence(cat)}",
                financials=fin,
            ))
        elif s["urgency"] == "URGENT":
            qty = round(daily * (lead + 14), 1)
            fin = _financials_order(cat, qty, cost_per, daily, lead)
            actions.append(_action(
                cat="inventory", sub="order", priority="HIGH",
                action=f"Order {qty} {unit} {name}",
                sku=sku, quantity=qty, unit=unit,
                deadline_hours=24, demand=sources,
                reason=f"{days_left:.0f} days left at {daily}/day. Lead: {lead}d.{trend_note}",
                impact=f"Protects EUR {fin['revenue_protected_eur']:.0f}. Cost: EUR {fin['action_cost_eur']:.2f}.",
                if_ignored=f"EUR {fin['loss_if_ignored_eur']:.0f} at risk in {days_left:.0f}d. {_consequence(cat)}",
                financials=fin,
            ))
        elif s["urgency"] == "SOON":
            qty = round(daily * (lead + 14), 1)
            fin = _financials_order(cat, qty, cost_per, daily, lead)
            actions.append(_action(
                cat="inventory", sub="order", priority="MEDIUM",
                action=f"Plan reorder: {qty} {unit} {name}",
                sku=sku, quantity=qty, unit=unit,
                deadline_hours=min(72, s["days_until_critical"] * 24), demand=sources,
                reason=f"{days_left:.0f} days left. Order within {s['days_until_critical']:.0f}d.",
                impact=f"Cost: EUR {fin['action_cost_eur']:.2f}. Prevents escalation.",
                if_ignored=f"Escalates to HIGH in {s['days_until_critical']:.0f} days.",
                financials=fin,
            ))
    return actions


# =============================================================================
# GREEN COFFEE PURCHASING
# =============================================================================


def _get_market_signal() -> dict:
    """Fetch the analyst buying signal synchronously for use in purchasing decisions.

    Returns {"recommendation": "BUY NOW" | "WAIT" | "MONITOR", "direction": ..., "reason": ...}
    or a fallback if the analyst is unavailable.
    """
    try:
        import asyncio
        from maillard.mcp.analyst.buying_signal import get_buying_signal
        # Run the async function synchronously (we're already in a sync context)
        loop = asyncio.new_event_loop()
        signal = loop.run_until_complete(get_buying_signal(days=14))
        loop.close()
        return {
            "recommendation": signal.get("recommendation", "MONITOR"),
            "direction": signal.get("direction", "UNKNOWN"),
            "confidence": signal.get("confidence", "LOW"),
            "reason": signal.get("reason", ""),
            "change_pct": signal.get("change_pct", 0),
        }
    except Exception as e:
        logger.warning(f"[DECISION-ENGINE] Could not fetch market signal: {e}")
        return {"recommendation": "MONITOR", "direction": "UNKNOWN", "confidence": "LOW", "reason": "Market data unavailable"}


def _green_coffee_purchasing(stock_status: list[dict], market_signal: dict | None = None) -> list[dict]:
    """Generate green coffee supplier orders, aligned with market timing.

    Rules:
      BUY NOW  + LOW stock   -> upgrade to CRITICAL (buy immediately, prices rising)
      BUY NOW  + MEDIUM stock -> upgrade to HIGH (buy today, don't wait)
      WAIT     + MEDIUM stock -> downgrade to LOW (delay, prices falling)
      WAIT     + LOW stock    -> keep priority (can't wait, stock too low)
      MONITOR                 -> no change
    """
    actions = []
    cost_map = _get_cost_map()
    signal = market_signal or {"recommendation": "MONITOR"}
    rec = signal.get("recommendation", "MONITOR")
    direction = signal.get("direction", "UNKNOWN")
    market_note = f" Market: {direction} ({rec})"

    for s in stock_status:
        if s["category"] != "green_coffee" or s["daily_rate"] <= 0:
            continue

        sku, name = s["sku"], s["name"]
        days_left = s["days_remaining"]
        daily = s["daily_rate"]
        lead = s["lead_time_days"]
        unit = s["unit"]
        cost_per = cost_map.get(sku, COST_DEFAULTS["green_coffee"])

        roasted_from_green = daily * GREEN_TO_ROASTED
        downstream = f"Produces ~{roasted_from_green:.1f}kg roasted/day"

        if s["urgency"] in ("CRITICAL", "URGENT"):
            qty = round(daily * (lead + 14), 1)
            base_priority = "CRITICAL" if s["urgency"] == "CRITICAL" else "HIGH"

            # BUY NOW + already urgent -> ensure CRITICAL
            if rec == "BUY NOW" and base_priority == "HIGH":
                base_priority = "CRITICAL"
                market_note_action = "Upgraded to CRITICAL: market rising, buy before prices increase further."
            else:
                market_note_action = ""

            # WAIT + urgent -> can't downgrade, stock too low
            # (business survival > market timing)

            fin = _financials_order("green_coffee", qty, cost_per, daily, lead)
            actions.append(_action(
                cat="inventory", sub="green_order", priority=base_priority,
                action=f"Order {qty}kg {name} from supplier",
                sku=sku, quantity=qty, unit=unit,
                deadline_hours=4 if base_priority == "CRITICAL" else 24,
                demand=f"roasting input. {downstream}.{market_note}",
                reason=f"{days_left:.0f} days green stock. Lead: {lead}d.{' ' + market_note_action if market_note_action else ''}",
                impact=f"Cost: EUR {fin['action_cost_eur']:.0f}. Protects EUR {fin['revenue_protected_eur']:.0f} downstream.",
                if_ignored=f"EUR {fin['loss_if_ignored_eur']:.0f} downstream revenue lost. Roasting halted.",
                financials=fin,
                market_signal=rec,
            ))

        elif s["urgency"] == "SOON":
            qty = round(daily * (lead + 21), 1)
            base_priority = "MEDIUM"
            deadline = min(72, s["days_until_critical"] * 24)

            # BUY NOW + SOON -> upgrade to HIGH (buy today, prices rising)
            if rec == "BUY NOW":
                base_priority = "HIGH"
                deadline = 24
                market_reason = f"Market rising ({direction}). Buy now to lock in current price."
            # WAIT + SOON -> downgrade (delay, prices falling)
            elif rec == "WAIT":
                base_priority = "LOW"
                deadline = min(168, s["days_until_critical"] * 24)  # up to 7 days
                market_reason = f"Market falling ({direction}). Safe to delay purchase."
            else:
                market_reason = ""

            fin = _financials_order("green_coffee", qty, cost_per, daily, lead)
            actions.append(_action(
                cat="inventory", sub="green_order", priority=base_priority,
                action=f"{'Order' if rec == 'BUY NOW' else 'Plan order:'} {qty}kg {name}",
                sku=sku, quantity=qty, unit=unit,
                deadline_hours=deadline,
                demand=f"roasting input. {downstream}.{market_note}",
                reason=f"{days_left:.0f} days left. Lead: {lead}d.{' ' + market_reason if market_reason else ''}",
                impact=f"Cost: EUR {fin['action_cost_eur']:.0f}. Prevents pipeline disruption.",
                if_ignored=f"Becomes urgent. EUR {fin['loss_if_ignored_eur']:.0f} at risk.",
                financials=fin,
                market_signal=rec,
            ))
    return actions


# =============================================================================
# PRODUCTION — inventory-aware, demand-linked
# =============================================================================


def _production_decisions(stock_status: list[dict]) -> list[dict]:
    """Generate roast session actions. Priority based on days-of-stock of roasted product."""
    actions = []
    plan = production_intelligence.recommend_next_batches()

    # Build roasted stock-days map
    roasted_days: dict[str, float] = {}
    for s in stock_status:
        if s["category"] == "roasted_coffee":
            roasted_days[s["sku"]] = s["days_remaining"]

    for sess in plan.get("sessions", []):
        green_name = sess["green_coffee_name"]
        green_sku = sess["green_coffee_sku"]
        n_batches = sess["batch_count"]
        total_green = sess["total_green_kg"]
        total_output = sess["total_output_kg"]
        est_time = sess["estimated_time_min"]

        # Determine priority from roasted stock days (not just planner priority)
        min_days = float("inf")
        for b in sess["batches"]:
            d = roasted_days.get(b["roasted_sku"], float("inf"))
            min_days = min(min_days, d)

        if min_days <= STOCK_CRITICAL_DAYS:
            priority = "CRITICAL"
            deadline = 4
        elif min_days <= STOCK_HIGH_DAYS:
            priority = "HIGH"
            deadline = 12
        elif sess["priority"] == "HIGH":
            priority = "HIGH"
            deadline = 24
        else:
            priority = "MEDIUM"
            deadline = 48

        ws_demand = sum(b.get("wholesale_demand_kg", 0) for b in sess["batches"])
        ws_alloc = round(total_output * WHOLESALE_ALLOC, 1)
        rt_alloc = round(total_output * RETAIL_ALLOC, 1)

        # Financials
        cost_map = _get_cost_map()
        green_cost_per = cost_map.get(green_sku, COST_DEFAULTS["green_coffee"])
        fin = _financials_roast(total_green, total_output, ws_demand, green_cost_per)

        # Demand source linkage per batch
        batch_details = []
        for b in sess["batches"]:
            b_ws = round(b["expected_output_kg"] * WHOLESALE_ALLOC, 1)
            b_rt = round(b["expected_output_kg"] * RETAIL_ALLOC, 1)
            days = roasted_days.get(b["roasted_sku"], None)
            demand_src = []
            if b.get("wholesale_demand_kg", 0) > 0:
                demand_src.append(f"wholesale: {b['wholesale_demand_kg']}kg")
            if days is not None and days < STOCK_MEDIUM_DAYS:
                demand_src.append(f"retail: {days:.0f} days stock left")

            batch_details.append({
                "batch_sequence": b["batch_sequence"],
                "green_kg": b["batch_kg"],
                "expected_output_kg": b["expected_output_kg"],
                "efficiency_pct": b["efficiency_pct"],
                "roasted_sku": b["roasted_sku"],
                "roasted_name": b["roasted_name"],
                "wholesale_kg": b_ws,
                "retail_kg": b_rt,
                "demand_sources": demand_src,
                "roasted_days_left": days,
            })

        time_str = f"{est_time // 60}h {est_time % 60}m" if est_time >= 60 else f"{est_time}m"
        demand_str = []
        if ws_demand > 0:
            demand_str.append(f"wholesale: {ws_demand}kg (EUR {fin['revenue_breakdown']['wholesale']:.0f})")
        if min_days < STOCK_MEDIUM_DAYS:
            demand_str.append(f"retail bar: roasted stock at {min_days:.0f} days")

        actions.append(_action(
            cat="production", sub="roast_session", priority=priority,
            action=f"Roast {total_green}kg {green_name} ({n_batches} batch{'es' if n_batches > 1 else ''})",
            deadline_hours=deadline,
            demand="; ".join(demand_str) if demand_str else "retail replenishment",
            reason=(
                f"{n_batches} batch{'es' if n_batches > 1 else ''}, "
                f"{total_green}kg -> ~{total_output}kg ({time_str})"
            ),
            impact=(
                f"Protects EUR {fin['revenue_protected_eur']:.0f} revenue. "
                f"Cost: EUR {fin['action_cost_eur']:.0f} (green: {fin['cost_breakdown']['green_coffee']:.0f} + roast: {fin['cost_breakdown']['roasting']:.0f}). "
                f"Alloc: {ws_alloc}kg WS / {rt_alloc}kg retail."
            ),
            if_ignored=(
                f"EUR {fin['loss_if_ignored_eur']:.0f} lost (WS: {fin['revenue_breakdown']['wholesale']:.0f} + retail: {fin['revenue_breakdown']['retail']:.0f})."
            ),
            financials=fin,
            green_sku=green_sku,
            total_green_kg=total_green,
            total_output_kg=total_output,
            batch_count=n_batches,
            batch_details=batch_details,
            allocation={"wholesale_kg": ws_alloc, "retail_kg": rt_alloc},
            time_minutes_override=est_time,
            dependency=f"Requires {total_green}kg {green_name} in green stock",
        ))

    # Loss anomalies
    loss = production_intelligence.analyze_roast_losses()
    for a in loss.get("anomalies", []):
        actions.append(_action(
            cat="production", sub="review", priority="LOW",
            action=f"Review batch {a['batch_code']}: {a['issue']}",
            deadline_hours=72,
            demand="quality control",
            reason=f"Loss {a['loss_pct']}% outside 14-22% tolerance",
            impact=a["detail"],
            if_ignored="Recurring quality issues. Check temp calibration, bean moisture.",
        ))

    return actions


# =============================================================================
# WHOLESALE DECISIONS
# =============================================================================


def _wholesale_decisions() -> list[dict]:
    actions = []
    risks = wholesale_intelligence.score_delivery_risk()
    for order in risks:
        if order["risk_level"] in ("CRITICAL", "HIGH"):
            rev = order["total_eur"]
            p = "CRITICAL" if order["risk_level"] == "CRITICAL" else "HIGH"
            fin = _financials_wholesale(rev)
            actions.append(_action(
                cat="wholesale", sub="fulfill", priority=p,
                action=f"Prepare order {order['order_number']} for {order['customer']}",
                order_number=order["order_number"],
                total_kg=order["total_kg"],
                deadline_hours=12 if p == "CRITICAL" else 24,
                demand=f"wholesale: {order['customer']} ({order['total_kg']}kg, EUR {rev})",
                reason=", ".join(order["risk_factors"]),
                impact=f"Protects EUR {fin['revenue_protected_eur']:.0f}. Pack/ship cost: EUR {fin['action_cost_eur']:.0f}.",
                if_ignored=f"EUR {fin['loss_if_ignored_eur']:.0f} lost + customer relationship damage.",
                financials=fin,
                dependency=f"Requires {order['total_kg']}kg roasted coffee packed",
            ))
    return actions


# =============================================================================
# WASTE
# =============================================================================


def _waste_actions() -> list[dict]:
    actions = []
    waste = inventory_intelligence.detect_waste_anomalies()
    for a in waste.get("anomalies", []):
        actions.append(_action(
            cat="inventory", sub="waste", priority="MEDIUM",
            action=f"Investigate {a['name']} waste ({a['waste_pct']}%)",
            sku=a["sku"], deadline_hours=48,
            demand="waste reduction",
            reason=f"Waste {a['waste_pct']}% vs {a['threshold_pct']}% threshold. EUR {a['waste_cost_eur']} lost.",
            impact=f"EUR {a['waste_cost_eur']} already lost. ~EUR {a['waste_cost_eur'] * 2:.0f}/month if unchecked.",
            if_ignored=f"~EUR {a['waste_cost_eur']:.0f}/month ongoing. Check FIFO, storage, expiry.",
        ))
    return actions


# =============================================================================
# RISK ALERTS
# =============================================================================


def _risk_alerts(stock_status: list[dict]) -> list[dict]:
    alerts = []

    for s in stock_status:
        if s["days_remaining"] <= STOCK_HIGH_DAYS and s["urgency"] in ("CRITICAL", "URGENT") and s["daily_rate"] > 0:
            sources = ", ".join(s["demand_sources"]) if s["demand_sources"] else s["category"]
            alerts.append({
                "type": "stockout",
                "severity": "CRITICAL" if s["days_remaining"] <= STOCK_CRITICAL_DAYS else "HIGH",
                "item": s["name"],
                "category": s["category"],
                "stock": f"{s['current_stock']} {s['unit']}",
                "days_left": round(s["days_remaining"], 1),
                "demand": sources,
                "consequence": _consequence(s["category"]),
            })

    for s in stock_status:
        if s["days_remaining"] > 60 and s["category"] not in ("green_coffee", "equipment") and s["daily_rate"] > 0:
            alerts.append({
                "type": "overstock",
                "severity": "LOW",
                "item": s["name"],
                "days_left": round(s["days_remaining"]),
                "consequence": "Capital tied up. Expiry risk for perishables.",
            })

    gap = wholesale_intelligence.analyze_production_gap()
    if gap.get("coverage_pct", 100) < 80:
        alerts.append({
            "type": "fulfillment_gap",
            "severity": "HIGH",
            "item": "Wholesale pipeline",
            "detail": f"Coverage {gap['coverage_pct']}%. Deficit: {gap['total_deficit_kg']}kg.",
            "consequence": "Orders delayed. Customer churn risk.",
        })

    alerts.sort(key=lambda a: {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}.get(a["severity"], 9))
    return alerts


# =============================================================================
# CONSOLIDATION
# =============================================================================


def _consolidate(actions: list[dict]) -> list[dict]:
    sessions = [a for a in actions if a.get("sub") == "roast_session"]
    orders = [a for a in actions if a.get("sub") in ("order", "green_order")]
    rest = [a for a in actions if a.get("sub") not in ("roast_session", "order", "green_order", "production_gap")]

    # Batch urgent orders
    crit_orders = [o for o in orders if o["priority"] in ("CRITICAL", "HIGH")]
    other_orders = [o for o in orders if o["priority"] not in ("CRITICAL", "HIGH")]

    if len(crit_orders) > 1:
        items = ", ".join(f"{o.get('quantity',0)} {o.get('unit','')} {o.get('sku','')}" for o in crit_orders)
        # Aggregate financials from sub-orders
        agg_cost = sum(o.get("financials", {}).get("action_cost_eur", 0) for o in crit_orders)
        agg_rev = sum(o.get("financials", {}).get("revenue_protected_eur", 0) for o in crit_orders)
        agg_loss = sum(o.get("financials", {}).get("loss_if_ignored_eur", 0) for o in crit_orders)
        agg_roi = round(agg_rev / agg_cost, 1) if agg_cost > 0 else None
        merged_fin = {
            "action_cost_eur": round(agg_cost, 2),
            "revenue_protected_eur": round(agg_rev, 0),
            "loss_if_ignored_eur": round(agg_loss, 0),
            "margin_impact": "positive" if agg_rev > agg_cost else "neutral",
            "roi": agg_roi,
        }
        merged = _action(
            cat="inventory", sub="order_batch",
            priority="CRITICAL" if any(o["priority"] == "CRITICAL" for o in crit_orders) else "HIGH",
            action=f"Place urgent orders ({len(crit_orders)} items)",
            deadline_hours=min(o.get("deadline_hours", 24) for o in crit_orders),
            demand="multiple supply lines critical",
            reason=items,
            impact=f"Cost: EUR {agg_cost:.0f}. Protects EUR {agg_rev:.0f}. {len(crit_orders)} items at stockout risk.",
            if_ignored=f"EUR {agg_loss:.0f} lost. Multiple stockouts. Service disruption.",
            financials=merged_fin,
            sub_orders=crit_orders,
        )
        rest.append(merged)
        rest.extend(other_orders)
    else:
        rest.extend(orders)

    return sessions + [a for a in rest if a.get("sub") != "production_gap"]


# =============================================================================
# SCHEDULING
# =============================================================================


def _assign_schedule(action: dict) -> None:
    sub = action.get("sub", "")

    if sub in ("roast", "roast_session"):
        action["when"] = SLOT_MORNING
        action["time_minutes"] = action.get("time_minutes_override") or (
            TIME_ROAST_SETUP + TIME_ROAST_PER_BATCH * action.get("batch_count", 1)
        )
        return

    if sub in ("order", "order_batch", "green_order"):
        action["when"] = SLOT_MORNING
        n = len(action.get("sub_orders", [action]))
        action["time_minutes"] = TIME_ORDER_CALL * n
        return

    if sub == "fulfill":
        action["when"] = SLOT_MIDDAY
        action["time_minutes"] = round(TIME_PACK_PER_KG * action.get("total_kg", 5))
        return

    if sub in ("waste", "review"):
        action["when"] = SLOT_EVENING
        action["time_minutes"] = TIME_WASTE_REVIEW
        return

    action["when"] = SLOT_MIDDAY
    action["time_minutes"] = 10


# =============================================================================
# HELPERS
# =============================================================================


def _action(*, cat, sub, priority, action, deadline_hours=24,
            demand="", reason="", impact="", if_ignored="", **extra) -> dict:
    d = {
        "category": cat, "sub": sub, "priority": priority,
        "action": action, "deadline_hours": deadline_hours,
        "demand_source": demand, "reason": reason,
        "impact_detail": impact, "if_ignored": if_ignored,
    }
    d.update(extra)

    # Auto-generate validation rules based on action type + SKU
    if "validation" not in d:
        d["validation"] = _auto_validation(d)

    return d


def _auto_validation(action: dict) -> dict | None:
    """Generate validation rules based on action type.

    Returns:
        {"metric": str, "success_condition": str, "sku": str,
         "expected_outcome": str} or None
    """
    sub = action.get("sub", "")
    sku = action.get("sku")

    # Inventory orders: stock should be above min after delivery
    if sub in ("order", "order_batch", "green_order") and sku:
        return {
            "metric": "stock_above_min",
            "success_condition": "above_min",
            "sku": sku,
            "expected_outcome": f"{sku} stock above reorder threshold after delivery",
        }

    # Roast sessions: roasted SKU should have stock > 0 after roasting
    if sub == "roast_session":
        # Use first batch's roasted SKU
        batches = action.get("batch_details", [])
        if batches:
            rsku = batches[0].get("roasted_sku")
            if rsku:
                return {
                    "metric": "roasted_available",
                    "success_condition": "gt_zero",
                    "sku": rsku,
                    "expected_outcome": f"{rsku} stock > 0kg after roasting",
                }

    # Wholesale fulfillment: order should have all lines fulfilled
    if sub == "fulfill":
        return {
            "metric": "stock_qty",
            "success_condition": "0",  # just check stock isn't negative
            "sku": action.get("sku") or action.get("order_number"),
            "expected_outcome": "Wholesale order packed and shipped",
        }

    return None


def _get_cost_map() -> dict[str, float]:
    """Load cost_per_unit for all items in one query."""
    try:
        with SessionLocal() as session:
            items = session.query(InventoryItem.sku, InventoryItem.cost_per_unit).all()
            return {sku: cost for sku, cost in items if cost}
    except Exception:
        return {}


def _estimate_lost_revenue(cat: str, daily_rate: float, days: int) -> float:
    if cat == "milk":
        return round(daily_rate * days * 4 * AVG_MILK_DRINK_REV * 0.6, 0)
    if cat == "roasted_coffee":
        return round(daily_rate * days * SHOTS_PER_KG * AVG_ESPRESSO_REV * 0.3, 0)
    if cat == "consumables":
        return round(daily_rate * days * AVG_ESPRESSO_REV * 0.1, 0)
    return 0


def _consequence(cat: str) -> str:
    return {
        "milk": "Cannot make lattes, cappuccinos, flat whites. ~60% of drink menu gone.",
        "roasted_coffee": "Cannot serve espresso drinks. Bar halted.",
        "green_coffee": "Cannot roast. Roasted stockout follows within days.",
        "consumables": "Cannot serve takeaway. Revenue limited to dine-in.",
        "packaging": "Cannot pack wholesale orders. Delivery delays.",
    }.get(cat, "Item unavailable.")


def _financials_order(cat: str, quantity: float, cost_per_unit: float, daily_rate: float, lead_days: int) -> dict:
    """Calculate financial impact for an inventory order action."""
    cost = round(quantity * cost_per_unit, 2) if cost_per_unit else round(quantity * COST_DEFAULTS.get(cat, 0), 2)

    # Revenue protected = what we'd lose during a stockout of lead_days
    if cat == "milk":
        rev_protected = round(daily_rate * lead_days * DRINKS_PER_LITER_MILK * AVG_MILK_DRINK_REV * 0.6, 0)
    elif cat == "roasted_coffee":
        rev_protected = round(daily_rate * lead_days * DRINKS_PER_KG_ROASTED * AVG_ESPRESSO_REV * 0.3, 0)
    elif cat == "consumables":
        rev_protected = round(daily_rate * lead_days * AVG_ESPRESSO_REV * 0.5, 0)
    elif cat == "green_coffee":
        roasted_per_day = daily_rate * GREEN_TO_ROASTED
        rev_protected = round(roasted_per_day * lead_days * WHOLESALE_REV_PER_KG, 0)
    else:
        rev_protected = 0

    loss_if_ignored = rev_protected  # same: it's what you lose if you don't order
    margin = "positive" if rev_protected > cost else ("neutral" if rev_protected == cost else "negative")

    return {
        "action_cost_eur": cost,
        "revenue_protected_eur": rev_protected,
        "loss_if_ignored_eur": loss_if_ignored,
        "margin_impact": margin,
        "roi": round(rev_protected / cost, 1) if cost > 0 else None,
    }


def _financials_roast(total_green_kg: float, total_output_kg: float, ws_demand_kg: float, green_cost_per_kg: float) -> dict:
    """Calculate financial impact for a roasting session."""
    # Cost = green coffee cost + roasting cost
    green_cost = round(total_green_kg * (green_cost_per_kg or COST_DEFAULTS["green_coffee"]), 2)
    roast_cost = round(total_green_kg * ROAST_COST_PER_KG, 2)
    total_cost = round(green_cost + roast_cost, 2)

    # Revenue protected
    ws_rev = round(ws_demand_kg * WHOLESALE_REV_PER_KG, 0)
    retail_output = total_output_kg - ws_demand_kg
    retail_rev = round(max(0, retail_output) * DRINKS_PER_KG_ROASTED * AVG_ESPRESSO_REV * 0.3, 0)
    rev_protected = ws_rev + retail_rev

    return {
        "action_cost_eur": total_cost,
        "cost_breakdown": {"green_coffee": green_cost, "roasting": roast_cost},
        "revenue_protected_eur": rev_protected,
        "revenue_breakdown": {"wholesale": ws_rev, "retail": retail_rev},
        "loss_if_ignored_eur": rev_protected,
        "margin_impact": "positive" if rev_protected > total_cost else "neutral",
        "roi": round(rev_protected / total_cost, 1) if total_cost > 0 else None,
    }


def _financials_wholesale(total_eur: float) -> dict:
    """Calculate financial impact for a wholesale fulfillment action."""
    pack_cost = round(total_eur * 0.02, 2)  # ~2% of order value for packing/shipping
    return {
        "action_cost_eur": pack_cost,
        "revenue_protected_eur": round(total_eur, 0),
        "loss_if_ignored_eur": round(total_eur, 0),
        "margin_impact": "positive",
        "roi": round(total_eur / pack_cost, 1) if pack_cost > 0 else None,
    }


# =============================================================================
# FORECAST VIEWS
# =============================================================================


def generate_morning_brief() -> dict:
    """Execution-grade brief. Max 3 actions. VERB + ITEM + QTY + DEADLINE.

    Priority: Risk > Money > Efficiency.
    Merges related orders. Drops noise. Validates every action.
    """
    plan = generate_daily_operations_plan()
    now = datetime.now(timezone.utc)
    actions = plan.get("actions", [])
    summary = plan.get("summary", {})
    fin = summary.get("financials", {})
    ms = plan.get("market_signal", {})
    ds = plan.get("demand_signals", {})
    ms_dir = ms.get("direction", "?").upper()

    # ── Yesterday's feedback: auto-adjust for past failures ──────────────
    try:
        from maillard.mcp.operations.feedback import analyze_execution_feedback
        feedback = analyze_execution_feedback()
    except Exception:
        feedback = {"missed_actions": [], "partial_execution": [], "unexpected_issues": [], "adjustments": []}

    # ── Sales intelligence: push/deprioritize ───────────────────────────
    try:
        from maillard.mcp.sales.intelligence import generate_sales_intelligence
        sales_intel = generate_sales_intelligence()
    except Exception:
        sales_intel = {"push": [], "deprioritize": [], "top_sellers": [], "best_margin_movers": []}

    push_products = {p["product"] for p in sales_intel.get("push", [])}
    depri_products = {d["product"] for d in sales_intel.get("deprioritize", [])}

    try:
        from maillard.mcp.operations.procurement import get_procurement_report
        proc_recs = get_procurement_report().get("recommendations", [])
    except Exception:
        proc_recs = []

    rs = plan.get("real_state", {})
    inv_risks = rs.get("inventory_risks", {})

    # ── Step 1: Build raw decisions ──────────────────────────────────────
    raw: list[dict] = []
    seen: set[str] = set()

    # Tier 0: stockouts / critical from real state + procurement
    for sku, risk in inv_risks.items():
        if risk["status"] in ("STOCKOUT", "CRITICAL") and sku not in seen:
            proc = next((r for r in proc_recs if r["item"] == sku), None)
            qty = proc["recommended_qty"] if proc else "?"
            unit = (proc or risk).get("unit", risk.get("unit", ""))
            supplier = proc["supplier"] if proc else "supplier"
            lead = proc["lead_time_days"] if proc else "?"
            raw.append({"verb": "Order", "item": sku.replace("_", " "), "qty": f"{qty} {unit}",
                        "deadline": "before 10 AM" if risk["status"] == "STOCKOUT" else "before noon",
                        "supplier": supplier, "why": f"{risk['stock']} left. Lead: {lead}d.", "tier": 0, "cat": "order"})
            seen.add(sku)

    for r in proc_recs:
        if r["item"] not in seen:
            dl = "before 10 AM" if r["urgency"] == "CRITICAL" else "before noon" if r["urgency"] == "HIGH" else "today"
            raw.append({"verb": "Order", "item": r["item"].replace("_", " "), "qty": f"{r['recommended_qty']} {r['unit']}",
                        "deadline": dl, "supplier": r["supplier"],
                        "why": f"{r['days_left']}d stock. Lead: {r['lead_time_days']}d.", "tier": 0 if r["urgency"] == "CRITICAL" else 1, "cat": "order"})
            seen.add(r["item"])

    # Tier 1: production
    for a in actions:
        if a.get("sub") == "roast_session":
            total = a.get("total_green_kg", "?")
            output = a.get("total_output_kg", "?")
            rev = a.get("financials", {}).get("revenue_protected_eur", 0)
            if rev > 0 or any(b.get("wholesale_demand_kg", 0) > 0 for b in a.get("batch_details", [])):
                raw.append({"verb": "Roast", "item": "coffee", "qty": f"{total}kg green -> {output}kg roasted",
                            "deadline": "before opening", "supplier": None,
                            "why": f"EUR {rev:.0f} revenue.", "tier": 1, "cat": "production"})
            break

    # ── Step 2: Merge related orders into single action ──────────────────
    orders = [r for r in raw if r["cat"] == "order"]
    non_orders = [r for r in raw if r["cat"] != "order"]

    if len(orders) > 1:
        # Merge: "Order X and Y before Z"
        # Use the tightest deadline
        deadlines = {"before 10 AM": 0, "before noon": 1, "today": 2, "this week": 3}
        orders.sort(key=lambda o: deadlines.get(o["deadline"], 9))
        tightest = orders[0]["deadline"]
        parts = [f"{o['qty']} {o['item']}" for o in orders]
        suppliers = list({o["supplier"] for o in orders if o["supplier"]})
        merged = {
            "verb": "Order",
            "item": " + ".join(parts),
            "qty": "",
            "deadline": tightest,
            "supplier": ", ".join(suppliers) if suppliers else None,
            "why": "; ".join(o["why"] for o in orders),
            "tier": min(o["tier"] for o in orders),
            "cat": "order",
        }
        final_raw = [merged] + non_orders
    else:
        final_raw = orders + non_orders

    final_raw.sort(key=lambda d: d["tier"])

    # ── Step 3: Format as VERB + ITEM + QTY + DEADLINE ───────────────────
    decisions = []
    for r in final_raw[:3]:
        # Build the action string
        parts = [r["verb"]]
        if r["qty"]:
            parts.append(r["qty"])
        if r["item"] and not r["qty"]:
            parts.append(r["item"])
        if r.get("supplier") and r["cat"] == "order" and len(final_raw) <= 2:
            parts.append(f"from {r['supplier']}")
        parts.append(r["deadline"])
        action_str = " ".join(parts)
        decisions.append({"action": action_str, "why": r["why"]})

    # ── Step 4: Drivers (only if they changed a decision) ────────────────
    drivers: list[str] = []
    for k, v in ds.get("products", {}).items():
        if v.get("trend") != "rising":
            continue
        name = v.get("name", k)
        is_relevant = any(name.lower().replace(" ", "_") in d.get("action", "").lower() or k.lower() in d.get("action", "").lower() for d in decisions)
        if is_relevant:
            drivers.append(f"{name} demand up")
    if ms.get("recommendation") == "BUY NOW":
        drivers.append("coffee prices rising")
    elif ms.get("recommendation") == "WAIT":
        drivers.append("prices falling -- delay non-critical orders")
    drivers = drivers[:2]

    # ── Step 4b: Sales intelligence filter ───────────────────────────────
    # Push items with rising demand get added to drivers
    focus_items: list[str] = []
    for p in sales_intel.get("push", []):
        name = p["product"].replace("_", " ")
        sig = ds.get("products", {}).get(p["product"], {})
        if sig.get("trend") == "rising":
            focus_items.append(f"{name} (rising + high margin)")
        else:
            focus_items.append(f"{name} ({p.get('reason', 'high margin')})")
    focus_items = focus_items[:2]

    # Deprioritized items: suppress from actions (don't promote them)
    if depri_products:
        decisions = [d for d in decisions if not any(
            dp.replace("_", " ") in d.get("action", "").lower() for dp in depri_products
        )]

    # ── Step 5: Build text ───────────────────────────────────────────────
    L: list[str] = []
    L.append("Maillard Morning Brief")
    L.append(now.strftime("%A, %B %d %Y"))
    L.append("=" * 40)
    L.append("")
    L.append(f"Market: {ms_dir}")

    # Yesterday's failures (if any) — prevents repeating mistakes
    yesterday_fixes = []
    for p in feedback.get("partial_execution", []):
        yesterday_fixes.append(f"Yesterday under-ordered: {p['gap']}. Corrected in today's quantities.")
    for m in feedback.get("missed_actions", []):
        yesterday_fixes.append(f"Not done yesterday: {m[:60]}")
    if yesterday_fixes:
        L.append("")
        L.append("Yesterday:")
        for fix in yesterday_fixes[:2]:
            L.append(f"  - {fix}")

    if drivers:
        L.append("")
        L.append("Drivers:")
        for drv in drivers:
            L.append(f"  - {drv}")

    if focus_items:
        L.append("")
        L.append("Focus:")
        for f in focus_items:
            L.append(f"  + {f}")

    L.append("")
    L.append("Actions:")
    if decisions:
        for i, d in enumerate(decisions, 1):
            L.append(f"  {i}. {d['action']}")
    else:
        L.append("  1. No action required today")

    # Notes: only if critical financial risk
    loss = fin.get("total_loss_if_ignored_eur", 0)
    if loss > 200:
        L.append("")
        L.append(f"EUR {loss:.0f} at risk if skipped.")

    L.append("")
    L.append("=" * 40)

    return {
        "generated_at": now.isoformat(),
        "brief": "\n".join(L),
        "decisions": decisions,
        "market": ms_dir,
        "key_drivers": drivers,
        "focus": focus_items,
        "deprioritized": list(depri_products),
        "yesterday_feedback": feedback,
        "sales_intelligence": sales_intel,
        "financials": fin,
        "full_plan": plan,
    }


def _build_item_forecast(horizon_days: int) -> list[dict]:
    """Project each item's status over the forecast horizon.

    For each active inventory item with usage, computes:
      - hours until stockout
      - stock level at each day boundary
      - whether a reorder must be placed and when
      - pending production that will replenish stock
    """
    predictions = inventory_intelligence.predict_stockout()
    batch_plan = production_intelligence.recommend_next_batches()

    # Incoming production by roasted SKU
    incoming: dict[str, float] = {}
    for sess in batch_plan.get("sessions", []):
        for b in sess.get("batches", []):
            rsku = b.get("roasted_sku", "")
            incoming[rsku] = incoming.get(rsku, 0) + b.get("expected_output_kg", 0)

    items = []
    for p in predictions:
        if p["daily_rate"] <= 0:
            continue

        sku = p["sku"]
        stock = p["current_stock"]
        daily = p["daily_rate"]
        lead = p["lead_time_days"]
        cat = p["category"]

        # Hour-level stockout
        hours_to_stockout = round(stock / daily * 24, 1) if daily > 0 else None

        # Day-by-day projection
        daily_levels = []
        running = stock
        # Add incoming production on day 1 if scheduled
        inc = incoming.get(sku, 0)
        for day in range(1, horizon_days + 1):
            running -= daily
            if day == 1 and inc > 0:
                running += inc
            daily_levels.append({"day": day, "projected_stock": round(max(0, running), 2)})

        stockout_day = None
        for dl in daily_levels:
            if dl["projected_stock"] <= 0:
                stockout_day = dl["day"]
                break

        # Reorder timing
        reorder_by_day = None
        if stockout_day:
            reorder_by_day = max(0, stockout_day - lead)
            if reorder_by_day == 0:
                reorder_timing = "ORDER NOW -- already past reorder window"
            elif reorder_by_day <= 1:
                reorder_timing = "Order today"
            else:
                reorder_timing = f"Order within {reorder_by_day} days"
        else:
            reorder_timing = "No reorder needed this period"

        items.append({
            "sku": sku,
            "name": p["name"],
            "category": cat,
            "unit": p["unit"],
            "current_stock": stock,
            "daily_rate": daily,
            "hours_to_stockout": hours_to_stockout,
            "stockout_day": stockout_day,
            "incoming_production_kg": round(inc, 1) if inc else 0,
            "lead_time_days": lead,
            "reorder_timing": reorder_timing,
            "reorder_by_day": reorder_by_day,
            "daily_projection": daily_levels,
        })

    # Sort: items stocking out soonest first
    items.sort(key=lambda i: i["hours_to_stockout"] if i["hours_to_stockout"] is not None else 9999)
    return items


def get_3_day_forecast() -> dict:
    """3-day predictive forecast: item-level stockout timelines, production needs, reorder timing."""
    plan = generate_daily_operations_plan()
    items = _build_item_forecast(horizon_days=3)

    # Separate at-risk from safe
    at_risk = [i for i in items if i["stockout_day"] is not None and i["stockout_day"] <= 3]
    safe = [i for i in items if i not in at_risk]

    # Production needs
    batch_plan = production_intelligence.recommend_next_batches()
    production_needed = []
    for sess in batch_plan.get("sessions", []):
        production_needed.append({
            "green_coffee": sess["green_coffee_name"],
            "total_green_kg": sess["total_green_kg"],
            "total_output_kg": sess["total_output_kg"],
            "batch_count": sess["batch_count"],
            "priority": sess["priority"],
        })

    # Aggregate consumption
    rates = inventory_intelligence.get_daily_usage_rate()
    roasted_3d = sum(r["daily_rate"] * 3 for r in rates if r["category"] == "roasted_coffee")
    milk_3d = sum(r["daily_rate"] * 3 for r in rates if r["category"] == "milk")

    # Format
    lines = ["3-Day Forecast -- Maillard", "=" * 40, ""]
    lines.append("PREDICTED STOCKOUTS")
    if at_risk:
        for i in at_risk:
            hrs = i["hours_to_stockout"]
            lines.append(f"  {i['name']}: {hrs:.0f}h remaining ({i['current_stock']} {i['unit']})")
            lines.append(f"    Stockout: day {i['stockout_day']} | {i['reorder_timing']}")
    else:
        lines.append("  None within 3 days")
    lines.append("")
    lines.append("PRODUCTION NEEDS")
    if production_needed:
        for p in production_needed:
            lines.append(f"  [{p['priority']}] {p['total_green_kg']}kg {p['green_coffee']} -> {p['total_output_kg']}kg roasted ({p['batch_count']} batches)")
    else:
        lines.append("  No production needed")
    lines.append("")
    lines.append("CONSUMPTION FORECAST (3 days)")
    lines.append(f"  Roasted coffee: {roasted_3d:.1f}kg ({round(roasted_3d * SHOTS_PER_KG)} shots)")
    lines.append(f"  Milk: {milk_3d:.1f}L")
    lines.append("=" * 40)

    return {
        "horizon": "3 days",
        "items": items,
        "at_risk": at_risk,
        "safe": safe,
        "production_needed": production_needed,
        "consumption": {
            "roasted_coffee_kg": round(roasted_3d, 1),
            "milk_liters": round(milk_3d, 1),
            "espresso_shots": round(roasted_3d * SHOTS_PER_KG),
        },
        "actions": plan["next_3_days"],
        "formatted": "\n".join(lines),
    }


def get_7_day_plan() -> dict:
    """7-day predictive plan: weekly stockout map, production schedule, reorder calendar."""
    plan = generate_daily_operations_plan()
    items = _build_item_forecast(horizon_days=7)

    at_risk = [i for i in items if i["stockout_day"] is not None and i["stockout_day"] <= 7]

    # Production schedule
    batch_plan = production_intelligence.recommend_next_batches()

    # Wholesale forecast
    ws_forecast = wholesale_intelligence.forecast_demand(days_history=90, forecast_weeks=1)

    # Weekly consumption
    rates = inventory_intelligence.get_daily_usage_rate()
    weekly_roasted = sum(r["daily_rate"] * 7 for r in rates if r["category"] == "roasted_coffee")
    weekly_milk = sum(r["daily_rate"] * 7 for r in rates if r["category"] == "milk")
    weekly_green = sum(r["daily_rate"] * 7 for r in rates if r["category"] == "green_coffee")

    # Reorder calendar: when each item needs ordering this week
    reorder_calendar = []
    for i in items:
        if i["reorder_by_day"] is not None and i["reorder_by_day"] <= 7:
            reorder_calendar.append({
                "item": i["name"],
                "sku": i["sku"],
                "order_by_day": i["reorder_by_day"],
                "timing": i["reorder_timing"],
                "stockout_day": i["stockout_day"],
            })
    reorder_calendar.sort(key=lambda r: r["order_by_day"] if r["order_by_day"] is not None else 99)

    # Format
    lines = ["7-Day Plan -- Maillard", "=" * 40, ""]
    lines.append("STOCKOUT MAP (next 7 days)")
    if at_risk:
        for i in at_risk:
            lines.append(f"  Day {i['stockout_day']}: {i['name']} runs out ({i['hours_to_stockout']:.0f}h from now)")
    else:
        lines.append("  No stockouts predicted this week")
    lines.append("")
    lines.append("REORDER CALENDAR")
    if reorder_calendar:
        for r in reorder_calendar:
            lines.append(f"  Day {r['order_by_day']}: Order {r['item']} ({r['timing']})")
    else:
        lines.append("  No orders needed this week")
    lines.append("")
    lines.append("PRODUCTION SCHEDULE")
    for sess in batch_plan.get("sessions", []):
        lines.append(f"  [{sess['priority']}] {sess['total_green_kg']}kg {sess['green_coffee_name']} ({sess['batch_count']} batches, ~{sess['estimated_time_min']}min)")
    lines.append("")
    lines.append("WEEKLY CONSUMPTION")
    lines.append(f"  Roasted: {weekly_roasted:.1f}kg | Milk: {weekly_milk:.1f}L | Green: {weekly_green:.1f}kg")
    lines.append(f"  WS forecast: {ws_forecast.get('weekly_avg_kg', 0)}kg")
    total_roast_need = weekly_roasted + ws_forecast.get("weekly_avg_kg", 0)
    lines.append(f"  Total roast needed: {total_roast_need:.1f}kg (requires {total_roast_need / GREEN_TO_ROASTED:.1f}kg green)")
    lines.append("=" * 40)

    return {
        "horizon": "7 days",
        "items": items,
        "at_risk": at_risk,
        "reorder_calendar": reorder_calendar,
        "production_schedule": batch_plan.get("sessions", []),
        "consumption": {
            "roasted_coffee_kg": round(weekly_roasted, 1),
            "milk_liters": round(weekly_milk, 1),
            "green_coffee_kg": round(weekly_green, 1),
            "wholesale_forecast_kg": ws_forecast.get("weekly_avg_kg", 0),
            "total_roast_needed_kg": round(total_roast_need, 1),
            "green_required_kg": round(total_roast_need / GREEN_TO_ROASTED, 1),
        },
        "actions": plan["actions"],
        "risk_alerts": plan["risk_alerts"],
        "formatted": "\n".join(lines),
    }


# =============================================================================
# FORMATTED OUTPUT
# =============================================================================


def _format_plan(actions: list[dict], completed: list[dict], risks: list[dict], stock_status: list[dict], now: datetime) -> str:
    L: list[str] = []
    L.append(f"Daily Operations Plan -- Maillard Coffee Roasters")
    L.append(f"Date: {now.strftime('%Y-%m-%d')}   Generated: {now.strftime('%H:%M UTC')}")
    L.append("=" * 55)

    total_min = sum(a.get("time_minutes", 0) for a in actions)
    crit = sum(1 for a in actions if a["priority"] == "CRITICAL")
    high = sum(1 for a in actions if a["priority"] == "HIGH")
    total_cost = sum(a.get("financials", {}).get("action_cost_eur", 0) for a in actions)
    total_rev = sum(a.get("financials", {}).get("revenue_protected_eur", 0) for a in actions)
    total_loss = sum(a.get("financials", {}).get("loss_if_ignored_eur", 0) for a in actions)

    L.append(f"  {len(actions)} steps | ~{total_min} min | {crit} CRITICAL | {high} HIGH")
    L.append(f"  Cost: EUR {total_cost:.0f} | Protects: EUR {total_rev:.0f} | At risk: EUR {total_loss:.0f}")
    L.append("")

    # Top 3 financially critical
    top3 = sorted(actions, key=lambda a: -a.get("priority_score", 0))[:3]
    if top3:
        L.append("TOP PRIORITIES (by financial + urgency score)")
        L.append("-" * 45)
        for i, a in enumerate(top3, 1):
            fin = a.get("financials", {})
            rev = fin.get("revenue_protected_eur", 0)
            loss = fin.get("loss_if_ignored_eur", 0)
            score = a.get("priority_score", 0)
            L.append(f"  {i}. [{a['priority']}] {a['action']}  (score: {score})")
            L.append(f"     Protects EUR {rev:.0f} | Risk EUR {loss:.0f}")
        L.append("")

    # Group by priority tier
    for tier, label in [
        ("CRITICAL", "CRITICAL (Do Immediately)"),
        ("HIGH", "HIGH (Same Day)"),
        ("MEDIUM", "MEDIUM (Within 2-3 Days)"),
    ]:
        tier_actions = [a for a in actions if a["priority"] == tier]
        if not tier_actions:
            continue

        L.append(label)
        L.append("-" * 45)

        for a in tier_actions:
            step = a.get("step", "?")
            mins = a.get("time_minutes", 0)
            time_str = f"{mins} min" if mins < 60 else f"{mins // 60}h {mins % 60}m"
            when = a.get("when", "?")

            score = a.get("priority_score", 0)
            status = a.get("status", "pending")
            marker = {
                "pending": "[ ]", "in_progress": "[~]",
                "completed_success": "[x]", "completed_failed": "[!]",
                "skipped": "[-]",
            }.get(status, "[ ]")
            L.append(f"  {marker} {step}. {a['action']}  [score: {score}]")
            L.append(f"     When: {when} | Time: {time_str}")
            L.append(f"     Demand: {a.get('demand_source', '?')}")
            L.append(f"     Reason: {a['reason']}")

            if a.get("dependency"):
                L.append(f"     Depends on: {a['dependency']}")

            L.append(f"     Impact: {a.get('impact_detail', '')}")

            fin = a.get("financials")
            if fin:
                roi = f" (ROI: {fin['roi']}x)" if fin.get("roi") else ""
                L.append(
                    f"     $: Cost EUR {fin['action_cost_eur']:.0f} | "
                    f"Protects EUR {fin['revenue_protected_eur']:.0f} | "
                    f"Margin: {fin['margin_impact']}{roi}"
                )

            if tier in ("CRITICAL", "HIGH"):
                L.append(f"     If ignored: {a.get('if_ignored', '')}")

            # Batch details for roast sessions
            for bd in a.get("batch_details", []):
                eff = bd.get("efficiency_pct", 0)
                eff_note = "" if eff >= 80 else f" [{eff:.0f}% cap]"
                src = ", ".join(bd.get("demand_sources", [])) or "replenishment"
                L.append(
                    f"       Batch {bd.get('batch_sequence', '?')}: "
                    f"{bd.get('green_kg', '?')}kg -> {bd.get('expected_output_kg', '?')}kg "
                    f"{bd.get('roasted_name', '')}{eff_note}"
                )
                L.append(f"         For: {src}")
                L.append(
                    f"         Allocate: {bd.get('wholesale_kg', 0)}kg WS / "
                    f"{bd.get('retail_kg', 0)}kg retail"
                )

            for o in a.get("sub_orders", []):
                L.append(f"       - {o.get('quantity', '?')} {o.get('unit', '')} {o.get('sku', '')} ({o.get('demand_source', '')})")

            L.append("")

    # Risks
    if risks:
        L.append("RISKS")
        L.append("-" * 45)
        for r in risks:
            sev = r["severity"]
            item = r["item"]
            if r["type"] == "stockout":
                L.append(f"  [{sev}] {item}: {r.get('stock', '?')} left ({r.get('days_left', '?')} days)")
                L.append(f"     Demand: {r.get('demand', '?')}")
                L.append(f"     Consequence: {r.get('consequence', '')}")
            elif r["type"] == "overstock":
                L.append(f"  [LOW] OVERSTOCK: {item} ({r.get('days_left', '?')} days supply)")
            elif r["type"] == "fulfillment_gap":
                L.append(f"  [{sev}] {r.get('detail', '')}")
                L.append(f"     Consequence: {r.get('consequence', '')}")
            L.append("")

    if not actions and not risks and not completed:
        L.append("  All systems nominal. No actions required.")

    # Completed today
    if completed:
        L.append("COMPLETED TODAY")
        L.append("-" * 45)
        for a in completed:
            L.append(f"  [x] {a.get('action', '?')}")
        L.append("")

    L.append("=" * 55)
    return "\n".join(L)
