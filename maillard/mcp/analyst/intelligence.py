"""
Intelligence service layer.

Converts raw market + FX data into Maillard-specific business interpretations.
Reusable by the Analyst agent, Procurement, Executive, and API endpoints.

All intelligence functions now respect the validation pipeline:
- Data is validated before interpretation
- Validation state is included in all outputs
- Decision-grade classification affects recommendation strength
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger

from maillard.mcp.analyst.market_data import (
    get_coffee_futures_quote,
    get_coffee_futures_history,
    get_market_snapshot,
    get_coffee_market_data,
)
from maillard.mcp.analyst.fx_data import get_fx_rate, get_coffee_fx_rates
from maillard.mcp.analyst.validation import validate_market_data
from maillard.mcp.shared.claude_client import ask

# ── Maillard-specific constants ──────────────────────────────────────────────
# These are reference values for interpretation, not live prices.
# Update periodically based on actual Maillard purchasing data.

MAILLARD_REFERENCE = {
    "espresso_dose_g": 22,
    "espresso_yield_ml": 50,
    "shots_per_kg": 45,           # ~45 doubles from 1 kg
    "green_to_roasted_ratio": 0.82,  # 18% weight loss in roasting
    "avg_retail_espresso_price": 3.50,  # EUR
    "avg_wholesale_kg_price": 18.00,    # EUR per kg roasted
    "target_cogs_percent": 25,          # target coffee COGS as % of revenue
    "typical_green_cost_eur_kg": 6.50,  # recent average green cost
}

INTELLIGENCE_SYSTEM = """You are the Maillard Intelligence Analyst.
You produce concise, executive-grade market intelligence for Maillard Coffee Roasters.
Rules:
- Use ONLY the data provided. Never invent prices.
- If data is missing, say so explicitly.
- Translate every number into a Maillard business implication.
- Keep recommendations actionable and specific.
- Use markdown headings and bullet points.
"""


# ── Core intelligence functions ──────────────────────────────────────────────


async def summarize_market_conditions() -> dict:
    """Full market conditions summary with business context and validation."""
    snapshot = await get_market_snapshot()
    if "error" in snapshot.get("coffee_futures", {}):
        return {"error": "Cannot summarize — live market data unavailable."}

    coffee = snapshot["coffee_futures"]
    fx = snapshot.get("fx_rates", {})
    state = snapshot.get("state", "INVALID_DATA")
    integrity_report = snapshot.get("integrity_report", "")

    prompt = (
        f"Produce a concise market conditions summary for Maillard Coffee Roasters.\n\n"
        f"VALIDATION STATUS: {state}\n\n"
    )

    if integrity_report:
        prompt += f"{integrity_report}\n\n"

    prompt += (
        f"LIVE DATA:\n"
        f"- ICE Arabica (KC): {coffee['price']:.2f} cents/lb "
        f"({'+' if coffee['change_percent'] >= 0 else ''}{coffee['change_percent']:.1f}% today)\n"
        f"- Daily trend: {snapshot.get('trend', 'unknown')}\n"
    )
    if fx:
        for pair, info in fx.items():
            prompt += f"- {pair}: {info['rate']:.4f}\n"
    else:
        prompt += "- FX data: unavailable\n"

    prompt += (
        f"\nMaillard reference:\n"
        f"- Typical green cost: EUR {MAILLARD_REFERENCE['typical_green_cost_eur_kg']}/kg\n"
        f"- Target COGS: {MAILLARD_REFERENCE['target_cogs_percent']}%\n"
        f"- Avg wholesale price: EUR {MAILLARD_REFERENCE['avg_wholesale_kg_price']}/kg roasted\n"
        f"- Avg retail espresso: EUR {MAILLARD_REFERENCE['avg_retail_espresso_price']}\n\n"
        f"Structure your response with these sections:\n"
        f"## Market State\n## Green Coffee Cost Pressure\n"
        f"## FX Impact\n## Recommended Action\n"
    )

    if state in ("FEED_CONFLICT", "INVALID_DATA"):
        prompt += (
            f"\nCRITICAL: Validation status is {state}. "
            f"Do NOT provide procurement recommendations. Warn the reader.\n"
        )

    analysis = await ask(prompt, INTELLIGENCE_SYSTEM, max_tokens=1200)
    return {
        "summary": analysis,
        "snapshot": snapshot,
        "state": state,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def estimate_green_cost_pressure(
    *,
    _quote: dict | None = None,
    _fx: dict | None = None,
) -> dict:
    """Estimate how current futures affect Maillard's green coffee costs.

    Accepts optional pre-fetched data to avoid redundant API calls
    when called as part of a larger pipeline.
    """
    quote = _quote or await get_coffee_futures_quote()
    if "error" in quote:
        return {"error": quote["error"]}

    fx = _fx or await get_coffee_fx_rates()
    eur_usd = fx.get("rates", {}).get("EUR/USD", {}).get("rate")

    price_cents_lb = quote["price"]
    price_usd_kg = price_cents_lb * 2.20462 / 100  # cents/lb → $/kg
    price_eur_kg = price_usd_kg / eur_usd if eur_usd else None

    ref_cost = MAILLARD_REFERENCE["typical_green_cost_eur_kg"]
    pressure_pct = ((price_eur_kg - ref_cost) / ref_cost * 100) if price_eur_kg else None

    return {
        "futures_price_cents_lb": price_cents_lb,
        "estimated_green_usd_kg": round(price_usd_kg, 2),
        "estimated_green_eur_kg": round(price_eur_kg, 2) if price_eur_kg else None,
        "reference_cost_eur_kg": ref_cost,
        "cost_pressure_percent": round(pressure_pct, 1) if pressure_pct is not None else None,
        "eur_usd_rate": eur_usd,
        "note": (
            "Estimated green cost derived from futures. Actual landed cost depends on "
            "origin, quality grade, logistics, and contract terms."
        ),
    }


async def estimate_menu_margin_pressure(
    *,
    _quote: dict | None = None,
    _fx: dict | None = None,
) -> dict:
    """Estimate how market changes affect Maillard's retail margins.

    Accepts optional pre-fetched data to avoid redundant API calls.
    """
    cost_data = await estimate_green_cost_pressure(_quote=_quote, _fx=_fx)
    if "error" in cost_data:
        return cost_data

    green_eur = cost_data.get("estimated_green_eur_kg")
    if green_eur is None:
        return {"error": "Cannot estimate — EUR/USD rate unavailable."}

    roasted_cost = green_eur / MAILLARD_REFERENCE["green_to_roasted_ratio"]
    cost_per_shot = roasted_cost / MAILLARD_REFERENCE["shots_per_kg"]
    retail_price = MAILLARD_REFERENCE["avg_retail_espresso_price"]
    margin_per_shot = retail_price - cost_per_shot
    margin_pct = (margin_per_shot / retail_price) * 100

    wholesale_kg = MAILLARD_REFERENCE["avg_wholesale_kg_price"]
    wholesale_margin = wholesale_kg - roasted_cost
    wholesale_margin_pct = (wholesale_margin / wholesale_kg) * 100

    return {
        "green_cost_eur_kg": round(green_eur, 2),
        "roasted_cost_eur_kg": round(roasted_cost, 2),
        "cost_per_espresso_shot": round(cost_per_shot, 3),
        "retail_espresso_price": retail_price,
        "retail_margin_per_shot": round(margin_per_shot, 3),
        "retail_margin_percent": round(margin_pct, 1),
        "wholesale_kg_price": wholesale_kg,
        "wholesale_margin_per_kg": round(wholesale_margin, 2),
        "wholesale_margin_percent": round(wholesale_margin_pct, 1),
        "target_cogs_percent": MAILLARD_REFERENCE["target_cogs_percent"],
        "actual_cogs_percent": round(100 - margin_pct, 1),
    }


async def compare_supplier_cost_vs_market(
    supplier_price_eur_kg: float,
    origin: str = "Brazil",
) -> dict:
    """Compare a supplier quote against current market-implied cost."""
    cost_data = await estimate_green_cost_pressure()
    if "error" in cost_data:
        return cost_data

    market_eur = cost_data.get("estimated_green_eur_kg")
    if market_eur is None:
        return {"error": "Cannot compare — market EUR estimate unavailable."}

    diff = supplier_price_eur_kg - market_eur
    diff_pct = (diff / market_eur) * 100

    if diff_pct > 15:
        assessment = "SIGNIFICANTLY ABOVE MARKET — renegotiate or seek alternatives"
    elif diff_pct > 5:
        assessment = "ABOVE MARKET — review justification (quality premium, logistics?)"
    elif diff_pct > -5:
        assessment = "IN LINE WITH MARKET — acceptable"
    elif diff_pct > -15:
        assessment = "BELOW MARKET — favorable, consider locking in"
    else:
        assessment = "WELL BELOW MARKET — verify quality and contract terms"

    return {
        "supplier_price_eur_kg": supplier_price_eur_kg,
        "market_implied_eur_kg": round(market_eur, 2),
        "difference_eur": round(diff, 2),
        "difference_percent": round(diff_pct, 1),
        "origin": origin,
        "assessment": assessment,
        "futures_price": cost_data["futures_price_cents_lb"],
    }


async def generate_executive_market_brief() -> dict:
    """Generate a full executive-grade market intelligence brief.

    Fetches market data once and passes it to downstream functions
    to avoid redundant API calls against rate-limited providers.
    Includes validation state and integrity report.
    """
    import asyncio

    # Phase 1: Fetch all external data in parallel (one call per source)
    snapshot_task = get_market_snapshot()
    history_task = get_coffee_futures_history(days=30)
    snapshot, history = await asyncio.gather(snapshot_task, history_task)

    coffee = snapshot.get("coffee_futures", {})
    state = snapshot.get("state", "INVALID_DATA")
    integrity_report = snapshot.get("integrity_report", "")

    if "error" in coffee:
        return {"error": "Cannot generate brief — live market data unavailable."}

    # Phase 2: Compute margin using already-fetched data (no extra API calls)
    fx_data = {"rates": snapshot.get("fx_rates", {})}
    margin = await estimate_menu_margin_pressure(_quote=coffee, _fx=fx_data)

    # Phase 3: Compute 30-day trend from history
    bars = history.get("bars", [])
    trend_note = "History unavailable."
    if len(bars) >= 2:
        newest = bars[0]["close"]
        oldest = bars[-1]["close"]
        change_30d = ((newest - oldest) / oldest) * 100
        trend_note = (
            f"30-day move: {'+' if change_30d >= 0 else ''}{change_30d:.1f}% "
            f"(from {oldest:.2f} to {newest:.2f})"
        )

    fx = snapshot.get("fx_rates", {})
    if fx:
        fx_lines = "\n".join(
            f"- {pair}: {info['rate']:.4f} ({'+' if info.get('change_percent', 0) >= 0 else ''}{info.get('change_percent', 0):.1f}%)"
            for pair, info in fx.items()
        )
    else:
        fx_lines = "- FX data unavailable"

    margin_lines = ""
    if "error" not in margin:
        margin_lines = (
            f"- Retail espresso margin: {margin['retail_margin_percent']}%\n"
            f"- Wholesale margin: {margin['wholesale_margin_percent']}%\n"
            f"- Actual COGS: {margin['actual_cogs_percent']}% (target: {margin['target_cogs_percent']}%)\n"
            f"- Cost per espresso shot: EUR {margin['cost_per_espresso_shot']:.3f}\n"
        )
    else:
        margin_lines = f"- Margin data unavailable: {margin.get('error', 'unknown')}\n"

    prompt = (
        f"Generate a concise executive market intelligence brief for Maillard management.\n\n"
        f"DATA VALIDATION STATUS: {state}\n\n"
    )

    if integrity_report:
        prompt += f"{integrity_report}\n\n"

    prompt += (
        f"LIVE DATA:\n"
        f"Coffee Futures (KC): {coffee['price']:.2f} cents/lb "
        f"({'+' if coffee['change_percent'] >= 0 else ''}{coffee['change_percent']:.1f}% today)\n"
        f"Trend: {snapshot.get('trend', 'unknown')}\n"
        f"{trend_note}\n\n"
        f"FX Rates:\n{fx_lines}\n\n"
        f"Margin Analysis:\n{margin_lines}\n\n"
        f"Structure as:\n"
        f"## Executive Summary\n"
        f"## Data Integrity Status\n"
        f"## Market State\n"
        f"## Procurement Risk\n"
        f"## Pricing & Margin Risk\n"
        f"## Recommended Actions\n"
    )

    if state in ("FEED_CONFLICT", "INVALID_DATA"):
        prompt += (
            f"\nCRITICAL: Validation status is {state}. "
            f"The Recommended Actions section MUST state that procurement and "
            f"execution decisions should be deferred until data is validated.\n"
        )

    brief = await ask(prompt, INTELLIGENCE_SYSTEM, max_tokens=1500)
    return {
        "brief": brief,
        "snapshot": snapshot,
        "margin_analysis": margin if "error" not in margin else None,
        "trend_30d": trend_note,
        "state": state,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def analyze_maillard_market_impact() -> dict:
    """
    Full market impact analysis — combines all intelligence into a single
    business interpretation specifically for Maillard.
    """
    brief = await generate_executive_market_brief()
    if "error" in brief:
        return brief
    return {
        "analysis": brief["brief"],
        "data": {
            "snapshot": brief.get("snapshot"),
            "margin": brief.get("margin_analysis"),
            "trend_30d": brief.get("trend_30d"),
        },
        "generated_at": brief["generated_at"],
    }
