"""
Intelligence API routes — /api/intelligence/*

JSON endpoints for the frontend intelligence dashboard and external consumers.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from loguru import logger

from maillard.mcp.analyst.market_data import (
    get_coffee_futures_quote,
    get_coffee_futures_history,
    get_market_snapshot,
)
from maillard.mcp.analyst.fx_data import get_fx_rate, get_coffee_fx_rates
from maillard.mcp.analyst.intelligence import (
    summarize_market_conditions,
    estimate_green_cost_pressure,
    estimate_menu_margin_pressure,
    generate_executive_market_brief,
    analyze_maillard_market_impact,
)
from maillard.models.storage import (
    save_market_snapshot,
    save_fx_snapshot,
    save_intelligence_report,
    get_recent_market_snapshots,
    get_recent_reports,
)

router = APIRouter(prefix="/intelligence", tags=["Intelligence"])


@router.get("/buying-signal")
async def buying_signal(days: int = 14):
    """Should I buy coffee now? Simple BUY/WAIT/MONITOR recommendation."""
    from maillard.mcp.analyst.buying_signal import get_buying_signal
    return await get_buying_signal(days=days)


@router.get("/market-snapshot")
async def market_snapshot(debug: bool = False, mode: str = "SAFE"):
    """Combined coffee futures + FX rates snapshot.

    Query params:
      ?debug=true  -- include raw engine debug data
      ?mode=SAFE|CAUTIOUS|AGGRESSIVE  -- decision mode
    """
    try:
        result = await get_market_snapshot(mode=mode.upper(), debug=debug)
        coffee = result.get("coffee_futures", {})
        if "error" not in coffee:
            save_market_snapshot(coffee)
        for pair, fx in result.get("fx_rates", {}).items():
            save_fx_snapshot(fx)
        return result
    except Exception as e:
        logger.error(f"[API] market-snapshot error: {e}")
        raise HTTPException(500, "Failed to fetch market snapshot.")


@router.get("/coffee-futures")
async def coffee_futures(days: int = 0):
    """Live coffee futures quote, optionally with history."""
    try:
        quote = await get_coffee_futures_quote()
        if "error" not in quote:
            save_market_snapshot(quote)
        response: dict = {"quote": quote}
        if days > 0:
            history = await get_coffee_futures_history(min(days, 90))
            response["history"] = history
        return response
    except Exception as e:
        logger.error(f"[API] coffee-futures error: {e}")
        raise HTTPException(500, "Failed to fetch coffee futures.")


@router.get("/fx")
async def fx_rates(pair: str | None = None):
    """FX rates — single pair or all coffee-relevant pairs."""
    try:
        if pair:
            result = await get_fx_rate(pair)
            if "error" not in result:
                save_fx_snapshot(result)
            return result
        else:
            result = await get_coffee_fx_rates()
            for p, fx in result.get("rates", {}).items():
                save_fx_snapshot(fx)
            return result
    except Exception as e:
        logger.error(f"[API] fx error: {e}")
        raise HTTPException(500, "Failed to fetch FX rates.")


@router.get("/executive-brief")
async def executive_brief():
    """Full executive-grade market intelligence brief."""
    try:
        result = await generate_executive_market_brief()
        if "error" in result:
            return result
        save_intelligence_report(
            "executive_brief", "Executive Market Brief",
            result["brief"][:500],
            {"trend_30d": result.get("trend_30d"), "margin": result.get("margin_analysis")}
        )
        return result
    except Exception as e:
        logger.error(f"[API] executive-brief error: {e}")
        raise HTTPException(500, "Failed to generate executive brief.")


@router.get("/green-cost-pressure")
async def green_cost_pressure():
    """Current green coffee cost pressure estimate."""
    try:
        return await estimate_green_cost_pressure()
    except Exception as e:
        logger.error(f"[API] green-cost error: {e}")
        raise HTTPException(500, "Failed to estimate green cost pressure.")


@router.get("/margin-pressure")
async def margin_pressure():
    """Current retail and wholesale margin pressure estimate."""
    try:
        return await estimate_menu_margin_pressure()
    except Exception as e:
        logger.error(f"[API] margin error: {e}")
        raise HTTPException(500, "Failed to estimate margin pressure.")


@router.get("/market-conditions")
async def market_conditions():
    """Summarized market conditions with AI analysis."""
    try:
        result = await summarize_market_conditions()
        if "error" not in result:
            save_intelligence_report(
                "market_conditions", "Market Conditions Summary",
                result.get("summary", "")[:500],
                result.get("snapshot"),
            )
        return result
    except Exception as e:
        logger.error(f"[API] market-conditions error: {e}")
        raise HTTPException(500, "Failed to summarize market conditions.")


@router.get("/history/snapshots")
async def snapshot_history(symbol: str = "KC", limit: int = 50):
    """Stored market snapshot history from the database."""
    return {"snapshots": get_recent_market_snapshots(symbol, limit)}


@router.get("/history/reports")
async def report_history(report_type: str | None = None, limit: int = 20):
    """Stored intelligence report history."""
    return {"reports": get_recent_reports(report_type, limit)}


@router.get("/fetch-url")
async def fetch_url(url: str):
    """Fetch and parse a URL, returning structured content."""
    from maillard.mcp.analyst.url_tools import extract_market_relevant_text
    try:
        return await extract_market_relevant_text(url)
    except Exception as e:
        logger.error(f"[API] fetch-url error: {e}")
        raise HTTPException(500, "Failed to fetch URL.")


@router.get("/source-reliability")
async def source_reliability():
    """Full reliability profile for all tracked data sources.

    Shows rolling score, valid/conflict rates, avg deviation, stability flag,
    and raw event history per source.
    """
    from maillard.mcp.analyst.market_data_engine import get_source_reliability
    return get_source_reliability()
