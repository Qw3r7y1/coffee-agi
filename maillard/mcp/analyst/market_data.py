"""
Analyst MCP -- Coffee futures market data tools.

Delegates to market_data_engine.py for the production pipeline.
This module provides the public API consumed by server.py and intelligence.py.

All prices returned in CANONICAL UNIT: $/lb (dollars per pound).
Cents/lb values are also included for display convenience.
"""
from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger

from maillard.mcp.analyst.providers import get_market_provider
from maillard.mcp.analyst.fx_data import get_coffee_fx_rates
from maillard.mcp.analyst.market_data_engine import (
    get_validated_coffee_data,
    detect_unit,
    normalize_price,
    PRICE_MIN_DOLLARS,
    PRICE_MAX_DOLLARS,
    STATE_VALIDATED,
    STATE_WARNING,
    STATE_FEED_CONFLICT,
    STATE_INVALID_DATA,
    MODE_SAFE,
    MODE_CAUTIOUS,
    MODE_AGGRESSIVE,
    VALID_MODES,
)

SYMBOL = "KC"

# Re-export for backward compat (cents values used by some callers)
COFFEE_MIN_CENTS = PRICE_MIN_DOLLARS * 100
COFFEE_MAX_CENTS = PRICE_MAX_DOLLARS * 100


# =============================================================================
# PRIMARY FUNCTION -- delegates to engine
# =============================================================================


async def get_coffee_market_data(
    reference_price_cents: float | None = None,
    reference_source: str | None = None,
    conflict_threshold_pct: float = 10.0,
    mode: str = MODE_SAFE,
    debug: bool = False,
) -> dict:
    """Unified coffee market data with full engine pipeline.

    Accepts reference price in CENTS/LB for backward compat with URL extractor.
    Internally normalizes everything to $/lb via the engine.

    Returns dict with:
        api_price ($/lb), reference_price ($/lb), state, decision_grade,
        integrity_report, api_data, validation, comparison, source_scores, ...
    """
    # Convert reference from cents to raw value (engine will auto-detect unit)
    ref_raw = reference_price_cents  # pass as-is; engine detects cents vs dollars

    engine_result = await get_validated_coffee_data(
        reference_price_raw=ref_raw,
        reference_unit="cents/lb" if ref_raw is not None else None,
        reference_source_name=reference_source or "Investing.com",
        conflict_threshold_pct=conflict_threshold_pct,
        mode=mode,
        debug=debug,
    )

    # Add backward-compat fields
    api_dollars = engine_result.get("api_price")
    engine_result["api_price_cents"] = round(api_dollars * 100, 2) if api_dollars else None
    engine_result["symbol"] = SYMBOL
    engine_result["unit"] = "$/lb"  # canonical

    return engine_result


# =============================================================================
# SINGLE-SOURCE QUOTE (backward compat)
# =============================================================================


async def get_coffee_futures_quote() -> dict:
    """Fetch latest ICE Arabica Coffee price via engine pipeline."""
    engine = await get_validated_coffee_data()

    api_data = engine.get("api_data", {})
    if "error" in api_data:
        return api_data

    if not engine.get("validation", {}).get("is_valid", False):
        issues = engine.get("validation", {}).get("issues", ["unknown"])
        return {
            "error": f"Internal API feed invalid: {'; '.join(issues)}",
            "validation_failure": issues[0] if issues else "unknown",
            "validation": engine.get("validation"),
        }

    # Attach normalized prices to the raw result
    api_dollars = engine.get("api_price")
    norm = engine.get("api_normalization", {})
    api_data["price_dollars_lb"] = api_dollars
    api_data["price_cents_lb"] = norm.get("price_cents_lb")
    api_data["unit"] = "$/lb"
    api_data["unit_display"] = "$/lb"
    api_data["name"] = api_data.get("name", "ICE Arabica Coffee Futures")
    api_data["feed_validated"] = True
    api_data["validation"] = engine.get("validation")
    api_data["source_score"] = engine.get("source_scores", {}).get("api", {}).get("score")

    logger.info(
        f"[MARKET-DATA] quote -> ${api_dollars}/lb "
        f"({norm.get('price_cents_lb')} c/lb) "
        f"source={api_data.get('source')} "
        f"score={api_data.get('source_score')}"
    )
    return api_data


# =============================================================================
# HISTORY
# =============================================================================


async def get_coffee_futures_history(days: int = 30) -> dict:
    """Fetch daily OHLC history for ICE Arabica Coffee futures."""
    provider = get_market_provider()
    result = await provider.get_history(SYMBOL, days)

    if "error" in result:
        return result

    # Validate the most recent bar via unit detection
    bars = result.get("bars", [])
    if bars:
        latest_close = bars[0].get("close", 0)
        unit_info = detect_unit(latest_close)
        if unit_info["detected_unit"] == "unknown":
            return {
                "error": f"History invalid: latest close {latest_close} unrecognizable unit",
                "validation_failure": "price_implausible",
            }

    result["name"] = "ICE Arabica Coffee Futures"
    result["unit"] = "cents/lb"  # Yahoo history returns cents
    result["source"] = result.get("source", "unknown")
    logger.info(f"[MARKET-DATA] history -> {result.get('count', '?')} bars")
    return result


# =============================================================================
# SNAPSHOT (coffee + FX combined)
# =============================================================================


async def get_market_snapshot(
    reference_price_cents: float | None = None,
    mode: str = MODE_SAFE,
    debug: bool = False,
) -> dict:
    """Combined snapshot: coffee futures + FX rates + engine validation."""
    import asyncio

    coffee_task = get_coffee_market_data(
        reference_price_cents=reference_price_cents,
        mode=mode,
        debug=debug,
    )
    fx_task = get_coffee_fx_rates()
    coffee_result, fx = await asyncio.gather(coffee_task, fx_task)

    now = datetime.now(timezone.utc).isoformat()
    api_data = coffee_result.get("api_data", {})
    api_dollars = coffee_result.get("api_price")

    snapshot: dict = {
        "timestamp": now,
        "coffee_futures": api_data if api_dollars else {
            "error": api_data.get("error", "unavailable"),
        },
        "fx_rates": fx.get("rates", {}),
        # Engine fields
        "market_data": coffee_result,
        "integrity_report": coffee_result.get("integrity_report", ""),
        "state": coffee_result.get("market_state", "INVALID_DATA"),
        "decision_grade": coffee_result.get("decision_grade", False),
        "source_scores": coffee_result.get("source_scores", {}),
    }

    # Trend from change_percent
    if api_dollars:
        pct = api_data.get("change_percent", 0)
        if pct > 1.5:
            snapshot["trend"] = "strong rally"
        elif pct > 0.3:
            snapshot["trend"] = "up"
        elif pct < -1.5:
            snapshot["trend"] = "sharp decline"
        elif pct < -0.3:
            snapshot["trend"] = "down"
        else:
            snapshot["trend"] = "flat"
    else:
        snapshot["trend"] = "unavailable"

    # Pass debug through
    if debug and coffee_result.get("debug"):
        snapshot["debug"] = coffee_result["debug"]

    return snapshot
