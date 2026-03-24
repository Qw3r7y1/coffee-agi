"""
Coffee Buying Signal for Maillard Coffee Roasters.

Simple, actionable output for the owner:
  - Market direction (UP / DOWN / STABLE)
  - Confidence (LOW / MEDIUM / HIGH)
  - Recommendation (BUY NOW / WAIT / MONITOR)
  - Reason (1-2 lines, no jargon)

Readable in under 15 seconds.
"""
from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger

from maillard.mcp.analyst.market_data import (
    get_coffee_futures_quote,
    get_coffee_futures_history,
)
from maillard.mcp.analyst.market_data_engine import (
    get_validated_coffee_data,
    STATE_VALIDATED,
    STATE_WARNING,
    MODE_SAFE,
)


async def get_buying_signal(days: int = 14) -> dict:
    """Generate a simple buying signal for the cafe owner.

    Fetches current price + recent history, computes trend,
    and returns a clear BUY/WAIT/MONITOR recommendation.
    """
    now = datetime.now(timezone.utc)

    # ── Fetch current price via engine ────────────────────────────────────
    engine = await get_validated_coffee_data(mode=MODE_SAFE)
    current_price = engine.get("api_price")  # $/lb
    state = engine.get("market_state", "INVALID_DATA")
    norm = engine.get("api_normalization", {})
    current_cents = norm.get("price_cents_lb")

    if current_price is None:
        return {
            "direction": "UNKNOWN",
            "confidence": "LOW",
            "recommendation": "MONITOR",
            "reason": "Live market data unavailable. Check feed and retry.",
            "price": None,
            "formatted": _format("UNKNOWN", "LOW", "MONITOR",
                                 "Live data unavailable.", None, None),
        }

    # ── Fetch history for trend ───────────────────────────────────────────
    history = await get_coffee_futures_history(days=days)
    bars = history.get("bars", [])

    # Compute trend from history
    trend = _compute_trend(bars, current_cents)

    # ── Determine direction ───────────────────────────────────────────────
    direction = trend["direction"]
    change_pct = trend["change_pct"]

    # ── Confidence from data quality + trend clarity ──────────────────────
    confidence = _compute_confidence(state, trend, len(bars))

    # ── Recommendation ────────────────────────────────────────────────────
    recommendation, reason = _compute_recommendation(direction, confidence, change_pct, trend)

    # ── Build result ──────────────────────────────────────────────────────
    result = {
        "direction": direction,
        "confidence": confidence,
        "recommendation": recommendation,
        "reason": reason,
        "price_dollars_lb": round(current_price, 4),
        "price_cents_lb": round(current_cents, 2) if current_cents else None,
        "change_pct": change_pct,
        "trend_period_days": days,
        "data_state": state,
        "bars_analyzed": len(bars),
        "generated_at": now.isoformat(),
        "formatted": _format(direction, confidence, recommendation, reason,
                             current_price, current_cents),
    }

    logger.info(
        f"[BUYING-SIGNAL] {direction} | {confidence} | {recommendation} | "
        f"${current_price:.4f}/lb | {change_pct:+.1f}% ({days}d)"
    )

    return result


# =============================================================================
# TREND COMPUTATION
# =============================================================================


def _compute_trend(bars: list[dict], current_cents: float | None) -> dict:
    """Compute market trend from OHLC bars.

    Returns direction (UP/DOWN/STABLE), change %, and trend details.
    """
    if not bars or len(bars) < 2:
        return {"direction": "UNKNOWN", "change_pct": 0, "detail": "Insufficient history"}

    # Bars are newest-first from Yahoo
    newest_close = bars[0].get("close", 0)
    oldest_close = bars[-1].get("close", 0)

    if oldest_close <= 0:
        return {"direction": "UNKNOWN", "change_pct": 0, "detail": "Invalid historical data"}

    change_pct = round((newest_close - oldest_close) / oldest_close * 100, 1)

    # Recent momentum: last 5 bars vs previous 5
    if len(bars) >= 10:
        recent_avg = sum(b["close"] for b in bars[:5]) / 5
        older_avg = sum(b["close"] for b in bars[5:10]) / 5
        momentum = (recent_avg - older_avg) / older_avg * 100
    else:
        momentum = change_pct

    # Direction classification
    if change_pct > 3:
        direction = "UP"
        if momentum > 2:
            detail = f"Rising {change_pct:+.1f}% with accelerating momentum"
        else:
            detail = f"Up {change_pct:+.1f}% but momentum slowing"
    elif change_pct < -3:
        direction = "DOWN"
        if momentum < -2:
            detail = f"Falling {change_pct:+.1f}% with accelerating decline"
        else:
            detail = f"Down {change_pct:+.1f}% but decline slowing"
    else:
        direction = "STABLE"
        detail = f"Sideways ({change_pct:+.1f}%), no clear trend"

    # Period high/low
    highs = [b.get("high", 0) for b in bars if b.get("high")]
    lows = [b.get("low", 0) for b in bars if b.get("low")]
    period_high = max(highs) if highs else 0
    period_low = min(lows) if lows else 0

    return {
        "direction": direction,
        "change_pct": change_pct,
        "momentum_pct": round(momentum, 1),
        "period_high_cents": round(period_high, 2),
        "period_low_cents": round(period_low, 2),
        "newest_close_cents": round(newest_close, 2),
        "oldest_close_cents": round(oldest_close, 2),
        "detail": detail,
    }


# =============================================================================
# CONFIDENCE
# =============================================================================


def _compute_confidence(state: str, trend: dict, bar_count: int) -> str:
    """Determine confidence level from data quality and trend clarity."""
    score = 0

    # Data quality
    if state == STATE_VALIDATED:
        score += 3
    elif state == STATE_WARNING:
        score += 1

    # Enough history
    if bar_count >= 14:
        score += 2
    elif bar_count >= 7:
        score += 1

    # Trend clarity (strong move = clearer signal)
    change = abs(trend.get("change_pct", 0))
    if change > 5:
        score += 3
    elif change > 2:
        score += 2
    elif change > 0.5:
        score += 1

    # Momentum alignment (momentum same direction as trend = stronger)
    momentum = trend.get("momentum_pct", 0)
    change_pct = trend.get("change_pct", 0)
    if (momentum > 0 and change_pct > 0) or (momentum < 0 and change_pct < 0):
        score += 1

    if score >= 7:
        return "HIGH"
    elif score >= 4:
        return "MEDIUM"
    return "LOW"


# =============================================================================
# RECOMMENDATION
# =============================================================================


def _compute_recommendation(
    direction: str, confidence: str, change_pct: float, trend: dict,
) -> tuple[str, str]:
    """Generate BUY NOW / WAIT / MONITOR recommendation with reason."""

    momentum = trend.get("momentum_pct", 0)

    # ── UP market ────────────────────────────────────────────────────────
    if direction == "UP":
        if confidence == "HIGH":
            return (
                "BUY NOW",
                f"Prices rising {change_pct:+.1f}% over the period with strong momentum. "
                f"Delaying increases your cost."
            )
        if confidence == "MEDIUM":
            if momentum > 0:
                return (
                    "BUY NOW",
                    f"Prices up {change_pct:+.1f}% and still climbing. "
                    f"Waiting likely means higher prices."
                )
            return (
                "MONITOR",
                f"Prices up {change_pct:+.1f}% but momentum fading. "
                f"May stabilize soon. Watch for 2-3 more days."
            )
        return (
            "MONITOR",
            f"Prices trending up {change_pct:+.1f}% but data confidence is low. "
            f"Verify with supplier before committing."
        )

    # ── DOWN market ──────────────────────────────────────────────────────
    if direction == "DOWN":
        if confidence == "HIGH":
            if momentum < -2:
                return (
                    "WAIT",
                    f"Prices falling {change_pct:+.1f}% with accelerating decline. "
                    f"Waiting may save you money."
                )
            return (
                "MONITOR",
                f"Prices down {change_pct:+.1f}% but decline slowing. "
                f"Could be near bottom. Watch closely."
            )
        if confidence == "MEDIUM":
            return (
                "WAIT",
                f"Prices dropping {change_pct:+.1f}%. "
                f"No rush to buy unless stock is critically low."
            )
        return (
            "MONITOR",
            f"Prices appear to be dropping but data confidence is low. "
            f"Check again tomorrow."
        )

    # ── STABLE market ────────────────────────────────────────────────────
    if confidence in ("HIGH", "MEDIUM"):
        return (
            "BUY NOW",
            f"Market stable ({change_pct:+.1f}%). "
            f"Good time to buy at current levels. No rush, but no reason to wait."
        )
    return (
        "MONITOR",
        f"Market appears stable but data confidence is low. "
        f"Confirm price with supplier."
    )


# =============================================================================
# FORMAT
# =============================================================================


def _format(direction: str, confidence: str, recommendation: str, reason: str,
            price_dollars: float | None, price_cents: float | None) -> str:
    L = []
    L.append("Coffee Market Signal -- Maillard")
    L.append("=" * 35)

    if price_dollars:
        L.append(f"Price: ${price_dollars:.4f}/lb ({price_cents:.0f} c/lb)")
    else:
        L.append("Price: unavailable")

    L.append(f"Market: {direction}")
    L.append(f"Confidence: {confidence}")
    L.append("")
    L.append(f"Recommendation: {recommendation}")
    L.append("")
    L.append(f"Reason:")
    L.append(f"  {reason}")
    L.append("=" * 35)
    return "\n".join(L)
