"""
Market Data Engine -- production-grade coffee futures data pipeline.

Canonical unit: USD per pound ($/lb).
All prices are normalized to $/lb before any validation or comparison.

Pipeline:
  1. Fetch raw API data
  2. Detect unit from raw price
  3. Normalize to $/lb
  4. Validate plausibility
  5. Fetch reference (if available)
  6. Compare sources
  7. Score source reliability
  8. Classify market state

Symbol mapping:
  - Yahoo Finance: KC=F  (ICE Coffee C Futures, returns cents/lb, currency=USX)
  - Twelve Data:   KC    (BROKEN -- resolves to Kingsoft Cloud NASDAQ stock)
  - Investing.com: reference benchmark (user-provided URL, typically cents/lb)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from loguru import logger

from maillard.mcp.analyst.providers import get_market_provider

# ── Canonical unit ───────────────────────────────────────────────────────────
# Everything normalizes to DOLLARS PER POUND.
# Coffee C historically trades ~$1.00-$8.00/lb (= 100-800 cents/lb).
CANONICAL_UNIT = "$/lb"

# Plausibility bounds in $/lb
PRICE_MIN_DOLLARS = 0.30   # 30 cents/lb -- extreme historical low
PRICE_MAX_DOLLARS = 8.00   # 800 cents/lb -- extreme high

# ── Unit detection thresholds ────────────────────────────────────────────────
# These distinguish cents/lb from $/lb from garbage
_CENTS_RANGE = (30.0, 800.0)    # if raw price is in this range -> cents/lb
_DOLLARS_RANGE = (0.30, 8.00)   # if raw price is in this range -> $/lb
_ERROR_FLOOR = 0.01             # below this -> error

# ── Source reliability ───────────────────────────────────────────────────────
_RELIABILITY_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "analyst" / "source_reliability.json"
_MAX_HISTORY = 10  # rolling window per source

# Sources permanently blacklisted for coffee futures (audit 2026-03-18).
# These providers resolve "KC" to wrong instruments.
# They are still valid for OTHER data (e.g., Twelve Data for FX rates).
COFFEE_BLACKLISTED_SOURCES = {
    "twelvedata": "KC resolves to Kingsoft Cloud Holdings (NASDAQ ADR), not ICE Coffee C. "
                  "Twelve Data does not carry ICE commodity futures on any tier.",
}

# ── Known symbol issues ──────────────────────────────────────────────────────
KNOWN_BAD_NAMES = ["kingsoft", "nasdaq", "holdings"]

# ── Staleness ────────────────────────────────────────────────────────────────
STALE_THRESHOLD_MINUTES = 120
ABNORMAL_DAILY_MOVE_PCT = 8.0

# ── Conflict ─────────────────────────────────────────────────────────────────
DEFAULT_CONFLICT_THRESHOLD_PCT = 10.0

# ── States ───────────────────────────────────────────────────────────────────
STATE_VALIDATED = "VALIDATED"
STATE_WARNING = "WARNING"
STATE_FEED_CONFLICT = "FEED_CONFLICT"
STATE_INVALID_DATA = "INVALID_DATA"

# ── Decision modes ───────────────────────────────────────────────────────────
# Controls how aggressively the analyst issues execution advice.
MODE_SAFE = "SAFE"            # default: no execution advice unless fully validated
MODE_CAUTIOUS = "CAUTIOUS"    # allow directional guidance, still block on conflict
MODE_AGGRESSIVE = "AGGRESSIVE" # allow execution suggestions even with minor uncertainty
VALID_MODES = (MODE_SAFE, MODE_CAUTIOUS, MODE_AGGRESSIVE)


# =============================================================================
# PART 1 -- UNIT DETECTION
# =============================================================================


def detect_unit(price: float) -> dict:
    """Detect whether a raw price is in cents/lb or $/lb.

    Rules:
      price < 0.01          -> error (garbage)
      0.01  <= price < 10   -> ambiguous low range, likely $/lb
      10    <= price < 30    -> ambiguous (could be low cents or high dollars)
      30    <= price <= 800  -> cents/lb (high confidence)
      0.30  <= price <= 8.0  -> $/lb (but overlaps with cents if 30-800)
      price > 800            -> error (out of range)

    The key insight: Yahoo Finance KC=F returns values like 292.55,
    which are CENTS per pound (currency=USX). A value of 2.92 would
    be DOLLARS per pound.

    Returns:
        {
            "raw_price": float,
            "detected_unit": "cents/lb" | "$/lb" | "unknown",
            "confidence": "high" | "medium" | "low",
            "reasoning": str,
        }
    """
    if price < _ERROR_FLOOR:
        return {
            "raw_price": price,
            "detected_unit": "unknown",
            "confidence": "low",
            "reasoning": f"Price {price} is below {_ERROR_FLOOR} -- likely error or zero",
        }

    if price > 800:
        return {
            "raw_price": price,
            "detected_unit": "unknown",
            "confidence": "low",
            "reasoning": f"Price {price} exceeds 800 -- out of any plausible coffee range",
        }

    # Clear cents range: 30-800
    if _CENTS_RANGE[0] <= price <= _CENTS_RANGE[1]:
        return {
            "raw_price": price,
            "detected_unit": "cents/lb",
            "confidence": "high",
            "reasoning": f"Price {price} is in {_CENTS_RANGE[0]}-{_CENTS_RANGE[1]} range -- cents/lb",
        }

    # Clear dollars range below cents floor: 0.30 - 29.99
    if _DOLLARS_RANGE[0] <= price < _CENTS_RANGE[0]:
        # If price < 10, almost certainly dollars
        if price < 10:
            return {
                "raw_price": price,
                "detected_unit": "$/lb",
                "confidence": "high" if price >= 1.0 else "medium",
                "reasoning": f"Price {price} is in $0.30-$10.00 range -- $/lb",
            }
        # 10-30 is ambiguous
        return {
            "raw_price": price,
            "detected_unit": "$/lb",
            "confidence": "low",
            "reasoning": f"Price {price} is ambiguous (10-30 range) -- guessing $/lb but low confidence",
        }

    # Below dollar floor but above error floor
    if _ERROR_FLOOR <= price < _DOLLARS_RANGE[0]:
        return {
            "raw_price": price,
            "detected_unit": "unknown",
            "confidence": "low",
            "reasoning": f"Price {price} is below minimum plausible coffee price -- likely wrong instrument",
        }

    return {
        "raw_price": price,
        "detected_unit": "unknown",
        "confidence": "low",
        "reasoning": f"Price {price} does not fit any expected range",
    }


# =============================================================================
# PART 2 -- PRICE NORMALIZATION
# =============================================================================


def normalize_price(raw_price: float, unit_type: str) -> dict:
    """Normalize a price to the canonical $/lb.

    Args:
        raw_price: The raw price value from the provider.
        unit_type: "cents/lb", "$/lb", or "unknown".

    Returns:
        {
            "price_dollars_lb": float | None,
            "price_cents_lb": float | None,
            "normalized": bool,
            "unit_input": str,
            "unit_output": "$/lb",
            "conversion_applied": str,
        }
    """
    if unit_type == "cents/lb":
        dollars = round(raw_price / 100.0, 4)
        return {
            "price_dollars_lb": dollars,
            "price_cents_lb": round(raw_price, 2),
            "normalized": True,
            "unit_input": "cents/lb",
            "unit_output": CANONICAL_UNIT,
            "conversion_applied": f"{raw_price} cents / 100 = ${dollars}/lb",
        }

    if unit_type == "$/lb":
        cents = round(raw_price * 100.0, 2)
        return {
            "price_dollars_lb": round(raw_price, 4),
            "price_cents_lb": cents,
            "normalized": True,
            "unit_input": "$/lb",
            "unit_output": CANONICAL_UNIT,
            "conversion_applied": f"${raw_price}/lb (no conversion needed)",
        }

    # Unknown unit -- cannot safely normalize
    return {
        "price_dollars_lb": None,
        "price_cents_lb": None,
        "normalized": False,
        "unit_input": "unknown",
        "unit_output": CANONICAL_UNIT,
        "conversion_applied": "FAILED -- unit could not be determined",
    }


# =============================================================================
# PART 3 -- VALIDATION (price in $/lb after normalization)
# =============================================================================


def validate_normalized(price_dollars: float | None, raw_data: dict) -> dict:
    """Validate a normalized $/lb price and its raw data.

    Returns:
        {
            "is_valid": bool,
            "issues": [str],
            "severity": "low" | "medium" | "high",
            "checks": {...},
        }
    """
    issues: list[str] = []
    checks = {
        "symbol_ok": True,
        "price_plausible": True,
        "data_fresh": True,
        "move_normal": True,
    }

    if not raw_data or "error" in raw_data:
        return {
            "is_valid": False,
            "issues": [f"Data unavailable: {(raw_data or {}).get('error', 'no data')}"],
            "severity": "high",
            "checks": {k: False for k in checks},
        }

    # Symbol check
    name_lower = raw_data.get("name", "").lower()
    exchange_lower = raw_data.get("exchange", "").lower()
    for bad in KNOWN_BAD_NAMES:
        if bad in name_lower or bad in exchange_lower:
            checks["symbol_ok"] = False
            issues.append(f"Symbol mismatch: '{raw_data.get('name')}' on '{raw_data.get('exchange', '?')}'")
            break

    # Price plausibility (in $/lb)
    if price_dollars is None:
        checks["price_plausible"] = False
        issues.append("Price normalization failed -- cannot validate")
    elif price_dollars <= 0:
        checks["price_plausible"] = False
        issues.append(f"Normalized price ${price_dollars}/lb is zero or negative")
    elif not (PRICE_MIN_DOLLARS <= price_dollars <= PRICE_MAX_DOLLARS):
        checks["price_plausible"] = False
        issues.append(
            f"Normalized price ${price_dollars:.4f}/lb outside plausible range "
            f"(${PRICE_MIN_DOLLARS}-${PRICE_MAX_DOLLARS}/lb)"
        )

    # Staleness
    ts_str = raw_data.get("timestamp") or raw_data.get("fetched_at")
    if ts_str:
        try:
            ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - ts
            if age > timedelta(minutes=STALE_THRESHOLD_MINUTES):
                checks["data_fresh"] = False
                issues.append(f"Data is {age.total_seconds()/3600:.1f}h old (threshold: {STALE_THRESHOLD_MINUTES}min)")
        except (ValueError, TypeError):
            pass
    else:
        checks["data_fresh"] = False
        issues.append("No timestamp -- cannot verify freshness")

    # Abnormal move
    change = raw_data.get("change_percent", 0)
    if abs(change) > ABNORMAL_DAILY_MOVE_PCT:
        checks["move_normal"] = False
        issues.append(f"Abnormal daily move: {change:+.1f}% (threshold: {ABNORMAL_DAILY_MOVE_PCT}%)")

    # Severity
    critical = ["symbol_ok", "price_plausible"]
    has_critical = any(not checks[c] for c in critical)
    has_warning = any(not checks[c] for c in checks if c not in critical)

    if has_critical:
        return {"is_valid": False, "issues": issues, "severity": "high", "checks": checks}
    if has_warning:
        return {"is_valid": True, "issues": issues, "severity": "medium", "checks": checks}
    return {"is_valid": True, "issues": issues, "severity": "low", "checks": checks}


# =============================================================================
# PART 4 -- SOURCE RELIABILITY TRACKING & SCORING
# =============================================================================
#
# Each source stores up to _MAX_HISTORY event records, not just scores.
# An event record captures everything needed to compute reliability:
#   - timestamp, score, valid, fresh, unit_ok, conflict, deviation_pct
#
# From these records we derive:
#   - rolling score (0-100)
#   - valid_rate (% of calls that passed validation)
#   - conflict_rate (% of calls that detected a feed conflict)
#   - avg_deviation (mean % difference vs reference when both available)
#   - stability flag (stable / degrading / unstable)


def _load_reliability() -> dict:
    """Load full reliability history from disk."""
    try:
        if _RELIABILITY_FILE.exists():
            return json.loads(_RELIABILITY_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_reliability(data: dict) -> None:
    """Persist reliability history to disk."""
    try:
        _RELIABILITY_FILE.parent.mkdir(parents=True, exist_ok=True)
        _RELIABILITY_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"[ENGINE] Could not save reliability: {e}")


def _compute_call_score(
    validation_passed: bool,
    data_fresh: bool,
    unit_detected_ok: bool,
    historical_avg: float,
) -> tuple[int, dict]:
    """Compute a single-call score 0-100 with breakdown."""
    score = 0
    bd = {}

    bd["validation"] = 40 if validation_passed else 0
    score += bd["validation"]

    bd["freshness"] = 25 if data_fresh else 0
    score += bd["freshness"]

    bd["unit_detection"] = 20 if unit_detected_ok else 0
    score += bd["unit_detection"]

    hist_bonus = round(historical_avg / 100.0 * 15) if historical_avg > 0 else 0
    bd["historical"] = hist_bonus
    score += hist_bonus

    return min(score, 100), bd


def _derive_stats(events: list[dict]) -> dict:
    """Derive aggregate statistics from a list of event records."""
    if not events:
        return {
            "rolling_avg": 0.0,
            "valid_rate": 0.0,
            "conflict_rate": 0.0,
            "avg_deviation_pct": None,
            "stability": "unknown",
            "total_events": 0,
        }

    n = len(events)
    scores = [e.get("score", 0) for e in events]
    rolling_avg = round(sum(scores) / n, 1)

    valid_count = sum(1 for e in events if e.get("valid"))
    valid_rate = round(valid_count / n * 100, 1)

    conflict_count = sum(1 for e in events if e.get("conflict"))
    conflict_rate = round(conflict_count / n * 100, 1)

    deviations = [e["deviation_pct"] for e in events if e.get("deviation_pct") is not None]
    avg_dev = round(sum(deviations) / len(deviations), 2) if deviations else None

    # Stability: compare recent half vs older half
    if n >= 4:
        mid = n // 2
        older_avg = sum(scores[:mid]) / mid
        newer_avg = sum(scores[mid:]) / (n - mid)
        if newer_avg >= older_avg - 5:
            stability = "stable"
        elif newer_avg >= older_avg - 20:
            stability = "degrading"
        else:
            stability = "unstable"
    elif n >= 2:
        stability = "stable" if scores[-1] >= scores[0] - 10 else "degrading"
    else:
        stability = "unknown"

    return {
        "rolling_avg": rolling_avg,
        "valid_rate": valid_rate,
        "conflict_rate": conflict_rate,
        "avg_deviation_pct": avg_dev,
        "stability": stability,
        "total_events": n,
    }


def score_source(
    source_name: str,
    validation_passed: bool,
    data_fresh: bool,
    unit_detected_ok: bool,
    asset_class: str = "coffee",
    conflict: bool = False,
    deviation_pct: float | None = None,
) -> dict:
    """Score a data source 0-100, record event, and compute rolling stats.

    Per-call scoring:
      - Valid data:     +40
      - Fresh data:     +25
      - Unit detected:  +20
      - Historical avg: +15 (from rolling window)

    Blacklisted sources permanently score 0.

    Args:
        source_name: Provider identifier (e.g., "yahoo_finance").
        validation_passed: Did the data pass validation?
        data_fresh: Is the data within staleness threshold?
        unit_detected_ok: Was the unit detected with confidence?
        asset_class: "coffee" or "fx" (blacklist is per-asset).
        conflict: Was there a feed conflict on this call?
        deviation_pct: Percent difference vs reference (None if no ref).

    Returns:
        {
            "source": str,
            "score": int,
            "rolling_avg": float,
            "valid_rate": float,         # % of calls that passed validation
            "conflict_rate": float,      # % of calls with a conflict
            "avg_deviation_pct": float,  # mean % diff vs reference
            "stability": str,            # stable / degrading / unstable
            "history_count": int,
            "breakdown": {...},
            "blacklisted": bool,
            "blacklist_reason": str | None,
        }
    """
    now = datetime.now(timezone.utc).isoformat()

    # ── Blacklist check ──────────────────────────────────────────────────
    if asset_class == "coffee" and source_name in COFFEE_BLACKLISTED_SOURCES:
        reason = COFFEE_BLACKLISTED_SOURCES[source_name]
        logger.warning(f"[SCORING] {source_name} is BLACKLISTED for coffee: {reason[:80]}")

        history = _load_reliability()
        events = history.get(source_name, [])
        events.append({
            "ts": now, "score": 0, "valid": False, "fresh": False,
            "unit_ok": False, "conflict": False, "deviation_pct": None,
        })
        if len(events) > _MAX_HISTORY:
            events = events[-_MAX_HISTORY:]
        history[source_name] = events
        _save_reliability(history)

        return {
            "source": source_name,
            "score": 0,
            "rolling_avg": 0.0,
            "valid_rate": 0.0,
            "conflict_rate": 0.0,
            "avg_deviation_pct": None,
            "stability": "blacklisted",
            "history_count": len(events),
            "breakdown": {"validation": 0, "freshness": 0, "unit_detection": 0, "historical": 0},
            "blacklisted": True,
            "blacklist_reason": reason,
        }

    # ── Normal scoring ───────────────────────────────────────────────────
    history = _load_reliability()
    events = history.get(source_name, [])

    # Compute historical avg from existing events for the bonus
    prev_scores = [e.get("score", 0) for e in events] if events else []
    historical_avg = sum(prev_scores) / len(prev_scores) if prev_scores else 0

    call_score, breakdown = _compute_call_score(
        validation_passed, data_fresh, unit_detected_ok, historical_avg,
    )

    # Record the event
    event = {
        "ts": now,
        "score": call_score,
        "valid": validation_passed,
        "fresh": data_fresh,
        "unit_ok": unit_detected_ok,
        "conflict": conflict,
        "deviation_pct": round(deviation_pct, 2) if deviation_pct is not None else None,
    }
    events.append(event)
    if len(events) > _MAX_HISTORY:
        events = events[-_MAX_HISTORY:]
    history[source_name] = events
    _save_reliability(history)

    # Derive stats from full window
    stats = _derive_stats(events)

    logger.info(
        f"[SCORING] {source_name}: score={call_score} "
        f"avg={stats['rolling_avg']} valid={stats['valid_rate']}% "
        f"conflict={stats['conflict_rate']}% stability={stats['stability']}"
    )

    return {
        "source": source_name,
        "score": call_score,
        "rolling_avg": stats["rolling_avg"],
        "valid_rate": stats["valid_rate"],
        "conflict_rate": stats["conflict_rate"],
        "avg_deviation_pct": stats["avg_deviation_pct"],
        "stability": stats["stability"],
        "history_count": stats["total_events"],
        "breakdown": breakdown,
        "blacklisted": False,
        "blacklist_reason": None,
    }


def get_source_reliability() -> dict:
    """Get full reliability profile for all tracked sources.

    Returns:
        {
            "source_name": {
                "rolling_avg": float,
                "valid_rate": float,
                "conflict_rate": float,
                "avg_deviation_pct": float | None,
                "stability": str,
                "last_score": int,
                "total_events": int,
                "blacklisted": bool,
                "events": [...]   # raw event history
            },
            ...
        }
    """
    history = _load_reliability()
    result = {}
    for source, events in history.items():
        if not events:
            continue
        stats = _derive_stats(events)
        last = events[-1] if events else {}
        result[source] = {
            **stats,
            "last_score": last.get("score", 0),
            "blacklisted": source in COFFEE_BLACKLISTED_SOURCES,
            "events": events,
        }
    return result


# Keep backward-compat alias
get_source_scores = get_source_reliability


# =============================================================================
# PART 5 -- COMPARE SOURCES (in canonical $/lb)
# =============================================================================


def compare_sources_dollars(
    api_dollars: float | None,
    ref_dollars: float | None,
    threshold_pct: float = DEFAULT_CONFLICT_THRESHOLD_PCT,
) -> dict:
    """Compare two prices in $/lb."""
    has_api = api_dollars is not None and api_dollars > 0
    has_ref = ref_dollars is not None and ref_dollars > 0

    if not has_api and not has_ref:
        return {"percent_difference": None, "is_conflict": False, "dominant_source": "none",
                "confidence_level": "low", "status": "no_data"}

    if has_api and not has_ref:
        return {"percent_difference": None, "is_conflict": False, "dominant_source": "api",
                "confidence_level": "medium", "status": "api_only"}

    if not has_api and has_ref:
        return {"percent_difference": None, "is_conflict": False, "dominant_source": "reference",
                "confidence_level": "medium", "status": "reference_only"}

    diff_pct = abs(api_dollars - ref_dollars) / ref_dollars * 100
    is_conflict = diff_pct > threshold_pct

    if is_conflict:
        return {"percent_difference": round(diff_pct, 1), "is_conflict": True,
                "dominant_source": "reference", "confidence_level": "low", "status": "conflict"}
    elif diff_pct > threshold_pct / 2:
        return {"percent_difference": round(diff_pct, 1), "is_conflict": False,
                "dominant_source": "reference", "confidence_level": "medium", "status": "aligned"}
    else:
        return {"percent_difference": round(diff_pct, 1), "is_conflict": False,
                "dominant_source": "reference", "confidence_level": "high", "status": "aligned"}


# =============================================================================
# PART 6 -- CLASSIFY MARKET STATE
# =============================================================================


def classify_state(
    validation: dict,
    comparison: dict,
    unit_confidence: str,
    api_reliability: dict | None = None,
    mode: str = MODE_SAFE,
) -> dict:
    """Classify market state and determine decision grade based on mode.

    Modes control how aggressively execution advice is given:

    SAFE (default):
      - decision_grade=True ONLY when state=VALIDATED
      - any uncertainty blocks execution advice

    CAUTIOUS:
      - decision_grade=True when VALIDATED
      - WARNING allows directional guidance (decision_grade="directional")
      - FEED_CONFLICT still blocks execution advice

    AGGRESSIVE:
      - VALIDATED and WARNING both get decision_grade=True
      - FEED_CONFLICT allows directional guidance
      - only INVALID_DATA fully blocks

    All modes: INVALID_DATA always blocks everything.
    """
    if mode not in VALID_MODES:
        mode = MODE_SAFE

    ds = comparison.get("dominant_source", "api")

    # ── INVALID_DATA: always blocks regardless of mode ───────────────────
    if not validation.get("is_valid") and validation.get("severity") == "high":
        return {
            "state": STATE_INVALID_DATA,
            "decision_grade": False,
            "mode": mode,
            "display_source": "none",
            "action_guidance": "Data failed critical validation. DO NOT use for decisions.",
        }

    # ── FEED_CONFLICT ────────────────────────────────────────────────────
    if comparison.get("is_conflict"):
        diff = comparison.get("percent_difference", 0)
        if mode == MODE_AGGRESSIVE:
            # Aggressive: allow directional guidance even on conflict
            return {
                "state": STATE_FEED_CONFLICT,
                "decision_grade": "directional",
                "mode": mode,
                "display_source": "both",
                "action_guidance": (
                    f"Sources diverge by {diff}%. AGGRESSIVE mode: directional guidance permitted, "
                    f"but execution requires manual broker confirmation."
                ),
            }
        # SAFE and CAUTIOUS: block on conflict
        return {
            "state": STATE_FEED_CONFLICT,
            "decision_grade": False,
            "mode": mode,
            "display_source": "both",
            "action_guidance": f"Sources diverge by {diff}%. Do NOT issue execution advice.",
        }

    # ── Determine base state from validation quality ─────────────────────
    # Collect all warning reasons to pick the right state
    warning_reason = None

    if unit_confidence == "low":
        warning_reason = "Unit detection uncertain. Treat data as directional only."

    elif validation.get("severity") == "medium":
        warning_reason = "Data has quality warnings. Provide cautious interpretation."

    elif api_reliability and not api_reliability.get("blacklisted"):
        avg = api_reliability.get("rolling_avg", 100)
        stability = api_reliability.get("stability", "stable")
        conflict_rate = api_reliability.get("conflict_rate", 0)

        if avg < 40 or stability == "unstable":
            warning_reason = (
                f"Source reliability low (avg {avg}/100, {stability}). "
                f"Data looks OK now but source has a poor track record."
            )
        elif conflict_rate > 30:
            warning_reason = (
                f"Source has {conflict_rate}% conflict rate. "
                f"Data may be unreliable. Verify with reference."
            )

    # ── WARNING state ────────────────────────────────────────────────────
    if warning_reason:
        if mode == MODE_AGGRESSIVE:
            # Aggressive: treat warnings as acceptable, grant full decision grade
            return {
                "state": STATE_WARNING,
                "decision_grade": True,
                "mode": mode,
                "display_source": ds,
                "action_guidance": (
                    f"AGGRESSIVE mode: {warning_reason} "
                    f"Proceeding with execution guidance despite warnings."
                ),
            }
        if mode == MODE_CAUTIOUS:
            # Cautious: allow directional guidance on warnings
            return {
                "state": STATE_WARNING,
                "decision_grade": "directional",
                "mode": mode,
                "display_source": ds,
                "action_guidance": (
                    f"{warning_reason} "
                    f"CAUTIOUS mode: directional guidance permitted, execution blocked."
                ),
            }
        # SAFE: block everything on warning
        return {
            "state": STATE_WARNING,
            "decision_grade": False,
            "mode": mode,
            "display_source": ds,
            "action_guidance": warning_reason,
        }

    # ── VALIDATED ────────────────────────────────────────────────────────
    return {
        "state": STATE_VALIDATED,
        "decision_grade": True,
        "mode": mode,
        "display_source": ds,
        "action_guidance": "Data validated. Safe for procurement guidance.",
    }


# =============================================================================
# PART 7 -- MAIN PIPELINE
# =============================================================================


async def get_validated_coffee_data(
    reference_price_raw: float | None = None,
    reference_unit: str | None = None,
    reference_source_name: str = "Investing.com",
    conflict_threshold_pct: float = DEFAULT_CONFLICT_THRESHOLD_PCT,
    mode: str = MODE_SAFE,
    debug: bool = False,
) -> dict:
    """Main pipeline: fetch, detect, normalize, validate, compare, score, classify.

    Args:
        reference_price_raw: Raw price from reference source (any unit).
        reference_unit: Unit hint for reference ("cents/lb" or "$/lb"). Auto-detected if None.
        reference_source_name: Label for the reference source.
        conflict_threshold_pct: Conflict threshold.
        mode: Decision mode -- "SAFE", "CAUTIOUS", or "AGGRESSIVE".
        debug: Include raw debug info in output.

    Returns:
        {
            "api_price": float ($/lb) or None,
            "reference_price": float ($/lb) or None,
            "normalized": bool,
            "unit": "$/lb",
            "validation": {...},
            "comparison": {...},
            "source_scores": {...},
            "market_state": str,
            "decision_grade": bool | "directional",
            "mode": str,
            "integrity_report": str,
            "debug": {...} | None,
        }
    """
    if mode not in VALID_MODES:
        mode = MODE_SAFE
    now = datetime.now(timezone.utc).isoformat()
    debug_info: dict = {} if debug else None

    # ── Step 1: Fetch raw API data ───────────────────────────────────────
    provider = get_market_provider()
    raw_api = await provider.get_price("KC")
    api_has_error = "error" in raw_api

    if debug:
        debug_info["raw_api_response"] = raw_api

    # ── Step 2: Detect unit ──────────────────────────────────────────────
    if not api_has_error:
        raw_price = raw_api.get("price", 0)
        api_unit = detect_unit(raw_price)

        # Provider hint: Yahoo Finance currency=USX means cents
        provider_currency = raw_api.get("currency", "")
        if provider_currency.upper() == "USX":
            api_unit["detected_unit"] = "cents/lb"
            api_unit["confidence"] = "high"
            api_unit["reasoning"] += " [confirmed by provider currency=USX]"
    else:
        raw_price = 0
        api_unit = {"raw_price": 0, "detected_unit": "unknown", "confidence": "low",
                     "reasoning": "API returned error"}

    if debug:
        debug_info["unit_detection"] = api_unit

    # ── Step 3: Normalize to $/lb ────────────────────────────────────────
    api_norm = normalize_price(raw_price, api_unit["detected_unit"])
    api_dollars = api_norm["price_dollars_lb"]

    if debug:
        debug_info["normalization"] = api_norm

    # ── Step 4: Validate ─────────────────────────────────────────────────
    validation = validate_normalized(api_dollars, raw_api)

    if debug:
        debug_info["validation"] = validation

    # ── Step 5: Normalize reference price ────────────────────────────────
    ref_dollars = None
    ref_norm = None
    if reference_price_raw is not None:
        ref_unit_hint = reference_unit
        if ref_unit_hint is None:
            ref_detect = detect_unit(reference_price_raw)
            ref_unit_hint = ref_detect["detected_unit"]
        ref_norm = normalize_price(reference_price_raw, ref_unit_hint)
        ref_dollars = ref_norm["price_dollars_lb"]

        if debug:
            debug_info["reference_normalization"] = ref_norm

    # ── Step 6: Compare ──────────────────────────────────────────────────
    comparison = compare_sources_dollars(api_dollars, ref_dollars, conflict_threshold_pct)

    if debug:
        debug_info["comparison"] = comparison

    # ── Step 7: Score sources ────────────────────────────────────────────
    is_conflict = comparison.get("is_conflict", False)
    diff_pct = comparison.get("percent_difference")

    api_score = score_source(
        source_name=raw_api.get("source", "unknown") if not api_has_error else "unknown",
        validation_passed=validation["is_valid"],
        data_fresh=validation.get("checks", {}).get("data_fresh", False),
        unit_detected_ok=api_unit["confidence"] in ("high", "medium"),
        conflict=is_conflict,
        deviation_pct=diff_pct,
    )

    ref_score = None
    if ref_dollars is not None:
        ref_score = score_source(
            source_name=reference_source_name,
            validation_passed=True,  # user-provided, assumed valid
            data_fresh=True,
            unit_detected_ok=ref_norm["normalized"] if ref_norm else False,
            conflict=is_conflict,
            deviation_pct=diff_pct,
        )

    source_scores = {"api": api_score}
    if ref_score:
        source_scores["reference"] = ref_score

    if debug:
        debug_info["source_scores"] = source_scores

    # ── Step 8: Classify (with reliability context + mode) ──────────
    classification = classify_state(
        validation, comparison, api_unit["confidence"],
        api_reliability=api_score,
        mode=mode,
    )

    # ── Build integrity report ───────────────────────────────────────────
    report = _build_engine_report(
        api_dollars=api_dollars,
        api_cents=api_norm["price_cents_lb"],
        api_source=raw_api.get("source", "unknown") if not api_has_error else None,
        api_unit_detection=api_unit,
        api_normalization=api_norm,
        api_score=api_score,
        ref_dollars=ref_dollars,
        ref_cents=ref_norm["price_cents_lb"] if ref_norm else None,
        ref_source=reference_source_name if ref_dollars else None,
        ref_score=ref_score,
        validation=validation,
        comparison=comparison,
        classification=classification,
        raw_api=raw_api,
        mode=mode,
    )

    result = {
        "api_price": api_dollars,
        "reference_price": ref_dollars,
        "normalized": api_norm["normalized"],
        "unit": CANONICAL_UNIT,
        "api_data": raw_api,
        "api_normalization": api_norm,
        "validation": validation,
        "comparison": comparison,
        "source_scores": source_scores,
        "market_state": classification["state"],
        "decision_grade": classification["decision_grade"],
        "mode": mode,
        "classification": classification,
        "integrity_report": report,
        "timestamp": now,
    }

    if debug:
        result["debug"] = debug_info

    logger.info(
        f"[ENGINE] state={classification['state']} grade={'DECISION' if classification['decision_grade'] else 'NON-DECISION'} "
        f"api=${api_dollars}/lb ref=${ref_dollars}/lb score={api_score['score']}"
    )

    return result


# =============================================================================
# INTEGRITY REPORT BUILDER
# =============================================================================


def _build_engine_report(
    *,
    api_dollars, api_cents, api_source, api_unit_detection, api_normalization,
    api_score, ref_dollars, ref_cents, ref_source, ref_score,
    validation, comparison, classification, raw_api, mode=MODE_SAFE,
) -> str:
    """Build the formatted Market Data Engine Output block."""
    L: list[str] = []
    L.append("=" * 50)
    L.append("Market Data Engine Output -- Coffee C")
    L.append("=" * 50)
    L.append("")

    # Normalized Price
    L.append("Normalized Price:")
    if api_dollars is not None:
        L.append(f"  ${api_dollars:.4f}/lb ({api_cents:.2f} cents/lb)")
    else:
        L.append("  UNAVAILABLE")
    L.append("")

    # Unit Handling
    L.append("Unit Handling:")
    det = api_unit_detection
    L.append(f"  Raw value: {det['raw_price']}")
    L.append(f"  Detected: {det['detected_unit']} (confidence: {det['confidence']})")
    norm = api_normalization
    L.append(f"  Conversion: {norm['conversion_applied']}")
    L.append("")

    # API Feed
    L.append("API Feed:")
    if api_dollars is not None:
        L.append(f"  ${api_dollars:.4f}/lb")
        L.append(f"  Source: {api_source}")
        ts = raw_api.get("timestamp") or raw_api.get("fetched_at", "N/A")
        L.append(f"  Timestamp: {ts}")
    else:
        err = raw_api.get("error", "unavailable") if raw_api else "no data"
        L.append(f"  UNAVAILABLE -- {str(err)[:100]}")
    L.append("")

    # Reference Source
    L.append("Reference Source:")
    if ref_dollars is not None:
        L.append(f"  ${ref_dollars:.4f}/lb ({ref_cents:.2f} cents/lb)")
        L.append(f"  Source: {ref_source}")
    else:
        L.append("  NOT PROVIDED")
    L.append("")

    # Difference
    diff = comparison.get("percent_difference")
    L.append("Difference:")
    L.append(f"  {diff:.1f}%" if diff is not None else "  N/A")
    L.append("")

    # Source Reliability
    L.append("Source Reliability:")
    if api_score.get("blacklisted"):
        L.append(f"  API: 0/100 -- BLACKLISTED ({api_score.get('blacklist_reason', '')[:80]})")
    else:
        L.append(f"  API: {api_score['score']}/100 (avg: {api_score['rolling_avg']})")
        L.append(f"    valid: {api_score.get('valid_rate', 0)}% | "
                 f"conflict: {api_score.get('conflict_rate', 0)}% | "
                 f"stability: {api_score.get('stability', '?')}")
        avg_dev = api_score.get("avg_deviation_pct")
        if avg_dev is not None:
            L.append(f"    avg deviation vs reference: {avg_dev}%")
    if ref_score:
        L.append(f"  Reference: {ref_score['score']}/100 (avg: {ref_score['rolling_avg']})")
    L.append("")

    # Market State & Decision Mode
    grade = classification['decision_grade']
    grade_str = "YES" if grade is True else ("DIRECTIONAL ONLY" if grade == "directional" else "NO")
    L.append(f"Decision Mode: {mode}")
    L.append(f"Market State: {classification['state']}")
    L.append(f"Decision Grade: {grade_str}")
    if classification.get("action_guidance"):
        L.append(f"Guidance: {classification['action_guidance']}")
    L.append("")

    # Issues
    issues = validation.get("issues", [])
    L.append("Key Issues:")
    if issues:
        for issue in issues:
            L.append(f"  - {issue}")
    else:
        L.append("  - None detected")
    L.append("")

    L.append(f"Confidence: {comparison.get('confidence_level', 'low').upper()}")
    L.append("=" * 50)

    return "\n".join(L)
