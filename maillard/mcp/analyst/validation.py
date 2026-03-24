"""
Analyst MCP — Market data validation, source comparison, and state classification.

This module implements the data integrity layer for the Maillard Analyst.
No market data passes to decision-makers without going through these checks.

Three-stage pipeline:
  1. validate_market_data()   — plausibility, staleness, unit consistency
  2. compare_sources()        — cross-reference API vs reference benchmark
  3. classify_market_state()  — final classification (VALIDATED / WARNING / FEED_CONFLICT / INVALID_DATA)
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger

# ── Plausibility bounds for ICE Coffee C futures (cents/lb) ──────────────────
# Historical range: ~40 c/lb (1990s lows) to ~440 c/lb (2025 highs).
# Anything outside 30–800 is not coffee.
COFFEE_MIN_CENTS = 30.0
COFFEE_MAX_CENTS = 800.0

# Known non-coffee instruments that resolve from "KC" on various providers
KNOWN_BAD_NAMES = ["kingsoft", "nasdaq", "holdings"]

# Staleness threshold — data older than this is flagged
STALE_THRESHOLD_MINUTES = 120

# Default conflict threshold (percent difference between sources)
DEFAULT_CONFLICT_THRESHOLD_PCT = 10.0

# Abnormal single-day move threshold (percent)
ABNORMAL_DAILY_MOVE_PCT = 8.0


# ═══════════════════════════════════════════════════════════════════════════════
# PART 1 — VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════


def validate_market_data(data: dict) -> dict:
    """Validate a market data result for plausibility and integrity.

    Checks:
      1. Symbol mismatch — is this actually coffee?
      2. Price plausibility — within realistic cents/lb range
      3. Unit consistency — detect dollars/lb vs cents/lb confusion
      4. Missing or stale data
      5. Abnormal daily deviation

    Args:
        data: Raw market data dict from a provider (must have 'price' key).

    Returns:
        {
            "is_valid": bool,
            "issues": [str, ...],
            "severity": "low" | "medium" | "high",
            "checks": {
                "symbol_ok": bool,
                "price_plausible": bool,
                "unit_consistent": bool,
                "data_fresh": bool,
                "move_normal": bool,
            },
        }
    """
    issues: list[str] = []
    checks = {
        "symbol_ok": True,
        "price_plausible": True,
        "unit_consistent": True,
        "data_fresh": True,
        "move_normal": True,
    }

    # ── Check 0: data exists at all ──────────────────────────────────────
    if not data or "error" in data:
        error_msg = data.get("error", "No data") if data else "No data"
        validation_failure = data.get("validation_failure") if data else None
        return {
            "is_valid": False,
            "issues": [f"Data unavailable: {error_msg}"],
            "severity": "high",
            "checks": {k: False for k in checks},
            "validation_failure": validation_failure,
        }

    # ── Check 1: Symbol mismatch ─────────────────────────────────────────
    name_lower = data.get("name", "").lower()
    exchange_lower = data.get("exchange", "").lower()
    for bad in KNOWN_BAD_NAMES:
        if bad in name_lower or bad in exchange_lower:
            checks["symbol_ok"] = False
            issues.append(
                f"Symbol mismatch: resolved to '{data.get('name')}' "
                f"on '{data.get('exchange', '?')}' — not coffee futures"
            )
            logger.error(f"[VALIDATION] SYMBOL MISMATCH: {data.get('name')} / {data.get('exchange')}")
            break

    # ── Check 2: Price plausibility ──────────────────────────────────────
    price = data.get("price", 0)
    if price <= 0:
        checks["price_plausible"] = False
        issues.append(f"Price is zero or negative: {price}")
    elif not (COFFEE_MIN_CENTS <= price <= COFFEE_MAX_CENTS):
        checks["price_plausible"] = False
        issues.append(
            f"Price {price} outside plausible coffee range "
            f"({COFFEE_MIN_CENTS}–{COFFEE_MAX_CENTS} cents/lb)"
        )
        logger.error(f"[VALIDATION] PRICE IMPLAUSIBLE: {price}")

    # ── Check 3: Unit consistency ────────────────────────────────────────
    # If price < 10, it's likely in dollars/lb, not cents/lb
    if 0 < price < 10:
        checks["unit_consistent"] = False
        issues.append(
            f"Price {price} looks like dollars/lb, not cents/lb. "
            f"Expected range {COFFEE_MIN_CENTS}–{COFFEE_MAX_CENTS} cents/lb. "
            f"Possible unit confusion."
        )
        logger.warning(f"[VALIDATION] UNIT SUSPECT: {price} — likely $/lb not c/lb")

    # ── Check 4: Staleness ───────────────────────────────────────────────
    timestamp_str = data.get("timestamp") or data.get("fetched_at")
    if timestamp_str:
        try:
            if isinstance(timestamp_str, str):
                # Handle both ISO format and other formats
                ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                ts = timestamp_str
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - ts
            if age > timedelta(minutes=STALE_THRESHOLD_MINUTES):
                checks["data_fresh"] = False
                hours = age.total_seconds() / 3600
                issues.append(f"Data is {hours:.1f} hours old (stale threshold: {STALE_THRESHOLD_MINUTES} min)")
        except (ValueError, TypeError):
            # Can't parse timestamp — not a critical failure
            pass
    else:
        checks["data_fresh"] = False
        issues.append("No timestamp in data — cannot verify freshness")

    # ── Check 5: Abnormal daily move ─────────────────────────────────────
    change_pct = data.get("change_percent", 0)
    if abs(change_pct) > ABNORMAL_DAILY_MOVE_PCT:
        checks["move_normal"] = False
        issues.append(
            f"Abnormal daily move: {change_pct:+.1f}% "
            f"(threshold: +/-{ABNORMAL_DAILY_MOVE_PCT}%)"
        )
        logger.warning(f"[VALIDATION] ABNORMAL MOVE: {change_pct:+.1f}%")

    # ── Determine overall validity and severity ──────────────────────────
    critical_checks = ["symbol_ok", "price_plausible"]
    warning_checks = ["unit_consistent", "data_fresh", "move_normal"]

    has_critical = any(not checks[c] for c in critical_checks)
    has_warning = any(not checks[c] for c in warning_checks)

    if has_critical:
        severity = "high"
        is_valid = False
    elif has_warning:
        severity = "medium"
        is_valid = True  # usable but with caveats
    else:
        severity = "low"
        is_valid = True

    result = {
        "is_valid": is_valid,
        "issues": issues,
        "severity": severity,
        "checks": checks,
    }

    if issues:
        logger.info(f"[VALIDATION] {severity.upper()}: {issues}")
    else:
        logger.info(f"[VALIDATION] PASSED — price={price} c/lb")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PART 2 — SOURCE COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════


def compare_sources(
    api_price: float | None,
    reference_price: float | None,
    threshold_pct: float = DEFAULT_CONFLICT_THRESHOLD_PCT,
) -> dict:
    """Compare API feed price against reference benchmark price.

    Both prices must be in the same unit (cents/lb).

    Args:
        api_price: Price from internal API feed (cents/lb), or None if unavailable.
        reference_price: Price from reference source like Investing.com (cents/lb), or None.
        threshold_pct: Percent difference that triggers a conflict flag.

    Returns:
        {
            "percent_difference": float | None,
            "is_conflict": bool,
            "dominant_source": "api" | "reference" | "none",
            "confidence_level": "low" | "medium" | "high",
            "status": "aligned" | "conflict" | "api_only" | "reference_only" | "no_data",
        }
    """
    has_api = api_price is not None and api_price > 0
    has_ref = reference_price is not None and reference_price > 0

    # ── No data at all ───────────────────────────────────────────────────
    if not has_api and not has_ref:
        return {
            "percent_difference": None,
            "is_conflict": False,
            "dominant_source": "none",
            "confidence_level": "low",
            "status": "no_data",
        }

    # ── Only API ─────────────────────────────────────────────────────────
    if has_api and not has_ref:
        return {
            "percent_difference": None,
            "is_conflict": False,
            "dominant_source": "api",
            "confidence_level": "medium",
            "status": "api_only",
        }

    # ── Only reference ───────────────────────────────────────────────────
    if not has_api and has_ref:
        return {
            "percent_difference": None,
            "is_conflict": False,
            "dominant_source": "reference",
            "confidence_level": "medium",
            "status": "reference_only",
        }

    # ── Both available — compute difference ──────────────────────────────
    diff_pct = abs(api_price - reference_price) / reference_price * 100
    is_conflict = diff_pct > threshold_pct

    if is_conflict:
        confidence = "low"
        status = "conflict"
        dominant = "reference"  # reference is the benchmark
    elif diff_pct > threshold_pct / 2:
        # Between half-threshold and threshold — notable but not blocking
        confidence = "medium"
        status = "aligned"
        dominant = "reference"
    else:
        confidence = "high"
        status = "aligned"
        dominant = "reference"  # even when aligned, reference is primary

    logger.info(
        f"[COMPARISON] API={api_price:.2f} REF={reference_price:.2f} "
        f"diff={diff_pct:.1f}% conflict={is_conflict} confidence={confidence}"
    )

    return {
        "percent_difference": round(diff_pct, 1),
        "is_conflict": is_conflict,
        "dominant_source": dominant,
        "confidence_level": confidence,
        "status": status,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PART 3 — MARKET STATE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

# Classification constants
STATE_VALIDATED = "VALIDATED"
STATE_WARNING = "WARNING"
STATE_FEED_CONFLICT = "FEED_CONFLICT"
STATE_INVALID_DATA = "INVALID_DATA"


def classify_market_state(
    validation: dict,
    comparison: dict,
) -> dict:
    """Classify the overall market data state from validation and comparison results.

    Args:
        validation: Output of validate_market_data()
        comparison: Output of compare_sources()

    Returns:
        {
            "state": "VALIDATED" | "WARNING" | "FEED_CONFLICT" | "INVALID_DATA",
            "decision_grade": bool,         # safe for procurement/execution decisions?
            "action_guidance": str,          # what the analyst should do with this state
            "display_source": str,           # which source to show as primary
        }
    """
    # ── INVALID_DATA: validation failed critically ───────────────────────
    if not validation.get("is_valid", False) and validation.get("severity") == "high":
        return {
            "state": STATE_INVALID_DATA,
            "decision_grade": False,
            "action_guidance": (
                "Data failed critical validation. DO NOT use for any procurement, "
                "pricing, or execution decisions. Request a reference URL or "
                "confirm prices with broker."
            ),
            "display_source": "none",
        }

    # ── FEED_CONFLICT: sources diverge beyond threshold ──────────────────
    if comparison.get("is_conflict", False):
        diff = comparison.get("percent_difference", 0)
        return {
            "state": STATE_FEED_CONFLICT,
            "decision_grade": False,
            "action_guidance": (
                f"Internal API and reference benchmark diverge by {diff}%. "
                f"DO NOT issue procurement or execution recommendations. "
                f"Show both values. Recommend manual verification with broker."
            ),
            "display_source": "both",
        }

    # ── WARNING: validation passed but with medium-severity issues ───────
    if validation.get("severity") == "medium":
        return {
            "state": STATE_WARNING,
            "decision_grade": False,
            "action_guidance": (
                "Data available but has quality warnings. "
                "Provide cautious interpretation. Flag issues to the user. "
                "Do not recommend execution without additional confirmation."
            ),
            "display_source": comparison.get("dominant_source", "api"),
        }

    # ── VALIDATED: all checks passed, sources aligned ────────────────────
    return {
        "state": STATE_VALIDATED,
        "decision_grade": True,
        "action_guidance": (
            "Data validated and sources aligned. "
            "Safe to provide actionable market intelligence and procurement guidance."
        ),
        "display_source": comparison.get("dominant_source", "api"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE — Full pipeline in one call
# ═══════════════════════════════════════════════════════════════════════════════


def run_full_validation(
    api_data: dict | None,
    reference_price_cents: float | None,
    threshold_pct: float = DEFAULT_CONFLICT_THRESHOLD_PCT,
) -> dict:
    """Run the complete validation pipeline.

    Args:
        api_data: Raw API provider result dict (or None / error dict).
        reference_price_cents: Extracted reference price in cents/lb (or None).
        threshold_pct: Conflict threshold percentage.

    Returns:
        {
            "validation": <validate_market_data result>,
            "comparison": <compare_sources result>,
            "classification": <classify_market_state result>,
            "api_price_cents": float | None,
            "reference_price_cents": float | None,
            "integrity_report": str,   # formatted text block for injection into prompts
        }
    """
    # Step 1: Validate API data
    validation = validate_market_data(api_data or {})

    # Extract validated API price (None if invalid)
    api_price = None
    if validation["is_valid"] and api_data:
        api_price = api_data.get("price_cents_lb") or api_data.get("price")

    # Step 2: Compare sources
    comparison = compare_sources(api_price, reference_price_cents, threshold_pct)

    # Step 3: Classify state
    classification = classify_market_state(validation, comparison)

    # Step 4: Build integrity report text block
    report = _build_integrity_report(
        api_data=api_data,
        api_price=api_price,
        reference_price=reference_price_cents,
        validation=validation,
        comparison=comparison,
        classification=classification,
    )

    result = {
        "validation": validation,
        "comparison": comparison,
        "classification": classification,
        "api_price_cents": api_price,
        "reference_price_cents": reference_price_cents,
        "integrity_report": report,
    }

    # Log the full result
    logger.info(
        f"[VALIDATION-PIPELINE] state={classification['state']} "
        f"decision_grade={classification['decision_grade']} "
        f"api={api_price} ref={reference_price_cents} "
        f"diff={comparison.get('percent_difference')}%"
    )

    return result


def _build_integrity_report(
    *,
    api_data: dict | None,
    api_price: float | None,
    reference_price: float | None,
    validation: dict,
    comparison: dict,
    classification: dict,
) -> str:
    """Build the formatted Market Data Integrity Report text block.

    This block is injected into every analyst prompt so Claude has
    full transparency on data quality.
    """
    lines: list[str] = []
    lines.append("=" * 50)
    lines.append("Market Data Integrity Report — Coffee C")
    lines.append("=" * 50)
    lines.append("")

    # ── API Feed section ─────────────────────────────────────────────────
    lines.append("API Feed:")
    if api_price is not None:
        lines.append(f"  {api_price:.2f} cents/lb (${api_price / 100:.4f}/lb)")
        source = (api_data or {}).get("source", "unknown")
        lines.append(f"  Source: {source}")
        ts = (api_data or {}).get("timestamp") or (api_data or {}).get("fetched_at", "N/A")
        lines.append(f"  Timestamp: {ts}")
    elif api_data and "error" in api_data:
        vf = api_data.get("validation_failure", "")
        lines.append(f"  UNAVAILABLE — {api_data['error'][:120]}")
        if vf:
            lines.append(f"  Validation failure: {vf}")
    else:
        lines.append("  UNAVAILABLE — no API data")
    lines.append("")

    # ── Reference Source section ──────────────────────────────────────────
    lines.append("Reference Source:")
    if reference_price is not None:
        lines.append(f"  {reference_price:.2f} cents/lb (${reference_price / 100:.4f}/lb)")
        lines.append("  Source: Investing.com (user provided)")
    else:
        lines.append("  NOT PROVIDED — no reference URL submitted")
    lines.append("")

    # ── Difference ───────────────────────────────────────────────────────
    diff = comparison.get("percent_difference")
    lines.append("Difference:")
    if diff is not None:
        lines.append(f"  {diff:.1f}%")
    else:
        lines.append("  N/A (cannot compare — missing source)")
    lines.append("")

    # ── Validation Status ────────────────────────────────────────────────
    state = classification["state"]
    lines.append(f"Validation Status: {state}")
    lines.append(f"Decision Grade: {'YES' if classification['decision_grade'] else 'NO'}")
    lines.append("")

    # ── Issues ───────────────────────────────────────────────────────────
    issues = validation.get("issues", [])
    lines.append("Key Issues:")
    if issues:
        for issue in issues:
            lines.append(f"  - {issue}")
    else:
        lines.append("  - None detected")
    lines.append("")

    # ── Confidence ───────────────────────────────────────────────────────
    lines.append(f"Confidence Level: {comparison.get('confidence_level', 'low').upper()}")
    lines.append("")
    lines.append("=" * 50)

    return "\n".join(lines)
