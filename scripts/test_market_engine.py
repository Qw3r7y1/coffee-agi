"""
Market Data Engine test suite.

Tests:
  1. Unit detection (cents vs dollars vs garbage)
  2. Price normalization
  3. Source reliability scoring
  4. Full pipeline (correct feed, wrong feed, conflicting feed)

Run:
  python scripts/test_market_engine.py
"""
from __future__ import annotations
import sys, os, json
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from maillard.mcp.analyst.market_data_engine import (
    detect_unit,
    normalize_price,
    score_source,
    validate_normalized,
    compare_sources_dollars,
    classify_state,
    PRICE_MIN_DOLLARS,
    PRICE_MAX_DOLLARS,
    STATE_VALIDATED,
    STATE_WARNING,
    STATE_FEED_CONFLICT,
    STATE_INVALID_DATA,
    _RELIABILITY_FILE,
)

_p = 0
_f = 0


def check(name, cond, detail=""):
    global _p, _f
    if cond:
        _p += 1
        print(f"  [PASS] {name}")
    else:
        _f += 1
        print(f"  [FAIL] {name} -- {detail}")


# Clean reliability file before tests
if _RELIABILITY_FILE.exists():
    _RELIABILITY_FILE.unlink()


# ======================================================================
# TEST 1: Unit detection
# ======================================================================
def test_unit_detection():
    print("\n=== Unit Detection ===")

    # Yahoo KC=F returns ~292 cents/lb
    r = detect_unit(292.55)
    check("292.55 -> cents/lb", r["detected_unit"] == "cents/lb", r)
    check("high confidence", r["confidence"] == "high", r)

    # If someone gives us $/lb
    r = detect_unit(2.9255)
    check("2.9255 -> $/lb", r["detected_unit"] == "$/lb", r)
    check("high confidence for $2.93", r["confidence"] == "high", r)

    # Kingsoft Cloud stock price (~3.45)
    r = detect_unit(3.45)
    check("3.45 -> $/lb (but actually stock)", r["detected_unit"] == "$/lb", r)

    # Zero / garbage
    r = detect_unit(0.001)
    check("0.001 -> unknown", r["detected_unit"] == "unknown", r)

    r = detect_unit(0)
    check("0 -> unknown", r["detected_unit"] == "unknown", r)

    r = detect_unit(1500)
    check("1500 -> unknown (too high)", r["detected_unit"] == "unknown", r)

    # Edge: 50 cents/lb (very low but plausible)
    r = detect_unit(50.0)
    check("50 -> cents/lb", r["detected_unit"] == "cents/lb", r)

    # Edge: 0.50 $/lb
    r = detect_unit(0.50)
    check("0.50 -> $/lb", r["detected_unit"] == "$/lb", r)


# ======================================================================
# TEST 2: Price normalization
# ======================================================================
def test_normalization():
    print("\n=== Price Normalization ===")

    # Cents to dollars
    r = normalize_price(292.55, "cents/lb")
    check("292.55 c -> $2.9255", r["price_dollars_lb"] == 2.9255, r)
    check("cents preserved", r["price_cents_lb"] == 292.55, r)
    check("normalized=True", r["normalized"] is True, r)

    # Dollars pass-through
    r = normalize_price(2.9255, "$/lb")
    check("$2.9255 -> $2.9255", r["price_dollars_lb"] == 2.9255, r)
    check("cents=292.55", r["price_cents_lb"] == 292.55, r)

    # Unknown unit -> fails
    r = normalize_price(3.45, "unknown")
    check("unknown -> normalized=False", r["normalized"] is False, r)
    check("dollars=None", r["price_dollars_lb"] is None, r)

    # 131.00 cents -> $1.31
    r = normalize_price(131.00, "cents/lb")
    check("131c -> $1.31", r["price_dollars_lb"] == 1.31, r)


# ======================================================================
# TEST 3: Validation (in $/lb)
# ======================================================================
def test_validation():
    print("\n=== Validation ($/lb) ===")

    from datetime import datetime, timezone

    fresh_ts = datetime.now(timezone.utc).isoformat()

    # Good data
    r = validate_normalized(2.9255, {
        "name": "Coffee C Futures", "exchange": "NYBOT",
        "timestamp": fresh_ts, "change_percent": 1.2,
    })
    check("valid coffee data", r["is_valid"] is True, r)
    check("severity low", r["severity"] == "low", r)

    # Kingsoft Cloud: symbol mismatch + wrong price range
    r = validate_normalized(0.0345, {
        "name": "Kingsoft Cloud Holdings", "exchange": "NASDAQ",
        "timestamp": fresh_ts, "change_percent": -2.0,
    })
    check("kingsoft invalid", r["is_valid"] is False, r)
    check("symbol_ok=False", r["checks"]["symbol_ok"] is False, r)

    # Price too high
    r = validate_normalized(12.00, {
        "name": "Coffee C Futures", "exchange": "NYBOT",
        "timestamp": fresh_ts,
    })
    check("$12/lb invalid", r["is_valid"] is False, r)

    # None price
    r = validate_normalized(None, {
        "name": "Coffee C Futures", "timestamp": fresh_ts,
    })
    check("None price invalid", r["is_valid"] is False, r)


# ======================================================================
# TEST 4: Source reliability tracking
# ======================================================================
def test_scoring():
    print("\n=== Source Reliability Tracking ===")

    # Good call
    r = score_source("test_api", validation_passed=True, data_fresh=True, unit_detected_ok=True)
    check("good call >= 80", r["score"] >= 80, f"score={r['score']}")
    check("has valid_rate", "valid_rate" in r, r)
    check("has conflict_rate", "conflict_rate" in r, r)
    check("has stability", "stability" in r, r)
    check("valid_rate=100", r["valid_rate"] == 100.0, f"valid_rate={r['valid_rate']}")

    # Bad call (validation failed, stale, no unit)
    r = score_source("test_api", validation_passed=False, data_fresh=False, unit_detected_ok=False)
    check("bad call low", r["score"] < 20, f"score={r['score']}")
    check("valid_rate dropped", r["valid_rate"] == 50.0, f"valid_rate={r['valid_rate']}")

    # Conflict call
    r = score_source("test_api", True, True, True, conflict=True, deviation_pct=12.5)
    check("conflict recorded", r["conflict_rate"] > 0, f"conflict_rate={r['conflict_rate']}")
    check("deviation tracked", r["avg_deviation_pct"] is not None, r)
    check("history_count=3", r["history_count"] == 3, r)

    # Build up more good calls to show recovery
    for _ in range(5):
        score_source("test_api", True, True, True)
    r = score_source("test_api", True, True, True)
    check("rolling avg > 50 after recovery", r["rolling_avg"] > 50, f"avg={r['rolling_avg']}")
    check("stability is stable or unknown", r["stability"] in ("stable", "unknown"), f"stab={r['stability']}")
    check("history capped at 20", r["history_count"] <= 20, r)

    # Simulate degrading source: 5 bad calls in a row
    for _ in range(5):
        score_source("test_degrade", False, False, False, conflict=True)
    r = score_source("test_degrade", False, False, False)
    check("degraded avg < 20", r["rolling_avg"] < 20, f"avg={r['rolling_avg']}")
    check("valid_rate low", r["valid_rate"] == 0.0, f"valid_rate={r['valid_rate']}")
    # All calls are 0 -- stability is "stable" (consistently bad, not degrading)
    check("stability consistent", r["stability"] in ("stable", "unstable", "degrading"), f"stab={r['stability']}")

    # Verify get_source_reliability returns all tracked sources
    from maillard.mcp.analyst.market_data_engine import get_source_reliability
    all_sources = get_source_reliability()
    check("test_api in history", "test_api" in all_sources, list(all_sources.keys()))
    check("test_degrade in history", "test_degrade" in all_sources, list(all_sources.keys()))
    check("events list present", "events" in all_sources.get("test_api", {}), all_sources.get("test_api", {}).keys())


# ======================================================================
# TEST 5: Source comparison (in $/lb)
# ======================================================================
def test_comparison():
    print("\n=== Source Comparison ($/lb) ===")

    # Aligned
    r = compare_sources_dollars(2.93, 2.95)
    check("aligned", r["status"] == "aligned", r)
    check("no conflict", r["is_conflict"] is False, r)

    # Conflict (>10%)
    r = compare_sources_dollars(2.93, 3.30)
    check("conflict", r["is_conflict"] is True, r)
    check("diff > 10%", r["percent_difference"] > 10, r)

    # API only
    r = compare_sources_dollars(2.93, None)
    check("api_only", r["status"] == "api_only", r)


# ======================================================================
# TEST 6: State classification
# ======================================================================
def test_classification():
    print("\n=== State Classification ===")

    # VALIDATED: valid data, aligned sources, good unit confidence
    v = {"is_valid": True, "severity": "low", "checks": {}}
    c = {"is_conflict": False, "dominant_source": "reference", "confidence_level": "high"}
    r = classify_state(v, c, "high")
    check("VALIDATED", r["state"] == STATE_VALIDATED, r)
    check("decision_grade=True", r["decision_grade"] is True, r)

    # WARNING from unit uncertainty
    r = classify_state(v, c, "low")
    check("WARNING from unit", r["state"] == STATE_WARNING, r)
    check("decision_grade=False", r["decision_grade"] is False, r)

    # FEED_CONFLICT
    c_conflict = {"is_conflict": True, "percent_difference": 15.0}
    r = classify_state(v, c_conflict, "high")
    check("FEED_CONFLICT", r["state"] == STATE_FEED_CONFLICT, r)

    # INVALID_DATA
    v_bad = {"is_valid": False, "severity": "high"}
    r = classify_state(v_bad, c, "high")
    check("INVALID_DATA", r["state"] == STATE_INVALID_DATA, r)

    # WARNING from low source reliability (data looks fine, but source has bad history)
    bad_reliability = {"rolling_avg": 25, "stability": "unstable", "conflict_rate": 10, "blacklisted": False}
    r = classify_state(v, c, "high", api_reliability=bad_reliability)
    check("WARNING from bad reliability", r["state"] == STATE_WARNING, r)
    check("no decision grade from bad source", r["decision_grade"] is False, r)

    # WARNING from high conflict rate
    high_conflict = {"rolling_avg": 70, "stability": "stable", "conflict_rate": 40, "blacklisted": False}
    r = classify_state(v, c, "high", api_reliability=high_conflict)
    check("WARNING from high conflict rate", r["state"] == STATE_WARNING, r)

    # Good reliability should still pass
    good_reliability = {"rolling_avg": 85, "stability": "stable", "conflict_rate": 5, "blacklisted": False}
    r = classify_state(v, c, "high", api_reliability=good_reliability)
    check("VALIDATED with good reliability", r["state"] == STATE_VALIDATED, r)


# ======================================================================
# TEST 6b: Decision modes
# ======================================================================
def test_decision_modes():
    print("\n=== Decision Modes ===")

    from maillard.mcp.analyst.market_data_engine import MODE_SAFE, MODE_CAUTIOUS, MODE_AGGRESSIVE

    v_ok = {"is_valid": True, "severity": "low", "checks": {}}
    v_warn = {"is_valid": True, "severity": "medium", "checks": {}}
    v_bad = {"is_valid": False, "severity": "high", "checks": {}}
    c_ok = {"is_conflict": False, "dominant_source": "reference", "confidence_level": "high"}
    c_conflict = {"is_conflict": True, "percent_difference": 15.0}

    # --- SAFE mode (default) ---
    # VALIDATED -> decision_grade=True
    r = classify_state(v_ok, c_ok, "high", mode=MODE_SAFE)
    check("SAFE + VALIDATED -> grade=True", r["decision_grade"] is True)
    check("mode=SAFE", r["mode"] == "SAFE")

    # WARNING -> decision_grade=False
    r = classify_state(v_warn, c_ok, "high", mode=MODE_SAFE)
    check("SAFE + WARNING -> grade=False", r["decision_grade"] is False)

    # CONFLICT -> decision_grade=False
    r = classify_state(v_ok, c_conflict, "high", mode=MODE_SAFE)
    check("SAFE + CONFLICT -> grade=False", r["decision_grade"] is False)

    # --- CAUTIOUS mode ---
    # VALIDATED -> True
    r = classify_state(v_ok, c_ok, "high", mode=MODE_CAUTIOUS)
    check("CAUTIOUS + VALIDATED -> grade=True", r["decision_grade"] is True)

    # WARNING -> directional
    r = classify_state(v_warn, c_ok, "high", mode=MODE_CAUTIOUS)
    check("CAUTIOUS + WARNING -> directional", r["decision_grade"] == "directional")

    # CONFLICT -> still blocked
    r = classify_state(v_ok, c_conflict, "high", mode=MODE_CAUTIOUS)
    check("CAUTIOUS + CONFLICT -> grade=False", r["decision_grade"] is False)

    # --- AGGRESSIVE mode ---
    # WARNING -> True (full execution)
    r = classify_state(v_warn, c_ok, "high", mode=MODE_AGGRESSIVE)
    check("AGGRESSIVE + WARNING -> grade=True", r["decision_grade"] is True)

    # CONFLICT -> directional (not blocked)
    r = classify_state(v_ok, c_conflict, "high", mode=MODE_AGGRESSIVE)
    check("AGGRESSIVE + CONFLICT -> directional", r["decision_grade"] == "directional")

    # INVALID -> still blocked even in AGGRESSIVE
    r = classify_state(v_bad, c_ok, "high", mode=MODE_AGGRESSIVE)
    check("AGGRESSIVE + INVALID -> grade=False", r["decision_grade"] is False)


# ======================================================================
# TEST 7: Example outputs
# ======================================================================
def test_example_outputs():
    print("\n=== Example Output Scenarios ===")

    from datetime import datetime, timezone
    fresh = datetime.now(timezone.utc).isoformat()

    # --- Correct feed (Yahoo KC=F returning 292.55 cents/lb) ---
    print("\n  [Scenario: Correct feed]")
    raw = 292.55
    unit = detect_unit(raw)
    check("detected cents/lb", unit["detected_unit"] == "cents/lb")
    norm = normalize_price(raw, unit["detected_unit"])
    check("normalized to $2.9255", norm["price_dollars_lb"] == 2.9255)
    val = validate_normalized(norm["price_dollars_lb"], {
        "name": "Coffee C Futures", "exchange": "NYBOT", "timestamp": fresh,
    })
    check("validation passed", val["is_valid"])
    comp = compare_sources_dollars(norm["price_dollars_lb"], 2.95)
    check("aligned with ref", comp["status"] == "aligned")
    cls = classify_state(val, comp, unit["confidence"])
    check("state=VALIDATED", cls["state"] == STATE_VALIDATED)
    print(f"    Price: ${norm['price_dollars_lb']}/lb | State: {cls['state']} | Grade: {cls['decision_grade']}")

    # --- Wrong feed (Twelve Data KC = Kingsoft Cloud at $3.45) ---
    print("\n  [Scenario: Wrong feed - Kingsoft Cloud]")
    raw = 3.45
    unit = detect_unit(raw)
    check("detected $/lb (stock price range)", unit["detected_unit"] == "$/lb")
    norm = normalize_price(raw, unit["detected_unit"])
    # $3.45/lb is in plausible coffee range, but symbol check catches it
    val = validate_normalized(norm["price_dollars_lb"], {
        "name": "Kingsoft Cloud Holdings Ltd", "exchange": "NASDAQ", "timestamp": fresh,
    })
    check("validation FAILED (symbol mismatch)", val["is_valid"] is False)
    cls = classify_state(val, {"is_conflict": False}, unit["confidence"])
    check("state=INVALID_DATA", cls["state"] == STATE_INVALID_DATA)
    print(f"    Price: ${norm['price_dollars_lb']}/lb | State: {cls['state']} | Issues: {val['issues']}")

    # --- Conflicting feeds ---
    print("\n  [Scenario: Conflicting feeds]")
    api_raw = 292.55
    ref_raw = 335.00
    api_unit = detect_unit(api_raw)
    api_norm = normalize_price(api_raw, api_unit["detected_unit"])
    ref_norm = normalize_price(ref_raw, "cents/lb")
    val = validate_normalized(api_norm["price_dollars_lb"], {
        "name": "Coffee C Futures", "exchange": "NYBOT", "timestamp": fresh,
    })
    check("api valid", val["is_valid"])
    comp = compare_sources_dollars(api_norm["price_dollars_lb"], ref_norm["price_dollars_lb"])
    check("conflict detected", comp["is_conflict"])
    cls = classify_state(val, comp, api_unit["confidence"])
    check("state=FEED_CONFLICT", cls["state"] == STATE_FEED_CONFLICT)
    check("decision_grade=False", cls["decision_grade"] is False)
    print(f"    API: ${api_norm['price_dollars_lb']}/lb | Ref: ${ref_norm['price_dollars_lb']}/lb | Diff: {comp['percent_difference']}% | State: {cls['state']}")


# ======================================================================
# RUN ALL
# ======================================================================
def main():
    print("=" * 60)
    print("MARKET DATA ENGINE -- TEST SUITE")
    print("=" * 60)

    test_unit_detection()
    test_normalization()
    test_validation()
    test_scoring()
    test_comparison()
    test_classification()
    test_decision_modes()
    test_example_outputs()

    # Clean up test reliability file
    if _RELIABILITY_FILE.exists():
        _RELIABILITY_FILE.unlink()

    total = _p + _f
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {_p} passed, {_f} failed, {total} total")
    print(f"{'=' * 60}")
    sys.exit(1 if _f else 0)


if __name__ == "__main__":
    main()
