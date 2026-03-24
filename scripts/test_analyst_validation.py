"""
Analyst validation test suite.

Tests the three-stage validation pipeline:
  1. validate_market_data()   - plausibility, staleness, units
  2. compare_sources()        - API vs reference cross-validation
  3. classify_market_state()  - final state classification
  4. run_full_validation()    - end-to-end pipeline

Run:
  python scripts/test_analyst_validation.py
"""
from __future__ import annotations

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from maillard.mcp.analyst.validation import (
    validate_market_data,
    compare_sources,
    classify_market_state,
    run_full_validation,
    STATE_VALIDATED,
    STATE_WARNING,
    STATE_FEED_CONFLICT,
    STATE_INVALID_DATA,
)

_passed = 0
_failed = 0
_total = 0


def assert_eq(name, actual, expected, msg=""):
    global _passed, _failed, _total
    _total += 1
    if actual == expected:
        _passed += 1
        print(f"  [PASS] {name}")
    else:
        _failed += 1
        detail = f" -- {msg}" if msg else ""
        print(f"  [FAIL] {name}: expected {expected!r}, got {actual!r}{detail}")


def assert_true(name, value, msg=""):
    assert_eq(name, bool(value), True, msg)


def assert_false(name, value, msg=""):
    assert_eq(name, bool(value), False, msg)


def _fresh_ts():
    return datetime.now(timezone.utc).isoformat()


def _make_valid_api(price=385.50, change=1.2, source="yahoo_finance"):
    return {
        "symbol": "KC",
        "name": "Coffee C Futures",
        "exchange": "NYBOT",
        "price": price,
        "change_percent": change,
        "timestamp": _fresh_ts(),
        "source": source,
    }


# ======================================================================
# TEST 1: Normal case - matching sources
# ======================================================================

def test_normal_case():
    print("\n=== TEST 1: Normal case (matching sources) ===")

    api_data = _make_valid_api(385.50)
    ref_price = 388.00  # ~0.6% difference

    validation = validate_market_data(api_data)
    assert_true("api_valid", validation["is_valid"])
    assert_eq("severity_low", validation["severity"], "low")
    assert_true("symbol_ok", validation["checks"]["symbol_ok"])
    assert_true("price_plausible", validation["checks"]["price_plausible"])
    assert_true("unit_consistent", validation["checks"]["unit_consistent"])
    assert_eq("no_issues", len(validation["issues"]), 0)

    comparison = compare_sources(385.50, ref_price)
    assert_false("no_conflict", comparison["is_conflict"])
    assert_eq("status_aligned", comparison["status"], "aligned")
    assert_eq("confidence_high", comparison["confidence_level"], "high")
    assert_true("diff_small", comparison["percent_difference"] < 2.0)

    classification = classify_market_state(validation, comparison)
    assert_eq("state_validated", classification["state"], STATE_VALIDATED)
    assert_true("decision_grade", classification["decision_grade"])

    pipeline = run_full_validation(api_data, ref_price)
    assert_eq("pipeline_state", pipeline["classification"]["state"], STATE_VALIDATED)
    assert_true("pipeline_grade", pipeline["classification"]["decision_grade"])
    assert_true("has_report", len(pipeline["integrity_report"]) > 100)
    assert_true("report_has_validated", "VALIDATED" in pipeline["integrity_report"])

    print("  --- Integrity Report Preview ---")
    for line in pipeline["integrity_report"].split("\n")[:15]:
        print(f"  | {line}")


# ======================================================================
# TEST 2: Conflict case - large difference between sources
# ======================================================================

def test_conflict_case():
    print("\n=== TEST 2: Conflict case (large difference) ===")

    api_data = _make_valid_api(385.50)
    ref_price = 435.00  # ~12.8% difference

    validation = validate_market_data(api_data)
    assert_true("api_valid", validation["is_valid"])

    comparison = compare_sources(385.50, ref_price)
    assert_true("is_conflict", comparison["is_conflict"])
    assert_eq("status_conflict", comparison["status"], "conflict")
    assert_eq("confidence_low", comparison["confidence_level"], "low")
    assert_true("diff_above_threshold", comparison["percent_difference"] > 10.0)

    classification = classify_market_state(validation, comparison)
    assert_eq("state_conflict", classification["state"], STATE_FEED_CONFLICT)
    assert_false("no_decision_grade", classification["decision_grade"])

    pipeline = run_full_validation(api_data, ref_price)
    assert_eq("pipeline_conflict", pipeline["classification"]["state"], STATE_FEED_CONFLICT)
    assert_false("pipeline_no_grade", pipeline["classification"]["decision_grade"])
    assert_true("report_has_conflict", "FEED_CONFLICT" in pipeline["integrity_report"])

    print("  --- Integrity Report Preview ---")
    for line in pipeline["integrity_report"].split("\n")[:15]:
        print(f"  | {line}")


# ======================================================================
# TEST 3: Invalid API data - wrong instrument (Kingsoft Cloud)
# ======================================================================

def test_invalid_api():
    print("\n=== TEST 3: Invalid API data (symbol mismatch) ===")

    api_data = {
        "symbol": "KC",
        "name": "Kingsoft Cloud Holdings Ltd",
        "exchange": "NASDAQ",
        "price": 3.45,
        "change_percent": -2.1,
        "timestamp": _fresh_ts(),
        "source": "twelvedata",
    }

    validation = validate_market_data(api_data)
    assert_false("api_invalid", validation["is_valid"])
    assert_eq("severity_high", validation["severity"], "high")
    assert_false("symbol_bad", validation["checks"]["symbol_ok"])
    assert_true("has_issues", len(validation["issues"]) > 0)
    assert_true("mentions_mismatch", "mismatch" in validation["issues"][0].lower())

    comparison = compare_sources(None, None)
    assert_eq("status_no_data", comparison["status"], "no_data")

    classification = classify_market_state(validation, comparison)
    assert_eq("state_invalid", classification["state"], STATE_INVALID_DATA)
    assert_false("no_decision_grade", classification["decision_grade"])

    pipeline = run_full_validation(api_data, None)
    assert_eq("pipeline_invalid", pipeline["classification"]["state"], STATE_INVALID_DATA)
    assert_eq("api_price_none", pipeline["api_price_cents"], None)
    assert_true("report_has_invalid", "INVALID_DATA" in pipeline["integrity_report"])

    print("  --- Integrity Report Preview ---")
    for line in pipeline["integrity_report"].split("\n")[:15]:
        print(f"  | {line}")


# ======================================================================
# TEST 4: Missing reference - API only
# ======================================================================

def test_missing_reference():
    print("\n=== TEST 4: Missing reference (API only) ===")

    api_data = _make_valid_api(390.25, 0.5)

    validation = validate_market_data(api_data)
    assert_true("api_valid", validation["is_valid"])

    comparison = compare_sources(390.25, None)
    assert_eq("status_api_only", comparison["status"], "api_only")
    assert_eq("dominant_api", comparison["dominant_source"], "api")
    assert_eq("confidence_medium", comparison["confidence_level"], "medium")
    assert_false("no_conflict", comparison["is_conflict"])

    classification = classify_market_state(validation, comparison)
    assert_eq("state_validated", classification["state"], STATE_VALIDATED)
    assert_true("decision_grade", classification["decision_grade"])

    pipeline = run_full_validation(api_data, None)
    assert_eq("pipeline_validated", pipeline["classification"]["state"], STATE_VALIDATED)
    assert_eq("api_price", pipeline["api_price_cents"], 390.25)
    assert_eq("ref_price_none", pipeline["reference_price_cents"], None)
    assert_true("report_has_not_provided", "NOT PROVIDED" in pipeline["integrity_report"])

    print("  --- Integrity Report Preview ---")
    for line in pipeline["integrity_report"].split("\n")[:15]:
        print(f"  | {line}")


# ======================================================================
# TEST 5: Unit confusion - price in dollars instead of cents
# ======================================================================

def test_unit_confusion():
    print("\n=== TEST 5: Unit confusion (dollars vs cents) ===")

    api_data = {
        "symbol": "KC",
        "name": "Coffee C Futures",
        "exchange": "NYBOT",
        "price": 3.85,
        "change_percent": 0.5,
        "timestamp": _fresh_ts(),
        "source": "unknown_provider",
    }

    validation = validate_market_data(api_data)
    assert_false("api_invalid", validation["is_valid"])
    assert_eq("severity_high", validation["severity"], "high")
    assert_false("price_bad", validation["checks"]["price_plausible"])
    assert_false("unit_bad", validation["checks"]["unit_consistent"])

    print("  Issues detected:")
    for issue in validation["issues"]:
        print(f"    - {issue}")


# ======================================================================
# TEST 6: Stale data
# ======================================================================

def test_stale_data():
    print("\n=== TEST 6: Stale data ===")

    api_data = {
        "symbol": "KC",
        "name": "Coffee C Futures",
        "exchange": "NYBOT",
        "price": 390.25,
        "change_percent": 0.5,
        "timestamp": "2026-03-17T08:00:00+00:00",  # yesterday
        "source": "yahoo_finance",
    }

    validation = validate_market_data(api_data)
    assert_true("still_valid", validation["is_valid"])
    assert_eq("severity_medium", validation["severity"], "medium")
    assert_false("not_fresh", validation["checks"]["data_fresh"])

    classification = classify_market_state(validation, compare_sources(390.25, None))
    assert_eq("state_warning", classification["state"], STATE_WARNING)
    assert_false("no_decision_grade", classification["decision_grade"])


# ======================================================================
# TEST 7: Abnormal daily move
# ======================================================================

def test_abnormal_move():
    print("\n=== TEST 7: Abnormal daily move ===")

    api_data = _make_valid_api(390.25, 12.5)

    validation = validate_market_data(api_data)
    assert_true("still_valid", validation["is_valid"])
    assert_eq("severity_medium", validation["severity"], "medium")
    assert_false("move_abnormal", validation["checks"]["move_normal"])

    print("  Issues detected:")
    for issue in validation["issues"]:
        print(f"    - {issue}")


# ======================================================================
# TEST 8: Reference only - no API
# ======================================================================

def test_reference_only():
    print("\n=== TEST 8: Reference only (no API) ===")

    api_data = {"error": "Yahoo Finance unavailable"}

    validation = validate_market_data(api_data)
    assert_false("api_invalid", validation["is_valid"])

    comparison = compare_sources(None, 395.50)
    assert_eq("status_ref_only", comparison["status"], "reference_only")
    assert_eq("dominant_ref", comparison["dominant_source"], "reference")
    assert_eq("confidence_medium", comparison["confidence_level"], "medium")

    pipeline = run_full_validation(api_data, 395.50)
    assert_eq("pipeline_state", pipeline["classification"]["state"], STATE_INVALID_DATA)
    assert_eq("ref_price", pipeline["reference_price_cents"], 395.50)
    assert_eq("api_price_none", pipeline["api_price_cents"], None)


# ======================================================================
# TEST 9: Edge case - zero price
# ======================================================================

def test_zero_price():
    print("\n=== TEST 9: Zero price ===")

    api_data = {
        "symbol": "KC",
        "name": "Coffee C Futures",
        "price": 0,
        "timestamp": _fresh_ts(),
    }

    validation = validate_market_data(api_data)
    assert_false("invalid", validation["is_valid"])
    assert_false("price_bad", validation["checks"]["price_plausible"])


# ======================================================================
# TEST 10: Compare sources - threshold edge cases
# ======================================================================

def test_threshold_edge():
    print("\n=== TEST 10: Threshold edge cases ===")

    comp_under = compare_sources(400.0, 440.0)  # 9.09% - under 10%
    assert_false("under_10_no_conflict", comp_under["is_conflict"])

    comp_over = compare_sources(400.0, 445.0)  # 10.1% - over 10%
    assert_true("over_10_conflict", comp_over["is_conflict"])

    comp_custom = compare_sources(400.0, 425.0, threshold_pct=5.0)  # 5.9% vs 5%
    assert_true("custom_threshold_conflict", comp_custom["is_conflict"])


# ======================================================================
# RUN ALL
# ======================================================================

def main():
    print("=" * 60)
    print("MAILLARD ANALYST -- VALIDATION TEST SUITE")
    print("=" * 60)

    test_normal_case()
    test_conflict_case()
    test_invalid_api()
    test_missing_reference()
    test_unit_confusion()
    test_stale_data()
    test_abnormal_move()
    test_reference_only()
    test_zero_price()
    test_threshold_edge()

    print("\n" + "=" * 60)
    print(f"RESULTS: {_passed} passed, {_failed} failed, {_total} total")
    print("=" * 60)

    if _failed > 0:
        print("\n*** FAILURES DETECTED ***")
        sys.exit(1)
    else:
        print("\nAll tests passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
