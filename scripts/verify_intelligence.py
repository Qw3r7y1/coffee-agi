"""
Maillard Intelligence System — endpoint verification runner.

Tests every intelligence API endpoint against the running local server.
Prints a color-coded pass/fail report with response details.

Usage:
    python scripts/verify_intelligence.py
    python scripts/verify_intelligence.py --skip-slow
    python scripts/verify_intelligence.py --base http://some-other-host:9000
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field

import httpx

# ── Config ───────────────────────────────────────────────────────────────────

DEFAULT_BASE = "http://127.0.0.1:8000"
TIMEOUT_FAST = 20       # endpoints that just call market APIs
TIMEOUT_SLOW = 90       # endpoints that also call Claude (executive brief)

# ── Terminal formatting ──────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"

TAG_PASS = f"{GREEN}PASS{RESET}"
TAG_FAIL = f"{RED}FAIL{RESET}"
TAG_WARN = f"{YELLOW}WARN{RESET}"


# ── Test definitions ─────────────────────────────────────────────────────────

@dataclass
class EndpointTest:
    """Declarative specification for one endpoint test."""
    name: str
    path: str
    required_keys: list[str] = field(default_factory=list)
    timeout: int = TIMEOUT_FAST
    # When the Twelve Data API key is missing the endpoint returns 200 with
    # an "error" key inside the JSON body.  We still count the structure as
    # valid if allow_error_key is True.
    allow_error_key: bool = True
    # Keys that only appear when live data succeeds (not when error returned).
    success_only_keys: list[str] = field(default_factory=list)


TESTS: list[EndpointTest] = [
    EndpointTest(
        name="Market Snapshot",
        path="/api/intelligence/market-snapshot",
        required_keys=["timestamp", "coffee_futures", "trend"],
        success_only_keys=["fx_rates"],
    ),
    EndpointTest(
        name="Coffee Futures + 30d History",
        path="/api/intelligence/coffee-futures?days=30",
        required_keys=["quote"],
        success_only_keys=["history"],
    ),
    EndpointTest(
        name="FX Rates (all pairs)",
        path="/api/intelligence/fx",
        required_keys=["rates", "pairs_requested"],
    ),
    EndpointTest(
        name="FX Rate (BRL/USD)",
        path="/api/intelligence/fx?pair=BRL/USD",
        # Single-pair returns {pair, rate, …} on success or {error} on failure
        success_only_keys=["pair", "rate"],
    ),
    EndpointTest(
        name="Green Cost Pressure",
        path="/api/intelligence/green-cost-pressure",
        success_only_keys=[
            "futures_price_cents_lb",
            "estimated_green_usd_kg",
            "cost_pressure_percent",
        ],
    ),
    EndpointTest(
        name="Margin Pressure",
        path="/api/intelligence/margin-pressure",
        success_only_keys=[
            "retail_margin_percent",
            "wholesale_margin_percent",
            "actual_cogs_percent",
        ],
    ),
    EndpointTest(
        name="Executive Brief  (slow — calls Claude)",
        path="/api/intelligence/executive-brief",
        success_only_keys=["brief", "snapshot", "generated_at"],
        timeout=TIMEOUT_SLOW,
    ),
    EndpointTest(
        name="Snapshot History (DB)",
        path="/api/intelligence/history/snapshots",
        required_keys=["snapshots"],
    ),
    EndpointTest(
        name="Report History (DB)",
        path="/api/intelligence/history/reports",
        required_keys=["reports"],
    ),
    EndpointTest(
        name="URL Fetch (example.com)",
        path="/api/intelligence/fetch-url?url=https://example.com",
        success_only_keys=["url", "status_code", "title", "content"],
    ),
]


# ── Result container ─────────────────────────────────────────────────────────

@dataclass
class TestResult:
    name: str
    passed: bool = False
    status_code: int | None = None
    elapsed_ms: int = 0
    detail: str = ""
    warnings: list[str] = field(default_factory=list)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_error(body: dict) -> str | None:
    """Walk common response shapes to find an error message, if any."""
    if "error" in body:
        return body["error"]
    for nested_key in ("quote", "coffee_futures"):
        nested = body.get(nested_key)
        if isinstance(nested, dict) and "error" in nested:
            return nested["error"]
    return None


# ── Core runner ──────────────────────────────────────────────────────────────

def run_test(client: httpx.Client, base: str, test: EndpointTest) -> TestResult:
    url = f"{base}{test.path}"
    result = TestResult(name=test.name)

    # ---- request ----
    try:
        t0 = time.monotonic()
        resp = client.get(url, timeout=test.timeout)
        result.elapsed_ms = int((time.monotonic() - t0) * 1000)
        result.status_code = resp.status_code
    except httpx.ConnectError:
        result.detail = "Connection refused — is the server running?"
        return result
    except httpx.TimeoutException:
        result.detail = f"Timed out after {test.timeout}s"
        return result
    except Exception as exc:
        result.detail = f"Request error: {exc}"
        return result

    # ---- HTTP status ----
    if resp.status_code != 200:
        result.detail = f"HTTP {resp.status_code}: {resp.text[:200]}"
        return result

    # ---- JSON parse ----
    try:
        body = resp.json()
    except Exception:
        result.detail = "Response body is not valid JSON"
        return result

    if not isinstance(body, dict):
        result.detail = f"Expected JSON object, got {type(body).__name__}"
        return result

    # ---- required keys (must always exist) ----
    missing = [k for k in test.required_keys if k not in body]
    if missing:
        result.detail = f"Missing required keys: {missing}"
        return result

    # ---- detect API-level error (e.g. missing Twelve Data key) ----
    err_msg = _extract_error(body)
    if err_msg:
        result.warnings.append(f"API-level error: {err_msg}")
        if test.allow_error_key:
            result.passed = True
            result.detail = "Structural OK (live data unavailable)"
            return result
        result.detail = f"Error in response: {err_msg}"
        return result

    # ---- success-only keys (only when live data present) ----
    missing_success = [k for k in test.success_only_keys if k not in body]
    if missing_success:
        result.detail = f"Missing keys in success response: {missing_success}"
        return result

    result.passed = True
    result.detail = "OK"
    return result


# ── Pretty printer ───────────────────────────────────────────────────────────

def print_result(result: TestResult) -> None:
    tag = TAG_PASS if result.passed else TAG_FAIL
    http = f"HTTP {result.status_code}" if result.status_code else "NO RESPONSE"
    ms = f"{DIM}{result.elapsed_ms:>5d}ms{RESET}" if result.elapsed_ms else f"{DIM}   --{RESET}"

    print(f"  {tag}  {http:<8}  {ms}  {result.detail}")
    for w in result.warnings:
        print(f"         {TAG_WARN}  {w}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify Maillard Intelligence API endpoints"
    )
    parser.add_argument("--base", default=DEFAULT_BASE, help="Server base URL")
    parser.add_argument(
        "--skip-slow",
        action="store_true",
        help="Skip slow endpoints that call Claude (executive brief)",
    )
    args = parser.parse_args()

    print(f"\n{BOLD}Maillard Intelligence — Endpoint Verification{RESET}")
    print(f"{DIM}Target: {args.base}{RESET}\n")

    # ---- pre-flight: is the server up? ----
    try:
        httpx.get(f"{args.base}/", timeout=5)
    except httpx.ConnectError:
        print(f"{RED}Server not reachable at {args.base}{RESET}")
        print("Start it first:")
        print(f"  .venv/Scripts/python.exe -m uvicorn main:app --reload\n")
        sys.exit(1)
    except Exception:
        pass  # any non-connect error means the server answered

    # ---- pre-flight: does this server actually have the intelligence routes? ----
    try:
        spec_resp = httpx.get(f"{args.base}/openapi.json", timeout=5)
        if spec_resp.status_code == 200:
            paths = spec_resp.json().get("paths", {})
            intel_paths = [p for p in paths if "/intelligence/" in p]
            if not intel_paths:
                print(f"{RED}Server is running but has ZERO /intelligence/ routes.{RESET}")
                print(f"{RED}This is a stale server started before the intelligence system was added.{RESET}")
                print()
                print(f"Fix: restart the server so it picks up the new code:")
                print(f"  1. Kill the old process  (find PID with: netstat -ano | findstr :8000)")
                print(f"  2. Restart:  .venv/Scripts/python.exe -m uvicorn main:app --reload")
                print()
                sys.exit(1)
            else:
                print(f"  {DIM}Server has {len(intel_paths)} intelligence routes registered.{RESET}\n")
    except Exception:
        pass  # non-critical — we'll find out from the actual tests

    # ---- pick tests ----
    tests_to_run = TESTS
    if args.skip_slow:
        tests_to_run = [t for t in TESTS if t.timeout <= TIMEOUT_FAST]
        print(f"{DIM}(skipping slow endpoints — use without --skip-slow for full run){RESET}\n")

    # ---- run ----
    results: list[TestResult] = []
    client = httpx.Client()

    for test in tests_to_run:
        print(f"  {CYAN}{test.name}{RESET}")
        print(f"  {DIM}GET {test.path}{RESET}")

        result = run_test(client, args.base, test)
        results.append(result)
        print_result(result)
        print()

    client.close()

    # ---- summary ----
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    total = len(results)
    warn_count = sum(len(r.warnings) for r in results)

    print(f"{BOLD}{'=' * 56}{RESET}")
    print(f"  {BOLD}Total: {total}    Passed: {passed}    Failed: {failed}{RESET}")

    if failed == 0:
        print(f"  {GREEN}All endpoints operational.{RESET}")
    else:
        print(f"  {RED}{failed} endpoint(s) need attention.{RESET}")

    if warn_count:
        print(f"  {YELLOW}{warn_count} warning(s) — likely TWELVEDATA_API_KEY not set.{RESET}")

    print()
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
