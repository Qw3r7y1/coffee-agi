"""
Maillard Intelligence System — chat routing smoke test.

Part 1 (always):  Verify routing logic via GET /mcp/route — fast, free,
                  no API keys needed.
Part 2 (--live):  Send real prompts via POST /mcp/chat — slow, uses
                  Anthropic + Twelve Data API credits.

Usage:
    python scripts/smoke_chat_routing.py                # routing only
    python scripts/smoke_chat_routing.py --live          # + live chat
    python scripts/smoke_chat_routing.py --base http://host:port
"""
from __future__ import annotations

import argparse
import sys
import time

import httpx

# ── Config ───────────────────────────────────────────────────────────────────

DEFAULT_BASE = "http://127.0.0.1:8000"

# ── Terminal colours ─────────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"

TAG_PASS = f"{GREEN}PASS{RESET}"
TAG_FAIL = f"{RED}FAIL{RESET}"


# ── Routing test cases ───────────────────────────────────────────────────────
# (prompt, expected_department)

ROUTING_CASES: list[tuple[str, str]] = [
    # ---- Intelligence / analyst prompts ----
    ("What is coffee futures trading at?",               "analyst"),
    ("What is coffee trading at?",                       "analyst"),
    ("What is the BRL/USD rate?",                        "analyst"),
    ("What is the margin pressure?",                     "analyst"),
    ("Give me an executive market brief",                "analyst"),
    ("What is the green coffee cost pressure?",          "analyst"),
    ("Show me a market snapshot",                        "analyst"),
    ("What is the market impact on Maillard?",           "analyst"),
    ("What is the procurement risk?",                    "analyst"),
    ("How does the Brazilian real affect coffee costs?",  "analyst"),
    ("What is the supplier cost pressure?",              "analyst"),
    ("What is the arabica price?",                       "analyst"),
    ("Show me the coffee trend",                         "analyst"),
    ("What is the EUR/USD exchange rate?",               "analyst"),
    ("Robusta price today",                              "analyst"),
    ("Should I buy coffee now?",                         "analyst"),
    ("Give me a market brief",                           "analyst"),
    # ---- URL routing ----
    ("https://www.investing.com/commodities/us-coffee-c", "analyst"),
    ("What does this link say about coffee futures? https://example.com/coffee", "analyst"),
    ("Check this article https://reuters.com/markets/coffee", "analyst"),
    # ---- Non-analyst (should NOT route to analyst) ----
    ("How do I make a latte?",                           "recipe"),
    ("Design a new logo for Maillard",                   "designer"),
    ("Create a social media campaign",                   "marketing"),
    ("Run an ad on Instagram",                           "marketing"),
    ("Show me the sales pipeline",                       "sales"),
    ("What are our operating hours?",                    "operations"),
]

# Live chat prompts — small subset to keep costs/time down.
LIVE_CHAT_CASES: list[tuple[str, str]] = [
    ("What is coffee futures trading at?", "analyst"),
    ("What is the BRL/USD rate?",          "analyst"),
    ("What is the margin pressure?",       "analyst"),
    ("Give me an executive market brief",  "analyst"),
]


# ── Part 1: routing logic (fast, free) ───────────────────────────────────────

def test_routing(client: httpx.Client, base: str) -> tuple[int, int]:
    print(f"  {BOLD}Part 1 — Routing Logic{RESET}")
    print(f"  {DIM}GET /mcp/route?task=...   (no API keys needed){RESET}\n")

    passed = failed = 0

    for prompt, expected in ROUTING_CASES:
        try:
            resp = client.get(
                f"{base}/mcp/route", params={"task": prompt}, timeout=5,
            )
            if resp.status_code != 200:
                print(f"  {TAG_FAIL}  HTTP {resp.status_code:<4}  {prompt[:55]}")
                failed += 1
                continue

            actual = resp.json().get("routed_to", "???")

            if actual == expected:
                print(f"  {TAG_PASS}  {DIM}{actual:<12}{RESET}  {prompt[:55]}")
                passed += 1
            else:
                print(
                    f"  {TAG_FAIL}  {RED}{actual:<12}{RESET}  {prompt[:55]}"
                    f"  {DIM}(expected {expected}){RESET}"
                )
                failed += 1

        except httpx.ConnectError:
            print(f"  {TAG_FAIL}  Connection refused  {prompt[:55]}")
            failed += 1
        except Exception as exc:
            print(f"  {TAG_FAIL}  {exc}  {prompt[:55]}")
            failed += 1

    print()
    return passed, failed


# ── Part 2: live chat pipeline (slow, costs API credits) ─────────────────────

def test_live_chat(client: httpx.Client, base: str) -> tuple[int, int]:
    print(f"  {BOLD}Part 2 — Live Chat Pipeline{RESET}")
    print(f"  {DIM}POST /mcp/chat   (needs ANTHROPIC_API_KEY + TWELVEDATA_API_KEY){RESET}\n")

    passed = failed = 0

    for prompt, expected_dept in LIVE_CHAT_CASES:
        label = prompt[:60]
        print(f"  {CYAN}{label}{RESET}")

        try:
            t0 = time.monotonic()
            resp = client.post(
                f"{base}/mcp/chat",
                json={"message": prompt, "session_id": "smoke-test"},
                timeout=90,
            )
            elapsed = int((time.monotonic() - t0) * 1000)

            if resp.status_code != 200:
                print(f"  {TAG_FAIL}  HTTP {resp.status_code}  {DIM}{elapsed}ms{RESET}")
                print(f"  {DIM}{resp.text[:200]}{RESET}\n")
                failed += 1
                continue

            body = resp.json()
            dept = body.get("department", "???")
            text = body.get("response", "")
            dept_ok = dept == expected_dept
            has_text = len(text) > 20

            if dept_ok and has_text:
                preview = text[:120].replace("\n", " ")
                print(
                    f"  {TAG_PASS}  dept={dept}  {DIM}{elapsed}ms  "
                    f"{len(text)} chars{RESET}"
                )
                print(f"  {DIM}{preview}...{RESET}\n")
                passed += 1
            else:
                reasons = []
                if not dept_ok:
                    reasons.append(f"dept={dept} (expected {expected_dept})")
                if not has_text:
                    reasons.append(f"response too short ({len(text)} chars)")
                print(f"  {TAG_FAIL}  {'; '.join(reasons)}  {DIM}{elapsed}ms{RESET}\n")
                failed += 1

        except httpx.TimeoutException:
            print(f"  {TAG_FAIL}  Timed out (90s)\n")
            failed += 1
        except httpx.ConnectError:
            print(f"  {TAG_FAIL}  Connection refused\n")
            failed += 1
        except Exception as exc:
            print(f"  {TAG_FAIL}  {exc}\n")
            failed += 1

    return passed, failed


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke-test Maillard chat routing and live analyst pipeline"
    )
    parser.add_argument("--base", default=DEFAULT_BASE, help="Server base URL")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Also run live /mcp/chat tests (slow, uses API credits)",
    )
    args = parser.parse_args()

    print(f"\n{BOLD}Maillard Intelligence — Chat Routing Smoke Test{RESET}")
    print(f"{DIM}Target: {args.base}{RESET}\n")

    # ---- pre-flight ----
    try:
        httpx.get(f"{args.base}/", timeout=5)
    except httpx.ConnectError:
        print(f"{RED}Server not reachable at {args.base}{RESET}")
        print("Start it first:")
        print(f"  .venv/Scripts/python.exe -m uvicorn main:app --reload\n")
        sys.exit(1)
    except Exception:
        pass

    # ---- stale server detection ----
    try:
        spec = httpx.get(f"{args.base}/openapi.json", timeout=5)
        if spec.status_code == 200:
            paths = spec.json().get("paths", {})
            if "/mcp/route" not in paths:
                print(f"{RED}Server is running but /mcp/route is missing from OpenAPI spec.{RESET}")
                print(f"Restart the server to pick up current code.\n")
                sys.exit(1)
    except Exception:
        pass

    client = httpx.Client()
    total_pass = total_fail = 0

    # Part 1 — always
    p, f = test_routing(client, args.base)
    total_pass += p
    total_fail += f

    # Part 2 — only with --live
    if args.live:
        p, f = test_live_chat(client, args.base)
        total_pass += p
        total_fail += f
    else:
        print(f"  {DIM}Skipping live chat tests — use --live to enable.{RESET}\n")

    client.close()

    # ---- summary ----
    total = total_pass + total_fail

    print(f"{BOLD}{'=' * 56}{RESET}")
    print(f"  {BOLD}Total: {total}    Passed: {total_pass}    Failed: {total_fail}{RESET}")

    if total_fail == 0:
        print(f"  {GREEN}All routing checks passed.{RESET}")
    else:
        print(f"  {RED}{total_fail} check(s) failed.{RESET}")

    print()
    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
