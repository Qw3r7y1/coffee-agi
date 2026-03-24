"""
Square POS Sales Connector for Maillard Coffee Roasters — read-only.

Uses the official Square SDK (v44+) to pull completed orders and return
structured JSON.  No sandbox, no seed data, no OAuth, no write-back.

Credentials from .env:
  SQUARE_ACCESS_TOKEN   — production personal access token
  SQUARE_LOCATION_ID    — Square location ID (starts with L…)
  SQUARE_ENV            — must be "production"

CLI:
  python scripts/square_sales_connector.py                          # today
  python scripts/square_sales_connector.py --start 2026-03-01       # range
  python scripts/square_sales_connector.py --validate               # check creds + location
  python scripts/square_sales_connector.py --save                   # write to current_state.json
"""
from __future__ import annotations

import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

from square import Square
from square.environment import SquareEnvironment

from maillard.mcp.sales.normalization import normalize_product_name

# ---------------------------------------------------------------------------
# Logging — suppress noisy httpx/httpcore chatter
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("square_sales")

STATE_FILE = ROOT / "data" / "current_state.json"

# ---------------------------------------------------------------------------
# Config from .env
# ---------------------------------------------------------------------------
_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
_LOCATION = os.getenv("SQUARE_LOCATION_ID", "")
_ENV = os.getenv("SQUARE_ENV", "production")


def _mask(secret: str) -> str:
    """Show first 6 and last 4 chars only."""
    if len(secret) <= 12:
        return "***"
    return f"{secret[:6]}…{secret[-4:]}"


log.info("token loaded: %s  (%s)", "YES" if _TOKEN else "NO", _mask(_TOKEN) if _TOKEN else "—")
log.info("location loaded: %s  (%s)", "YES" if _LOCATION else "NO", _LOCATION or "—")
log.info("environment: %s", _ENV)


# ---------------------------------------------------------------------------
# Safety checks
# ---------------------------------------------------------------------------
def _check_env() -> str | None:
    """Return error string if anything is wrong, else None."""
    if not _TOKEN:
        return "SQUARE_ACCESS_TOKEN is not set in .env"
    if not _LOCATION:
        return "SQUARE_LOCATION_ID is not set in .env — run --validate to discover it"
    if _ENV.lower() != "production":
        return f"SQUARE_ENV={_ENV!r} — only 'production' is allowed"
    if _TOKEN.lower().startswith("sandbox-"):
        return "Token starts with 'sandbox-' — use a production token"
    return None


def _abort(msg: str) -> dict:
    log.error("ABORT: %s", msg)
    return {"error": msg}


# ---------------------------------------------------------------------------
# SDK client
# ---------------------------------------------------------------------------
def _client() -> Square:
    return Square(token=_TOKEN, environment=SquareEnvironment.PRODUCTION)


# ---------------------------------------------------------------------------
# Validate connection + location
# ---------------------------------------------------------------------------
def validate() -> dict:
    """Check token works, list locations, confirm SQUARE_LOCATION_ID is valid.

    Returns {"ok": True, "locations": [...], "active_location": ...}
    or      {"ok": False, "error": ...}
    """
    if not _TOKEN:
        return {"ok": False, "error": "SQUARE_ACCESS_TOKEN not set"}

    try:
        result = _client().locations.list()
    except Exception as e:
        return {"ok": False, "error": f"ListLocations failed: {_clean_error(e)}"}

    locations = result.locations or []
    if not locations:
        return {"ok": False, "error": "No locations returned — check token permissions (MERCHANT_PROFILE_READ)"}

    locs = [{"id": loc.id, "name": loc.name, "status": loc.status} for loc in locations]
    active = [l for l in locs if l["status"] == "ACTIVE"]

    if _LOCATION:
        matched = [l for l in locs if l["id"] == _LOCATION]
        if not matched:
            return {
                "ok": False,
                "error": f"SQUARE_LOCATION_ID={_LOCATION} not found. Available: {[l['id'] for l in locs]}",
                "locations": locs,
            }
        if matched[0]["status"] != "ACTIVE":
            return {"ok": False, "error": f"Location {_LOCATION} exists but is {matched[0]['status']}", "locations": locs}
        return {"ok": True, "locations": locs, "active_location": matched[0]}

    # No location configured — suggest the active one
    if active:
        return {
            "ok": False,
            "error": f"SQUARE_LOCATION_ID not set. Add this to .env: SQUARE_LOCATION_ID={active[0]['id']}",
            "locations": locs,
        }
    return {"ok": False, "error": "No ACTIVE locations found", "locations": locs}


# ---------------------------------------------------------------------------
# Fetch orders for a date range
# ---------------------------------------------------------------------------
def fetch_orders(start_date: str | None = None, end_date: str | None = None) -> dict:
    """Fetch completed orders and return structured JSON.

    Returns:
        {
            "sales_today": {slug: qty},
            "sales_amounts": {slug: cents},
            "top_items": [{name, display, variation, qty, revenue_cents}],
            "raw_order_count": int,
            "period": {"start": ..., "end": ...},
        }
    """
    err = _check_env()
    if err:
        return _abort(err)

    # Resolve dates
    now = datetime.now(timezone.utc)
    if not start_date:
        start_date = now.strftime("%Y-%m-%d")
    if not end_date:
        end_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    if "T" not in start_date:
        start_date = f"{start_date}T00:00:00Z"
    if "T" not in end_date:
        end_date = f"{end_date}T00:00:00Z"

    log.info("SearchOrders %s -> %s  location=%s", start_date, end_date, _LOCATION)

    client = _client()
    query = {
        "filter": {
            "state_filter": {"states": ["COMPLETED"]},
            "date_time_filter": {
                "closed_at": {"start_at": start_date, "end_at": end_date}
            },
        },
        "sort": {"sort_field": "CLOSED_AT", "sort_order": "DESC"},
    }

    all_orders: list = []
    tally: dict[str, dict] = defaultdict(
        lambda: {"qty": 0, "revenue_cents": 0, "display": "", "variation": ""}
    )
    cursor: str | None = None

    while True:
        try:
            result = client.orders.search(
                location_ids=[_LOCATION],
                limit=100,
                query=query,
                cursor=cursor,
            )
        except Exception as e:
            return _abort(f"SearchOrders failed: {_clean_error(e)}")

        orders = result.orders or []
        if not orders:
            break

        for order in orders:
            all_orders.append(order)
            for li in order.line_items or []:
                name = (li.name or "").strip()
                if not name:
                    continue
                product = normalize_product_name(name)
                slug = product["slug"]
                if slug == "unknown":
                    continue

                try:
                    qty = int(float(li.quantity or "1"))
                except (ValueError, TypeError):
                    qty = 1

                cents = li.base_price_money.amount if li.base_price_money else 0

                tally[slug]["qty"] += qty
                tally[slug]["revenue_cents"] += cents * qty
                tally[slug]["display"] = product["display"]
                tally[slug]["variation"] = li.variation_name or ""

        cursor = result.cursor
        if not cursor:
            break

    log.info("orders fetched: %d", len(all_orders))

    # Build output
    sales_today: dict[str, int] = {}
    sales_amounts: dict[str, int] = {}
    top_items: list[dict] = []

    for slug, d in tally.items():
        sales_today[slug] = d["qty"]
        sales_amounts[slug] = d["revenue_cents"]
        top_items.append({
            "name": slug,
            "display": d["display"],
            "variation": d["variation"],
            "qty": d["qty"],
            "revenue_cents": d["revenue_cents"],
        })

    top_items.sort(key=lambda x: -x["qty"])

    return {
        "sales_today": dict(sorted(sales_today.items(), key=lambda x: -x[1])),
        "sales_amounts": dict(sorted(sales_amounts.items(), key=lambda x: -x[1])),
        "top_items": top_items,
        "raw_order_count": len(all_orders),
        "period": {"start": start_date, "end": end_date},
    }


# ---------------------------------------------------------------------------
# State wiring
# ---------------------------------------------------------------------------
def merge_into_state(report: dict) -> None:
    """Write all Square sales keys into current_state.json (replaces sales, keeps inventory)."""
    state: dict = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    state["sales_today"] = report.get("sales_today", {})
    state["sales_amounts"] = report.get("sales_amounts", {})
    state["top_items"] = report.get("top_items", [])
    state["raw_order_count"] = report.get("raw_order_count", 0)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    log.info("saved %d products, %d orders to %s",
             len(state["sales_today"]), state["raw_order_count"], STATE_FILE)


# ---------------------------------------------------------------------------
# Error helper
# ---------------------------------------------------------------------------
def _clean_error(exc: Exception) -> str:
    """Extract the useful part from Square SDK exceptions."""
    s = str(exc)
    # SDK dumps headers + body — just grab the body errors
    if "'errors'" in s:
        try:
            idx = s.find("body:")
            if idx >= 0:
                raw = s[idx + 5:].strip()
                body = json.loads(raw.replace("'", '"'))
                parts = [f"{e['code']}: {e['detail']}" for e in body.get("errors", [])]
                if parts:
                    return "; ".join(parts)
        except Exception:
            pass
    return s[:200]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Maillard — Square sales connector (read-only)")
    p.add_argument("--start", help="Start date YYYY-MM-DD (default: today)")
    p.add_argument("--end", help="End date YYYY-MM-DD (default: tomorrow)")
    p.add_argument("--days", type=int, help="Fetch last N days instead of --start/--end")
    p.add_argument("--validate", action="store_true", help="Check credentials and list locations")
    p.add_argument("--save", action="store_true", help="Merge results into current_state.json")
    args = p.parse_args()

    if args.validate:
        print(json.dumps(validate(), indent=2))
        sys.exit(0)

    # --days shorthand
    start = args.start
    end = args.end
    if args.days:
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=args.days)).strftime("%Y-%m-%d")
        end = (now + timedelta(days=1)).strftime("%Y-%m-%d")

    report = fetch_orders(start_date=start, end_date=end)
    print(json.dumps(report, indent=2))

    if "error" in report:
        sys.exit(1)

    if args.save and report.get("sales_today"):
        merge_into_state(report)
        print(f"\nSaved to {STATE_FILE}")
