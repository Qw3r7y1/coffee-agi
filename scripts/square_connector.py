"""
Square API Connector for Maillard Coffee Roasters.

Fetches today's orders from Square, normalizes product names,
aggregates totals.

# TODO: Add webhook listener for real-time order updates
# TODO: Add inventory sync via Square Inventory API
# TODO: Add refund reconciliation

Setup:
  Set in .env: SQUARE_ACCESS_TOKEN=your_token
  Optionally:  SQUARE_LOCATION_ID=your_location

Usage:
  python scripts/square_connector.py                # fetch + print
  python scripts/square_connector.py --save         # fetch + merge into state
  python scripts/square_connector.py --debug        # diagnose connection issues
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env so standalone script gets credentials
from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

from maillard.mcp.sales.normalization import normalize_product_name

STATE_FILE = ROOT / "data" / "current_state.json"

SQUARE_BASE = "https://connect.squareup.com"
SQUARE_SANDBOX = "https://connect.squareupsandbox.com"


# =============================================================================
# CORE FUNCTION
# =============================================================================


def get_square_sales(
    access_token: str | None = None,
    location_id: str | None = None,
    sandbox: bool = False,
) -> dict[str, int]:
    """Fetch today's orders from Square and return normalized sales.

    Returns:
        {"latte": 42, "espresso": 18, ...}
    """
    token = access_token or os.getenv("SQUARE_ACCESS_TOKEN", "")
    loc_id = location_id or os.getenv("SQUARE_LOCATION_ID", "")
    base = SQUARE_SANDBOX if sandbox else SQUARE_BASE

    if not token:
        raise ValueError("No Square access token. Set SQUARE_ACCESS_TOKEN in .env")

    # Today's date range (UTC midnight to midnight)
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    body: dict = {
        "query": {
            "filter": {
                "date_time_filter": {
                    "created_at": {
                        "start_at": start.isoformat(),
                        "end_at": end.isoformat(),
                    }
                },
                "state_filter": {
                    "states": ["COMPLETED"]
                },
            },
            "sort": {
                "sort_field": "CREATED_AT",
                "sort_order": "DESC",
            },
        },
        "limit": 500,
    }

    if loc_id:
        body["location_ids"] = [loc_id]

    all_orders = []
    cursor = None

    while True:
        if cursor:
            body["cursor"] = cursor

        resp = httpx.post(
            f"{base}/v2/orders/search",
            json=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Square-Version": "2024-01-18",
            },
            timeout=15,
        )

        if resp.status_code != 200:
            try:
                error = resp.json().get("errors", [{}])[0].get("detail", "")
            except Exception:
                error = resp.text[:200]
            raise RuntimeError(f"Square API error {resp.status_code}: {error}")

        data = resp.json()
        all_orders.extend(data.get("orders", []))

        cursor = data.get("cursor")
        if not cursor:
            break

    # Extract and normalize
    totals: dict[str, int] = {}

    for order in all_orders:
        if order.get("state") != "COMPLETED":
            continue
        if order.get("refunds"):
            continue

        for item in order.get("line_items", []):
            raw_name = (item.get("name") or "").strip()
            if not raw_name:
                continue

            raw_qty = item.get("quantity", "1")
            try:
                qty = int(float(raw_qty))
            except (ValueError, TypeError):
                qty = 1

            product = normalize_product_name(raw_name)
            if product["slug"] == "unknown":
                continue

            totals[product["slug"]] = totals.get(product["slug"], 0) + qty

    return dict(sorted(totals.items(), key=lambda x: -x[1]))


def get_today_sales() -> dict:
    """Safe wrapper: returns {} on any failure."""
    try:
        return get_square_sales()
    except Exception:
        return {}


# =============================================================================
# DEBUG / DIAGNOSE
# =============================================================================


def test_square_connection() -> dict:
    """Diagnose the Square connection step by step. Prints everything."""
    print("=" * 50)
    print("Square Connection Debug")
    print("=" * 50)

    result = {"steps": []}

    # 1. Credentials
    token = os.getenv("SQUARE_ACCESS_TOKEN", "")
    loc_id = os.getenv("SQUARE_LOCATION_ID", "")

    print(f"\n1. CREDENTIALS")
    if token:
        print(f"   Token: loaded ({len(token)} chars, starts with {token[:8]}...)")
        result["token_loaded"] = True
    else:
        print(f"   Token: NOT SET")
        print(f"   Fix: add SQUARE_ACCESS_TOKEN=xxx to .env file")
        result["token_loaded"] = False
        result["diagnosis"] = "No access token. Add SQUARE_ACCESS_TOKEN to .env"
        return result

    if loc_id:
        print(f"   Location: {loc_id}")
        result["location_loaded"] = True
    else:
        print(f"   Location: NOT SET (will search all locations)")
        result["location_loaded"] = False

    # 2. Environment
    base = SQUARE_BASE
    print(f"\n2. ENVIRONMENT")
    print(f"   URL: {base}")
    print(f"   Endpoint: {base}/v2/orders/search")

    # 3. Date range
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    print(f"\n3. DATE RANGE")
    print(f"   Now (UTC): {now.isoformat()}")
    print(f"   Start: {start.isoformat()}")
    print(f"   End: {end.isoformat()}")
    print(f"   Local time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # Check if local timezone might cause issues
    local_hour = datetime.now().hour
    utc_hour = now.hour
    tz_diff = local_hour - utc_hour
    if tz_diff != 0:
        print(f"   WARNING: Local is UTC{'+' if tz_diff > 0 else ''}{tz_diff}h. Sales made locally after midnight UTC may not appear until next day.")

    # 4. API call
    body = {
        "query": {
            "filter": {
                "date_time_filter": {
                    "created_at": {
                        "start_at": start.isoformat(),
                        "end_at": end.isoformat(),
                    }
                },
                "state_filter": {"states": ["COMPLETED"]},
            },
        },
        "limit": 10,
    }
    if loc_id:
        body["location_ids"] = [loc_id]

    print(f"\n4. API REQUEST")
    print(f"   POST {base}/v2/orders/search")
    print(f"   Auth: Bearer {token[:8]}...{token[-4:]}")
    if loc_id:
        print(f"   Location filter: {loc_id}")

    try:
        resp = httpx.post(
            f"{base}/v2/orders/search",
            json=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Square-Version": "2024-01-18",
            },
            timeout=15,
        )

        print(f"\n5. RESPONSE")
        print(f"   HTTP {resp.status_code}")
        result["http_status"] = resp.status_code

        data = resp.json()

        if resp.status_code != 200:
            errors = data.get("errors", [])
            for err in errors:
                code = err.get("code", "?")
                detail = err.get("detail", "?")
                cat = err.get("category", "?")
                print(f"   ERROR: [{cat}] {code}: {detail}")

                if code == "UNAUTHORIZED":
                    print(f"\n   DIAGNOSIS: Token is invalid or expired.")
                    print(f"   Fix: Generate a new token at https://developer.squareup.com")
                    result["diagnosis"] = "Auth failed. Token invalid or expired."
                elif code == "NOT_FOUND":
                    print(f"\n   DIAGNOSIS: Location ID not found.")
                    print(f"   Fix: Check SQUARE_LOCATION_ID in .env")
                    result["diagnosis"] = "Location not found."
                else:
                    result["diagnosis"] = f"{code}: {detail}"
            return result

        orders = data.get("orders", [])
        result["order_count"] = len(orders)
        print(f"   Orders returned: {len(orders)}")

        if not orders:
            print(f"\n   NO ORDERS FOUND.")
            print(f"   Possible reasons:")
            print(f"   - No completed orders today (UTC: {start.strftime('%Y-%m-%d')})")
            print(f"   - Location ID filters out all orders")
            print(f"   - Orders are in a different state (not COMPLETED)")
            print(f"   - Timezone mismatch: your local day vs UTC day")
            result["diagnosis"] = "No orders returned. Check date range and location."
        else:
            # Show sample
            print(f"\n6. SAMPLE DATA (first order)")
            o = orders[0]
            print(f"   Order ID: {o.get('id', '?')[:20]}")
            print(f"   State: {o.get('state')}")
            print(f"   Created: {o.get('created_at', '?')}")
            items = o.get("line_items", [])
            print(f"   Line items: {len(items)}")
            for item in items[:5]:
                name = item.get("name", "?")
                qty = item.get("quantity", "?")
                slug = normalize_product_name(name)["slug"]
                print(f"     \"{name}\" x{qty} -> {slug}")

            result["diagnosis"] = f"OK. {len(orders)} orders found."
            result["sample_items"] = [item.get("name") for item in items[:5]]

    except httpx.ConnectError as e:
        print(f"\n5. CONNECTION FAILED")
        print(f"   Cannot reach {base}")
        print(f"   Error: {e}")
        result["diagnosis"] = "Cannot connect to Square API. Check internet."
    except httpx.TimeoutException:
        print(f"\n5. TIMEOUT")
        print(f"   Request timed out after 15s")
        result["diagnosis"] = "API timeout."
    except Exception as e:
        print(f"\n5. UNEXPECTED ERROR")
        print(f"   {type(e).__name__}: {e}")
        result["diagnosis"] = str(e)[:100]

    print(f"\n{'=' * 50}")
    print(f"DIAGNOSIS: {result.get('diagnosis', 'Unknown')}")
    print(f"{'=' * 50}")
    return result


# =============================================================================
# MERGE + MAIN
# =============================================================================


def merge_into_state(sales: dict[str, int]) -> None:
    state = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    existing = state.get("sales_today", {})
    for product, qty in sales.items():
        existing[product] = existing.get(product, 0) + qty
    state["sales_today"] = existing
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def main():
    if "--debug" in sys.argv:
        test_square_connection()
        return

    save = "--save" in sys.argv
    dry_run = "--dry-run" in sys.argv
    sandbox = "--sandbox" in sys.argv

    token = os.getenv("SQUARE_ACCESS_TOKEN", "")
    if not token:
        print("SQUARE_ACCESS_TOKEN not set. Run with --debug to diagnose.")
        sys.exit(1)

    try:
        sales = get_square_sales(sandbox=sandbox)
    except Exception as e:
        print(f"Error: {e}")
        print("Run with --debug for detailed diagnosis.")
        sys.exit(1)

    print(f"Square: {sum(sales.values())} units across {len(sales)} products:")
    for product, qty in sales.items():
        print(f"  {product:25s} {qty}")

    if save and not dry_run:
        merge_into_state(sales)
        print(f"\nMerged into {STATE_FILE}")
    elif dry_run:
        print("\n--dry-run: not saved.")
    else:
        print("\nUse --save to write to current_state.json")


if __name__ == "__main__":
    main()
