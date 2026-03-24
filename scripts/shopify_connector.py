"""
Shopify Admin API Connector for Maillard Coffee Roasters.

Fetches today's orders, normalizes product names, aggregates totals.

# TODO: Add webhook listener for real-time order updates
# TODO: Add inventory sync via Shopify Inventory API
# TODO: Add fulfillment status tracking

Setup:
  SHOPIFY_STORE=your-store.myshopify.com
  SHOPIFY_ACCESS_TOKEN=shpat_xxxxx

Usage:
  python scripts/shopify_connector.py
  python scripts/shopify_connector.py --save
  python scripts/shopify_connector.py --dry-run
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

from maillard.mcp.sales.normalization import normalize_product_name

STATE_FILE = ROOT / "data" / "current_state.json"
API_VERSION = "2024-01"


def get_shopify_sales(
    store: str | None = None,
    access_token: str | None = None,
) -> dict[str, int]:
    """Fetch today's orders from Shopify Admin API.

    Args:
        store: e.g. "maillard.myshopify.com". Falls back to SHOPIFY_STORE env.
        access_token: Shopify Admin token. Falls back to SHOPIFY_ACCESS_TOKEN env.

    Returns:
        {"ethiopia_yirgacheffe": 10, "espresso_blend": 5, ...}
    """
    store = store or os.getenv("SHOPIFY_STORE", "")
    token = access_token or os.getenv("SHOPIFY_ACCESS_TOKEN", "")

    if not store or not token:
        raise ValueError("Set SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN env vars.")

    base = f"https://{store}/admin/api/{API_VERSION}"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

    # Today midnight UTC
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    totals: dict[str, int] = {}
    url = f"{base}/orders.json"
    params: dict = {
        "status": "any",
        "created_at_min": today.isoformat(),
        "limit": 250,
    }

    while url:
        resp = httpx.get(url, headers=headers, params=params, timeout=15)

        if resp.status_code != 200:
            raise RuntimeError(f"Shopify API {resp.status_code}: {resp.text[:200]}")

        data = resp.json()

        for order in data.get("orders", []):
            # Skip cancelled / refunded
            if order.get("cancelled_at"):
                continue
            if order.get("financial_status") in ("refunded", "voided"):
                continue

            for item in order.get("line_items", []):
                raw_name = (item.get("title") or item.get("name") or "").strip()
                if not raw_name:
                    continue

                try:
                    qty = int(item.get("quantity", 1))
                except (ValueError, TypeError):
                    qty = 1

                product = normalize_product_name(raw_name)
                if product["slug"] == "unknown":
                    continue

                totals[product["slug"]] = totals.get(product["slug"], 0) + qty

        # Pagination via Link header
        url = None
        params = {}
        link = resp.headers.get("Link", "")
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]
                    break

    return dict(sorted(totals.items(), key=lambda x: -x[1]))


def merge_into_state(sales: dict[str, int]) -> None:
    """Merge Shopify sales into existing sales_today."""
    state = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))

    existing = state.get("sales_today", {})
    for product, qty in sales.items():
        existing[product] = existing.get(product, 0) + qty

    state["sales_today"] = existing
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def main():
    save = "--save" in sys.argv
    dry_run = "--dry-run" in sys.argv

    if not os.getenv("SHOPIFY_STORE") or not os.getenv("SHOPIFY_ACCESS_TOKEN"):
        print("SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN not set.")
        print("Set with:")
        print("  export SHOPIFY_STORE=your-store.myshopify.com")
        print("  export SHOPIFY_ACCESS_TOKEN=shpat_xxxxx")
        print("\nTo test without Shopify: python scripts/ingest_shopify.py data/sample_shopify.csv")
        sys.exit(1)

    try:
        sales = get_shopify_sales()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"Shopify: {sum(sales.values())} units, {len(sales)} products:")
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
