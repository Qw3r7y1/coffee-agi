"""
Daily Sales Sync for Maillard Coffee Roasters.

Fetches from Square + Shopify, merges, saves to current_state.json.
Falls back gracefully if a source is unavailable.

Usage:
  python scripts/sync_data.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.merge_all_sales import merge_sales, save_sales


def main():
    square = {}
    shopify = {}

    # Square
    try:
        from scripts.square_connector import get_square_sales
        square = get_square_sales()
        print(f"Square:  {sum(square.values())} units, {len(square)} products")
    except Exception as e:
        print(f"Square:  skipped ({e})")

    # Shopify
    try:
        from scripts.shopify_connector import get_shopify_sales
        shopify = get_shopify_sales()
        print(f"Shopify: {sum(shopify.values())} units, {len(shopify)} products")
    except Exception as e:
        print(f"Shopify: skipped ({e})")

    if not square and not shopify:
        print("\nNo data fetched. Check API credentials.")
        sys.exit(1)

    merged = merge_sales(square, shopify)
    save_sales(merged)

    print(f"\nSales updated successfully")
    print(f"Total: {sum(merged.values())} units, {len(merged)} products")


if __name__ == "__main__":
    main()
