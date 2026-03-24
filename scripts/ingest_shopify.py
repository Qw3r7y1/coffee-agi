"""
Shopify Sales Parser for Maillard Coffee Roasters.

Reads a Shopify orders CSV, normalizes product names,
aggregates totals, and MERGES into data/current_state.json -> sales_today.

Does NOT overwrite POS data -- adds to existing totals.

Usage:
  python scripts/ingest_shopify.py data/sample_shopify.csv
  python scripts/ingest_shopify.py data/sample_shopify.csv --dry-run
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from maillard.mcp.sales.normalization import normalize_product_name

STATE_FILE = ROOT / "data" / "current_state.json"


def ingest_shopify_csv(csv_path: str) -> dict[str, int]:
    """Read a Shopify CSV, normalize names, aggregate quantities.

    Tries columns in order: title, variant, product_name, line_item_name.
    """
    totals: dict[str, int] = {}

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = (
                row.get("title") or row.get("variant") or
                row.get("product_name") or row.get("line_item_name") or ""
            ).strip()
            if not raw_name:
                continue

            raw_qty = (row.get("quantity") or "1").strip()
            try:
                qty = int(float(raw_qty)) if raw_qty else 1
            except (ValueError, TypeError):
                qty = 1

            product = normalize_product_name(raw_name)
            if product["slug"] == "unknown":
                continue

            totals[product["slug"]] = totals.get(product["slug"], 0) + qty

    return dict(sorted(totals.items(), key=lambda x: -x[1]))


def merge_into_state(shopify_sales: dict[str, int]) -> dict[str, int]:
    """Merge Shopify sales into existing sales_today. Adds to POS totals."""
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    else:
        state = {}

    existing = state.get("sales_today", {})

    for product, qty in shopify_sales.items():
        existing[product] = existing.get(product, 0) + qty

    state["sales_today"] = existing
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return existing


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/ingest_shopify.py <csv_file> [--dry-run]")
        sys.exit(1)

    csv_path = sys.argv[1]
    dry_run = "--dry-run" in sys.argv

    sales = ingest_shopify_csv(csv_path)

    print(f"Shopify: {sum(sales.values())} units across {len(sales)} products:")
    for product, qty in sales.items():
        print(f"  {product:25s} +{qty}")

    if dry_run:
        print("\n--dry-run: not saved.")
    else:
        merged = merge_into_state(sales)
        print(f"\nMerged into {STATE_FILE}. Combined sales_today:")
        for product, qty in sorted(merged.items(), key=lambda x: -x[1]):
            print(f"  {product:25s} {qty}")


if __name__ == "__main__":
    main()
