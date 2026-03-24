"""
Data Merger for Maillard Coffee Roasters.

Merges POS + Shopify + QuickBooks into one sales_today.
Normalizes names. Sums quantities. No duplicates.

Usage:
  python scripts/merge_all_sales.py --pos data/sample_pos.csv --shopify data/sample_shopify.csv --qb data/sample_quickbooks.csv
  python scripts/merge_all_sales.py --pos data/sample_pos.csv --dry-run
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.ingest_pos import ingest_pos_csv
from scripts.ingest_shopify import ingest_shopify_csv
from scripts.ingest_quickbooks import ingest_quickbooks_csv

STATE_FILE = ROOT / "data" / "current_state.json"


def merge_sales(*sources: dict[str, int]) -> dict[str, int]:
    """Merge any number of sales dicts. Sums quantities. No duplicates.

    Usage:
        merged = merge_sales(square_data, shopify_data)
        merged = merge_sales(square_data, shopify_data, quickbooks_data)
    """
    merged: dict[str, int] = {}
    for source in sources:
        if not source:
            continue
        for product, qty in source.items():
            merged[product] = merged.get(product, 0) + qty
    return dict(sorted(merged.items(), key=lambda x: -x[1]))


def save_sales(sales: dict[str, int]) -> None:
    """Write merged sales into data/current_state.json -> sales_today."""
    state = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    state["sales_today"] = sales
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def merge_all_sales(
    pos_csv: str | None = None,
    shopify_csv: str | None = None,
    quickbooks_csv: str | None = None,
    qb_date: str | None = None,
) -> dict[str, int]:
    """Merge all sources into one sales_today dict.

    Names normalized. Quantities summed. No duplicates.
    """
    merged: dict[str, int] = {}

    if pos_csv:
        for product, qty in ingest_pos_csv(pos_csv).items():
            merged[product] = merged.get(product, 0) + qty

    if shopify_csv:
        for product, qty in ingest_shopify_csv(shopify_csv).items():
            merged[product] = merged.get(product, 0) + qty

    if quickbooks_csv:
        for product, qty in ingest_quickbooks_csv(quickbooks_csv, qb_date).items():
            merged[product] = merged.get(product, 0) + qty

    return dict(sorted(merged.items(), key=lambda x: -x[1]))


def save_merged(sales: dict[str, int]) -> None:
    """Write merged sales into data/current_state.json -> sales_today."""
    state = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    state["sales_today"] = sales
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def main():
    args = sys.argv[1:]
    pos = shopify = qb = None
    dry_run = "--dry-run" in args
    qb_date = None

    for i, a in enumerate(args):
        if a == "--pos" and i + 1 < len(args):
            pos = args[i + 1]
        if a == "--shopify" and i + 1 < len(args):
            shopify = args[i + 1]
        if a == "--qb" and i + 1 < len(args):
            qb = args[i + 1]
        if a == "--date" and i + 1 < len(args):
            qb_date = args[i + 1]

    if not any([pos, shopify, qb]):
        print("Usage: python scripts/merge_all_sales.py --pos FILE --shopify FILE --qb FILE [--date YYYY-MM-DD] [--dry-run]")
        sys.exit(1)

    sales = merge_all_sales(pos, shopify, qb, qb_date)

    sources = [s for s, f in [("POS", pos), ("Shopify", shopify), ("QuickBooks", qb)] if f]
    print(f"Merged {'+'.join(sources)}: {sum(sales.values())} units, {len(sales)} products:")
    for product, qty in sales.items():
        print(f"  {product:35s} {qty}")

    if dry_run:
        print("\n--dry-run: not saved.")
    else:
        save_merged(sales)
        print(f"\nSaved to {STATE_FILE}")


if __name__ == "__main__":
    main()
