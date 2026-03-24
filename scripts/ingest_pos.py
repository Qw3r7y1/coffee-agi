"""
POS Ingestion Script for Maillard Coffee Roasters.

Reads a CSV from Square / Simphony, normalizes product names,
aggregates totals, and writes into data/current_state.json -> sales_today.

Usage:
  python scripts/ingest_pos.py data/sample_pos.csv
  python scripts/ingest_pos.py data/sample_pos.csv --dry-run
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


def ingest_pos_csv(csv_path: str) -> dict[str, int]:
    """Read a POS CSV, normalize names, aggregate quantities per product.

    Expects columns: item_name, quantity (others ignored).
    Handles missing values safely.
    """
    totals: dict[str, int] = {}

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_name = (row.get("item_name") or "").strip()
            if not raw_name:
                continue

            raw_qty = row.get("quantity", "1").strip()
            try:
                qty = int(float(raw_qty)) if raw_qty else 1
            except (ValueError, TypeError):
                qty = 1

            product = normalize_product_name(raw_name)
            slug = product["slug"]

            if slug == "unknown":
                continue

            totals[slug] = totals.get(slug, 0) + qty

    return dict(sorted(totals.items(), key=lambda x: -x[1]))


def save_to_state(sales: dict[str, int]) -> None:
    """Write aggregated sales into data/current_state.json -> sales_today."""
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    else:
        state = {}

    state["sales_today"] = sales
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/ingest_pos.py <csv_file> [--dry-run]")
        sys.exit(1)

    csv_path = sys.argv[1]
    dry_run = "--dry-run" in sys.argv

    sales = ingest_pos_csv(csv_path)

    print(f"Ingested {sum(sales.values())} units across {len(sales)} products:")
    for product, qty in sales.items():
        print(f"  {product:25s} {qty}")

    if dry_run:
        print("\n--dry-run: not saved.")
    else:
        save_to_state(sales)
        print(f"\nSaved to {STATE_FILE}")


if __name__ == "__main__":
    main()
