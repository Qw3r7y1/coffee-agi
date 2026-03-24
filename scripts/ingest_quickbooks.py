"""
QuickBooks Connector (Read-Only) for Maillard Coffee Roasters.

Reads invoice CSV export from QuickBooks, extracts wholesale product sales,
and merges into data/current_state.json -> sales_today.

No write access to QuickBooks. No syncing. Optional use.

# TODO: Build live API connector (OAuth2 required)
# TODO: Endpoint: GET /v3/company/{id}/query?query=SELECT * FROM Invoice
# TODO: Extract Line[].SalesItemLineDetail for product + quantity

Usage:
  python scripts/ingest_quickbooks.py data/sample_quickbooks.csv
  python scripts/ingest_quickbooks.py data/sample_quickbooks.csv --dry-run
  python scripts/ingest_quickbooks.py data/sample_quickbooks.csv --date 2026-03-19
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from maillard.mcp.sales.normalization import normalize_product_name

STATE_FILE = ROOT / "data" / "current_state.json"


def ingest_quickbooks_csv(csv_path: str, date_filter: str | None = None) -> dict[str, int]:
    """Read a QuickBooks invoice CSV. Extract wholesale product quantities.

    Args:
        csv_path: Path to the CSV file.
        date_filter: Only include invoices from this date (YYYY-MM-DD). None = today.

    Returns:
        {product_slug: total_quantity}
    """
    if date_filter is None:
        date_filter = datetime.now().strftime("%Y-%m-%d")

    totals: dict[str, int] = {}

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Filter by date
            inv_date = (row.get("date") or "").strip()
            if date_filter and not inv_date.startswith(date_filter):
                continue

            raw_name = (row.get("description") or row.get("product") or "").strip()
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

            slug = f"wholesale_{product['slug']}"
            totals[slug] = totals.get(slug, 0) + qty

    return dict(sorted(totals.items(), key=lambda x: -x[1]))


def merge_into_state(wholesale_sales: dict[str, int]) -> dict[str, int]:
    """Merge wholesale sales into existing sales_today. Adds to totals."""
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    else:
        state = {}

    existing = state.get("sales_today", {})

    for product, qty in wholesale_sales.items():
        existing[product] = existing.get(product, 0) + qty

    state["sales_today"] = existing
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return existing


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/ingest_quickbooks.py <csv_file> [--dry-run] [--date YYYY-MM-DD]")
        sys.exit(1)

    csv_path = sys.argv[1]
    dry_run = "--dry-run" in sys.argv
    date_filter = None
    for i, arg in enumerate(sys.argv):
        if arg == "--date" and i + 1 < len(sys.argv):
            date_filter = sys.argv[i + 1]

    sales = ingest_quickbooks_csv(csv_path, date_filter)

    print(f"QuickBooks: {sum(sales.values())} wholesale units across {len(sales)} products:")
    for product, qty in sales.items():
        print(f"  {product:35s} +{qty}")

    if dry_run:
        print("\n--dry-run: not saved.")
    else:
        merged = merge_into_state(sales)
        print(f"\nMerged into {STATE_FILE}")


if __name__ == "__main__":
    main()
