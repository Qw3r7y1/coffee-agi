"""
Query sales data from SQLite (data/sales.db).

Usage:
  python scripts/query_sales.py                     # today
  python scripts/query_sales.py --date 2026-03-24   # specific date
  python scripts/query_sales.py --yesterday         # yesterday
  python scripts/query_sales.py --compare           # today vs yesterday
  python scripts/query_sales.py --top 10            # top N items today
  python scripts/query_sales.py --status            # sync diagnostics
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

from maillard import sales_db

_LOCATION = os.getenv("SQUARE_LOCATION_ID", "")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _yesterday() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def show_day(date: str) -> dict:
    summary = sales_db.get_daily_summary(date, _LOCATION)
    products = sales_db.get_product_sales(date)
    order_count = sales_db.get_order_count(date, _LOCATION)
    return {
        "date": date,
        "orders": order_count,
        "gross_sales_cents": summary["gross_sales"] if summary else 0,
        "avg_ticket_cents": int(summary["avg_ticket"]) if summary else 0,
        "products": products,
    }


def show_status() -> dict:
    last = sales_db.get_last_sync()
    conn = sales_db._connect()
    total_orders = conn.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"]
    total_items = conn.execute("SELECT COUNT(*) AS c FROM order_items").fetchone()["c"]
    dates = conn.execute("SELECT DISTINCT business_date FROM orders ORDER BY business_date DESC LIMIT 10").fetchall()
    conn.close()
    return {
        "last_sync": dict(last) if last else None,
        "total_orders": total_orders,
        "total_items": total_items,
        "recent_dates": [d["business_date"] for d in dates],
        "db_path": str(sales_db._DB_PATH),
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Query sales from SQLite")
    p.add_argument("--date", help="Business date YYYY-MM-DD")
    p.add_argument("--yesterday", action="store_true")
    p.add_argument("--compare", action="store_true", help="Compare today vs yesterday")
    p.add_argument("--top", type=int, default=0, help="Show top N items")
    p.add_argument("--status", action="store_true", help="Show sync diagnostics")
    args = p.parse_args()

    sales_db.init_db()

    if args.status:
        print(json.dumps(show_status(), indent=2, default=str))
    elif args.compare:
        result = sales_db.compare_days(_today(), _yesterday())
        print(json.dumps(result, indent=2, default=str))
    elif args.top:
        date = args.date or _today()
        products = sales_db.get_product_sales(date)[:args.top]
        print(json.dumps({"date": date, "top_items": products}, indent=2, default=str))
    else:
        date = _yesterday() if args.yesterday else (args.date or _today())
        print(json.dumps(show_day(date), indent=2, default=str))
