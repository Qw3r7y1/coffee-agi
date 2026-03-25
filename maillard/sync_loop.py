"""
Background sales sync loop — runs inside the FastAPI process.

Calls Square every 60 seconds, writes to SQLite, refreshes the JSON snapshot.
All reads from the frontend/chat hit the cached snapshot — never Square directly.

Usage:
    from maillard.sync_loop import start_sync, get_sales_sync_status
    start_sync()  # call once at app startup
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("sync_loop")

ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = ROOT / "data" / "current_state.json"

# ---------------------------------------------------------------------------
# Sync metadata (module-level, thread-safe for reads)
# ---------------------------------------------------------------------------
_meta = {
    "status": "starting",
    "last_successful_sync": None,
    "last_attempted_sync": None,
    "data_age_seconds": 0,
    "orders_synced": 0,
    "products_synced": 0,
    "error_message": "",
    "consecutive_failures": 0,
}


def get_sales_sync_status() -> dict:
    """Return current sync metadata. Safe to call from any thread."""
    now = datetime.now(timezone.utc)
    age = 0
    if _meta["last_successful_sync"]:
        try:
            last = datetime.fromisoformat(_meta["last_successful_sync"])
            age = round((now - last).total_seconds())
        except Exception:
            pass

    if _meta["status"] == "syncing":
        status = "syncing"
    elif _meta["last_successful_sync"] is None:
        status = "starting"
    elif age <= 90:
        status = "live"
    elif age <= 180:
        status = "delayed"
    else:
        status = "stale"

    if _meta["consecutive_failures"] > 0 and _meta["status"] != "syncing":
        status = "error"

    return {
        "status": status,
        "last_successful_sync": _meta["last_successful_sync"],
        "last_attempted_sync": _meta["last_attempted_sync"],
        "data_age_seconds": age,
        "orders_synced": _meta["orders_synced"],
        "products_synced": _meta["products_synced"],
        "error_message": _meta["error_message"],
        "consecutive_failures": _meta["consecutive_failures"],
    }


# ---------------------------------------------------------------------------
# Single sync cycle (runs in background thread)
# ---------------------------------------------------------------------------
def _do_sync() -> None:
    """One sync cycle: Square → SQLite → JSON snapshot."""
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)

    token = os.getenv("SQUARE_ACCESS_TOKEN", "")
    location = os.getenv("SQUARE_LOCATION_ID", "")
    env = os.getenv("SQUARE_ENV", "production")

    if not token or not location or env != "production":
        _meta["status"] = "error"
        _meta["error_message"] = "Missing SQUARE_ACCESS_TOKEN, SQUARE_LOCATION_ID, or SQUARE_ENV!=production"
        _meta["consecutive_failures"] += 1
        return

    _meta["status"] = "syncing"
    _meta["last_attempted_sync"] = datetime.now(timezone.utc).isoformat()

    try:
        from square import Square
        from square.environment import SquareEnvironment
        from maillard.mcp.sales.normalization import normalize_product_name
        from maillard import sales_db

        sales_db.init_db()

        # Fetch last 3h from Square
        client = Square(token=token, environment=SquareEnvironment.PRODUCTION)
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(hours=3)
        window_end = now + timedelta(minutes=5)

        query = {
            "filter": {
                "state_filter": {"states": ["COMPLETED"]},
                "date_time_filter": {
                    "updated_at": {
                        "start_at": window_start.isoformat(),
                        "end_at": window_end.isoformat(),
                    }
                },
            },
            "sort": {"sort_field": "UPDATED_AT", "sort_order": "DESC"},
        }

        all_orders = []
        cursor = None
        while True:
            result = client.orders.search(
                location_ids=[location], limit=100, query=query, cursor=cursor,
            )
            orders = result.orders or []
            if not orders:
                break
            all_orders.extend(orders)
            cursor = result.cursor
            if not cursor:
                break

        # Upsert into SQLite
        conn = sales_db._connect()
        synced_at = now.isoformat()
        orders_upserted = 0
        items_upserted = 0
        dates_affected = set()

        for order in all_orders:
            oid = order.id
            closed_at = order.closed_at if isinstance(order.closed_at, str) else (order.closed_at.isoformat() if order.closed_at else None)
            created_at = order.created_at if isinstance(order.created_at, str) else (order.created_at.isoformat() if order.created_at else None)
            updated_at = order.updated_at if isinstance(order.updated_at, str) else (order.updated_at.isoformat() if order.updated_at else None)

            ref = closed_at or created_at or now.isoformat()
            biz_date = ref[:10]
            dates_affected.add(biz_date)

            total = order.total_money.amount if order.total_money else 0
            tax = order.total_tax_money.amount if order.total_tax_money else 0
            discount = order.total_discount_money.amount if order.total_discount_money else 0
            tip = order.total_tip_money.amount if order.total_tip_money else 0

            sales_db.upsert_order(conn, {
                "order_id": oid, "location_id": location,
                "state": order.state or "COMPLETED",
                "created_at": created_at, "closed_at": closed_at,
                "updated_at": updated_at,
                "total_amount": total, "tax_amount": tax,
                "discount_amount": discount, "tip_amount": tip,
                "business_date": biz_date, "synced_at": synced_at,
            })
            orders_upserted += 1

            for li in (order.line_items or []):
                raw_name = (li.name or "").strip()
                if not raw_name:
                    continue
                uid = li.uid or f"{oid}_{raw_name}"
                product = normalize_product_name(raw_name)
                try:
                    qty = int(float(li.quantity or "1"))
                except (ValueError, TypeError):
                    qty = 1
                base = li.base_price_money.amount if li.base_price_money else 0
                li_total = li.total_money.amount if li.total_money else (base * qty)

                sales_db.upsert_item(conn, {
                    "order_id": oid, "item_uid": uid,
                    "item_name": raw_name, "variation_name": li.variation_name or "",
                    "quantity": qty, "base_price_amount": base,
                    "total_amount": li_total,
                    "catalog_object_id": li.catalog_object_id or "",
                    "product_slug": product["slug"],
                    "product_display": product["display"],
                    "product_category": product["category"],
                    "business_date": biz_date,
                })
                items_upserted += 1

        for d in sorted(dates_affected):
            sales_db.recompute_summaries(conn, d, location)

        finished_at = datetime.now(timezone.utc).isoformat()
        sales_db.log_sync(conn, {
            "started_at": synced_at, "finished_at": finished_at,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "orders_fetched": len(all_orders),
            "orders_upserted": orders_upserted,
            "items_upserted": items_upserted,
            "dates_affected": json.dumps(sorted(dates_affected)),
            "status": "ok",
        })
        conn.commit()
        conn.close()

        # Refresh JSON snapshot
        business_date = now.strftime("%Y-%m-%d")
        products = sales_db.get_product_sales(business_date)
        order_count = sales_db.get_order_count(business_date, location)

        sales_today = {}
        sales_amounts = {}
        top_items = []
        for p in products:
            slug = p["item_name"]
            qty = int(p["quantity_sold"])
            rev = p["gross_sales"]
            sales_today[slug] = qty
            sales_amounts[slug] = rev
            top_items.append({
                "name": slug,
                "display": p.get("product_display", slug),
                "category": p.get("product_category", "other"),
                "qty": qty, "revenue_cents": rev,
            })

        existing = {}
        if STATE_FILE.exists():
            try:
                existing = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            except Exception:
                pass

        snapshot = {
            "inventory": existing.get("inventory", {}),
            "sales_today": sales_today,
            "sales_amounts": sales_amounts,
            "top_items": top_items,
            "raw_order_count": order_count,
            "business_date": business_date,
            "location_id": location,
            "source": "sqlite",
            "snapshot_generated_at": datetime.now(timezone.utc).isoformat(),
            "last_square_sync_at": finished_at,
            "freshness_seconds": 0,
        }
        STATE_FILE.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

        # Update metadata
        _meta["status"] = "live"
        _meta["last_successful_sync"] = finished_at
        _meta["orders_synced"] = order_count
        _meta["products_synced"] = len(sales_today)
        _meta["error_message"] = ""
        _meta["consecutive_failures"] = 0

        log.info("sync ok: %d orders, %d items, %d products",
                 orders_upserted, items_upserted, len(sales_today))

        # Enforce recipe coverage — create drafts for any new sold items
        try:
            from maillard.recipe_builder import enforce_recipe_coverage
            cov = enforce_recipe_coverage()
            if cov["created"] > 0:
                log.info("recipe coverage: created %d new drafts (coverage: %.0f%%)",
                         cov["created"], cov["coverage_pct"])
        except Exception as ce:
            log.warning("recipe coverage check failed: %s", ce)

    except Exception as e:
        _meta["status"] = "error"
        _meta["error_message"] = str(e)[:200]
        _meta["consecutive_failures"] += 1
        log.error("sync failed: %s", e)


# ---------------------------------------------------------------------------
# Background loop (daemon thread)
# ---------------------------------------------------------------------------
def _loop(interval: int = 60) -> None:
    """Run _do_sync every `interval` seconds. Blocks forever."""
    log.info("background sync started (every %ds)", interval)
    while True:
        _do_sync()
        time.sleep(interval)


_started = False


def start_sync(interval: int = 60) -> None:
    """Start the background sync thread. Safe to call multiple times."""
    global _started
    if _started:
        return
    _started = True
    t = Thread(target=_loop, args=(interval,), daemon=True, name="sales-sync")
    t.start()
    log.info("sync thread launched")
