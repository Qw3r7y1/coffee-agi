"""
Square → SQLite → current_state.json  (1-minute sync runner)

Pipeline:
  1. Acquire file lock (skip if another instance running)
  2. Fetch recent orders from Square (rolling 3h window)
  3. Upsert into data/sales.db
  4. Recompute daily summaries
  5. Refresh data/current_state.json from SQLite
  6. Log results and exit

Usage:
  python scripts/run_minute_sync.py              # normal run (3h window)
  python scripts/run_minute_sync.py --hours 24   # backfill last 24h
  python scripts/run_minute_sync.py --loop 60    # internal loop every 60s

Windows Task Scheduler (every minute):
  Program:   C:\\Users\\maill\\Desktop\\coffee-agi\\.venv\\Scripts\\python.exe
  Arguments: scripts\\run_minute_sync.py
  Start in:  C:\\Users\\maill\\Desktop\\coffee-agi
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s  %(message)s",
                    datefmt="%H:%M:%S")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("minute_sync")

from square import Square
from square.environment import SquareEnvironment
from maillard.mcp.sales.normalization import normalize_product_name
from maillard import sales_db

STATE_FILE = ROOT / "data" / "current_state.json"
LOCK_FILE = ROOT / "data" / ".minute_sync.lock"

_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
_LOCATION = os.getenv("SQUARE_LOCATION_ID", "")
_ENV = os.getenv("SQUARE_ENV", "production")


def _mask(s: str) -> str:
    return f"{s[:6]}…{s[-4:]}" if len(s) > 12 else "***"


def _check_env() -> str | None:
    if not _TOKEN: return "SQUARE_ACCESS_TOKEN not set"
    if not _LOCATION: return "SQUARE_LOCATION_ID not set"
    if _ENV.lower() != "production": return f"SQUARE_ENV={_ENV!r} — must be 'production'"
    if _TOKEN.lower().startswith("sandbox-"): return "Sandbox token detected"
    return None


def _client() -> Square:
    return Square(token=_TOKEN, environment=SquareEnvironment.PRODUCTION)


# ---------------------------------------------------------------------------
# File lock
# ---------------------------------------------------------------------------
class FileLock:
    def __init__(self, path: Path, max_age: int = 300):
        self.path = path
        self.max_age = max_age
        self._acquired = False

    def acquire(self) -> bool:
        if self.path.exists():
            try:
                if time.time() - self.path.stat().st_mtime > self.max_age:
                    log.warning("stale lock (>%ds), removing", self.max_age)
                    self.path.unlink()
                else:
                    return False
            except OSError:
                return False
        try:
            self.path.write_text(str(os.getpid()), encoding="utf-8")
            self._acquired = True
            return True
        except OSError:
            return False

    def release(self):
        if self._acquired:
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                pass
            self._acquired = False


# ---------------------------------------------------------------------------
# Step 1: Sync Square → SQLite
# ---------------------------------------------------------------------------
def sync_square(hours_back: float = 3.0) -> dict:
    """Fetch recent Square orders and upsert into sales.db."""
    err = _check_env()
    if err:
        return {"error": err}

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=hours_back)
    window_end = now + timedelta(minutes=5)

    log.info("env=%s  location=%s  token=%s", _ENV, _LOCATION, _mask(_TOKEN))
    log.info("window: %s → %s (%.1fh)", window_start.isoformat()[:19],
             window_end.isoformat()[:19], hours_back)

    # Fetch from Square (paginated)
    client = _client()
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
        try:
            result = client.orders.search(
                location_ids=[_LOCATION], limit=100, query=query, cursor=cursor,
            )
        except Exception as e:
            msg = str(e)[:200]
            log.error("Square API: %s", msg)
            return {"error": f"Square API: {msg}"}
        orders = result.orders or []
        if not orders:
            break
        all_orders.extend(orders)
        cursor = result.cursor
        if not cursor:
            break

    log.info("orders fetched: %d", len(all_orders))

    if not all_orders:
        return {"orders_fetched": 0, "orders_upserted": 0, "items_upserted": 0,
                "dates_affected": [],
                "window": {"start": window_start.isoformat(), "end": window_end.isoformat()}}

    # Upsert into SQLite
    sales_db.init_db()
    conn = sales_db._connect()
    orders_upserted = 0
    items_upserted = 0
    dates_affected = set()
    synced_at = now.isoformat()

    try:
        for order in all_orders:
            oid = order.id
            closed_at = _dt_str(order.closed_at)
            created_at = _dt_str(order.created_at)
            updated_at = _dt_str(order.updated_at)

            ref = closed_at or created_at or now.isoformat()
            biz_date = ref[:10]
            dates_affected.add(biz_date)

            total = order.total_money.amount if order.total_money else 0
            tax = order.total_tax_money.amount if order.total_tax_money else 0
            discount = order.total_discount_money.amount if order.total_discount_money else 0
            tip = order.total_tip_money.amount if order.total_tip_money else 0

            sales_db.upsert_order(conn, {
                "order_id": oid,
                "location_id": _LOCATION,
                "state": order.state or "COMPLETED",
                "created_at": created_at,
                "closed_at": closed_at,
                "updated_at": updated_at,
                "total_amount": total,
                "tax_amount": tax,
                "discount_amount": discount,
                "tip_amount": tip,
                "business_date": biz_date,
                "synced_at": synced_at,
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
                    "order_id": oid,
                    "item_uid": uid,
                    "item_name": raw_name,
                    "variation_name": li.variation_name or "",
                    "quantity": qty,
                    "base_price_amount": base,
                    "total_amount": li_total,
                    "catalog_object_id": li.catalog_object_id or "",
                    "product_slug": product["slug"],
                    "product_display": product["display"],
                    "product_category": product["category"],
                    "business_date": biz_date,
                })
                items_upserted += 1

        # Recompute summaries for affected dates
        dates_list = sorted(dates_affected)
        for d in dates_list:
            sales_db.recompute_summaries(conn, d, _LOCATION)
        log.info("summaries recomputed: %s", dates_list)

        # Log this sync run
        finished_at = datetime.now(timezone.utc).isoformat()
        sales_db.log_sync(conn, {
            "started_at": synced_at,
            "finished_at": finished_at,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "orders_fetched": len(all_orders),
            "orders_upserted": orders_upserted,
            "items_upserted": items_upserted,
            "dates_affected": json.dumps(dates_list),
            "status": "ok",
        })

        conn.commit()
        log.info("upserted: %d orders, %d items", orders_upserted, items_upserted)

    except Exception as e:
        conn.rollback()
        log.error("DB error: %s", e)
        return {"error": str(e)}
    finally:
        conn.close()

    return {
        "orders_fetched": len(all_orders),
        "orders_upserted": orders_upserted,
        "items_upserted": items_upserted,
        "dates_affected": dates_list,
        "window": {"start": window_start.isoformat(), "end": window_end.isoformat()},
    }


# ---------------------------------------------------------------------------
# Step 2: Refresh current_state.json from SQLite
# ---------------------------------------------------------------------------
def refresh_snapshot(business_date: str | None = None) -> dict:
    """Read SQLite and write current_state.json as a derived snapshot."""
    now = datetime.now(timezone.utc)
    if not business_date:
        business_date = now.strftime("%Y-%m-%d")

    products = sales_db.get_product_sales(business_date)
    order_count = sales_db.get_order_count(business_date, _LOCATION)
    last_sync = sales_db.get_last_sync()

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
            "qty": qty,
            "revenue_cents": rev,
        })

    # Preserve inventory from existing state
    existing = {}
    if STATE_FILE.exists():
        try:
            existing = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    last_sync_at = last_sync["finished_at"] if last_sync else None
    freshness = None
    if last_sync_at:
        try:
            sync_dt = datetime.fromisoformat(last_sync_at)
            freshness = round((now - sync_dt).total_seconds())
        except Exception:
            pass

    snapshot = {
        "inventory": existing.get("inventory", {}),
        "sales_today": sales_today,
        "sales_amounts": sales_amounts,
        "top_items": top_items,
        "raw_order_count": order_count,
        "business_date": business_date,
        "location_id": _LOCATION,
        "source": "sqlite",
        "snapshot_generated_at": now.isoformat(),
        "last_square_sync_at": last_sync_at,
        "freshness_seconds": freshness,
    }

    STATE_FILE.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    log.info("snapshot: %d products, %d orders → %s", len(sales_today), order_count, STATE_FILE.name)
    return snapshot


# ---------------------------------------------------------------------------
# Combined run
# ---------------------------------------------------------------------------
def run_once(hours_back: float = 3.0) -> dict:
    start = datetime.now(timezone.utc)
    log.info("=" * 50)
    log.info("SYNC START  %s", start.strftime("%H:%M:%S"))

    result = {"started_at": start.isoformat()}

    # Step 1: Square → SQLite
    sync = sync_square(hours_back=hours_back)
    result["sync"] = sync
    if "error" in sync:
        log.error("SYNC FAILED: %s", sync["error"])
        result["status"] = "sync_failed"
        return result

    # Step 2: SQLite → current_state.json
    snap = refresh_snapshot()
    result["snapshot"] = {
        "products": len(snap.get("sales_today", {})),
        "orders": snap.get("raw_order_count", 0),
        "freshness_seconds": snap.get("freshness_seconds"),
    }
    if "error" in snap:
        result["status"] = "snapshot_failed"
        return result

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    result["elapsed_seconds"] = round(elapsed, 1)
    result["status"] = "ok"

    log.info("SYNC DONE  %.1fs  orders=%d products=%d",
             elapsed, snap.get("raw_order_count", 0), len(snap.get("sales_today", {})))
    log.info("=" * 50)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dt_str(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, str):
        return val
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Maillard 1-minute sync (Square → SQLite → JSON)")
    p.add_argument("--hours", type=float, default=3.0, help="Rolling window hours (default 3)")
    p.add_argument("--loop", type=int, default=0, help="Loop every N seconds (0=run once)")
    args = p.parse_args()

    if args.loop > 0:
        log.info("loop mode: every %ds", args.loop)
        while True:
            lock = FileLock(LOCK_FILE)
            if lock.acquire():
                try:
                    run_once(hours_back=args.hours)
                finally:
                    lock.release()
            else:
                log.info("skipped — lock held")
            time.sleep(args.loop)
    else:
        lock = FileLock(LOCK_FILE)
        if not lock.acquire():
            log.info("SKIP — another sync running")
            sys.exit(0)
        try:
            result = run_once(hours_back=args.hours)
            print(json.dumps(result, indent=2, default=str))
            sys.exit(1 if result.get("status") != "ok" else 0)
        finally:
            lock.release()
