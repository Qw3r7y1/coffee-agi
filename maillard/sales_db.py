"""
Sales database — SQLite source of truth for Square sales data.

Tables:
  orders              — one row per completed Square order (upsert by order_id)
  order_items         — one row per line item (upsert by order_id + item_uid)
  daily_sales_summary — aggregated daily totals (recomputed after sync)
  product_daily_sales — per-product daily totals (recomputed after sync)
  sync_log            — one row per sync run for diagnostics

DB path: data/sales.db
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables if they don't exist. Safe to call repeatedly."""
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id        TEXT PRIMARY KEY,
            location_id     TEXT NOT NULL,
            state           TEXT NOT NULL DEFAULT 'COMPLETED',
            created_at      TEXT,
            closed_at       TEXT,
            updated_at      TEXT,
            total_amount    INTEGER NOT NULL DEFAULT 0,
            tax_amount      INTEGER NOT NULL DEFAULT 0,
            discount_amount INTEGER NOT NULL DEFAULT 0,
            tip_amount      INTEGER NOT NULL DEFAULT 0,
            business_date   TEXT NOT NULL,
            synced_at       TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS ix_orders_biz_date
            ON orders(business_date);
        CREATE INDEX IF NOT EXISTS ix_orders_closed
            ON orders(closed_at);

        CREATE TABLE IF NOT EXISTS order_items (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id            TEXT NOT NULL REFERENCES orders(order_id),
            item_uid            TEXT NOT NULL,
            item_name           TEXT NOT NULL,
            variation_name      TEXT NOT NULL DEFAULT '',
            quantity            REAL NOT NULL DEFAULT 1,
            base_price_amount   INTEGER NOT NULL DEFAULT 0,
            total_amount        INTEGER NOT NULL DEFAULT 0,
            catalog_object_id   TEXT,
            product_slug        TEXT NOT NULL,
            product_display     TEXT NOT NULL,
            product_category    TEXT NOT NULL DEFAULT 'other',
            business_date       TEXT NOT NULL,
            UNIQUE(order_id, item_uid)
        );

        CREATE INDEX IF NOT EXISTS ix_items_biz_date_slug
            ON order_items(business_date, product_slug);
        CREATE INDEX IF NOT EXISTS ix_items_order
            ON order_items(order_id);

        CREATE TABLE IF NOT EXISTS daily_sales_summary (
            business_date   TEXT NOT NULL,
            location_id     TEXT NOT NULL,
            gross_sales     INTEGER NOT NULL DEFAULT 0,
            orders_count    INTEGER NOT NULL DEFAULT 0,
            avg_ticket      REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (business_date, location_id)
        );

        CREATE TABLE IF NOT EXISTS product_daily_sales (
            business_date   TEXT NOT NULL,
            item_name       TEXT NOT NULL,
            quantity_sold   REAL NOT NULL DEFAULT 0,
            gross_sales     INTEGER NOT NULL DEFAULT 0,
            product_display TEXT NOT NULL DEFAULT '',
            product_category TEXT NOT NULL DEFAULT 'other',
            PRIMARY KEY (business_date, item_name)
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      TEXT NOT NULL,
            finished_at     TEXT NOT NULL,
            window_start    TEXT NOT NULL,
            window_end      TEXT NOT NULL,
            orders_fetched  INTEGER NOT NULL DEFAULT 0,
            orders_upserted INTEGER NOT NULL DEFAULT 0,
            items_upserted  INTEGER NOT NULL DEFAULT 0,
            dates_affected  TEXT NOT NULL DEFAULT '[]',
            status          TEXT NOT NULL DEFAULT 'ok'
        );
    """)
    conn.close()


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def upsert_order(conn: sqlite3.Connection, order: dict) -> None:
    """Insert or replace an order row."""
    conn.execute("""
        INSERT INTO orders (order_id, location_id, state, created_at, closed_at,
                            updated_at, total_amount, tax_amount, discount_amount,
                            tip_amount, business_date, synced_at)
        VALUES (:order_id, :location_id, :state, :created_at, :closed_at,
                :updated_at, :total_amount, :tax_amount, :discount_amount,
                :tip_amount, :business_date, :synced_at)
        ON CONFLICT(order_id) DO UPDATE SET
            state=excluded.state,
            closed_at=excluded.closed_at,
            updated_at=excluded.updated_at,
            total_amount=excluded.total_amount,
            tax_amount=excluded.tax_amount,
            discount_amount=excluded.discount_amount,
            tip_amount=excluded.tip_amount,
            business_date=excluded.business_date,
            synced_at=excluded.synced_at
    """, order)


def upsert_item(conn: sqlite3.Connection, item: dict) -> None:
    """Insert or replace a line item row."""
    conn.execute("""
        INSERT INTO order_items (order_id, item_uid, item_name, variation_name,
                                 quantity, base_price_amount, total_amount,
                                 catalog_object_id, product_slug, product_display,
                                 product_category, business_date)
        VALUES (:order_id, :item_uid, :item_name, :variation_name,
                :quantity, :base_price_amount, :total_amount,
                :catalog_object_id, :product_slug, :product_display,
                :product_category, :business_date)
        ON CONFLICT(order_id, item_uid) DO UPDATE SET
            item_name=excluded.item_name,
            variation_name=excluded.variation_name,
            quantity=excluded.quantity,
            base_price_amount=excluded.base_price_amount,
            total_amount=excluded.total_amount,
            product_slug=excluded.product_slug,
            product_display=excluded.product_display,
            product_category=excluded.product_category
    """, item)


def log_sync(conn: sqlite3.Connection, entry: dict) -> None:
    """Record a sync run in sync_log."""
    conn.execute("""
        INSERT INTO sync_log (started_at, finished_at, window_start, window_end,
                              orders_fetched, orders_upserted, items_upserted,
                              dates_affected, status)
        VALUES (:started_at, :finished_at, :window_start, :window_end,
                :orders_fetched, :orders_upserted, :items_upserted,
                :dates_affected, :status)
    """, entry)


# ---------------------------------------------------------------------------
# Recompute summaries
# ---------------------------------------------------------------------------

def recompute_summaries(conn: sqlite3.Connection, business_date: str, location_id: str) -> None:
    """Delete and recompute both summary tables for a given date."""
    # daily_sales_summary
    conn.execute("DELETE FROM daily_sales_summary WHERE business_date=? AND location_id=?",
                 (business_date, location_id))
    conn.execute("""
        INSERT INTO daily_sales_summary (business_date, location_id, gross_sales, orders_count, avg_ticket)
        SELECT
            o.business_date,
            o.location_id,
            COALESCE(SUM(o.total_amount), 0),
            COUNT(*),
            CASE WHEN COUNT(*) > 0 THEN ROUND(1.0 * SUM(o.total_amount) / COUNT(*), 0) ELSE 0 END
        FROM orders o
        WHERE o.business_date=? AND o.location_id=? AND o.state='COMPLETED'
        GROUP BY o.business_date, o.location_id
    """, (business_date, location_id))

    # product_daily_sales
    conn.execute("DELETE FROM product_daily_sales WHERE business_date=?", (business_date,))
    conn.execute("""
        INSERT INTO product_daily_sales (business_date, item_name, quantity_sold, gross_sales,
                                         product_display, product_category)
        SELECT
            oi.business_date,
            oi.product_slug,
            SUM(oi.quantity),
            SUM(oi.total_amount),
            oi.product_display,
            oi.product_category
        FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id
        WHERE oi.business_date=? AND o.state='COMPLETED'
        GROUP BY oi.business_date, oi.product_slug, oi.product_display, oi.product_category
    """, (business_date,))


# ---------------------------------------------------------------------------
# Query helpers (for query_sales.py and chat)
# ---------------------------------------------------------------------------

def get_daily_summary(business_date: str, location_id: str) -> dict | None:
    """Get the daily_sales_summary row for a date."""
    conn = _connect()
    row = conn.execute(
        "SELECT * FROM daily_sales_summary WHERE business_date=? AND location_id=?",
        (business_date, location_id)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_product_sales(business_date: str) -> list[dict]:
    """Get product_daily_sales rows for a date, sorted by quantity desc."""
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM product_daily_sales WHERE business_date=? ORDER BY quantity_sold DESC",
        (business_date,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_order_count(business_date: str, location_id: str) -> int:
    """Count completed orders for a date."""
    conn = _connect()
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM orders WHERE business_date=? AND location_id=? AND state='COMPLETED'",
        (business_date, location_id)
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def get_last_sync() -> dict | None:
    """Get the most recent sync_log entry."""
    conn = _connect()
    row = conn.execute("SELECT * FROM sync_log ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None


def compare_days(date_a: str, date_b: str) -> dict:
    """Compare product sales between two dates."""
    a = {r["item_name"]: r for r in get_product_sales(date_a)}
    b = {r["item_name"]: r for r in get_product_sales(date_b)}
    all_products = sorted(set(list(a.keys()) + list(b.keys())))

    rows = []
    for p in all_products:
        qa = a.get(p, {}).get("quantity_sold", 0)
        qb = b.get(p, {}).get("quantity_sold", 0)
        diff = qa - qb
        rows.append({"product": p, date_a: qa, date_b: qb, "diff": diff})

    rows.sort(key=lambda x: -abs(x["diff"]))
    return {"date_a": date_a, "date_b": date_b, "products": rows}
