"""Ingredients repository — reads/writes data/coffee_agi.db."""
from __future__ import annotations
from app.core.db import get_conn, now_iso


def get_ingredient(key: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_ingredients() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM ingredients ORDER BY ingredient_key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_ingredient(data: dict) -> dict:
    conn = get_conn()
    now = now_iso()
    conn.execute("""
        INSERT INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, vendor_name, invoice_date, updated_at)
        VALUES (:key, :display, :unit, :cost, :source, :vendor, :inv_date, :now)
        ON CONFLICT(ingredient_key) DO UPDATE SET
            display_name=excluded.display_name, base_unit=excluded.base_unit,
            latest_unit_cost=excluded.latest_unit_cost, cost_source=excluded.cost_source,
            vendor_name=excluded.vendor_name, invoice_date=excluded.invoice_date,
            updated_at=excluded.updated_at
    """, {
        "key": data["ingredient_key"], "display": data.get("display_name", data["ingredient_key"]),
        "unit": data.get("base_unit", "ea"), "cost": data.get("latest_unit_cost", 0),
        "source": data.get("cost_source", "manual"), "vendor": data.get("vendor_name"),
        "inv_date": data.get("invoice_date"), "now": now,
    })
    conn.commit()
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=?", (data["ingredient_key"],)).fetchone()
    conn.close()
    return dict(row)


def update_ingredient_cost(key: str, cost: float, source: str = "manual",
                           vendor: str | None = None, invoice_date: str | None = None) -> dict | None:
    conn = get_conn()
    now = now_iso()
    cur = conn.execute("""
        UPDATE ingredients SET latest_unit_cost=?, cost_source=?, vendor_name=?, invoice_date=?, updated_at=?
        WHERE ingredient_key=?
    """, (cost, source, vendor, invoice_date, now, key))
    conn.commit()
    if cur.rowcount == 0:
        conn.close()
        return None
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=?", (key,)).fetchone()
    conn.close()
    return dict(row)
