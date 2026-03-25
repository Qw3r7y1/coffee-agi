"""Invoices repository — reads/writes data/coffee_agi.db."""
from __future__ import annotations
from app.core.db import get_conn, now_iso


def create_invoice(data: dict) -> int:
    """Insert an invoice, return its id."""
    conn = get_conn()
    cur = conn.execute("""
        INSERT INTO invoices (vendor, invoice_date, invoice_number, total, source_file, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data.get("vendor"), data.get("invoice_date"), data.get("invoice_number"),
          data.get("total"), data.get("source_file"), now_iso()))
    conn.commit()
    inv_id = cur.lastrowid
    conn.close()
    return inv_id


def add_invoice_items(invoice_id: int, items: list[dict]) -> int:
    """Insert line items for an invoice. Returns count inserted."""
    conn = get_conn()
    count = 0
    for item in items:
        conn.execute("""
            INSERT INTO invoice_items (invoice_id, raw_name, normalized_name, quantity, unit,
                                       price_basis, unit_price, line_total, override_source,
                                       confidence, review_required)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (invoice_id, item.get("raw_name"), item.get("normalized_name"),
              item.get("quantity"), item.get("unit"), item.get("price_basis"),
              item.get("unit_price"), item.get("line_total"), item.get("override_source"),
              item.get("confidence"), item.get("review_required", 0)))
        count += 1
    conn.commit()
    conn.close()
    return count


def get_latest_invoice() -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM invoices ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None


def get_latest_price_for_item(normalized_name: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("""
        SELECT ii.*, i.vendor, i.invoice_date
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.normalized_name = ? AND ii.unit_price > 0
        ORDER BY i.invoice_date DESC, ii.id DESC LIMIT 1
    """, (normalized_name,)).fetchone()
    conn.close()
    return dict(row) if row else None


def compare_vendor_prices(normalized_name: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT i.vendor, ii.unit_price, ii.unit, i.invoice_date, ii.confidence
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.normalized_name = ? AND ii.unit_price > 0
        ORDER BY i.invoice_date DESC
    """, (normalized_name,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def list_invoices(limit: int = 20) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM invoices ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_invoice_items(invoice_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM invoice_items WHERE invoice_id=?", (invoice_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
