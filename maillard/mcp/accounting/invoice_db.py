"""
Maillard Invoice Database — SQLite vendor price comparison.

Schema:
  invoices      → one row per invoice document
  invoice_items → one row per line item, linked to invoice

Keeps extraction → interpretation → storage cleanly separated.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any

from loguru import logger

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "invoices.db")


# ── Schema ───────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS invoices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor          TEXT NOT NULL,
    invoice_date    TEXT,
    invoice_number  TEXT,
    total           REAL,
    computed_total  REAL,
    total_matches   INTEGER DEFAULT 1,
    source_file     TEXT,
    notes           TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(vendor, invoice_number)
);

CREATE TABLE IF NOT EXISTS invoice_items (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_id            INTEGER NOT NULL REFERENCES invoices(id),
    raw_name              TEXT,
    normalized_name       TEXT,
    category              TEXT,
    quantity              REAL,
    unit                  TEXT,
    price_basis           TEXT,
    unit_price            REAL,
    line_total            REAL,
    printed_quantity      REAL,
    handwritten_quantity  REAL,
    printed_unit_price    REAL,
    handwritten_unit_price REAL,
    has_handwriting       INTEGER DEFAULT 0,
    handwriting_note      TEXT,
    override_source       TEXT DEFAULT 'printed',
    confidence            TEXT DEFAULT 'medium',
    review_required       INTEGER DEFAULT 0,
    pack_size_json        TEXT
);

CREATE INDEX IF NOT EXISTS idx_items_invoice   ON invoice_items(invoice_id);
CREATE INDEX IF NOT EXISTS idx_items_norm_name ON invoice_items(normalized_name);
CREATE INDEX IF NOT EXISTS idx_items_vendor    ON invoice_items(invoice_id);
CREATE INDEX IF NOT EXISTS idx_invoices_vendor ON invoices(vendor);
CREATE INDEX IF NOT EXISTS idx_invoices_date   ON invoices(invoice_date);
"""


def _get_conn() -> sqlite3.Connection:
    """Get a connection, creating the DB + tables if needed."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


# ── Save ─────────────────────────────────────────────────────────


def save_invoice_to_db(parsed_invoice: dict) -> dict:
    """
    Save a fully interpreted invoice (from extract_invoice_data) to SQLite.

    Returns: {"status": "stored"|"duplicate"|"error", "invoice_id": int|None, ...}
    """
    conn = _get_conn()
    try:
        vendor = parsed_invoice.get("vendor", "Unknown")
        inv_num = parsed_invoice.get("invoice_number")
        inv_date = parsed_invoice.get("invoice_date")
        total = parsed_invoice.get("invoice_total")
        computed = parsed_invoice.get("computed_total")
        matches = 1 if parsed_invoice.get("total_matches", True) else 0
        source = parsed_invoice.get("source_file")
        notes = parsed_invoice.get("notes")
        now = datetime.now().isoformat()

        # Check for duplicate
        if inv_num:
            existing = conn.execute(
                "SELECT id FROM invoices WHERE vendor = ? AND invoice_number = ?",
                (vendor, inv_num),
            ).fetchone()
            if existing:
                return {"status": "duplicate", "invoice_id": existing["id"],
                        "message": f"Invoice {inv_num} from {vendor} already in DB"}

        # Insert invoice
        cur = conn.execute(
            """INSERT INTO invoices (vendor, invoice_date, invoice_number, total,
               computed_total, total_matches, source_file, notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (vendor, inv_date, inv_num, total, computed, matches, source, notes, now),
        )
        invoice_id = cur.lastrowid

        # Insert line items
        items = parsed_invoice.get("line_items", [])
        for item in items:
            import json
            pack_json = json.dumps(item.get("pack_size")) if item.get("pack_size") else None

            conn.execute(
                """INSERT INTO invoice_items
                   (invoice_id, raw_name, normalized_name, category, quantity, unit,
                    price_basis, unit_price, line_total,
                    printed_quantity, handwritten_quantity,
                    printed_unit_price, handwritten_unit_price,
                    has_handwriting, handwriting_note, override_source,
                    confidence, review_required, pack_size_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    invoice_id,
                    item.get("raw_name"),
                    item.get("normalized_name"),
                    item.get("category"),
                    item.get("quantity"),
                    item.get("unit"),
                    item.get("price_basis"),
                    item.get("unit_price"),
                    item.get("line_total"),
                    item.get("printed_quantity"),
                    item.get("handwritten_quantity"),
                    item.get("printed_unit_price"),
                    item.get("handwritten_unit_price"),
                    1 if item.get("has_handwriting") else 0,
                    item.get("handwriting_note"),
                    item.get("override_source", "printed"),
                    item.get("confidence", "medium"),
                    1 if item.get("review_required") else 0,
                    pack_json,
                ),
            )

        conn.commit()
        logger.info(f"[INVOICE-DB] Stored invoice #{invoice_id}: {vendor} / {inv_num} ({len(items)} items)")
        return {"status": "stored", "invoice_id": invoice_id, "items_stored": len(items)}

    except Exception as e:
        conn.rollback()
        logger.error(f"[INVOICE-DB] Failed to store invoice: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


# ── Query Helpers ────────────────────────────────────────────────


def get_latest_invoices(limit: int = 10) -> list[dict]:
    """Get the most recent invoices with their item counts and totals."""
    conn = _get_conn()
    rows = conn.execute(
        """SELECT i.id, i.vendor, i.invoice_date, i.invoice_number, i.total,
                  i.source_file, COUNT(ii.id) as item_count
           FROM invoices i
           LEFT JOIN invoice_items ii ON ii.invoice_id = i.id
           GROUP BY i.id
           ORDER BY i.invoice_date DESC, i.id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_invoice_by_vendor(vendor_name: str) -> dict | None:
    """Get the most recent invoice from a specific vendor, with line items."""
    conn = _get_conn()
    inv = conn.execute(
        """SELECT i.id, i.vendor, i.invoice_date, i.invoice_number, i.total, i.source_file
           FROM invoices i
           WHERE LOWER(i.vendor) LIKE LOWER(?)
           ORDER BY i.invoice_date DESC
           LIMIT 1""",
        (f"%{vendor_name}%",),
    ).fetchone()
    if not inv:
        conn.close()
        return None

    items = conn.execute(
        """SELECT raw_name, normalized_name, unit_price, unit, quantity, line_total,
                  confidence, pack_size_json
           FROM invoice_items WHERE invoice_id = ?""",
        (inv["id"],),
    ).fetchall()
    conn.close()

    result = dict(inv)
    result["line_items"] = [dict(it) for it in items]
    return result


def get_latest_price_for_item(normalized_name: str) -> dict | None:
    """
    Get the most recent price for an item across all vendors.

    Returns: {vendor, item, unit_price, unit, price_basis, confidence, date} or None.
    """
    conn = _get_conn()
    row = conn.execute(
        """SELECT i.vendor, i.invoice_date, ii.normalized_name, ii.unit_price,
                  ii.unit, ii.price_basis, ii.confidence, ii.override_source,
                  ii.quantity
           FROM invoice_items ii
           JOIN invoices i ON ii.invoice_id = i.id
           WHERE LOWER(ii.normalized_name) = LOWER(?)
           ORDER BY i.invoice_date DESC
           LIMIT 1""",
        (normalized_name,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


def compare_vendor_prices(normalized_name: str) -> list[dict]:
    """
    Compare the latest price for an item across all vendors.

    Returns list of {vendor, unit_price, unit, price_basis, date, confidence},
    sorted cheapest first.
    """
    conn = _get_conn()
    rows = conn.execute(
        """SELECT i.vendor, ii.unit_price, ii.unit, ii.price_basis,
                  ii.confidence, i.invoice_date,
                  ii.override_source, ii.quantity
           FROM invoice_items ii
           JOIN invoices i ON ii.invoice_id = i.id
           WHERE LOWER(ii.normalized_name) = LOWER(?)
             AND ii.unit_price > 0
           ORDER BY i.invoice_date DESC""",
        (normalized_name,),
    ).fetchall()
    conn.close()

    # Keep only the latest entry per vendor
    seen: dict[str, dict] = {}
    for row in rows:
        v = row["vendor"]
        if v not in seen:
            seen[v] = dict(row)

    return sorted(seen.values(), key=lambda x: x["unit_price"])


def get_vendor_price_history(vendor_name: str, normalized_name: str | None = None) -> list[dict]:
    """
    Get price history for a vendor, optionally filtered by item.

    Returns list of {item, unit_price, unit, date, invoice_number, confidence, override_source},
    sorted newest first.
    """
    conn = _get_conn()
    if normalized_name:
        rows = conn.execute(
            """SELECT ii.normalized_name AS item, ii.unit_price, ii.unit,
                      ii.price_basis, ii.confidence, ii.override_source,
                      ii.quantity, ii.line_total,
                      i.invoice_date, i.invoice_number
               FROM invoice_items ii
               JOIN invoices i ON ii.invoice_id = i.id
               WHERE LOWER(i.vendor) LIKE LOWER(?)
                 AND LOWER(ii.normalized_name) = LOWER(?)
               ORDER BY i.invoice_date DESC""",
            (f"%{vendor_name}%", normalized_name),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT ii.normalized_name AS item, ii.unit_price, ii.unit,
                      ii.price_basis, ii.confidence, ii.override_source,
                      ii.quantity, ii.line_total,
                      i.invoice_date, i.invoice_number
               FROM invoice_items ii
               JOIN invoices i ON ii.invoice_id = i.id
               WHERE LOWER(i.vendor) LIKE LOWER(?)
               ORDER BY i.invoice_date DESC""",
            (f"%{vendor_name}%",),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_cheapest_vendor(normalized_name: str) -> dict | None:
    """
    Find the cheapest vendor for a given item (most recent price per vendor).

    Returns: {vendor, unit_price, unit, date, savings_vs_highest} or None.
    """
    prices = compare_vendor_prices(normalized_name)
    if not prices:
        return None
    cheapest = prices[0]
    most_expensive = prices[-1] if len(prices) > 1 else cheapest
    savings = round(most_expensive["unit_price"] - cheapest["unit_price"], 2)
    return {
        "vendor": cheapest["vendor"],
        "unit_price": cheapest["unit_price"],
        "unit": cheapest["unit"],
        "price_basis": cheapest["price_basis"],
        "date": cheapest["invoice_date"],
        "confidence": cheapest["confidence"],
        "savings_vs_highest": savings,
        "num_vendors": len(prices),
    }


def get_items_needing_review() -> list[dict]:
    """Get all line items flagged for review."""
    conn = _get_conn()
    rows = conn.execute(
        """SELECT i.vendor, i.invoice_date, i.invoice_number,
                  ii.raw_name, ii.normalized_name, ii.unit_price,
                  ii.confidence, ii.handwriting_note, ii.override_source
           FROM invoice_items ii
           JOIN invoices i ON ii.invoice_id = i.id
           WHERE ii.review_required = 1
           ORDER BY i.invoice_date DESC""",
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_db_summary() -> dict:
    """Quick summary of what's in the database."""
    conn = _get_conn()
    inv_count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    item_count = conn.execute("SELECT COUNT(*) FROM invoice_items").fetchone()[0]
    vendor_count = conn.execute("SELECT COUNT(DISTINCT vendor) FROM invoices").fetchone()[0]
    review_count = conn.execute("SELECT COUNT(*) FROM invoice_items WHERE review_required = 1").fetchone()[0]
    hw_count = conn.execute("SELECT COUNT(*) FROM invoice_items WHERE has_handwriting = 1").fetchone()[0]

    conf = {}
    for row in conn.execute("SELECT confidence, COUNT(*) as cnt FROM invoice_items GROUP BY confidence"):
        conf[row["confidence"]] = row["cnt"]

    conn.close()
    return {
        "invoices": inv_count,
        "items": item_count,
        "vendors": vendor_count,
        "review_required": review_count,
        "handwritten_lines": hw_count,
        "confidence": conf,
    }
