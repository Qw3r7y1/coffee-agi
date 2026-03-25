"""
Bulk Parse Review repository — review queue, edit, approve invoice item pack breakdowns.
"""
from __future__ import annotations
from app.core.db import get_conn, now_iso


def migrate_columns() -> None:
    """Add bulk parse columns if they don't exist. Safe to call repeatedly."""
    conn = get_conn()
    existing = [c[1] for c in conn.execute("PRAGMA table_info(invoice_items)").fetchall()]
    for col, typ in [("pack_count", "INTEGER"), ("pack_size_text", "TEXT"),
                     ("base_unit", "TEXT"), ("total_base_units", "REAL"),
                     ("derived_unit_cost", "REAL")]:
        if col not in existing:
            conn.execute(f"ALTER TABLE invoice_items ADD COLUMN {col} {typ}")
    conn.commit()
    conn.close()


def backfill_bulk_parse() -> int:
    """Parse pack sizes and compute derived costs for all invoice items. Returns rows updated."""
    from maillard.mcp.accounting.invoice_intake import (
        parse_pack_size, normalize_base_unit, convert_to_base_units,
        _extract_count_from_name,
    )
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, raw_name, unit_price, unit, quantity, line_total
        FROM invoice_items WHERE unit_price > 0
    """).fetchall()

    updated = 0
    for r in rows:
        raw = r["raw_name"] or ""
        unit_price = r["unit_price"] or 0
        unit = r["unit"] or "ea"
        qty = r["quantity"] or 1
        line_total = r["line_total"] or 0

        # Parse pack
        pack = parse_pack_size(raw)
        count_from_name = _extract_count_from_name(raw)

        pack_count = None
        pack_text = None
        base_unit = None
        total_base = None
        derived_cost = None

        if pack:
            pack_count = pack["pack_count"]
            pack_text = f"{pack['pack_count']}x{pack['per_unit_size']}{pack['per_unit_unit']}"
            base = convert_to_base_units(qty, unit, pack["pack_count"],
                                         pack["per_unit_size"], pack["per_unit_unit"])
            base_unit = base["base_unit"]
            total_base = base["total_base_units"]
        elif count_from_name and count_from_name > 1 and unit in ("box", "case", "pack", "bag"):
            pack_count = count_from_name
            pack_text = f"{count_from_name} per {unit}"
            total_base = qty * count_from_name
            base_unit = "unit"

        # Derive single-unit cost
        if total_base and total_base > 0 and line_total > 0:
            derived_cost = round(line_total / total_base, 5)
        elif unit_price > 0 and pack_count and pack_count > 1:
            derived_cost = round(unit_price / pack_count, 5)

        conn.execute("""
            UPDATE invoice_items SET pack_count=?, pack_size_text=?, base_unit=?,
                total_base_units=?, derived_unit_cost=?
            WHERE id=?
        """, (pack_count, pack_text, base_unit, total_base, derived_cost, r["id"]))
        updated += 1

    conn.commit()
    conn.close()
    return updated


# ── Review queue ──

def get_bulk_parse_review_queue() -> list[dict]:
    """Return items needing review: review_required=1, confidence != high, or missing parse."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT ii.*, i.vendor, i.invoice_date
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.unit_price > 0
          AND (ii.review_required = 1
               OR ii.confidence != 'high'
               OR ii.pack_count IS NULL
               OR ii.derived_unit_cost IS NULL)
        ORDER BY ii.review_required DESC, ii.confidence ASC, i.invoice_date DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_parsed_items() -> list[dict]:
    """Return all invoice items with bulk parse data."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT ii.*, i.vendor, i.invoice_date
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.unit_price > 0
        ORDER BY i.invoice_date DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_item_price_history(normalized_name: str) -> dict | None:
    """Get median and recent prices for comparison."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT derived_unit_cost FROM invoice_items
        WHERE normalized_name = ? AND derived_unit_cost IS NOT NULL AND derived_unit_cost > 0
        ORDER BY id DESC LIMIT 10
    """, (normalized_name,)).fetchall()
    conn.close()
    if not rows:
        return None
    costs = sorted(r["derived_unit_cost"] for r in rows)
    median = costs[len(costs) // 2]
    return {"median": round(median, 5), "count": len(costs), "min": costs[0], "max": costs[-1]}


# ── Edit/approve ──

def update_invoice_item_bulk_parse(item_id: int, updates: dict) -> dict | None:
    """Update bulk parse fields for an invoice item."""
    conn = get_conn()
    item = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    if not item:
        conn.close()
        return None

    fields = ["normalized_name", "quantity", "unit", "pack_count", "pack_size_text",
              "base_unit", "total_base_units", "derived_unit_cost", "confidence", "review_required"]
    sets = []
    vals = []
    for f in fields:
        if f in updates:
            sets.append(f"{f}=?")
            vals.append(updates[f])
    if not sets:
        conn.close()
        return dict(item)

    vals.append(item_id)
    conn.execute(f"UPDATE invoice_items SET {','.join(sets)} WHERE id=?", vals)
    conn.commit()
    row = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return dict(row)


def recalculate_invoice_item(item_id: int) -> dict | None:
    """Recalculate total_base_units and derived_unit_cost from current fields."""
    from maillard.mcp.accounting.invoice_intake import convert_to_base_units, normalize_unit
    conn = get_conn()
    item = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    if not item:
        conn.close()
        return None

    pack_count = item["pack_count"] or 1
    unit = item["unit"] or "ea"
    qty = item["quantity"] or 1
    line_total = item["line_total"] or 0

    # Try to compute base units
    total_base = qty * pack_count
    base_unit = "unit"
    derived = None

    if item["base_unit"] and item["base_unit"] != "unit":
        base_unit = item["base_unit"]

    if total_base > 0 and line_total > 0:
        derived = round(line_total / total_base, 5)

    conn.execute("""
        UPDATE invoice_items SET total_base_units=?, derived_unit_cost=?, base_unit=?
        WHERE id=?
    """, (total_base, derived, base_unit, item_id))
    conn.commit()
    row = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return dict(row)


def approve_invoice_item(item_id: int) -> dict | None:
    """Mark item as reviewed: review_required=0, confidence=high."""
    conn = get_conn()
    item = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    if not item:
        conn.close()
        return None
    conn.execute("UPDATE invoice_items SET review_required=0, confidence='high' WHERE id=?", (item_id,))
    conn.commit()

    # Update ingredient cost in ingredients table if normalized_name matches
    if item["derived_unit_cost"] and item["derived_unit_cost"] > 0:
        from app.data_access.ingredients_repo import list_ingredients
        # Simple match: check if any ingredient key is close to normalized_name
        # This is optional — exact matching only
        pass

    row = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return dict(row)


def validate_bulk_parse(item: dict) -> list[str]:
    """Validate an item's bulk parse fields. Returns list of warnings."""
    warns = []
    if not item.get("pack_count"):
        warns.append("Pack count missing")
    if not item.get("base_unit"):
        warns.append("Base unit unclear")
    if not item.get("total_base_units") or item["total_base_units"] <= 0:
        warns.append("Total base units missing or zero")
    if not item.get("derived_unit_cost"):
        warns.append("Derived unit cost not calculable")
    elif item["derived_unit_cost"] < 0.0001:
        warns.append("Derived unit cost suspiciously low")
    elif item["derived_unit_cost"] > 500:
        warns.append("Derived unit cost suspiciously high")
    return warns
