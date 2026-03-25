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
    """Parse pack sizes and compute derived costs for all invoice items.

    Key rule: derived_unit_cost must be the cost PER SELLABLE UNIT, not per gram.
    - Case of 42 croissants at $53 -> $1.2619/unit (not $0.0099/gram)
    - Box of 1000 cups at $44.95 -> $0.04495/unit
    - Milk at $4.95/gallon -> $4.95/gallon
    """
    from maillard.mcp.accounting.invoice_intake import (
        parse_pack_size, _extract_count_from_name,
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
        line_total = r["line_total"] or (unit_price * qty)

        pack = parse_pack_size(raw)
        count_from_name = _extract_count_from_name(raw)

        pack_count = None
        pack_text = None
        base_unit = None
        total_items = None
        derived_cost = None

        if pack and pack["pack_count"] > 1 and unit in ("case", "box", "pack", "bag", "ea"):
            # Case/box with N items inside -> cost per item
            # Use unit_price (price per case) / pack_count (items per case)
            # This is independent of qty (number of cases ordered)
            pack_count = pack["pack_count"]
            pack_text = f"{pack['pack_count']}x{pack['per_unit_size']}{pack['per_unit_unit']}"
            total_items = qty * pack_count
            base_unit = "unit"
            if unit_price and unit_price > 0:
                derived_cost = round(unit_price / pack_count, 5)
            elif line_total and line_total > 0 and qty > 0:
                derived_cost = round(line_total / qty / pack_count, 5)
        elif count_from_name and count_from_name > 1 and unit in ("box", "case", "pack", "bag"):
            # Count from name (e.g., "Cup - 1,000")
            pack_count = count_from_name
            pack_text = f"{count_from_name} per {unit}"
            total_items = qty * count_from_name
            base_unit = "unit"
            if unit_price and unit_price > 0:
                derived_cost = round(unit_price / count_from_name, 5)
            elif line_total and line_total > 0 and qty > 0:
                derived_cost = round(line_total / qty / count_from_name, 5)
        elif pack and pack["pack_count"] == 1:
            # Single item with weight (e.g., "5lb bag") -> cost per weight unit
            pack_count = 1
            per_size = pack["per_unit_size"]
            per_unit = pack["per_unit_unit"]
            pack_text = f"{per_size}{per_unit}"
            base_unit = per_unit
            total_items = qty * per_size
            if total_items > 0 and line_total > 0:
                derived_cost = round(line_total / total_items, 5)
        elif unit in ("lb", "kg", "oz", "gal", "L"):
            # Already priced per weight/volume unit
            pack_count = 1
            base_unit = unit
            total_items = qty
            derived_cost = unit_price  # already per unit
        elif unit in ("case", "box", "pack", "bag") and qty > 1:
            # Qty > 1 in a case = qty is the count of items in the case
            pack_count = int(qty)
            pack_text = f"{int(qty)} per {unit}"
            total_items = qty
            base_unit = "unit"
            if line_total and line_total > 0:
                derived_cost = round(line_total / qty, 5)
            elif unit_price and unit_price > 0:
                derived_cost = round(unit_price / qty, 5) if unit_price > qty else round(line_total / qty, 5) if line_total else None
        else:
            # Simple ea pricing
            pack_count = 1
            base_unit = "unit"
            total_items = qty
            if qty > 0 and line_total > 0:
                derived_cost = round(line_total / qty, 5)

        conn.execute("""
            UPDATE invoice_items SET pack_count=?, pack_size_text=?, base_unit=?,
                total_base_units=?, derived_unit_cost=?
            WHERE id=?
        """, (pack_count, pack_text, base_unit, total_items, derived_cost, r["id"]))
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
    """Mark item as reviewed and push derived_unit_cost into ingredients table."""
    conn = get_conn()
    row = conn.execute("""
        SELECT ii.*, i.vendor, i.invoice_date
        FROM invoice_items ii JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.id=?
    """, (item_id,)).fetchone()
    if not row:
        conn.close()
        return None

    conn.execute("UPDATE invoice_items SET review_required=0, confidence='high' WHERE id=?", (item_id,))

    # Push derived cost into ingredients table
    derived = row["derived_unit_cost"]
    norm_name = row["normalized_name"]
    base_unit = row["base_unit"] or "unit"
    vendor = row["vendor"]
    inv_date = row["invoice_date"]

    if derived and derived > 0 and norm_name:
        # Normalize the key
        import re
        ing_key = re.sub(r"[^a-z0-9]+", "_", norm_name.lower()).strip("_")

        conn.execute("""
            INSERT INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, vendor_name, invoice_date, updated_at)
            VALUES (?, ?, ?, ?, 'invoice_approved', ?, ?, ?)
            ON CONFLICT(ingredient_key) DO UPDATE SET
                latest_unit_cost=excluded.latest_unit_cost, base_unit=excluded.base_unit,
                cost_source='invoice_approved', vendor_name=excluded.vendor_name,
                invoice_date=excluded.invoice_date, updated_at=excluded.updated_at
        """, (ing_key, norm_name, base_unit, derived, vendor, inv_date, now_iso()))

    conn.commit()
    result = conn.execute("SELECT * FROM invoice_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return dict(result)


def rebuild_ingredient_costs() -> int:
    """Rebuild ALL ingredient costs from approved/high-confidence bulk parse rows.

    For each normalized item, takes the latest invoice with confidence=high.
    Updates ingredients.latest_unit_cost with derived_unit_cost.
    """
    import re
    conn = get_conn()
    now = now_iso()

    # Get latest derived cost per normalized_name (confidence=high only)
    rows = conn.execute("""
        SELECT ii.normalized_name, ii.derived_unit_cost, ii.base_unit,
               i.vendor, i.invoice_date,
               MAX(ii.id) as latest_id
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.derived_unit_cost > 0 AND ii.derived_unit_cost IS NOT NULL
          AND ii.confidence = 'high'
        GROUP BY ii.normalized_name
        ORDER BY ii.normalized_name
    """).fetchall()

    updated = 0
    for r in rows:
        norm = r["normalized_name"]
        if not norm:
            continue
        ing_key = re.sub(r"[^a-z0-9]+", "_", norm.lower()).strip("_")
        base_unit = r["base_unit"] or "unit"
        cost = r["derived_unit_cost"]
        vendor = r["vendor"]
        inv_date = r["invoice_date"]

        conn.execute("""
            INSERT INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, vendor_name, invoice_date, updated_at)
            VALUES (?, ?, ?, ?, 'invoice_approved', ?, ?, ?)
            ON CONFLICT(ingredient_key) DO UPDATE SET
                latest_unit_cost=excluded.latest_unit_cost, base_unit=excluded.base_unit,
                cost_source='invoice_approved', vendor_name=excluded.vendor_name,
                invoice_date=excluded.invoice_date, updated_at=excluded.updated_at
        """, (ing_key, norm, base_unit, cost, vendor, inv_date, now))
        updated += 1

    conn.commit()
    conn.close()
    return updated


def detect_price_inconsistencies() -> list[dict]:
    """Find items where same vendor + same product has different unit_price values.

    Returns list of inconsistencies with suggested correction.
    """
    conn = get_conn()
    # Group by vendor + normalized_name, find those with >1 distinct unit_price
    rows = conn.execute("""
        SELECT i.vendor, ii.normalized_name, ii.unit, ii.quantity,
               GROUP_CONCAT(DISTINCT CAST(ii.unit_price AS TEXT)) as prices,
               COUNT(DISTINCT ROUND(ii.unit_price, 2)) as price_count,
               ROUND(AVG(ii.derived_unit_cost), 5) as avg_derived,
               COUNT(*) as row_count,
               MAX(ii.line_total) as max_line_total
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.unit_price > 0
        GROUP BY i.vendor, ii.normalized_name, ii.unit
        HAVING price_count > 1
        ORDER BY i.vendor, ii.normalized_name
    """).fetchall()
    conn.close()

    results = []
    for r in rows:
        prices = [float(p) for p in r["prices"].split(",")]
        most_likely = r["max_line_total"]  # the case total is usually the correct price

        results.append({
            "vendor": r["vendor"],
            "item": r["normalized_name"],
            "unit": r["unit"],
            "quantity": r["quantity"],
            "prices_found": prices,
            "price_count": r["price_count"],
            "avg_derived": r["avg_derived"],
            "rows": r["row_count"],
            "suggested_unit_price": most_likely,
            "issue": f"Same item has {r['price_count']} different unit prices: {r['prices']}"
        })

    return results


def auto_fix_price_inconsistencies() -> dict:
    """Auto-correct unit_price where same vendor+item has inconsistent prices.

    Rule: when derived_unit_cost is consistent but unit_price varies,
    set unit_price to line_total (the case/total price) for all rows.
    """
    conn = get_conn()
    now = now_iso()

    # Find groups with inconsistent unit_price but consistent derived
    groups = conn.execute("""
        SELECT LOWER(i.vendor) as vendor_lower, ii.normalized_name, ii.unit,
               COUNT(DISTINCT ROUND(ii.unit_price, 2)) as price_count,
               COUNT(DISTINCT ROUND(ii.derived_unit_cost, 4)) as derived_count,
               MAX(ii.line_total) as correct_total
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.unit_price > 0
        GROUP BY vendor_lower, ii.normalized_name, ii.unit
        HAVING price_count > 1 AND derived_count = 1
    """).fetchall()

    fixed = 0
    for g in groups:
        correct = g["correct_total"]
        if correct and correct > 0:
            conn.execute("""
                UPDATE invoice_items SET unit_price = ?
                WHERE normalized_name = ? AND unit = ?
                  AND invoice_id IN (SELECT id FROM invoices WHERE LOWER(vendor) = ?)
            """, (correct, g["normalized_name"], g["unit"], g["vendor_lower"]))
            fixed += conn.execute("SELECT changes()").fetchone()[0]

    conn.commit()
    conn.close()
    return {"groups_fixed": len(groups), "rows_updated": fixed}


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
