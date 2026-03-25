"""
Invoice-to-central-DB bridge.

Called after invoice_db.save_invoice_to_db() writes to invoices.db.
Copies invoice + items into coffee_agi.db and updates ingredients.
"""
from __future__ import annotations

import re
from app.core.db import get_conn, now_iso
from loguru import logger


def post_ingest_to_central_db(parsed_invoice: dict) -> dict:
    """Write a parsed invoice into coffee_agi.db and update ingredient costs.

    Args:
        parsed_invoice: output of extract_invoice_data() or ingest_invoice()

    Returns:
        {"invoice_id": int, "items_stored": int, "ingredients_updated": int, "aliases_added": int}
    """
    conn = get_conn()
    now = now_iso()

    vendor = parsed_invoice.get("vendor", "Unknown")
    inv_num = parsed_invoice.get("invoice_number")
    inv_date = parsed_invoice.get("invoice_date")
    total = parsed_invoice.get("invoice_total") or parsed_invoice.get("total")
    source = parsed_invoice.get("source_file") or parsed_invoice.get("_source_file")

    # Dedupe check
    if inv_num:
        existing = conn.execute(
            "SELECT id FROM invoices WHERE vendor=? AND invoice_number=?",
            (vendor, inv_num)
        ).fetchone()
        if existing:
            conn.close()
            return {"status": "duplicate", "invoice_id": existing["id"]}

    # Insert invoice
    cur = conn.execute("""
        INSERT INTO invoices (vendor, invoice_date, invoice_number, total, source_file, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (vendor, inv_date, inv_num, total, source, now))
    invoice_id = cur.lastrowid

    # Process line items
    items = parsed_invoice.get("line_items", [])
    items_stored = 0
    ingredients_updated = 0
    aliases_added = 0

    for item in items:
        raw_name = item.get("raw_name", "")
        norm_name = item.get("normalized_name", raw_name)
        qty = item.get("quantity")
        unit = item.get("unit", "ea")
        unit_price = item.get("unit_price")
        line_total = item.get("line_total")
        confidence = item.get("confidence", "medium")
        review_req = 1 if item.get("review_required") else 0

        # Compute pack/derived cost
        from maillard.mcp.accounting.invoice_intake import parse_pack_size, _extract_count_from_name
        pack = parse_pack_size(raw_name)
        count_from_name = _extract_count_from_name(raw_name)

        pack_count = None
        pack_text = None
        base_unit = "unit"
        total_items = None
        derived_cost = None
        effective_total = line_total or ((unit_price or 0) * (qty or 1))

        if pack and pack["pack_count"] > 1 and unit in ("case", "box", "pack", "bag", "ea"):
            pack_count = pack["pack_count"]
            pack_text = f"{pack['pack_count']}x{pack['per_unit_size']}{pack['per_unit_unit']}"
            total_items = (qty or 1) * pack_count
            if total_items > 0 and effective_total > 0:
                derived_cost = round(effective_total / total_items, 5)
        elif count_from_name and count_from_name > 1 and unit in ("box", "case", "pack", "bag"):
            pack_count = count_from_name
            pack_text = f"{count_from_name} per {unit}"
            total_items = (qty or 1) * count_from_name
            if total_items > 0 and effective_total > 0:
                derived_cost = round(effective_total / total_items, 5)
        elif pack and pack["pack_count"] == 1:
            pack_count = 1
            base_unit = pack["per_unit_unit"]
            total_items = (qty or 1) * pack["per_unit_size"]
            if total_items > 0 and effective_total > 0:
                derived_cost = round(effective_total / total_items, 5)
        elif unit in ("lb", "kg", "oz", "gal", "L"):
            pack_count = 1
            base_unit = unit
            derived_cost = unit_price
        else:
            pack_count = 1
            total_items = qty or 1
            if total_items > 0 and effective_total > 0:
                derived_cost = round(effective_total / total_items, 5)

        # Insert item
        conn.execute("""
            INSERT INTO invoice_items (invoice_id, raw_name, normalized_name, quantity, unit,
                unit_price, line_total, confidence, review_required,
                pack_count, pack_size_text, base_unit, total_base_units, derived_unit_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (invoice_id, raw_name, norm_name, qty, unit,
              unit_price, line_total, confidence, review_req,
              pack_count, pack_text, base_unit, total_items, derived_cost))
        items_stored += 1

        # Update ingredient if high confidence and derived cost exists
        if derived_cost and derived_cost > 0 and confidence in ("high", "medium"):
            ing_key = re.sub(r"[^a-z0-9]+", "_", (norm_name or raw_name).lower()).strip("_")

            conn.execute("""
                INSERT INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost,
                    cost_source, vendor_name, invoice_date, updated_at)
                VALUES (?, ?, ?, ?, 'invoice_approved', ?, ?, ?)
                ON CONFLICT(ingredient_key) DO UPDATE SET
                    latest_unit_cost = CASE
                        WHEN excluded.latest_unit_cost > 0 THEN excluded.latest_unit_cost
                        ELSE ingredients.latest_unit_cost
                    END,
                    base_unit=excluded.base_unit, cost_source='invoice_approved',
                    vendor_name=excluded.vendor_name, invoice_date=excluded.invoice_date,
                    updated_at=excluded.updated_at
            """, (ing_key, norm_name or raw_name, base_unit, derived_cost,
                  vendor, inv_date, now))
            ingredients_updated += 1

            # Add aliases
            if raw_name and raw_name != norm_name:
                conn.execute("""
                    INSERT OR IGNORE INTO ingredient_aliases (ingredient_key, alias_text, source_type, created_at)
                    VALUES (?, ?, 'invoice_raw', ?)
                """, (ing_key, raw_name, now))
                aliases_added += 1
            if norm_name:
                conn.execute("""
                    INSERT OR IGNORE INTO ingredient_aliases (ingredient_key, alias_text, source_type, created_at)
                    VALUES (?, ?, 'invoice_normalized', ?)
                """, (ing_key, norm_name, now))
                aliases_added += 1

    conn.commit()
    conn.close()

    logger.info(f"[CENTRAL-DB] Invoice #{invoice_id}: {items_stored} items, "
                f"{ingredients_updated} ingredients updated, {aliases_added} aliases")

    return {
        "status": "stored",
        "invoice_id": invoice_id,
        "items_stored": items_stored,
        "ingredients_updated": ingredients_updated,
        "aliases_added": aliases_added,
    }


def refresh_downstream() -> dict:
    """Trigger downstream cost refreshes after invoice import."""
    results = {}

    # 1. Rebuild bulk parse costs
    try:
        from app.data_access.bulk_parse_repo import backfill_bulk_parse, rebuild_ingredient_costs
        results["bulk_parse_updated"] = backfill_bulk_parse()
        results["ingredients_rebuilt"] = rebuild_ingredient_costs()
    except Exception as e:
        results["bulk_parse_error"] = str(e)

    # 2. Rebuild aliases
    try:
        from app.data_access.ingredient_resolver import build_aliases_from_invoices, seed_common_aliases
        results["aliases_from_invoices"] = build_aliases_from_invoices()
        results["common_aliases"] = seed_common_aliases()
    except Exception as e:
        results["alias_error"] = str(e)

    return results
