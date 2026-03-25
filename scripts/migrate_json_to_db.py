"""
Migrate all JSON + old SQLite data into data/coffee_agi.db.

Sources:
  data/costs.json        -> ingredients table
  data/recipes.json      -> recipes + recipe_ingredients tables
  data/prices.json       -> recipes.sell_price
  data/modifiers.json    -> modifiers + modifier_rules tables
  data/invoices.db       -> invoices + invoice_items tables

Idempotent: uses INSERT OR REPLACE / INSERT OR IGNORE.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.db import get_conn, init_db, now_iso

_DATA = ROOT / "data"


def _load_json(name: str) -> dict | list:
    p = _DATA / name
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def _infer_unit(key: str) -> str:
    k = key.lower()
    if "kg" in k: return "kg"
    if "liter" in k: return "liters"
    if "ml" in k: return "ml"
    if "_g" in k or "gram" in k: return "g"
    if "oz" in k: return "oz"
    if "lb" in k: return "lb"
    return "ea"


def migrate() -> dict:
    init_db()
    conn = get_conn()
    now = now_iso()
    counts = {"ingredients": 0, "recipes": 0, "recipe_ingredients": 0,
              "modifiers": 0, "modifier_rules": 0, "invoices": 0, "invoice_items": 0}

    # ── 1. Ingredients from costs.json ──
    costs = _load_json("costs.json")
    for key, cost in costs.items():
        unit = _infer_unit(key)
        display = key.replace("_", " ").title()
        conn.execute("""
            INSERT OR REPLACE INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, updated_at)
            VALUES (?, ?, ?, ?, 'manual', ?)
        """, (key, display, unit, cost, now))
        counts["ingredients"] += 1
    print(f"  ingredients: {counts['ingredients']} from costs.json")

    # ── 2. Recipes from recipes.json + prices.json ──
    recipes = _load_json("recipes.json")
    prices = _load_json("prices.json")
    for rkey, ingredients in recipes.items():
        display = rkey.replace("_", " ").title()
        price = prices.get(rkey, 0)
        conn.execute("""
            INSERT OR REPLACE INTO recipes (recipe_key, display_name, sell_price, status, created_at, updated_at)
            VALUES (?, ?, ?, 'approved', ?, ?)
        """, (rkey, display, price, now, now))
        counts["recipes"] += 1

        # Clear and re-insert ingredients
        conn.execute("DELETE FROM recipe_ingredients WHERE recipe_key=?", (rkey,))
        for ikey, qty in ingredients.items():
            # Ensure ingredient exists
            conn.execute("""
                INSERT OR IGNORE INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, updated_at)
                VALUES (?, ?, ?, 0, 'unknown', ?)
            """, (ikey, ikey.replace("_", " ").title(), _infer_unit(ikey), now))
            conn.execute("""
                INSERT INTO recipe_ingredients (recipe_key, ingredient_key, quantity, unit)
                VALUES (?, ?, ?, ?)
            """, (rkey, ikey, qty, _infer_unit(ikey)))
            counts["recipe_ingredients"] += 1
    print(f"  recipes: {counts['recipes']}, recipe_ingredients: {counts['recipe_ingredients']}")

    # ── 3. Modifiers from modifiers.json ──
    modifiers = _load_json("modifiers.json")
    for mkey, mdata in modifiers.items():
        conn.execute("""
            INSERT OR REPLACE INTO modifiers (modifier_key, display_name, type, upcharge, scale_factor, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'approved', ?, ?)
        """, (mkey, mdata.get("display", mkey), mdata.get("type", "add"),
              mdata.get("upcharge", 0), mdata.get("scale_factor"), now, now))
        counts["modifiers"] += 1

        conn.execute("DELETE FROM modifier_rules WHERE modifier_key=?", (mkey,))
        for add in mdata.get("adds", []):
            ikey = add["ingredient_key"]
            conn.execute("""
                INSERT OR IGNORE INTO ingredients (ingredient_key, display_name, base_unit, updated_at)
                VALUES (?, ?, ?, ?)
            """, (ikey, ikey.replace("_", " ").title(), add.get("unit", "ea"), now))
            conn.execute("""
                INSERT INTO modifier_rules (modifier_key, ingredient_key, action, quantity, unit, quantity_from_removed)
                VALUES (?, ?, 'add', ?, ?, ?)
            """, (mkey, ikey, add.get("quantity"), add.get("unit", "ea"),
                  1 if add.get("quantity_from_removed") else 0))
            counts["modifier_rules"] += 1
        for rem in mdata.get("removes", []):
            conn.execute("""
                INSERT INTO modifier_rules (modifier_key, ingredient_key, action, quantity, unit, quantity_from_removed)
                VALUES (?, ?, 'remove', NULL, '', 0)
            """, (mkey, rem["ingredient_key"]))
            counts["modifier_rules"] += 1
    print(f"  modifiers: {counts['modifiers']}, modifier_rules: {counts['modifier_rules']}")

    # ── 4. Invoices from invoices.db ──
    old_db = _DATA / "invoices.db"
    if old_db.exists():
        old = sqlite3.connect(str(old_db))
        old.row_factory = sqlite3.Row

        inv_rows = old.execute("SELECT * FROM invoices").fetchall()
        id_map = {}  # old id -> new id
        for inv in inv_rows:
            # Skip duplicates
            existing = conn.execute(
                "SELECT id FROM invoices WHERE LOWER(vendor)=LOWER(?) AND invoice_number=?",
                (inv["vendor"], inv["invoice_number"])
            ).fetchone()
            if existing:
                id_map[inv["id"]] = existing["id"]
                continue
            cur = conn.execute("""
                INSERT INTO invoices (vendor, invoice_date, invoice_number, total, source_file, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (inv["vendor"], inv["invoice_date"], inv["invoice_number"],
                  inv["total"], inv["source_file"], inv["created_at"]))
            id_map[inv["id"]] = cur.lastrowid
            counts["invoices"] += 1

        item_rows = old.execute("SELECT * FROM invoice_items").fetchall()
        for item in item_rows:
            new_inv_id = id_map.get(item["invoice_id"])
            if new_inv_id is None:
                continue
            conn.execute("""
                INSERT INTO invoice_items (invoice_id, raw_name, normalized_name, quantity, unit,
                                           price_basis, unit_price, line_total, override_source,
                                           confidence, review_required)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_inv_id, item["raw_name"], item["normalized_name"],
                  item["quantity"], item["unit"], item["price_basis"],
                  item["unit_price"], item["line_total"], item["override_source"],
                  item["confidence"], item["review_required"]))
            counts["invoice_items"] += 1

        old.close()
        print(f"  invoices: {counts['invoices']}, invoice_items: {counts['invoice_items']}")
    else:
        print("  invoices.db not found — skipping invoice migration")

    conn.commit()
    conn.close()
    print(f"\nMigration complete -> {_DATA / 'coffee_agi.db'}")
    return counts


if __name__ == "__main__":
    print("Migrating JSON + invoices.db -> coffee_agi.db...")
    result = migrate()
    print(json.dumps(result, indent=2))
