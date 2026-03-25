"""
Centralized product database for Coffee AGI.

Single source of truth for ingredients, recipes, modifiers, and costs.
SQLite at data/products.db. All reads/writes go through this module.

Tables:
  ingredients        — ingredient catalog with costs
  recipes            — product recipe headers
  recipe_ingredients — recipe → ingredient quantities
  modifiers          — modifier headers
  modifier_rules     — modifier → ingredient add/remove rules
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "data"
_DB_PATH = _DATA / "products.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════════════════

def init_db() -> None:
    """Create all tables. Safe to call repeatedly."""
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_key  TEXT PRIMARY KEY,
            display_name    TEXT NOT NULL,
            base_unit       TEXT NOT NULL DEFAULT 'ea',
            latest_unit_cost REAL NOT NULL DEFAULT 0,
            cost_source     TEXT NOT NULL DEFAULT 'manual',
            updated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS recipes (
            recipe_key      TEXT PRIMARY KEY,
            display_name    TEXT NOT NULL,
            sell_price      REAL NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'approved',
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_key      TEXT NOT NULL REFERENCES recipes(recipe_key) ON DELETE CASCADE,
            ingredient_key  TEXT NOT NULL REFERENCES ingredients(ingredient_key),
            quantity        REAL NOT NULL DEFAULT 0,
            unit            TEXT NOT NULL DEFAULT 'ea',
            UNIQUE(recipe_key, ingredient_key)
        );
        CREATE INDEX IF NOT EXISTS ix_ri_recipe ON recipe_ingredients(recipe_key);

        CREATE TABLE IF NOT EXISTS modifiers (
            modifier_key    TEXT PRIMARY KEY,
            display_name    TEXT NOT NULL,
            type            TEXT NOT NULL DEFAULT 'add',
            upcharge        REAL NOT NULL DEFAULT 0,
            scale_factor    REAL,
            updated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS modifier_rules (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            modifier_key    TEXT NOT NULL REFERENCES modifiers(modifier_key) ON DELETE CASCADE,
            action          TEXT NOT NULL DEFAULT 'add',
            ingredient_key  TEXT NOT NULL,
            quantity        REAL,
            unit            TEXT NOT NULL DEFAULT 'ea',
            quantity_from_removed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(modifier_key, action, ingredient_key)
        );
        CREATE INDEX IF NOT EXISTS ix_mr_mod ON modifier_rules(modifier_key);
    """)
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# SEED FROM JSON (one-time migration)
# ═══════════════════════════════════════════════════════════════════════════

def seed_from_json() -> dict:
    """Import all existing JSON data into the database. Idempotent — uses INSERT OR REPLACE."""
    init_db()
    conn = _connect()
    now = _now()
    counts = {"ingredients": 0, "recipes": 0, "recipe_ingredients": 0, "modifiers": 0, "modifier_rules": 0}

    # ── Ingredients from costs.json ──
    costs = _load_json("costs.json")
    for key, cost in costs.items():
        unit = _infer_unit(key)
        display = key.replace("_", " ").replace(" kg", " (kg)").replace(" liters", " (L)").replace(" ml", " (ml)").title()
        conn.execute("""
            INSERT OR REPLACE INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, updated_at)
            VALUES (?, ?, ?, ?, 'manual', ?)
        """, (key, display, unit, cost, now))
        counts["ingredients"] += 1

    # ── Recipes from recipes.json + prices.json ──
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

        # Ensure all recipe ingredients exist in ingredients table
        for ikey, qty in ingredients.items():
            conn.execute("""
                INSERT OR IGNORE INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, updated_at)
                VALUES (?, ?, ?, 0, 'unknown', ?)
            """, (ikey, ikey.replace("_", " ").title(), _infer_unit(ikey), now))

            conn.execute("""
                INSERT OR REPLACE INTO recipe_ingredients (recipe_key, ingredient_key, quantity, unit)
                VALUES (?, ?, ?, ?)
            """, (rkey, ikey, qty, _infer_unit(ikey)))
            counts["recipe_ingredients"] += 1

    # ── Modifiers from modifiers.json ──
    modifiers = _load_json("modifiers.json")
    for mkey, mdata in modifiers.items():
        conn.execute("""
            INSERT OR REPLACE INTO modifiers (modifier_key, display_name, type, upcharge, scale_factor, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (mkey, mdata.get("display", mkey), mdata.get("type", "add"),
              mdata.get("upcharge", 0), mdata.get("scale_factor"), now))
        counts["modifiers"] += 1

        # Delete existing rules and re-insert
        conn.execute("DELETE FROM modifier_rules WHERE modifier_key=?", (mkey,))

        for add in mdata.get("adds", []):
            ikey = add["ingredient_key"]
            conn.execute("""
                INSERT OR IGNORE INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, updated_at)
                VALUES (?, ?, ?, 0, 'unknown', ?)
            """, (ikey, ikey.replace("_", " ").title(), add.get("unit", "ea"), now))

            conn.execute("""
                INSERT INTO modifier_rules (modifier_key, action, ingredient_key, quantity, unit, quantity_from_removed)
                VALUES (?, 'add', ?, ?, ?, ?)
            """, (mkey, ikey, add.get("quantity"), add.get("unit", "ea"),
                  1 if add.get("quantity_from_removed") else 0))
            counts["modifier_rules"] += 1

        for rem in mdata.get("removes", []):
            conn.execute("""
                INSERT INTO modifier_rules (modifier_key, action, ingredient_key, quantity, unit, quantity_from_removed)
                VALUES (?, 'remove', ?, NULL, '', 0)
            """, (mkey, rem["ingredient_key"]))
            counts["modifier_rules"] += 1

    conn.commit()
    conn.close()
    return counts


# ═══════════════════════════════════════════════════════════════════════════
# INGREDIENTS
# ═══════════════════════════════════════════════════════════════════════════

def get_ingredient(key: str) -> dict | None:
    conn = _connect()
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_ingredients() -> list[dict]:
    conn = _connect()
    rows = conn.execute("SELECT * FROM ingredients ORDER BY ingredient_key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_ingredient_cost(key: str, unit_cost: float, source: str = "manual") -> dict | None:
    conn = _connect()
    now = _now()
    conn.execute("""
        UPDATE ingredients SET latest_unit_cost=?, cost_source=?, updated_at=?
        WHERE ingredient_key=?
    """, (unit_cost, source, now, key))
    conn.commit()
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_ingredient(key: str, display: str, unit: str, cost: float = 0, source: str = "manual") -> dict:
    conn = _connect()
    now = _now()
    conn.execute("""
        INSERT INTO ingredients (ingredient_key, display_name, base_unit, latest_unit_cost, cost_source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(ingredient_key) DO UPDATE SET
            display_name=excluded.display_name, base_unit=excluded.base_unit,
            latest_unit_cost=excluded.latest_unit_cost, cost_source=excluded.cost_source,
            updated_at=excluded.updated_at
    """, (key, display, unit, cost, source, now))
    conn.commit()
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=?", (key,)).fetchone()
    conn.close()
    return dict(row)


# ═══════════════════════════════════════════════════════════════════════════
# RECIPES
# ═══════════════════════════════════════════════════════════════════════════

def get_recipe(key: str) -> dict | None:
    conn = _connect()
    recipe = conn.execute("SELECT * FROM recipes WHERE recipe_key=?", (key,)).fetchone()
    if not recipe:
        conn.close()
        return None
    ings = conn.execute("""
        SELECT ri.ingredient_key, ri.quantity, ri.unit, i.display_name, i.latest_unit_cost, i.cost_source
        FROM recipe_ingredients ri
        LEFT JOIN ingredients i ON ri.ingredient_key = i.ingredient_key
        WHERE ri.recipe_key=?
    """, (key,)).fetchall()
    conn.close()
    return {**dict(recipe), "ingredients": [dict(r) for r in ings]}


def get_all_recipes(status: str | None = None) -> list[dict]:
    conn = _connect()
    if status:
        rows = conn.execute("SELECT * FROM recipes WHERE status=? ORDER BY recipe_key", (status,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM recipes ORDER BY recipe_key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recipe_ingredients(key: str) -> dict:
    """Return {ingredient_key: quantity} for a recipe — matches old recipes.json format."""
    conn = _connect()
    rows = conn.execute("SELECT ingredient_key, quantity FROM recipe_ingredients WHERE recipe_key=?", (key,)).fetchall()
    conn.close()
    return {r["ingredient_key"]: r["quantity"] for r in rows}


def get_all_recipes_as_dict() -> dict:
    """Return all approved recipes as {recipe_key: {ingredient_key: qty}} — drop-in for recipes.json."""
    conn = _connect()
    recipes = conn.execute("SELECT recipe_key FROM recipes WHERE status='approved'").fetchall()
    result = {}
    for r in recipes:
        rk = r["recipe_key"]
        ings = conn.execute("SELECT ingredient_key, quantity FROM recipe_ingredients WHERE recipe_key=?", (rk,)).fetchall()
        result[rk] = {i["ingredient_key"]: i["quantity"] for i in ings}
    conn.close()
    return result


def get_all_prices() -> dict:
    """Return {recipe_key: sell_price} — drop-in for prices.json."""
    conn = _connect()
    rows = conn.execute("SELECT recipe_key, sell_price FROM recipes WHERE status='approved' AND sell_price > 0").fetchall()
    conn.close()
    return {r["recipe_key"]: r["sell_price"] for r in rows}


def get_all_costs() -> dict:
    """Return {ingredient_key: latest_unit_cost} — drop-in for costs.json."""
    conn = _connect()
    rows = conn.execute("SELECT ingredient_key, latest_unit_cost FROM ingredients WHERE latest_unit_cost > 0").fetchall()
    conn.close()
    return {r["ingredient_key"]: r["latest_unit_cost"] for r in rows}


def update_recipe(key: str, display: str | None = None, sell_price: float | None = None,
                  status: str | None = None, ingredients: list[dict] | None = None) -> dict | None:
    conn = _connect()
    now = _now()
    recipe = conn.execute("SELECT * FROM recipes WHERE recipe_key=?", (key,)).fetchone()
    if not recipe:
        conn.close()
        return None
    if display is not None:
        conn.execute("UPDATE recipes SET display_name=?, updated_at=? WHERE recipe_key=?", (display, now, key))
    if sell_price is not None:
        conn.execute("UPDATE recipes SET sell_price=?, updated_at=? WHERE recipe_key=?", (sell_price, now, key))
    if status is not None:
        conn.execute("UPDATE recipes SET status=?, updated_at=? WHERE recipe_key=?", (status, now, key))
    if ingredients is not None:
        conn.execute("DELETE FROM recipe_ingredients WHERE recipe_key=?", (key,))
        for ing in ingredients:
            conn.execute("""
                INSERT INTO recipe_ingredients (recipe_key, ingredient_key, quantity, unit)
                VALUES (?, ?, ?, ?)
            """, (key, ing["ingredient_key"], ing.get("quantity", 0), ing.get("unit", "ea")))
    conn.commit()
    conn.close()
    return get_recipe(key)


# ═══════════════════════════════════════════════════════════════════════════
# MODIFIERS
# ═══════════════════════════════════════════════════════════════════════════

def get_modifier(key: str) -> dict | None:
    conn = _connect()
    mod = conn.execute("SELECT * FROM modifiers WHERE modifier_key=?", (key,)).fetchone()
    if not mod:
        conn.close()
        return None
    rules = conn.execute("SELECT * FROM modifier_rules WHERE modifier_key=?", (key,)).fetchall()
    conn.close()
    result = dict(mod)
    result["adds"] = [dict(r) for r in rules if r["action"] == "add"]
    result["removes"] = [dict(r) for r in rules if r["action"] == "remove"]
    return result


def get_all_modifiers() -> dict:
    """Return all modifiers in the format cost_engine expects — drop-in for modifiers.json."""
    conn = _connect()
    mods = conn.execute("SELECT * FROM modifiers").fetchall()
    rules = conn.execute("SELECT * FROM modifier_rules").fetchall()
    conn.close()

    rules_by_mod: dict[str, list] = {}
    for r in rules:
        rules_by_mod.setdefault(r["modifier_key"], []).append(dict(r))

    result = {}
    for m in mods:
        mk = m["modifier_key"]
        mod_rules = rules_by_mod.get(mk, [])
        entry: dict = {
            "display": m["display_name"],
            "type": m["type"],
            "upcharge": m["upcharge"],
        }
        if m["scale_factor"] is not None:
            entry["scale_factor"] = m["scale_factor"]

        adds = []
        for r in mod_rules:
            if r["action"] == "add":
                a: dict = {"ingredient_key": r["ingredient_key"], "unit": r["unit"]}
                if r["quantity_from_removed"]:
                    a["quantity_from_removed"] = True
                else:
                    a["quantity"] = r["quantity"]
                adds.append(a)
        if adds:
            entry["adds"] = adds

        removes = [{"ingredient_key": r["ingredient_key"]} for r in mod_rules if r["action"] == "remove"]
        if removes:
            entry["removes"] = removes

        result[mk] = entry
    return result


def update_modifier(key: str, display: str | None = None, mod_type: str | None = None,
                    upcharge: float | None = None, scale_factor: float | None = None,
                    adds: list[dict] | None = None, removes: list[dict] | None = None) -> dict | None:
    conn = _connect()
    now = _now()
    mod = conn.execute("SELECT * FROM modifiers WHERE modifier_key=?", (key,)).fetchone()
    if not mod:
        conn.close()
        return None
    if display is not None:
        conn.execute("UPDATE modifiers SET display_name=?, updated_at=? WHERE modifier_key=?", (display, now, key))
    if mod_type is not None:
        conn.execute("UPDATE modifiers SET type=?, updated_at=? WHERE modifier_key=?", (mod_type, now, key))
    if upcharge is not None:
        conn.execute("UPDATE modifiers SET upcharge=?, updated_at=? WHERE modifier_key=?", (upcharge, now, key))
    if scale_factor is not None:
        conn.execute("UPDATE modifiers SET scale_factor=?, updated_at=? WHERE modifier_key=?", (scale_factor, now, key))
    if adds is not None or removes is not None:
        conn.execute("DELETE FROM modifier_rules WHERE modifier_key=?", (key,))
        for a in (adds or []):
            conn.execute("""
                INSERT INTO modifier_rules (modifier_key, action, ingredient_key, quantity, unit, quantity_from_removed)
                VALUES (?, 'add', ?, ?, ?, ?)
            """, (key, a["ingredient_key"], a.get("quantity"), a.get("unit", "ea"),
                  1 if a.get("quantity_from_removed") else 0))
        for r in (removes or []):
            conn.execute("""
                INSERT INTO modifier_rules (modifier_key, action, ingredient_key, quantity, unit, quantity_from_removed)
                VALUES (?, 'remove', ?, NULL, '', 0)
            """, (key, r["ingredient_key"]))
    conn.commit()
    conn.close()
    return get_modifier(key)


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(name: str) -> dict:
    p = _DATA / name
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
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
