"""
Cost Intelligence Engine for Maillard Coffee Roasters.

Calculates per-ingredient and total product cost from approved recipes.
Reads from products.db (centralized DB). Falls back to JSON if DB not seeded.

No overhead, no labor — ingredient cost only.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent.parent.parent / "data"
_INVOICES_DB = _DATA / "invoices.db"


def _load(name: str) -> dict:
    """Load from coffee_agi.db first, fall back to JSON file."""
    try:
        if name == "recipes.json":
            from app.data_access.recipes_repo import get_all_recipes_dict
            data = get_all_recipes_dict()
            if data:
                return data
        elif name == "costs.json":
            from app.core.db import get_conn
            conn = get_conn()
            rows = conn.execute("SELECT ingredient_key, latest_unit_cost FROM ingredients WHERE latest_unit_cost > 0").fetchall()
            conn.close()
            if rows:
                return {r["ingredient_key"]: r["latest_unit_cost"] for r in rows}
        elif name == "prices.json":
            from app.data_access.recipes_repo import get_all_prices
            data = get_all_prices()
            if data:
                return data
        elif name == "modifiers.json":
            from app.data_access.modifiers_repo import get_all_modifiers_dict
            data = get_all_modifiers_dict()
            if data:
                return data
    except Exception:
        pass
    # Fallback to JSON
    p = _DATA / name
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


# ═══════════════════════════════════════════════════════════════════════════
# UNIT CONVERSION
# ═══════════════════════════════════════════════════════════════════════════

# All conversions go through a base unit per dimension:
#   mass  → grams
#   volume → ml
#   count → units (no conversion)

_TO_BASE = {
    # Mass → grams
    "g": 1.0, "gram": 1.0, "grams": 1.0,
    "kg": 1000.0,
    "oz": 28.3495, "ounce": 28.3495,
    "lb": 453.592, "lbs": 453.592, "pound": 453.592,
    # Volume → ml
    "ml": 1.0, "milliliter": 1.0,
    "liters": 1000.0, "liter": 1000.0, "l": 1000.0,
    "gallon": 3785.41, "gal": 3785.41,
    "cup": 236.588,
    "tbsp": 14.787, "tablespoon": 14.787,
    "tsp": 4.929, "teaspoon": 4.929,
    "fl_oz": 29.5735,
    # Count
    "ea": 1.0, "unit": 1.0, "units": 1.0, "pcs": 1.0,
    "box": 1.0, "case": 1.0,  # these are count units — pack-level pricing
}

_MASS_UNITS = {"g", "gram", "grams", "kg", "oz", "ounce", "lb", "lbs", "pound"}
_VOLUME_UNITS = {"ml", "milliliter", "liters", "liter", "l", "gallon", "gal", "cup",
                 "tbsp", "tablespoon", "tsp", "teaspoon", "fl_oz"}
_COUNT_UNITS = {"ea", "unit", "units", "pcs", "box", "case", ""}


def _unit_dimension(u: str) -> str:
    u = u.lower().strip()
    if u in _MASS_UNITS:
        return "mass"
    if u in _VOLUME_UNITS:
        return "volume"
    return "count"


def convert_units(quantity: float, from_unit: str, to_unit: str) -> float | None:
    """Convert quantity between compatible units. Returns None if incompatible."""
    from_u = from_unit.lower().strip()
    to_u = to_unit.lower().strip()

    if from_u == to_u:
        return quantity

    dim_from = _unit_dimension(from_u)
    dim_to = _unit_dimension(to_u)

    if dim_from != dim_to:
        return None  # incompatible (e.g., grams vs ml)

    if dim_from == "count":
        return quantity  # count units are all 1:1 at the unit level

    base_from = _TO_BASE.get(from_u)
    base_to = _TO_BASE.get(to_u)

    if base_from is None or base_to is None:
        return None

    # Convert: quantity * (from→base) / (to→base)
    return quantity * base_from / base_to


# ═══════════════════════════════════════════════════════════════════════════
# INGREDIENT COST LOOKUP
# ═══════════════════════════════════════════════════════════════════════════

def get_ingredient_cost(ingredient_key: str, _depth: int = 0) -> dict:
    """Look up cost for an ingredient using the canonical ingredient resolver.

    Priority:
      1. Exact ingredient_key match in DB
      2. Alias resolution (maps generic keys like 'bakery_bag_or_wrapper' to canonical ingredients)
      3. costs.json manual fallback

    STRICT: Never returns raw invoice package price. Only derived per-unit costs.

    Returns:
        {"unit_cost": float, "unit": str, "source": str, "resolved_key": str}
    """
    # 1. Try resolver (exact key -> alias -> fuzzy)
    try:
        from app.data_access.ingredient_resolver import resolve_ingredient
        resolved = resolve_ingredient(ingredient_key)
        if resolved and resolved.get("latest_unit_cost") and resolved["latest_unit_cost"] > 0:
            return {
                "unit_cost": resolved["latest_unit_cost"],
                "unit": resolved.get("base_unit") or _infer_unit_from_key(ingredient_key),
                "source": resolved.get("cost_source") or "db",
                "vendor": resolved.get("vendor_name"),
                "resolved_key": resolved.get("ingredient_key"),
                "match_type": resolved.get("match_type"),
            }
    except Exception:
        pass

    # 2. Check if it's another recipe (recipe-as-ingredient)
    try:
        recipes = _load("recipes.json")
        if ingredient_key in recipes:
            # Calculate that recipe's cost and use it
            sub_cost = _calculate_recipe_cost_no_recurse(ingredient_key, _depth=_depth)
            if sub_cost and sub_cost > 0:
                return {
                    "unit_cost": sub_cost,
                    "unit": "ea",
                    "source": "recipe",
                    "resolved_key": ingredient_key,
                    "match_type": "sub_recipe",
                }
    except Exception:
        pass

    # 3. Fallback to costs.json (manual per-unit costs)
    manual = _load("costs.json")
    if ingredient_key in manual:
        unit = _infer_unit_from_key(ingredient_key)
        return {"unit_cost": manual[ingredient_key], "unit": unit, "source": "manual"}

    return {"unit_cost": 0, "unit": "", "source": "none"}


def _calculate_recipe_cost_no_recurse(recipe_key: str, _depth: int = 0) -> float | None:
    """Calculate a recipe's total cost for use as a sub-ingredient. Max depth 3 to prevent loops."""
    if _depth > 3:
        return None
    recipes = _load("recipes.json")
    if recipe_key not in recipes:
        return None
    ingredients = recipes[recipe_key]
    total = 0
    for ing_key, quantity in ingredients.items():
        if quantity is None or quantity == 0:
            continue
        # Prevent infinite recursion: don't let sub-recipe look up itself
        if ing_key == recipe_key:
            continue
        unit = _infer_unit_from_key(ing_key)
        cost_info = get_ingredient_cost(ing_key, _depth=_depth + 1)
        total += float(quantity) * cost_info["unit_cost"]
    return round(total, 4) if total > 0 else None


def _lookup_invoice_cost(ingredient_key: str) -> dict | None:
    """Search invoice DB for the latest price matching this ingredient key."""
    if not _INVOICES_DB.exists():
        return None

    # Build search terms from the ingredient key
    clean = ingredient_key.replace("_", " ").lower()
    # Remove unit suffixes for searching
    for suffix in ("kg", "liters", "liter", "ml", "g", "oz", "lb", "ea"):
        clean = clean.replace(suffix, "").strip()

    if not clean or len(clean) < 3:
        return None

    try:
        conn = sqlite3.connect(str(_INVOICES_DB))
        conn.row_factory = sqlite3.Row

        # Search by tokens
        tokens = [t for t in clean.split() if len(t) >= 3]
        if not tokens:
            conn.close()
            return None

        # Try exact match first, then fuzzy
        for token in tokens:
            rows = conn.execute("""
                SELECT normalized_name, unit_price, unit
                FROM invoice_items
                WHERE LOWER(normalized_name) LIKE ? AND unit_price > 0
                ORDER BY rowid DESC LIMIT 1
            """, (f"%{token}%",)).fetchall()

            if rows:
                r = rows[0]
                conn.close()
                return {
                    "unit_cost": round(r["unit_price"], 2),
                    "unit": r["unit"] or "ea",
                    "source": "invoice",
                    "invoice_item": r["normalized_name"],
                }

        conn.close()
    except Exception:
        pass

    return None


def _infer_unit_from_key(key: str) -> str:
    """Guess the unit from the ingredient key name."""
    k = key.lower()
    if k.endswith("_kg") or "kg" in k:
        return "kg"
    if k.endswith("_liters") or "liter" in k:
        return "liters"
    if k.endswith("_ml") or "ml" in k:
        return "ml"
    if k.endswith("_g") or k.endswith("_gram"):
        return "g"
    if k.endswith("_oz"):
        return "oz"
    if k.endswith("_lb"):
        return "lb"
    return "ea"


# ═══════════════════════════════════════════════════════════════════════════
# RECIPE COST CALCULATION
# ═══════════════════════════════════════════════════════════════════════════

def calculate_recipe_line_cost(
    ingredient_key: str,
    quantity: float,
    recipe_unit: str,
) -> dict:
    """Calculate cost for one recipe line.

    Returns:
        {"ingredient_key", "quantity", "unit", "unit_cost", "cost_unit",
         "line_cost", "cost_source", "converted_qty"}
    """
    cost_info = get_ingredient_cost(ingredient_key)
    unit_cost = cost_info["unit_cost"]
    cost_unit = cost_info["unit"]
    source = cost_info["source"]

    if source == "none" or unit_cost == 0:
        return {
            "ingredient_key": ingredient_key,
            "quantity": quantity,
            "unit": recipe_unit,
            "unit_cost": 0,
            "cost_unit": "",
            "line_cost": 0,
            "cost_source": "none",
            "converted_qty": quantity,
        }

    # Try to convert recipe quantity to cost unit for accurate pricing
    converted_qty = quantity
    if recipe_unit and cost_unit and recipe_unit != cost_unit:
        conv = convert_units(quantity, recipe_unit, cost_unit)
        if conv is not None:
            converted_qty = conv
        # If conversion fails, assume units are compatible (same dimension)
        # This handles cases like recipe in "kg" and cost in "kg"

    line_cost = round(converted_qty * unit_cost, 4)

    return {
        "ingredient_key": ingredient_key,
        "quantity": quantity,
        "unit": recipe_unit,
        "unit_cost": unit_cost,
        "cost_unit": cost_unit,
        "line_cost": round(line_cost, 2),
        "cost_source": source,
        "converted_qty": round(converted_qty, 4),
    }


def calculate_recipe_cost(recipe_key: str) -> dict | None:
    """Calculate full cost breakdown for an approved recipe.

    Returns None if recipe not found. Only uses approved recipes.

    Returns:
        {
            "recipe_key": str,
            "display_name": str,
            "cost_breakdown": [line_cost_dicts],
            "total_cost": float,
            "missing_costs": [ingredient_keys with no cost source],
        }
    """
    recipes = _load("recipes.json")
    if recipe_key not in recipes:
        return None

    ingredients = recipes[recipe_key]
    breakdown = []
    missing = []

    for ing_key, quantity in ingredients.items():
        if quantity is None or quantity == 0:
            continue

        # Infer unit from the key
        unit = _infer_unit_from_key(ing_key)

        line = calculate_recipe_line_cost(ing_key, float(quantity), unit)
        breakdown.append(line)

        if line["cost_source"] == "none":
            missing.append(ing_key)

    total = round(sum(l["line_cost"] for l in breakdown), 2)

    # Try to get display name from recipe drafts
    display = recipe_key.replace("_", " ").title()
    try:
        drafts_file = _DATA / "recipe_drafts.json"
        if drafts_file.exists():
            drafts = json.loads(drafts_file.read_text(encoding="utf-8"))
            for d in drafts:
                if d.get("recipe_key") == recipe_key:
                    display = d.get("display_name", display)
                    break
    except Exception:
        pass

    # Product economics: sell price, gross profit, margin
    prices = _load("prices.json")
    sell_price = round(prices.get(recipe_key, 0), 2)
    gross_profit = round(sell_price - total, 2) if sell_price > 0 else 0
    margin_pct = round(gross_profit / sell_price * 100, 1) if sell_price > 0 else 0

    return {
        "recipe_key": recipe_key,
        "display_name": display,
        "total_cost": total,
        "sell_price": sell_price,
        "gross_profit": gross_profit,
        "margin_pct": margin_pct,
        "cost_breakdown": breakdown,
        "missing_costs": missing,
    }


def calculate_all_recipe_costs() -> dict:
    """Calculate costs for all approved recipes. Returns {key: cost_dict}."""
    recipes = _load("recipes.json")
    results = {}
    for key in recipes:
        cost = calculate_recipe_cost(key)
        if cost:
            results[key] = cost
    return results


# ═══════════════════════════════════════════════════════════════════════════
# MODIFIER COSTING
# ═══════════════════════════════════════════════════════════════════════════

def calculate_item_cost_with_modifiers(
    recipe_key: str,
    modifier_keys: list[str],
) -> dict | None:
    """Calculate the cost of a product with modifiers applied.

    Args:
        recipe_key: Approved recipe key (e.g. "latte")
        modifier_keys: List of modifier keys (e.g. ["oat_milk", "extra_shot"])

    Returns:
        {
            "recipe_key", "display_name",
            "base_cost", "base_breakdown",
            "modifiers": [{"modifier_key", "display", "type", "cost_impact",
                           "upcharge", "modifier_profit"}],
            "final_cost", "total_upcharge", "total_modifier_profit",
            "sell_price", "gross_profit", "margin_pct",
        }
    """
    base = calculate_recipe_cost(recipe_key)
    if base is None:
        return None

    modifiers_data = _load("modifiers.json")
    recipes = _load("recipes.json")
    recipe_ingredients = dict(recipes.get(recipe_key, {}))

    # Working copy of ingredient quantities for replacement tracking
    working = {k: v for k, v in recipe_ingredients.items() if v}

    modifier_results = []
    total_cost_delta = 0
    total_upcharge = 0

    for mk in modifier_keys:
        mod = modifiers_data.get(mk)
        if not mod:
            modifier_results.append({
                "modifier_key": mk, "display": mk, "type": "unknown",
                "cost_impact": 0, "upcharge": 0, "modifier_profit": 0,
                "error": f"Modifier '{mk}' not found",
            })
            continue

        mod_type = mod.get("type", "add")
        upcharge = mod.get("upcharge", 0)
        cost_impact = 0

        if mod_type == "size_upgrade":
            # Scale all current ingredients by the factor
            scale = mod.get("scale_factor", 1.5)
            added_cost = 0
            for ing_key, qty in working.items():
                extra_qty = qty * (scale - 1)
                unit = _infer_unit_from_key(ing_key)
                line = calculate_recipe_line_cost(ing_key, extra_qty, unit)
                added_cost += line["line_cost"]
            # Update working quantities
            for ing_key in working:
                working[ing_key] *= scale
            cost_impact = round(added_cost, 2)

        else:
            # Handle removes (for replace type)
            removed_cost = 0
            removed_quantities = {}
            for rem in mod.get("removes", []):
                rem_key = rem["ingredient_key"]
                if rem_key in working:
                    qty = working[rem_key]
                    unit = _infer_unit_from_key(rem_key)
                    line = calculate_recipe_line_cost(rem_key, qty, unit)
                    removed_cost += line["line_cost"]
                    removed_quantities[rem_key] = qty
                    del working[rem_key]

            # Handle adds
            added_cost = 0
            for add in mod.get("adds", []):
                add_key = add["ingredient_key"]
                add_unit = add.get("unit", _infer_unit_from_key(add_key))

                if add.get("quantity_from_removed"):
                    # Use the same quantity as the removed ingredient
                    qty = 0
                    for rk, rq in removed_quantities.items():
                        qty = rq
                        break
                else:
                    qty = add.get("quantity", 0)

                if qty > 0:
                    line = calculate_recipe_line_cost(add_key, qty, add_unit)
                    added_cost += line["line_cost"]
                    working[add_key] = working.get(add_key, 0) + qty

            cost_impact = round(added_cost - removed_cost, 2)

        total_cost_delta += cost_impact
        total_upcharge += upcharge
        modifier_profit = round(upcharge - cost_impact, 2) if upcharge > 0 else 0

        modifier_results.append({
            "modifier_key": mk,
            "display": mod.get("display", mk),
            "type": mod_type,
            "cost_impact": cost_impact,
            "upcharge": upcharge,
            "modifier_profit": modifier_profit,
        })

    final_cost = round(base["total_cost"] + total_cost_delta, 2)
    sell_price = round(base["sell_price"] + total_upcharge, 2)
    gross_profit = round(sell_price - final_cost, 2) if sell_price > 0 else 0
    margin_pct = round(gross_profit / sell_price * 100, 1) if sell_price > 0 else 0

    return {
        "recipe_key": recipe_key,
        "display_name": base["display_name"],
        "base_cost": base["total_cost"],
        "base_breakdown": base["cost_breakdown"],
        "modifiers": modifier_results,
        "final_cost": final_cost,
        "total_upcharge": total_upcharge,
        "total_modifier_profit": round(sum(m["modifier_profit"] for m in modifier_results), 2),
        "sell_price": sell_price,
        "gross_profit": gross_profit,
        "margin_pct": margin_pct,
    }


# ═══════════════════════════════════════════════════════════════════════════
# MARGIN CALCULATION (used by sales intelligence)
# ═══════════════════════════════════════════════════════════════════════════

def calculate_product_costs(
    recipes: dict | None = None,
    costs: dict | None = None,
    prices: dict | None = None,
) -> dict:
    """Calculate ingredient cost, price, margin, and margin % per product.

    This is the existing interface used by sales intelligence and the chat.
    Now uses the per-ingredient cost calculation internally.

    Returns:
        {product: {"cost": X, "price": Y, "margin": Z, "margin_pct": %, "grade": str}}
    """
    recipes = recipes or _load("recipes.json")
    prices_data = prices or _load("prices.json")

    result = {}
    for product, ingredients in recipes.items():
        # Calculate total cost using the new per-ingredient engine
        total_cost = 0
        for ing_key, quantity in ingredients.items():
            if quantity is None or quantity == 0:
                continue
            unit = _infer_unit_from_key(ing_key)
            line = calculate_recipe_line_cost(ing_key, float(quantity), unit)
            total_cost += line["line_cost"]

        total_cost = round(total_cost, 2)
        price = round(prices_data.get(product, 0), 2)

        if price <= 0:
            continue

        margin = round(price - total_cost, 2)
        margin_pct = round(margin / price * 100, 1)

        if margin_pct > 95 or margin_pct < 0:
            continue

        if margin_pct < 50:
            grade, action = "CRITICAL", "RAISE PRICE or REDUCE COST"
        elif margin_pct < 65:
            grade, action = "LOW", "REVIEW PRICING"
        elif margin_pct < 75:
            grade, action = "GOOD", "OK"
        else:
            grade, action = "STRONG", "OK"

        result[product] = {
            "cost": total_cost,
            "price": price,
            "margin": margin,
            "margin_pct": margin_pct,
            "grade": grade,
            "action": action,
        }

    return result
