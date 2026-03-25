"""
Recipe Builder Pipeline for Maillard Coffee Roasters.

Extracts sold items from Square, purchased ingredients from invoices,
generates draft recipe mappings, and provides a review/approve workflow.

Architecture:
  Square sales → sold items catalog
  Invoice DB   → purchased ingredients catalog
  Heuristics   → draft recipe mappings (editable, reviewable)
  Owner review → approved recipes → recipes.json + costs.json
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "data"
_SALES_DB = _DATA / "sales.db"
_INVOICES_DB = _DATA / "invoices.db"
_RECIPES_FILE = _DATA / "recipes.json"
_COSTS_FILE = _DATA / "costs.json"
_PRICES_FILE = _DATA / "prices.json"
_DRAFTS_FILE = _DATA / "recipe_drafts.json"


# ═══════════════════════════════════════════════════════════════════════════
# A. SOLD ITEMS CATALOG
# ═══════════════════════════════════════════════════════════════════════════

def extract_sold_items() -> list[dict]:
    """Extract all distinct products sold from Square sales data.

    Returns:
        [{"key": slug, "display": name, "category": str, "total_qty": float, "days_sold": int}]
    """
    if not _SALES_DB.exists():
        return []

    conn = sqlite3.connect(str(_SALES_DB))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT item_name, product_display, product_category,
               SUM(quantity_sold) as total_qty,
               COUNT(DISTINCT business_date) as days_sold
        FROM product_daily_sales
        GROUP BY item_name
        ORDER BY total_qty DESC
    """).fetchall()
    conn.close()

    return [
        {
            "key": r["item_name"],
            "display": r["product_display"],
            "category": r["product_category"],
            "total_qty": round(r["total_qty"], 1),
            "days_sold": r["days_sold"],
        }
        for r in rows
    ]


# ═══════════════════════════════════════════════════════════════════════════
# B. PURCHASED INGREDIENTS CATALOG
# ═══════════════════════════════════════════════════════════════════════════

def extract_purchased_ingredients() -> list[dict]:
    """Extract all distinct purchased items from invoice database.

    Returns:
        [{"key": normalized_key, "display": normalized_name, "vendors": int,
          "avg_price": float, "unit": str, "category": str}]
    """
    if not _INVOICES_DB.exists():
        return []

    conn = sqlite3.connect(str(_INVOICES_DB))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT
            ii.normalized_name,
            ii.category,
            ii.unit,
            COUNT(DISTINCT i.vendor) as vendors,
            ROUND(AVG(ii.unit_price), 2) as avg_price
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.unit_price > 0
        GROUP BY ii.normalized_name
        ORDER BY ii.normalized_name
    """).fetchall()
    conn.close()

    return [
        {
            "key": _normalize_ingredient_key(r["normalized_name"]),
            "display": r["normalized_name"],
            "vendors": r["vendors"],
            "avg_price": r["avg_price"],
            "unit": r["unit"] or "ea",
            "category": r["category"] or "other",
        }
        for r in rows
    ]


def _normalize_ingredient_key(name: str) -> str:
    """Turn invoice item name into a clean key."""
    clean = name.lower().strip()
    clean = re.sub(r"\b\d+\s*(oz|ml|g|kg|lb|ct|pk|cs)\b", "", clean)
    clean = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
    return clean or "unknown"


# ═══════════════════════════════════════════════════════════════════════════
# C. RECIPE DRAFT GENERATION
# ═══════════════════════════════════════════════════════════════════════════

# Heuristic rules: product patterns → likely ingredient categories
_INGREDIENT_RULES: list[tuple[str, list[dict]]] = [
    # Espresso-based drinks
    (r"espresso|americano|latte|cappuccino|flat.?white|mocha|macchiato|cortado|freddo|frape",
     [
         {"ingredient_key": "espresso_beans_kg", "quantity": None, "unit": "kg",
          "confidence": "high", "reason": "espresso-based drink"},
         {"ingredient_key": "cups_hot_or_cold", "quantity": None, "unit": "ea",
          "confidence": "medium", "reason": "serving vessel"},
     ]),

    # Milk-based drinks (subset of espresso)
    (r"latte|cappuccino|flat.?white|mocha|macchiato|cortado|freddo.?cap|frape",
     [
         {"ingredient_key": "whole_milk_liters", "quantity": None, "unit": "liters",
          "confidence": "high", "reason": "milk-based drink"},
     ]),

    # Cold drinks need cold cups + lids
    (r"freddo|iced|cold.?brew|frape|japanese.*iced",
     [
         {"ingredient_key": "cups_cold_plastic", "quantity": None, "unit": "ea",
          "confidence": "medium", "reason": "cold drink cup"},
         {"ingredient_key": "lids_cold", "quantity": None, "unit": "ea",
          "confidence": "medium", "reason": "cold drink lid"},
     ]),

    # Filter/drip coffee
    (r"filter|drip|pour.?over|batch|japanese.*coffee",
     [
         {"ingredient_key": "espresso_beans_kg", "quantity": None, "unit": "kg",
          "confidence": "high", "reason": "filter/drip coffee beans"},
     ]),

    # Matcha
    (r"matcha",
     [
         {"ingredient_key": "matcha_powder", "quantity": None, "unit": "g",
          "confidence": "high", "reason": "matcha drink"},
         {"ingredient_key": "whole_milk_liters", "quantity": None, "unit": "liters",
          "confidence": "medium", "reason": "usually served with milk"},
     ]),

    # Chocolate drinks
    (r"chocolate|mocha",
     [
         {"ingredient_key": "chocolate_sauce_or_powder", "quantity": None, "unit": "g",
          "confidence": "medium", "reason": "chocolate ingredient"},
     ]),

    # Coffee bags (retail)
    (r"coffee.?bag|bag.*coffee|retail.*bag",
     [
         {"ingredient_key": "roasted_coffee_kg", "quantity": None, "unit": "kg",
          "confidence": "high", "reason": "retail coffee bag"},
         {"ingredient_key": "bag_packaging", "quantity": None, "unit": "ea",
          "confidence": "high", "reason": "retail packaging"},
     ]),

    # Pastry / bakery items
    (r"croissant|danish|turnover|muffin|cookie|bougatsa|pie|peinirli|koulouri|nutella",
     [
         {"ingredient_key": "bakery_item_wholesale", "quantity": None, "unit": "ea",
          "confidence": "high", "reason": "purchased baked good (wholesale)"},
         {"ingredient_key": "bakery_bag_or_wrapper", "quantity": None, "unit": "ea",
          "confidence": "low", "reason": "serving wrapper"},
     ]),

    # Overnight oats / parfait
    (r"overnight.?oat|parfait",
     [
         {"ingredient_key": "oats", "quantity": None, "unit": "g",
          "confidence": "high", "reason": "oat base"},
         {"ingredient_key": "yogurt", "quantity": None, "unit": "ml",
          "confidence": "medium", "reason": "dairy component"},
         {"ingredient_key": "cup_container", "quantity": None, "unit": "ea",
          "confidence": "medium", "reason": "serving container"},
     ]),

    # Crepe
    (r"crepe",
     [
         {"ingredient_key": "crepe_batter_mix", "quantity": None, "unit": "g",
          "confidence": "high", "reason": "crepe base"},
     ]),

    # Juice
    (r"\boj\b|orange.?juice|juice",
     [
         {"ingredient_key": "orange_juice", "quantity": None, "unit": "ml",
          "confidence": "high", "reason": "juice product"},
     ]),

    # Tea / chai
    (r"\btea\b|chai|\bchai\b",
     [
         {"ingredient_key": "tea_bag_or_concentrate", "quantity": None, "unit": "ea",
          "confidence": "high", "reason": "tea base"},
         {"ingredient_key": "cups_hot_or_cold", "quantity": None, "unit": "ea",
          "confidence": "medium", "reason": "serving cup"},
     ]),

    # Hot chocolate
    (r"hot.?choco",
     [
         {"ingredient_key": "chocolate_sauce_or_powder", "quantity": None, "unit": "g",
          "confidence": "high", "reason": "chocolate base"},
         {"ingredient_key": "whole_milk_liters", "quantity": None, "unit": "liters",
          "confidence": "high", "reason": "milk for hot chocolate"},
         {"ingredient_key": "cups_hot_or_cold", "quantity": None, "unit": "ea",
          "confidence": "medium", "reason": "serving cup"},
     ]),

    # Water (bottled)
    (r"water|theoni",
     [
         {"ingredient_key": "bottled_water", "quantity": None, "unit": "ea",
          "confidence": "high", "reason": "bottled water (wholesale)"},
     ]),
]


# ── Category inference ──

_CATEGORY_PATTERNS: list[tuple[str, str]] = [
    (r"espresso|americano|latte|cappuccino|flat.?white|mocha|macchiato|cortado|freddo|frape", "espresso_drink"),
    (r"filter|drip|pour.?over|batch|japanese.*coffee", "brewed_coffee"),
    (r"\btea\b|chai", "tea"),
    (r"matcha", "tea"),
    (r"hot.?choco|iced.?choco", "chocolate_drink"),
    (r"coffee.?bag|bag.*coffee|retail.*bag|\d+\s*(oz|g|kg)\s*bag", "retail_bag"),
    (r"croissant|danish|turnover|muffin|cookie|bougatsa|pie|peinirli|koulouri|nutella|crepe|parfait|oat", "food"),
    (r"\boj\b|juice|water|theoni", "beverage_other"),
]


def infer_sales_item_category(item_name: str) -> str:
    """Classify a sales item into a recipe category."""
    check = item_name.lower()
    for pattern, category in _CATEGORY_PATTERNS:
        if re.search(pattern, check):
            return category
    return "unknown"


def generate_recipe_drafts() -> list[dict]:
    """Generate draft recipes for all sold items that don't have approved recipes.

    Returns list of draft recipe dicts. Saves to data/recipe_drafts.json.
    """
    sold = extract_sold_items()
    approved = _load_approved_recipes()
    existing_drafts = _load_drafts()

    # Index existing drafts by key
    draft_index = {d["recipe_key"]: d for d in existing_drafts}

    new_drafts = []
    for item in sold:
        key = item["key"]

        # Skip if already approved
        if key in approved:
            continue

        # Skip if draft already exists (don't overwrite edits)
        if key in draft_index:
            new_drafts.append(draft_index[key])
            continue

        # Generate new draft
        ingredients = _infer_ingredients(key, item["display"], item["category"])

        draft = {
            "recipe_key": key,
            "display_name": item["display"],
            "category": item["category"],
            "status": "draft",
            "ingredients": ingredients,
            "total_qty_sold": item["total_qty"],
            "days_sold": item["days_sold"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "notes": "",
        }
        new_drafts.append(draft)

    # Keep any existing drafts for items no longer sold
    for key, draft in draft_index.items():
        if key not in {d["recipe_key"] for d in new_drafts}:
            new_drafts.append(draft)

    _save_drafts(new_drafts)
    return new_drafts


def _infer_ingredients(key: str, display: str, category: str) -> list[dict]:
    """Apply heuristic rules to suggest ingredients for a product."""
    matched: dict[str, dict] = {}  # ingredient_key → best match
    check_text = f"{key} {display}".lower()

    for pattern, ingredients in _INGREDIENT_RULES:
        if re.search(pattern, check_text, re.IGNORECASE):
            for ing in ingredients:
                ik = ing["ingredient_key"]
                # Keep highest confidence match
                if ik not in matched or _conf_rank(ing["confidence"]) > _conf_rank(matched[ik]["confidence"]):
                    matched[ik] = {**ing, "needs_review": True}

    # If nothing matched, add a generic placeholder
    if not matched:
        matched["unknown_ingredient"] = {
            "ingredient_key": "unknown_ingredient",
            "quantity": None,
            "unit": "",
            "confidence": "low",
            "reason": "no heuristic match — needs manual recipe",
            "needs_review": True,
        }

    return list(matched.values())


def _conf_rank(c: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(c, 0)


# ═══════════════════════════════════════════════════════════════════════════
# D. REVIEW / UPDATE / APPROVE WORKFLOW
# ═══════════════════════════════════════════════════════════════════════════

_VALID_UNITS = {"kg", "g", "liters", "ml", "ea", "unit", "oz", "lb", "cup", "tbsp", "tsp", ""}


def get_recipe_drafts(status: str | None = None) -> list[dict]:
    """Get all drafts, optionally filtered by status."""
    drafts = _load_drafts()
    if status:
        drafts = [d for d in drafts if d.get("status") == status]
    return drafts


def get_recipe_draft(recipe_key: str) -> dict | None:
    """Get a single draft by key."""
    for d in _load_drafts():
        if d["recipe_key"] == recipe_key:
            return d
    return None


def get_incomplete_recipes() -> list[dict]:
    """Get drafts where any ingredient has needs_review=True or quantity=None."""
    drafts = _load_drafts()
    incomplete = []
    for d in drafts:
        if d.get("status") == "approved":
            continue
        for ing in d.get("ingredients", []):
            if ing.get("needs_review") or ing.get("quantity") is None:
                incomplete.append(d)
                break
    return incomplete


def validate_recipe_draft(recipe: dict) -> list[str]:
    """Validate a recipe draft. Returns list of error strings (empty = valid).

    Checks:
      - ingredient_key present and non-empty
      - quantity numeric and positive where set
      - unit is recognized
      - no duplicate ingredient keys
      - at least one ingredient
    """
    errors = []
    ings = recipe.get("ingredients", [])

    if not ings:
        errors.append("Recipe has no ingredients")
        return errors

    seen_keys = set()
    for i, ing in enumerate(ings):
        ik = ing.get("ingredient_key", "").strip()
        if not ik:
            errors.append(f"Ingredient {i+1}: missing ingredient_key")
            continue

        if ik in seen_keys:
            errors.append(f"Duplicate ingredient: '{ik}'")
        seen_keys.add(ik)

        qty = ing.get("quantity")
        if qty is not None:
            try:
                q = float(qty)
                if q < 0:
                    errors.append(f"'{ik}': quantity must be >= 0 (got {q})")
            except (ValueError, TypeError):
                errors.append(f"'{ik}': quantity must be a number (got {qty!r})")

        unit = ing.get("unit", "")
        if unit and unit not in _VALID_UNITS:
            errors.append(f"'{ik}': unrecognized unit '{unit}' (valid: {', '.join(sorted(_VALID_UNITS - {''}))})")

    return errors


def update_recipe_draft(recipe_key: str, updates: dict) -> dict | None:
    """Update a draft recipe. Returns updated draft or None if not found."""
    drafts = _load_drafts()
    for d in drafts:
        if d["recipe_key"] == recipe_key:
            if "ingredients" in updates:
                d["ingredients"] = updates["ingredients"]
            if "display_name" in updates:
                d["display_name"] = updates["display_name"]
            if "notes" in updates:
                d["notes"] = updates["notes"]
            d["updated_at"] = datetime.now(timezone.utc).isoformat()
            _save_drafts(drafts)
            return d
    return None


def approve_recipe(recipe_key: str) -> dict | None:
    """Approve a draft recipe and merge it into recipes.json.

    Runs full validation. Returns approved recipe or {"error": ...}.
    """
    drafts = _load_drafts()
    target = None
    for d in drafts:
        if d["recipe_key"] == recipe_key:
            target = d
            break

    if not target:
        return None

    # Full validation
    errors = validate_recipe_draft(target)
    if errors:
        return {"error": errors[0], "all_errors": errors}

    # Check all quantities set and reviewed
    for ing in target.get("ingredients", []):
        if ing.get("quantity") is None:
            return {"error": f"Ingredient '{ing['ingredient_key']}' has no quantity set"}
        if ing.get("needs_review"):
            return {"error": f"Ingredient '{ing['ingredient_key']}' still needs review"}

    # Build the recipe entry for recipes.json
    recipe_entry = {}
    for ing in target["ingredients"]:
        recipe_entry[ing["ingredient_key"]] = ing["quantity"]

    # Update recipes.json
    recipes = _load_json(_RECIPES_FILE)
    recipes[recipe_key] = recipe_entry
    _save_json(_RECIPES_FILE, recipes)

    # Update draft status
    target["status"] = "approved"
    target["approved_at"] = datetime.now(timezone.utc).isoformat()
    _save_drafts(drafts)

    return target


def reject_recipe_draft(recipe_key: str) -> dict | None:
    """Mark a draft as rejected. Does not delete — keeps for audit trail."""
    drafts = _load_drafts()
    for d in drafts:
        if d["recipe_key"] == recipe_key:
            d["status"] = "rejected"
            d["rejected_at"] = datetime.now(timezone.utc).isoformat()
            _save_drafts(drafts)
            return d
    return None


def get_recipe_status_summary() -> dict:
    """Overview of recipe coverage."""
    sold = extract_sold_items()
    approved = _load_approved_recipes()
    drafts = _load_drafts()

    sold_keys = {s["key"] for s in sold}
    approved_keys = set(approved.keys())
    draft_keys = {d["recipe_key"] for d in drafts if d.get("status") == "draft"}

    covered = sold_keys & approved_keys
    drafted = sold_keys & draft_keys - approved_keys
    missing = sold_keys - approved_keys - draft_keys

    return {
        "total_sold_products": len(sold_keys),
        "approved_recipes": len(covered),
        "draft_recipes": len(drafted),
        "no_recipe": len(missing),
        "coverage_pct": round(len(covered) / len(sold_keys) * 100, 1) if sold_keys else 0,
        "missing": sorted(missing),
        "drafted": sorted(drafted),
    }


# ═══════════════════════════════════════════════════════════════════════════
# E. COVERAGE ENFORCEMENT
# ═══════════════════════════════════════════════════════════════════════════

def find_unmapped_sales_items() -> list[dict]:
    """Return all sold items that have no approved recipe.

    Every sellable item must have a recipe record — beverages, food, retail, all.
    Items with only a draft or rejected recipe are included as unmapped.
    """
    sold = extract_sold_items()
    approved = _load_approved_recipes()
    approved_keys = set(approved.keys())

    unmapped = []
    for item in sold:
        if item["key"] not in approved_keys:
            # Check draft status
            drafts = _load_drafts()
            draft = next((d for d in drafts if d["recipe_key"] == item["key"]), None)
            unmapped.append({
                **item,
                "has_draft": draft is not None,
                "draft_status": draft["status"] if draft else None,
            })

    return unmapped


def enforce_recipe_coverage() -> dict:
    """Ensure every sold item has at least a draft recipe. Creates missing drafts.

    Returns:
        {"created": int, "already_drafted": int, "approved": int, "total_sold": int}
    """
    sold = extract_sold_items()
    approved = _load_approved_recipes()
    existing_drafts = _load_drafts()
    draft_index = {d["recipe_key"]: d for d in existing_drafts}

    created = 0
    already_drafted = 0
    approved_count = 0

    for item in sold:
        key = item["key"]
        if key in approved:
            approved_count += 1
            continue
        if key in draft_index:
            already_drafted += 1
            continue

        # Create draft for this unmapped item
        ingredients = _infer_ingredients(key, item["display"], item["category"])
        draft = {
            "recipe_key": key,
            "display_name": item["display"],
            "category": item["category"],
            "status": "draft",
            "ingredients": ingredients,
            "total_qty_sold": item["total_qty"],
            "days_sold": item["days_sold"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "notes": "auto-created by coverage enforcement",
        }
        existing_drafts.append(draft)
        created += 1

    if created > 0:
        _save_drafts(existing_drafts)

    return {
        "total_sold": len(sold),
        "approved": approved_count,
        "already_drafted": already_drafted,
        "created": created,
        "coverage_pct": round(approved_count / len(sold) * 100, 1) if sold else 0,
    }


# ═══════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _load_approved_recipes() -> dict:
    return _load_json(_RECIPES_FILE)


def _load_drafts() -> list[dict]:
    if _DRAFTS_FILE.exists():
        try:
            return json.loads(_DRAFTS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_drafts(drafts: list[dict]) -> None:
    _DRAFTS_FILE.write_text(json.dumps(drafts, indent=2), encoding="utf-8")


def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
