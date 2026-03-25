"""
Maillard Invoice Intake & Vendor Cost Extraction — V2

Interpretation + normalization layer. Sits between raw extraction
(invoice_reader.py) and storage/query.

Pipeline:  Vision extract → interpret → normalize → score → store

No ML training, no overengineering. Practical rule-based interpretation
with confidence scoring and vendor-aware parsing.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any

# ── Paths ────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
STORAGE_PATH = os.path.join(DATA_DIR, "invoices_normalized.json")


# ── Unit Normalization ───────────────────────────────────────────

UNIT_ALIASES: dict[str, str] = {
    # Weight
    "lb": "lb", "lbs": "lb", "pound": "lb", "pounds": "lb", "#": "lb",
    "kg": "kg", "kgs": "kg", "kilo": "kg", "kilos": "kg", "kilogram": "kg", "kilograms": "kg",
    "oz": "oz", "ounce": "oz", "ounces": "oz",
    "g": "g", "gram": "g", "grams": "g", "gr": "g",
    # Volume
    "gal": "gal", "gallon": "gal", "gallons": "gal",
    "l": "L", "liter": "L", "liters": "L", "litre": "L", "litres": "L",
    "ml": "mL", "milliliter": "mL", "milliliters": "mL",
    "qt": "qt", "quart": "qt", "quarts": "qt",
    # Container
    "cs": "case", "case": "case", "cases": "case", "cse": "case",
    "pk": "pack", "pack": "pack", "packs": "pack",
    "bx": "box", "box": "box", "boxes": "box",
    "bg": "bag", "bag": "bag", "bags": "bag",
    "bt": "bottle", "bottle": "bottle", "bottles": "bottle", "btl": "bottle",
    "cn": "can", "can": "can", "cans": "can",
    "tub": "tub", "tubs": "tub",
    "jar": "jar", "jars": "jar",
    # Count
    "ea": "ea", "each": "ea", "unit": "ea", "units": "ea", "pc": "ea", "pcs": "ea",
    "ct": "ct", "count": "ct",
    # Batch
    "batch": "batch", "lot": "batch",
}

WEIGHT_UNITS = {"lb", "kg", "oz", "g"}
VOLUME_UNITS = {"gal", "L", "mL", "qt"}
CONTAINER_UNITS = {"case", "pack", "box", "bag", "bottle", "can", "tub", "jar"}
COUNT_UNITS = {"ea", "ct"}


def normalize_unit(raw_unit: str | None) -> str:
    """Normalize a unit string to a canonical form."""
    if not raw_unit:
        return "ea"
    cleaned = raw_unit.strip().lower().rstrip(".")
    # Handle compound like "1/2 gallon"
    cleaned = re.sub(r"^1/2\s*", "half ", cleaned)
    return UNIT_ALIASES.get(cleaned, cleaned)


# ── Price Basis Detection ────────────────────────────────────────

PRICE_BASIS_PATTERNS = [
    # Explicit per-weight markers
    (r"per\s*(lb|pound|kg|kilo|oz|gram|g)\b", "per_weight"),
    (r"/\s*(lb|kg|oz|g)\b", "per_weight"),
    (r"\$[\d.]+\s*/\s*(lb|kg|oz|g)\b", "per_weight"),
    # Per-case / per-pack
    (r"per\s*(case|cs|pack|pk|box|bx|crate)\b", "per_case"),
    (r"/\s*(case|cs|pack|pk|box)\b", "per_case"),
    (r"case\s*(of|x)\s*\d+", "per_case"),
    (r"\d+\s*x\s*\d+", "per_case"),
    # Per-batch
    (r"per\s*batch\b", "per_batch"),
    (r"batch\s*price", "per_batch"),
    (r"flat\s*(rate|fee|charge)", "per_batch"),
    # Per-unit (explicit)
    (r"per\s*(ea|each|unit|piece|pc)\b", "per_unit"),
    (r"/\s*(ea|each|unit)\b", "per_unit"),
]


def detect_price_basis(row_text: str, unit: str = "", vendor: str = "") -> str:
    """
    Detect how the price is expressed for an invoice line.

    Returns: per_unit, per_weight, per_case, per_batch, or unknown.
    """
    lower = row_text.lower()

    # Check explicit patterns in the text
    for pattern, basis in PRICE_BASIS_PATTERNS:
        if re.search(pattern, lower):
            return basis

    # Infer from normalized unit
    norm = normalize_unit(unit)
    if norm in WEIGHT_UNITS:
        return "per_weight"
    if norm in CONTAINER_UNITS:
        return "per_case"
    if norm in VOLUME_UNITS:
        return "per_unit"  # volume items are typically priced per unit (per bottle, per gallon)
    if norm in COUNT_UNITS:
        return "per_unit"

    # Vendor-specific defaults
    vendor_rules = _get_vendor_rules(vendor)
    if vendor_rules and "default_price_basis" in vendor_rules:
        return vendor_rules["default_price_basis"]

    return "unknown"


# ── Vendor-Aware Parsing ─────────────────────────────────────────

VENDOR_RULES: dict[str, dict] = {
    "the dairy wagon": {
        "default_price_basis": "per_unit",
        "known_items": {
            "battenkill whole milk": {"unit": "gal", "category": "milk_dairy"},
            "battenkill half & half": {"unit": "gal", "category": "milk_dairy"},
            "oatly": {"unit": "case", "category": "milk_dairy", "price_basis": "per_case"},
            "greek yogurt": {"unit": "ea", "category": "milk_dairy"},
        },
        "unit_hint": "gal",
    },
    "wheatfield": {
        "default_price_basis": "per_unit",
        "known_items": {},
        "category_override": "food",
    },
    "impact food": {
        "default_price_basis": "per_case",
        "known_items": {},
    },
    "sysco": {
        "default_price_basis": "per_case",
        "known_items": {},
    },
    "pure produce": {
        "default_price_basis": "per_unit",
        "known_items": {},
        "category_override": "grocery",
    },
    "optima": {
        "default_price_basis": "per_unit",
        "known_items": {},
        "category_override": "grocery",
    },
    "loumidis": {
        "default_price_basis": "per_case",
        "known_items": {},
        "category_override": "grocery",
    },
    "rite-a-way": {
        "default_price_basis": "per_unit",
        "known_items": {},
        "category_override": "paper_goods",
    },
    "redway": {
        "default_price_basis": "per_unit",
        "known_items": {},
    },
    "odeko": {
        "default_price_basis": "per_unit",
        "known_items": {},
    },
}


def _get_vendor_rules(vendor: str) -> dict | None:
    """Find vendor rules by partial name match."""
    if not vendor:
        return None
    lower = vendor.lower()
    for key, rules in VENDOR_RULES.items():
        if key in lower:
            return rules
    return None


def _vendor_item_override(vendor: str, item_name: str) -> dict | None:
    """Check if a vendor has known item rules."""
    rules = _get_vendor_rules(vendor)
    if not rules:
        return None
    lower = item_name.lower()
    for pattern, overrides in rules.get("known_items", {}).items():
        if pattern in lower:
            return overrides
    return None


# ── Category Detection ───────────────────────────────────────────

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "green_coffee": ["green coffee", "green bean", "unroasted", "raw coffee", "grainpro", "jute",
                     "60kg", "69kg", "70kg", "yirgacheffe", "huila", "supremo", "santos",
                     "antigua", "gesha", "sidamo", "guji", "arabica green", "robusta green"],
    "roasted_coffee": ["roasted coffee", "roasted bean", "espresso blend", "house blend"],
    "milk_dairy": ["milk", "cream", "half and half", "half & half", "oat milk", "almond milk",
                   "soy milk", "coconut milk", "whip", "yogurt", "butter", "cheese"],
    "sweetener": ["sugar", "syrup", "honey", "agave", "sweetener", "vanilla syrup",
                  "caramel syrup", "simple syrup"],
    "cups_lids": ["cup", "lid", "sleeve", "straw", "napkin", "stirrer"],
    "paper_goods": ["paper towel", "toilet paper", "tissue", "trash bag", "garbage bag", "foil",
                    "plastic wrap", "cling wrap", "parchment", "wax paper", "paper plate",
                    "takeout container", "to-go container", "deli paper", "receipt roll",
                    "register tape"],
    "packaging": ["label", "sticker", "valve bag", "kraft bag", "retail bag", "shipping box"],
    "tea": ["tea", "matcha", "chai"],
    "chocolate": ["chocolate", "cocoa", "cacao"],
    "grocery": ["flour", "baking", "oil", "olive oil", "vinegar", "salt", "pepper", "spice",
                "cinnamon", "nutmeg", "vanilla extract", "lemon", "fruit", "juice", "water",
                "sparkling", "soda", "ice", "egg", "produce"],
    "cleaning": ["cleaner", "sanitizer", "detergent", "descaler", "tablet", "cleaning",
                 "soap", "dish soap", "bleach", "sponge", "glove"],
    "equipment": ["filter", "portafilter", "gasket", "screen", "group head", "burr"],
    "food": ["pastry", "cookie", "muffin", "croissant", "bread", "sandwich", "crepe",
             "bagel", "cake", "scone", "wrap", "panini", "danish", "donut", "roll",
             "pie", "tart", "biscuit"],
    "other": [],
}


def _detect_category(name: str, vendor: str = "") -> str:
    """Detect category from item name using keyword matching + vendor context."""
    # Vendor category override
    rules = _get_vendor_rules(vendor)
    if rules and "category_override" in rules:
        # Only use as fallback, not override if keywords match
        pass

    # Item-specific vendor override
    override = _vendor_item_override(vendor, name)
    if override and "category" in override:
        return override["category"]

    lower = name.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if category == "other":
            continue
        for kw in keywords:
            if kw in lower:
                return category

    # Vendor fallback
    if rules and "category_override" in rules:
        return rules["category_override"]

    return "other"


# ── Name Cleaning ────────────────────────────────────────────────


def _clean_name(raw: str) -> str:
    """Clean up a raw item name for normalization."""
    name = raw.strip()
    # Remove leading line-item numbers like "1.", "2)", "001 -"
    # BUT preserve size prefixes like "16 oz", "24 oz" — only strip if followed by
    # a punctuation separator (dot, paren, dash) not a unit word.
    name = re.sub(r"^\d+[\.\)][\s]*", "", name).strip()  # "1." or "2)"
    name = re.sub(r"^\d+\s*-\s+(?!\d)", "", name).strip()  # "001 - " but not "16 - 24oz"
    # Remove "(unclear)" markers from Vision extraction
    name = re.sub(r"\(unclear\)", "", name).strip()
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name)
    return name


def _parse_pack_size(raw: str) -> dict | None:
    """Try to extract pack/weight info from item name."""
    return parse_pack_size(raw)


def parse_pack_size(raw: str) -> dict | None:
    """Extract pack size from patterns like '12x8oz', '1/1000', '4/1 gal', '6 x 5 lb', 'dozen'.

    Returns:
        {"pack_count": int, "per_unit_size": float, "per_unit_unit": str}
        or None if no pattern matched.
    """
    # Pattern: N/N unit  (e.g., 4/1 gal, 6/5 lb) — only recognized units
    _KNOWN_UNITS_RE = r"(?:lb|lbs|kg|oz|gal|gallon|gallons|L|liter|liters|ml|qt|quart)\b"
    m = re.search(r"(\d+)\s*/\s*(\d+\.?\d*)\s*(" + _KNOWN_UNITS_RE + r")", raw, re.IGNORECASE)
    if m:
        return {"pack_count": int(m.group(1)), "per_unit_size": float(m.group(2)),
                "per_unit_unit": normalize_unit(m.group(3))}

    # Pattern: NxN unit  (e.g., 12x8oz, 6x1L)
    m = re.search(r"(\d+)\s*[xX]\s*(\d+\.?\d*)\s*([a-zA-Z]+)", raw)
    if m:
        return {"pack_count": int(m.group(1)), "per_unit_size": float(m.group(2)),
                "per_unit_unit": normalize_unit(m.group(3))}

    # Pattern: N/count (no unit, e.g., "1/1000" = 1 case of 1000)
    m = re.search(r"(\d+)\s*/\s*([\d,]+)(?:\s|$|-)", raw)
    if m:
        count = int(m.group(2).replace(",", ""))
        if count >= 10:
            return {"pack_count": count, "per_unit_size": 1, "per_unit_unit": "ea"}

    # Pattern: "N ct" or "N count" (e.g., "42 ct", "500ct")
    m = re.search(r"(\d+)\s*(?:ct|count)\b", raw, re.IGNORECASE)
    if m:
        count = int(m.group(1))
        if count >= 2:
            return {"pack_count": count, "per_unit_size": 1, "per_unit_unit": "ea"}

    # Pattern: "dozen" or "dz"
    if re.search(r"\bdozen\b|\bdz\b", raw, re.IGNORECASE):
        return {"pack_count": 12, "per_unit_size": 1, "per_unit_unit": "ea"}

    # Pattern: N unit/N (e.g., "4.5 Oz/42" = 42 items at 4.5oz each)
    m = re.search(
        r"(\d+\.?\d*)\s*(lb|lbs|kg|kgs|oz|gal|gallon|gallons|L|liter|liters|ml|qt|quart)\s*/\s*(\d+)",
        raw, re.IGNORECASE,
    )
    if m:
        return {"pack_count": int(m.group(3)), "per_unit_size": float(m.group(1)),
                "per_unit_unit": normalize_unit(m.group(2))}

    # Pattern: N unit (e.g., 5lb, 2.5kg, 32oz, 4.5 Oz)
    m = re.search(
        r"(\d+\.?\d*)\s*(lb|lbs|kg|kgs|oz|gal|gallon|gallons|L|liter|liters|ml|qt|quart)\b",
        raw, re.IGNORECASE,
    )
    if m:
        return {"pack_count": 1, "per_unit_size": float(m.group(1)),
                "per_unit_unit": normalize_unit(m.group(2))}

    return None


# ── Base unit normalization + conversion ──

_BASE_UNIT_MAP = {
    # Weight -> grams
    "lb": ("g", 453.592), "kg": ("g", 1000), "oz": ("g", 28.3495), "g": ("g", 1),
    # Volume -> ml
    "gal": ("ml", 3785.41), "L": ("ml", 1000), "mL": ("ml", 1), "qt": ("ml", 946.353),
    # Count -> units
    "ea": ("unit", 1), "ct": ("unit", 1), "case": ("unit", 1), "box": ("unit", 1),
    "pack": ("unit", 1), "bag": ("unit", 1), "bottle": ("unit", 1),
}


def normalize_base_unit(unit_text: str) -> str:
    """Convert any unit to its base dimension: gram, ml, or unit."""
    norm = normalize_unit(unit_text)
    base, _ = _BASE_UNIT_MAP.get(norm, ("unit", 1))
    return base


def convert_to_base_units(quantity: float, purchase_unit: str, pack_count: int = 1,
                          per_unit_size: float = 1, per_unit_unit: str = "") -> dict:
    """Convert a purchase quantity to total base units.

    Args:
        quantity: number of containers purchased (e.g., 2 cases)
        purchase_unit: the container unit (e.g., "case")
        pack_count: items per container (e.g., 1000 cups per case)
        per_unit_size: size of each item in per_unit_unit (e.g., 16 oz per cup)
        per_unit_unit: unit of each item inside the pack (e.g., "oz")

    Returns:
        {"total_base_units": float, "base_unit": str, "confidence": str}
    """
    # If per_unit_unit has a base conversion, use it
    if per_unit_unit:
        norm = normalize_unit(per_unit_unit)
        base, factor = _BASE_UNIT_MAP.get(norm, ("unit", 1))
        total = quantity * pack_count * per_unit_size * factor
        return {"total_base_units": round(total, 2), "base_unit": base, "confidence": "high"}

    # Container with count only (e.g., 2 cases of 1000 cups)
    if pack_count > 1:
        total = quantity * pack_count
        return {"total_base_units": round(total, 2), "base_unit": "unit", "confidence": "high"}

    # Weight/volume purchase unit
    norm = normalize_unit(purchase_unit)
    base, factor = _BASE_UNIT_MAP.get(norm, ("unit", 1))
    total = quantity * per_unit_size * factor
    return {"total_base_units": round(total, 2), "base_unit": base,
            "confidence": "high" if base != "unit" else "medium"}


def calculate_derived_single_unit_cost(line_total: float, total_base_units: float) -> dict | None:
    """Calculate cost per single base unit from line total and total base units.

    Returns:
        {"derived_unit_cost": float, "base_unit": str} or None
    """
    if not line_total or line_total <= 0 or not total_base_units or total_base_units <= 0:
        return None
    return {"derived_unit_cost": round(line_total / total_base_units, 5)}


# ── Confidence Scoring ───────────────────────────────────────────


def score_invoice_line_confidence(
    raw_name: str,
    final_quantity: float | None,
    final_unit_price: float | None,
    line_total: float | None,
    stated_total_matches: bool = True,
    has_handwriting: bool = False,
    handwriting_unclear: bool = False,
) -> tuple[str, list[str]]:
    """
    Score confidence for a single invoice line.

    Returns: (confidence: "high"|"medium"|"low", reasons: [str])
    """
    issues: list[str] = []

    # Handwriting detection
    if handwriting_unclear:
        issues.append("handwriting_unclear")
    elif has_handwriting:
        issues.append("handwriting_override")  # not unclear, but still a manual change

    # Check for markers in name
    if "(unclear)" in raw_name.lower():
        if "handwriting_unclear" not in issues:
            issues.append("handwriting_unclear")
    if "?" in raw_name:
        issues.append("questionable_field")

    # Check numeric sanity
    if final_quantity is None or final_quantity <= 0:
        issues.append("missing_or_zero_quantity")
    if final_unit_price is None or final_unit_price <= 0:
        issues.append("missing_or_zero_price")

    # Check line total reconciliation
    if final_quantity and final_unit_price and line_total:
        expected = round(final_quantity * final_unit_price, 2)
        if abs(expected - line_total) > 0.05:
            issues.append(f"line_total_mismatch: expected={expected} got={line_total}")

    # Suspicious prices
    if final_unit_price is not None:
        if final_unit_price > 5000:
            issues.append("suspiciously_high_price")
        if 0 < final_unit_price < 0.01:
            issues.append("suspiciously_low_price")

    # Very short names
    cleaned = _clean_name(raw_name)
    if len(cleaned) < 3:
        issues.append("name_too_short")

    # Invoice-level reconciliation
    if not stated_total_matches:
        issues.append("invoice_total_mismatch")

    # Score
    if not issues:
        return "high", []
    # Unclear handwriting → always low
    if "handwriting_unclear" in issues:
        return "low", issues
    # Clean handwriting override with no other issues → medium (human made a correction, probably right)
    if issues == ["handwriting_override"]:
        return "medium", issues
    if len(issues) == 1 and issues[0] == "invoice_total_mismatch":
        return "medium", issues
    if any(i in ("questionable_field", "missing_or_zero_price",
                 "missing_or_zero_quantity") for i in issues):
        return "low", issues
    if any("mismatch" in i for i in issues):
        return "medium", issues
    if len(issues) >= 2:
        return "low", issues
    return "medium", issues


def reconcile_line_total(
    quantity: float | None,
    unit_price: float | None,
    stated_line_total: float | None,
) -> dict:
    """
    Reconcile a line total against qty * price.

    Returns:
        {
            "line_total": float,
            "method": "computed" | "stated" | "zero",
            "discrepancy": float | None
        }
    """
    qty = quantity or 0
    price = unit_price or 0
    computed = round(qty * price, 2)

    if stated_line_total is not None and stated_line_total > 0:
        disc = round(abs(stated_line_total - computed), 2) if computed > 0 else None
        # Trust stated if close or if computed is zero
        if computed == 0 or (disc is not None and disc < 0.05):
            return {"line_total": stated_line_total, "method": "stated", "discrepancy": None}
        return {"line_total": stated_line_total, "method": "stated", "discrepancy": disc}

    if computed > 0:
        return {"line_total": computed, "method": "computed", "discrepancy": None}

    return {"line_total": 0.0, "method": "zero", "discrepancy": None}


# ── Derived Unit Cost ────────────────────────────────────────────


def _extract_count_from_name(raw_name: str) -> int | None:
    """Extract item count from name like '16 oz Clear Plastic Cup - 1,000' or 'Napkins 500ct'."""
    # Pattern: "- 1,000" or "- 600" or "- 200" at end
    m = re.search(r"-\s*([\d,]+)\s*$", raw_name)
    if m:
        return int(m.group(1).replace(",", ""))
    # Pattern: "1,000 ct" or "500ct" or "(1000)"
    m = re.search(r"([\d,]+)\s*ct\b", raw_name, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", ""))
    m = re.search(r"\(([\d,]+)\)", raw_name)
    if m:
        val = int(m.group(1).replace(",", ""))
        if val >= 10:  # likely a count, not a size
            return val
    return None


def _friendly_price(value: float) -> str:
    """Format a price in a human-friendly way with cent notation for small values."""
    if value >= 1.0:
        return f"${value:.2f}"
    cents = value * 100
    if cents >= 1.0:
        return f"${value:.4f} (~{cents:.1f}¢)"
    return f"${value:.5f} (~{cents:.2f}¢)"


def calculate_derived_unit_cost(item: dict) -> dict | None:
    """
    Calculate the most useful per-unit cost from invoice line data.

    Input: a dict with keys from interpret_invoice_line or DB row:
        raw_name, unit_price, unit, quantity, pack_size (or pack_size_json), line_total

    Returns:
        {
            "base_price": "$44.95 per box",
            "derived_unit_cost": 0.04495,
            "derived_unit_cost_display": "$0.04495 each (~4.5¢)",
            "derived_unit_label": "each",
            "items_per_container": 1000,
            "confidence": "high" | "medium"
        }
        or None if derivation not possible.
    """
    import json as _json

    raw_name = item.get("raw_name", "")
    unit_price = item.get("unit_price", 0)
    unit = item.get("unit", "ea")
    quantity = item.get("quantity", 1)
    line_total = item.get("line_total", 0)

    # Parse pack_size from JSON string if needed
    pack_size = item.get("pack_size")
    if pack_size is None and item.get("pack_size_json"):
        try:
            pack_size = _json.loads(item["pack_size_json"])
        except (ValueError, TypeError):
            pass

    if not unit_price or unit_price <= 0:
        return None

    base_label = f"${unit_price:.2f} per {unit}"
    confidence = "high"

    # ── Case 1: Box/case with count in name (e.g. "Cup - 1,000" at $44.95/box) ──
    count = _extract_count_from_name(raw_name)
    if count and count > 1 and unit in ("box", "case", "pack", "bag"):
        per_item = round(unit_price / count, 5)
        return {
            "base_price": base_label,
            "derived_unit_cost": per_item,
            "derived_unit_cost_display": _friendly_price(per_item) + " each",
            "derived_unit_label": "each",
            "items_per_container": count,
            "confidence": "high",
        }

    # ── Case 2: Pack size with count (e.g. pack_count=12, per_unit_size=32, per_unit_unit=oz) ──
    if pack_size and isinstance(pack_size, dict):
        pc = pack_size.get("pack_count", 1)
        pus = pack_size.get("per_unit_size", 0)
        puu = pack_size.get("per_unit_unit", "")

        # Multi-pack: 12x32oz at $38.50/case → per item
        if pc > 1 and unit in ("case", "box", "pack"):
            per_item = round(unit_price / pc, 4)
            label = f"{pus}{puu}" if pus and puu else "item"
            return {
                "base_price": base_label,
                "derived_unit_cost": per_item,
                "derived_unit_cost_display": _friendly_price(per_item) + f" per {label}",
                "derived_unit_label": f"per {label}",
                "items_per_container": pc,
                "confidence": "high",
            }

        # Single weight/volume item: 5lb at $38.50 → per lb
        if pc == 1 and pus > 0 and puu in ("lb", "kg", "oz", "g", "gal", "L", "mL"):
            per_unit_wt = round(unit_price / pus, 4)
            return {
                "base_price": base_label,
                "derived_unit_cost": per_unit_wt,
                "derived_unit_cost_display": _friendly_price(per_unit_wt) + f" per {puu}",
                "derived_unit_label": f"per {puu}",
                "items_per_container": None,
                "confidence": "high",
            }

    # ── Case 3: Weight-based unit (unit=lb/kg/gal) with quantity ──
    # e.g. 7 gallons at $4.95/gal → already per unit, but line_total / qty gives per-unit
    if unit in ("lb", "kg", "oz", "gal", "L"):
        # Already priced per weight/volume unit — this IS the derived cost
        return {
            "base_price": base_label,
            "derived_unit_cost": unit_price,
            "derived_unit_cost_display": _friendly_price(unit_price) + f" per {unit}",
            "derived_unit_label": f"per {unit}",
            "items_per_container": None,
            "confidence": "high",
        }

    # ── Case 4: Line total with quantity > 1 and unit=ea ──
    if unit == "ea" and quantity > 1 and line_total and line_total > 0:
        per_item = round(line_total / quantity, 4)
        return {
            "base_price": f"${line_total:.2f} for {int(quantity)}",
            "derived_unit_cost": per_item,
            "derived_unit_cost_display": _friendly_price(per_item) + " each",
            "derived_unit_label": "each",
            "items_per_container": int(quantity),
            "confidence": "medium",
        }

    return None


# ── Full Line Interpretation ─────────────────────────────────────


def _resolve_handwriting(printed_val: float | None, handwritten_val: float | None) -> tuple[float | None, str]:
    """
    Resolve final value from printed vs handwritten.
    Returns (final_value, override_source).
    override_source: "printed" | "handwritten" | "none"
    """
    if handwritten_val is not None:
        return handwritten_val, "handwritten"
    if printed_val is not None:
        return printed_val, "printed"
    return None, "none"


def interpret_invoice_line(item: dict, vendor: str = "") -> dict:
    """
    Take a raw extracted line item and produce a fully interpreted row.
    Handles handwritten corrections: preserves printed and handwritten values
    separately, computes final values using handwritten when detected.

    Input: raw dict from Vision extraction or JSON.
    Output: normalized, scored, interpreted line with handwriting tracking.
    """
    raw_name = item.get("name") or item.get("description") or item.get("item") or ""

    # ── Extract printed vs handwritten values ──
    # New Vision format with separate fields
    printed_qty = _safe_float(item.get("printed_quantity"), None)
    hw_qty = _safe_float(item.get("handwritten_quantity"), None)
    printed_price = _safe_float(item.get("printed_unit_price"), None)
    hw_price = _safe_float(item.get("handwritten_unit_price"), None)
    has_handwriting = bool(item.get("has_handwriting", False))
    handwriting_note = item.get("handwriting_note") or None

    # Backward compat: old format with flat quantity/unit_price
    if printed_qty is None and hw_qty is None:
        printed_qty = _safe_float(item.get("quantity") or item.get("qty"), None)
    if printed_price is None and hw_price is None:
        printed_price = _safe_float(item.get("unit_price") or item.get("price"), None)

    raw_unit = item.get("unit") or item.get("uom") or ""
    raw_total = _safe_float(item.get("line_total") or item.get("total") or item.get("amount"), None)

    # Resolve final values (handwritten overrides printed)
    final_qty, qty_source = _resolve_handwriting(printed_qty, hw_qty)
    final_price, price_source = _resolve_handwriting(printed_price, hw_price)

    # Determine overall override source
    if hw_qty is not None or hw_price is not None:
        override_source = "handwritten"
    else:
        override_source = "printed"

    # Detect if handwriting is unclear
    handwriting_unclear = False
    if handwriting_note and "unclear" in handwriting_note.lower():
        handwriting_unclear = True
    if "(unclear)" in raw_name.lower() and has_handwriting:
        handwriting_unclear = True

    # Clean and normalize name
    cleaned = _clean_name(raw_name)
    category = _detect_category(cleaned, vendor)
    pack_size = _parse_pack_size(cleaned)

    # Apply vendor item overrides
    item_override = _vendor_item_override(vendor, cleaned)
    if item_override:
        if "unit" in item_override and not raw_unit:
            raw_unit = item_override["unit"]
        if "category" in item_override:
            category = item_override["category"]

    unit = normalize_unit(raw_unit)

    # Default final values
    if final_qty is None or final_qty <= 0:
        final_qty = 1.0
    if final_price is None:
        final_price = 0.0

    # Detect price basis
    row_text = f"{raw_name} {raw_unit}"
    price_basis = detect_price_basis(row_text, raw_unit, vendor)
    if item_override and "price_basis" in item_override:
        price_basis = item_override["price_basis"]

    # Reconcile line total using final values
    recon = reconcile_line_total(final_qty, final_price, raw_total)

    # Build normalized display name
    normalized = cleaned.lower()
    normalized = re.sub(r"\d+\s*[x/]\s*\d+\.?\d*\s*[a-zA-Z]+", "", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = normalized.title()

    # Confidence scoring
    confidence, issues = score_invoice_line_confidence(
        raw_name, final_qty, final_price, raw_total,
        stated_total_matches=True,  # set at invoice level
        has_handwriting=has_handwriting,
        handwriting_unclear=handwriting_unclear,
    )

    # Review required if: low confidence, unclear handwriting, or uncategorized
    review_required = (
        confidence == "low"
        or handwriting_unclear
        or category == "other"
    )

    return {
        "raw_name": raw_name,
        "normalized_name": normalized,
        "category": category,
        # Printed values (as machine-printed on invoice)
        "printed_quantity": printed_qty,
        "printed_unit_price": printed_price,
        # Handwritten corrections (pen/marker overrides)
        "handwritten_quantity": hw_qty,
        "handwritten_unit_price": hw_price,
        "has_handwriting": has_handwriting,
        "handwriting_note": handwriting_note,
        # Final resolved values
        "quantity": final_qty,
        "unit": unit,
        "unit_price": final_price,
        "override_source": override_source,
        "pack_size": pack_size,
        "price_basis": price_basis,
        "line_total": recon["line_total"],
        "line_total_method": recon["method"],
        "line_total_discrepancy": recon["discrepancy"],
        # Confidence
        "confidence": confidence,
        "confidence_issues": issues,
        "review_required": review_required,
    }


# ── Invoice Extraction (full pipeline) ───────────────────────────


def extract_invoice_data(source: str | dict) -> dict:
    """
    Extract + interpret structured invoice data from a file path (JSON) or a dict.

    Returns:
        {
            "vendor": "...",
            "invoice_date": "...",
            "invoice_number": "...",
            "line_items": [...],
            "invoice_total": float,
            "extracted_at": "...",
            "confidence_summary": {...},
            "warnings": [...]
        }
    """
    warnings: list[str] = []

    # Load data
    if isinstance(source, dict):
        raw = source
    elif isinstance(source, str):
        try:
            with open(source, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except FileNotFoundError:
            return {"error": f"File not found: {source}", "warnings": []}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON: {e}", "warnings": []}
    else:
        return {"error": f"Unsupported source type: {type(source)}", "warnings": []}

    # Extract top-level fields
    vendor = raw.get("vendor") or raw.get("vendor_name") or raw.get("supplier") or "Unknown"
    invoice_date = raw.get("invoice_date") or raw.get("date") or None
    invoice_number = raw.get("invoice_number") or raw.get("invoice_no") or raw.get("number") or None
    source_file = raw.get("_source_file") or None
    notes = raw.get("notes") or None

    # Extract and interpret line items
    raw_items = raw.get("line_items") or raw.get("items") or raw.get("lines") or []
    line_items = []
    computed_total = 0.0

    for i, item in enumerate(raw_items):
        try:
            interpreted = interpret_invoice_line(item, vendor)
            line_items.append(interpreted)
            computed_total += interpreted["line_total"]
        except Exception as e:
            warnings.append(f"Line {i+1}: failed to interpret — {e}")

    # Invoice total reconciliation
    stated_total = _safe_float(raw.get("total") or raw.get("invoice_total"), None)
    computed_total = round(computed_total, 2)
    total_matches = True

    if stated_total is not None and computed_total > 0:
        if abs(stated_total - computed_total) > 0.50:
            total_matches = False
            warnings.append(
                f"Total mismatch: stated ${stated_total:.2f} vs computed ${computed_total:.2f}"
            )
            # Re-score lines with invoice-level mismatch flag
            for item in line_items:
                conf, issues = score_invoice_line_confidence(
                    item["raw_name"], item["quantity"], item["unit_price"],
                    item["line_total"], stated_total_matches=False,
                )
                item["confidence"] = conf
                item["confidence_issues"] = issues
                item["review_required"] = conf == "low" or item["category"] == "other"

    invoice_total = stated_total if stated_total is not None else computed_total

    # Confidence summary
    high = sum(1 for it in line_items if it["confidence"] == "high")
    med = sum(1 for it in line_items if it["confidence"] == "medium")
    low = sum(1 for it in line_items if it["confidence"] == "low")
    review = sum(1 for it in line_items if it["review_required"])

    return {
        "vendor": vendor,
        "invoice_date": invoice_date,
        "invoice_number": invoice_number,
        "source_file": source_file,
        "notes": notes,
        "line_items": line_items,
        "invoice_total": invoice_total,
        "computed_total": computed_total,
        "total_matches": total_matches,
        "confidence_summary": {
            "high": high,
            "medium": med,
            "low": low,
            "review_required": review,
            "total_lines": len(line_items),
        },
        "extracted_at": datetime.now().isoformat(),
        "warnings": warnings,
    }


def _safe_float(val: Any, default: float | None) -> float | None:
    """Safely convert to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ── Storage ──────────────────────────────────────────────────────


def _load_storage() -> list[dict]:
    """Load normalized invoices from disk."""
    try:
        with open(STORAGE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_storage(data: list[dict]) -> None:
    """Save normalized invoices to disk."""
    os.makedirs(os.path.dirname(STORAGE_PATH), exist_ok=True)
    with open(STORAGE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def store_invoice(invoice: dict) -> dict:
    """Append a parsed invoice to the normalized storage file."""
    invoices = _load_storage()

    # Dedupe by invoice_number + vendor
    key = (invoice.get("vendor", ""), invoice.get("invoice_number", ""))
    for existing in invoices:
        if (existing.get("vendor", ""), existing.get("invoice_number", "")) == key and key[1]:
            return {"status": "duplicate", "message": f"Invoice {key[1]} from {key[0]} already exists"}

    invoices.append(invoice)
    _save_storage(invoices)
    return {"status": "stored", "total_invoices": len(invoices)}


# ── Query Helpers ────────────────────────────────────────────────


def get_latest_vendor_prices() -> dict[str, list[dict]]:
    """
    Get the latest prices per vendor from all stored invoices.
    Returns: { "Vendor Name": [ {item, unit_price, unit, price_basis, confidence, date}, ... ] }
    """
    invoices = _load_storage()
    if not invoices:
        return {}

    invoices.sort(key=lambda x: x.get("invoice_date") or "", reverse=True)

    vendor_prices: dict[str, dict[str, dict]] = {}
    for inv in invoices:
        vendor = inv.get("vendor", "Unknown")
        date = inv.get("invoice_date", "")
        if vendor not in vendor_prices:
            vendor_prices[vendor] = {}
        for item in inv.get("line_items", []):
            name = item.get("normalized_name", item.get("raw_name", "?"))
            if name not in vendor_prices[vendor]:
                vendor_prices[vendor][name] = {
                    "item": name,
                    "category": item.get("category", "other"),
                    "unit_price": item.get("unit_price", 0),
                    "unit": item.get("unit", "ea"),
                    "price_basis": item.get("price_basis", "unknown"),
                    "quantity": item.get("quantity", 1),
                    "confidence": item.get("confidence", "unknown"),
                    "date": date,
                }

    return {
        vendor: list(items.values())
        for vendor, items in vendor_prices.items()
    }


def get_vendor_item_history(vendor_name: str) -> list[dict]:
    """Get full item price history for a specific vendor."""
    invoices = _load_storage()
    history = []

    for inv in invoices:
        if inv.get("vendor", "").lower() != vendor_name.lower():
            continue
        date = inv.get("invoice_date", "")
        inv_num = inv.get("invoice_number", "")
        for item in inv.get("line_items", []):
            history.append({
                "item": item.get("normalized_name", item.get("raw_name", "?")),
                "category": item.get("category", "other"),
                "unit_price": item.get("unit_price", 0),
                "unit": item.get("unit", "ea"),
                "price_basis": item.get("price_basis", "unknown"),
                "quantity": item.get("quantity", 1),
                "line_total": item.get("line_total", 0),
                "confidence": item.get("confidence", "unknown"),
                "date": date,
                "invoice_number": inv_num,
            })

    history.sort(key=lambda x: x.get("date", ""), reverse=True)
    return history


def get_latest_price_for_item(normalized_name: str) -> dict | None:
    """Find the most recent price for a given normalized item name across all vendors."""
    invoices = _load_storage()
    if not invoices:
        return None

    invoices.sort(key=lambda x: x.get("invoice_date") or "", reverse=True)

    target = normalized_name.lower()
    for inv in invoices:
        for item in inv.get("line_items", []):
            name = (item.get("normalized_name") or item.get("raw_name", "")).lower()
            if name == target:
                return {
                    "vendor": inv.get("vendor", "Unknown"),
                    "item": item.get("normalized_name", item.get("raw_name", "")),
                    "category": item.get("category", "other"),
                    "unit_price": item.get("unit_price", 0),
                    "unit": item.get("unit", "ea"),
                    "price_basis": item.get("price_basis", "unknown"),
                    "quantity": item.get("quantity", 1),
                    "confidence": item.get("confidence", "unknown"),
                    "date": inv.get("invoice_date", ""),
                }

    return None


# ── Convenience: full pipeline ───────────────────────────────────


def ingest_invoice(source: str | dict) -> dict:
    """Extract, interpret, normalize, score, and store an invoice in one call.
    Saves to both JSON (legacy) and SQLite (price comparison DB)."""
    result = extract_invoice_data(source)
    if "error" in result:
        return result

    # JSON storage (legacy)
    store_result = store_invoice(result)
    result["storage"] = store_result

    # SQLite storage (vendor price DB)
    try:
        from maillard.mcp.accounting.invoice_db import save_invoice_to_db
        db_result = save_invoice_to_db(result)
        result["db_storage"] = db_result
    except Exception as e:
        result["db_storage"] = {"status": "error", "message": str(e)}

    # Central DB (coffee_agi.db) — updates ingredients, aliases, derived costs
    try:
        from app.data_access.invoice_ingest import post_ingest_to_central_db
        central_result = post_ingest_to_central_db(result)
        result["central_db"] = central_result
    except Exception as e:
        result["central_db"] = {"status": "error", "message": str(e)}

    return result
