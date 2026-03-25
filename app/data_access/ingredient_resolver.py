"""
Ingredient Resolution Layer — maps any text reference to a canonical ingredient_key.

Search order:
  1. Exact ingredient_key match
  2. Exact alias match
  3. Fuzzy alias/display_name match
  4. No match → return None
"""
from __future__ import annotations

import re
from app.core.db import get_conn, now_iso


def resolve_ingredient(text: str) -> dict | None:
    """Resolve any text to a canonical ingredient. Returns ingredient row or None."""
    if not text or not text.strip():
        return None

    conn = get_conn()
    clean = text.strip()
    lower = clean.lower()
    key_form = re.sub(r"[^a-z0-9]+", "_", lower).strip("_")

    # 1. Exact ingredient_key match — only if it has a cost
    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=? AND latest_unit_cost > 0", (key_form,)).fetchone()
    if row:
        conn.close()
        return {**dict(row), "match_type": "exact_key"}

    row = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=? AND latest_unit_cost > 0", (clean,)).fetchone()
    if row:
        conn.close()
        return {**dict(row), "match_type": "exact_key"}

    # 2. Exact alias match — resolve to ingredient WITH cost
    row = conn.execute("""
        SELECT i.*, ia.alias_text FROM ingredient_aliases ia
        JOIN ingredients i ON ia.ingredient_key = i.ingredient_key
        WHERE LOWER(ia.alias_text) = ? AND i.latest_unit_cost > 0
        ORDER BY i.latest_unit_cost DESC LIMIT 1
    """, (lower,)).fetchone()
    if not row:
        row = conn.execute("""
            SELECT i.*, ia.alias_text FROM ingredient_aliases ia
            JOIN ingredients i ON ia.ingredient_key = i.ingredient_key
            WHERE LOWER(ia.alias_text) = ? AND i.latest_unit_cost > 0
            ORDER BY i.latest_unit_cost DESC LIMIT 1
        """, (key_form,)).fetchone()
    if row:
        conn.close()
        return {**dict(row), "match_type": "alias_exact"}

    # 3. Fuzzy: search by tokens in ingredient_key, display_name, and aliases
    tokens = [t for t in key_form.split("_") if len(t) >= 3]
    # Also try the full clean text as a single search term
    search_terms = list(tokens)
    if len(clean) >= 4:
        search_terms.append(clean)

    if search_terms:
        # Score all ingredients by token overlap — accept single strong match
        all_ings = conn.execute("SELECT * FROM ingredients WHERE latest_unit_cost > 0").fetchall()
        best = None
        best_score = 0
        for ing in all_ings:
            ik = ing["ingredient_key"].lower()
            dn = (ing["display_name"] or "").lower()
            score = sum(1 for t in search_terms if t in ik or t in dn)
            # Bonus: if the full search term appears as substring
            if clean in ik or clean in dn:
                score += 2
            if score > best_score:
                best_score = score
                best = ing
        # Accept: 2+ multi-token match OR 1 single-token match if clean text is 5+ chars
        threshold = 1 if len(clean) >= 5 else 2
        if best and best_score >= threshold:
            conn.close()
            return {**dict(best), "match_type": "fuzzy", "match_score": best_score}

    # Also search aliases fuzzy
    if search_terms:
        alias_rows = conn.execute("""
            SELECT ia.ingredient_key, ia.alias_text FROM ingredient_aliases ia
            JOIN ingredients i ON ia.ingredient_key = i.ingredient_key
            WHERE i.latest_unit_cost > 0
        """).fetchall()
        best_alias = None
        best_alias_score = 0
        for ar in alias_rows:
            alias_lower = ar["alias_text"].lower()
            score = sum(1 for t in search_terms if t in alias_lower)
            if clean in alias_lower:
                score += 2
            if score > best_alias_score:
                best_alias_score = score
                best_alias = ar
        threshold = 1 if len(clean) >= 5 else 2
        if best_alias and best_alias_score >= threshold:
            ing = conn.execute("SELECT * FROM ingredients WHERE ingredient_key=? AND latest_unit_cost > 0",
                               (best_alias["ingredient_key"],)).fetchone()
            if ing:
                conn.close()
                return {**dict(ing), "match_type": "alias_fuzzy", "match_score": best_alias_score}

    conn.close()
    return None


def add_alias(ingredient_key: str, alias_text: str, source: str = "manual") -> None:
    """Add an alias for an ingredient. Idempotent."""
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO ingredient_aliases (ingredient_key, alias_text, source_type, created_at)
        VALUES (?, ?, ?, ?)
    """, (ingredient_key, alias_text, source, now_iso()))
    conn.commit()
    conn.close()


def get_aliases(ingredient_key: str) -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT alias_text FROM ingredient_aliases WHERE ingredient_key=?", (ingredient_key,)).fetchall()
    conn.close()
    return [r["alias_text"] for r in rows]


def build_aliases_from_invoices() -> int:
    """Create aliases from invoice raw_name and normalized_name for all ingredients."""
    conn = get_conn()
    now = now_iso()

    # For each ingredient, find invoice items whose normalized key matches
    ings = conn.execute("SELECT ingredient_key, display_name FROM ingredients").fetchall()
    count = 0

    for ing in ings:
        ik = ing["ingredient_key"]
        # Search invoice items where the normalized key (lowered, underscored) matches this ingredient
        items = conn.execute("""
            SELECT DISTINCT raw_name, normalized_name FROM invoice_items
            WHERE raw_name IS NOT NULL
        """).fetchall()

        for item in items:
            raw = item["raw_name"] or ""
            norm = item["normalized_name"] or ""
            raw_key = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")
            norm_key = re.sub(r"[^a-z0-9]+", "_", norm.lower()).strip("_")

            if raw_key == ik or norm_key == ik:
                # Exact match — add raw name as alias
                conn.execute("""
                    INSERT OR IGNORE INTO ingredient_aliases (ingredient_key, alias_text, source_type, created_at)
                    VALUES (?, ?, 'invoice_raw', ?)
                """, (ik, raw, now))
                if norm and norm != raw:
                    conn.execute("""
                        INSERT OR IGNORE INTO ingredient_aliases (ingredient_key, alias_text, source_type, created_at)
                        VALUES (?, ?, 'invoice_normalized', ?)
                    """, (ik, norm, now))
                count += 1

    conn.commit()
    conn.close()
    return count


def seed_common_aliases() -> int:
    """Seed manual aliases for generic recipe ingredient keys to canonical invoice ingredients."""
    MANUAL_ALIASES = [
        # Generic recipe keys -> specific invoice ingredient keys
        ("bakery_item_wholesale", ["all_butter_croissant_rtb_4_5_oz_42", "all_butter_croissant_rtd_4_5_oz_42"]),
        ("bakery_bag_or_wrapper", ["kraft_pastry_dry_wax_bag_6_5x8_2_000"]),
        ("cups_hot_or_cold", ["12_oz_white_hot_paper_cup_1_000", "16_oz_white_hot_paper_cup_1_000"]),
        ("cups_cold_plastic", ["16_oz_clear_plastic_cup_1_000", "24_oz_clear_plastic_cup_600"]),
        ("lids_cold", ["clear_flat_lid_for_plastic_cups_12_24oz_1_000"]),
        ("napkin", ["black_beverage_napkins_1_000"]),
        ("whole milk", ["battenkill_whole_milk_gallon"]),
        ("sleeve", ["java_jacket_eco_ii_kraft_hot_cup_sleeves_12_20oz"]),
        ("pastry bag", ["kraft_pastry_dry_wax_bag_6_5x8_2_000"]),
        ("spinach pie", ["peinirli_wheat_base_56_pcs_210262"]),
        ("cheese pie", ["koulouri_cheese_1106g_alfa", "koulouri_with_cheese_1106g_alfa"]),
        ("16 oz plastic cup", ["16_oz_clear_plastic_cup_1_000"]),
        ("chocolate powder", ["coffee_nescafe_loose"]),
        ("cup_container", ["12_oz_white_hot_paper_cup_1_000"]),
        ("bou", ["rodoula_bougatsa_with_vanilla_cream", "rodouia_bougatsa_with_vanilla_cream"]),
        ("bougatsa", ["rodoula_bougatsa_with_vanilla_cream", "rodouia_bougatsa_with_vanilla_cream"]),
        ("peinirli", ["peinirli_wheat_base_56_pcs_210262"]),
        ("koulouri", ["koulouri_cheese_1106g_alfa"]),
        ("nutella", ["croissant_w_choco_banana_100224"]),
        ("banana choco", ["croissant_w_choco_banana_100224"]),
    ]

    conn = get_conn()
    now = now_iso()
    count = 0

    for generic_key, aliases in MANUAL_ALIASES:
        for alias in aliases:
            # Check if the alias points to an existing ingredient
            alias_key = re.sub(r"[^a-z0-9]+", "_", alias.lower()).strip("_")
            target = conn.execute("SELECT ingredient_key FROM ingredients WHERE ingredient_key=? AND latest_unit_cost > 0", (alias_key,)).fetchone()

            if target:
                # Create alias: generic_key is an alias for the real ingredient
                conn.execute("""
                    INSERT OR IGNORE INTO ingredient_aliases (ingredient_key, alias_text, source_type, created_at)
                    VALUES (?, ?, 'manual_mapping', ?)
                """, (target["ingredient_key"], generic_key, now))
                count += 1

    conn.commit()
    conn.close()
    return count
