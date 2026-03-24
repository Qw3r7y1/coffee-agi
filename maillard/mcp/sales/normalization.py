"""
Sales Normalization Layer for Maillard Coffee Roasters.

Converts sales data from any source (Square POS, Shopify, QuickBooks, wholesale)
into one unified format for demand calculation, production planning, and financials.

Every sale becomes:
{
    "timestamp": ISO string,
    "product": normalized slug,
    "product_display": human-readable name,
    "quantity": float,
    "channel": "pos" | "shopify" | "wholesale" | "quickbooks",
    "revenue": float (EUR),
    "category": "drink" | "bag" | "wholesale" | "food" | "merchandise"
}
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from loguru import logger


# =============================================================================
# PRODUCT NORMALIZATION MAP
# =============================================================================
# Maps messy POS/Shopify/invoice names to clean slugs.
# Add entries as new products appear -- this IS the source of truth.

_PRODUCT_MAP: dict[str, dict] = {
    # ── Espresso drinks ──────────────────────────────────────────────────
    "espresso":          {"slug": "espresso",          "display": "Espresso",              "category": "drink"},
    "double_espresso":   {"slug": "double_espresso",   "display": "Double Espresso",       "category": "drink"},
    "americano":         {"slug": "americano",          "display": "Americano",             "category": "drink"},
    "latte":             {"slug": "latte",              "display": "Latte",                 "category": "drink"},
    "cappuccino":        {"slug": "cappuccino",         "display": "Cappuccino",            "category": "drink"},
    "flat_white":        {"slug": "flat_white",         "display": "Flat White",            "category": "drink"},
    "mocha":             {"slug": "mocha",              "display": "Mocha",                 "category": "drink"},
    "macchiato":         {"slug": "macchiato",          "display": "Macchiato",             "category": "drink"},
    "cortado":           {"slug": "cortado",            "display": "Cortado",               "category": "drink"},
    # ── Cold drinks ──────────────────────────────────────────────────────
    "freddo_espresso":   {"slug": "freddo_espresso",   "display": "Freddo Espresso",       "category": "drink"},
    "freddo_cappuccino": {"slug": "freddo_cappuccino", "display": "Freddo Cappuccino",     "category": "drink"},
    "cold_brew":         {"slug": "cold_brew",          "display": "Cold Brew",             "category": "drink"},
    "iced_latte":        {"slug": "iced_latte",         "display": "Iced Latte",            "category": "drink"},
    # ── Filter ───────────────────────────────────────────────────────────
    "filter_coffee":     {"slug": "filter_coffee",      "display": "Filter Coffee",         "category": "drink"},
    "pour_over":         {"slug": "pour_over",          "display": "Pour Over",             "category": "drink"},
    # ── Food ─────────────────────────────────────────────────────────────
    "crepe":             {"slug": "crepe",              "display": "Crepe",                 "category": "food"},
    "parfait":           {"slug": "parfait",            "display": "Parfait",               "category": "food"},
    "overnight_oats":    {"slug": "overnight_oats",     "display": "Overnight Oats",        "category": "food"},
    # ── Retail bags ──────────────────────────────────────────────────────
    "ethiopia_yirgacheffe": {"slug": "ethiopia_yirgacheffe", "display": "Ethiopia Yirgacheffe", "category": "bag"},
    "colombia_huila":    {"slug": "colombia_huila",     "display": "Colombia Huila",        "category": "bag"},
    "brazil_santos":     {"slug": "brazil_santos",      "display": "Brazil Santos",         "category": "bag"},
    "espresso_blend":    {"slug": "espresso_blend",     "display": "Espresso Blend",        "category": "bag"},
    "house_blend":       {"slug": "house_blend",        "display": "House Blend",           "category": "bag"},
    # ── Wholesale ────────────────────────────────────────────────────────
    "wholesale_coffee":  {"slug": "wholesale_coffee",   "display": "Wholesale Coffee",      "category": "wholesale"},
}

# Regex patterns to match messy input names to product slugs
_NAME_PATTERNS: list[tuple[str, str]] = [
    # Drinks — order matters: specific patterns before generic ones
    (r"freddo\s*cap", "freddo_cappuccino"),
    (r"freddo", "freddo_espresso"),
    (r"double\s*espresso|doppio", "double_espresso"),
    (r"espresso(?!\s*blend)", "espresso"),
    (r"americano", "americano"),
    (r"flat\s*white", "flat_white"),
    (r"cappuccino|capp\b", "cappuccino"),
    (r"latte(?!\s*art).*iced|iced.*latte", "iced_latte"),
    (r"latte", "latte"),
    (r"mocha", "mocha"),
    (r"macchiato", "macchiato"),
    (r"cortado", "cortado"),
    (r"cold\s*brew", "cold_brew"),
    (r"filter|drip|batch\s*brew", "filter_coffee"),
    (r"pour\s*over|v60|chemex", "pour_over"),
    # Food
    (r"crepe|cr[eê]pe", "crepe"),
    (r"parfait", "parfait"),
    (r"overnight\s*oat", "overnight_oats"),
    # Bags
    (r"ethiopia|yirgacheffe|yirg", "ethiopia_yirgacheffe"),
    (r"colombia|huila", "colombia_huila"),
    (r"brazil|santos", "brazil_santos"),
    (r"espresso\s*blend", "espresso_blend"),
    (r"house\s*blend", "house_blend"),
    # Wholesale
    (r"wholesale|bulk\s*coffee|invoice.*coffee", "wholesale_coffee"),
]


def normalize_product_name(raw_name: Any) -> dict:
    """Normalize a messy product name to a clean slug + metadata.

    Accepts any type — coerces to string, never crashes.

    Returns:
        {"slug": str, "display": str, "category": str, "matched": bool}
    """
    if raw_name is None or raw_name == "":
        return {"slug": "unknown", "display": "Unknown", "category": "other", "matched": False}
    raw_name = str(raw_name)
    clean = raw_name.strip().lower()
    # Strip size suffixes: "12oz", "250g", "1kg", etc.
    clean = re.sub(r"\b\d+\s*(oz|ml|g|kg|lb)\b", "", clean).strip()
    # Strip parenthetical notes
    clean = re.sub(r"\(.*?\)", "", clean).strip()

    for pattern, slug in _NAME_PATTERNS:
        if re.search(pattern, clean, re.IGNORECASE):
            info = _PRODUCT_MAP.get(slug, {"slug": slug, "display": slug.replace("_", " ").title(), "category": "drink"})
            return {**info, "matched": True}

    # No match -- return cleaned name as slug
    slug = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
    return {"slug": slug or "unknown", "display": raw_name.strip(), "category": "other", "matched": False}


# =============================================================================
# SAFE TYPE COERCION
# =============================================================================


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert anything to float without crashing."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_str(val: Any, default: str = "") -> str:
    """Convert anything to string without crashing."""
    if val is None:
        return default
    return str(val).strip()


# =============================================================================
# CHANNEL-SPECIFIC NORMALIZERS
# =============================================================================


def normalize_pos(records: list[dict]) -> list[dict]:
    """Normalize Square/POS data. Skips invalid records, never crashes."""
    results = []
    for i, r in enumerate(records or []):
        try:
            if not isinstance(r, dict):
                logger.warning(f"[SALES-NORM] pos record {i}: not a dict, skipping")
                continue
            raw_name = _safe_str(r.get("item_name"), "unknown")
            prod = normalize_product_name(raw_name)
            results.append({
                "timestamp": _parse_ts(r.get("timestamp")),
                "product": prod["slug"],
                "product_display": prod["display"],
                "quantity": _safe_float(r.get("quantity"), 1),
                "channel": "pos",
                "revenue": round(_safe_float(r.get("total")), 2),
                "category": prod["category"],
                "raw_name": raw_name,
            })
        except Exception as e:
            logger.error(f"[SALES-NORM] pos record {i} failed: {e} | raw={r}")
    return results


def normalize_shopify(records: list[dict]) -> list[dict]:
    """Normalize Shopify/website orders. Skips invalid records, never crashes."""
    results = []
    for i, r in enumerate(records or []):
        try:
            if not isinstance(r, dict):
                logger.warning(f"[SALES-NORM] shopify record {i}: not a dict, skipping")
                continue
            raw_name = _safe_str(r.get("title"), "unknown")
            prod = normalize_product_name(raw_name)
            qty = _safe_float(r.get("quantity"), 1)
            price = _safe_float(r.get("price"))
            results.append({
                "timestamp": _parse_ts(r.get("created_at")),
                "product": prod["slug"],
                "product_display": prod["display"],
                "quantity": qty,
                "channel": "shopify",
                "revenue": round(qty * price, 2),
                "category": prod["category"],
                "raw_name": raw_name,
            })
        except Exception as e:
            logger.error(f"[SALES-NORM] shopify record {i} failed: {e} | raw={r}")
    return results


def normalize_quickbooks(records: list[dict]) -> list[dict]:
    """Normalize QuickBooks invoice lines. Skips invalid records, never crashes."""
    results = []
    for i, r in enumerate(records or []):
        try:
            if not isinstance(r, dict):
                logger.warning(f"[SALES-NORM] quickbooks record {i}: not a dict, skipping")
                continue
            raw_name = _safe_str(r.get("description"), "unknown")
            prod = normalize_product_name(raw_name)
            results.append({
                "timestamp": _parse_ts(r.get("date")),
                "product": prod["slug"],
                "product_display": prod["display"],
                "quantity": _safe_float(r.get("quantity"), 1),
                "channel": "quickbooks",
                "revenue": round(_safe_float(r.get("amount")), 2),
                "category": prod["category"],
                "raw_name": raw_name,
            })
        except Exception as e:
            logger.error(f"[SALES-NORM] quickbooks record {i} failed: {e} | raw={r}")
    return results


def normalize_wholesale(records: list[dict]) -> list[dict]:
    """Normalize manual wholesale records. Skips invalid records, never crashes."""
    results = []
    for i, r in enumerate(records or []):
        try:
            if not isinstance(r, dict):
                logger.warning(f"[SALES-NORM] wholesale record {i}: not a dict, skipping")
                continue
            raw_name = _safe_str(r.get("product"), "wholesale coffee")
            prod = normalize_product_name(raw_name)
            kg = _safe_float(r.get("kg"))
            ppkg = _safe_float(r.get("price_per_kg"))
            results.append({
                "timestamp": _parse_ts(r.get("date")),
                "product": prod["slug"],
                "product_display": prod["display"],
                "quantity": kg,
                "channel": "wholesale",
                "revenue": round(kg * ppkg, 2),
                "category": "wholesale",
                "raw_name": raw_name,
                "customer": _safe_str(r.get("customer")),
            })
        except Exception as e:
            logger.error(f"[SALES-NORM] wholesale record {i} failed: {e} | raw={r}")
    return results


# =============================================================================
# UNIFIED FEED
# =============================================================================


def unified_sales_feed(
    pos: list[dict] | None = None,
    shopify: list[dict] | None = None,
    quickbooks: list[dict] | None = None,
    wholesale: list[dict] | None = None,
) -> dict:
    """Merge all channels into one normalized feed.

    Returns:
        {
            "sales": [normalized records],
            "total_revenue": float,
            "by_channel": {channel: {count, revenue}},
            "by_category": {category: {count, revenue}},
            "by_product": {slug: {count, revenue}},
            "unmatched": [records that couldn't be matched to known products],
        }
    """
    all_sales: list[dict] = []

    if pos:
        all_sales.extend(normalize_pos(pos))
    if shopify:
        all_sales.extend(normalize_shopify(shopify))
    if quickbooks:
        all_sales.extend(normalize_quickbooks(quickbooks))
    if wholesale:
        all_sales.extend(normalize_wholesale(wholesale))

    # Deduplicate: same timestamp + product + channel + revenue = duplicate
    seen = set()
    deduped = []
    for s in all_sales:
        key = (s["timestamp"], s["product"], s["channel"], s["revenue"])
        if key not in seen:
            seen.add(key)
            deduped.append(s)

    # Sort by timestamp
    deduped.sort(key=lambda s: s["timestamp"] or "")

    # Aggregations
    total_rev = sum(s["revenue"] for s in deduped)

    by_channel: dict[str, dict] = {}
    by_category: dict[str, dict] = {}
    by_product: dict[str, dict] = {}

    for s in deduped:
        ch = s["channel"]
        by_channel.setdefault(ch, {"count": 0, "revenue": 0})
        by_channel[ch]["count"] += 1
        by_channel[ch]["revenue"] += s["revenue"]

        cat = s["category"]
        by_category.setdefault(cat, {"count": 0, "revenue": 0})
        by_category[cat]["count"] += 1
        by_category[cat]["revenue"] += s["revenue"]

        p = s["product"]
        by_product.setdefault(p, {"count": 0, "revenue": 0, "display": s["product_display"]})
        by_product[p]["count"] += s["quantity"]
        by_product[p]["revenue"] += s["revenue"]

    # Round aggregates
    for d in [by_channel, by_category]:
        for v in d.values():
            v["revenue"] = round(v["revenue"], 2)
    for v in by_product.values():
        v["count"] = round(v["count"], 1)
        v["revenue"] = round(v["revenue"], 2)

    unmatched = [s for s in deduped if s.get("category") == "other"]

    logger.info(f"[SALES-NORM] {len(deduped)} sales, EUR {total_rev:.2f}, {len(unmatched)} unmatched")

    return {
        "sales": deduped,
        "total_count": len(deduped),
        "total_revenue": round(total_rev, 2),
        "by_channel": by_channel,
        "by_category": by_category,
        "by_product": by_product,
        "unmatched": unmatched,
        "unmatched_count": len(unmatched),
    }


# =============================================================================
# DEMAND AGGREGATION
# =============================================================================


def aggregate_daily_demand(sales: list[dict]) -> dict:
    """Sum quantities per product per day, ignoring channel.

    Args:
        sales: list of normalized sale records (output of unified_sales_feed()["sales"])

    Returns:
        {
            "2026-03-19": {"latte": 12, "espresso": 8, ...},
            "2026-03-20": {...},
            ...
        }
    """
    by_day: dict[str, dict[str, float]] = {}

    for s in sales:
        ts = s.get("timestamp", "")
        day = ts[:10] if ts and len(ts) >= 10 else "unknown"
        product = s.get("product", "unknown")
        qty = _safe_float(s.get("quantity"), 0)

        by_day.setdefault(day, {})
        by_day[day][product] = round(by_day[day].get(product, 0) + qty, 2)

    return dict(sorted(by_day.items()))


def get_demand_summary(sales: list[dict]) -> dict:
    """Aggregate demand across all days into a single product summary.

    Returns:
        {
            "demand": {"latte": 120, "espresso": 80, ...},
            "by_day": {"2026-03-19": {...}, ...},
            "total_units": float,
            "days": int,
            "daily_avg": {"latte": 17.1, ...},
        }
    """
    daily = aggregate_daily_demand(sales)

    # Totals across all days
    totals: dict[str, float] = {}
    for day_products in daily.values():
        for product, qty in day_products.items():
            totals[product] = round(totals.get(product, 0) + qty, 2)

    # Sort by volume descending
    totals = dict(sorted(totals.items(), key=lambda x: -x[1]))

    days = len(daily)
    daily_avg = {p: round(q / days, 1) for p, q in totals.items()} if days > 0 else {}

    return {
        "demand": totals,
        "by_day": daily,
        "total_units": round(sum(totals.values()), 1),
        "days": days,
        "daily_avg": daily_avg,
    }


# =============================================================================
# HELPERS
# =============================================================================


def _parse_ts(raw: str | None) -> str | None:
    """Parse a timestamp string into ISO format. Lenient."""
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
    except (ValueError, AttributeError):
        return raw
