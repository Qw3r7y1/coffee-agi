"""
State Loader for Maillard Coffee Roasters.

Loads real-world data from data/current_state.json and data/recipes.json.
Converts sales into inventory consumption using recipe ingredients.
Provides a snapshot of true operational state for the decision engine.

No database. No API. Just JSON files.
Live Square sales data is written by scripts/square_sales_connector.py --save.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"
_STATE_FILE = _DATA_DIR / "current_state.json"
_RECIPES_FILE = _DATA_DIR / "recipes.json"

_EMPTY_STATE = {
    "inventory": {},
    "sales_today": {},
    "sales_last_7_days": {},
    "sales_amounts": {},
    "top_items": [],
    "raw_order_count": 0,
}

_EMPTY_RECIPES = {}


def get_state_meta() -> dict:
    """Return metadata about the current_state.json file for diagnostics."""
    if not _STATE_FILE.exists():
        return {"exists": False, "path": str(_STATE_FILE)}

    stat = _STATE_FILE.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    age_seconds = (now - mtime).total_seconds()

    try:
        data = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    has_live_sales = bool(data.get("sales_today"))
    has_amounts = bool(data.get("sales_amounts"))
    has_top_items = bool(data.get("top_items"))
    order_count = data.get("raw_order_count", 0)
    source = data.get("source", "live_square" if has_live_sales else "empty")

    # Freshness: use snapshot_generated_at if present (postgres pipeline),
    # otherwise fall back to file mtime
    generated_at = data.get("snapshot_generated_at")
    if generated_at:
        try:
            gen_dt = datetime.fromisoformat(generated_at)
            age_seconds = (now - gen_dt).total_seconds()
        except ValueError:
            pass

    # Stale threshold: 5 minutes for db-backed pipeline, 24h for legacy
    stale_threshold = 300 if source in ("postgres", "sqlite") else 86400

    return {
        "exists": True,
        "path": str(_STATE_FILE),
        "last_updated": generated_at or mtime.isoformat(),
        "age_seconds": round(age_seconds),
        "is_stale": age_seconds > stale_threshold,
        "stale_threshold": stale_threshold,
        "has_live_sales": has_live_sales,
        "has_sales_amounts": has_amounts,
        "has_top_items": has_top_items,
        "raw_order_count": order_count,
        "product_count": len(data.get("sales_today", {})),
        "source": source,
        "business_date": data.get("business_date"),
        "last_square_sync_at": data.get("last_square_sync_at"),
        "freshness_seconds": data.get("freshness_seconds"),
    }


# =============================================================================
# LOADERS
# =============================================================================


def load_current_state() -> dict:
    """Load current operational state from data/current_state.json.

    Returns:
        {
            "inventory": {sku: {stock, unit, category, cost_per_unit, min_stock}},
            "sales_today": {product: quantity},
            "sales_last_7_days": {product: quantity},
        }

    Falls back to safe empty defaults if file missing or corrupt.
    """
    try:
        if not _STATE_FILE.exists():
            logger.warning(f"[STATE] {_STATE_FILE} not found, using empty defaults")
            return dict(_EMPTY_STATE)

        data = json.loads(_STATE_FILE.read_text(encoding="utf-8"))

        # Validate structure
        if not isinstance(data, dict):
            logger.error("[STATE] current_state.json is not a dict")
            return dict(_EMPTY_STATE)

        result = {
            "inventory": data.get("inventory") or {},
            "suppliers": data.get("suppliers") or {},
            "sales_today": data.get("sales_today") or {},
            "sales_last_7_days": data.get("sales_last_7_days") or {},
            # Square connector keys — passed through for chat/resolver
            "sales_amounts": data.get("sales_amounts") or {},
            "top_items": data.get("top_items") or [],
            "raw_order_count": data.get("raw_order_count", 0),
        }

        meta = get_state_meta()
        logger.info(
            f"[STATE] loaded from {_STATE_FILE.name}: "
            f"{sum(result['sales_today'].values())} units today, "
            f"{result['raw_order_count']} orders, "
            f"{len(result['sales_today'])} products "
            f"(updated: {meta['last_updated']}, stale: {meta['is_stale']})"
        )
        return result

    except json.JSONDecodeError as e:
        logger.error(f"[STATE] JSON parse error in current_state.json: {e}")
        return dict(_EMPTY_STATE)
    except Exception as e:
        logger.error(f"[STATE] failed to load state: {e}")
        return dict(_EMPTY_STATE)


def load_recipes() -> dict:
    """Load recipe definitions from data/recipes.json.

    Returns:
        {product: {ingredient: amount_per_unit, ...}, ...}
    """
    try:
        if not _RECIPES_FILE.exists():
            logger.warning(f"[STATE] {_RECIPES_FILE} not found")
            return dict(_EMPTY_RECIPES)

        data = json.loads(_RECIPES_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return dict(_EMPTY_RECIPES)

        logger.info(f"[STATE] loaded {len(data)} recipes")
        return data

    except Exception as e:
        logger.error(f"[STATE] failed to load recipes: {e}")
        return dict(_EMPTY_RECIPES)


# =============================================================================
# CONSUMPTION CALCULATOR
# =============================================================================


def calculate_inventory_usage(sales: dict, recipes: dict) -> dict:
    """Convert sales quantities into ingredient consumption using recipes.

    Args:
        sales: {product: quantity_sold} e.g. {"latte": 42, "espresso": 18}
        recipes: {product: {ingredient: amount_per_unit}} from recipes.json

    Returns:
        {ingredient: total_consumed} e.g. {"espresso_beans_kg": 2.51, "whole_milk_liters": 15.5}
    """
    usage: dict[str, float] = {}

    for product, qty_sold in sales.items():
        recipe = recipes.get(product)
        if not recipe:
            continue  # no recipe = no consumption (e.g. merchandise)

        for ingredient, amount_per_unit in recipe.items():
            if not amount_per_unit:
                continue
            usage[ingredient] = round(
                usage.get(ingredient, 0) + qty_sold * amount_per_unit, 4
            )

    # Round final values
    return {k: round(v, 3) for k, v in usage.items()}


def apply_inventory_consumption(inventory: dict, usage: dict) -> dict:
    """Subtract ingredient usage from inventory. Floors at 0.

    Args:
        inventory: {sku: {stock: float, ...}} from current_state.json
        usage: {ingredient: total_consumed} from calculate_inventory_usage()

    Returns:
        Updated inventory dict (copy, does NOT modify original)
    """
    # Map ingredient names to inventory SKUs
    # e.g. "espresso_beans_kg" could map to "ethiopia_yirgacheffe" or "brazil_espresso"
    ingredient_to_sku = _build_ingredient_map(inventory)

    updated = {}
    for sku, info in inventory.items():
        updated[sku] = {**info}  # copy

    consumed_from: dict[str, list] = {}  # track what was consumed from where

    for ingredient, amount in usage.items():
        target_skus = ingredient_to_sku.get(ingredient, [])
        remaining = amount

        for sku in target_skus:
            if remaining <= 0:
                break
            available = updated[sku]["stock"]
            take = min(available, remaining)
            updated[sku]["stock"] = round(max(0, available - take), 3)
            remaining = round(remaining - take, 3)

            consumed_from.setdefault(ingredient, []).append(
                {"sku": sku, "consumed": round(take, 3)}
            )

        if remaining > 0:
            consumed_from.setdefault(ingredient, []).append(
                {"sku": "DEFICIT", "shortfall": round(remaining, 3)}
            )

    return updated


def _build_ingredient_map(inventory: dict) -> dict[str, list[str]]:
    """Map recipe ingredient names to inventory SKUs.

    Convention:
      "espresso_beans_kg" -> all roasted_coffee SKUs (consumed in order)
      "whole_milk_liters" -> all milk SKUs with 'whole' or 'full'
      "oat_milk_liters"   -> milk SKUs with 'oat'
      "ethiopia_yirgacheffe_kg" -> SKU containing 'ethiopia'
      "brazil_espresso_kg" -> SKU containing 'brazil'
    """
    mapping: dict[str, list[str]] = {}

    roasted = [sku for sku, info in inventory.items() if info.get("category") == "roasted_coffee"]
    milk_all = [sku for sku, info in inventory.items() if info.get("category") == "milk"]
    green = [sku for sku, info in inventory.items() if info.get("category") == "green_coffee"]

    mapping["espresso_beans_kg"] = roasted  # consumed from any roasted
    mapping["whole_milk_liters"] = [s for s in milk_all if "oat" not in s.lower()] or milk_all
    mapping["oat_milk_liters"] = [s for s in milk_all if "oat" in s.lower()] or milk_all

    # Origin-specific: match by name
    for sku in roasted + green:
        lower = sku.lower()
        if "ethiopia" in lower:
            mapping["ethiopia_yirgacheffe_kg"] = [sku]
        if "brazil" in lower:
            mapping["brazil_espresso_kg"] = [sku]

    return mapping


# =============================================================================
# FULL SNAPSHOT (combines everything)
# =============================================================================


def get_operational_snapshot() -> dict:
    """Load state, calculate consumption, return full operational picture.

    Returns:
        {
            "state": raw state from file,
            "recipes": recipe definitions,
            "usage_today": ingredient consumption from today's sales,
            "usage_7d_avg": average daily ingredient consumption,
            "updated_inventory": inventory after subtracting today's usage,
            "demand_signals": per-product trend (today vs 7d avg),
        }
    """
    state = load_current_state()
    recipes = load_recipes()

    inv = state.get("inventory", {})
    sales_today = state.get("sales_today", {})
    sales_7d = state.get("sales_last_7_days", {})

    # Usage from today's sales
    usage_today = calculate_inventory_usage(sales_today, recipes)

    # Average daily usage from 7-day sales
    avg_daily_sales = {p: round(q / 7, 1) for p, q in sales_7d.items()}
    usage_7d_avg = calculate_inventory_usage(avg_daily_sales, recipes)

    # Updated inventory after today's consumption
    updated_inv = apply_inventory_consumption(inv, usage_today)

    # Demand signals: today vs 7d average
    demand_signals = {}
    for product in set(list(sales_today.keys()) + list(sales_7d.keys())):
        today = sales_today.get(product, 0)
        avg = round(sales_7d.get(product, 0) / 7, 1)
        if avg > 0:
            change = round((today - avg) / avg * 100)
            trend = "rising" if change >= 20 else ("dropping" if change <= -20 else "stable")
        else:
            change = 0
            trend = "new" if today > 0 else "stable"
        demand_signals[product] = {
            "today": today,
            "daily_avg": avg,
            "change_pct": change,
            "trend": trend,
        }

    # Stock days remaining (after today's consumption)
    stock_days = {}
    for sku, info in updated_inv.items():
        daily = usage_7d_avg.get(_sku_to_ingredient(sku, inv), 0)
        if daily > 0:
            days = round(info["stock"] / daily, 1)
        else:
            days = None
        stock_days[sku] = days

    return {
        "state": state,
        "recipes": recipes,
        "usage_today": usage_today,
        "usage_7d_daily_avg": usage_7d_avg,
        "updated_inventory": updated_inv,
        "demand_signals": demand_signals,
        "stock_days": stock_days,
        "inventory_risks": detect_low_inventory(updated_inv),
    }


def detect_low_inventory(updated_inventory: dict) -> dict:
    """Flag items with dangerously low stock relative to their minimum.

    Rules:
      stock <= 0         -> "STOCKOUT"
      stock < min * 0.10 -> "CRITICAL" (under 10% of safe level)
      stock < min * 0.20 -> "LOW" (under 20% of safe level)
      else               -> "OK"

    Returns:
        {sku: {"status": str, "stock": float, "min": float, "unit": str, "name": str}}
    """
    risks = {}
    for sku, info in updated_inventory.items():
        stock = info.get("stock", 0)
        min_stock = info.get("min_stock", 0)
        unit = info.get("unit", "")

        if stock <= 0:
            status = "STOCKOUT"
        elif min_stock > 0 and stock < min_stock * 0.10:
            status = "CRITICAL"
        elif min_stock > 0 and stock < min_stock * 0.20:
            status = "LOW"
        else:
            status = "OK"

        if status != "OK":
            risks[sku] = {
                "status": status,
                "stock": stock,
                "min_stock": min_stock,
                "unit": unit,
                "category": info.get("category", ""),
            }

    return risks


def _sku_to_ingredient(sku: str, inventory: dict) -> str:
    """Best-guess map from an inventory SKU back to an ingredient name."""
    info = inventory.get(sku, {})
    cat = info.get("category", "")
    lower = sku.lower()

    if cat == "roasted_coffee":
        if "ethiopia" in lower:
            return "ethiopia_yirgacheffe_kg"
        if "brazil" in lower:
            return "brazil_espresso_kg"
        return "espresso_beans_kg"
    if cat == "milk":
        if "oat" in lower:
            return "oat_milk_liters"
        return "whole_milk_liters"
    if cat == "consumables":
        return ""
    if cat == "green_coffee":
        return ""
    return ""
