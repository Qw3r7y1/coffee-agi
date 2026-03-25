"""
Modifier Manager for Maillard Coffee Roasters.

CRUD + validation for product modifiers (data/modifiers.json).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "data"
_MOD_FILE = _DATA / "modifiers.json"

_VALID_TYPES = {"add", "replace", "size_upgrade"}
_VALID_UNITS = {"kg", "g", "liters", "ml", "ea", "unit", "oz", "lb", "cup", "tbsp", "tsp", ""}


def _load() -> dict:
    if _MOD_FILE.exists():
        try:
            return json.loads(_MOD_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save(data: dict) -> None:
    _MOD_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ── CRUD ──

def get_modifiers() -> dict:
    return _load()


def get_modifier(key: str) -> dict | None:
    mods = _load()
    if key not in mods:
        return None
    return {"modifier_key": key, **mods[key]}


def create_modifier(key: str, data: dict) -> dict:
    """Create a new modifier. Returns the created modifier or {"error": ...}."""
    mods = _load()
    if key in mods:
        return {"error": f"Modifier '{key}' already exists"}
    errors = validate_modifier(key, data)
    if errors:
        return {"error": errors[0], "all_errors": errors}
    mods[key] = data
    _save(mods)
    return {"modifier_key": key, **data}


def update_modifier(key: str, updates: dict) -> dict | None:
    """Update a modifier. Returns updated modifier, None if not found, or {"error":...}."""
    mods = _load()
    if key not in mods:
        return None
    merged = {**mods[key], **updates}
    errors = validate_modifier(key, merged)
    if errors:
        return {"error": errors[0], "all_errors": errors}
    mods[key] = merged
    _save(mods)
    return {"modifier_key": key, **merged}


def delete_modifier(key: str) -> dict | None:
    """Delete a modifier. Returns deleted modifier or None."""
    mods = _load()
    if key not in mods:
        return None
    removed = mods.pop(key)
    _save(mods)
    return {"modifier_key": key, **removed, "deleted": True}


# ── VALIDATION ──

def validate_modifier(key: str, data: dict) -> list[str]:
    """Validate modifier data. Returns list of errors (empty = valid)."""
    errors = []

    if not key or not re.match(r"^[a-z][a-z0-9_]*$", key):
        errors.append(f"Invalid modifier_key: '{key}' (must be lowercase snake_case)")

    mod_type = data.get("type", "")
    if mod_type not in _VALID_TYPES:
        errors.append(f"Invalid type: '{mod_type}' (must be one of: {', '.join(sorted(_VALID_TYPES))})")

    if mod_type == "size_upgrade":
        sf = data.get("scale_factor")
        if sf is None:
            errors.append("size_upgrade requires scale_factor")
        elif not isinstance(sf, (int, float)) or sf <= 0:
            errors.append(f"scale_factor must be a positive number (got {sf!r})")
    else:
        adds = data.get("adds", [])
        removes = data.get("removes", [])

        if mod_type == "add" and not adds:
            errors.append("'add' modifier must have at least one item in adds[]")

        if mod_type == "replace":
            if not adds:
                errors.append("'replace' modifier must have at least one item in adds[]")
            if not removes:
                errors.append("'replace' modifier must have at least one item in removes[]")

        # Validate ingredient rows
        seen_add = set()
        for i, a in enumerate(adds):
            ik = a.get("ingredient_key", "").strip()
            if not ik:
                errors.append(f"adds[{i}]: missing ingredient_key")
            elif ik in seen_add:
                errors.append(f"adds[{i}]: duplicate ingredient '{ik}'")
            seen_add.add(ik)

            if not a.get("quantity_from_removed"):
                qty = a.get("quantity")
                if qty is not None:
                    try:
                        q = float(qty)
                        if q < 0:
                            errors.append(f"adds[{i}] '{ik}': quantity must be >= 0")
                    except (ValueError, TypeError):
                        errors.append(f"adds[{i}] '{ik}': invalid quantity {qty!r}")

            unit = a.get("unit", "")
            if unit and unit not in _VALID_UNITS:
                errors.append(f"adds[{i}] '{ik}': invalid unit '{unit}'")

        seen_rem = set()
        for i, r in enumerate(removes):
            ik = r.get("ingredient_key", "").strip()
            if not ik:
                errors.append(f"removes[{i}]: missing ingredient_key")
            elif ik in seen_rem:
                errors.append(f"removes[{i}]: duplicate ingredient '{ik}'")
            seen_rem.add(ik)

    upcharge = data.get("upcharge")
    if upcharge is not None:
        try:
            float(upcharge)
        except (ValueError, TypeError):
            errors.append(f"upcharge must be a number (got {upcharge!r})")

    return errors
