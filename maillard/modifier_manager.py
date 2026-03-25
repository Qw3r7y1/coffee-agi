"""
Modifier Manager for Maillard Coffee Roasters.

CRUD + validation for product modifiers. Reads/writes coffee_agi.db via repos.
"""
from __future__ import annotations

import re
from app.data_access.modifiers_repo import (
    get_modifier as _db_get,
    get_all_modifiers_dict as _db_all,
    list_modifiers as _db_list,
    upsert_modifier as _db_upsert,
    replace_modifier_rules as _db_replace_rules,
)
from app.core.db import get_conn

_VALID_TYPES = {"add", "replace", "size_upgrade"}
_VALID_UNITS = {"kg", "g", "liters", "ml", "ea", "unit", "oz", "lb", "cup", "tbsp", "tsp", ""}


def get_modifiers() -> dict:
    return _db_all()


def get_modifier(key: str) -> dict | None:
    return _db_get(key)


def create_modifier(key: str, data: dict) -> dict:
    existing = _db_get(key)
    if existing:
        return {"error": f"Modifier '{key}' already exists"}
    errors = validate_modifier(key, data)
    if errors:
        return {"error": errors[0], "all_errors": errors}
    _db_upsert(key, data)
    # Write rules
    rules = []
    for a in data.get("adds", []):
        rules.append({**a, "action": "add"})
    for r in data.get("removes", []):
        rules.append({"ingredient_key": r["ingredient_key"], "action": "remove"})
    _db_replace_rules(key, rules)
    return _db_get(key)


def update_modifier(key: str, updates: dict) -> dict | None:
    existing = _db_get(key)
    if not existing:
        return None
    merged = {**existing, **updates}
    errors = validate_modifier(key, merged)
    if errors:
        return {"error": errors[0], "all_errors": errors}
    _db_upsert(key, merged)
    if "adds" in updates or "removes" in updates:
        rules = []
        for a in merged.get("adds", []):
            rules.append({**a, "action": "add"} if isinstance(a, dict) else a)
        for r in merged.get("removes", []):
            rules.append({"ingredient_key": r["ingredient_key"], "action": "remove"} if isinstance(r, dict) else r)
        _db_replace_rules(key, rules)
    return _db_get(key)


def delete_modifier(key: str) -> dict | None:
    existing = _db_get(key)
    if not existing:
        return None
    conn = get_conn()
    conn.execute("DELETE FROM modifier_rules WHERE modifier_key=?", (key,))
    conn.execute("DELETE FROM modifiers WHERE modifier_key=?", (key,))
    conn.commit()
    conn.close()
    return {**existing, "deleted": True}


def validate_modifier(key: str, data: dict) -> list[str]:
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
            errors.append(f"scale_factor must be positive (got {sf!r})")
    else:
        adds = data.get("adds", [])
        removes = data.get("removes", [])
        if mod_type == "add" and not adds:
            errors.append("'add' modifier must have at least one item in adds[]")
        if mod_type == "replace":
            if not adds:
                errors.append("'replace' modifier must have adds[]")
            if not removes:
                errors.append("'replace' modifier must have removes[]")
        seen = set()
        for i, a in enumerate(adds):
            ik = (a.get("ingredient_key", "") if isinstance(a, dict) else "").strip()
            if not ik:
                errors.append(f"adds[{i}]: missing ingredient_key")
            elif ik in seen:
                errors.append(f"adds[{i}]: duplicate '{ik}'")
            seen.add(ik)
    return errors
