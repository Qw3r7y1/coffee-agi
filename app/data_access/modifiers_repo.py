"""Modifiers repository — reads/writes data/coffee_agi.db."""
from __future__ import annotations
from app.core.db import get_conn, now_iso


def get_modifier(key: str) -> dict | None:
    conn = get_conn()
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


def list_modifiers() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM modifiers ORDER BY modifier_key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_modifiers_dict() -> dict:
    """Return all modifiers in the format cost_engine expects."""
    conn = get_conn()
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
        entry: dict = {"display": m["display_name"], "type": m["type"], "upcharge": m["upcharge"]}
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


def upsert_modifier(key: str, data: dict) -> dict:
    conn = get_conn()
    now = now_iso()
    conn.execute("""
        INSERT INTO modifiers (modifier_key, display_name, type, upcharge, scale_factor, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, 'approved', ?, ?)
        ON CONFLICT(modifier_key) DO UPDATE SET
            display_name=excluded.display_name, type=excluded.type,
            upcharge=excluded.upcharge, scale_factor=excluded.scale_factor,
            updated_at=excluded.updated_at
    """, (key, data.get("display", key), data.get("type", "add"),
          data.get("upcharge", 0), data.get("scale_factor"), now, now))
    conn.commit()
    conn.close()
    return get_modifier(key)


def replace_modifier_rules(key: str, rules: list[dict]) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM modifier_rules WHERE modifier_key=?", (key,))
    for r in rules:
        conn.execute("""
            INSERT INTO modifier_rules (modifier_key, ingredient_key, action, quantity, unit, quantity_from_removed)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (key, r["ingredient_key"], r.get("action", "add"),
              r.get("quantity"), r.get("unit", "ea"),
              1 if r.get("quantity_from_removed") else 0))
    conn.commit()
    conn.close()
