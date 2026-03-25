"""Recipes repository — reads/writes data/coffee_agi.db."""
from __future__ import annotations
from app.core.db import get_conn, now_iso


def get_recipe(key: str) -> dict | None:
    conn = get_conn()
    recipe = conn.execute("SELECT * FROM recipes WHERE recipe_key=?", (key,)).fetchone()
    if not recipe:
        conn.close()
        return None
    ings = conn.execute("""
        SELECT ri.ingredient_key, ri.quantity, ri.unit,
               i.display_name AS ingredient_display, i.latest_unit_cost, i.cost_source
        FROM recipe_ingredients ri
        LEFT JOIN ingredients i ON ri.ingredient_key = i.ingredient_key
        WHERE ri.recipe_key=?
    """, (key,)).fetchall()
    conn.close()
    return {**dict(recipe), "ingredients": [dict(r) for r in ings]}


def list_recipes(status: str | None = None) -> list[dict]:
    conn = get_conn()
    if status:
        rows = conn.execute("SELECT * FROM recipes WHERE status=? ORDER BY recipe_key", (status,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM recipes ORDER BY recipe_key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recipe_ingredients(key: str) -> dict:
    """Return {ingredient_key: quantity} — matches old recipes.json format."""
    conn = get_conn()
    rows = conn.execute("SELECT ingredient_key, quantity FROM recipe_ingredients WHERE recipe_key=?", (key,)).fetchall()
    conn.close()
    return {r["ingredient_key"]: r["quantity"] for r in rows}


def get_all_recipes_dict() -> dict:
    """Return {recipe_key: {ingredient_key: qty}} for all approved recipes."""
    conn = get_conn()
    recipes = conn.execute("SELECT recipe_key FROM recipes WHERE status='approved'").fetchall()
    result = {}
    for r in recipes:
        rk = r["recipe_key"]
        ings = conn.execute("SELECT ingredient_key, quantity FROM recipe_ingredients WHERE recipe_key=?", (rk,)).fetchall()
        result[rk] = {i["ingredient_key"]: i["quantity"] for i in ings}
    conn.close()
    return result


def get_all_prices() -> dict:
    """Return {recipe_key: sell_price} for approved recipes."""
    conn = get_conn()
    rows = conn.execute("SELECT recipe_key, sell_price FROM recipes WHERE status='approved' AND sell_price > 0").fetchall()
    conn.close()
    return {r["recipe_key"]: r["sell_price"] for r in rows}


def create_recipe(key: str, display: str, status: str = "draft", sell_price: float = 0) -> dict:
    conn = get_conn()
    now = now_iso()
    conn.execute("""
        INSERT OR IGNORE INTO recipes (recipe_key, display_name, sell_price, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (key, display, sell_price, status, now, now))
    conn.commit()
    conn.close()
    return get_recipe(key)


def replace_recipe_ingredients(key: str, ingredients: list[dict]) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM recipe_ingredients WHERE recipe_key=?", (key,))
    for ing in ingredients:
        # Ensure ingredient exists
        conn.execute("""
            INSERT OR IGNORE INTO ingredients (ingredient_key, display_name, base_unit, updated_at)
            VALUES (?, ?, ?, ?)
        """, (ing["ingredient_key"], ing["ingredient_key"].replace("_", " ").title(),
              ing.get("unit", "ea"), now_iso()))
        conn.execute("""
            INSERT INTO recipe_ingredients (recipe_key, ingredient_key, quantity, unit)
            VALUES (?, ?, ?, ?)
        """, (key, ing["ingredient_key"], ing.get("quantity", 0), ing.get("unit", "ea")))
    conn.commit()
    conn.close()


def approve_recipe(key: str) -> dict | None:
    conn = get_conn()
    cur = conn.execute("UPDATE recipes SET status='approved', updated_at=? WHERE recipe_key=?", (now_iso(), key))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        return None
    return get_recipe(key)
