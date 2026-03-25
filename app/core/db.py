"""
Central database connection for Coffee AGI.

Single SQLite file: data/coffee_agi.db
All repos import get_conn() from here.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "coffee_agi.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    """Create all tables. Safe to call repeatedly."""
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_key   TEXT PRIMARY KEY,
            display_name     TEXT NOT NULL,
            base_unit        TEXT NOT NULL DEFAULT 'ea',
            latest_unit_cost REAL,
            cost_source      TEXT,
            vendor_name      TEXT,
            invoice_date     TEXT,
            updated_at       TEXT
        );

        CREATE TABLE IF NOT EXISTS recipes (
            recipe_key   TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            sell_price   REAL NOT NULL DEFAULT 0,
            status       TEXT NOT NULL DEFAULT 'draft',
            created_at   TEXT,
            updated_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_key     TEXT NOT NULL REFERENCES recipes(recipe_key) ON DELETE CASCADE,
            ingredient_key TEXT NOT NULL REFERENCES ingredients(ingredient_key),
            quantity       REAL NOT NULL,
            unit           TEXT NOT NULL,
            UNIQUE(recipe_key, ingredient_key)
        );
        CREATE INDEX IF NOT EXISTS ix_ri_recipe ON recipe_ingredients(recipe_key);

        CREATE TABLE IF NOT EXISTS modifiers (
            modifier_key TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            type         TEXT NOT NULL DEFAULT 'add',
            upcharge     REAL DEFAULT 0,
            scale_factor REAL,
            status       TEXT NOT NULL DEFAULT 'approved',
            created_at   TEXT,
            updated_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS modifier_rules (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            modifier_key   TEXT NOT NULL REFERENCES modifiers(modifier_key) ON DELETE CASCADE,
            ingredient_key TEXT NOT NULL,
            action         TEXT NOT NULL DEFAULT 'add',
            quantity       REAL,
            unit           TEXT NOT NULL DEFAULT 'ea',
            quantity_from_removed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(modifier_key, action, ingredient_key)
        );
        CREATE INDEX IF NOT EXISTS ix_mr_mod ON modifier_rules(modifier_key);

        CREATE TABLE IF NOT EXISTS invoices (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor         TEXT,
            invoice_date   TEXT,
            invoice_number TEXT,
            total          REAL,
            source_file    TEXT,
            created_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS invoice_items (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id       INTEGER NOT NULL REFERENCES invoices(id),
            raw_name         TEXT,
            normalized_name  TEXT,
            quantity         REAL,
            unit             TEXT,
            price_basis      TEXT,
            unit_price       REAL,
            line_total       REAL,
            override_source  TEXT,
            confidence       TEXT,
            review_required  INTEGER DEFAULT 0,
            pack_count       INTEGER,
            pack_size_text   TEXT,
            base_unit        TEXT,
            total_base_units REAL,
            derived_unit_cost REAL
        );
        CREATE INDEX IF NOT EXISTS ix_ii_inv ON invoice_items(invoice_id);
        CREATE INDEX IF NOT EXISTS ix_ii_norm ON invoice_items(normalized_name);
    """)
    conn.close()
