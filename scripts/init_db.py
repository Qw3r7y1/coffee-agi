"""
Initialize data/coffee_agi.db with the full schema.
Safe to run repeatedly — uses CREATE TABLE IF NOT EXISTS.

Usage:
  python scripts/init_db.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.db import init_db, DB_PATH

if __name__ == "__main__":
    init_db()
    print(f"DB initialized: {DB_PATH} ({DB_PATH.stat().st_size} bytes)")
