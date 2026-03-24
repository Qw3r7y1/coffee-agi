"""
Database engine, session, and base model setup.

Uses SQLite by default (data/maillard.db) — no external DB required.
Override with DATABASE_URL env var for Postgres etc.
"""
from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

_DB_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_DB_DIR.mkdir(parents=True, exist_ok=True)

_DEFAULT_DB = (_DB_DIR / "maillard.db").as_posix()
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{_DEFAULT_DB}")

engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def init_db() -> None:
    """Create all tables if they don't exist. Safe to call repeatedly.

    Imports model modules explicitly so their tables are registered
    with Base.metadata regardless of caller import order.
    """
    import maillard.models.snapshots      # noqa: F401 — registers models with Base
    import maillard.models.operations     # noqa: F401 — inventory, production, wholesale
    Base.metadata.create_all(bind=engine)


def get_session():
    """Yield a database session, auto-closed after use."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
