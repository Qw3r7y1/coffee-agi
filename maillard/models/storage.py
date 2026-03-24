"""
Persistence helpers — save snapshots and reports to the database.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger

from maillard.models.database import SessionLocal
from maillard.models.snapshots import MarketSnapshot, FXSnapshot, IntelligenceReport


def save_market_snapshot(data: dict) -> None:
    """Persist a market data snapshot."""
    try:
        with SessionLocal() as session:
            snap = MarketSnapshot(
                symbol=data.get("symbol", ""),
                price=data.get("price", 0),
                change_percent=data.get("change_percent"),
                source=data.get("source", "unknown"),
                raw_payload=data,
                fetched_at=datetime.now(timezone.utc),
            )
            session.add(snap)
            session.commit()
    except Exception as e:
        logger.warning(f"[STORAGE] Failed to save market snapshot: {e}")


def save_fx_snapshot(data: dict) -> None:
    """Persist an FX rate snapshot."""
    try:
        with SessionLocal() as session:
            snap = FXSnapshot(
                pair=data.get("pair", ""),
                rate=data.get("rate", 0),
                source=data.get("source", "unknown"),
                raw_payload=data,
                fetched_at=datetime.now(timezone.utc),
            )
            session.add(snap)
            session.commit()
    except Exception as e:
        logger.warning(f"[STORAGE] Failed to save FX snapshot: {e}")


def save_intelligence_report(
    report_type: str, title: str, summary: str, details: dict | None = None
) -> None:
    """Persist an intelligence report."""
    try:
        with SessionLocal() as session:
            report = IntelligenceReport(
                report_type=report_type,
                title=title,
                summary=summary,
                details=details,
                created_at=datetime.now(timezone.utc),
            )
            session.add(report)
            session.commit()
    except Exception as e:
        logger.warning(f"[STORAGE] Failed to save intelligence report: {e}")


def get_recent_market_snapshots(symbol: str = "KC", limit: int = 50) -> list[dict]:
    """Fetch recent market snapshots for a symbol."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(MarketSnapshot)
                .filter(MarketSnapshot.symbol == symbol)
                .order_by(MarketSnapshot.fetched_at.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "id": r.id,
                    "symbol": r.symbol,
                    "price": r.price,
                    "change_percent": r.change_percent,
                    "source": r.source,
                    "fetched_at": r.fetched_at.isoformat() if r.fetched_at else None,
                }
                for r in rows
            ]
    except Exception as e:
        logger.warning(f"[STORAGE] Failed to read market snapshots: {e}")
        return []


def get_recent_fx_snapshots(pair: str, limit: int = 50) -> list[dict]:
    """Fetch recent FX snapshots for a pair."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(FXSnapshot)
                .filter(FXSnapshot.pair == pair)
                .order_by(FXSnapshot.fetched_at.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "id": r.id,
                    "pair": r.pair,
                    "rate": r.rate,
                    "source": r.source,
                    "fetched_at": r.fetched_at.isoformat() if r.fetched_at else None,
                }
                for r in rows
            ]
    except Exception as e:
        logger.warning(f"[STORAGE] Failed to read FX snapshots: {e}")
        return []


def get_recent_reports(report_type: str | None = None, limit: int = 20) -> list[dict]:
    """Fetch recent intelligence reports."""
    try:
        with SessionLocal() as session:
            q = session.query(IntelligenceReport)
            if report_type:
                q = q.filter(IntelligenceReport.report_type == report_type)
            rows = q.order_by(IntelligenceReport.created_at.desc()).limit(limit).all()
            return [
                {
                    "id": r.id,
                    "report_type": r.report_type,
                    "title": r.title,
                    "summary": r.summary[:500] if r.summary else None,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in rows
            ]
    except Exception as e:
        logger.warning(f"[STORAGE] Failed to read reports: {e}")
        return []
