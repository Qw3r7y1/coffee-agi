"""
SQLAlchemy models for market and FX snapshots and intelligence reports.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, JSON
from maillard.models.database import Base


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False, index=True)
    price = Column(Float, nullable=False)
    change_percent = Column(Float)
    source = Column(String(50), default="twelvedata")
    raw_payload = Column(JSON)
    fetched_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<MarketSnapshot {self.symbol}={self.price} @ {self.fetched_at}>"


class FXSnapshot(Base):
    __tablename__ = "fx_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pair = Column(String(10), nullable=False, index=True)
    rate = Column(Float, nullable=False)
    source = Column(String(50), default="twelvedata")
    raw_payload = Column(JSON)
    fetched_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<FXSnapshot {self.pair}={self.rate} @ {self.fetched_at}>"


class IntelligenceReport(Base):
    __tablename__ = "intelligence_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_type = Column(String(50), nullable=False, index=True)
    title = Column(String(200))
    summary = Column(Text)
    details = Column(JSON)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<IntelligenceReport {self.report_type}: {self.title}>"
