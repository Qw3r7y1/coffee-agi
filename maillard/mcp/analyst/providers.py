"""
Market data provider abstraction layer.

Decouples the intelligence system from any single API vendor.
Swap providers by changing MARKET_DATA_PROVIDER / FX_DATA_PROVIDER env vars.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import httpx
from loguru import logger

# ── Base interfaces ──────────────────────────────────────────────────────────


class MarketDataProvider(ABC):
    """Interface for commodity market data."""

    @abstractmethod
    async def get_price(self, symbol: str) -> dict:
        """Return {symbol, price, change_percent, timestamp, ...} or {error: ...}."""

    @abstractmethod
    async def get_history(self, symbol: str, days: int = 30) -> dict:
        """Return {symbol, bars: [{date, open, high, low, close}], count, ...} or {error: ...}."""


class FXDataProvider(ABC):
    """Interface for foreign exchange rate data."""

    @abstractmethod
    async def get_rate(self, pair: str) -> dict:
        """Return {pair, rate, timestamp, ...} or {error: ...}."""

    @abstractmethod
    async def get_rates(self, pairs: list[str]) -> list[dict]:
        """Fetch multiple FX pairs at once."""


# ── Twelve Data implementation ───────────────────────────────────────────────

TWELVEDATA_BASE = "https://api.twelvedata.com"
_TIMEOUT = 15


def _twelvedata_key() -> str:
    return os.getenv("TWELVEDATA_API_KEY", "")


class TwelveDataMarketProvider(MarketDataProvider):
    """Twelve Data API implementation.

    AUDIT RESULT (2026-03-18):
    Twelve Data does NOT carry ICE commodity futures. Their free tier only
    has: Common Stock, Depositary Receipt, ETF, Mutual Fund, and Forex pairs.

    Symbol "KC" resolves to Kingsoft Cloud Holdings Limited (NASDAQ ADR),
    a Chinese cloud computing company stock at ~$14.78/share.

    This CANNOT be fixed with a different symbol -- the platform does not
    have ICE Coffee C futures data at all. Confirmed via:
      - symbol_search("KC") -> 30 results, all stocks, zero futures
      - symbol_search("coffee") -> 15 results, all stocks/ETFs, zero futures
      - symbol_search("arabica") -> 1 result, not a future
      - KC/USD, CC/USD, SB/USD, CT/USD -> all "invalid symbol"
      - instrument_types available: Common Stock, DR, ETF, Mutual Fund only

    This provider is ONLY valid for FX rates (BRL/USD, COP/USD, EUR/USD).
    For coffee futures, use Yahoo Finance (KC=F) as the primary source.
    """

    async def get_price(self, symbol: str) -> dict:
        api_key = _twelvedata_key()
        if not api_key:
            logger.warning("[PROVIDER] TWELVEDATA_API_KEY not set")
            return {"error": "TWELVEDATA_API_KEY not configured."}

        now = datetime.now(timezone.utc).isoformat()
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                price_resp = await client.get(
                    f"{TWELVEDATA_BASE}/price",
                    params={"symbol": symbol, "apikey": api_key},
                )
                quote_resp = await client.get(
                    f"{TWELVEDATA_BASE}/quote",
                    params={"symbol": symbol, "apikey": api_key},
                )
                logger.info(
                    f"[PROVIDER] price={price_resp.status_code} quote={quote_resp.status_code} symbol={symbol}"
                )

            if price_resp.status_code != 200 or quote_resp.status_code != 200:
                return _unavailable()

            price_data = price_resp.json()
            quote_data = quote_resp.json()

            if "code" in price_data or "code" in quote_data:
                err = price_data.get("message") or quote_data.get("message") or "API error"
                logger.error(f"[PROVIDER] API error: {err}")
                return _unavailable()

            return {
                "symbol": symbol,
                "name": quote_data.get("name", symbol),
                "exchange": quote_data.get("exchange", ""),
                "price": float(price_data.get("price", 0)),
                "currency": quote_data.get("currency", "USD"),
                # Twelve Data KC = Kingsoft Cloud stock, NOT coffee.
                # raw_unit left as reported; the engine will detect the mismatch.
                "raw_unit": "unknown",
                "change_percent": float(quote_data.get("percent_change", 0)),
                "previous_close": float(quote_data.get("previous_close", 0)),
                "open": float(quote_data.get("open", 0)),
                "high": float(quote_data.get("high", 0)),
                "low": float(quote_data.get("low", 0)),
                "volume": quote_data.get("volume"),
                "timestamp": quote_data.get("datetime", now),
                "fetched_at": now,
                "source": "twelvedata",
            }
        except Exception as e:
            logger.error(f"[PROVIDER] market request failed: {e}")
            return _unavailable()

    async def get_history(self, symbol: str, days: int = 30) -> dict:
        api_key = _twelvedata_key()
        if not api_key:
            return {"error": "TWELVEDATA_API_KEY not configured."}

        now = datetime.now(timezone.utc).isoformat()
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(
                    f"{TWELVEDATA_BASE}/time_series",
                    params={
                        "symbol": symbol,
                        "interval": "1day",
                        "outputsize": min(days, 90),
                        "apikey": api_key,
                    },
                )
                logger.info(f"[PROVIDER] time_series symbol={symbol} → {resp.status_code}")

            if resp.status_code != 200:
                return _unavailable()

            data = resp.json()
            if "code" in data:
                logger.error(f"[PROVIDER] API error: {data.get('message', '')}")
                return _unavailable()

            values = data.get("values", [])
            bars = [
                {
                    "date": v.get("datetime"),
                    "open": float(v.get("open", 0)),
                    "high": float(v.get("high", 0)),
                    "low": float(v.get("low", 0)),
                    "close": float(v.get("close", 0)),
                }
                for v in values
            ]
            return {
                "symbol": symbol,
                "interval": "1day",
                "bars": bars,
                "count": len(bars),
                "fetched_at": now,
                "source": "twelvedata",
            }
        except Exception as e:
            logger.error(f"[PROVIDER] history request failed: {e}")
            return _unavailable()


class TwelveDataFXProvider(FXDataProvider):
    """Twelve Data API implementation for FX rates."""

    # Map friendly pair names to Twelve Data symbols
    PAIR_MAP: dict[str, str] = {
        "BRL/USD": "USD/BRL",  # Twelve Data uses USD as base; we invert
        "COP/USD": "USD/COP",
        "EUR/USD": "EUR/USD",
        "ETB/USD": "USD/ETB",
        "VND/USD": "USD/VND",
        "GBP/USD": "GBP/USD",
    }

    async def get_rate(self, pair: str) -> dict:
        api_key = _twelvedata_key()
        if not api_key:
            return {"error": "TWELVEDATA_API_KEY not configured."}

        now = datetime.now(timezone.utc).isoformat()
        td_symbol = self.PAIR_MAP.get(pair.upper(), pair.upper())
        invert = pair.upper() in ("BRL/USD", "COP/USD", "ETB/USD", "VND/USD")

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.get(
                    f"{TWELVEDATA_BASE}/quote",
                    params={"symbol": td_symbol, "apikey": api_key},
                )
                logger.info(f"[PROVIDER] FX {td_symbol} → {resp.status_code}")

            if resp.status_code != 200:
                return {"error": f"FX data unavailable for {pair}."}

            data = resp.json()
            if "code" in data:
                logger.error(f"[PROVIDER] FX error: {data.get('message', '')}")
                return {"error": f"FX data unavailable for {pair}."}

            raw_rate = float(data.get("close") or data.get("price") or 0)
            raw_change = float(data.get("percent_change", 0))
            if invert and raw_rate != 0:
                rate = 1.0 / raw_rate
                # Inverting the rate flips the direction of the change
                change_pct = -raw_change
            else:
                rate = raw_rate
                change_pct = raw_change

            return {
                "pair": pair.upper(),
                "rate": round(rate, 6),
                "change_percent": round(change_pct, 2),
                "timestamp": data.get("datetime", now),
                "fetched_at": now,
                "source": "twelvedata",
            }
        except Exception as e:
            logger.error(f"[PROVIDER] FX request failed for {pair}: {e}")
            return {"error": f"FX data unavailable for {pair}."}

    async def get_rates(self, pairs: list[str]) -> list[dict]:
        import asyncio
        results = await asyncio.gather(*[self.get_rate(p) for p in pairs])
        return list(results)


# ── Yahoo Finance implementation ─────────────────────────────────────────────
# No API key required. Uses the public chart endpoint.
# Coffee C futures symbol: KC=F

_YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
_YAHOO_HEADERS = {"User-Agent": "MaillardIntelligenceBot/1.0"}

# Map our internal symbols to Yahoo Finance symbols
_YAHOO_SYMBOL_MAP = {
    "KC": "KC=F",   # ICE Coffee C Futures
}


class YahooFinanceMarketProvider(MarketDataProvider):
    """Yahoo Finance implementation for commodity futures. No API key needed."""

    async def get_price(self, symbol: str) -> dict:
        yahoo_sym = _YAHOO_SYMBOL_MAP.get(symbol, symbol)
        now = datetime.now(timezone.utc).isoformat()

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(
                    f"{_YAHOO_BASE}/{yahoo_sym}",
                    params={"interval": "1d", "range": "2d"},
                    headers=_YAHOO_HEADERS,
                )
                logger.info(f"[PROVIDER-YAHOO] {yahoo_sym} → {resp.status_code}")

            if resp.status_code != 200:
                logger.error(f"[PROVIDER-YAHOO] HTTP {resp.status_code}")
                return _unavailable()

            data = resp.json()
            results = data.get("chart", {}).get("result", [])
            if not results:
                logger.error("[PROVIDER-YAHOO] empty result set")
                return _unavailable()

            meta = results[0].get("meta", {})
            price = meta.get("regularMarketPrice")
            if price is None:
                logger.error("[PROVIDER-YAHOO] no regularMarketPrice in response")
                return _unavailable()

            price = float(price)
            prev_close = float(meta.get("chartPreviousClose") or meta.get("previousClose") or 0)
            change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0

            ts_unix = meta.get("regularMarketTime", 0)
            ts_str = (
                datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()
                if ts_unix else now
            )

            # Yahoo KC=F returns currency=USX (US cents).
            # The raw price is in CENTS per pound.
            reported_currency = meta.get("currency", "USX")

            return {
                "symbol": symbol,
                "name": meta.get("shortName") or meta.get("longName") or yahoo_sym,
                "exchange": meta.get("exchangeName", ""),
                "price": price,
                "currency": reported_currency,
                # Tag raw unit: USX = cents, USD = dollars
                "raw_unit": "cents/lb" if reported_currency.upper() == "USX" else "$/lb",
                "change_percent": round(change_pct, 2),
                "previous_close": prev_close,
                "open": float(meta.get("regularMarketOpen") or meta.get("open") or 0),
                "high": float(meta.get("regularMarketDayHigh") or 0),
                "low": float(meta.get("regularMarketDayLow") or 0),
                "volume": meta.get("regularMarketVolume"),
                "timestamp": ts_str,
                "fetched_at": now,
                "source": "yahoo_finance",
            }

        except Exception as e:
            logger.error(f"[PROVIDER-YAHOO] request failed: {e}")
            return _unavailable()

    async def get_history(self, symbol: str, days: int = 30) -> dict:
        yahoo_sym = _YAHOO_SYMBOL_MAP.get(symbol, symbol)
        now = datetime.now(timezone.utc).isoformat()

        # Yahoo range strings: 5d, 1mo, 3mo
        if days <= 5:
            range_str = "5d"
        elif days <= 30:
            range_str = "1mo"
        else:
            range_str = "3mo"

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(
                    f"{_YAHOO_BASE}/{yahoo_sym}",
                    params={"interval": "1d", "range": range_str},
                    headers=_YAHOO_HEADERS,
                )
                logger.info(f"[PROVIDER-YAHOO] history {yahoo_sym} {range_str} → {resp.status_code}")

            if resp.status_code != 200:
                return _unavailable()

            data = resp.json()
            results = data.get("chart", {}).get("result", [])
            if not results:
                return _unavailable()

            timestamps = results[0].get("timestamp", [])
            quote = results[0].get("indicators", {}).get("quote", [{}])[0]
            opens = quote.get("open", [])
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            closes = quote.get("close", [])

            bars = []
            for i, ts in enumerate(timestamps):
                c = closes[i] if i < len(closes) else None
                if c is None:
                    continue  # skip bars with no close
                bars.append({
                    "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                    "open": float(opens[i]) if i < len(opens) and opens[i] else 0,
                    "high": float(highs[i]) if i < len(highs) and highs[i] else 0,
                    "low": float(lows[i]) if i < len(lows) and lows[i] else 0,
                    "close": float(c),
                })

            return {
                "symbol": symbol,
                "interval": "1day",
                "bars": bars,
                "count": len(bars),
                "fetched_at": now,
                "source": "yahoo_finance",
            }

        except Exception as e:
            logger.error(f"[PROVIDER-YAHOO] history request failed: {e}")
            return _unavailable()


# ── Factory ──────────────────────────────────────────────────────────────────

def get_market_provider() -> MarketDataProvider:
    """Return the configured market data provider.

    Default: Yahoo Finance (KC=F = ICE Coffee C Futures, correct).

    Twelve Data is NOT valid for coffee futures (audit 2026-03-18):
    KC -> Kingsoft Cloud Holdings (NASDAQ), not ICE Coffee C.
    If explicitly configured, a warning is logged.
    """
    provider = os.getenv("MARKET_DATA_PROVIDER", "yahoo").lower()
    if provider in ("yahoo", "yahoo_finance"):
        return YahooFinanceMarketProvider()
    if provider == "twelvedata":
        logger.warning(
            "[PROVIDER] Twelve Data selected but DOES NOT carry ICE coffee futures. "
            "KC resolves to Kingsoft Cloud (NASDAQ). Data will fail validation."
        )
        return TwelveDataMarketProvider()
    logger.warning(f"[PROVIDER] Unknown provider '{provider}', falling back to Yahoo Finance")
    return YahooFinanceMarketProvider()


def get_fx_provider() -> FXDataProvider:
    """Return the configured FX data provider."""
    provider = os.getenv("FX_DATA_PROVIDER", "twelvedata").lower()
    if provider == "twelvedata":
        return TwelveDataFXProvider()
    logger.warning(f"[PROVIDER] Unknown FX provider '{provider}', falling back to Twelve Data")
    return TwelveDataFXProvider()


# ── Shared helpers ───────────────────────────────────────────────────────────

def _unavailable() -> dict:
    return {"error": "Live market data is currently unavailable. Please try again."}
