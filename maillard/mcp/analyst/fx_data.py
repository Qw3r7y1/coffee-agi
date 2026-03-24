"""
FX rate tools for the Analyst intelligence system.

Provides live exchange rate data for currencies relevant to coffee sourcing:
- BRL/USD — Brazil (largest Arabica producer)
- COP/USD — Colombia
- EUR/USD — European wholesale market
- ETB/USD — Ethiopia (specialty origins)
"""
from __future__ import annotations

from loguru import logger

from maillard.mcp.analyst.providers import get_fx_provider

# Default pairs relevant to Maillard's supply chain
COFFEE_FX_PAIRS = ["BRL/USD", "COP/USD", "EUR/USD"]


async def get_fx_rate(pair: str) -> dict:
    """Fetch a single FX rate."""
    provider = get_fx_provider()
    result = await provider.get_rate(pair)
    logger.info(f"[FX] {pair} → {'error' if 'error' in result else result.get('rate')}")
    return result


async def get_coffee_fx_rates() -> dict:
    """Fetch all coffee-relevant FX rates in one call."""
    provider = get_fx_provider()
    results = await provider.get_rates(COFFEE_FX_PAIRS)
    rates = {}
    errors = []
    for r in results:
        if "error" in r:
            errors.append(r["error"])
        else:
            rates[r["pair"]] = r
    return {
        "rates": rates,
        "errors": errors if errors else None,
        "pairs_requested": COFFEE_FX_PAIRS,
    }
