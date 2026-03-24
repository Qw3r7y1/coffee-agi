"""
Analyst MCP -- Coffee market intelligence for Maillard Coffee Roasters.

Simple, practical, decision-focused.

Agent tools (5):
  buying_signal   -- should I buy now? (primary interface)
  market_snapshot -- current price + FX + validation
  fetch_url       -- read a pasted URL (Investing.com reference)
  query_analyst   -- catch-all with auto-injected market data
  executive_brief -- full market brief (when more detail needed)
"""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask
from maillard.mcp.analyst.market_data import (
    get_coffee_market_data,
    get_coffee_futures_quote,
    get_coffee_futures_history,
    get_market_snapshot,
)
from maillard.mcp.analyst.fx_data import get_fx_rate, get_coffee_fx_rates
from maillard.mcp.analyst.intelligence import (
    generate_executive_market_brief,
    estimate_green_cost_pressure,
    estimate_menu_margin_pressure,
)
from maillard.mcp.analyst.url_tools import extract_market_relevant_text, extract_urls
from maillard.mcp.analyst.buying_signal import get_buying_signal
from maillard.mcp.analyst.market_data_engine import (
    get_validated_coffee_data,
    STATE_VALIDATED, STATE_WARNING, STATE_FEED_CONFLICT, STATE_INVALID_DATA,
    MODE_SAFE,
)
from maillard.models.storage import save_market_snapshot, save_fx_snapshot, save_intelligence_report
from loguru import logger

SYSTEM_PROMPT = """
You are the Maillard Market Analyst -- a simple, practical buying advisor
for a coffee roastery owner.

You help answer three questions:
1. Should I buy coffee now or wait?
2. Are prices going up or down?
3. Is there risk in delaying purchases?

Rules:
- Keep it short. Owner reads this in 15 seconds.
- No financial jargon. No macroeconomic essays.
- Always show: direction, confidence, recommendation, reason.
- Use real numbers from the data, never estimate.
- If data quality is poor, say so clearly and recommend MONITOR.
"""

TOOLS: list[dict] = [
    {
        "name": "buying_signal",
        "description": "Should I buy coffee now? Returns market direction (UP/DOWN/STABLE), confidence, and BUY NOW/WAIT/MONITOR recommendation. Use for 'should I buy', 'what is the market doing', 'coffee price'.",
        "inputSchema": {
            "type": "object",
            "properties": {"days": {"type": "integer", "default": 14, "description": "Trend period in days"}},
            "required": []
        }
    },
    {
        "name": "market_snapshot",
        "description": "Current coffee price + FX rates + data validation status.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "fetch_url",
        "description": "Read a pasted URL (e.g., Investing.com) and extract price data as reference benchmark.",
        "inputSchema": {
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"]
        }
    },
    {
        "name": "executive_brief",
        "description": "Full market intelligence brief with margins, cost pressure, and procurement risk. Use when owner wants more detail than the buying signal.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "query_analyst",
        "description": "Answer any market, pricing, or sourcing question. Auto-fetches live data and validates sources.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    },
]

_MARKET_KW = [
    "coffee", "futures", "price", "arabica", "market", "buy", "sell",
    "cost", "trend", "supplier", "margin", "wholesale", "procurement",
    "should i", "should we", "when to buy",
]
_FX_KW = ["fx", "exchange rate", "brl", "cop", "eur/usd", "currency", "euro", "dollar"]

_FEED_UNAVAILABLE = "Live market feed currently unavailable. Please try again shortly."


class AnalystMCP(BaseMCPServer):
    department = "analyst"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "buying_signal":
                days = arguments.get("days", 14)
                signal = await get_buying_signal(days=days)
                return self.ok({"answer": signal["formatted"], "signal": signal})

            case "market_snapshot":
                result = await get_market_snapshot()
                coffee = result.get("coffee_futures", {})
                if "error" not in coffee:
                    save_market_snapshot(coffee)
                for _, fx in result.get("fx_rates", {}).items():
                    save_fx_snapshot(fx)
                report = result.get("integrity_report", "")
                return self.ok({"answer": report, "snapshot": result})

            case "fetch_url":
                url = arguments.get("url", "")
                result = await extract_market_relevant_text(url)
                if "error" in result:
                    return self.ok({"answer": f"Could not read URL: {result['error']}"})

                # If reference source, cross-validate
                if result.get("is_reference_source") and result.get("extracted_price"):
                    ref = result["extracted_price"]
                    market = await get_coffee_market_data(reference_price_cents=ref)
                    report = market.get("integrity_report", "")
                    return self.ok({
                        "answer": f"Reference price: {ref} c/lb\n\n{report}",
                        "url_result": result,
                        "market_data": market,
                    })

                prompt = (
                    f"[URL] {result['url']}\nTitle: {result.get('title', 'N/A')}\n"
                    f"Content:\n{result.get('content', '')[:2000]}\n\n"
                    f"Summarize for the Maillard owner. Keep it short."
                )
                answer = await ask(prompt, SYSTEM_PROMPT, max_tokens=800)
                return self.ok({"answer": answer, "url_result": result})

            case "executive_brief":
                result = await generate_executive_market_brief()
                if "error" in result:
                    return self.ok({"answer": _FEED_UNAVAILABLE})
                save_intelligence_report(
                    "executive_brief", "Executive Brief",
                    result["brief"][:500],
                    {"trend_30d": result.get("trend_30d")}
                )
                return self.ok({"answer": result["brief"], "data": result})

            case "query_analyst":
                return await self._handle_query(arguments.get("query", ""))

            case _:
                return self.err(f"Unknown tool: {name}")

    async def _handle_query(self, query: str) -> dict:
        """Catch-all: auto-inject market data for any question."""
        lower = query.lower()
        urls = extract_urls(query)
        ctx: list[str] = []
        tools_used = ["query_analyst"]

        # Fetch URLs
        ref_price = None
        for url in urls[:2]:
            result = await extract_market_relevant_text(url)
            if "error" in result:
                continue
            if result.get("is_reference_source") and result.get("extracted_price"):
                ref_price = result["extracted_price"]
                ctx.append(f"[REFERENCE] {ref_price} c/lb from {result['url']}")
            else:
                ctx.append(f"[URL] {result.get('title', 'N/A')}: {result.get('content', '')[:1000]}")
            tools_used.append("fetch_url")

        # Market data
        if any(kw in lower for kw in _MARKET_KW) or ref_price:
            market = await get_coffee_market_data(reference_price_cents=ref_price)
            ctx.append(market.get("integrity_report", ""))
            tools_used.append("market_data")

            if market.get("api_data") and "error" not in market["api_data"]:
                save_market_snapshot(market["api_data"])

        # FX
        if any(kw in lower for kw in _FX_KW):
            fx = await get_coffee_fx_rates()
            rates = fx.get("rates", {})
            if rates:
                for _, info in rates.items():
                    save_fx_snapshot(info)
                ctx.append("[FX]\n" + "\n".join(
                    f"{p}: {info['rate']:.4f}" for p, info in rates.items()
                ))
                tools_used.append("fx_rates")

        enriched = query + ("\n\n" + "\n\n".join(ctx) if ctx else "")
        answer = await ask(enriched, SYSTEM_PROMPT)
        return self.ok({"answer": answer, "tool_used": "+".join(tools_used)})
