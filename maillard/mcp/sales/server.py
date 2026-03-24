"""Sales MCP — Revenue, wholesale, retail, customer accounts, pricing, upsell."""
from __future__ import annotations
from typing import Any
from loguru import logger
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Sales Department AI.
Responsibilities: retail and wholesale sales strategy, customer account management,
pricing optimization, upsell/cross-sell tactics, wholesale outreach, and revenue tracking.

STRICT RULES:
- For questions about today's sales, orders, or top items: use ONLY the live data provided.
- NEVER invent sales numbers, order counts, or revenue figures.
- NEVER fabricate product names or margin percentages.
- If the data says status=no_live_data, say exactly: "Live sales data is not currently loaded."
- Do NOT estimate, approximate, or guess when live data is missing.
"""

TOOLS: list[dict] = [
    {
        "name": "generate_sales_proposal",
        "description": "Generate a wholesale or B2B sales proposal.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "client_name": {"type": "string"},
                "client_type": {"type": "string", "enum": ["restaurant", "hotel", "office", "retail", "cafe"]},
                "volume_estimate": {"type": "string"},
                "products": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["client_name", "client_type"]
        }
    },
    {
        "name": "pricing_analysis",
        "description": "Analyze current pricing or suggest pricing for a new product.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "product": {"type": "string"},
                "cogs": {"type": "number"},
                "competitor_price": {"type": "number"},
                "target_margin": {"type": "number", "default": 0.65}
            },
            "required": ["product", "cogs"]
        }
    },
    {
        "name": "upsell_script",
        "description": "Generate barista upsell scripts for specific products.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "base_order": {"type": "string"},
                "upsell_target": {"type": "string"},
                "context": {"type": "string", "enum": ["morning_rush", "slow_period", "new_customer", "regular"]}
            },
            "required": ["base_order", "upsell_target"]
        }
    },
    {
        "name": "query_sales",
        "description": "Answer a sales or revenue question.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class SalesMCP(BaseMCPServer):
    department = "sales"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "generate_sales_proposal":
                prompt = f"Write a premium wholesale sales proposal for {arguments.get('client_name')} ({arguments.get('client_type')}). Estimated volume: {arguments.get('volume_estimate', 'TBD')}. Products: {arguments.get('products', ['specialty coffee'])}. Position Maillard as a premium specialty roaster."
                return self.ok({"proposal": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "pricing_analysis":
                margin = arguments.get('target_margin', 0.65)
                suggested = arguments.get('cogs', 0) / (1 - margin)
                prompt = f"Pricing analysis for '{arguments.get('product')}': COGS=${arguments.get('cogs')}, competitor=${arguments.get('competitor_price', 'N/A')}, target margin={margin*100:.0f}%, suggested price=${suggested:.2f}. Recommend a pricing strategy."
                return self.ok({"suggested_price": round(suggested, 2), "analysis": await ask(prompt, SYSTEM_PROMPT)})
            case "upsell_script":
                prompt = f"Write a natural barista upsell script for a customer ordering '{arguments.get('base_order')}' to try '{arguments.get('upsell_target')}'. Context: {arguments.get('context', 'general')}. Keep it brief, genuine, and premium — never pushy."
                return self.ok({"script": await ask(prompt, SYSTEM_PROMPT)})
            case "query_sales":
                return await self._handle_query(arguments.get("query", ""))
            case _:
                return self.err(f"Unknown tool: {name}")

    async def _handle_query(self, query: str) -> dict:
        """Return real sales intelligence. No fallback to LLM for analytics."""
        try:
            from maillard.mcp.sales.intelligence import generate_sales_intelligence
            intel = generate_sales_intelligence()

            # If no live data, return the block — do NOT pass to Claude
            if intel.get("status") == "no_live_data":
                logger.warning("[SALES] No live data — refusing to generate analytics")
                return self.ok(intel)

            lower = query.lower()
            if any(kw in lower for kw in [
                "top", "best", "push", "sell", "product", "what to",
                "deprioritize", "promote", "focus", "order", "revenue",
                "today", "sales",
            ]):
                return self.ok({"answer": intel["formatted"], "sales_intelligence": intel})

            # Non-analytics sales question: enrich Claude with verified data
            context = intel["formatted"]
            answer = await ask(
                f"{query}\n\nVerified live sales data:\n{context}",
                SYSTEM_PROMPT,
            )
            return self.ok({"answer": answer, "sales_intelligence": intel})

        except Exception as e:
            logger.error(f"[SALES] Intelligence generation failed: {e}")
            return self.ok({
                "status": "no_live_data",
                "message": f"Sales intelligence unavailable: {e}",
            })
