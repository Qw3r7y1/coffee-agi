"""Sales MCP — Revenue, wholesale, retail, customer accounts, pricing, upsell."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Sales Department AI.
Responsibilities: retail and wholesale sales strategy, customer account management,
pricing optimization, upsell/cross-sell tactics, wholesale outreach, and revenue tracking.

Maillard menu pricing reference:
- Espresso $3.25 | Americano $3.50/4.30 | Latte $4.25/5.00 | Cappuccino $4.25/5.00
- Freddo Espresso $4.25/4.75 | Freddo Cappuccino $4.50/5.25
- Sweet Crepes $8.95 | Savory Crepes $13.95
Always upsell quality and craft over discounting.
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
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT)})
            case _:
                return self.err(f"Unknown tool: {name}")
