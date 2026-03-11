"""Accounting MCP — Financial tracking, invoicing, budgets, cost analysis."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Accounting Department AI.
Responsibilities: financial reporting, invoicing, budget tracking, cost-of-goods analysis,
cash flow management, tax compliance, and expense review.
Always apply specialty coffee industry standards for COGS (green coffee, labor, overhead).
Be precise, data-driven, and flag anomalies clearly.
"""

TOOLS: list[dict] = [
    {
        "name": "generate_invoice",
        "description": "Generate a formatted invoice for a customer order or wholesale account.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "client": {"type": "string"},
                "line_items": {"type": "array", "items": {"type": "object", "properties": {"description": {"type": "string"}, "qty": {"type": "number"}, "unit_price": {"type": "number"}}}},
                "due_days": {"type": "integer", "default": 30}
            },
            "required": ["client", "line_items"]
        }
    },
    {
        "name": "calculate_cogs",
        "description": "Calculate cost of goods sold for a menu item or product.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "product": {"type": "string"},
                "ingredients": {"type": "array", "items": {"type": "object"}},
                "labor_minutes": {"type": "number"},
                "overhead_rate": {"type": "number"}
            },
            "required": ["product", "ingredients"]
        }
    },
    {
        "name": "budget_analysis",
        "description": "Analyze budget vs actuals for a department or period.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "department": {"type": "string"},
                "period": {"type": "string"},
                "budget": {"type": "number"},
                "actual": {"type": "number"}
            },
            "required": ["department", "period", "budget", "actual"]
        }
    },
    {
        "name": "query_accounting",
        "description": "Answer a general accounting or finance question for Maillard.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class AccountingMCP(BaseMCPServer):
    department = "accounting"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "generate_invoice":
                prompt = f"Generate a professional invoice for {arguments.get('client')} with items: {arguments.get('line_items')}. Due in {arguments.get('due_days', 30)} days."
                return self.ok({"invoice": await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)})
            case "calculate_cogs":
                prompt = f"Calculate COGS for '{arguments.get('product')}' with ingredients: {arguments.get('ingredients')}, labor: {arguments.get('labor_minutes', 0)} min, overhead rate: {arguments.get('overhead_rate', 0)}."
                return self.ok({"cogs_analysis": await ask(prompt, SYSTEM_PROMPT)})
            case "budget_analysis":
                variance = arguments.get('actual', 0) - arguments.get('budget', 0)
                prompt = f"Budget analysis for {arguments.get('department')} ({arguments.get('period')}): Budget=${arguments.get('budget')}, Actual=${arguments.get('actual')}, Variance=${variance}. Provide analysis and recommendations."
                return self.ok({"analysis": await ask(prompt, SYSTEM_PROMPT)})
            case "query_accounting":
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT)})
            case _:
                return self.err(f"Unknown tool: {name}")
