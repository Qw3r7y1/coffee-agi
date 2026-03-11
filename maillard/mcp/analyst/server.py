"""Analyst MCP — Data analysis, KPIs, reporting, forecasting."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Data Analyst AI.
Responsibilities: sales reporting, KPI dashboards, trend analysis, revenue forecasting,
menu performance analysis, customer behavior insights, and operational benchmarking.
Apply specialty coffee industry benchmarks. Be precise, visual in your data descriptions,
and always surface actionable insights, not just numbers.
"""

TOOLS: list[dict] = [
    {
        "name": "analyze_sales",
        "description": "Analyze sales data and surface key trends and insights.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "data": {"type": "object", "description": "Sales data as key-value pairs"},
                "period": {"type": "string"},
                "focus": {"type": "string", "enum": ["revenue", "volume", "margin", "product_mix", "all"]}
            },
            "required": ["data", "period"]
        }
    },
    {
        "name": "generate_report",
        "description": "Generate a business performance report for a given period and department.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "department": {"type": "string"},
                "period": {"type": "string"},
                "metrics": {"type": "array", "items": {"type": "string"}},
                "format": {"type": "string", "enum": ["executive_summary", "detailed", "dashboard"]}
            },
            "required": ["department", "period"]
        }
    },
    {
        "name": "forecast",
        "description": "Generate a revenue or demand forecast.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "metric": {"type": "string"},
                "historical_data": {"type": "object"},
                "horizon_weeks": {"type": "integer", "default": 4}
            },
            "required": ["metric", "historical_data"]
        }
    },
    {
        "name": "query_analyst",
        "description": "Answer a data or analytics question for Maillard.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class AnalystMCP(BaseMCPServer):
    department = "analyst"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "analyze_sales":
                prompt = f"Analyze this sales data for {arguments.get('period')} with focus on {arguments.get('focus', 'all')}:\n{arguments.get('data')}\nProvide key trends, insights, and recommendations."
                return self.ok({"analysis": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "generate_report":
                prompt = f"Generate a {arguments.get('format', 'executive_summary')} report for the {arguments.get('department')} department for {arguments.get('period')}. Key metrics: {arguments.get('metrics', [])}."
                return self.ok({"report": await ask(prompt, SYSTEM_PROMPT, max_tokens=2000)})
            case "forecast":
                prompt = f"Forecast '{arguments.get('metric')}' for the next {arguments.get('horizon_weeks', 4)} weeks based on: {arguments.get('historical_data')}."
                return self.ok({"forecast": await ask(prompt, SYSTEM_PROMPT)})
            case "query_analyst":
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT)})
            case _:
                return self.err(f"Unknown tool: {name}")
