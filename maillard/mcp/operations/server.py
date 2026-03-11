"""Operations MCP — Café operations, barista workflows, scheduling, recipes, equipment."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask
from maillard.mcp.shared import kb_client

SYSTEM_PROMPT = """
You are the Maillard Operations Department AI.
Responsibilities: café operations, barista workflow management, shift scheduling,
recipe execution standards, equipment maintenance, and quality control.

You have deep knowledge of Maillard's drink and food recipes:
- Espresso: 1:2.3 ratio, 22g → 50ml, 195–205F, 9–10 atm, 20–30s + 5s pre-infusion
- Steaming: 155–165F without extra air
- Frothing: introduce air first, then heat to 155–165F
- Full recipe guide in: data/maillard/recipes/Maillard coffee guide .pdf

When answering recipe or procedure questions, always reference the Maillard guide exactly.
Never improvise recipe specifications.
"""

TOOLS: list[dict] = [
    {
        "name": "get_recipe",
        "description": "Retrieve the exact Maillard recipe and execution steps for a drink or food item.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "item": {"type": "string", "description": "Drink or food item name (e.g. 'Freddo Cappuccino', 'Cold Brew', 'Overnight Oats')"}
            },
            "required": ["item"]
        }
    },
    {
        "name": "create_shift_schedule",
        "description": "Generate a shift schedule for the café.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "staff": {"type": "array", "items": {"type": "string"}},
                "week_start": {"type": "string"},
                "open_hours": {"type": "string"},
                "min_staff_per_shift": {"type": "integer", "default": 2}
            },
            "required": ["staff", "week_start"]
        }
    },
    {
        "name": "equipment_checklist",
        "description": "Generate an equipment maintenance or opening/closing checklist.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "checklist_type": {"type": "string", "enum": ["opening", "closing", "maintenance", "deep_clean"]},
                "equipment": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["checklist_type"]
        }
    },
    {
        "name": "query_operations",
        "description": "Answer an operations or café management question.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class OperationsMCP(BaseMCPServer):
    department = "operations"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "get_recipe":
                item = arguments.get("item", "")
                # Pull from knowledge base first
                docs = kb_client.search(item, n_results=3, topic_filter="maillard-recipes")
                if docs:
                    recipe_context = "\n\n".join(d["text"] for d in docs)
                    prompt = f"From the Maillard recipe guide, provide the exact recipe and execution steps for: {item}\n\nContext:\n{recipe_context}"
                else:
                    prompt = f"Provide the exact Maillard recipe and execution steps for: {item}"
                return self.ok({"item": item, "recipe": await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)})

            case "create_shift_schedule":
                prompt = f"Create a weekly shift schedule for the Maillard café starting {arguments.get('week_start')}. Staff: {arguments.get('staff')}. Hours: {arguments.get('open_hours', '7am-6pm')}. Min staff per shift: {arguments.get('min_staff_per_shift', 2)}."
                return self.ok({"schedule": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})

            case "equipment_checklist":
                prompt = f"Generate a {arguments.get('checklist_type')} checklist for a specialty coffee café. Equipment: {arguments.get('equipment', ['espresso machine', 'grinder', 'cold brew system', 'refrigerator'])}."
                return self.ok({"checklist": await ask(prompt, SYSTEM_PROMPT)})

            case "query_operations":
                query = arguments.get("query", "")
                docs = kb_client.search(query, n_results=3, topic_filter="maillard-recipes")
                context = "\n\n".join(d["text"] for d in docs) if docs else ""
                prompt = f"{query}\n\nRelevant Maillard context:\n{context}" if context else query
                return self.ok({"answer": await ask(prompt, SYSTEM_PROMPT)})

            case _:
                return self.err(f"Unknown tool: {name}")
