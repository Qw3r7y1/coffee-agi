"""Procurement MCP — Supplier management, green coffee sourcing, purchasing, inventory."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Procurement Department AI.
Responsibilities: green coffee sourcing, supplier evaluation, purchase orders,
inventory management, vendor negotiations, and supply chain optimization.
Deep knowledge of: specialty coffee origins (Ethiopia, Colombia, Brazil, Guatemala),
processing methods, grading (SCA standards), import regulations, and seasonal availability.
Always prioritize quality, traceability, and direct-trade relationships.
"""

TOOLS: list[dict] = [
    {
        "name": "evaluate_supplier",
        "description": "Evaluate a coffee supplier or vendor against Maillard quality standards.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "supplier_name": {"type": "string"},
                "origin": {"type": "string"},
                "process": {"type": "string", "enum": ["washed", "natural", "honey", "anaerobic", "other"]},
                "cupping_score": {"type": "number"},
                "price_per_lb": {"type": "number"},
                "certifications": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["supplier_name", "origin"]
        }
    },
    {
        "name": "create_purchase_order",
        "description": "Create a purchase order for coffee or café supplies.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "supplier": {"type": "string"},
                "items": {"type": "array", "items": {"type": "object"}},
                "delivery_date": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["supplier", "items"]
        }
    },
    {
        "name": "sourcing_brief",
        "description": "Generate a green coffee sourcing brief for a specific flavor profile or market need.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "flavor_profile": {"type": "string"},
                "roast_target": {"type": "string", "enum": ["light", "medium", "dark", "espresso", "filter"]},
                "volume_kg": {"type": "number"},
                "budget_per_kg": {"type": "number"}
            },
            "required": ["flavor_profile", "roast_target"]
        }
    },
    {
        "name": "query_procurement",
        "description": "Answer a procurement or sourcing question.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class ProcurementMCP(BaseMCPServer):
    department = "procurement"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "evaluate_supplier":
                prompt = f"Evaluate supplier '{arguments.get('supplier_name')}' from {arguments.get('origin')}. Process: {arguments.get('process', 'N/A')}, Score: {arguments.get('cupping_score', 'N/A')}, Price: ${arguments.get('price_per_lb', 'N/A')}/lb. Certifications: {arguments.get('certifications', [])}. Provide evaluation against specialty coffee standards."
                return self.ok({"evaluation": await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)})
            case "create_purchase_order":
                prompt = f"Create a purchase order for {arguments.get('supplier')}. Items: {arguments.get('items')}. Delivery: {arguments.get('delivery_date', 'ASAP')}. Notes: {arguments.get('notes', '')}."
                return self.ok({"purchase_order": await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)})
            case "sourcing_brief":
                prompt = f"Generate a green coffee sourcing brief. Target profile: {arguments.get('flavor_profile')}. Roast: {arguments.get('roast_target')}. Volume: {arguments.get('volume_kg', 'TBD')}kg. Budget: ${arguments.get('budget_per_kg', 'TBD')}/kg."
                return self.ok({"sourcing_brief": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "query_procurement":
                return await self._handle_query(arguments.get("query", ""))
            case _:
                return self.err(f"Unknown tool: {name}")

    async def _handle_query(self, query: str) -> dict:
        """Return real procurement recommendations, fall back to Claude."""
        try:
            from maillard.mcp.operations.procurement import get_procurement_report
            report = get_procurement_report()
            recs = report.get("recommendations", [])

            lower = query.lower()
            if any(kw in lower for kw in ["order", "buy", "stock", "supplier", "reorder", "purchase", "what to", "need"]):
                if not recs:
                    return self.ok({"answer": "No purchases needed right now. All stock levels sufficient."})

                lines = ["Purchase Recommendations", "=" * 35, ""]
                for r in recs:
                    lines.append(f"[{r['urgency']}] {r['item']}: order {r['recommended_qty']} {r['unit']} from {r['supplier']}")
                    lines.append(f"  {r['days_left']}d stock left. Lead: {r['lead_time_days']}d. {r['reason']}")
                    lines.append("")
                lines.append(f"Total estimated cost: EUR {report['total_estimated_cost']}")
                return self.ok({"answer": "\n".join(lines), "procurement": report})

            # Other procurement question: enrich Claude
            context = "\n".join(f"{r['urgency']}: {r['item']} ({r['days_left']}d left)" for r in recs) if recs else "No purchases needed."
            answer = await ask(f"{query}\n\nCurrent procurement status:\n{context}", SYSTEM_PROMPT)
            return self.ok({"answer": answer, "procurement": report})
        except Exception:
            return self.ok({"answer": await ask(query, SYSTEM_PROMPT)})
