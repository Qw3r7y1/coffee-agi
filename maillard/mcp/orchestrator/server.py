"""
Orchestrator MCP — Master router for all Maillard department MCPs.

Routing strategy:
  1. Parse intent from the incoming task/message.
  2. Classify the primary department via keyword + semantic rules.
  3. Optionally identify secondary departments for handoff.
  4. Dispatch to the primary department server.
  5. If the department emits a handoff, route it and chain the result.
"""
from __future__ import annotations

import re
from typing import Any

from loguru import logger

from maillard.mcp.shared.handoff import HandoffRequest, build_handoff, validate_handoff


# ── Department registry ───────────────────────────────────────────────────────
# Lazy imports to avoid circular deps at module load time.
def _load_registry() -> dict[str, Any]:
    from maillard.mcp.designer.server import DesignerMCP
    from maillard.mcp.accounting.server import AccountingMCP
    from maillard.mcp.legal.server import LegalMCP
    from maillard.mcp.analyst.server import AnalystMCP
    from maillard.mcp.operations.server import OperationsMCP
    from maillard.mcp.procurement.server import ProcurementMCP
    from maillard.mcp.hr.server import HRMCP
    from maillard.mcp.marketing.server import MarketingMCP
    from maillard.mcp.sales.server import SalesMCP
    from maillard.mcp.executive.server import ExecutiveMCP
    from maillard.mcp.recipe.server import RecipeMCP

    return {
        "designer":    DesignerMCP(),
        "accounting":  AccountingMCP(),
        "legal":       LegalMCP(),
        "analyst":     AnalystMCP(),
        "operations":  OperationsMCP(),
        "procurement": ProcurementMCP(),
        "hr":          HRMCP(),
        "marketing":   MarketingMCP(),
        "sales":       SalesMCP(),
        "executive":   ExecutiveMCP(),
        "recipe":      RecipeMCP(),
    }


# ── Routing rules ─────────────────────────────────────────────────────────────
# Each entry: (regex pattern, department)
# Evaluated in order — first match wins.
ROUTING_RULES: list[tuple[str, str]] = [
    # Recipe — must come before operations to catch recipe/drink/food queries first
    (r"recipe|how (to |do you |do i )?(make|prepare|brew|steam|froth|pull)|latte|cappuccino|freddo|cold brew|americano|espresso (shot|recipe|ratio|extract)|how many (shot|gram|ml)|what (goes|is in|temperature)|crepe|parfait|menu item|drink recipe|food item|barista technique", "recipe"),
    # Designer
    (r"brand|design|logo|packaging|label|visual|typography|color|aesthetic|guideline|creative|banner|menu design|signage|template", "designer"),
    # Marketing
    (r"campaign|social media|promo|promotion|launch|content|ad|advertis|instagram|email blast|newsletter|engagement", "marketing"),
    # Sales
    (r"sale|revenue|customer|order|wholesale|retail price|upsell|client|lead|deal|pipeline|quote", "sales"),
    # Analyst
    (r"analys|report|metric|kpi|dashboard|trend|data|insight|forecast|benchmark|performance", "analyst"),
    # Operations
    (r"operat|workflow|shift|schedule|barista|café|store|equipment|supply|inventory|procedure", "operations"),
    # Procurement
    (r"procure|supplier|vendor|purchase|sourcing|bean|green coffee|buy|order stock|price negotiat", "procurement"),
    # Accounting
    (r"account|invoice|payment|budget|cost|expense|profit|loss|tax|financial|cash flow|balance sheet", "accounting"),
    # Legal
    (r"legal|contract|compliance|regulation|trademark|copyright|license|liability|agreement|gdpr|policy", "legal"),
    # HR
    (r"hr|hire|recruit|employ|staff|onboard|payroll|benefit|performance review|termination|culture|training", "hr"),
    # Executive
    (r"strateg|vision|board|executive|decision|priorit|roadmap|okr|investor|expand|partner", "executive"),
]


class OrchestratorMCP:
    """
    Central router. Receives any task, resolves the correct department,
    dispatches, and handles handoff chaining.
    """

    def __init__(self):
        self._registry: dict | None = None
        logger.info("[ORCHESTRATOR-MCP] initialised")

    @property
    def registry(self) -> dict:
        if self._registry is None:
            self._registry = _load_registry()
        return self._registry

    # ── Public API ────────────────────────────────────────────────────────────

    def route(self, task: str) -> str:
        """Determine which department should handle this task."""
        lower = task.lower()
        for pattern, dept in ROUTING_RULES:
            if re.search(pattern, lower):
                logger.info(f"[ORCHESTRATOR] '{task[:60]}...' → {dept}")
                return dept
        logger.info(f"[ORCHESTRATOR] no match for '{task[:60]}' → executive (fallback)")
        return "executive"

    async def dispatch(
        self,
        task: str,
        tool_name: str | None = None,
        arguments: dict | None = None,
        department: str | None = None,
    ) -> dict:
        """
        Route a task to a department and call a tool.
        If department is not specified, auto-route from task text.
        """
        dept = department or self.route(task)
        server = self.registry.get(dept)
        if not server:
            return {"status": "error", "error": f"Unknown department: {dept}"}

        # If no specific tool, use the department's default query tool
        if tool_name is None:
            tool_name = f"query_{dept}"
        args = arguments or {"query": task}

        logger.info(f"[ORCHESTRATOR] dispatching → {dept}.{tool_name}")
        result = await server.handle_tool(tool_name, args)

        # Chain handoff if the result requests one
        if result.get("handoff"):
            result = await self._chain_handoff(result["handoff"], result)

        return result

    async def dispatch_handoff(self, request: HandoffRequest) -> dict:
        """Route an explicit inter-department handoff request."""
        if not validate_handoff(request.from_dept, request.to_dept):
            return {
                "status": "error",
                "error": f"Handoff from {request.from_dept} to {request.to_dept} not permitted.",
            }
        return await self.dispatch(
            task=request.task,
            department=request.to_dept,
            arguments=request.context,
        )

    def list_departments(self) -> list[str]:
        return list(self.registry.keys())

    def list_tools(self, department: str) -> list[dict]:
        server = self.registry.get(department)
        if not server:
            return []
        return server.tools

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _chain_handoff(self, handoff: dict, original_result: dict) -> dict:
        try:
            req = HandoffRequest(**handoff)
            chained = await self.dispatch_handoff(req)
            return {**original_result, "chained_result": chained}
        except Exception as e:
            logger.warning(f"[ORCHESTRATOR] handoff chain failed: {e}")
            return original_result
