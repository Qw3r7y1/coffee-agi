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
#
# PRIORITY LOGIC:
#   1. Recipe is first (explicit drink/food making intent)
#   2. Analyst has TWO rules:
#      a. Explicit commodity/market terms (line 2)
#      b. Coffee-signal + any business word that would otherwise leak (line 3)
#      Both fire before any other department.
#   3. Then designer, marketing, sales, etc.
#   4. Executive is LAST among named departments (only for pure strategy)
#
# This ensures that "sourcing cost for Brazilian beans" or "profit margin
# on espresso" routes to Analyst (because coffee is present), while
# "sourcing cost for office supplies" routes to Procurement.

# Coffee/commodity signal words used in the guard rule below
_COFFEE_SIGNALS = (
    r"coffee|arabica|robusta|espresso"
    r"|green.?coffee|commodity|futures|KC\b"
    r"|bean.{0,10}(cost|price|sourc)|brazilian.{0,10}bean"
    r"|roast.{0,5}(cost|margin)|brewing cost"
)

ROUTING_RULES: list[tuple[str, str]] = [
    # ── 1. Recipe (explicit drink/food making intent) ──
    (r"recipe|how (to |do you |do i )?(make|prepare|brew|steam|froth|pull)|latte|cappuccino|freddo|cold brew|americano|espresso (shot|recipe|ratio|extract)|how many (shot|gram|ml)|what (goes|is in|temperature)|crepe|parfait|menu item|drink recipe|food item|barista technique", "recipe"),

    # ── 2a. Analyst — explicit commodity/market/intelligence terms ──
    (r"coffee (futures|price|market|commodity|trading|trend)|arabica price|commodity coffee|KC futures|green coffee (price|cost)|coffee commodity|robusta|futures|market impact|procurement risk|fx (impact|rate)|brazilian real|supplier cost pressure|margin (pressure|analysis|risk)|green cost|currency impact|executive.{0,20}brief|market (brief|snapshot|intelligence|doing|direction)|exchange rate|brl.?usd|cop.?usd|eur.?usd|(should (i|we)|when to) buy.{0,15}coffee|buy coffee|coffee.{0,10}(outlook|forecast|buy|sell)|what.{0,5}(is |are )?(the )?market", "analyst"),

    # ── 2b. Analyst — coffee-signal + business word guard ──
    # If the message mentions coffee AND any business term that would
    # otherwise leak to procurement/accounting/sales/executive, catch it
    # here so the Analyst handles the market-intelligence angle.
    (r"(?=.*(?:" + _COFFEE_SIGNALS + r"))(?=.*(?:sourcing|procure|purchase|supplier|vendor|bean|cost|expense|budget|profit|margin|wholesale|retail|pricing|strateg|executive|partner|order|financial|invest))", "analyst"),

    # ── 3. Designer ──
    (r"brand|design|logo|packaging|label|visual|typography|color|aesthetic|guideline|creative|banner|menu design|signage|template|generate.{0,10}(image|photo|picture|post|content|video|reel)|instagram.{0,5}post|create.{0,10}(image|photo|post|content|visual)", "designer"),

    # ── 4. Marketing ──
    (r"campaign|social media|promo|promotion|launch|content|\bad\b|\bads\b|advertis|instagram|email blast|newsletter|engagement", "marketing"),

    # ── 4b. Accounting guard — ANY cost/price/vendor/invoice query ──
    # MUST come before operations to prevent "milk", "cups" etc. leaking to ops for price questions.
    (r"(latest|last|recent|current).{0,15}(price|invoice|cost)"
     r"|(cheapest|cheap|best price).{0,15}(vendor|supplier|for)"
     r"|compare.{0,15}(vendor|price|supplier)"
     r"|price.{0,15}(history|comparison|change|from|for|of)"
     r"|how much.{0,15}(pay|cost|spend|paying|is|for|does)"
     r"|(what|how much) did (i|we).{0,15}(pay|spend|cost)"
     r"|(unit|per) cost"
     r"|\bcost of\b"
     r"|vendor.{0,15}(price|cost|history|compare)"
     r"|invoice.{0,15}(from|for|show|list|last|latest)"
     r"|redway|optima|loumidis|odeko|sysco|impact food|rite.?a.?way|pure produce|dairy wagon|wheatfield"
     r"|(oat milk|whole milk|oat|yogurt|cream|butter|cups?|napkin|paper).{0,10}(cost|price|pay)"
     r"|(cost|price|pay).{0,10}(oat milk|whole milk|oat|yogurt|cream|butter|cups?|napkin|paper)"
     , "accounting"),

    # ── 4c. Operations guard — inventory/production/wholesale/decisions before Sales ──
    (r"inventory|stock level|reorder|roast batch|roast schedule|production (summary|plan)|waste (report|summary|log)|spoil|wholesale (order|customer|demand|fulfil)|delivery schedule|how much .{0,10}(have|left|remaining)|daily plan|what should (i|we) do|what.{0,5}priorit|priorit.{0,10}today|what.{0,10}(need|happen).{0,10}today|morning (plan|brief)|what.{0,10}roast|batch plan|stockout|running out|3.?day|7.?day|weekly plan|what.{0,10}(reorder|order)|what do (i|we) need to order", "operations"),

    # ── 5. Sales (revenue/pipeline/product focus) ──
    (r"sale|revenue|customer|order|wholesale|retail price|upsell|client|lead|deal|pipeline|quote|top sell|best sell|push|deprioritize|what.{0,10}(push|promote|sell)|should (i|we) (push|sell)", "sales"),

    # ── 6a. Accounting guard — margins/cost/P&L ──
    (r"profit.{0,5}loss|balance sheet|tax return|tax filing|P&L|income statement|margin|profitab|cost.{0,5}(report|analysis|breakdown)", "accounting"),

    # ── 6b. Analyst — generic analytics (no coffee required) ──
    (r"analys|report|metric|kpi|dashboard|trend|data|insight|forecast|benchmark|performance", "analyst"),

    # ── 7. Operations (includes inventory, production, wholesale fulfillment) ──
    (r"operat|workflow|shift|schedule|barista|café|store|equipment|supply|inventory|procedure|stock|reorder|roast batch|roast schedule|production summary|production plan|waste|spoil|wholesale order|wholesale customer|wholesale demand|delivery schedule|fulfil|how much .{0,10}(have|left|remaining)|milk|cups|packaging", "operations"),

    # ── 8a. Accounting guard — invoice/vendor/price data queries ──
    (r"invoice|invoices on file|vendor.{0,15}(detail|history|price|cost|list|spend)|what did we (buy|spend|pay)|purchase history|dropbox.{0,10}invoice|dairy wagon|wheatfield|supplier.{0,15}(cost|price|history|invoice)|(latest|last|recent).{0,15}(price|invoice)|cheapest.{0,15}vendor|compare.{0,15}(vendor|price|supplier)|price.{0,15}(history|comparison|change)|how much.{0,15}(pay|cost|spend)|redway|optima|loumidis|odeko|sysco|impact food|rite.?a.?way|pure produce", "accounting"),

    # ── 8b. Procurement (non-coffee) ──
    (r"procure|supplier|vendor|purchase|sourcing|bean|green coffee|buy|order stock|price negotiat", "procurement"),

    # ── 9. Accounting ──
    (r"account|payment|budget|cost|expense|profit|loss|tax|financial|cash flow|balance sheet", "accounting"),

    # ── 10. Legal ──
    (r"legal|contract|compliance|regulation|trademark|copyright|license|liability|agreement|gdpr|policy", "legal"),

    # ── 11. HR ──
    (r"hr|hire|recruit|employ|staff|onboard|payroll|benefit|performance review|termination|culture|training", "hr"),

    # ── 12. Executive (pure strategy only — data questions go to Analyst) ──
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

        # If the message contains a URL → analyst (URL reader)
        # Finance domains always go to analyst regardless of surrounding text.
        has_url = bool(re.search(r'https?://\S+', lower))
        if has_url:
            # Finance domains that must NEVER default to Executive
            finance_domains = [
                "investing.com", "tradingview.com", "bloomberg.com",
                "reuters.com", "cmegroup.com", "ice.com", "barchart.com",
                "tradingeconomics.com", "marketwatch.com", "cnbc.com",
            ]
            if any(domain in lower for domain in finance_domains):
                logger.info(f"[ORCHESTRATOR] finance-domain URL → analyst")
                return "analyst"

            market_url_kw = [
                "coffee", "futures", "arabica", "commodity", "market",
                "price", "trading", "link", "article", "url", "page",
                "what does this", "what does that", "read this", "check this",
            ]
            if any(kw in lower for kw in market_url_kw):
                logger.info(f"[ORCHESTRATOR] URL + market keywords → analyst")
                return "analyst"

            # URL with no market context — still route to analyst for URL reading
            logger.info(f"[ORCHESTRATOR] URL detected → analyst (url reader)")
            return "analyst"

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
            logger.error(f"[ORCHESTRATOR] unknown department: {dept}")
            return {"status": "error", "department": dept, "error": f"Unknown department: {dept}"}

        # If no specific tool, use smart tool picker or default query
        if tool_name is None:
            lower_task = task.lower()
            if dept == "analyst":
                tool_name, arguments = self._pick_analyst_tool(lower_task)
            elif dept == "operations":
                tool_name, arguments = self._pick_operations_tool(lower_task)
            elif dept == "designer":
                tool_name, arguments = self._pick_designer_tool(lower_task, task)
            else:
                tool_name = f"query_{dept}"
        args = arguments or {"query": task}

        logger.info(f"[ORCHESTRATOR] dispatching -> {dept}.{tool_name}")
        try:
            result = await server.handle_tool(tool_name, args)
        except Exception as e:
            logger.error(f"[ORCHESTRATOR] {dept}.{tool_name} FAILED: {e}")
            return {
                "status": "error",
                "department": dept,
                "tool": tool_name,
                "error": f"Department '{dept}' encountered an error: {str(e)[:200]}",
            }

        # Ensure result always has department tag
        if isinstance(result, dict):
            result.setdefault("department", dept)

        # Chain handoff if the result requests one
        if isinstance(result, dict) and result.get("handoff"):
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

    # ── Analyst tool picker ─────────────────────────────────────────────────

    @staticmethod
    def _pick_designer_tool(lower_task: str, original_task: str) -> tuple[str, dict]:
        """Choose designer tool — execution tools for generation, query for everything else."""
        import re
        # Instagram post / content generation → execution engine
        if re.search(r"(generate|create|make).{0,15}(instagram|post|content|reel|image|photo|picture|video)", lower_task):
            # Extract topic from the task
            topic = re.sub(r"^(generate|create|make)\s+(an?\s+)?(instagram\s+post|post|content|image|photo|video|reel)\s*(about|for|of|on)?\s*", "", lower_task, flags=re.IGNORECASE).strip()
            if not topic:
                topic = original_task
            return "generate_instagram_post", {"topic": topic}
        # Image generation
        if re.search(r"(generate|create|make).{0,10}(image|photo|picture|visual)", lower_task):
            return "generate_design_image", {"subject": original_task, "prompt": original_task}
        # Default to query
        return "query_designer", {"query": original_task}

    @staticmethod
    def _pick_analyst_tool(lower_task: str) -> tuple[str, dict]:
        """Choose the best analyst tool (5 tools only)."""

        # Buying decision (primary)
        if re.search(
            r"should (i|we) (buy|sell|purchase|order)|when to buy|buy.{0,10}(coffee|now)"
            r"|coffee.{0,15}(price|market|trend|futures)|arabica price|what.{0,15}market"
            r"|prices? (going|rising|falling|up|down)|analyze.{0,10}(coffee|price|market)",
            lower_task,
        ):
            logger.info("[ORCHESTRATOR] -> buying_signal")
            return "buying_signal", {}

        # Executive brief (more detail)
        if re.search(r"executive.{0,20}brief|market brief|intelligence brief|full.{0,5}(report|analysis)", lower_task):
            logger.info("[ORCHESTRATOR] -> executive_brief")
            return "executive_brief", {}

        # Market snapshot
        if re.search(r"market (snapshot|overview|summary)|current price", lower_task):
            logger.info("[ORCHESTRATOR] -> market_snapshot")
            return "market_snapshot", {}

        # URL
        if re.search(r'https?://\S+', lower_task):
            logger.info("[ORCHESTRATOR] -> query_analyst (URL)")
            return "query_analyst", {}

        # Fallback — query_analyst will still auto-fetch data if keywords match
        logger.info("[ORCHESTRATOR] → query_analyst (fallback)")
        return "query_analyst", {}

    # ── Operations tool picker ──────────────────────────────────────────────

    @staticmethod
    def _pick_operations_tool(lower_task: str) -> tuple[str, dict]:
        """Choose the best operations tool for a free-text query."""

        # Morning brief (concise executive summary)
        if re.search(
            r"morning brief|brief.{0,10}(today|morning)|summary.{0,10}today"
            r"|what do i need to know|owner.{0,5}brief|executive.{0,5}brief.{0,5}(ops|oper)"
            r"|quick.{0,5}(summary|update|overview)",
            lower_task,
        ):
            logger.info("[ORCHESTRATOR] -> morning_brief")
            return "morning_brief", {}

        # Full daily plan (detailed execution steps)
        if re.search(
            r"what should (i|we) do|daily plan|today.{0,15}(plan|priorit|action)"
            r"|what.{0,10}(need|happen|do).{0,10}today"
            r"|priorities|what.{0,5}first|operations plan|full plan",
            lower_task,
        ):
            logger.info("[ORCHESTRATOR] -> daily_plan (decision engine)")
            return "daily_plan", {}

        if re.search(r"3.?day|next (few|three|3) days|72.?h|short.?term forecast", lower_task):
            logger.info("[ORCHESTRATOR] -> forecast_3day")
            return "forecast_3day", {}

        if re.search(r"7.?day|week.{0,5}(plan|schedule|ahead)|weekly (plan|production|schedule)|next week", lower_task):
            logger.info("[ORCHESTRATOR] -> plan_7day")
            return "plan_7day", {}

        # Intelligence triggers
        if re.search(r"stockout|running out|when.{0,10}(run out|finish|empty)|how long.{0,10}(last|left)", lower_task):
            return "stockout_forecast", {}

        if re.search(r"what.{0,10}(reorder|order|need to order)|reorder plan|purchase order", lower_task):
            return "reorder_plan", {}

        if re.search(r"waste.{0,10}(analy|report|anomal|detect)", lower_task):
            return "waste_analysis", {}

        if re.search(r"roast loss|loss.{0,10}(analy|trend|report)", lower_task):
            return "roast_loss_analysis", {}

        if re.search(r"what.{0,10}roast|batch plan|next batch|roast.{0,5}(next|plan|recommend)", lower_task):
            return "batch_plan", {}

        if re.search(r"demand forecast|forecast.{0,10}(wholesale|demand)", lower_task):
            return "demand_forecast", {"weeks": 4, "history_days": 90}

        if re.search(r"production gap|supply.{0,5}gap|coverage", lower_task):
            return "production_gap", {}

        if re.search(r"delivery risk|at.?risk order", lower_task):
            return "delivery_risk", {}

        if re.search(r"customer rank|top customer|customer value", lower_task):
            return "customer_rankings", {}

        if re.search(r"inventory (health|report|status|overview)|how.{0,5}(is|our) inventory", lower_task):
            return "inventory_health", {}

        if re.search(r"production (health|report|status|overview)|how.{0,5}(is|our) production", lower_task):
            return "production_health", {}

        if re.search(r"wholesale (health|report|status|overview)|how.{0,5}(is|our) wholesale", lower_task):
            return "wholesale_health", {}

        # Fallback
        return "query_operations", {"query": lower_task}

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _chain_handoff(self, handoff: dict, original_result: dict) -> dict:
        try:
            req = HandoffRequest(**handoff)
            chained = await self.dispatch_handoff(req)
            return {**original_result, "chained_result": chained}
        except Exception as e:
            logger.warning(f"[ORCHESTRATOR] handoff chain failed: {e}")
            return original_result
