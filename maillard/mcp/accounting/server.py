"""Accounting MCP — Financial tracking, invoicing, budgets, cost analysis."""
from __future__ import annotations
from typing import Any
from loguru import logger
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Accounting Department AI.
Responsibilities: financial reporting, invoicing, budget tracking, cost-of-goods analysis,
cash flow management, tax compliance, and expense review.

STRICT FINANCE RULES — YOU MUST FOLLOW THESE:
1. When real invoice/vendor data is provided below, use ONLY those exact numbers. Quote them directly.
2. NEVER invent, estimate, or guess prices. NEVER use "typically", "usually", "around", or "approximately" for costs.
3. NEVER mention menu prices, customer surcharges, or retail pricing unless the user specifically asked about retail.
4. If the data below contains the answer, give it with the exact dollar amount, vendor, and date.
5. If the data below does NOT contain the answer, say exactly: "No verified data found for [item]. Check the invoice database or ask me to pull from Dropbox."
6. If the query is ambiguous (e.g. "how much is oat"), ask a clarifying question:
   "Did you mean: (1) vendor cost from invoices, (2) product cost/COGS, or (3) customer menu price?"
7. NEVER say "Great news!", NEVER upsell, NEVER give generic advice when a specific price was asked for.
8. Be short and direct. Lead with the number.
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
                return await self._handle_query(arguments.get("query", ""))
            case _:
                return self.err(f"Unknown tool: {name}")

    async def _handle_query(self, query: str) -> dict:
        """Route queries to real data sources. Local data first. Never hallucinate prices."""
        lower = query.lower()
        logger.info(f"[ACCOUNTING] Query received: {query[:80]}")

        # ── Step 1: Detect if this is a money/price question ──
        is_money_question = any(kw in lower for kw in [
            "price", "cost", "pay", "spend", "invoice", "vendor", "supplier",
            "cheapest", "compare", "buy", "charge", "how much",
        ])

        # ── Step 2: Detect ambiguity ──
        # Short queries with a product name but no clear intent
        ambiguous_products = ["oat", "milk", "cups", "sugar", "syrup", "cream", "yogurt", "napkin", "paper"]
        query_words = lower.split()
        is_ambiguous = (
            len(query_words) <= 5
            and any(p in lower for p in ambiguous_products)
            and not any(kw in lower for kw in [
                "invoice", "vendor", "supplier", "latest price", "last price",
                "cheapest", "compare", "history", "paid", "pay for", "cost of",
                "unit cost", "per unit",
            ])
        )

        if is_ambiguous:
            logger.info(f"[ACCOUNTING] Ambiguous query detected: {query[:60]}")
            # Still check DB — if we have data, show it with a clarification
            db_data = self._query_invoice_db(query)
            if db_data:
                answer = await ask(
                    f"The user asked: \"{query}\"\n\n"
                    f"This is ambiguous. They might mean vendor cost, product COGS, or menu price.\n"
                    f"Here is REAL invoice data we have:\n{db_data}\n\n"
                    f"Show the invoice data we have, then ask which meaning they intended:\n"
                    f"1) Vendor cost (what we pay suppliers)\n"
                    f"2) Product COGS (our total cost to make it)\n"
                    f"3) Customer menu price (what we charge)",
                    SYSTEM_PROMPT,
                )
                return self.ok({"answer": answer, "source": "invoice_db_with_clarification"})
            else:
                return self.ok({
                    "answer": (
                        f"I'm not sure what you mean by \"{query}\". Could you clarify?\n\n"
                        f"1. **Vendor cost** — what we pay our suppliers (from invoices)\n"
                        f"2. **Product COGS** — our total cost to produce it\n"
                        f"3. **Menu price** — what we charge customers\n\n"
                        f"Which one are you looking for?"
                    ),
                    "source": "ambiguity_clarification",
                })

        # ── Step 3: Try invoice DB first for any cost/price/vendor question ──
        db_data = self._query_invoice_db(query)
        if db_data:
            logger.info(f"[ACCOUNTING] DB hit — verified data found for: {query[:60]}")
            answer = await ask(
                f"{query}\n\nHere is REAL data from our invoice database:\n{db_data}",
                SYSTEM_PROMPT,
            )
            return self.ok({"answer": answer, "source": "invoice_db"})
        else:
            logger.info(f"[ACCOUNTING] DB returned no data for: {query[:60]}")

        # ── Step 4: Cost engine for margin/COGS queries ──
        try:
            from maillard.mcp.operations.cost_engine import calculate_product_costs
            costs = calculate_product_costs()
        except Exception:
            costs = {}

        if costs and any(kw in lower for kw in ["margin", "profit", "cogs", "expensive"]):
            logger.info(f"[ACCOUNTING] Cost engine hit for: {query[:60]}")
            lines = ["Product Cost & Margin Report", "=" * 35, ""]
            alerts = []
            for product, d in sorted(costs.items(), key=lambda x: x[1]["margin_pct"]):
                lines.append(f"{product:22s} cost={d['cost']:5.2f}  price={d['price']:5.2f}  margin={d['margin_pct']:5.1f}%  {d['grade']}")
                if d["grade"] in ("CRITICAL", "LOW"):
                    alerts.append(f"{product}: {d['margin_pct']:.0f}% -- {d['action']}")
            if alerts:
                lines.append("")
                lines.append("ALERTS:")
                for a in alerts:
                    lines.append(f"  {a}")
            return self.ok({"answer": "\n".join(lines), "costs": costs, "source": "cost_engine"})

        # ── Step 5: Finance-safe fallback ──
        # If this was a money question and we found nothing, say so explicitly
        if is_money_question:
            logger.info(f"[ACCOUNTING] No verified data — finance-safe fallback for: {query[:60]}")
            db_summary = self._get_db_summary_text()
            return self.ok({
                "answer": (
                    f"No verified local data found for this query.\n\n"
                    f"{db_summary or 'Invoice database is empty.'}\n\n"
                    f"To get this data, either:\n"
                    f"- Pull latest invoices from Dropbox\n"
                    f"- Ask about a specific vendor or product we have on file"
                ),
                "source": "finance_safe_fallback",
            })

        # ── Step 6: Non-financial question → Claude with context ──
        context_parts = []
        if costs:
            context_parts.append(f"Cost data: {len(costs)} products analyzed.")
        db_summary = self._get_db_summary_text()
        if db_summary:
            context_parts.append(db_summary)
        context = "\n".join(context_parts) if context_parts else "No live data loaded."
        logger.info(f"[ACCOUNTING] General question → Claude: {query[:60]}")
        answer = await ask(f"{query}\n\n{context}", SYSTEM_PROMPT)
        return self.ok({"answer": answer, "source": "claude_general"})

    def _query_invoice_db(self, query: str) -> str | None:
        """Query the SQLite invoice DB and build a data block for Claude."""
        try:
            from maillard.mcp.accounting.invoice_db import (
                get_db_summary, compare_vendor_prices, get_latest_price_for_item,
                get_vendor_price_history, get_cheapest_vendor, get_items_needing_review,
            )
        except Exception as e:
            logger.error(f"[ACCOUNTING] Failed to import invoice_db: {e}")
            return None

        summary = get_db_summary()
        if summary["invoices"] == 0:
            return None

        lower = query.lower()
        lines = [f"=== INVOICE DATABASE: {summary['invoices']} invoices, {summary['items']} items, {summary['vendors']} vendors ===", ""]

        # ── Vendor-specific query ──
        vendor_names = {
            "redway": "REDWAY", "dairy": "Dairy Wagon", "wheatfield": "WHEATFIELD",
            "optima": "Optima", "loumidis": "LOUMIDIS", "odeko": "Odeko",
            "sysco": "SYSCO", "impact": "Impact Food", "rite": "RITE-A-WAY",
            "pure produce": "Pure Produce",
        }
        matched_vendor = None
        for key, name in vendor_names.items():
            if key in lower:
                matched_vendor = name
                break

        if matched_vendor:
            hist = get_vendor_price_history(matched_vendor)
            if hist:
                lines.append(f"VENDOR HISTORY: {matched_vendor} ({len(hist)} items)")
                # Group by invoice date
                seen_dates = set()
                for h in hist:
                    date_key = h.get("invoice_date", "?")
                    inv_num = h.get("invoice_number", "?")
                    if date_key not in seen_dates:
                        seen_dates.add(date_key)
                        lines.append(f"\n  Invoice #{inv_num} ({date_key}):")
                    src = f" [HW]" if h.get("override_source") == "handwritten" else ""
                    lines.append(f"    {h['item']:40s} ${h['unit_price']:>7.2f}/{h.get('unit','ea')}  qty={h.get('quantity',1)}{src}")
                lines.append("")

        # ── Item price lookup ──
        # Try to extract an item name from the query
        item_keywords = [
            "oat milk", "whole milk", "milk", "espresso", "coffee", "yogurt",
            "cream", "sugar", "syrup", "cups", "napkin", "bread", "croissant",
            "danish", "muffin", "butter", "eggs", "flour",
        ]
        matched_item = None
        for kw in item_keywords:
            if kw in lower:
                matched_item = kw
                break

        if matched_item:
            # Search DB for items containing this keyword
            try:
                import sqlite3
                from maillard.mcp.accounting.invoice_db import DB_PATH
                conn = sqlite3.connect(DB_PATH)
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """SELECT DISTINCT normalized_name FROM invoice_items
                       WHERE LOWER(normalized_name) LIKE ? AND unit_price > 0
                       ORDER BY normalized_name""",
                    (f"%{matched_item}%",),
                ).fetchall()
                conn.close()
                item_names = [r["normalized_name"] for r in rows]
            except Exception:
                item_names = []

            if item_names:
                for item_name in item_names[:5]:
                    # Compare prices across vendors
                    cmp = compare_vendor_prices(item_name)
                    if cmp:
                        lines.append(f"PRICE COMPARISON: {item_name}")
                        for c in cmp:
                            src = " [HW]" if c.get("override_source") == "handwritten" else ""
                            lines.append(f"  {c['vendor']:30s} ${c['unit_price']:>7.2f}/{c.get('unit','ea')}  ({c.get('invoice_date','?')})  conf={c.get('confidence','?')}{src}")

                        cheapest = get_cheapest_vendor(item_name)
                        if cheapest and cheapest["num_vendors"] > 1:
                            lines.append(f"  >>> CHEAPEST: {cheapest['vendor']} — saves ${cheapest['savings_vs_highest']:.2f} vs highest")
                        lines.append("")

        # ── Cheapest vendor query ──
        if "cheapest" in lower or "cheap" in lower or "best price" in lower:
            if not matched_item:
                # Show cheapest across common items
                lines.append("CHEAPEST VENDORS (common items):")
                for test_item in ["Whole Milk", "Oat Milk", "Yogurt", "Cream", "Butter"]:
                    try:
                        import sqlite3
                        from maillard.mcp.accounting.invoice_db import DB_PATH
                        conn = sqlite3.connect(DB_PATH)
                        conn.row_factory = sqlite3.Row
                        rows = conn.execute(
                            "SELECT DISTINCT normalized_name FROM invoice_items WHERE LOWER(normalized_name) LIKE ? AND unit_price > 0",
                            (f"%{test_item.lower()}%",),
                        ).fetchall()
                        conn.close()
                        for r in rows[:2]:
                            ch = get_cheapest_vendor(r["normalized_name"])
                            if ch:
                                lines.append(f"  {r['normalized_name']:35s} -> {ch['vendor']} ${ch['unit_price']:.2f}")
                    except Exception:
                        pass
                lines.append("")

        # ── Review queue ──
        if "review" in lower or "flagged" in lower or "unclear" in lower:
            review = get_items_needing_review()
            if review:
                lines.append(f"ITEMS NEEDING REVIEW ({len(review)}):")
                for r in review[:10]:
                    hw = r.get("handwriting_note") or "-"
                    lines.append(f"  {r['vendor']:25s} {r['raw_name']:30s} ${r.get('unit_price',0)}  conf={r.get('confidence','?')}  hw={hw}")
                lines.append("")

        # Only return if we actually found data beyond the header
        if len(lines) <= 2:
            return None

        return "\n".join(lines)

    @staticmethod
    def _get_db_summary_text() -> str:
        """Quick summary of DB for general context."""
        try:
            from maillard.mcp.accounting.invoice_db import get_db_summary
            s = get_db_summary()
            if s["invoices"] == 0:
                return ""
            return (f"Invoice DB: {s['invoices']} invoices, {s['items']} items, "
                    f"{s['vendors']} vendors. {s['review_required']} items need review. "
                    f"{s['handwritten_lines']} handwritten corrections detected.")
        except Exception:
            return ""
