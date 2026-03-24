"""
Operations MCP -- Lean operational system for Maillard Coffee Roasters.

Agent tools (22 total):
  Decision Engine (4): morning_brief, daily_plan, forecast_3day, plan_7day
  Inventory CRUD (6): add, update, receive, list, log_usage, log_waste
  Production CRUD (4): schedule_roast, start_roast, complete_roast, list_batches
  Wholesale CRUD (4): add_customer, create_order, update_status, list_orders
  Cafe (3): get_recipe, shift_schedule, equipment_checklist
  Catch-all (1): query_operations (auto-injects intelligence for any question)

Intelligence functions (inventory_health, stockout_forecast, batch_plan, etc.)
are accessible via query_operations and the REST API at /api/ops/intelligence/*.
They do not need separate MCP tools.
"""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask
from maillard.mcp.shared import kb_client
from maillard.mcp.operations import inventory, production, wholesale
from maillard.mcp.operations import inventory_intelligence, production_intelligence, wholesale_intelligence
from maillard.mcp.operations import decision_engine
from loguru import logger

SYSTEM_PROMPT = """
You are the Maillard Operations AI -- a daily operator for the business.

You help the owner answer three questions quickly:
1. What do I need to do today?
2. What will break if I don't?
3. What is worth my time first?

You manage inventory, production, and wholesale for a specialty coffee roastery.

Key specs:
- Espresso: 22g dose, 1:2.3 ratio, 195-205F, 9-10 atm, 20-30s
- Green-to-roasted: 0.82 (18% loss), ~45 shots per kg
- Stock thresholds: <1 day = CRITICAL, <2 days = HIGH, <5 days = MEDIUM

Rules:
- Always use real numbers from the systems, never estimate
- Keep responses concise and actionable
- Prioritize business continuity, then cash protection, then margin
"""

TOOLS: list[dict] = [
    # ── Decision Engine (primary interface) ───────────────────
    {
        "name": "morning_brief",
        "description": "Owner Morning Brief: concise daily summary with top priorities, risks, money at stake, and recommended first move. Use for 'morning brief', 'what do I need to know', 'quick summary'.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "daily_plan",
        "description": "Full daily execution plan with scored and prioritized steps, times, financials, and execution tracking. Use for 'what should I do today', 'daily plan', 'priorities'.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "forecast_3day",
        "description": "3-day predictive forecast: stockout timelines, production needs, reorder timing.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "plan_7day",
        "description": "7-day plan: weekly stockout map, production schedule, reorder calendar.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    # ── Inventory CRUD ────────────────────────────────────────
    {
        "name": "inventory_add",
        "description": "Add a new inventory item (green_coffee/roasted_coffee/milk/consumables).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"}, "name": {"type": "string"},
                "category": {"type": "string"}, "unit": {"type": "string"},
                "quantity": {"type": "number", "default": 0},
                "min_quantity": {"type": "number", "default": 0},
                "cost_per_unit": {"type": "number", "default": 0},
                "supplier": {"type": "string"},
            },
            "required": ["sku", "name", "category", "unit"]
        }
    },
    {
        "name": "inventory_update",
        "description": "Update stock level. mode: 'set' or 'adjust'.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"}, "quantity": {"type": "number"},
                "mode": {"type": "string", "default": "set"},
            },
            "required": ["sku", "quantity"]
        }
    },
    {
        "name": "inventory_receive",
        "description": "Receive stock delivery from supplier.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"}, "quantity": {"type": "number"},
                "cost_per_unit": {"type": "number"},
            },
            "required": ["sku", "quantity"]
        }
    },
    {
        "name": "inventory_list",
        "description": "List all inventory items, optionally by category.",
        "inputSchema": {
            "type": "object",
            "properties": {"category": {"type": "string"}},
            "required": []
        }
    },
    {
        "name": "log_usage",
        "description": "Log stock consumption (bar_service/roasting/wholesale_order).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"}, "quantity_used": {"type": "number"},
                "usage_type": {"type": "string"}, "staff": {"type": "string"},
            },
            "required": ["sku", "quantity_used", "usage_type"]
        }
    },
    {
        "name": "log_waste",
        "description": "Log waste/spoilage (expired/spoiled/spilled/failed_roast).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sku": {"type": "string"}, "quantity_wasted": {"type": "number"},
                "reason": {"type": "string"}, "staff": {"type": "string"},
            },
            "required": ["sku", "quantity_wasted", "reason"]
        }
    },
    # ── Production CRUD ───────────────────────────────────────
    {
        "name": "schedule_roast",
        "description": "Schedule a roast batch. Validates green stock.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "green_coffee_sku": {"type": "string"}, "green_weight_kg": {"type": "number"},
                "roast_level": {"type": "string", "default": "medium"}, "roaster": {"type": "string"},
            },
            "required": ["green_coffee_sku", "green_weight_kg"]
        }
    },
    {
        "name": "start_roast",
        "description": "Start a scheduled roast. Deducts green coffee.",
        "inputSchema": {
            "type": "object",
            "properties": {"batch_code": {"type": "string"}, "roast_temp_c": {"type": "number"}},
            "required": ["batch_code"]
        }
    },
    {
        "name": "complete_roast",
        "description": "Complete a roast. Records output, loss %, adds to inventory.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "batch_code": {"type": "string"}, "roasted_weight_kg": {"type": "number"},
                "roasted_sku": {"type": "string"}, "roast_duration_min": {"type": "number"},
                "retail_allocation_kg": {"type": "number", "default": 0},
                "wholesale_allocation_kg": {"type": "number", "default": 0},
            },
            "required": ["batch_code", "roasted_weight_kg"]
        }
    },
    {
        "name": "list_roast_batches",
        "description": "List roast batches by status (scheduled/in_progress/completed/failed).",
        "inputSchema": {
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": []
        }
    },
    # ── Wholesale CRUD ────────────────────────────────────────
    {
        "name": "add_wholesale_customer",
        "description": "Add a wholesale customer.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"}, "contact_person": {"type": "string"},
                "email": {"type": "string"}, "customer_type": {"type": "string", "default": "cafe"},
            },
            "required": ["name"]
        }
    },
    {
        "name": "create_wholesale_order",
        "description": "Create a wholesale order. lines: [{product_sku, quantity_kg, price_per_kg}].",
        "inputSchema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer"},
                "lines": {"type": "array", "items": {"type": "object"}},
                "requested_delivery": {"type": "string"},
            },
            "required": ["customer_id", "lines"]
        }
    },
    {
        "name": "update_order_status",
        "description": "Update wholesale order status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_number": {"type": "string"},
                "new_status": {"type": "string"},
                "notes": {"type": "string"},
            },
            "required": ["order_number", "new_status"]
        }
    },
    {
        "name": "list_wholesale_orders",
        "description": "List wholesale orders by status or customer.",
        "inputSchema": {
            "type": "object",
            "properties": {"status": {"type": "string"}, "customer_id": {"type": "integer"}},
            "required": []
        }
    },
    # ── Cafe ──────────────────────────────────────────────────
    {
        "name": "get_recipe",
        "description": "Get exact Maillard recipe for a drink or food item.",
        "inputSchema": {
            "type": "object",
            "properties": {"item": {"type": "string"}},
            "required": ["item"]
        }
    },
    {
        "name": "create_shift_schedule",
        "description": "Generate a shift schedule.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "staff": {"type": "array", "items": {"type": "string"}},
                "week_start": {"type": "string"},
            },
            "required": ["staff", "week_start"]
        }
    },
    {
        "name": "equipment_checklist",
        "description": "Generate opening/closing/maintenance checklist.",
        "inputSchema": {
            "type": "object",
            "properties": {"checklist_type": {"type": "string"}},
            "required": ["checklist_type"]
        }
    },
    # ── Catch-all ─────────────────────────────────────────────
    {
        "name": "query_operations",
        "description": "Answer any operations question using live system data. Auto-injects inventory, production, wholesale, and waste intelligence.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    },
]


class OperationsMCP(BaseMCPServer):
    department = "operations"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            # ── Decision Engine ───────────────────────────────────
            case "morning_brief":
                brief = decision_engine.generate_morning_brief()
                return self.ok({"answer": brief["brief"], "brief": brief})
            case "daily_plan":
                plan = decision_engine.generate_daily_operations_plan()
                return self.ok({"answer": plan["formatted_plan"], "plan": plan})
            case "forecast_3day":
                r = decision_engine.get_3_day_forecast()
                return self.ok({"answer": r.get("formatted", ""), **r})
            case "plan_7day":
                r = decision_engine.get_7_day_plan()
                return self.ok({"answer": r.get("formatted", ""), **r})

            # ── Inventory CRUD ────────────────────────────────────
            case "inventory_add":
                return self.ok(inventory.add_item(**arguments))
            case "inventory_update":
                return self.ok(inventory.update_stock(**arguments))
            case "inventory_receive":
                return self.ok(inventory.receive_stock(**arguments))
            case "inventory_list":
                return self.ok({"items": inventory.list_items(**arguments)})
            case "log_usage":
                return self.ok(inventory.log_usage(**arguments))
            case "log_waste":
                return self.ok(inventory.log_waste(**arguments))

            # ── Production CRUD ───────────────────────────────────
            case "schedule_roast":
                return self.ok(production.schedule_roast(**arguments))
            case "start_roast":
                return self.ok(production.start_roast(**arguments))
            case "complete_roast":
                return self.ok(production.complete_roast(**arguments))
            case "list_roast_batches":
                return self.ok({"batches": production.list_batches(**arguments)})

            # ── Wholesale CRUD ────────────────────────────────────
            case "add_wholesale_customer":
                return self.ok(wholesale.add_customer(**arguments))
            case "create_wholesale_order":
                return self.ok(wholesale.create_order(**arguments))
            case "update_order_status":
                return self.ok(wholesale.update_order_status(**arguments))
            case "list_wholesale_orders":
                return self.ok({"orders": wholesale.list_orders(**arguments)})

            # ── Cafe ──────────────────────────────────────────────
            case "get_recipe":
                item = arguments.get("item", "")
                docs = kb_client.search(item, n_results=3, topic_filter="maillard-recipes")
                ctx = "\n\n".join(d["text"] for d in docs) if docs else ""
                prompt = f"From the Maillard recipe guide, provide the exact recipe for: {item}\n\nContext:\n{ctx}" if ctx else f"Provide the Maillard recipe for: {item}"
                return self.ok({"item": item, "recipe": await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)})
            case "create_shift_schedule":
                prompt = f"Create a weekly shift schedule starting {arguments.get('week_start')}. Staff: {arguments.get('staff')}. Hours: 7am-6pm."
                return self.ok({"schedule": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "equipment_checklist":
                prompt = f"Generate a {arguments.get('checklist_type')} checklist for a specialty coffee cafe."
                return self.ok({"checklist": await ask(prompt, SYSTEM_PROMPT)})

            # ── Catch-all with intelligence injection ─────────────
            case "query_operations":
                return await self._handle_query(arguments.get("query", ""))

            case _:
                return self.err(f"Unknown tool: {name}")

    async def _handle_query(self, query: str) -> dict:
        """Answer any question with auto-injected intelligence data."""
        lower = query.lower()
        ctx: list[str] = []

        if any(kw in lower for kw in ["stock", "inventory", "reorder", "supply", "how much", "what do we have", "stockout", "running low"]):
            items = inventory.list_items()
            if items:
                ctx.append(f"[INVENTORY]\n{_fmt_inv(items)}")
            preds = inventory_intelligence.predict_stockout()
            crit = [p for p in preds if p["urgency"] in ("CRITICAL", "URGENT")]
            if crit:
                ctx.append("[STOCKOUT ALERTS]\n" + "\n".join(
                    f"  {p['urgency']}: {p['name']} - {p['days_remaining']:.0f}d left ({p['daily_rate']}{p['unit']}/d)"
                    for p in crit))

        if any(kw in lower for kw in ["roast", "batch", "production", "capacity", "loss"]):
            plan = production_intelligence.recommend_next_batches()
            if plan.get("sessions"):
                ctx.append("[BATCH PLAN]\n" + "\n".join(
                    f"  [{s['priority']}] {s['total_green_kg']}kg {s['green_coffee_name']} ({s['batch_count']} batches)"
                    for s in plan["sessions"]))

        if any(kw in lower for kw in ["wholesale", "order", "customer", "delivery", "demand"]):
            gap = wholesale_intelligence.analyze_production_gap()
            if "error" not in gap:
                ctx.append(f"[WHOLESALE] Demand: {gap['total_pending_demand_kg']}kg | Available: {gap['total_available_kg']}kg | Coverage: {gap['coverage_pct']}%")

        if any(kw in lower for kw in ["waste", "spoil", "expired"]):
            waste = inventory_intelligence.detect_waste_anomalies()
            if waste.get("anomalies"):
                ctx.append(f"[WASTE] {waste['anomaly_count']} anomalies, EUR {waste.get('summary',{}).get('total_waste_cost_eur',0)}")

        docs = kb_client.search(query, n_results=3, topic_filter="maillard-recipes")
        if docs:
            ctx.append("[KNOWLEDGE BASE]\n" + "\n\n".join(d["text"] for d in docs))

        enriched = query + ("\n\n" + "\n\n".join(ctx) if ctx else "")
        return self.ok({"answer": await ask(enriched, SYSTEM_PROMPT), "tool_used": "query_operations"})


def _fmt_inv(items: list[dict]) -> str:
    by_cat: dict[str, list] = {}
    for i in items:
        by_cat.setdefault(i["category"], []).append(i)
    lines = []
    for cat, cat_items in sorted(by_cat.items()):
        lines.append(f"  [{cat.upper()}]")
        for i in cat_items:
            alert = " ** LOW **" if i.get("needs_reorder") else ""
            lines.append(f"    {i['sku']}: {i['name']} = {i['quantity']} {i['unit']}{alert}")
    return "\n".join(lines)
