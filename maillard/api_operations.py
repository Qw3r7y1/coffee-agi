"""
Operations API routes -- /api/ops/*

REST endpoints for inventory, production, and wholesale systems.
These run alongside the MCP agent interface for direct programmatic access.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from loguru import logger

from maillard.mcp.operations import inventory, production, wholesale
from maillard.mcp.operations import inventory_intelligence, production_intelligence, wholesale_intelligence

router = APIRouter(prefix="/ops", tags=["Operations"])


# ═══════════════════════════════════════════════════════════════════════════════
# INVENTORY
# ═══════════════════════════════════════════════════════════════════════════════


class AddItemReq(BaseModel):
    sku: str
    name: str
    category: str
    unit: str
    quantity: float = 0
    min_quantity: float = 0
    cost_per_unit: float = 0
    supplier: str | None = None
    location: str = "main_store"


class StockUpdateReq(BaseModel):
    quantity: float
    mode: str = "set"


class ReceiveReq(BaseModel):
    quantity: float
    cost_per_unit: float | None = None


class UsageReq(BaseModel):
    sku: str
    quantity_used: float
    usage_type: str
    reference: str | None = None
    staff: str | None = None


class WasteReq(BaseModel):
    sku: str
    quantity_wasted: float
    reason: str
    staff: str | None = None


@router.post("/inventory")
def add_inventory_item(req: AddItemReq):
    result = inventory.add_item(**req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/inventory")
def list_inventory(category: str | None = None, location: str | None = None):
    return {"items": inventory.list_items(category=category, location=location)}


@router.get("/inventory/{sku}")
def get_inventory_item(sku: str):
    result = inventory.get_item(sku)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.put("/inventory/{sku}/stock")
def update_stock(sku: str, req: StockUpdateReq):
    result = inventory.update_stock(sku, req.quantity, req.mode)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/inventory/{sku}/receive")
def receive_stock(sku: str, req: ReceiveReq):
    result = inventory.receive_stock(sku, req.quantity, req.cost_per_unit)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/inventory/alerts/reorder")
def reorder_alerts():
    alerts = inventory.get_reorder_alerts()
    return {"alerts": alerts, "count": len(alerts)}


@router.get("/inventory/value/total")
def stock_value(category: str | None = None):
    return inventory.get_stock_value(category)


@router.post("/usage")
def log_usage(req: UsageReq):
    result = inventory.log_usage(**req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/waste")
def log_waste(req: WasteReq):
    result = inventory.log_waste(**req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/waste/summary")
def waste_summary(days: int = 30):
    return inventory.get_waste_summary(days)


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCTION
# ═══════════════════════════════════════════════════════════════════════════════


class ScheduleRoastReq(BaseModel):
    green_coffee_sku: str
    green_weight_kg: float
    roast_level: str = "medium"
    scheduled_date: str | None = None
    roaster: str | None = None


class CompleteRoastReq(BaseModel):
    roasted_weight_kg: float
    roasted_sku: str | None = None
    roast_duration_min: float | None = None
    retail_allocation_kg: float = 0
    wholesale_allocation_kg: float = 0


@router.post("/roast/schedule")
def schedule_roast(req: ScheduleRoastReq):
    result = production.schedule_roast(**req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/roast/{batch_code}/start")
def start_roast(batch_code: str, roast_temp_c: float | None = None):
    result = production.start_roast(batch_code, roast_temp_c)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/roast/{batch_code}/complete")
def complete_roast(batch_code: str, req: CompleteRoastReq):
    result = production.complete_roast(batch_code, **req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/roast/batches")
def list_batches(status: str | None = None):
    return {"batches": production.list_batches(status=status)}


@router.get("/roast/{batch_code}")
def get_batch(batch_code: str):
    result = production.get_batch(batch_code)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.get("/production/summary")
def production_summary(days: int = 30):
    return production.get_production_summary(days)


# ═══════════════════════════════════════════════════════════════════════════════
# WHOLESALE
# ═══════════════════════════════════════════════════════════════════════════════


class AddCustomerReq(BaseModel):
    name: str
    contact_person: str | None = None
    email: str | None = None
    phone: str | None = None
    address: str | None = None
    customer_type: str = "cafe"
    payment_terms: str = "net_30"


class CreateOrderReq(BaseModel):
    customer_id: int
    lines: list[dict]
    requested_delivery: str | None = None
    delivery_address: str | None = None
    notes: str | None = None


class UpdateStatusReq(BaseModel):
    new_status: str
    notes: str | None = None


@router.post("/wholesale/customers")
def add_customer(req: AddCustomerReq):
    result = wholesale.add_customer(**req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/wholesale/customers")
def list_customers():
    return {"customers": wholesale.list_customers()}


@router.post("/wholesale/orders")
def create_order(req: CreateOrderReq):
    result = wholesale.create_order(**req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/wholesale/orders")
def list_orders(status: str | None = None, customer_id: int | None = None):
    return {"orders": wholesale.list_orders(status=status, customer_id=customer_id)}


@router.get("/wholesale/orders/{order_number}")
def get_order(order_number: str):
    result = wholesale.get_order(order_number)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@router.put("/wholesale/orders/{order_number}/status")
def update_order_status(order_number: str, req: UpdateStatusReq):
    result = wholesale.update_order_status(order_number, **req.model_dump())
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/wholesale/demand")
def pending_demand():
    return wholesale.get_pending_demand()


@router.get("/wholesale/deliveries")
def delivery_schedule(days_ahead: int = 14):
    return {"schedule": wholesale.get_delivery_schedule(days_ahead)}


# =============================================================================
# INTELLIGENCE
# =============================================================================


@router.get("/intelligence/inventory")
def inventory_health(days: int = 30):
    """Full inventory intelligence: stockout predictions, reorder plan, waste anomalies."""
    return inventory_intelligence.get_inventory_health_report(days)


@router.get("/intelligence/inventory/stockout")
def stockout_forecast(days: int = 30):
    """Predict when each item will run out."""
    return {"predictions": inventory_intelligence.predict_stockout(days)}


@router.get("/intelligence/inventory/reorder")
def reorder_plan(days: int = 30):
    """Generate reorder recommendations with costs and deadlines."""
    return inventory_intelligence.get_reorder_recommendations(days)


@router.get("/intelligence/inventory/waste")
def waste_anomalies(days: int = 30):
    """Detect waste anomalies and spikes."""
    return inventory_intelligence.detect_waste_anomalies(days)


@router.get("/intelligence/production")
def production_health(days: int = 30):
    """Full production intelligence: loss analysis, capacity, batch plan."""
    return production_intelligence.get_production_health_report(days)


@router.get("/intelligence/production/losses")
def roast_loss_analysis(days: int = 30):
    """Analyze roast loss patterns and anomalies."""
    return production_intelligence.analyze_roast_losses(days)


@router.get("/intelligence/production/capacity")
def capacity_analysis(days: int = 30):
    """Roaster capacity utilization analysis."""
    return production_intelligence.get_capacity_analysis(days)


@router.get("/intelligence/production/plan")
def batch_plan():
    """Recommend what to roast next based on demand and stock."""
    return production_intelligence.recommend_next_batches()


@router.get("/intelligence/wholesale")
def wholesale_health(days: int = 90):
    """Full wholesale intelligence: demand forecast, gaps, risk, customers."""
    return wholesale_intelligence.get_wholesale_health_report(days)


@router.get("/intelligence/wholesale/forecast")
def demand_forecast(weeks: int = 4, history_days: int = 90):
    """Forecast wholesale demand by product and customer."""
    return wholesale_intelligence.forecast_demand(history_days, weeks)


@router.get("/intelligence/wholesale/gap")
def production_gap():
    """Compare pending demand against available stock and scheduled production."""
    return wholesale_intelligence.analyze_production_gap()


@router.get("/intelligence/wholesale/risk")
def delivery_risk():
    """Score each pending order for delivery risk."""
    return {"orders": wholesale_intelligence.score_delivery_risk()}


@router.get("/intelligence/wholesale/customers")
def customer_rankings(days: int = 90):
    """Rank wholesale customers by value and growth."""
    return {"rankings": wholesale_intelligence.rank_customers(days)}


# =============================================================================
# DECISION ENGINE
# =============================================================================

from maillard.mcp.operations import decision_engine
from maillard.mcp.operations import execution


# =============================================================================
# EXECUTION TRACKING
# =============================================================================


class ActionStatusReq(BaseModel):
    action_id: str
    assigned_to: str | None = None
    notes: str | None = None


@router.post("/actions/start")
def start_action(req: ActionStatusReq):
    """Mark an action as in_progress."""
    result = execution.start_action(req.action_id, req.assigned_to, req.notes)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/actions/complete")
def complete_action(req: ActionStatusReq):
    """Mark an action as completed."""
    result = execution.complete_action(req.action_id, req.notes)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/actions/skip")
def skip_action(req: ActionStatusReq):
    """Mark an action as skipped."""
    result = execution.skip_action(req.action_id, req.notes)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/actions/today")
def today_actions(status: str | None = None):
    """Get all tracked actions for today."""
    return {"actions": execution.get_today_actions(status)}


@router.get("/actions/stats")
def action_stats():
    """Get execution stats for today."""
    return execution.get_action_stats()


# =============================================================================
# APPROVAL & EXECUTION
# =============================================================================

from maillard.mcp.operations.approval import (
    add_from_dict, add_action,
    approve_action as _approve, reject_action as _reject,
    mark_executed as _execute, mark_failed as _fail,
    get_pending_actions, get_all_actions, get_queue_summary,
)
# Backward compat aliases
request_approval = add_from_dict


class ApprovalReq(BaseModel):
    action_id: str
    approved_by: str = "owner"


class RejectReq(BaseModel):
    action_id: str
    reason: str = ""


@router.post("/approval/request")
def api_request_approval(action: dict):
    """Submit an action for approval."""
    return request_approval(action)


@router.post("/approval/approve")
def api_approve(req: ApprovalReq):
    """Approve a pending action."""
    result = _approve(req.action_id, req.approved_by)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/approval/reject")
def api_reject(req: RejectReq):
    """Reject a pending action."""
    result = _reject(req.action_id, req.reason)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/approval/execute")
def api_execute(req: ApprovalReq):
    """Execute an approved action (simulated)."""
    result = _execute(req.action_id)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.post("/approval/fail")
def api_fail(req: RejectReq):
    """Mark an action as failed."""
    result = _fail(req.action_id, req.reason)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@router.get("/approval/pending")
def api_pending():
    """Get all pending actions."""
    return {"actions": get_pending_actions()}


@router.get("/approval/queue")
def api_queue_summary():
    """Get queue status counts."""
    return get_queue_summary()


@router.get("/approval/log")
def api_log(status: str | None = None):
    """Get full action queue."""
    return {"actions": get_all_actions(status)}


@router.post("/shrinkage")
def shrinkage_check(actual_counts: dict):
    """Run shrinkage check: compare actual physical counts vs system expected."""
    from maillard.mcp.operations.shrinkage import run_shrinkage_check
    return run_shrinkage_check(actual_counts)


@router.post("/approval/generate")
def api_generate_actions():
    """Generate today's actions from the decision engine and submit for approval."""
    brief = decision_engine.generate_morning_brief()
    decisions = brief.get("decisions", [])
    submitted = []
    for d in decisions:
        std = request_approval(d)
        submitted.append(std)
    return {"submitted": len(submitted), "actions": submitted}


# =============================================================================
# DECISIONS
# =============================================================================


@router.get("/decision-summary")
def decision_summary():
    """Full operator dashboard data: 6 sections + Today's Focus."""
    from maillard.mcp.operations.state_loader import get_operational_snapshot
    from maillard.mcp.operations.procurement import get_procurement_report
    from maillard.mcp.operations.cost_engine import calculate_product_costs
    from maillard.mcp.sales.intelligence import generate_sales_intelligence

    snap = get_operational_snapshot()
    proc = get_procurement_report()
    costs = calculate_product_costs()
    sales_intel = generate_sales_intelligence()
    state = snap.get("state", {})
    updated_inv = snap.get("updated_inventory", {})
    sales_today = state.get("sales_today", {})
    demand_signals = snap.get("demand_signals", {})
    risks = snap.get("inventory_risks", {})

    # 1. Sales snapshot
    total_units = sum(sales_today.values())
    top3 = sorted(sales_today.items(), key=lambda x: -x[1])[:3]

    # 2. Demand signals (only non-stable)
    demand_out = []
    for k, v in demand_signals.items():
        t = v.get("trend", "stable")
        if t != "stable":
            demand_out.append({"product": k, "trend": t, "change_pct": v.get("change_pct", 0)})
    demand_out.sort(key=lambda x: -abs(x["change_pct"]))
    demand_out = demand_out[:5]

    # 3. Cost/margin alerts (CRITICAL + LOW only)
    margin_alerts = []
    for product, data in costs.items():
        if data.get("grade") in ("CRITICAL", "LOW"):
            margin_alerts.append({
                "product": product, "grade": data["grade"],
                "margin_pct": data["margin_pct"], "action": data["action"],
            })

    # 4. Inventory status
    inv_out = {}
    for sku, info in updated_inv.items():
        status = risks.get(sku, {}).get("status", "OK")
        inv_out[sku] = {"stock": round(info["stock"], 1), "unit": info.get("unit", ""), "status": status}

    # 5. Sales intelligence
    si = {
        "push": sales_intel.get("push", []),
        "deprioritize": sales_intel.get("deprioritize", []),
    }

    # 6. Operations actions (from morning brief)
    brief = decision_engine.generate_morning_brief()
    actions = [d["action"] for d in brief.get("decisions", [])]
    focus = brief.get("focus", [])

    # Today's Focus = push items + critical inventory + top action
    todays_focus = []
    for f in focus[:2]:
        todays_focus.append(f"Push: {f}")
    for sku, r in risks.items():
        if r["status"] in ("STOCKOUT", "CRITICAL"):
            todays_focus.append(f"Fix: {sku.replace('_',' ')} ({r['status']})")
    if actions:
        todays_focus.append(f"Do first: {actions[0][:60]}")
    todays_focus = todays_focus[:4]

    return {
        "sales": {"total_units": total_units, "top_products": [{"product": k, "units": v} for k, v in top3]},
        "demand": demand_out,
        "margin_alerts": margin_alerts,
        "inventory": inv_out,
        "sales_intelligence": si,
        "actions": actions,
        "todays_focus": todays_focus,
        "procurement": proc.get("recommendations", []),
        "market": brief.get("market", "?"),
    }


@router.get("/decisions/morning-brief")
def morning_brief():
    """Owner Morning Brief: concise executive summary before opening."""
    return decision_engine.generate_morning_brief()


@router.get("/decisions/daily")
def daily_plan():
    """Daily operations plan: prioritized actions across inventory, production, wholesale."""
    return decision_engine.generate_daily_operations_plan()


@router.get("/decisions/3day")
def forecast_3day():
    """3-day operational forecast with resource projections."""
    return decision_engine.get_3_day_forecast()


@router.get("/decisions/7day")
def plan_7day():
    """7-day operational plan with production scheduling."""
    return decision_engine.get_7_day_plan()
