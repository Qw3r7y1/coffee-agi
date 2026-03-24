"""
Maillard MCP — FastAPI router.
Mount this into the main app via: app.include_router(maillard_router, prefix="/mcp")
"""
from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel

from maillard.mcp.orchestrator.server import OrchestratorMCP
from maillard.schemas.handoff import (
    DispatchRequest,
    DispatchResponse,
    ToolListResponse,
    DepartmentListResponse,
)

router = APIRouter(tags=["MCP"])
_orchestrator = OrchestratorMCP()


@router.get("/departments", response_model=DepartmentListResponse)
def list_departments():
    """List all available MCP departments."""
    return DepartmentListResponse(departments=_orchestrator.list_departments())


@router.get("/tools/{department}", response_model=ToolListResponse)
def list_tools(department: str):
    """List all tools for a given department."""
    tools = _orchestrator.list_tools(department)
    if not tools and department not in _orchestrator.list_departments():
        raise HTTPException(404, f"Department '{department}' not found.")
    return ToolListResponse(department=department, tools=tools)


@router.post("/dispatch", response_model=DispatchResponse)
async def dispatch(req: DispatchRequest):
    """
    Dispatch a task to the appropriate department MCP.
    If department is omitted, the orchestrator auto-routes based on task content.
    """
    try:
        result = await _orchestrator.dispatch(
            task=req.task,
            tool_name=req.tool_name,
            arguments=req.arguments,
            department=req.department,
        )
        return DispatchResponse(**result) if isinstance(result, dict) else DispatchResponse(
            status="ok", department=req.department or "auto", data=result
        )
    except Exception as e:
        logger.error(f"MCP dispatch error: {e}")
        raise HTTPException(500, str(e))


@router.get("/route")
def route_preview(task: str):
    """Preview which department would handle a given task (without executing)."""
    dept = _orchestrator.route(task)
    return {"task": task, "routed_to": dept}


@router.get("/state-check")
def state_check():
    """Diagnostic: verify current_state.json freshness and live data status."""
    from maillard.mcp.operations.state_loader import get_state_meta, load_current_state
    from maillard.sync_loop import get_sales_sync_status
    meta = get_state_meta()
    state = load_current_state()
    sync = get_sales_sync_status()
    return {
        "meta": meta,
        "sync": sync,
        "sales_today_sample": dict(list(state.get("sales_today", {}).items())[:5]),
        "top_items_count": len(state.get("top_items", [])),
        "raw_order_count": state.get("raw_order_count", 0),
        "live_data_active": sync["status"] in ("live", "syncing"),
    }


@router.get("/sync-status")
def sync_status():
    """Return current sales sync status for dashboard polling."""
    from maillard.sync_loop import get_sales_sync_status
    return get_sales_sync_status()


@router.post("/sync-now")
def sync_now():
    """Trigger an immediate sync (runs in background thread)."""
    from maillard.sync_loop import _do_sync
    from threading import Thread
    t = Thread(target=_do_sync, daemon=True, name="manual-sync")
    t.start()
    return {"triggered": True, "message": "Sync started in background"}


class ChatMCPRequest(BaseModel):
    message: str
    department: str | None = None
    session_id: str = "default"


SUMMARIZER_SYSTEM = (
    "You are the Maillard Coffee Roasters AI agent. "
    "The user asked a question and our systems produced the data below. "
    "Turn it into a short, direct answer.\n\n"
    "STRICT RULES:\n"
    "- Use the EXACT numbers from the data. Never round aggressively.\n"
    "- Lead with the number or direct answer, not filler.\n"
    "- For market data: show price, change, FX, recommendation.\n"
    "- If the data contains an error key, say 'Live feed unavailable' — do NOT guess.\n"
    "- FINANCE SAFETY (for price/cost/invoice/vendor answers):\n"
    "  - Quote ONLY exact dollar amounts from the data.\n"
    "  - NEVER invent prices, surcharges, or estimates.\n"
    "  - NEVER say 'typically', 'usually', 'around', 'approximately' for costs.\n"
    "  - NEVER mention menu prices unless user asked about retail.\n"
    "  - If source=unavailable, say 'No verified local data available.' — do NOT guess.\n"
    "  - If ambiguous=true, show whatever data exists THEN ask which meaning they intended.\n"
    "  - NEVER say 'Great news!', never upsell, never give generic advice for a price question.\n"
    "  - Include source info when available: vendor name, invoice date.\n"
    "- Be concise. Use markdown. No filler."
)


@router.post("/chat")
async def chat_mcp(req: ChatMCPRequest):
    """
    Natural-language chat through the MCP orchestrator.
    Data-bound queries hit local data FIRST via the resolver.
    """
    import json as _json
    import anthropic
    from maillard.mcp.shared.data_resolver import resolve_data_bound_query, detect_intent, is_ambiguous

    query = req.message

    # ── Step 1: Try data resolver for data-bound queries ──
    resolved = resolve_data_bound_query(query)

    if resolved:
        intent = resolved["intent"]
        logger.info(f"[CHAT] Data-bound: intent={intent} conf={resolved['confidence']} src={resolved['source']} ambiguous={resolved['ambiguous']}")

        # ── Square live check → execute-first, return structured result ──
        if intent == "square_live_check":
            data = resolved["data"]
            status = data.get("status", "unknown")
            debug = data.get("debug", {})

            if status == "connected_live":
                orders = data.get("orders_today", 0)
                top = data.get("top_items", {})
                top_str = "\n".join(f"  {k}: {v}" for k, v in list(top.items())[:5]) if top else "  (sample only)"
                response = (
                    f"**Square is live.**\n\n"
                    f"- Orders today: **{orders}**\n"
                    f"- Top items (sample):\n{top_str}\n\n"
                    f"Debug: token={debug.get('token_loaded')}, "
                    f"location={debug.get('location_loaded')}, "
                    f"api_called={debug.get('api_called')}, "
                    f"orders_returned={debug.get('orders_returned')}"
                )
            else:
                reason = data.get("reason", "unknown")
                response = (
                    f"**Square connection failed.**\n\n"
                    f"- Reason: `{reason}`\n"
                    f"- Error: {data.get('error', 'none')}\n\n"
                    f"Debug: token={debug.get('token_loaded')}, "
                    f"location={debug.get('location_loaded')}, "
                    f"api_called={debug.get('api_called')}"
                )

            return {
                "response": response,
                "department": "sales",
                "session_id": req.session_id,
                "square_check": data,
            }

        # ── Ambiguous buy/price → clarification with whatever data we have ──
        if resolved["ambiguous"] or intent in ("ambiguous_buy", "ambiguous_price"):
            data_block = resolved["data_text"]

            if resolved["confidence"] == "unavailable":
                return {
                    "response": (
                        f"Could you clarify what you mean by \"{query}\"?\n\n"
                        f"1. **Latest price paid** — what we paid our supplier (from invoices)\n"
                        f"2. **How much to order** — reorder quantity based on stock levels\n"
                        f"3. **Product COGS** — our total cost to produce it\n"
                        f"4. **Menu price** — what we charge customers"
                    ),
                    "department": "accounting",
                    "session_id": req.session_id,
                }

            # Have data → show it and ask for clarification
            client = anthropic.Anthropic()
            try:
                resp = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=1024,
                    system=SUMMARIZER_SYSTEM,
                    messages=[{
                        "role": "user",
                        "content": (
                            f"User question: {query}\n\n"
                            f"This is AMBIGUOUS. Show the relevant data, then ask:\n"
                            f"'Did you mean: (1) the latest price paid to suppliers, or (2) how much to order based on stock?'\n\n"
                            f"Data [source={resolved['source']}]:\n{data_block[:4000]}"
                        ),
                    }],
                )
                answer = resp.content[0].text
            except Exception as e:
                answer = f"I found some data but I'm not sure what you're asking.\n\n{data_block[:500]}"

            return {"response": answer, "department": "accounting", "session_id": req.session_id}

        # ── Reorder/quantity intent → use inventory data ──
        if intent == "reorder_quantity":
            data_block = resolved["data_text"]
            if resolved["confidence"] == "verified" and data_block:
                client = anthropic.Anthropic()
                try:
                    resp = client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=1024,
                        system=SUMMARIZER_SYSTEM,
                        messages=[{
                            "role": "user",
                            "content": (
                                f"User question: {query}\n\n"
                                f"This is a REORDER/QUANTITY question. Use inventory data to suggest amounts.\n\n"
                                f"Data [source={resolved['source']}]:\n{data_block[:4000]}"
                            ),
                        }],
                    )
                    answer = resp.content[0].text
                except Exception as e:
                    answer = data_block or "Check inventory levels."
                return {"response": answer, "department": "operations", "session_id": req.session_id}

        # ── Verified price/vendor/invoice data → format directly ──
        if resolved["confidence"] == "verified":
            client = anthropic.Anthropic()
            data_block = resolved["data_text"]

            try:
                resp = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=1024,
                    system=SUMMARIZER_SYSTEM,
                    messages=[{
                        "role": "user",
                        "content": (
                            f"User question: {query}\n\n"
                            f"Data [source={resolved['source']}, confidence=verified]:\n{data_block[:4000]}"
                        ),
                    }],
                )
                answer = resp.content[0].text
            except Exception as e:
                logger.warning(f"Summarizer failed: {e}")
                answer = data_block or "Data found but formatting failed."

            price_intents = ("price_lookup", "compare_vendors", "vendor_lookup", "invoice_lookup", "margin")
            dept = "sales" if intent == "live_sales" else ("accounting" if intent in price_intents else "operations")
            return {"response": answer, "department": dept, "session_id": req.session_id}

        elif resolved["confidence"] == "unavailable":
            # Data-bound query but no data found
            logger.info(f"[CHAT] Data-bound but no data — safe fallback (intent={intent})")
            if intent == "live_sales":
                msg = "Live Square sales data is not currently loaded. Run `python scripts/run_minute_sync.py` to sync."
            else:
                msg = "No verified local data available for this query. Try syncing Dropbox invoices or asking about a specific vendor/product we have on file."
            return {
                "response": msg,
                "department": "sales" if intent == "live_sales" else "accounting",
                "session_id": req.session_id,
            }

    # ── Step 2: Non-data-bound → normal department dispatch ──
    dept = req.department or _orchestrator.route(query)
    logger.info(f"[CHAT] Non-data-bound dispatch -> {dept}")

    try:
        raw = await _orchestrator.dispatch(task=query, department=dept)
    except Exception as e:
        logger.error(f"MCP chat dispatch error: {e}")
        raise HTTPException(500, str(e))

    raw_str = _json.dumps(raw, indent=2, default=str) if isinstance(raw, dict) else str(raw)
    client = anthropic.Anthropic()
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SUMMARIZER_SYSTEM,
            messages=[{
                "role": "user",
                "content": (
                    f"User question: {query}\n\n"
                    f"Department: {dept}\n\n"
                    f"Raw MCP response:\n```json\n{raw_str[:4000]}\n```"
                ),
            }],
        )
        answer = resp.content[0].text
    except Exception as e:
        logger.warning(f"Claude summarisation failed, returning raw: {e}")
        answer = raw_str

    return {"response": answer, "department": dept, "session_id": req.session_id}
