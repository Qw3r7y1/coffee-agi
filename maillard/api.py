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


class ChatMCPRequest(BaseModel):
    message: str
    department: str | None = None
    session_id: str = "default"


@router.post("/chat")
async def chat_mcp(req: ChatMCPRequest):
    """
    Natural-language chat through the MCP orchestrator.
    Dispatches to the right department, then uses Claude to turn
    the raw MCP result into a conversational reply.
    """
    import json as _json
    import anthropic

    # Dispatch
    dept = req.department or _orchestrator.route(req.message)
    try:
        raw = await _orchestrator.dispatch(
            task=req.message,
            department=dept,
        )
    except Exception as e:
        logger.error(f"MCP chat dispatch error: {e}")
        raise HTTPException(500, str(e))

    # Summarise with Claude
    raw_str = _json.dumps(raw, indent=2, default=str) if isinstance(raw, dict) else str(raw)
    client = anthropic.Anthropic()
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=(
                "You are the Maillard Coffee Roasters AI agent. "
                "The user asked a question and one of our internal departments just produced "
                "the raw data below. Turn it into a helpful, conversational answer. "
                "Be concise, use markdown-style formatting where helpful. "
                "If the data contains an error, explain it clearly."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"User question: {req.message}\n\n"
                        f"Department: {dept}\n\n"
                        f"Raw MCP response:\n```json\n{raw_str[:4000]}\n```"
                    ),
                }
            ],
        )
        answer = resp.content[0].text
    except Exception as e:
        logger.warning(f"Claude summarisation failed, returning raw: {e}")
        answer = raw_str

    return {"response": answer, "department": dept, "session_id": req.session_id}
