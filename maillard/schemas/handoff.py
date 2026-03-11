"""Shared Pydantic schemas for inter-department handoffs and API contracts."""
from pydantic import BaseModel
from typing import Any


class DispatchRequest(BaseModel):
    task: str
    tool_name: str | None = None
    arguments: dict[str, Any] | None = None
    department: str | None = None  # None = auto-route


class DispatchResponse(BaseModel):
    status: str
    department: str
    deliverable_type: str | None = None
    brand_guidelines_loaded: bool | None = None
    brand_sources_used: list | None = None
    data: Any = None
    error: str | None = None
    chained_result: dict | None = None


class ToolListResponse(BaseModel):
    department: str
    tools: list[dict]


class DepartmentListResponse(BaseModel):
    departments: list[str]
