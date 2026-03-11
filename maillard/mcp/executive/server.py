"""Executive MCP — Strategy, decisions, OKRs, cross-department oversight, escalations."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask, build_system_prompt

SYSTEM_PROMPT = """
You are the Maillard Executive AI.
You operate at the strategic level. You have full visibility across all departments.
Responsibilities: strategic planning, OKR setting, investment decisions, partnership evaluation,
brand direction, executive reporting, escalation resolution, and cross-departmental prioritization.

Maillard's mission: provide top coffee products and equipment with the expectation of inspiring
more people to the world of Specialty Coffee.
Tagline: "Distinctive Flavor Coffee Roasters"

Think at the 30,000-foot level. Be decisive. Surface trade-offs. Prioritize growth, brand integrity,
and team excellence. All major decisions must consider brand impact first.
"""

TOOLS: list[dict] = [
    {
        "name": "strategic_brief",
        "description": "Generate a strategic brief or recommendation on a business initiative.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "initiative": {"type": "string"},
                "context": {"type": "string"},
                "stakeholders": {"type": "array", "items": {"type": "string"}},
                "timeline": {"type": "string"}
            },
            "required": ["initiative"]
        }
    },
    {
        "name": "set_okrs",
        "description": "Generate OKRs for a department or company for a given period.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "department": {"type": "string"},
                "period": {"type": "string"},
                "company_priorities": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["department", "period"]
        }
    },
    {
        "name": "escalation_decision",
        "description": "Provide executive-level guidance on an escalated cross-department issue.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "issue": {"type": "string"},
                "departments_involved": {"type": "array", "items": {"type": "string"}},
                "urgency": {"type": "string", "enum": ["low", "medium", "high", "critical"]}
            },
            "required": ["issue", "departments_involved"]
        }
    },
    {
        "name": "query_executive",
        "description": "Answer a strategic or executive-level business question.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class ExecutiveMCP(BaseMCPServer):
    department = "executive"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        # Executive uses the more capable model
        match name:
            case "strategic_brief":
                prompt = f"Provide a strategic brief on: {arguments.get('initiative')}. Context: {arguments.get('context', '')}. Stakeholders: {arguments.get('stakeholders', [])}. Timeline: {arguments.get('timeline', 'TBD')}. Include recommendation, risks, and success metrics."
                return self.ok({"brief": await ask(prompt, SYSTEM_PROMPT, model="claude-opus-4-6", max_tokens=2000)})
            case "set_okrs":
                prompt = f"Define OKRs for the {arguments.get('department')} department for {arguments.get('period')}. Company priorities: {arguments.get('company_priorities', [])}. Format as 3 Objectives with 2-3 Key Results each."
                return self.ok({"okrs": await ask(prompt, SYSTEM_PROMPT, model="claude-opus-4-6", max_tokens=1500)})
            case "escalation_decision":
                prompt = f"Executive decision needed. Issue: {arguments.get('issue')}. Departments: {arguments.get('departments_involved')}. Urgency: {arguments.get('urgency', 'medium')}. Provide a clear decision, rationale, and action plan."
                return self.ok({"decision": await ask(prompt, SYSTEM_PROMPT, model="claude-opus-4-6", max_tokens=1500)})
            case "query_executive":
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT, model="claude-opus-4-6", max_tokens=2000)})
            case _:
                return self.err(f"Unknown tool: {name}")
