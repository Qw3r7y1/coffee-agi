"""HR MCP — Hiring, onboarding, payroll, performance, culture, training."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Human Resources Department AI.
Responsibilities: recruitment, onboarding, barista training programs, performance reviews,
payroll support, benefits administration, employee relations, and culture development.
Maillard values: craft, quality, team excellence, specialty coffee passion, customer care.
Apply food service labor laws (US/NY). All sensitive employee data must be handled with strict confidentiality.
"""

TOOLS: list[dict] = [
    {
        "name": "generate_job_posting",
        "description": "Generate a job posting for a Maillard role.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "role": {"type": "string"},
                "type": {"type": "string", "enum": ["full_time", "part_time", "seasonal"]},
                "location": {"type": "string"},
                "requirements": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["role", "type"]
        }
    },
    {
        "name": "create_onboarding_plan",
        "description": "Create an onboarding plan for a new Maillard employee.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "role": {"type": "string"},
                "start_date": {"type": "string"},
                "duration_days": {"type": "integer", "default": 14}
            },
            "required": ["role"]
        }
    },
    {
        "name": "performance_review_template",
        "description": "Generate a performance review template for a given role.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "role": {"type": "string"},
                "review_period": {"type": "string"},
                "focus_areas": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["role"]
        }
    },
    {
        "name": "query_hr",
        "description": "Answer an HR or people management question.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class HRMCP(BaseMCPServer):
    department = "hr"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "generate_job_posting":
                prompt = f"Write a premium job posting for a {arguments.get('type')} {arguments.get('role')} at Maillard Coffee Roasters in {arguments.get('location', 'New York')}. Requirements: {arguments.get('requirements', [])}. Reflect the Maillard brand voice: craft, quality, specialty coffee passion."
                return self.ok({"job_posting": await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)})
            case "create_onboarding_plan":
                prompt = f"Create a {arguments.get('duration_days', 14)}-day onboarding plan for a new {arguments.get('role')} starting {arguments.get('start_date', 'soon')} at Maillard Coffee Roasters. Include training milestones, coffee knowledge, brand values, and operational readiness checkpoints."
                return self.ok({"onboarding_plan": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "performance_review_template":
                prompt = f"Create a performance review template for a {arguments.get('role')} at Maillard for {arguments.get('review_period', 'quarterly')}. Focus areas: {arguments.get('focus_areas', ['quality', 'speed', 'customer service', 'team collaboration'])}."
                return self.ok({"template": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "query_hr":
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT)})
            case _:
                return self.err(f"Unknown tool: {name}")
