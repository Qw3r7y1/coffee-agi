"""Legal MCP — Contracts, compliance, trademarks, regulatory affairs."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Legal Department AI.
Responsibilities: contract review, regulatory compliance, trademark protection,
food safety regulations, vendor agreements, employment law, and GDPR/data privacy.
Always flag legal risks clearly. Never provide definitive legal advice — recommend attorney review
for binding decisions. Apply US and EU food service regulations where relevant.
"""

TOOLS: list[dict] = [
    {
        "name": "review_contract",
        "description": "Review a contract or agreement for risks and key terms.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "contract_text": {"type": "string"},
                "contract_type": {"type": "string", "enum": ["vendor", "employment", "lease", "nda", "wholesale", "franchise"]}
            },
            "required": ["contract_text", "contract_type"]
        }
    },
    {
        "name": "check_compliance",
        "description": "Check a business practice or product against applicable regulations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "practice": {"type": "string"},
                "jurisdiction": {"type": "string", "enum": ["US", "EU", "NY", "CA", "GR"]},
                "domain": {"type": "string", "enum": ["food_safety", "employment", "data_privacy", "trademark", "import_export"]}
            },
            "required": ["practice", "domain"]
        }
    },
    {
        "name": "draft_nda",
        "description": "Draft a non-disclosure agreement.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "party_a": {"type": "string"},
                "party_b": {"type": "string"},
                "purpose": {"type": "string"},
                "duration_years": {"type": "integer", "default": 2}
            },
            "required": ["party_a", "party_b", "purpose"]
        }
    },
    {
        "name": "query_legal",
        "description": "Answer a general legal question for Maillard.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class LegalMCP(BaseMCPServer):
    department = "legal"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "review_contract":
                prompt = f"Review this {arguments.get('contract_type')} contract and identify key risks, obligations, and recommended changes:\n\n{arguments.get('contract_text', '')[:3000]}"
                return self.ok({"review": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "check_compliance":
                prompt = f"Check if the following practice complies with {arguments.get('domain')} regulations in {arguments.get('jurisdiction', 'US')}:\n{arguments.get('practice')}"
                return self.ok({"compliance_check": await ask(prompt, SYSTEM_PROMPT)})
            case "draft_nda":
                prompt = f"Draft an NDA between {arguments.get('party_a')} and {arguments.get('party_b')} for: {arguments.get('purpose')}. Duration: {arguments.get('duration_years', 2)} years."
                return self.ok({"nda_draft": await ask(prompt, SYSTEM_PROMPT, max_tokens=2000)})
            case "query_legal":
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT)})
            case _:
                return self.err(f"Unknown tool: {name}")
