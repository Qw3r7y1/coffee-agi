"""Marketing MCP — Campaigns, social media, content, brand voice, promotions."""
from __future__ import annotations
from typing import Any
from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask

SYSTEM_PROMPT = """
You are the Maillard Marketing Department AI.
Responsibilities: campaign strategy, social media content, email marketing,
promotional calendar, brand voice management, influencer coordination, and launch planning.

Brand voice: premium, knowledgeable, warm but not casual, specialty-forward.
Tagline: "Distinctive Flavor Coffee Roasters"
Channels: Instagram, email, in-café, partnerships.
Never use generic coffee clichés. Always connect marketing to craft, origin, and quality.
Collaborate with Designer for all visual assets.
"""

TOOLS: list[dict] = [
    {
        "name": "create_campaign_brief",
        "description": "Create a marketing campaign brief.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "campaign_name": {"type": "string"},
                "objective": {"type": "string"},
                "product_focus": {"type": "string"},
                "channels": {"type": "array", "items": {"type": "string"}},
                "timeline_weeks": {"type": "integer"},
                "budget": {"type": "number"}
            },
            "required": ["campaign_name", "objective"]
        }
    },
    {
        "name": "generate_social_content",
        "description": "Generate social media content for a product, event, or promotion.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "platform": {"type": "string", "enum": ["instagram", "facebook", "tiktok", "email", "all"]},
                "subject": {"type": "string"},
                "tone": {"type": "string", "enum": ["educational", "promotional", "storytelling", "behind_the_scenes"]},
                "count": {"type": "integer", "default": 3}
            },
            "required": ["platform", "subject"]
        }
    },
    {
        "name": "promotional_calendar",
        "description": "Generate a promotional calendar for a given period.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "period": {"type": "string"},
                "themes": {"type": "array", "items": {"type": "string"}},
                "product_launches": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["period"]
        }
    },
    {
        "name": "query_marketing",
        "description": "Answer a marketing strategy or content question.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


class MarketingMCP(BaseMCPServer):
    department = "marketing"

    @property
    def tools(self): return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name})
        match name:
            case "create_campaign_brief":
                prompt = f"Write a marketing campaign brief for '{arguments.get('campaign_name')}'. Objective: {arguments.get('objective')}. Product: {arguments.get('product_focus', 'Maillard coffee')}. Channels: {arguments.get('channels', ['instagram', 'email'])}. Timeline: {arguments.get('timeline_weeks', 4)} weeks. Budget: ${arguments.get('budget', 'TBD')}."
                return self.ok({"brief": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "generate_social_content":
                prompt = f"Generate {arguments.get('count', 3)} {arguments.get('tone', 'educational')} {arguments.get('platform')} posts about: {arguments.get('subject')}. Use Maillard brand voice: premium, specialty-forward, warm but professional."
                return self.ok({"content": await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)})
            case "promotional_calendar":
                prompt = f"Create a promotional calendar for {arguments.get('period')}. Themes: {arguments.get('themes', [])}. Product launches: {arguments.get('product_launches', [])}. Include weekly focus areas, content hooks, and channel priorities."
                return self.ok({"calendar": await ask(prompt, SYSTEM_PROMPT, max_tokens=2000)})
            case "query_marketing":
                return self.ok({"answer": await ask(arguments.get("query", ""), SYSTEM_PROMPT)})
            case _:
                return self.err(f"Unknown tool: {name}")
