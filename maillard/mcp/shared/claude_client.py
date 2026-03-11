"""
Shared Anthropic client factory with department-aware system prompts.
Preserves full coffee domain knowledge as base layer for all departments.
"""
import os
import anthropic
from loguru import logger

# ── Base coffee knowledge injected into EVERY department MCP ──────────────────
COFFEE_KNOWLEDGE_BASE = """
=== MAILLARD COFFEE ROASTERS — CORE COFFEE KNOWLEDGE ===
You have deep specialty coffee expertise:
- Espresso: 1:2.3 ratio, 22g → 50ml, 195–205F, 9–10 atm, 20–30s + 5s pre-infusion
- Steaming milk: 155–165F, no extra air (elastic, less foam)
- Frothing milk: add air first at 155–165F (more foam, less elastic)
- Cold Brew: extra-coarse grind, toddy bucket, 16h fridge, keg with water
- Sugar: A little=0.5sp | Medium=1.5sp | Sweet=3sp
- Full drink recipes in: data/maillard/recipes/Maillard coffee guide .pdf
- Menu & pricing in: data/maillard/recipes/menu maillard_NEW.pdf
- Brand guidelines in: data/maillard/guidelines/Maillard Design Guideline.pdf
=== END COFFEE KNOWLEDGE ===
"""

_client: anthropic.Anthropic | None = None


def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


def build_system_prompt(department_prompt: str) -> str:
    """Prepend core coffee knowledge to any department prompt."""
    return f"{COFFEE_KNOWLEDGE_BASE}\n\n{department_prompt}"


async def ask(
    prompt: str,
    system: str,
    model: str = "claude-haiku-4-5-20251001",
    max_tokens: int = 1024,
) -> str:
    """Simple one-shot Claude call. Department MCPs use this for AI-assisted tools."""
    import asyncio

    client = get_client()
    full_system = build_system_prompt(system)

    try:
        response = await asyncio.to_thread(
            client.messages.create,
            model=model,
            max_tokens=max_tokens,
            system=full_system,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude call failed: {e}")
        raise
