"""
Recipe MCP — Authoritative source of truth for all Maillard recipes.

This MCP reads every PDF in data/maillard/recipes/ at startup and provides
a BM25-grounded, recipe-accurate AI for all drink, food, and technique questions.

Cross-department API
--------------------
Any department MCP can import and call get_recipe_context() to ground its
responses in actual Maillard recipe data:

    from maillard.mcp.recipe.server import get_recipe_context

    context = get_recipe_context("how to make freddo cappuccino")
    # → returns the most relevant recipe chunks as a formatted string
"""
from __future__ import annotations

from typing import Any

from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask
from maillard.mcp.recipe.loader import get_loader
from maillard.mcp.recipe.tools import TOOLS

SYSTEM_PROMPT = """
You are the Maillard Recipe Department AI — the single authoritative source for all
Maillard Coffee Roasters recipes, menu items, and preparation techniques.

CRITICAL RULE: Answer using ONLY the Maillard recipe content provided in the context.
Never substitute general coffee knowledge, SCA standards, or outside recipes.
If the recipe is in the context, quote it exactly — cup size, shots, milk type, steps, notes.
If an item is not found in the provided context, say so clearly: "This item is not in the Maillard guide."

You have access to:
- Maillard Coffee Guide (full drink recipes, brew parameters, techniques)
- Maillard Menu (all items, sizes, prices)
""".strip()


# ── Cross-department API ──────────────────────────────────────────────────────

def get_recipe_context(query: str, n_chunks: int = 6) -> str:
    """
    Public cross-department function.
    Returns relevant Maillard recipe chunks for any query as a formatted string.

    Usage from any department:
        from maillard.mcp.recipe.server import get_recipe_context
        context = get_recipe_context("freddo cappuccino recipe")
    """
    loader = get_loader()
    chunks = loader.search(query, n=n_chunks)
    if chunks:
        return "\n\n".join(f"[{c['source']}]\n{c['text']}" for c in chunks)
    # Fallback: first 4000 chars of full text
    return loader.get_full_text()[:4000]


# ── RecipeMCP ─────────────────────────────────────────────────────────────────

class RecipeMCP(BaseMCPServer):
    department = "recipe"

    def __init__(self):
        super().__init__()
        self._loader = get_loader()

    @property
    def tools(self) -> list[dict]:
        return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name, "args": arguments})

        match name:

            case "query_recipe":
                query = arguments.get("query", "")
                context = get_recipe_context(query)
                prompt = (
                    f"Using ONLY the Maillard recipe content below, answer this question:\n"
                    f"{query}\n\n"
                    f"RECIPE CONTEXT:\n{context}"
                )
                answer = await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)
                return self.ok({"query": query, "answer": answer})

            case "lookup_drink":
                drink = arguments.get("drink", "")
                context = get_recipe_context(drink, n_chunks=8)
                prompt = (
                    f"Find and return the complete Maillard recipe for '{drink}'.\n"
                    f"Include ALL of: cup size, espresso shots, milk volume and temperature, "
                    f"technique steps in order, and any barista notes.\n"
                    f"Use ONLY the Maillard guide below. Quote exact values.\n\n"
                    f"RECIPE CONTEXT:\n{context}"
                )
                recipe = await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)
                return self.ok({"drink": drink, "recipe": recipe})

            case "lookup_food":
                item = arguments.get("item", "")
                context = get_recipe_context(item, n_chunks=5)
                prompt = (
                    f"Find the complete Maillard recipe and description for food item '{item}'.\n"
                    f"Include ingredients, preparation steps, and any notes.\n"
                    f"Use ONLY the Maillard guide below.\n\n"
                    f"RECIPE CONTEXT:\n{context}"
                )
                result = await ask(prompt, SYSTEM_PROMPT, max_tokens=800)
                return self.ok({"item": item, "details": result})

            case "get_menu":
                category = arguments.get("category", "all")
                context = get_recipe_context("menu prices all items drinks food crepe parfait", n_chunks=10)
                prompt = (
                    f"List the complete Maillard menu"
                    f"{' — drinks only' if category == 'drinks' else ' — food only' if category == 'food' else ''}.\n"
                    f"Include item names, sizes, and prices. Organise by category.\n"
                    f"Use ONLY the content below.\n\n"
                    f"CONTEXT:\n{context}"
                )
                menu = await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)
                return self.ok({"category": category, "menu": menu, "sources": self._loader.get_sources()})

            case "get_technique":
                technique = arguments.get("technique", "")
                context = get_recipe_context(technique, n_chunks=6)
                prompt = (
                    f"Explain the Maillard technique for '{technique}'.\n"
                    f"Include: key parameters (temp, time, ratios), step-by-step instructions, "
                    f"and quality/consistency notes.\n"
                    f"Use ONLY the Maillard guide below.\n\n"
                    f"CONTEXT:\n{context}"
                )
                explanation = await ask(prompt, SYSTEM_PROMPT, max_tokens=1000)
                return self.ok({"technique": technique, "explanation": explanation})

            case "list_sources":
                return self.ok({
                    "sources": self._loader.get_sources(),
                    "total_chunks": self._loader.total_chunks(),
                    "recipes_dir": str(self._loader.__class__.__module__),
                })

            case _:
                return self.err(f"Unknown tool: {name}")
