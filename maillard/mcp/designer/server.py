"""
Designer MCP Server — Creative authority for Maillard Coffee Roasters.
Source of truth: data/maillard/guidelines/
"""
from __future__ import annotations
import json
import re
from typing import Any

from maillard.mcp.shared.base_server import BaseMCPServer
from maillard.mcp.shared.claude_client import ask, build_system_prompt
from maillard.mcp.designer.tools import TOOLS
from maillard.mcp.designer import generators
from maillard.mcp.designer.prompts import (
    SYSTEM_PROMPT,
    PACKAGING_BRIEF_PROMPT,
    IMAGE_PROMPT_TEMPLATE,
    AUDIT_PROMPT,
)
from maillard.mcp.designer import resources


# ── JSON parsing helper ───────────────────────────────────────────────────────

def _extract_json(text: str) -> dict | None:
    """Extract and parse the first JSON object from a Claude response."""
    # Try fenced code block — greedy inner match so nested braces are captured
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # Try raw JSON — find from first { to last }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    return None


_BRIEF_FIELDS = [
    "title", "design_concept", "layout_structure", "typography_hierarchy",
    "color_usage", "production_considerations", "brand_compliance",
]

# Speculative phrases scrubbed from all output — replaced with PROVISIONAL flag
_SPECULATIVE_PATTERNS = [
    (re.compile(r"\bassumed?\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\bassuming\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\blikely\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\bprobably\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\bappears to be\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\btypical(?:ly)? of\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\bconsistent with what\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
    (re.compile(r"\bsimilar to\b", re.IGNORECASE), "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"),
]


def _scrub_speculative(value: Any) -> Any:
    """Replace any remaining speculative phrases in a string field value."""
    if not isinstance(value, str):
        return value
    for pattern, replacement in _SPECULATIVE_PATTERNS:
        value = pattern.sub(replacement, value)
    return value


def _classify_field(value: str) -> dict:
    """
    Wrap a brief field string in a structured object for machine consumption.

    Returns:
        value   — the raw string with inline classification tags
        sources — list of guideline files referenced via [FROM: filename] tags
        status  — "confirmed" | "partial" | "provisional" | "vendor_dependent"
    """
    if not isinstance(value, str):
        return {"value": value, "sources": [], "status": "unknown"}

    has_from = bool(re.search(r"\[FROM:", value))
    has_provisional = "[PROVISIONAL" in value
    has_vendor = "[RECOMMENDED FORMAT" in value
    has_decision = "[DESIGN DECISION]" in value

    if has_provisional:
        status = "provisional"
    elif has_vendor and not has_from and not has_decision:
        status = "vendor_dependent"
    elif has_from and not has_provisional and not has_decision:
        status = "confirmed"
    else:
        status = "partial"

    sources = list({s.strip() for s in re.findall(r"\[FROM:\s*([^\]]+)\]", value)})

    return {"value": value, "sources": sources, "status": status}


class DesignerMCP(BaseMCPServer):
    department = "designer"

    @property
    def tools(self) -> list[dict]:
        return TOOLS

    async def handle_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        self._audit.log("tool_called", {"tool": name, "args": arguments})

        match name:

            case "load_brand_system":
                section = arguments.get("section", "all")
                brand = resources.load_brand_system()
                assets = resources.list_brand_assets()
                if section != "all":
                    filtered = {k: v for k, v in brand.items() if section.lower() in k.lower()}
                    return self.ok({"section": section, "resources": filtered, "assets": assets})
                return self.ok({"resources": brand, "assets": assets})

            case "get_brand_rules":
                context = arguments.get("context", "print")
                brand_text = resources.get_brand_text()
                prompt = (
                    f"Based on the Maillard brand guidelines below, provide the specific design rules "
                    f"that apply to the '{context}' context. Be exact — quote guidelines where possible. "
                    f"Do not invent rules not present in the guidelines.\n\n"
                    f"BRAND GUIDELINES:\n{brand_text or 'No guidelines extracted yet.'}"
                )
                rules = await ask(prompt, SYSTEM_PROMPT)
                return self.ok({"context": context, "rules": rules})

            case "audit_creative_output":
                submission = arguments.get("submission", "")
                deliverable_type = arguments.get("deliverable_type", "")
                brand_text = resources.get_brand_text()
                audit_prompt = AUDIT_PROMPT.format(
                    submission=submission,
                    deliverable_type=deliverable_type,
                )
                if brand_text:
                    audit_prompt += f"\n\nBRAND GUIDELINES:\n{brand_text[:3000]}"
                report = await ask(audit_prompt, SYSTEM_PROMPT, max_tokens=1500)
                return self.ok({
                    "deliverable_type": deliverable_type,
                    "submission": submission[:200],
                    "audit_report": report,
                })

            case "generate_packaging_brief":
                product = arguments.get("product", "")
                fmt = arguments.get("format", "bag")
                size = arguments.get("size", "250g")
                audience = arguments.get("audience", "specialty coffee enthusiasts")
                key_message = arguments.get("key_message", "Distinctive flavor, premium quality")

                brand_text = resources.get_brand_text()
                sources_manifest = resources.get_brand_sources_used()
                guidelines_loaded = bool(brand_text)

                brand_content = brand_text[:4000] if brand_text else (
                    "No brand data available. Mark all brand-specific values as: "
                    "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL]"
                )
                unavailable = "\n".join(
                    f"- {s['file']} (governs: {', '.join(s['governs'])})"
                    for s in sources_manifest if not s["available"]
                ) or "None — all expected resources loaded."

                brief_prompt = PACKAGING_BRIEF_PROMPT.format(
                    product=product,
                    format=fmt,
                    size=size,
                    audience=audience,
                    key_message=key_message,
                    brand_content=brand_content,
                    missing_resources=unavailable,
                )

                raw = await ask(brief_prompt, SYSTEM_PROMPT, max_tokens=6000)

                parsed = _extract_json(raw)
                if parsed:
                    data = {
                        f: _classify_field(_scrub_speculative(
                            parsed.get(f, "[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL] Not generated.")
                        ))
                        for f in _BRIEF_FIELDS
                    }
                else:
                    data = {f: _classify_field("[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL] Parse failed.")
                            for f in _BRIEF_FIELDS}
                    data["design_concept"] = _classify_field(_scrub_speculative(raw))

                result = self.ok(data)
                result["deliverable_type"] = fmt
                result["brand_guidelines_loaded"] = guidelines_loaded
                result["brand_sources_used"] = sources_manifest
                return result

            case "query_designer":
                query = arguments.get("query", "")
                brand_text = resources.get_brand_text()
                sources_manifest = resources.get_brand_sources_used()
                brand_content = brand_text[:3000] if brand_text else (
                    "No brand data available. Base answers on confirmed Maillard brand values: "
                    "premium, minimal, specialty-forward. Tagline: Distinctive Flavor Coffee Roasters."
                )
                unavailable_files = [s["file"] for s in sources_manifest if not s["available"]]
                prompt = (
                    f"=== BRAND SOURCE FILE: brand_extracted.md ===\n{brand_content}\n=== END ===\n\n"
                    f"UNAVAILABLE SOURCE FILES: {', '.join(unavailable_files) or 'none'}\n\n"
                    f"DESIGN REQUEST: \"{query}\"\n\n"
                    f"Provide structured creative direction using the four-tier classification:\n"
                    f"[FROM: filename] — fact from a loaded source file\n"
                    f"[DESIGN DECISION] — department recommendation\n"
                    f"[RECOMMENDED FORMAT — CONFIRM WITH VENDOR] — production/vendor specs\n"
                    f"[PROVISIONAL — REQUIRES DESIGN DIRECTOR APPROVAL] — missing specific values\n"
                    f"No speculative language. No inference presented as fact."
                )
                answer = await ask(prompt, SYSTEM_PROMPT, max_tokens=1500)
                result = self.ok({
                    "query": query,
                    "design_direction": answer,
                })
                result["brand_guidelines_loaded"] = bool(brand_text)
                result["brand_sources_used"] = sources_manifest
                return result

            case "generate_image_prompt":
                img_prompt = IMAGE_PROMPT_TEMPLATE.format(
                    subject=arguments.get("subject", ""),
                    usage=arguments.get("usage", "social_media"),
                    mood=arguments.get("mood", "premium"),
                )
                result = await ask(img_prompt, SYSTEM_PROMPT)
                return self.ok({
                    "subject": arguments.get("subject"),
                    "image_prompt": result,
                })

            case "generate_packaging_layout":
                import asyncio
                result = await asyncio.to_thread(
                    generators.generate_packaging_layout,
                    product=arguments.get("product", ""),
                    size=arguments.get("size", "250g"),
                    brand=arguments.get("brand", "Maillard"),
                    style=arguments.get("style", "minimal premium specialty coffee"),
                )
                return self.ok(result)

            case "generate_design_image":
                # Real image generation via API cascade
                from maillard.mcp.marketing.media_pipeline import generate_image
                result = await generate_image(
                    prompt=arguments.get("prompt") or arguments.get("subject", "specialty coffee"),
                    style=arguments.get("style", "cinematic"),
                )
                return self.ok(result)

            case "generate_instagram_post":
                from maillard.mcp.marketing.media_pipeline import create_viral_post
                topic = arguments.get("topic", "specialty coffee")
                result = await create_viral_post(topic)

                # Flatten for clean output
                image = result.get("image", {})
                video = result.get("video", {})
                content = result

                return self.ok({
                    "image": image.get("path"),
                    "video": video.get("path"),
                    "caption": content.get("caption", ""),
                    "hashtags": content.get("hashtags", []),
                    "hook": content.get("hook", ""),
                    "image_status": image["status"],
                    "video_status": video["status"],
                })

            case "generate_product_mockup":
                import asyncio
                result = await asyncio.to_thread(
                    generators.generate_product_mockup,
                    product=arguments.get("product", ""),
                    design_file=arguments.get("design_file", ""),
                    size=arguments.get("size", "250g"),
                )
                return self.ok(result)

            case "generate_vector_graphic":
                import asyncio
                result = await asyncio.to_thread(
                    generators.generate_vector_graphic,
                    element=arguments.get("element", ""),
                    style=arguments.get("style", "minimal line icon"),
                    brand=arguments.get("brand", "Maillard"),
                )
                return self.ok(result)

            case "build_typography_system":
                import asyncio
                result = await asyncio.to_thread(
                    generators.build_typography_system,
                    product_context=arguments.get("product_context", "packaging"),
                )
                return self.ok(result)

            case _:
                return self.err(f"Unknown tool: {name}")
