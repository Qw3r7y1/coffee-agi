"""
Designer MCP — Brand system resource loader.
Source of truth: data/maillard/guidelines/

Key function: get_brand_text()
  Returns the full brand guideline content extracted via Claude vision OCR.
  First call runs OCR on Maillard Design Guideline.pdf and caches to brand_extracted.md.
  All subsequent calls read from cache.
"""
from __future__ import annotations
import os
import json
from loguru import logger

GUIDELINES_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "maillard", "guidelines")
)

BRAND_ASSETS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "maillard")
)

_BRAND_TEXT_CACHE: str | None = None


def get_brand_text() -> str:
    """
    Return the full extracted brand guideline content as a string.
    Reads from brand_extracted.md if available; otherwise runs Claude vision OCR
    on Maillard Design Guideline.pdf and saves the result as brand_extracted.md.
    """
    global _BRAND_TEXT_CACHE
    if _BRAND_TEXT_CACHE is not None:
        return _BRAND_TEXT_CACHE

    cache_path = os.path.join(GUIDELINES_DIR, "brand_extracted.md")
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as f:
            _BRAND_TEXT_CACHE = f.read()
        logger.info(f"[DESIGNER] brand_extracted.md loaded: {len(_BRAND_TEXT_CACHE)} chars")
        return _BRAND_TEXT_CACHE

    logger.info("[DESIGNER] brand_extracted.md not found — running OCR on design guideline PDF")
    _BRAND_TEXT_CACHE = _ocr_guidelines_pdf()
    if _BRAND_TEXT_CACHE:
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(_BRAND_TEXT_CACHE)
        logger.info(f"[DESIGNER] brand_extracted.md saved: {len(_BRAND_TEXT_CACHE)} chars")
    else:
        logger.warning("[DESIGNER] OCR produced no content — brand data unavailable")
        _BRAND_TEXT_CACHE = ""

    return _BRAND_TEXT_CACHE


def _ocr_guidelines_pdf() -> str:
    """
    Run Claude vision OCR on Maillard Design Guideline.pdf.
    Renders each page at 150 DPI, sends to claude-haiku-4-5-20251001, and assembles
    structured markdown output covering: colors, typography, logo rules, layout,
    brand values, and spacing system.
    """
    pdf_path = os.path.join(GUIDELINES_DIR, "Maillard Design Guideline.pdf")
    if not os.path.exists(pdf_path):
        logger.error(f"[DESIGNER] PDF not found: {pdf_path}")
        return ""

    try:
        import base64
        import anthropic
        import fitz

        doc = fitz.open(pdf_path)
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        pages_output: list[str] = []

        for page_num, page in enumerate(doc):
            # Compute DPI so the largest dimension stays under 7800px
            max_dim_pts = max(page.rect.width, page.rect.height)
            dpi = min(72, int(7800 * 72 / max_dim_pts))
            mat = fitz.Matrix(dpi / 72, dpi / 72)
            pix = page.get_pixmap(matrix=mat)
            img_bytes = pix.tobytes("jpeg", jpg_quality=80)
            img_b64 = base64.standard_b64encode(img_bytes).decode()

            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2048,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64},
                        },
                        {
                            "type": "text",
                            "text": (
                                "You are extracting brand identity specifications from a design guideline document. "
                                "Extract every piece of brand information visible on this page. "
                                "Output as structured markdown. Include — exactly as shown:\n"
                                "- Color swatches: name, HEX, CMYK, RGB (if shown)\n"
                                "- Typefaces: font family name, weights, sizes, use case\n"
                                "- Logo: versions, clear space rules, minimum size, prohibited uses\n"
                                "- Layout: grid system, margins, column counts, spacing units\n"
                                "- Brand values, taglines, tone-of-voice rules\n"
                                "- Packaging hierarchy and layout rules\n"
                                "If a value is not visible on this page, do not invent it. Skip it.\n"
                                "Output only what is explicitly shown."
                            ),
                        },
                    ],
                }],
            )
            page_text = resp.content[0].text.strip()
            if page_text:
                pages_output.append(f"## Page {page_num + 1}\n\n{page_text}")
            logger.info(f"[DESIGNER] OCR page {page_num + 1}/{len(doc)}: {len(page_text)} chars")

        doc.close()
        return "\n\n---\n\n".join(pages_output)

    except Exception as e:
        logger.error(f"[DESIGNER] OCR failed: {e}")
        return ""


# ── Expected brand resource files and which brief sections they govern ────────

_EXPECTED_RESOURCES: dict[str, list[str]] = {
    "brand_extracted.md":   ["design_concept", "brand_compliance"],
    "colors.json":          ["color_usage"],
    "typography.md":        ["typography_hierarchy"],
    "packaging_rules.md":   ["layout_structure", "production_considerations"],
    "layout_principles.md": ["layout_structure"],
    "logo_usage.pdf":       ["brand_compliance"],
}


def get_brand_sources_used() -> list[dict]:
    """
    Return all brand resource files with availability status and which brief
    sections they govern. Used to populate `brand_sources_used` in the response.
    """
    sources = []
    for fname, sections in _EXPECTED_RESOURCES.items():
        path = os.path.join(GUIDELINES_DIR, fname)
        sources.append({
            "file": fname,
            "path": f"data/maillard/guidelines/{fname}",
            "available": os.path.exists(path),
            "governs": sections,
        })
    return sources


def get_brand_guidelines_status() -> dict:
    """
    Audit the guidelines directory and return a structured status report:
    - core_loaded: files that exist and can be read
    - missing_resources: expected files that are absent
    - provisional_sections: brief sections affected by missing files
    """
    core_loaded: list[str] = []
    missing_resources: list[str] = []
    provisional_sections: set[str] = set()

    for fname, affected_sections in _EXPECTED_RESOURCES.items():
        path = os.path.join(GUIDELINES_DIR, fname)
        if os.path.exists(path):
            core_loaded.append(fname)
        else:
            missing_resources.append(fname)
            provisional_sections.update(affected_sections)

    return {
        "core_loaded": core_loaded,
        "missing_resources": missing_resources,
        "provisional_sections": sorted(provisional_sections),
    }


def load_brand_system() -> dict:
    """
    Load all available brand resources from data/maillard/guidelines/.
    Includes both raw file contents (JSON, MD) and the OCR-extracted brand text.
    """
    resources: dict = {}

    if not os.path.exists(GUIDELINES_DIR):
        logger.warning(f"Guidelines directory not found: {GUIDELINES_DIR}")
        return resources

    for fname in os.listdir(GUIDELINES_DIR):
        fpath = os.path.join(GUIDELINES_DIR, fname)
        ext = os.path.splitext(fname)[1].lower()
        try:
            if ext == ".json":
                with open(fpath, encoding="utf-8") as f:
                    resources[fname] = json.load(f)
            elif ext in (".md", ".txt"):
                with open(fpath, encoding="utf-8") as f:
                    resources[fname] = f.read()
            else:
                resources[fname] = {
                    "type": "binary",
                    "path": fpath,
                    "size_bytes": os.path.getsize(fpath),
                }
        except Exception as e:
            logger.warning(f"Could not load {fname}: {e}")

    # Always inject extracted brand text
    brand_text = get_brand_text()
    if brand_text:
        resources["_brand_guidelines_extracted"] = brand_text

    return resources


def list_brand_assets() -> dict[str, list[str]]:
    """List all brand asset files grouped by folder."""
    result = {}
    for folder in ["logos", "images", "fonts", "guidelines", "Branding", "recipes"]:
        folder_path = os.path.join(BRAND_ASSETS_DIR, folder)
        if os.path.exists(folder_path):
            result[folder] = sorted(os.listdir(folder_path))
    return result


def get_colors() -> dict | None:
    """Load colors.json if present in guidelines."""
    path = os.path.join(GUIDELINES_DIR, "colors.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def get_typography() -> str | None:
    """Load typography.md if present in guidelines."""
    path = os.path.join(GUIDELINES_DIR, "typography.md")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return f.read()
    return None
