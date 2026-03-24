"""
Brand Lock Layer — enforces Maillard identity on all generated media.

Loads brand_system.json, builds branded prompts, applies logo overlay.
Single source of truth for visual identity in the generation pipeline.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from loguru import logger

BRAND_SYSTEM_PATH = Path(__file__).resolve().parent.parent.parent.parent / "data" / "maillard" / "guidelines" / "brand_system.json"

_brand_cache: dict | None = None


# ── Load Brand Identity ──────────────────────────────────────────


def load_brand_identity() -> dict:
    """Load brand system from JSON. Cached after first load."""
    global _brand_cache
    if _brand_cache is not None:
        return _brand_cache

    try:
        with open(BRAND_SYSTEM_PATH, "r", encoding="utf-8") as f:
            _brand_cache = json.load(f)
        logger.info("[BRAND] Loaded brand system")
        return _brand_cache
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"[BRAND] Failed to load brand_system.json: {e}")
        return {}


# ── Branded Prompt Builder ───────────────────────────────────────


def build_branded_prompt(topic: str, aspect: str = "feed") -> str:
    """
    Build an image generation prompt locked to Maillard brand identity.

    Args:
        topic: What to generate (e.g. "espresso shot", "cold brew process")
        aspect: "feed" (4:5) or "story" (9:16)

    Returns:
        Full prompt string with brand rules injected.
    """
    brand = load_brand_identity()
    if not brand:
        # Fallback if brand system missing
        return (
            f"{topic}. Premium specialty coffee aesthetic. Cinematic warm lighting. "
            f"Dark moody background. High contrast. Shallow depth of field. "
            f"Clean composition with negative space. No text in image."
        )

    mood = brand.get("visual_mood", {})
    colors = brand.get("colors", {})
    comp = brand.get("composition_rules", {})
    forbidden = brand.get("forbidden_styles", [])

    # Build color palette description
    palette_desc = ", ".join(
        f"{v.get('hex', '')} ({k.replace('_', ' ')})"
        for k, v in colors.items()
        if isinstance(v, dict) and v.get("hex")
    )

    # Build forbidden list for negative prompt embedding
    forbidden_str = ". ".join(f"NOT {f}" for f in forbidden[:8])

    prompt = (
        f"{topic}.\n\n"
        f"BRAND: {brand.get('brand_name', 'MAILLARD')} — {brand.get('tagline', 'Distinctive Flavor Coffee Roasters')}.\n"
        f"MOOD: {mood.get('tone', 'Premium, minimal, confident')}.\n"
        f"LIGHTING: {mood.get('lighting', 'Warm cinematic, dramatic shadows')}.\n"
        f"BACKGROUND: {mood.get('backgrounds', 'Dark moody or clean white')}.\n"
        f"TEXTURES: {mood.get('textures', 'Coffee crema, steam, beans')}.\n"
        f"COMPOSITION: {mood.get('composition', 'Single focused subject, negative space')}.\n"
        f"STYLE: {mood.get('photography_style', 'Professional, 35mm cinematic')}.\n"
        f"COLOR PALETTE: {palette_desc}.\n"
        f"NEGATIVE SPACE: {comp.get('negative_space', '30% minimum clean space')}.\n"
        f"LOGO ZONE: {comp.get('safe_zone_logo', 'Bottom 15% reserved for logo')} — keep this area dark/simple.\n"
        f"CRITICAL — NO TEXT: Do NOT render any text, letters, words, brand names, logos, or watermarks in the image. The image must be purely photographic with ZERO text of any kind. Leave the bottom 15% as clean dark space.\n"
        f"FORBIDDEN: {forbidden_str}."
    )

    return prompt


def build_negative_prompt() -> str:
    """Build a negative prompt from forbidden styles."""
    brand = load_brand_identity()
    forbidden = brand.get("forbidden_styles", [])
    base = [
        "text", "watermark", "logo", "words", "letters", "signature",
        "writing", "font", "typography", "brand name", "label text",
        "blurry", "low quality", "low resolution",
    ]
    return ", ".join(base + forbidden[:10])


# ── Logo Overlay ─────────────────────────────────────────────────


def _detect_image_brightness(image_path: str) -> str:
    """Detect if the bottom of an image is dark or light. Returns 'dark' or 'light'."""
    try:
        from PIL import Image
        img = Image.open(image_path).convert("RGB")
        w, h = img.size
        # Sample the bottom 20% of the image
        bottom = img.crop((0, int(h * 0.8), w, h))
        pixels = list(bottom.getdata())
        avg_brightness = sum(sum(p) / 3 for p in pixels) / len(pixels)
        return "dark" if avg_brightness < 128 else "light"
    except Exception:
        return "dark"  # default to dark bg (most common for coffee imagery)


def apply_logo_overlay(image_path: str, logo_path: str | None = None) -> str:
    """
    Apply the official Maillard logo as an overlay on a generated image.
    Auto-selects light logo on dark images, dark logo on light images.
    Adds a subtle gradient for legibility.

    Args:
        image_path: Path to the generated image
        logo_path: Path to logo PNG (transparent). Auto-detects if None.

    Returns:
        Path to the final branded image (overwrites original).
    """
    from PIL import Image, ImageDraw

    brand = load_brand_identity()
    overlay_cfg = brand.get("overlay_config", {})
    logo_info = brand.get("logo", {})

    # Resolve logo path — auto-select based on image brightness
    if logo_path is None:
        if not logo_info.get("logo_available", False):
            logger.info("[BRAND] No logo file available — adding brand text watermark instead")
            return _apply_text_watermark(image_path, brand)

        brightness = _detect_image_brightness(image_path)
        if brightness == "dark":
            logo_path = logo_info.get("logo_light_path", logo_info.get("logo_path", ""))
            logger.info("[BRAND] Dark image detected — using light logo")
        else:
            logo_path = logo_info.get("logo_path", "")
            logger.info("[BRAND] Light image detected — using dark logo")

    if not Path(logo_path).exists():
        logger.warning(f"[BRAND] Logo not found at {logo_path} — using text watermark")
        return _apply_text_watermark(image_path, brand)

    try:
        img = Image.open(image_path).convert("RGBA")
        logo = Image.open(logo_path).convert("RGBA")
        w, h = img.size

        # Resize logo
        logo_w_pct = overlay_cfg.get("logo_width_pct", 25) / 100
        target_w = int(w * logo_w_pct)
        ratio = target_w / logo.width
        target_h = int(logo.height * ratio)
        logo = logo.resize((target_w, target_h), Image.LANCZOS)

        # Apply opacity
        opacity = overlay_cfg.get("opacity", 0.85)
        if opacity < 1.0:
            alpha = logo.split()[3]
            alpha = alpha.point(lambda p: int(p * opacity))
            logo.putalpha(alpha)

        # Add bottom gradient fade for legibility
        if overlay_cfg.get("background_fade", True):
            fade_h = int(h * overlay_cfg.get("fade_height_pct", 20) / 100)
            gradient = Image.new("RGBA", (w, fade_h), (0, 0, 0, 0))
            draw = ImageDraw.Draw(gradient)
            for y in range(fade_h):
                alpha_val = int(160 * (y / fade_h))
                draw.line([(0, y), (w, y)], fill=(0, 0, 0, alpha_val))
            img.paste(gradient, (0, h - fade_h), gradient)

        # Position logo
        margin_bottom = int(h * overlay_cfg.get("margin_bottom_pct", 5) / 100)
        x = (w - target_w) // 2
        y = h - target_h - margin_bottom
        img.paste(logo, (x, y), logo)

        # Save
        img = img.convert("RGB")
        img.save(image_path, quality=95)
        logger.info(f"[BRAND] Logo overlay applied: {image_path}")
        return image_path

    except Exception as e:
        logger.error(f"[BRAND] Logo overlay failed: {e}")
        return image_path


def _apply_text_watermark(image_path: str, brand: dict) -> str:
    """Fallback: add brand name as subtle text watermark when logo PNG isn't available."""
    try:
        from PIL import Image, ImageDraw, ImageFont

        img = Image.open(image_path).convert("RGBA")
        w, h = img.size
        overlay_cfg = brand.get("overlay_config", {})

        # Create text overlay
        txt_layer = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        draw = ImageDraw.Draw(txt_layer)

        brand_name = brand.get("brand_name", "MAILLARD")
        tagline = brand.get("tagline", "")

        # Try to use a nice font, fall back to default
        font_size = int(w * 0.06)
        tag_size = int(w * 0.025)
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
            tag_font = ImageFont.truetype("arial.ttf", tag_size)
        except (OSError, IOError):
            font = ImageFont.load_default()
            tag_font = font

        # Add bottom gradient
        fade_h = int(h * overlay_cfg.get("fade_height_pct", 20) / 100)
        for y in range(fade_h):
            alpha_val = int(140 * (y / fade_h))
            draw.line([(0, h - fade_h + y), (w, h - fade_h + y)], fill=(0, 0, 0, alpha_val))

        # Draw brand name
        margin_bottom = int(h * overlay_cfg.get("margin_bottom_pct", 5) / 100)
        opacity = int(255 * overlay_cfg.get("opacity", 0.85))
        gold = brand.get("colors", {}).get("warm_gold", {}).get("hex", "#C49A2A")
        # Convert hex to RGB
        r_c = int(gold[1:3], 16)
        g_c = int(gold[3:5], 16)
        b_c = int(gold[5:7], 16)

        # Brand name position
        bbox = draw.textbbox((0, 0), brand_name, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        x = (w - text_w) // 2

        if tagline:
            tag_bbox = draw.textbbox((0, 0), tagline, font=tag_font)
            tag_w = tag_bbox[2] - tag_bbox[0]
            tag_h = tag_bbox[3] - tag_bbox[1]
            total_h = text_h + tag_h + int(h * 0.01)
            y = h - total_h - margin_bottom
            draw.text((x, y), brand_name, fill=(r_c, g_c, b_c, opacity), font=font)
            tag_x = (w - tag_w) // 2
            tag_y = y + text_h + int(h * 0.01)
            draw.text((tag_x, tag_y), tagline, fill=(255, 255, 255, int(opacity * 0.7)), font=tag_font)
        else:
            y = h - text_h - margin_bottom
            draw.text((x, y), brand_name, fill=(r_c, g_c, b_c, opacity), font=font)

        # Composite
        img = Image.alpha_composite(img, txt_layer)
        img = img.convert("RGB")
        img.save(image_path, quality=95)
        logger.info(f"[BRAND] Text watermark applied: {image_path}")
        return image_path

    except Exception as e:
        logger.error(f"[BRAND] Text watermark failed: {e}")
        return image_path
