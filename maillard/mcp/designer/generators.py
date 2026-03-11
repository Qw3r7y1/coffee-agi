"""
Designer MCP — Asset generation engine.

Generates real design files using Python:
  - SVG packaging layouts (front + back panels, grid, typography hierarchy)
  - PNG design images (brand-aligned compositions via Pillow)
  - PNG product mockups (3D bag silhouette via Pillow)
  - SVG vector graphics (programmatic brand icons)
  - Typography system JSON (from brand guidelines)

All output files go to:  data/maillard/generated/
Served at:               /brand/generated/<filename>
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Optional

from loguru import logger

# ── Output directory ───────────────────────────────────────────────────────────

GENERATED_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "maillard", "generated")
)


def _ensure_dir() -> str:
    os.makedirs(GENERATED_DIR, exist_ok=True)
    return GENERATED_DIR


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:40]


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _file(name: str) -> tuple[str, str]:
    """Return (absolute_filepath, url_path)."""
    _ensure_dir()
    filepath = os.path.join(GENERATED_DIR, name)
    url = f"/brand/generated/{name}"
    return filepath, url


# ── Brand palette ──────────────────────────────────────────────────────────────

PALETTE = {
    "black":  "#1A1A1A",
    "white":  "#F5F0E8",
    "gold":   "#C8A96E",
    "mid":    "#3D3832",
    "light":  "#E8E2D8",
    "cream":  "#FAF6EF",
}


def _hex_to_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))  # type: ignore


# ── 1. generate_packaging_layout ──────────────────────────────────────────────

def generate_packaging_layout(
    product: str,
    size: str = "250g",
    brand: str = "Maillard",
    style: str = "minimal premium specialty coffee",
) -> dict:
    """
    Generate a production-ready SVG packaging layout with front and back panels.
    Includes: 6-column grid, typography zones, bleed guides, colour swatches.
    """
    slug = _slug(f"{product}_{size}")
    filename = f"{slug}_layout_{_ts()}.svg"
    filepath, url = _file(filename)

    # Canvas: two panels side by side
    panel_w, panel_h = 756, 1134   # ~200 × 300 mm at 3.78 px/mm
    gap, margin = 60, 60
    bleed = 9                       # 3 mm bleed at 3 px/mm
    total_w = panel_w * 2 + gap + margin * 2
    total_h = panel_h + margin * 2 + 60  # extra for legend

    fx = margin          # front panel x
    bx = margin + panel_w + gap   # back panel x

    B = PALETTE["black"]
    G = PALETTE["gold"]
    W = PALETTE["white"]
    M = PALETTE["mid"]
    L = PALETTE["light"]

    col_w = panel_w / 6
    row_h = panel_h / 12
    inner = 48   # inner margin

    def _grid_lines(ox: float) -> list[str]:
        out = []
        for i in range(1, 6):
            out.append(f'  <line class="grid" x1="{ox + col_w*i:.1f}" y1="{margin}" x2="{ox + col_w*i:.1f}" y2="{margin + panel_h}"/>')
        for j in range(1, 12):
            out.append(f'  <line class="grid" x1="{ox}" y1="{margin + row_h*j:.1f}" x2="{ox + panel_w}" y2="{margin + row_h*j:.1f}"/>')
        return out

    lines: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_w}" height="{total_h}" viewBox="0 0 {total_w} {total_h}">',
        "  <defs>",
        "    <style>",
        f'      text {{ font-family: Georgia, "Times New Roman", serif; fill: {B}; }}',
        f'      .sans {{ font-family: "Helvetica Neue", Arial, sans-serif; }}',
        f'      .label {{ font-family: "Helvetica Neue", Arial, sans-serif; font-size: 9px; fill: {M}; letter-spacing: 2px; }}',
        f'      .grid {{ stroke: {G}; stroke-width: 0.5; stroke-dasharray: 4,4; opacity: 0.35; }}',
        "    </style>",
        # Bleed guides (red dashed)
        f'    <rect id="bf" x="{fx - bleed}" y="{margin - bleed}" width="{panel_w + bleed*2}" height="{panel_h + bleed*2}" fill="none" stroke="#E8000070" stroke-width="0.75" stroke-dasharray="3,3"/>',
        f'    <rect id="bb" x="{bx - bleed}" y="{margin - bleed}" width="{panel_w + bleed*2}" height="{panel_h + bleed*2}" fill="none" stroke="#E8000070" stroke-width="0.75" stroke-dasharray="3,3"/>',
        "  </defs>",
        # Canvas background
        f'  <rect width="{total_w}" height="{total_h}" fill="{L}"/>',
    ]

    for panel_x, label in [(fx, "FRONT PANEL"), (bx, "BACK PANEL")]:
        # Panel base
        lines += [
            f'  <!-- {label} -->',
            f'  <rect x="{panel_x}" y="{margin}" width="{panel_w}" height="{panel_h}" fill="{W}"/>',
            f'  <rect x="{panel_x}" y="{margin}" width="{panel_w}" height="{panel_h}" fill="none" stroke="{B}" stroke-width="1.5"/>',
            # Gold top bar
            f'  <rect x="{panel_x}" y="{margin}" width="{panel_w}" height="5" fill="{G}"/>',
            # Gold bottom bar
            f'  <rect x="{panel_x}" y="{margin + panel_h - 5}" width="{panel_w}" height="5" fill="{G}"/>',
        ]
        # Grid
        lines += _grid_lines(panel_x)
        # Inner margin guide
        lines.append(f'  <rect x="{panel_x + inner}" y="{margin + inner}" width="{panel_w - inner*2}" height="{panel_h - inner*2}" fill="none" stroke="{G}" stroke-width="0.75" stroke-dasharray="6,3" opacity="0.5"/>')
        # Panel label
        lines.append(f'  <text x="{panel_x + 6}" y="{margin + 14}" class="label">{label} · {panel_w}×{panel_h}px · +3mm BLEED</text>')

        if label == "FRONT PANEL":
            # Logo zone (rows 1–2)
            logo_y = margin + inner + 10
            logo_h = row_h * 2 - 20
            lines += [
                f'  <!-- Logo zone -->',
                f'  <rect x="{panel_x + inner + 20}" y="{logo_y:.1f}" width="{panel_w - (inner+20)*2}" height="{logo_h:.1f}" fill="none" stroke="{G}" stroke-width="1" opacity="0.7"/>',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{logo_y + logo_h/2 + 5:.0f}" text-anchor="middle" font-size="16" letter-spacing="5" fill="{M}" class="sans">{brand.upper()}</text>',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{logo_y + logo_h/2 + 20:.0f}" text-anchor="middle" class="label">LOGO ZONE — MINIMUM 20mm WIDE</text>',
            ]
            # Product name (rows 3–5)
            pname_y = margin + row_h * 2 + 60
            lines += [
                f'  <!-- Product name -->',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{pname_y:.0f}" text-anchor="middle" font-size="38" font-weight="bold" letter-spacing="1">{product.upper()}</text>',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{pname_y + 52:.0f}" text-anchor="middle" font-size="13" letter-spacing="3" fill="{M}" class="sans">SINGLE ORIGIN · SPECIALTY GRADE</text>',
            ]
            # Divider rule
            rule_y = pname_y + 82
            lines.append(f'  <line x1="{panel_x + inner + 50}" y1="{rule_y}" x2="{panel_x + panel_w - inner - 50}" y2="{rule_y}" stroke="{G}" stroke-width="1"/>')
            # Flavour zone (rows 6–8)
            desc_y = rule_y + 52
            lines += [
                f'  <!-- Flavour descriptor zone -->',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{desc_y:.0f}" text-anchor="middle" font-size="13" fill="{M}">[FLAVOUR DESCRIPTOR]</text>',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{desc_y + 24:.0f}" text-anchor="middle" font-size="11" fill="{M}" class="sans">[PROCESSING METHOD · ALTITUDE · REGION]</text>',
            ]
            # Weight zone (rows 11–12)
            weight_y = margin + panel_h - inner - 40
            lines += [
                f'  <!-- Weight zone -->',
                f'  <line x1="{panel_x + inner + 50}" y1="{weight_y - 28}" x2="{panel_x + panel_w - inner - 50}" y2="{weight_y - 28}" stroke="{G}" stroke-width="1"/>',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{weight_y:.0f}" text-anchor="middle" font-size="20" letter-spacing="4" class="sans">{size.upper()}</text>',
                f'  <text x="{panel_x + panel_w/2:.0f}" y="{weight_y + 22:.0f}" text-anchor="middle" class="label">DISTINCTIVE FLAVOR COFFEE ROASTERS</text>',
            ]

        else:  # BACK PANEL
            # Origin story zone (rows 1–4)
            story_y = margin + inner + 10
            lines += [
                f'  <!-- Origin story zone -->',
                f'  <text x="{panel_x + inner}" y="{story_y:.0f}" font-size="9" letter-spacing="3" fill="{M}" class="sans">ORIGIN</text>',
                f'  <line x1="{panel_x + inner}" y1="{story_y + 8}" x2="{panel_x + panel_w - inner}" y2="{story_y + 8}" stroke="{G}" stroke-width="0.75"/>',
                f'  <text x="{panel_x + inner}" y="{story_y + 30}" font-size="11" fill="{M}">[Country · Region · Farm / Co-op name]</text>',
                f'  <text x="{panel_x + inner}" y="{story_y + 50}" font-size="11" fill="{M}">[Altitude: _ masl · Varietal: _]</text>',
                f'  <text x="{panel_x + inner}" y="{story_y + 70}" font-size="11" fill="{M}">[Process: _ · Harvest: _]</text>',
                f'  <text x="{panel_x + inner}" y="{story_y + 95}" font-size="10" fill="{M}" class="sans">[2–3 sentence origin story. Flavour notes. Roast profile.]</text>',
            ]
            # Brew guide zone (rows 5–8)
            brew_y = margin + row_h * 4 + 10
            brew_items = [
                ("ESPRESSO",   "22 g → 50 ml · 93 °C · 28–32 s"),
                ("POUR OVER",  "15 g : 250 ml · 93 °C · 3:30 min"),
                ("FRENCH PRESS", "60 g/L · 93 °C · 4:00 min"),
                ("COLD BREW",  "100 g/L · room temp · 16 h"),
            ]
            lines += [
                f'  <!-- Brew guide zone -->',
                f'  <text x="{panel_x + inner}" y="{brew_y:.0f}" font-size="9" letter-spacing="3" fill="{M}" class="sans">BREW GUIDE</text>',
                f'  <line x1="{panel_x + inner}" y1="{brew_y + 8}" x2="{panel_x + panel_w - inner}" y2="{brew_y + 8}" stroke="{G}" stroke-width="0.75"/>',
            ]
            for k, (method, spec) in enumerate(brew_items):
                by = brew_y + 28 + k * 28
                lines += [
                    f'  <text x="{panel_x + inner}" y="{by:.0f}" font-size="9" letter-spacing="2" fill="{M}" class="sans">{method}</text>',
                    f'  <text x="{panel_x + inner + 100}" y="{by:.0f}" font-size="10" fill="{M}">{spec}</text>',
                ]
            # Barcode zone (bottom-right)
            bc_w, bc_h = 140, 76
            bc_x = panel_x + panel_w - inner - bc_w
            bc_y = margin + panel_h - inner - bc_h - 28
            lines += [
                f'  <!-- Barcode zone -->',
                f'  <rect x="{bc_x}" y="{bc_y}" width="{bc_w}" height="{bc_h}" fill="none" stroke="{M}" stroke-width="0.75" stroke-dasharray="3,2" opacity="0.6"/>',
                f'  <text x="{bc_x + bc_w/2:.0f}" y="{bc_y + bc_h/2 + 4:.0f}" text-anchor="middle" font-size="9" fill="{M}">EAN-13 BARCODE</text>',
            ]
            # Legal zone (row 12)
            legal_y = margin + panel_h - inner - 8
            lines.append(f'  <text x="{panel_x + inner}" y="{legal_y}" font-size="8" fill="{M}">[Net weight · Roaster address · Roast date · Best before · Lot #]</text>')

    # Legend
    legend_y = total_h - 36
    swatches = [("Black", B), ("Gold", G), ("Cream", W)]
    lines += [
        f'  <!-- Legend -->',
        f'  <text x="{margin}" y="{legend_y}" font-size="9" fill="{M}">Maillard Coffee Roasters — {product} {size} Packaging Layout · 6-col grid · 3 mm bleed</text>',
        f'  <text x="{margin}" y="{legend_y + 14}" font-size="8" fill="{M}">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} · Print: CMYK PDF/X-4 · Digital: RGB PNG @300 dpi</text>',
    ]
    swatch_x = total_w - 200
    for i, (name, color) in enumerate(swatches):
        sx = swatch_x + i * 62
        stroke = f' stroke="{M}" stroke-width="0.5"' if color == W else ""
        lines += [
            f'  <rect x="{sx}" y="{legend_y - 10}" width="12" height="12" fill="{color}"{stroke}/>',
            f'  <text x="{sx + 15}" y="{legend_y}" font-size="9" fill="{M}">{name}</text>',
        ]

    lines.append("</svg>")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(f"[DESIGNER] Layout saved: {filepath}")
    return {
        "file": filename,
        "path": filepath,
        "url": url,
        "panels": ["front", "back"],
        "canvas": f"{total_w}×{total_h}px",
        "panel_size": f"{panel_w}×{panel_h}px",
        "bleed": "3mm",
        "grid": "6 columns × 12 rows",
        "color_space": "RGB — convert to CMYK for print",
    }


# ── 2. generate_design_image ───────────────────────────────────────────────────

def generate_design_image(
    subject: str,
    prompt: str = "",
    style: str = "minimal specialty coffee brand",
    brand: str = "Maillard",
) -> dict:
    """
    Generate a brand-aligned design image as a PNG using Pillow.
    Produces a 1200×1500 editorial-style composition.
    """
    from PIL import Image, ImageDraw

    slug = _slug(subject or prompt)
    filename = f"{slug}_art_{_ts()}.png"
    filepath, url = _file(filename)

    W, H = 1200, 1500
    img = Image.new("RGB", (W, H), _hex_to_rgb(PALETTE["cream"]))
    draw = ImageDraw.Draw(img)

    black = _hex_to_rgb(PALETTE["black"])
    gold  = _hex_to_rgb(PALETTE["gold"])
    mid   = _hex_to_rgb(PALETTE["mid"])
    white = _hex_to_rgb(PALETTE["white"])
    light = _hex_to_rgb(PALETTE["light"])

    # Header block
    draw.rectangle([0, 0, W, 190], fill=black)
    draw.rectangle([0, 190, W, 196], fill=gold)

    # Large geometric composition — minimal circles
    cx, cy = W // 2, H // 2 + 60
    r_outer = 340
    r_inner = 210
    draw.ellipse([cx - r_outer, cy - r_outer, cx + r_outer, cy + r_outer],
                 outline=gold, width=2)
    draw.ellipse([cx - r_inner, cy - r_inner, cx + r_inner, cy + r_inner],
                 fill=light, outline=gold, width=1)

    # Crosshair tick marks (minimal brand language)
    tick = 22
    for dx, dy in [(-r_outer - tick, 0), (r_outer, 0), (0, -r_outer - tick), (0, r_outer)]:
        x0 = cx + (dx if dx < 0 else 0)
        y0 = cy + (dy if dy < 0 else 0)
        x1 = cx + (dx + tick if dx < 0 else dx if dx > 0 else 0)
        y1 = cy + (dy + tick if dy < 0 else dy if dy > 0 else 0)
        draw.line([cx + dx, cy + dy, cx + dx + (tick if dx < 0 else -tick if dx > 0 else 0),
                   cy + dy + (tick if dy < 0 else -tick if dy > 0 else 0)], fill=gold, width=1)

    # Horizontal rule pair
    rule_y = cy + r_outer + 60
    draw.line([cx - 180, rule_y, cx + 180, rule_y], fill=gold, width=1)
    draw.line([cx - 180, rule_y + 4, cx + 180, rule_y + 4], fill=gold, width=1)

    # Footer block
    draw.rectangle([0, H - 150, W, H], fill=black)
    draw.rectangle([0, H - 150, W, H - 144], fill=gold)

    # ── Text via Pillow ImageFont ─────────────────────────────────────────────
    font_paths = {
        "serif_bold": ["C:/Windows/Fonts/georgiab.ttf", "C:/Windows/Fonts/georgia.ttf"],
        "serif":      ["C:/Windows/Fonts/georgia.ttf"],
        "sans":       ["C:/Windows/Fonts/arial.ttf", "C:/Windows/Fonts/calibri.ttf"],
        "sans_bold":  ["C:/Windows/Fonts/arialbd.ttf", "C:/Windows/Fonts/calibrib.ttf"],
    }

    def _load_font(key: str, size: int):
        from PIL import ImageFont
        for fp in font_paths.get(key, []):
            if os.path.exists(fp):
                try:
                    return ImageFont.truetype(fp, size)
                except Exception:
                    pass
        return ImageFont.load_default()

    f_brand  = _load_font("sans",       20)
    f_sub    = _load_font("sans",       12)
    f_large  = _load_font("serif_bold", 48)
    f_medium = _load_font("serif",      18)
    f_caption = _load_font("sans",      11)

    def _ctext(text, y, font, color, draw=draw, w=W):
        bbox = draw.textbbox((0, 0), text, font=font)
        tw = bbox[2] - bbox[0]
        draw.text(((w - tw) // 2, y), text, font=font, fill=color)

    # Header
    _ctext(brand.upper(), 72, f_brand, (255, 255, 255))
    _ctext("DISTINCTIVE FLAVOR COFFEE ROASTERS", 108, f_sub, gold)
    _ctext("DESIGN DEPARTMENT", 136, f_sub, (120, 110, 100))

    # Subject inside circle
    subj_text = (subject or slug.replace("_", " ")).upper()
    _ctext(subj_text, cy - 36, f_large, black)
    _ctext("SINGLE ORIGIN · SPECIALTY", cy + 24, f_sub, mid)
    _ctext(style.upper(), rule_y + 20, f_caption, mid)

    # Footer
    _ctext(brand.upper(), H - 108, f_medium, (255, 255, 255))
    _ctext("EST. 2021 · ATHENS, GR", H - 78, f_sub, gold)

    # Corner registration marks
    def _reg(x, y):
        sz = 14
        draw.line([x - sz, y, x + sz, y], fill=gold, width=1)
        draw.line([x, y - sz, x, y + sz], fill=gold, width=1)
        draw.ellipse([x - 3, y - 3, x + 3, y + 3], outline=gold, width=1)

    for rx, ry in [(40, 216), (W - 40, 216), (40, H - 166), (W - 40, H - 166)]:
        _reg(rx, ry)

    img.save(filepath, "PNG", dpi=(300, 300))
    logger.info(f"[DESIGNER] Design image saved: {filepath}")
    return {
        "file": filename,
        "path": filepath,
        "url": url,
        "dimensions": f"{W}×{H}px",
        "dpi": 300,
        "format": "PNG",
        "brand": brand,
        "subject": subject,
    }


# ── 3. generate_product_mockup ─────────────────────────────────────────────────

def generate_product_mockup(
    product: str,
    design_file: str = "",
    size: str = "250g",
) -> dict:
    """
    Generate a PNG product mockup showing the coffee bag in perspective.
    Creates a stylised flat-bottom gusseted bag silhouette using Pillow.
    """
    from PIL import Image, ImageDraw, ImageFilter

    slug = _slug(product)
    filename = f"{slug}_mockup_{_ts()}.png"
    filepath, url = _file(filename)

    CW, CH = 1400, 1800
    img = Image.new("RGB", (CW, CH), (242, 240, 238))   # studio grey
    draw = ImageDraw.Draw(img)

    black = _hex_to_rgb(PALETTE["black"])
    gold  = _hex_to_rgb(PALETTE["gold"])
    mid   = _hex_to_rgb(PALETTE["mid"])
    cream = _hex_to_rgb(PALETTE["cream"])
    light = _hex_to_rgb(PALETTE["light"])

    # ── Bag silhouette (perspective — front face) ─────────────────────────────
    # Flat-bottom gusseted bag: wider at top (seal), slight taper at base
    bag_cx  = CW // 2
    bag_top = 180
    bag_bot = 1560
    bag_h   = bag_bot - bag_top
    hw_top  = 290      # half-width at top seal
    hw_mid  = 260      # half-width at body
    hw_bot  = 230      # half-width at base

    # Front face polygon (slight trapezoid)
    front = [
        (bag_cx - hw_top, bag_top),          # top-left
        (bag_cx + hw_top, bag_top),          # top-right
        (bag_cx + hw_bot, bag_bot),          # bottom-right
        (bag_cx - hw_bot, bag_bot),          # bottom-left
    ]

    # Shadow (offset polygon, blurred via a separate layer)
    shadow_layer = Image.new("RGBA", (CW, CH), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow_layer)
    shadow_pts = [(x + 22, y + 30) for x, y in front]
    sd.polygon(shadow_pts, fill=(0, 0, 0, 80))
    shadow_layer_blur = shadow_layer.filter(ImageFilter.GaussianBlur(22))
    img.paste(Image.new("RGB", (CW, CH), (242, 240, 238)), (0, 0),
              mask=shadow_layer_blur.split()[3])
    # Draw shadow (flatten to RGB)
    shadow_rgb = Image.new("RGB", (CW, CH), (220, 218, 215))
    img.paste(shadow_rgb, (0, 0), mask=shadow_layer_blur.split()[3])

    # Bag body fill — brand black
    draw.polygon(front, fill=black)

    # Gold top seal band
    seal_h = 60
    seal_pts = [
        (bag_cx - hw_top,          bag_top),
        (bag_cx + hw_top,          bag_top),
        (bag_cx + hw_top,          bag_top + seal_h),
        (bag_cx - hw_top,          bag_top + seal_h),
    ]
    draw.polygon(seal_pts, fill=_hex_to_rgb(PALETTE["mid"]))

    # Gold accent stripe below seal
    stripe_y = bag_top + seal_h
    draw.polygon([
        (bag_cx - hw_top, stripe_y),
        (bag_cx + hw_top, stripe_y),
        (bag_cx + hw_top, stripe_y + 4),
        (bag_cx - hw_top, stripe_y + 4),
    ], fill=gold)

    # ── Design elements on bag face ───────────────────────────────────────────
    font_paths = {
        "serif_bold": ["C:/Windows/Fonts/georgiab.ttf", "C:/Windows/Fonts/georgia.ttf"],
        "serif":      ["C:/Windows/Fonts/georgia.ttf"],
        "sans":       ["C:/Windows/Fonts/arial.ttf"],
        "sans_bold":  ["C:/Windows/Fonts/arialbd.ttf"],
    }

    def _load_font(key: str, size: int):
        from PIL import ImageFont
        for fp in font_paths.get(key, []):
            if os.path.exists(fp):
                try:
                    return ImageFont.truetype(fp, size)
                except Exception:
                    pass
        return ImageFont.load_default()

    f_brand  = _load_font("sans",       16)
    f_sub    = _load_font("sans",       11)
    f_name   = _load_font("serif_bold", 44)
    f_desc   = _load_font("sans",       12)
    f_weight = _load_font("sans",       18)

    def _ctext(text, y, font, color):
        bbox = draw.textbbox((0, 0), text, font=font)
        tw = bbox[2] - bbox[0]
        draw.text((bag_cx - tw // 2, y), text, font=font, fill=color)

    # Brand name in seal
    _ctext("MAILLARD", bag_top + 16, f_brand, (220, 215, 208))
    _ctext("COFFEE ROASTERS", bag_top + 36, f_sub, gold)

    # Circular brand mark on bag
    mark_cy = bag_top + seal_h + 180
    mr = 110
    draw.ellipse([bag_cx - mr, mark_cy - mr, bag_cx + mr, mark_cy + mr],
                 outline=gold, width=2)
    draw.ellipse([bag_cx - mr + 8, mark_cy - mr + 8, bag_cx + mr - 8, mark_cy + mr - 8],
                 outline=(60, 52, 44), width=1)
    _ctext("M", mark_cy - 28, _load_font("serif_bold", 52), gold)
    _ctext("·  ·  ·", mark_cy + 30, f_sub, gold)

    # Product name
    name_y = mark_cy + mr + 50
    _ctext(product.upper(), name_y, f_name, cream)
    _ctext("SINGLE ORIGIN · SPECIALTY", name_y + 58, f_desc, (160, 140, 110))

    # Gold rule
    rule_y = name_y + 90
    draw.line([bag_cx - 120, rule_y, bag_cx + 120, rule_y], fill=gold, width=1)

    # Tasting notes placeholder
    _ctext("[TASTING NOTES]", rule_y + 28, f_sub, (140, 125, 100))
    _ctext("[PROCESSING · ALTITUDE]", rule_y + 50, f_sub, (120, 108, 88))

    # Weight at base
    weight_y = bag_bot - 110
    draw.line([bag_cx - 100, weight_y - 16, bag_cx + 100, weight_y - 16], fill=gold, width=1)
    _ctext(size.upper(), weight_y, f_weight, cream)
    _ctext("DISTINCTIVE FLAVOR COFFEE ROASTERS", weight_y + 26, f_sub, (120, 108, 88))

    # Bottom base (slightly darker strip)
    base_h = 30
    base_pts = [
        (bag_cx - hw_bot, bag_bot - base_h),
        (bag_cx + hw_bot, bag_bot - base_h),
        (bag_cx + hw_bot, bag_bot),
        (bag_cx - hw_bot, bag_bot),
    ]
    draw.polygon(base_pts, fill=(14, 14, 14))

    # ── Watermark / generation note ────────────────────────────────────────────
    note_font = _load_font("sans", 10)
    note = f"Maillard Design Dept — {product} {size} Mockup — {datetime.now().strftime('%Y-%m-%d')}"
    draw.text((20, CH - 24), note, font=note_font, fill=(160, 155, 150))

    img.save(filepath, "PNG", dpi=(300, 300))
    logger.info(f"[DESIGNER] Mockup saved: {filepath}")
    return {
        "file": filename,
        "path": filepath,
        "url": url,
        "dimensions": f"{CW}×{CH}px",
        "dpi": 300,
        "format": "PNG",
        "product": product,
        "size": size,
    }


# ── 4. generate_vector_graphic ─────────────────────────────────────────────────

def generate_vector_graphic(
    element: str,
    style: str = "minimal line icon",
    brand: str = "Maillard",
) -> dict:
    """
    Generate an SVG vector graphic using programmatic shapes.
    Produces brand-aligned minimal icons matching the Maillard aesthetic.
    """
    slug = _slug(element)
    filename = f"{slug}_vector_{_ts()}.svg"
    filepath, url = _file(filename)

    G = PALETTE["gold"]
    B = PALETTE["black"]
    M = PALETTE["mid"]
    W = PALETTE["white"]

    VW = VH = 200   # viewBox

    # Determine icon type from element name
    kw = element.lower()
    shapes: list[str] = []

    if any(w in kw for w in ["coffee", "bean", "origin"]):
        # Coffee bean / origin icon
        shapes = [
            f'<ellipse cx="100" cy="100" rx="55" ry="80" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<path d="M100 22 Q130 100 100 178" fill="none" stroke="{G}" stroke-width="1.5"/>',
            f'<circle cx="100" cy="100" r="90" fill="none" stroke="{M}" stroke-width="0.75" stroke-dasharray="4,4"/>',
        ]
    elif any(w in kw for w in ["cup", "espresso", "coffee cup"]):
        # Espresso cup icon
        shapes = [
            f'<path d="M65 80 L135 80 L125 145 Q100 155 75 145 Z" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<path d="M135 95 Q158 95 158 115 Q158 135 135 135" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<line x1="55" y1="150" x2="145" y2="150" stroke="{G}" stroke-width="2"/>',
            f'<path d="M85 65 Q90 45 100 65" fill="none" stroke="{M}" stroke-width="1.5"/>',
        ]
    elif any(w in kw for w in ["leaf", "plant", "farm"]):
        # Leaf / farm icon
        shapes = [
            f'<path d="M100 160 Q60 120 80 60 Q100 30 120 60 Q140 120 100 160 Z" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<line x1="100" y1="160" x2="100" y2="170" stroke="{G}" stroke-width="1.5"/>',
            f'<path d="M100 160 Q80 140 75 120" fill="none" stroke="{G}" stroke-width="1"/>',
            f'<path d="M100 160 Q120 140 125 120" fill="none" stroke="{G}" stroke-width="1"/>',
        ]
    elif any(w in kw for w in ["mountain", "altitude", "elevation"]):
        # Mountain / altitude icon
        shapes = [
            f'<path d="M30 155 L100 40 L170 155 Z" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<path d="M60 155 L100 80 L140 155" fill="none" stroke="{M}" stroke-width="1"/>',
            f'<line x1="20" y1="155" x2="180" y2="155" stroke="{G}" stroke-width="1.5"/>',
        ]
    elif any(w in kw for w in ["bag", "package", "packaging"]):
        # Bag icon
        shapes = [
            f'<path d="M55 80 L65 180 L135 180 L145 80 Z" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<path d="M80 80 Q80 50 100 50 Q120 50 120 80" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<line x1="55" y1="80" x2="145" y2="80" stroke="{G}" stroke-width="1.5"/>',
        ]
    else:
        # Default: abstract brand mark (teardrop + circle)
        shapes = [
            f'<circle cx="100" cy="100" r="75" fill="none" stroke="{G}" stroke-width="1.5"/>',
            f'<path d="M100 35 Q130 70 130 100 Q130 130 100 150 Q70 130 70 100 Q70 70 100 35 Z" fill="none" stroke="{G}" stroke-width="2"/>',
            f'<circle cx="100" cy="100" r="8" fill="{G}"/>',
        ]

    svg_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{VW}" height="{VH}" viewBox="0 0 {VW} {VH}">',
        f'  <rect width="{VW}" height="{VH}" fill="{W}"/>',
        "  <!-- Maillard brand icon -->",
    ] + [f"  {s}" for s in shapes] + [
        f'  <text x="100" y="{VH - 8}" text-anchor="middle" font-family="Helvetica Neue, Arial, sans-serif" font-size="7" letter-spacing="2" fill="{M}">{brand.upper()}</text>',
        "</svg>",
    ]

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(svg_parts))

    logger.info(f"[DESIGNER] Vector graphic saved: {filepath}")
    return {
        "file": filename,
        "path": filepath,
        "url": url,
        "format": "SVG",
        "viewBox": f"{VW}×{VH}",
        "element": element,
        "style": style,
    }


# ── 5. build_typography_system ─────────────────────────────────────────────────

def build_typography_system(product_context: str = "packaging") -> dict:
    """
    Build a structured typography system from brand guidelines.
    Reads brand_extracted.md and returns a machine-ready hierarchy.
    """
    from maillard.mcp.designer.resources import get_brand_text, GUIDELINES_DIR

    brand_text = get_brand_text()

    # Try to load typography.md if it exists
    typo_path = os.path.join(GUIDELINES_DIR, "typography.md")
    typo_md = ""
    if os.path.exists(typo_path):
        with open(typo_path, encoding="utf-8") as f:
            typo_md = f.read()

    # Extract any font names mentioned in brand text
    font_mentions: list[str] = []
    if brand_text:
        found = re.findall(r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)*(?:\s(?:Regular|Bold|Light|Medium|Italic|SemiBold|Black))?)\b', brand_text)
        font_mentions = list(dict.fromkeys(f for f in found if any(kw in f.lower() for kw in ["font", "type", "gothic", "sans", "serif", "neue", "grotesk", "roman"])))[:6]

    status = "confirmed" if (brand_text or typo_md) else "provisional"
    source = "brand_extracted.md" if brand_text else None

    levels = {
        "brand_wordmark": {
            "role": "Primary logotype — Maillard wordmark",
            "typeface": font_mentions[0] if font_mentions else "[PROVISIONAL — typeface from logo_usage.pdf]",
            "weight": "Custom / Regular",
            "size": "[FROM: brand_extracted.md] — see logo artwork files",
            "tracking": "Optical",
            "case": "Mixed case per brand artwork",
            "status": status,
            "source": source,
        },
        "headline": {
            "role": "Primary display text — product names, campaign headers",
            "typeface": font_mentions[0] if font_mentions else "[PROVISIONAL — confirm from typography.md]",
            "weight": "Bold",
            "size": "36–48 pt print / 32–42 px digital",
            "tracking": "–10 to 0",
            "leading": "1.1×",
            "case": "ALL CAPS or Title Case",
            "status": status,
            "source": source,
        },
        "subheadline": {
            "role": "Secondary hierarchy — section titles, descriptors",
            "typeface": "[PROVISIONAL — confirm from typography.md]",
            "weight": "Regular or Medium",
            "size": "14–18 pt print / 13–16 px digital",
            "tracking": "+100–200 (widely tracked, caps)",
            "case": "ALL CAPS",
            "status": "provisional",
            "source": None,
        },
        "body": {
            "role": "Origin story, description, brew notes",
            "typeface": "[PROVISIONAL — confirm from typography.md]",
            "weight": "Regular",
            "size": "9–11 pt print / 13–15 px digital",
            "tracking": "0",
            "leading": "1.5×",
            "case": "Sentence case",
            "status": "provisional",
            "source": None,
        },
        "caption": {
            "role": "Labels, processing notes, small descriptors",
            "typeface": "[PROVISIONAL — confirm from typography.md]",
            "weight": "Light or Regular",
            "size": "7–9 pt print / 11–12 px digital",
            "tracking": "+50–150",
            "case": "ALL CAPS",
            "status": "provisional",
            "source": None,
        },
        "legal": {
            "role": "Regulatory text, net weight declaration, allergens",
            "typeface": "[PROVISIONAL — confirm from typography.md]",
            "weight": "Regular",
            "size": "6–7 pt minimum (legal min) / 10 px digital",
            "tracking": "0",
            "case": "Sentence case",
            "status": "provisional",
            "source": None,
        },
    }

    return {
        "product_context": product_context,
        "source_files_read": ["brand_extracted.md"] + (["typography.md"] if typo_md else []),
        "font_names_detected": font_mentions,
        "typography.md_available": bool(typo_md),
        "note": "Load typography.md into data/maillard/guidelines/ for confirmed values.",
        "hierarchy": levels,
    }
