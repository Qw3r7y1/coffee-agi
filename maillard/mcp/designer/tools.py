"""
Designer MCP — Tool definitions and JSON schemas.
"""

TOOLS: list[dict] = [
    {
        "name": "load_brand_system",
        "description": (
            "Load the complete Maillard brand system from the guidelines directory. "
            "Returns all available brand resources: colors, typography, packaging rules, layout principles."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "section": {
                    "type": "string",
                    "description": "Optional specific section to load (e.g. 'colors', 'typography', 'packaging'). Omit for full system.",
                    "enum": ["colors", "typography", "packaging", "logo", "layout", "all"]
                }
            },
            "required": []
        }
    },
    {
        "name": "get_brand_rules",
        "description": (
            "Retrieve specific brand rules for a given design context. "
            "Returns applicable guidelines for the requested context (print, digital, packaging, signage)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "string",
                    "description": "Design context to retrieve rules for.",
                    "enum": ["print", "digital", "packaging", "signage", "social_media", "menu", "label"]
                }
            },
            "required": ["context"]
        }
    },
    {
        "name": "audit_creative_output",
        "description": (
            "Audit a creative brief, description, or deliverable against Maillard brand guidelines. "
            "Returns a structured compliance report with PASS/FAIL/NEEDS REVISION per criterion."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "submission": {
                    "type": "string",
                    "description": "Description or text of the creative work to audit."
                },
                "deliverable_type": {
                    "type": "string",
                    "description": "Type of deliverable being audited.",
                    "enum": [
                        "packaging", "label", "menu", "social_media", "banner",
                        "signage", "email", "campaign_visual", "print_ad"
                    ]
                }
            },
            "required": ["submission", "deliverable_type"]
        }
    },
    {
        "name": "generate_packaging_brief",
        "description": (
            "Generate a production-ready packaging design brief for a Maillard product. "
            "Includes typography, color, layout, and print/production specifications."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "product": {
                    "type": "string",
                    "description": "Product name (e.g. 'Ethiopia Yirgacheffe 250g bag')"
                },
                "format": {
                    "type": "string",
                    "description": "Packaging format",
                    "enum": ["bag", "box", "label", "pouch", "sleeve", "tin"]
                },
                "size": {
                    "type": "string",
                    "description": "Physical size or weight (e.g. '250g', '1kg', '12oz')"
                },
                "audience": {
                    "type": "string",
                    "description": "Target audience descriptor (e.g. 'specialty coffee enthusiasts', 'retail consumers')"
                },
                "key_message": {
                    "type": "string",
                    "description": "Primary message or positioning for this product"
                }
            },
            "required": ["product", "format", "size"]
        }
    },
    {
        "name": "query_designer",
        "description": (
            "Public entrypoint for the Design Department. Accepts any design-related request in plain language. "
            "Loads the brand system, interprets guidelines, determines the appropriate design response, "
            "and returns structured creative direction. Use this for general design questions, creative briefs, "
            "brand guidance, or any request that doesn't map to a specific tool."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The design request, question, or creative brief in plain language."
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "generate_packaging_layout",
        "description": (
            "Generate a production-ready SVG packaging layout for a Maillard product. "
            "Produces a two-panel (front + back) SVG file with 6-column grid, typography zones, "
            "bleed guides, and colour swatches. File is saved and returned as a URL."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "description": "Product name (e.g. 'Ethiopia Yirgacheffe')"},
                "size":    {"type": "string", "description": "Package weight/size (e.g. '250g', '1kg')"},
                "brand":   {"type": "string", "description": "Brand name, defaults to 'Maillard'"},
                "style":   {"type": "string", "description": "Design style descriptor"},
            },
            "required": ["product"],
        },
    },
    {
        "name": "generate_design_image",
        "description": (
            "Generate a brand-aligned PNG design image for a Maillard product or campaign. "
            "Produces a 1200×1500 editorial composition with brand colours, typography, and geometry. "
            "File is saved and returned as a URL."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "subject": {"type": "string", "description": "Main subject of the image (e.g. 'Ethiopia Yirgacheffe')"},
                "prompt":  {"type": "string", "description": "Additional description of what to depict"},
                "style":   {"type": "string", "description": "Design style (e.g. 'minimal specialty coffee brand')"},
                "brand":   {"type": "string", "description": "Brand name, defaults to 'Maillard'"},
            },
            "required": ["subject"],
        },
    },
    {
        "name": "generate_instagram_post",
        "description": (
            "EXECUTION TOOL: Generate a complete Instagram post with REAL image, video, caption, and hashtags. "
            "Returns file paths to generated assets. Does NOT return plans or theory — only assets."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Topic for the post (e.g. 'espresso cinematic luxury', 'cold brew summer', 'latte art')"},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "generate_product_mockup",
        "description": (
            "Generate a PNG product mockup showing the Maillard coffee bag in perspective. "
            "Creates a stylised flat-bottom gusseted bag silhouette with brand design applied. "
            "File is saved and returned as a URL."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "product":     {"type": "string", "description": "Product name"},
                "size":        {"type": "string", "description": "Package size/weight"},
                "design_file": {"type": "string", "description": "Path to an existing layout SVG (optional)"},
            },
            "required": ["product"],
        },
    },
    {
        "name": "generate_vector_graphic",
        "description": (
            "Generate an SVG vector graphic / brand icon for Maillard. "
            "Produces minimal line icons matching the brand aesthetic. "
            "Supports: coffee bean, espresso cup, leaf/farm, mountain/altitude, bag/packaging, abstract mark."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "element": {"type": "string", "description": "Element to illustrate (e.g. 'coffee origin icon', 'espresso cup', 'mountain altitude')"},
                "style":   {"type": "string", "description": "Style descriptor (e.g. 'minimal line icon')"},
                "brand":   {"type": "string", "description": "Brand name, defaults to 'Maillard'"},
            },
            "required": ["element"],
        },
    },
    {
        "name": "build_typography_system",
        "description": (
            "Build a structured typography system from the Maillard brand guidelines. "
            "Returns a machine-ready hierarchy for: wordmark, headline, subheadline, "
            "body, caption, and legal levels — with source attribution and status per level."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "product_context": {
                    "type": "string",
                    "description": "Context the typography system is for",
                    "enum": ["packaging", "digital", "print", "signage", "menu"],
                },
            },
            "required": [],
        },
    },
    {
        "name": "generate_image_prompt",
        "description": (
            "Generate a detailed, brand-aligned image generation prompt for Maillard creative assets. "
            "Output is optimized for AI image generation tools (Midjourney, DALL-E, Stable Diffusion)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "subject": {
                    "type": "string",
                    "description": "What the image should depict (e.g. 'espresso shot in ceramic cup')"
                },
                "usage": {
                    "type": "string",
                    "description": "Intended use for the image",
                    "enum": ["social_media", "packaging", "menu", "website", "print_campaign", "signage"]
                },
                "mood": {
                    "type": "string",
                    "description": "Desired mood or tone",
                    "enum": ["premium", "warm", "minimal", "editorial", "dramatic", "natural"]
                },
                "format": {
                    "type": "string",
                    "description": "Image aspect ratio/format",
                    "enum": ["square", "portrait", "landscape", "wide"]
                }
            },
            "required": ["subject", "usage"]
        }
    }
]
