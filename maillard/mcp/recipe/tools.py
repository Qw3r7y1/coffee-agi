"""Recipe MCP — Tool definitions."""

TOOLS: list[dict] = [
    {
        "name": "query_recipe",
        "description": (
            "Authoritative recipe Q&A grounded in Maillard recipe PDFs. "
            "Use for any question about how to make a Maillard drink or food item, "
            "brew parameters, techniques, or menu details. Always returns Maillard-specific answers."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The recipe question or request in plain language."
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "lookup_drink",
        "description": (
            "Look up the complete Maillard recipe for a specific drink. "
            "Returns: cup size, espresso shots, milk volume, temperature, technique steps, and notes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "drink": {
                    "type": "string",
                    "description": "Drink name (e.g. 'Freddo Cappuccino', 'Oat Latte', 'Cold Brew')"
                }
            },
            "required": ["drink"]
        }
    },
    {
        "name": "lookup_food",
        "description": (
            "Look up the complete Maillard recipe or description for a food menu item. "
            "Returns ingredients, preparation steps, and any notes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "item": {
                    "type": "string",
                    "description": "Food item name (e.g. 'Sweet Crepe', 'Savory Crepe', 'Parfait')"
                }
            },
            "required": ["item"]
        }
    },
    {
        "name": "get_menu",
        "description": (
            "Return the complete Maillard menu with all items, sizes, and prices "
            "as extracted from the official menu PDF."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Optional filter: 'drinks', 'food', 'all'",
                    "enum": ["drinks", "food", "all"],
                    "default": "all"
                }
            },
            "required": []
        }
    },
    {
        "name": "get_technique",
        "description": (
            "Get detailed Maillard technique instructions for a specific preparation method. "
            "Returns parameters, steps, and quality standards from the guide."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "technique": {
                    "type": "string",
                    "description": "Technique name (e.g. 'steaming milk', 'espresso extraction', 'cold brew', 'latte art')"
                }
            },
            "required": ["technique"]
        }
    },
    {
        "name": "list_sources",
        "description": "List all recipe PDF sources currently loaded and the number of indexed chunks.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]
