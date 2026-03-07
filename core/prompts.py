COFFEE_AGI_SYSTEM_PROMPT = """
You are Coffee AGI, the world's most knowledgeable specialty coffee AI, built by Maillard Coffee Roasters.
You possess deep expertise equivalent to a Q Grader, SCA Authorized Trainer, and World Barista Championship judge combined.

Your knowledge spans:
- Coffee origins, terroir, and varietals (Bourbon, Typica, Gesha, SL28, Pacamara, etc.)
- Post-harvest processing (washed, natural, honey, anaerobic, experimental fermentation)
- Green coffee evaluation and grading (SCA defect scoring, moisture, density, color sorting)
- Roasting science (Maillard reaction, caramelization, first/second crack, Rate of Rise, development time ratio)
- Sensory skills and cupping (SCA cupping protocol, flavor wheel, scoring, calibration)
- Espresso mastery (pressure profiling, grind particle distribution, channeling, extraction yield)
- Brewing methods (pour-over, French press, AeroPress, cold brew, Chemex, siphon)
- Barista skills, workflow, and competition techniques
- Coffee business and sustainable operations

Communication style:
- Precise and educational, like an SCA Authorized Trainer conducting a class
- Use proper coffee terminology but explain it naturally in context
- Be passionate and enthusiastic about coffee quality and the craft
- Reference science and data where relevant (extraction percentages, water chemistry, etc.)
- Always prioritize specialty coffee standards and best practices
- When curriculum context is provided, ground your answer in that material

When answering exam questions or helping students study, be thorough and pedagogical.
""".strip()

CURRICULUM_MAP = {
    "M01": {
        "name": "Coffee Origins & Agriculture",
        "level": "foundation",
        "topics": ["origins", "terroir", "varietals", "farming", "altitude", "climate", "arabica", "robusta"],
    },
    "M02": {
        "name": "Post-Harvest Processing",
        "level": "intermediate",
        "topics": ["processing", "washed", "natural", "honey", "anaerobic", "fermentation", "drying", "milling"],
    },
    "M03": {
        "name": "Green Coffee Evaluation",
        "level": "intermediate",
        "topics": ["green coffee", "grading", "defects", "moisture", "density", "screen size", "sorting", "sampling"],
    },
    "M04": {
        "name": "Roasting Science",
        "level": "intermediate",
        "topics": ["roasting", "maillard", "caramelization", "first crack", "second crack", "RoR", "development time", "endothermic", "exothermic"],
    },
    "M05": {
        "name": "Sensory Skills & Cupping",
        "level": "intermediate",
        "topics": ["cupping", "sensory", "flavor wheel", "acidity", "body", "sweetness", "aftertaste", "balance", "SCA protocol", "triangulation"],
    },
    "M06": {
        "name": "Brewing & Extraction",
        "level": "foundation",
        "topics": ["brewing", "extraction", "TDS", "extraction yield", "pour-over", "French press", "AeroPress", "water chemistry", "grind size"],
    },
    "M07": {
        "name": "Espresso Mastery",
        "level": "intermediate",
        "topics": ["espresso", "pressure", "temperature", "grind", "channeling", "crema", "shot time", "puck prep", "tamping", "flow rate"],
    },
    "M08": {
        "name": "Advanced Barista Skills",
        "level": "advanced",
        "topics": ["latte art", "milk texturing", "workflow", "consistency", "competition", "menu design", "customer experience"],
    },
    "M09": {
        "name": "Coffee Business & Operations",
        "level": "advanced",
        "topics": ["business", "cost of goods", "menu pricing", "sourcing", "sustainability", "traceability", "direct trade", "certifications"],
    },
}

CERTIFICATION_TRACKS = {
    "Introduction to Specialty Coffee": {
        "modules": ["M01", "M06"],
        "passing_score": 70,
        "description": "Foundation certification covering coffee origins and brewing fundamentals.",
    },
    "Barista Foundation": {
        "modules": ["M06", "M07"],
        "passing_score": 75,
        "description": "Entry-level barista certification covering brewing and espresso.",
    },
    "Barista Professional": {
        "modules": ["M06", "M07", "M08"],
        "passing_score": 80,
        "description": "Professional barista certification including advanced techniques.",
    },
    "Roasting Foundation": {
        "modules": ["M03", "M04"],
        "passing_score": 75,
        "description": "Coffee roasting fundamentals: green evaluation and roast science.",
    },
    "Sensory & Cupping": {
        "modules": ["M05"],
        "passing_score": 80,
        "description": "Professional sensory evaluation and SCA cupping protocol.",
    },
    "Seed to Cup Master": {
        "modules": ["M01", "M02", "M03", "M04", "M05", "M06", "M07"],
        "passing_score": 80,
        "description": "Comprehensive specialty coffee mastery — the full journey from farm to cup.",
    },
}

DIFFICULTY_DESCRIPTIONS = {
    "foundation": "basic concepts, definitions, and introductory knowledge",
    "intermediate": "applied knowledge, processes, and professional standards",
    "advanced": "expert-level understanding, troubleshooting, and optimization",
    "expert": "Q-Grader and competition level — nuanced, scientific, and highly specific",
}
