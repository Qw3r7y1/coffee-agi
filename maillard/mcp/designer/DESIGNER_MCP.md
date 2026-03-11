# Maillard Design Department
## MCP Capability Overview

*Maillard Coffee Roasters — Internal Department Documentation*
*Source of truth: `maillard/guidelines/` — Design system by nineteendesign, Athens 2021*

---

### Department Mission

The Maillard Design Department is the centralized creative authority for Maillard Coffee Roasters. It governs all visual output — from packaging systems and product labels to café signage, digital campaigns, and seasonal collections. Every touchpoint that carries the Maillard name falls under the Design Department's jurisdiction.

The department operates as the final arbiter of brand compliance. No asset is distributed, printed, or published without clearing design review. The `designer-mcp` is the AI-powered intelligence layer that enforces this standard at scale, translating brand policy into actionable creative direction and auditing all creative submissions against the official brand system.

The department does not function as a support service. It is a revenue-protecting, brand-building authority that ensures Maillard's premium positioning is expressed consistently across every customer touchpoint.

---

### Core Responsibilities

**1. Brand Governance**
The Design Department owns the Maillard brand system and enforces its application across all departments and external vendors. This includes maintaining the guidelines directory, versioning all brand assets, reviewing all incoming creative requests, and issuing compliance decisions. No modification to the core brand identity — typefaces, palette, logo system, or voice — may be made without Design Department authorization.

**2. Packaging and Label Design**
Design develops production-ready briefs for all Maillard packaging: whole bean and ground coffee bags across all SKU sizes, cold brew pouches and bottles, seasonal tins and gift boxes, retail sleeve packaging, and product labels. Each brief specifies typography hierarchy, color values, layout grid, content hierarchy, and print production requirements. Labels carry mandatory content: origin, elevation, variety, process, roast level, and tasting notes — each positioned within the approved layout system.

**3. Café Environment and Signage**
The Design Department maintains visual standards for all in-café environments: menu boards (static and digital), take-away menu cards, counter signage, loyalty program materials, wayfinding, and exterior signage. In-café design must translate the Maillard premium aesthetic to physical space — materials, scale, and hierarchy are all specified at the department level.

**4. Campaign and Digital Creative**
For all marketing activations, Design provides: creative concept direction, hero image prompts and briefs, social media visual templates (feed, stories, reels), email header specifications, and printed promotional collateral. Campaign assets are reviewed against the brand system before publication. The department does not produce final digital files directly — it provides direction and briefs that guide execution.

**5. Brand Compliance Review**
Any department producing external-facing material must submit to Design review before distribution. The `audit_creative_output` tool formalizes this process: a submission is evaluated against six criteria (typography, color, logo usage, layout, tone, production readiness) and returned with a structured compliance report. The report issues a verdict of APPROVED, NEEDS REVISION, or NON-COMPLIANT, with specific actionable notes for each criterion.

---

### Department Tools

---

#### `load_brand_system`

**What it does**
Loads the complete Maillard brand system from the guidelines directory. Returns all available brand resources as structured data, making them accessible for design direction and compliance decisions.

**What it reads**
- `maillard/guidelines/Maillard Design Guideline.pdf` — master identity document
- `maillard/guidelines/brand_identity.md` — brand values, positioning, voice
- `maillard/guidelines/typography.md` — approved typefaces and sizing scale
- `maillard/guidelines/colors.json` — official palette (CMYK, RGB, HEX)
- `maillard/guidelines/packaging_rules.md` — packaging system rules
- `maillard/guidelines/layout_principles.md` — grid, whitespace, proportion
- `maillard/guidelines/logo_usage.pdf` — logo versions and prohibited uses

**What it returns**
A structured summary of the brand system for the requested section, or the full system if no section is specified. Includes actionable values: color codes, typeface names, layout principles, and compliance rules.

**Strategic value**
Every design response begins with a brand system load. This ensures that direction given by the department is always grounded in the current, authoritative version of the brand guidelines — never from memory or approximation.

**Input schema**
```json
{
  "section": "colors | typography | packaging | logo | layout | all"
}
```

---

#### `get_brand_rules`

**What it does**
Retrieves the specific brand rules that apply to a given design context. Different contexts have different requirements — digital work has different color space and resolution standards than print; signage has different hierarchy rules than social media templates.

**What it reads**
The relevant section of the brand system for the specified context, cross-referenced against the master guidelines document.

**What it returns**
A structured set of rules applicable to the requested context: typography specifications, color values, layout principles, prohibited treatments, and production standards. Formatted as actionable direction, not raw guidelines text.

**Strategic value**
Eliminates ambiguity for designers, vendors, and departments working in a specific medium. A social media producer gets social-specific rules; a packaging vendor gets print-specific rules. No manual guidelines cross-referencing required.

**Input schema**
```json
{
  "context": "print | digital | packaging | signage | social_media | menu | label"
}
```

---

#### `audit_creative_output`

**What it does**
Conducts a formal brand compliance audit on any creative brief, deliverable description, or asset concept. Evaluates the submission against six criteria drawn from the Maillard brand system and returns a structured compliance report with a binding verdict.

**What it reads**
The full brand system (via `load_brand_system`) plus the submission text provided in the request. The audit references specific guidelines for each of the six criteria.

**What it returns**
A structured audit report with the following for each criterion: status (PASS / FAIL / NEEDS REVISION), specific findings referenced to guidelines, and required corrective action if status is not PASS. Concludes with an overall compliance status, a priority actions list, and an approval authority notation.

**Strategic value**
Scales brand review across the organization. Any department can submit creative work for review and receive a consistent, detailed, authoritative compliance assessment — without requiring human Design Director time for routine submissions. High-risk or novel work is flagged for human review.

**Input schema**
```json
{
  "submission": "Description or text of the creative work to audit",
  "deliverable_type": "packaging | label | menu | social_media | banner | signage | email | campaign_visual | print_ad"
}
```

---

#### `generate_packaging_brief`

**What it does**
Generates a complete, production-ready packaging design brief for any Maillard product. The brief covers every dimension required to hand off to a designer or print production vendor: concept, layout, typography, color, production specs, and brand compliance notes.

**What it reads**
The full brand system — with particular emphasis on `packaging_rules.md`, `typography.md`, and `colors.json` — combined with the product parameters provided in the request.

**What it returns**
A structured brief with six mandatory sections (Design Concept, Layout Structure, Typography Hierarchy, Color Usage, Production Considerations, Brand Compliance) plus objective, brand context, assets required list, and vendor notes. Every typographic specification includes typeface, weight, size, tracking, and leading. Every color specification includes HEX, CMYK, and RGB values.

**Strategic value**
Compresses packaging brief development from days to minutes. Enables the business to move rapidly on product launches, seasonal releases, and SKU expansions while maintaining full brand compliance from the first deliverable.

**Input schema**
```json
{
  "product": "Product name, e.g. Ethiopia Yirgacheffe 250g bag",
  "format": "bag | box | label | pouch | sleeve | tin",
  "size": "Physical size or weight, e.g. 250g, 1kg, 12oz",
  "audience": "Target audience descriptor",
  "key_message": "Primary positioning for this product"
}
```

---

#### `generate_image_prompt`

**What it does**
Generates a detailed, brand-aligned image generation prompt for AI creative tools. The prompt is calibrated to produce imagery consistent with the Maillard aesthetic: premium, minimal, specialty-forward, editorial — not commercial stock photography.

**What it reads**
The brand system's visual identity specifications, aesthetic values from `brand_identity.md`, and the approved color palette from `colors.json`. Applies these to the subject and usage context provided.

**What it returns**
A single, detailed image generation prompt optimized for Midjourney, DALL-E, or Stable Diffusion — covering subject, lighting, composition, color palette, mood, style reference, and technical quality modifiers. Followed by a Brand Compliance note confirming alignment with the Maillard visual system.

**Strategic value**
Bridges the gap between brand standards and AI image generation. Without this tool, AI-generated imagery defaults to generic stock aesthetics that conflict with the Maillard premium positioning. With it, every generated image is calibrated to brand from the first prompt.

**Input schema**
```json
{
  "subject": "What the image should depict, e.g. espresso shot in ceramic cup",
  "usage": "social_media | packaging | menu | website | print_campaign | signage",
  "mood": "premium | warm | minimal | editorial | dramatic | natural",
  "format": "square | portrait | landscape | wide"
}
```

---

### Department Policy

The following policies govern all creative work at Maillard. They are non-negotiable. Any creative output that cannot be reconciled with these policies is non-compliant and must not be distributed, printed, or published.

**Rule 1: Brand compliance is mandatory for every deliverable.**
Every asset — regardless of originating department, urgency, or medium — must clear Design Department review before distribution or publication. There are no exceptions for speed, convenience, or informal use cases.

**Rule 2: Premium aesthetic standards are absolute.**
Maillard does not produce busy, cluttered, or cheap-looking work. Clean layouts. Generous whitespace. Confident hierarchy. If a piece does not read as premium on first impression, it is not approved. This standard applies equally to a permanent packaging label and a one-day social media post.

**Rule 3: Only approved typefaces are used in final deliverables.**
Typefaces are specified in `maillard/guidelines/typography.md`. No decorative fonts, no improvised pairings, no system fonts (Arial, Helvetica Neue default, Times New Roman) in any final output. Typography hierarchy must be legible, intentional, and consistent with the brand scale.

**Rule 4: All color usage references the official Maillard palette.**
Color values are specified in `maillard/guidelines/colors.json` as CMYK (for print), RGB, and HEX. No off-brand shades, approximations, or palette extensions are permitted without executive sign-off. Print work requires CMYK specification. Digital work requires RGB/HEX specification.

**Rule 5: All deliverables meet production-ready technical standards.**
Print output: PDF/X-1a or PDF/X-4, CMYK color space, with bleed and crop marks, minimum 300 DPI at final size. Digital output: RGB color space, minimum 2x resolution for all display contexts. Source files must be versioned and archived in the designated asset directories.

**Rule 6: Prohibited treatments are never used.**
The following are prohibited in all Maillard creative work without explicit Design Director sign-off: gradient overlays not specified in the brand system, glow effects, drop shadows, bevels and emboss effects, decorative borders and ornamental framing, mixed brand voices within a single piece, low-resolution assets used at display size, clip art and stock illustration aesthetics, and generic coffee imagery that does not reflect the Maillard specialty positioning.

---

### Department Resources

All brand files are maintained in the source of truth directory: `maillard/guidelines/`

| File | Type | Purpose |
|------|------|---------|
| `Maillard Design Guideline.pdf` | PDF | Master brand identity document — the comprehensive visual identity system |
| `brand_identity.md` | Markdown | Brand overview, values, positioning, tagline, and voice guidelines |
| `typography.md` | Markdown | Approved typefaces, weights, sizes, tracking, leading, and pairing rules |
| `colors.json` | JSON | Official palette with CMYK, RGB, and HEX values for all brand colors |
| `packaging_rules.md` | Markdown | Packaging system architecture, hierarchy rules, and print specifications |
| `layout_principles.md` | Markdown | Grid system, whitespace standards, proportion, and spatial relationships |
| `logo_usage.pdf` | PDF | Logo versions, minimum sizes, clear space requirements, and prohibited uses |

Brand asset directories:
- `data/maillard/logos/` — official logo files in all approved formats
- `data/maillard/images/` — approved photography and visual assets
- `data/maillard/fonts/` — licensed font files
- `data/maillard/guidelines/` — uploaded brand documents (synced from Dropbox)

---

### Design Deliverables

#### Packaging Systems
- Whole bean and ground coffee bags: 250g, 500g, 1kg standard formats
- Cold brew pouches and bottles
- Gift boxes and seasonal tins
- Retail sleeve packaging and shipper cartons
- Sample packaging and subscription box inserts

#### Product Labels
- Single-origin labels: origin, elevation, variety, process, roast level, tasting notes
- Blend labels: blend name, roast profile, flavor story
- Cold brew product labels
- Tea and matcha labels
- Wholesale and B2B packaging variants

#### Seasonal and Limited Edition Systems
- Holiday packaging identity and label system
- Seasonal limited edition visual extensions
- Collaboration and co-branded packaging specifications

#### Campaign Visuals
- Hero imagery briefs and AI generation prompts for product launches
- Social media visual templates: feed posts (square and portrait), stories, reels covers
- Email header design specifications
- Printed promotional collateral: flyers, posters, shelf talkers
- Out-of-home advertising briefs

#### Café Environment
- Menu boards: static print and digital display formats
- Take-away menu cards and table cards
- Counter and point-of-sale signage
- Loyalty program cards and materials
- Wayfinding and environmental signage
- Packaging for in-café retail: bags, cups, merchandise
- Staff uniforms and workwear specification briefs

#### Digital and Web
- Website hero imagery direction
- Product photography briefs and shot lists
- Digital advertising creative specifications
- Brand presentation templates

---

### Interdepartmental Collaboration

#### Design — Marketing
Marketing submits campaign briefs via the MCP system. Design reviews all briefs for brand alignment, generates image prompts and visual concept direction, specifies asset requirements, and returns approved creative direction with compliance notes. Marketing may not publish any campaign asset without Design approval.

The standard workflow: Marketing submits brief → Design reviews for brand alignment → Design generates image prompts and visual direction → Marketing executes with approved assets → Design audits final submission before publication.

#### Design — Sales
Sales submits requests for wholesale presentation materials. Design produces pitch deck specifications, sell sheet layouts, product photography briefs, and wholesale catalog designs. All sales materials carry the Maillard brand and must reflect the premium positioning — no ad-hoc or improvised sales collateral.

#### Design — Operations
Operations submits requests for in-café printed materials: menus, signage, training cards, operational notices. Design produces and approves all physical touchpoints within the café environment. Operations does not self-produce printed materials.

#### Design — Executive
Executive holds approval authority over any modification to the core brand system. This includes changes to the approved typeface palette, color system, logo system, or foundational brand values. No brand system modifications are made without executive sign-off. Design communicates all proposed system changes to Executive before implementation.

---

### Example Workflow

**New Product Launch: Ethiopia Yirgacheffe — Instagram Campaign and Packaging Update**

1. **Brief intake**: Marketing submits a campaign brief for the Ethiopia Yirgacheffe launch — new single-origin, Instagram campaign, updated bag label.

2. **Brand system load**: Designer MCP calls `load_brand_system` to confirm the current palette, typography rules, and packaging hierarchy. Flags any seasonal updates or recent system changes.

3. **Packaging brief**: Designer MCP calls `generate_packaging_brief` with product="Ethiopia Yirgacheffe 250g bag", format="bag", key_message="floral brightness, washed process, high elevation". Returns a complete production brief with typography hierarchy, color specification, layout grid, and print specs.

4. **Campaign image prompts**: Designer MCP calls `generate_image_prompt` for the campaign hero: subject="whole coffee beans on a linen surface with a white ceramic cup", usage="social_media", mood="editorial". Returns a detailed, brand-aligned prompt for the creative team.

5. **Label copy review**: Marketing submits draft label copy. Designer MCP calls `audit_creative_output` with submission=label copy draft, deliverable_type="label". Returns compliance report — flags a non-approved abbreviation for "washed process" and confirms typography and color direction.

6. **Creative execution**: Creative team executes the packaging update and campaign imagery using the Design Department brief and approved prompts.

7. **Final audit**: Marketing submits final campaign creative for pre-publication review. Designer MCP calls `audit_creative_output`. Returns APPROVED with notes.

8. **Publication**: Campaign proceeds. Packaging goes to print with approved PDF/X-4 file.

---

### Strategic Value

The Design Department is not a support function — it is a revenue-protecting and brand-building asset that operates at the intersection of creative excellence and operational discipline.

A consistent, premium visual identity is one of the most powerful commercial tools available to a specialty coffee brand. It commands higher price points by signaling quality before the product is tasted. It builds recognition and loyalty with specialty consumers who make purchasing decisions based on aesthetic signals as much as product information. It differentiates Maillard from commodity competitors whose packaging communicates neither care nor craft.

Without a functioning design governance system, brand consistency degrades over time as individual contributors make ad-hoc decisions that accumulate into visual noise. The premium positioning that justifies Maillard's price point erodes. Consumer trust follows.

The `designer-mcp` ensures that brand standards scale with the organization. As Maillard grows — more SKUs, more departments, more external partners, more markets — the Design Department maintains consistent governance without requiring proportional increases in human design capacity. Every department can move faster, knowing that creative requests are reviewed, briefed, and approved against a single authoritative standard. The standard does not drift. The brand does not dilute.

This is the strategic case for design governance as infrastructure, not overhead.

---

### Connection Notes

#### Accessing the Designer MCP via the Maillard AI Control Panel

Navigate to `http://localhost:8000/dashboard` for the multi-department control panel. Open the Design Department card to access the Designer interface at `http://localhost:8000/designer`.

#### Direct API Access

The Designer MCP is available via the MCP Orchestrator at the following endpoints:

```
POST http://localhost:8000/mcp/dispatch
Content-Type: application/json

{
  "task": "Your design request or brief",
  "department": "designer"
}
```

To use a specific tool directly:
```json
{
  "task": "Generate a packaging brief for Ethiopia Yirgacheffe",
  "department": "designer",
  "tool_name": "generate_packaging_brief",
  "arguments": {
    "product": "Ethiopia Yirgacheffe",
    "format": "bag",
    "size": "250g",
    "audience": "specialty coffee enthusiasts",
    "key_message": "Floral brightness, washed process, high elevation"
  }
}
```

To preview routing without executing:
```
GET http://localhost:8000/mcp/route?task=Design a packaging label for a new single origin
```

To list all available tools:
```
GET http://localhost:8000/mcp/tools/designer
```

To list all active departments:
```
GET http://localhost:8000/mcp/departments
```

#### Environment Requirements

- `ANTHROPIC_API_KEY` — required for Claude-powered design intelligence
- FastAPI server running at port 8000 (`uvicorn main:app --reload`)
- Brand guidelines uploaded to `maillard/guidelines/` for full brand system access

---

*Maillard Coffee Roasters — Design Department MCP Capability Overview*
*Source of truth: `maillard/guidelines/` | Design system by nineteendesign, Athens 2021*
*Version: 2.0 | Updated: March 2026*
