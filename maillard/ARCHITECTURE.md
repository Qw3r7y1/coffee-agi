# Maillard MCP Architecture

## Overview

Multi-department AI agent layer for Maillard Coffee Roasters.
11 department MCP servers + 1 orchestrator, built on the existing FastAPI stack.

---

## Folder Structure

```
maillard/
├── __init__.py
├── api.py                          # FastAPI router — mount at /mcp
├── ARCHITECTURE.md                 # This file
│
├── mcp/
│   ├── __init__.py
│   ├── orchestrator/
│   │   ├── __init__.py
│   │   └── server.py               # Master router + dispatch logic
│   │
│   ├── shared/
│   │   ├── __init__.py
│   │   ├── base_server.py          # Abstract base class for all MCPs
│   │   ├── claude_client.py        # Shared Anthropic client + coffee knowledge
│   │   ├── handoff.py              # Inter-department handoff protocol
│   │   └── kb_client.py            # KnowledgeBase wrapper
│   │
│   ├── designer/
│   │   ├── server.py | tools.py | prompts.py | resources.py | policy.md | DESIGNER_MCP.md
│   ├── accounting/server.py
│   ├── legal/server.py
│   ├── analyst/server.py
│   ├── operations/server.py
│   ├── procurement/server.py
│   ├── hr/server.py
│   ├── marketing/server.py
│   ├── sales/server.py
│   └── executive/server.py
│
├── schemas/
│   ├── __init__.py
│   └── handoff.py                  # Pydantic models for API contracts
│
└── data/
    ├── designer/                   # Design assets, versions, audit logs
    ├── accounting/                 # Invoices, budgets, audit logs
    ├── legal/                      # Contracts, compliance docs, audit logs
    ├── analyst/                    # Reports, datasets, audit logs
    ├── operations/                 # Schedules, checklists, audit logs
    ├── procurement/                # POs, supplier evaluations, audit logs
    ├── hr/                         # Job postings, onboarding plans, audit logs
    ├── marketing/                  # Campaign briefs, content, audit logs
    ├── sales/                      # Proposals, pricing, audit logs
    └── executive/                  # Strategic briefs, OKRs, audit logs
```

---

## API Endpoints

Once mounted, the MCP layer exposes:

| Method | Path | Description |
|--------|------|-------------|
| GET | /mcp/departments | List all departments |
| GET | /mcp/tools/{department} | List tools for a department |
| POST | /mcp/dispatch | Route + execute any task |
| GET | /mcp/route?task=... | Preview routing without executing |

---

## Orchestrator Routing Logic

The orchestrator uses regex keyword matching against the task text.
Rules are evaluated in priority order:

| Keywords | Routed To |
|----------|-----------|
| brand, design, logo, packaging, visual, typography, guideline | designer |
| campaign, social media, promo, content, instagram, newsletter | marketing |
| sale, revenue, wholesale, customer, lead, quote | sales |
| analysis, report, kpi, dashboard, trend, data, forecast | analyst |
| operations, workflow, shift, schedule, barista, recipe, equipment | operations |
| procure, supplier, vendor, sourcing, green coffee, purchase | procurement |
| invoice, budget, cost, expense, profit, tax, financial | accounting |
| legal, contract, compliance, trademark, license, gdpr | legal |
| hr, hire, recruit, staff, onboard, payroll, performance review | hr |
| strategy, vision, board, executive, okr, decision, roadmap | executive |

Fallback: `executive`

---

## Inter-Department Handoff Routes

```
sales        → accounting, operations, marketing, executive
marketing    → designer, sales, analyst, executive
designer     → marketing, executive
operations   → procurement, hr, accounting, executive
procurement  → accounting, operations, legal
accounting   → executive, legal
legal        → executive, hr, accounting
hr           → legal, accounting, executive
analyst      → executive, marketing, sales, operations
executive    → * (all departments)
orchestrator → * (all departments)
```

---

## Department Responsibilities

| Department | Primary Function |
|------------|-----------------|
| designer | Brand governance, packaging, visual systems, creative audits |
| accounting | Invoicing, COGS, budgets, financial reporting |
| legal | Contracts, compliance, trademarks, regulatory |
| analyst | KPIs, sales analysis, forecasting, reporting |
| operations | Café ops, recipes, scheduling, equipment |
| procurement | Green coffee sourcing, supplier mgmt, POs |
| hr | Hiring, onboarding, training, performance |
| marketing | Campaigns, social media, content, brand voice |
| sales | Revenue, wholesale, pricing, upsell |
| executive | Strategy, OKRs, decisions, escalations |
| orchestrator | Routes all tasks to correct department |

---

## Core Design Decisions

**Coffee knowledge in every department.**
`shared/claude_client.py` prepends the full Maillard coffee knowledge base into every
department's system prompt. Every MCP agent understands espresso ratios, milk technique,
recipes, and menu pricing — regardless of their department function.

**Operations uses RAG.**
`operations-mcp` queries the BM25 knowledge base (topic: `maillard-recipes`) before
answering recipe questions, ensuring exact Maillard guide accuracy.

**Executive uses Opus.**
The executive MCP calls `claude-opus-4-6` for strategic and escalation tasks.
All other departments use `claude-haiku-4-5-20251001` for speed and cost efficiency.

**Audit trail on every call.**
Every tool invocation is logged to `maillard/data/{department}/audit/audit.jsonl`.

**Handoff validation.**
All inter-department handoffs are validated against the route table before execution.
Invalid routes return a structured error — no silent failures.

---

## Integration with main.py

Add to `main.py`:
```python
from maillard.api import router as mcp_router
app.include_router(mcp_router, prefix="/mcp")
```

---

## Recommended Implementation Order

1. shared/ — base_server, claude_client, handoff, kb_client (done)
2. operations-mcp — highest daily use, recipe RAG critical path
3. designer-mcp — brand governance, blocks all other visual work
4. orchestrator-mcp — enables full routing (done)
5. marketing-mcp — depends on designer
6. sales-mcp — depends on pricing data
7. analyst-mcp — depends on sales + operations data
8. accounting-mcp — depends on sales + procurement
9. procurement-mcp — semi-independent
10. hr-mcp — semi-independent
11. legal-mcp — semi-independent
12. executive-mcp — aggregates all departments
