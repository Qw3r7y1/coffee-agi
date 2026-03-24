"""
Coffee AGI Health Check — verifies all systems are operational.

Run: python scripts/health_check.py
"""
from __future__ import annotations
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_ok = 0
_fail = 0


def check(name: str, condition: bool, detail: str = ""):
    global _ok, _fail
    if condition:
        _ok += 1
        print(f"  [OK]   {name}")
    else:
        _fail += 1
        print(f"  [FAIL] {name} -- {detail}")


def main():
    print("=" * 50)
    print("Coffee AGI Health Check")
    print("=" * 50)

    # 1. Data files
    print("\nDATA FILES")
    for f in ["data/current_state.json", "data/recipes.json", "data/costs.json", "data/prices.json"]:
        p = ROOT / f
        check(f, p.exists())
        if p.exists():
            try:
                json.loads(p.read_text(encoding="utf-8"))
            except Exception as e:
                check(f"{f} valid JSON", False, str(e))

    # 2. Core imports
    print("\nCORE IMPORTS")
    imports = [
        ("State loader", "maillard.mcp.operations.state_loader", "get_operational_snapshot"),
        ("Cost engine", "maillard.mcp.operations.cost_engine", "calculate_product_costs"),
        ("Procurement", "maillard.mcp.operations.procurement", "get_procurement_report"),
        ("Decision engine", "maillard.mcp.operations.decision_engine", "generate_morning_brief"),
        ("Sales intel", "maillard.mcp.sales.intelligence", "generate_sales_intelligence"),
        ("Normalization", "maillard.mcp.sales.normalization", "normalize_product_name"),
        ("Buying signal", "maillard.mcp.analyst.buying_signal", "get_buying_signal"),
        ("Market engine", "maillard.mcp.analyst.market_data_engine", "get_validated_coffee_data"),
        ("Approval", "maillard.mcp.operations.approval", "add_action"),
        ("Feedback", "maillard.mcp.operations.feedback", "analyze_execution_feedback"),
        ("Shrinkage", "maillard.mcp.operations.shrinkage", "run_shrinkage_check"),
    ]
    for label, mod, func in imports:
        try:
            m = __import__(mod, fromlist=[func])
            getattr(m, func)
            check(label, True)
        except Exception as e:
            check(label, False, str(e)[:60])

    # 3. Orchestrator + departments
    print("\nDEPARTMENTS")
    try:
        from maillard.mcp.orchestrator.server import OrchestratorMCP
        o = OrchestratorMCP()
        depts = list(o.registry.keys())
        check(f"Orchestrator ({len(depts)} depts)", len(depts) >= 10)
        for dept, server in o.registry.items():
            tools = server.tools
            query_tool = f"query_{dept}"
            has_query = any(t["name"] == query_tool for t in tools)
            check(f"  {dept} ({len(tools)} tools)", has_query or dept in ("analyst", "operations"),
                  f"missing {query_tool}" if not has_query else "")
    except Exception as e:
        check("Orchestrator", False, str(e)[:60])

    # 4. State snapshot
    print("\nSTATE SNAPSHOT")
    try:
        from maillard.mcp.operations.state_loader import get_operational_snapshot
        snap = get_operational_snapshot()
        inv = snap.get("updated_inventory", {})
        sales = snap.get("state", {}).get("sales_today", {})
        check(f"Inventory: {len(inv)} items", len(inv) > 0)
        check(f"Sales: {sum(sales.values())} units", True)
        check(f"Recipes: {len(snap.get('recipes', {}))} loaded", len(snap.get("recipes", {})) > 0)
    except Exception as e:
        check("State snapshot", False, str(e)[:60])

    # 5. Cost engine
    print("\nCOST ENGINE")
    try:
        from maillard.mcp.operations.cost_engine import calculate_product_costs
        costs = calculate_product_costs()
        check(f"Products: {len(costs)}", len(costs) > 0)
        crit = [p for p, d in costs.items() if d.get("grade") == "CRITICAL"]
        check(f"Critical margins: {len(crit)}", True)
    except Exception as e:
        check("Cost engine", False, str(e)[:60])

    # 6. Server
    print("\nSERVER")
    try:
        import httpx
        r = httpx.get("http://127.0.0.1:8000/", timeout=5)
        check("API running", r.status_code == 200)
    except Exception:
        check("API running", False, "Server not running (start with uvicorn)")

    print(f"\n{'=' * 50}")
    print(f"RESULT: {_ok} passed, {_fail} failed")
    print(f"{'=' * 50}")
    return 1 if _fail else 0


if __name__ == "__main__":
    sys.exit(main())
