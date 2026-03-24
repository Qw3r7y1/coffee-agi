"""
Coffee AGI Daily Run — sequential execution of all systems.

Run every morning before opening:
  python scripts/run_daily.py

Steps:
  1. Load state from JSON files
  2. Calculate costs + margins
  3. Run procurement analysis
  4. Generate sales intelligence
  5. Fetch market signal
  6. Generate morning brief
  7. Submit actions for approval
  8. Print summary

No crashes. Falls back gracefully if any step fails.
"""
from __future__ import annotations
import sys, json, asyncio
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def step(name: str, fn, *args):
    """Run a step, return result or None on failure."""
    try:
        result = fn(*args)
        print(f"  [OK] {name}")
        return result
    except Exception as e:
        print(f"  [!!] {name}: {str(e)[:60]}")
        return None


def main():
    now = datetime.now(timezone.utc)
    print("=" * 50)
    print(f"Coffee AGI Daily Run")
    print(f"{now.strftime('%A, %B %d %Y %H:%M UTC')}")
    print("=" * 50)

    # 1. State
    print("\n1. Loading state...")
    from maillard.mcp.operations.state_loader import get_operational_snapshot
    snap = step("State snapshot", get_operational_snapshot)
    if snap:
        inv = snap.get("updated_inventory", {})
        sales = snap.get("state", {}).get("sales_today", {})
        print(f"     Inventory: {len(inv)} items | Sales: {sum(sales.values())} units")

    # 1b. Square POS — validate then pull
    print("\n1b. Square POS sales...")
    square_live = False
    try:
        from scripts.square_sales_connector import (
            validate_square_connection,
            get_today_square_sales,
            merge_into_state as square_merge,
        )
        validation = validate_square_connection()
        print(f"  [..] Square status: {validation['status']}")
        if validation["status"] == "connected_live":
            square_sales = get_today_square_sales()
            if square_sales:
                square_merge(square_sales)
                square_live = True
                print(f"  [OK] Square sales: {sum(square_sales.values())} units across {len(square_sales)} products")
            else:
                print(f"  [--] Square: live but no sales today")
        else:
            print(f"  [!!] Square blocked: {validation['details'][:80]}")
    except Exception as e:
        print(f"  [!!] Square sales: {str(e)[:60]}")

    # 2. Costs
    print("\n2. Cost analysis...")
    from maillard.mcp.operations.cost_engine import calculate_product_costs
    costs = step("Product costs", calculate_product_costs)
    if costs:
        crit = [p for p, d in costs.items() if d.get("grade") in ("CRITICAL", "LOW")]
        print(f"     {len(costs)} products | {len(crit)} margin alerts")

    # 3. Procurement
    print("\n3. Procurement...")
    from maillard.mcp.operations.procurement import get_procurement_report
    proc = step("Procurement report", get_procurement_report)
    if proc:
        recs = proc.get("recommendations", [])
        print(f"     {len(recs)} items to order | EUR {proc.get('total_estimated_cost', 0):.0f}")

    # 4. Sales intelligence (requires live Square data)
    print("\n4. Sales intelligence...")
    sales_intel = None
    if square_live:
        from maillard.mcp.sales.intelligence import generate_sales_intelligence
        sales_intel = step("Sales intelligence", generate_sales_intelligence)
        if sales_intel:
            push = sales_intel.get("push", [])
            depri = sales_intel.get("deprioritize", [])
            print(f"     Push: {len(push)} | Deprioritize: {len(depri)}")
    else:
        print("  [--] Skipped: Square not validated as live production")

    # 5. Market signal
    print("\n5. Market signal...")
    try:
        from maillard.mcp.analyst.buying_signal import get_buying_signal
        loop = asyncio.new_event_loop()
        signal = loop.run_until_complete(get_buying_signal(days=14))
        loop.close()
        print(f"  [OK] Market signal")
        print(f"     Market: {signal.get('direction')} | Signal: {signal.get('recommendation')} | Price: ${signal.get('price_dollars_lb', 0):.4f}/lb")
    except Exception as e:
        print(f"  [!!] Market signal: {str(e)[:60]}")

    # 6. Morning brief
    print("\n6. Morning brief...")
    from maillard.mcp.operations.decision_engine import generate_morning_brief
    brief = step("Morning brief", generate_morning_brief)
    if brief:
        decisions = brief.get("decisions", [])
        print(f"     {len(decisions)} actions generated")
        print()
        print(brief.get("brief", ""))

    # 7. Submit to queue
    print("\n7. Action queue...")
    if brief and brief.get("decisions"):
        from maillard.mcp.operations.approval import add_from_dict
        count = 0
        for d in brief["decisions"]:
            add_from_dict(d)
            count += 1
        print(f"  [OK] {count} actions submitted for approval")
    else:
        print("  [--] No actions to submit")

    # 8. Feedback
    print("\n8. Yesterday's feedback...")
    from maillard.mcp.operations.feedback import analyze_execution_feedback
    fb = step("Feedback analysis", analyze_execution_feedback)
    if fb:
        missed = len(fb.get("missed_actions", []))
        partial = len(fb.get("partial_execution", []))
        if missed or partial:
            print(f"     Missed: {missed} | Partial: {partial}")
        else:
            print(f"     No issues from yesterday")

    print(f"\n{'=' * 50}")
    print("Daily run complete. Check dashboard: http://127.0.0.1:8000/ops")
    print("=" * 50)


if __name__ == "__main__":
    main()
