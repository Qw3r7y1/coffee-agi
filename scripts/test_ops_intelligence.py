"""
Operations Intelligence end-to-end test.

Seeds the DB with realistic data, then tests all intelligence functions.

Run: python scripts/test_ops_intelligence.py
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

API = "http://127.0.0.1:8000/api/ops"
_p = 0
_f = 0


def check(name, cond, detail=""):
    global _p, _f
    if cond:
        _p += 1
        print(f"  [PASS] {name}")
    else:
        _f += 1
        print(f"  [FAIL] {name} -- {detail}")


def post(path, data):
    r = httpx.post(f"{API}{path}", json=data, timeout=10)
    return r.status_code, r.json()


def get(path):
    r = httpx.get(f"{API}{path}", timeout=10)
    return r.status_code, r.json()


def put(path, data):
    r = httpx.put(f"{API}{path}", json=data, timeout=10)
    return r.status_code, r.json()


def main():
    print("=" * 60)
    print("OPERATIONS INTELLIGENCE -- E2E TEST")
    print("=" * 60)

    # ══════════════════════════════════════════════════════════
    # SEED DATA
    # ══════════════════════════════════════════════════════════
    print("\n--- Seeding inventory ---")

    # Green coffees
    for sku, name, qty, cost in [
        ("GC-ETH-YRG", "Ethiopia Yirgacheffe Gr1", 40, 8.50),
        ("GC-COL-HUI", "Colombia Huila Supremo", 30, 7.20),
    ]:
        post("/inventory", {
            "sku": sku, "name": name, "category": "green_coffee",
            "unit": "kg", "quantity": qty, "min_quantity": 10,
            "cost_per_unit": cost, "supplier": "Ally Coffee",
        })

    # Roasted coffees
    for sku, name, qty in [
        ("RC-ETH-MED", "Ethiopia Yirg Medium Roast", 8),
        ("RC-COL-MED", "Colombia Huila Medium Roast", 3),
    ]:
        post("/inventory", {
            "sku": sku, "name": name, "category": "roasted_coffee",
            "unit": "kg", "quantity": qty, "min_quantity": 5,
            "cost_per_unit": 22.00,
        })

    # Milk and consumables
    post("/inventory", {
        "sku": "MILK-OAT", "name": "Oat Milk", "category": "milk",
        "unit": "liters", "quantity": 15, "min_quantity": 8,
        "cost_per_unit": 2.50, "supplier": "Oatly",
    })
    post("/inventory", {
        "sku": "CUP-12OZ", "name": "12oz Takeaway Cup", "category": "consumables",
        "unit": "units", "quantity": 200, "min_quantity": 100,
        "cost_per_unit": 0.12,
    })

    # Log some usage
    print("\n--- Logging usage ---")
    for _ in range(5):
        post("/usage", {"sku": "MILK-OAT", "quantity_used": 2.5, "usage_type": "bar_service", "staff": "George"})
        post("/usage", {"sku": "CUP-12OZ", "quantity_used": 30, "usage_type": "bar_service"})
        post("/usage", {"sku": "RC-ETH-MED", "quantity_used": 0.5, "usage_type": "bar_service"})

    # Log some waste
    print("--- Logging waste ---")
    post("/waste", {"sku": "MILK-OAT", "quantity_wasted": 3, "reason": "expired"})
    post("/waste", {"sku": "RC-ETH-MED", "quantity_wasted": 0.5, "reason": "spoiled"})

    # Schedule and complete a roast
    print("--- Running production ---")
    code, batch = post("/roast/schedule", {
        "green_coffee_sku": "GC-ETH-YRG", "green_weight_kg": 15,
        "roast_level": "medium", "roaster": "George",
    })
    if code == 200 and "batch_code" in batch:
        bc = batch["batch_code"]
        post(f"/roast/{bc}/start", {"roast_temp_c": 205})
        post(f"/roast/{bc}/complete", {
            "roasted_weight_kg": 12.3, "roasted_sku": "RC-ETH-MED",
            "roast_duration_min": 13.5,
            "retail_allocation_kg": 5, "wholesale_allocation_kg": 7.3,
        })

    # Add wholesale customer and order
    print("--- Setting up wholesale ---")
    code, cust = post("/wholesale/customers", {
        "name": "Parthenon Hotel", "contact_person": "Nikos Georgiou",
        "email": "nikos@parthenon.gr", "customer_type": "hotel",
        "address": "Syntagma Square, Athens",
    })
    cust_id = cust.get("id")

    if cust_id:
        code, order = post("/wholesale/orders", {
            "customer_id": cust_id,
            "lines": [
                {"product_sku": "RC-ETH-MED", "product_name": "Ethiopia Medium", "quantity_kg": 10, "price_per_kg": 22},
                {"product_sku": "RC-COL-MED", "product_name": "Colombia Medium", "quantity_kg": 5, "price_per_kg": 22},
            ],
            "requested_delivery": "2026-03-25T10:00:00",
        })
        if code == 200:
            put(f"/wholesale/orders/{order['order_number']}/status", {"new_status": "confirmed"})

    # ══════════════════════════════════════════════════════════
    # TEST INTELLIGENCE ENDPOINTS
    # ══════════════════════════════════════════════════════════

    print("\n--- Inventory Intelligence ---")
    code, data = get("/intelligence/inventory")
    check("inventory health 200", code == 200)
    check("has overview", "overview" in data, list(data.keys()))
    check("has predictions", "stockout_predictions" in data)
    check("has reorder", "reorder_recommendations" in data)
    check("has waste", "waste_analysis" in data)
    check("has actions", "action_summary" in data)
    actions = data.get("action_summary", [])
    print(f"  Actions: {len(actions)}")
    for a in actions[:3]:
        print(f"    {a[:80]}")

    code, data = get("/intelligence/inventory/stockout")
    check("stockout forecast 200", code == 200)
    preds = data.get("predictions", [])
    check("has predictions", len(preds) > 0, f"got {len(preds)}")
    if preds:
        top = preds[0]
        check("has urgency", "urgency" in top)
        check("has daily_rate", "daily_rate" in top)
        print(f"  Top risk: {top['name']} - {top['urgency']} ({top['days_remaining']:.0f} days)")

    code, data = get("/intelligence/inventory/reorder")
    check("reorder plan 200", code == 200)
    recs = data.get("recommendations", [])
    print(f"  Reorder items: {len(recs)}, est cost: EUR {data.get('total_estimated_cost_eur', 0)}")

    code, data = get("/intelligence/inventory/waste")
    check("waste analysis 200", code == 200)
    check("has anomaly_count", "anomaly_count" in data)
    print(f"  Anomalies: {data.get('anomaly_count', 0)}, total waste: EUR {data.get('summary',{}).get('total_waste_cost_eur',0)}")

    print("\n--- Production Intelligence ---")
    code, data = get("/intelligence/production")
    check("production health 200", code == 200)
    check("has loss_analysis", "loss_analysis" in data)
    check("has capacity", "capacity" in data)
    check("has batch_plan", "batch_plan" in data)
    loss = data.get("loss_analysis", {})
    if loss.get("batches_analyzed", 0) > 0:
        print(f"  Avg loss: {loss['avg_loss_pct']}% (expected {loss['expected_loss_pct']}%)")
    cap = data.get("capacity", {})
    if "error" not in cap:
        print(f"  Capacity utilization: {cap.get('capacity_utilization_pct', '?')}%")

    code, data = get("/intelligence/production/plan")
    check("batch plan 200", code == 200)
    recs = data.get("recommendations", [])
    print(f"  Batches recommended: {len(recs)}")
    for r in recs[:3]:
        print(f"    [{r['priority']}] {r['recommended_batch_kg']}kg -> {r['roasted_name']} (need {r['total_need_kg']}kg)")

    print("\n--- Wholesale Intelligence ---")
    code, data = get("/intelligence/wholesale")
    check("wholesale health 200", code == 200)
    check("has demand_forecast", "demand_forecast" in data)
    check("has production_gap", "production_gap" in data)
    check("has delivery_risk", "delivery_risk" in data)
    check("has customer_rankings", "customer_rankings" in data)
    check("has fulfillment_metrics", "fulfillment_metrics" in data)

    gap = data.get("production_gap", {})
    if "error" not in gap:
        print(f"  Demand: {gap.get('total_pending_demand_kg',0)}kg | Available: {gap.get('total_available_kg',0)}kg | Coverage: {gap.get('coverage_pct',0)}%")

    code, data = get("/intelligence/wholesale/risk")
    check("delivery risk 200", code == 200)
    orders = data.get("orders", [])
    print(f"  At-risk orders: {len(orders)}")
    for o in orders[:3]:
        print(f"    {o['risk_level']}: {o['order_number']} ({o['customer']}) score={o['risk_score']}")

    code, data = get("/intelligence/wholesale/gap")
    check("production gap 200", code == 200)
    needs = data.get("needs_production", [])
    print(f"  Products needing production: {len(needs)}")

    # ══════════════════════════════════════════════════════════
    # RESULTS
    # ══════════════════════════════════════════════════════════
    total = _p + _f
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {_p} passed, {_f} failed, {total} total")
    print(f"{'=' * 60}")
    sys.exit(1 if _f else 0)


if __name__ == "__main__":
    main()
