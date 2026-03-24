"""
End-to-end test for the Operations systems:
  1. Inventory - add items, receive stock, log usage, log waste, check alerts
  2. Production - schedule roast, start, complete, verify loss calc
  3. Wholesale - add customer, create order, update status, check demand

Run against live API:
  python scripts/test_operations.py
"""
from __future__ import annotations
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

API = "http://127.0.0.1:8000/api/ops"
_passed = 0
_failed = 0


def check(name, condition, detail=""):
    global _passed, _failed
    if condition:
        _passed += 1
        print(f"  [PASS] {name}")
    else:
        _failed += 1
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
    print("MAILLARD OPERATIONS -- END-TO-END TEST")
    print("=" * 60)

    # ══════════════════════════════════════════════════════════
    # 1. INVENTORY
    # ══════════════════════════════════════════════════════════
    print("\n--- INVENTORY ---")

    # Add green coffee
    code, data = post("/inventory", {
        "sku": "GC-BRAZIL-001",
        "name": "Brazil Santos Natural",
        "category": "green_coffee",
        "unit": "kg",
        "quantity": 50,
        "min_quantity": 10,
        "cost_per_unit": 6.50,
        "supplier": "Ally Coffee",
    })
    check("add green coffee", code == 200 and data.get("sku") == "GC-BRAZIL-001", f"{code} {data}")

    # Add roasted coffee (empty, will be filled by production)
    code, data = post("/inventory", {
        "sku": "RC-BRAZIL-MED",
        "name": "Brazil Santos Medium Roast",
        "category": "roasted_coffee",
        "unit": "kg",
        "quantity": 0,
        "min_quantity": 5,
        "cost_per_unit": 18.00,
    })
    check("add roasted coffee", code == 200, f"{code} {data}")

    # Add milk
    code, data = post("/inventory", {
        "sku": "MILK-FULL",
        "name": "Full Cream Milk",
        "category": "milk",
        "unit": "liters",
        "quantity": 30,
        "min_quantity": 10,
        "cost_per_unit": 1.20,
        "supplier": "Delta Dairy",
    })
    check("add milk", code == 200, f"{code} {data}")

    # List inventory
    code, data = get("/inventory")
    check("list inventory", code == 200 and len(data.get("items", [])) >= 3, f"got {len(data.get('items', []))} items")

    # Receive stock
    code, data = post("/inventory/GC-BRAZIL-001/receive", {"quantity": 25})
    check("receive stock", code == 200 and data.get("item", {}).get("quantity") == 75, f"{data}")

    # Log usage (bar service)
    code, data = post("/usage", {
        "sku": "MILK-FULL",
        "quantity_used": 5,
        "usage_type": "bar_service",
        "staff": "George",
    })
    check("log milk usage", code == 200 and data.get("remaining") == 25, f"{data}")

    # Log waste
    code, data = post("/waste", {
        "sku": "MILK-FULL",
        "quantity_wasted": 2,
        "reason": "expired",
        "staff": "George",
    })
    check("log waste", code == 200 and data.get("cost_lost_eur") == 2.40, f"{data}")

    # Check reorder alerts
    code, data = get("/inventory/alerts/reorder")
    # roasted coffee should be at 0, below min of 5
    alert_skus = [a["sku"] for a in data.get("alerts", [])]
    check("reorder alerts", "RC-BRAZIL-MED" in alert_skus, f"alerts: {alert_skus}")

    # Stock value
    code, data = get("/inventory/value/total")
    check("stock value", code == 200 and data.get("total_value_eur", 0) > 0, f"{data}")

    # Waste summary
    code, data = get("/waste/summary?days=30")
    check("waste summary", code == 200 and data.get("total_cost_lost_eur", 0) == 2.40, f"{data}")

    # ══════════════════════════════════════════════════════════
    # 2. PRODUCTION
    # ══════════════════════════════════════════════════════════
    print("\n--- PRODUCTION ---")

    # Schedule roast
    code, data = post("/roast/schedule", {
        "green_coffee_sku": "GC-BRAZIL-001",
        "green_weight_kg": 20,
        "roast_level": "medium",
        "roaster": "George",
    })
    check("schedule roast", code == 200 and "batch_code" in data, f"{data}")
    batch_code = data.get("batch_code", "")

    # List scheduled batches
    code, data = get("/roast/batches?status=scheduled")
    check("list scheduled", code == 200 and len(data.get("batches", [])) >= 1, f"{data}")

    # Start roast (deducts green coffee)
    code, data = post(f"/roast/{batch_code}/start", {"roast_temp_c": 210})
    check("start roast", code == 200, f"{data}")

    # Check green coffee was deducted (75 - 20 = 55)
    code, data = get("/inventory/GC-BRAZIL-001")
    check("green deducted", data.get("quantity") == 55, f"qty={data.get('quantity')}")

    # Complete roast (20kg green -> 16.4kg roasted = 18% loss)
    code, data = post(f"/roast/{batch_code}/complete", {
        "roasted_weight_kg": 16.4,
        "roasted_sku": "RC-BRAZIL-MED",
        "roast_duration_min": 14,
        "retail_allocation_kg": 6,
        "wholesale_allocation_kg": 10.4,
    })
    check("complete roast", code == 200, f"{data}")
    batch_data = data.get("batch", {})
    check("roast loss calc", batch_data.get("roast_loss_percent") == 18.0,
          f"loss={batch_data.get('roast_loss_percent')}")

    # Check roasted coffee was added to inventory
    code, data = get("/inventory/RC-BRAZIL-MED")
    check("roasted in inventory", data.get("quantity") == 16.4, f"qty={data.get('quantity')}")

    # Production summary
    code, data = get("/production/summary?days=30")
    check("production summary", code == 200 and data.get("completed", 0) >= 1, f"{data}")

    # ══════════════════════════════════════════════════════════
    # 3. WHOLESALE
    # ══════════════════════════════════════════════════════════
    print("\n--- WHOLESALE ---")

    # Add customer
    code, data = post("/wholesale/customers", {
        "name": "Athena Cafe",
        "contact_person": "Maria Papadopoulos",
        "email": "maria@athenacafe.gr",
        "customer_type": "cafe",
        "address": "Ermou 45, Athens",
    })
    check("add customer", code == 200 and data.get("name") == "Athena Cafe", f"{data}")
    customer_id = data.get("id")

    # List customers
    code, data = get("/wholesale/customers")
    check("list customers", code == 200 and len(data.get("customers", [])) >= 1, f"{data}")

    # Create order
    code, data = post("/wholesale/orders", {
        "customer_id": customer_id,
        "lines": [
            {"product_sku": "RC-BRAZIL-MED", "product_name": "Brazil Medium Roast", "quantity_kg": 5, "price_per_kg": 18.00},
            {"product_sku": "RC-BRAZIL-MED", "product_name": "Brazil Medium Roast", "quantity_kg": 3, "price_per_kg": 18.00},
        ],
        "requested_delivery": "2026-03-25T10:00:00",
    })
    check("create order", code == 200 and "order_number" in data, f"{data}")
    order_number = data.get("order_number", "")
    check("order total", data.get("total_kg") == 8 and data.get("total_eur") == 144.0,
          f"kg={data.get('total_kg')} eur={data.get('total_eur')}")

    # Update order status
    code, data = put(f"/wholesale/orders/{order_number}/status", {
        "new_status": "confirmed",
    })
    check("confirm order", code == 200 and data.get("status") == "confirmed", f"{data}")

    # Pending demand
    code, data = get("/wholesale/demand")
    check("pending demand", code == 200 and data.get("total_pending_kg", 0) >= 8, f"{data}")

    # Delivery schedule
    code, data = get("/wholesale/deliveries?days_ahead=30")
    check("delivery schedule", code == 200 and len(data.get("schedule", [])) >= 1, f"{data}")

    # ══════════════════════════════════════════════════════════
    # RESULTS
    # ══════════════════════════════════════════════════════════
    total = _passed + _failed
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {_passed} passed, {_failed} failed, {total} total")
    print(f"{'=' * 60}")
    sys.exit(1 if _failed else 0)


if __name__ == "__main__":
    main()
