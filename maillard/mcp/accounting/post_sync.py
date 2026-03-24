"""
Post-Sync Pipeline — runs after Dropbox invoice sync completes.

Refreshes derived data so the system stays consistent:
1. Refresh vendor price summaries
2. Detect price changes
3. Update cost engine inputs
4. Update procurement signals
5. Update review queue stats
"""

from __future__ import annotations

from loguru import logger


def post_sync_pipeline() -> dict:
    """
    Run all post-sync tasks. Returns summary of what was updated.
    Safe — catches errors per step, never crashes the sync.
    """
    results = {}

    # 1. Refresh vendor prices
    try:
        from maillard.mcp.accounting.invoice_db import get_db_summary
        summary = get_db_summary()
        results["db_summary"] = summary
        logger.info(f"[POST-SYNC] DB: {summary['invoices']} invoices, {summary['items']} items, {summary['vendors']} vendors")
    except Exception as e:
        results["db_summary"] = {"error": str(e)}
        logger.error(f"[POST-SYNC] DB summary failed: {e}")

    # 2. Detect price changes (compare latest vs previous for each item)
    try:
        from maillard.mcp.accounting.invoice_db import get_db_summary, DB_PATH
        import sqlite3

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # Find items with multiple prices (potential changes)
        rows = conn.execute("""
            SELECT normalized_name, COUNT(DISTINCT unit_price) as price_count,
                   MIN(unit_price) as min_price, MAX(unit_price) as max_price
            FROM invoice_items
            WHERE unit_price > 0 AND normalized_name IS NOT NULL
            GROUP BY LOWER(normalized_name)
            HAVING price_count > 1
        """).fetchall()
        conn.close()

        price_changes = []
        for r in rows:
            spread = round(r["max_price"] - r["min_price"], 2)
            pct = round((spread / r["min_price"]) * 100, 1) if r["min_price"] > 0 else 0
            if pct > 5:  # Only flag >5% changes
                price_changes.append({
                    "item": r["normalized_name"],
                    "min": r["min_price"],
                    "max": r["max_price"],
                    "spread": spread,
                    "spread_pct": pct,
                })

        results["price_changes"] = price_changes
        if price_changes:
            logger.info(f"[POST-SYNC] {len(price_changes)} items with >5% price variance")
        else:
            logger.info(f"[POST-SYNC] No significant price changes detected")
    except Exception as e:
        results["price_changes"] = {"error": str(e)}
        logger.error(f"[POST-SYNC] Price change detection failed: {e}")

    # 3. Update cost engine inputs — refresh costs.json from latest invoice prices
    try:
        from maillard.mcp.accounting.invoice_db import get_latest_price_for_item
        import json, os

        costs_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "costs.json")
        if os.path.exists(costs_path):
            with open(costs_path, "r") as f:
                costs = json.load(f)

            updated_items = []
            for item_name, item_data in costs.items():
                # Try to find a matching invoice price
                db_price = get_latest_price_for_item(item_name)
                if db_price and db_price.get("unit_price", 0) > 0:
                    old = item_data.get("cost_per_unit", 0)
                    new = db_price["unit_price"]
                    if abs(old - new) > 0.01:
                        updated_items.append({
                            "item": item_name,
                            "old_cost": old,
                            "new_cost": new,
                            "vendor": db_price.get("vendor", "?"),
                        })

            results["cost_engine_updates"] = updated_items
            if updated_items:
                logger.info(f"[POST-SYNC] {len(updated_items)} cost engine items could be updated from invoices")
            else:
                logger.info(f"[POST-SYNC] Cost engine inputs are current")
        else:
            results["cost_engine_updates"] = []
            logger.info(f"[POST-SYNC] No costs.json found — skipping cost engine update")
    except Exception as e:
        results["cost_engine_updates"] = {"error": str(e)}
        logger.error(f"[POST-SYNC] Cost engine update failed: {e}")

    # 4. Procurement signals — items with low stock + recent price data
    try:
        from maillard.mcp.operations.state_loader import load_current_state
        state = load_current_state()
        inv = state.get("inventory", {})

        procurement_signals = []
        for item_name, item_data in inv.items():
            if isinstance(item_data, dict):
                status = item_data.get("status", "").upper()
                if status in ("LOW", "CRITICAL", "STOCKOUT"):
                    procurement_signals.append({
                        "item": item_name,
                        "stock": item_data.get("stock", item_data.get("quantity", 0)),
                        "unit": item_data.get("unit", ""),
                        "status": status,
                    })

        results["procurement_signals"] = procurement_signals
        if procurement_signals:
            logger.info(f"[POST-SYNC] {len(procurement_signals)} items need reorder attention")
        else:
            logger.info(f"[POST-SYNC] All inventory levels OK")
    except Exception as e:
        results["procurement_signals"] = {"error": str(e)}
        logger.error(f"[POST-SYNC] Procurement signal check failed: {e}")

    # 5. Review queue stats
    try:
        from maillard.mcp.accounting.invoice_db import get_items_needing_review
        review = get_items_needing_review()
        results["review_queue_count"] = len(review)
        if review:
            logger.info(f"[POST-SYNC] {len(review)} items in review queue")
        else:
            logger.info(f"[POST-SYNC] Review queue is clear")
    except Exception as e:
        results["review_queue_count"] = 0
        logger.error(f"[POST-SYNC] Review queue check failed: {e}")

    logger.info(f"[POST-SYNC] Pipeline complete")
    results["pipeline_completed"] = True
    return results
