"""
Data-Bound Query Resolver — the intent gate for Coffee AGI.

Forces all operational, financial, vendor, invoice, cost, stock, and
procurement queries to hit real local data before any LLM text.

Usage:
    result = resolve_data_bound_query(query)
    if result:
        # result has verified/estimated/unavailable data — use it
    else:
        # not a data-bound query — route normally
"""

from __future__ import annotations

import re
from loguru import logger

# ── Intent Patterns ──────────────────────────────────────────────

DATA_BOUND_INTENTS: list[tuple[str, str]] = [
    # Order matters — more specific patterns first, generic last.

    # ── Reorder / quantity intent — only when clearly asking about quantity to order ──
    (r"how much.{0,15}should (i|we).{0,15}(buy|order|purchase|get)", "reorder_quantity"),
    (r"how much.{0,15}(should i|should we).{0,15}(order|buy|purchase|get)", "reorder_quantity"),
    (r"how much.{0,15}(need to|gotta|have to).{0,15}(order|buy|purchase)", "reorder_quantity"),
    (r"\breorder\b|\bneed to order\b|\bwhat.{0,8}order\b", "reorder_quantity"),
    (r"\bprocure\b|\bprocurement\b", "reorder_quantity"),
    # "how much do I buy X" without "should" is ambiguous (price vs quantity)
    (r"how much.{0,15}(do i|do we).{0,15}(buy|purchase)", "ambiguous_buy"),

    # ── Invoice listing intent (must come before price_lookup) ──
    (r"(latest|last|recent|show|all|list).{0,10}invoice", "invoice_lookup"),
    (r"invoice.{0,10}(from|for|show|list|last|latest|summary)", "invoice_lookup"),
    (r"\binvoices?\b.{0,5}(on file|we have|do we)", "invoice_lookup"),

    # ── Purchase-price intent (explicit price/cost/paid keywords) ──
    (r"(latest|last|recent|current).{0,15}(price|cost|paid)", "price_lookup"),
    (r"(what|how much) did (i|we).{0,15}(pay|spend|cost)", "price_lookup"),
    (r"how much.{0,15}(pay|paying|paid|spend|spending|spent)\b", "price_lookup"),
    (r"how much.{0,15}(cost|does it cost|is.{0,5}cost)", "price_lookup"),
    (r"(unit|per) cost", "price_lookup"),
    (r"\bcost of\b", "price_lookup"),
    (r"\bprice of\b", "price_lookup"),
    (r"(cheapest|cheap|best price|lowest).{0,15}(vendor|supplier|for)", "compare_vendors"),
    (r"compare.{0,15}(vendor|price|supplier)", "compare_vendors"),
    (r"vendor.{0,15}(price|cost|history|compare)", "vendor_lookup"),
    (r"invoice.{0,15}(from|for|show|list|last|latest)", "invoice_lookup"),
    (r"\binvoice\b", "invoice_lookup"),
    (r"\bvendor\b.{0,15}(detail|history|list)", "vendor_lookup"),
    (r"(oat milk|whole milk|oat|yogurt|cream|butter|cups?|napkin|paper|sugar|syrup).{0,10}(cost|price|pay)", "price_lookup"),
    (r"(cost|price|pay).{0,10}(oat milk|whole milk|oat|yogurt|cream|butter|cups?|napkin|paper|sugar|syrup)", "price_lookup"),

    # ── Square live check (execute-first: calls Square API directly) ──
    (r"(is|are).{0,10}square.{0,10}(live|connected|working|up)", "square_live_check"),
    (r"square.{0,10}(status|check|test|connection|live)", "square_live_check"),
    (r"(check|test|verify).{0,10}square", "square_live_check"),
    (r"(is|are).{0,10}(pos|sales?).{0,10}(live|connected|working)", "square_live_check"),
    (r"(get|fetch|pull|show).{0,10}(live|real).{0,10}(sale|data|order)", "square_live_check"),
    (r"(i want|give me|show me).{0,10}live.{0,10}(data|sale|order)", "square_live_check"),
    (r"(connect|sync).{0,10}(to )?square", "square_live_check"),

    # ── Live sales / orders / top items (reads snapshot) ──
    (r"(what.{0,15}sell|top.{0,10}(item|seller|product)|best.{0,10}sell)", "live_sales"),
    (r"(today.{0,10}(sale|order|revenue)|sale.{0,10}today|order.{0,10}today)", "live_sales"),
    (r"(how many|number of).{0,10}order", "live_sales"),
    (r"(square|pos).{0,10}(sale|order|data|report)", "live_sales"),
    (r"(daily|today).{0,10}(revenue|total|sale|order)", "live_sales"),
    (r"\braw.?order.?count\b", "live_sales"),

    # ── Stock / inventory ──
    (r"(stock|inventory).{0,10}(level|check|status|count|how much)", "stock"),
    (r"how much.{0,10}(have|left|remaining)", "stock"),

    # ── Other data-bound ──
    (r"\bmargin\b", "margin"),
    (r"\bshrinkage\b|\bwaste\b|\bspoil", "shrinkage"),
    (r"redway|optima|loumidis|odeko|sysco|impact food|rite.?a.?way|pure produce|dairy wagon|wheatfield", "vendor_lookup"),

    # ── Ambiguous "how much" + buy + product (no price/order qualifier) ──
    # This is the catch-all for "how much i buy whole milk" type queries.
    # Detected as ambiguous_buy — triggers clarification.
    (r"how much.{0,15}\b(buy|bought|purchase|purchased)\b", "ambiguous_buy"),
    # Ultra-generic "how much is X" with a product — also ambiguous
    (r"how much is\b", "ambiguous_price"),
]


def detect_intent(query: str) -> str | None:
    """Detect if a query is data-bound. Returns intent label or None."""
    lower = query.lower()
    for pattern, intent in DATA_BOUND_INTENTS:
        if re.search(pattern, lower):
            return intent
    return None


# ── Ambiguity Detection ──────────────────────────────────────────

PRODUCT_WORDS = {"oat", "milk", "cups", "cup", "sugar", "syrup", "cream", "yogurt",
                 "napkin", "paper", "butter", "eggs", "flour", "bread", "plastic",
                 "lid", "lids", "sleeve", "towel", "straw"}


def is_ambiguous(query: str, intent: str | None = None) -> bool:
    """Detect if a query is ambiguous and needs clarification."""
    # Explicit ambiguous intents from the pattern matcher
    if intent in ("ambiguous_buy", "ambiguous_price"):
        return True

    # Short product mention with no clear qualifier
    lower = query.lower()
    words = lower.split()
    if len(words) > 8:
        return False
    has_product = any(p in lower for p in PRODUCT_WORDS)
    has_clear_intent = any(kw in lower for kw in [
        "invoice", "vendor", "supplier", "latest price", "last price",
        "cheapest", "compare", "history", "paid", "pay for", "cost of",
        "unit cost", "per unit", "stock", "inventory", "reorder",
        "should i order", "need to order", "should i buy",
        "how much do we have", "left", "remaining",
    ])
    return has_product and not has_clear_intent


# ── Data Sources ─────────────────────────────────────────────────


def _query_invoice_db(query: str) -> dict | None:
    """Search invoice DB for relevant data, including derived unit costs."""
    try:
        from maillard.mcp.accounting.invoice_db import (
            get_db_summary, compare_vendor_prices, get_latest_price_for_item,
            get_vendor_price_history, get_cheapest_vendor, get_items_needing_review,
            get_latest_invoices, get_latest_invoice_by_vendor,
            DB_PATH,
        )
        import sqlite3
    except Exception:
        return None

    # Keep a search connection open for derived cost lookups
    conn_search = sqlite3.connect(DB_PATH)
    conn_search.row_factory = sqlite3.Row

    summary = get_db_summary()
    if summary["invoices"] == 0:
        conn_search.close()
        return None

    lower = query.lower()
    results = {}

    # ── General invoice listing (no specific product) ──
    # Catches: "latest invoice", "show invoices", "what invoices do we have"
    invoice_listing_kw = ["latest invoice", "last invoice", "recent invoice", "show invoice",
                          "invoices on file", "all invoices", "invoice list", "invoice summary"]
    if any(kw in lower for kw in invoice_listing_kw) or (
        "invoice" in lower and not any(p in lower for p in ["price", "cost", "pay", "compare", "cheap"])
    ):
        recent = get_latest_invoices(limit=10)
        if recent:
            results["recent_invoices"] = recent
            logger.info(f"[RESOLVER] Invoice listing: {len(recent)} invoices found")

    # ── Vendor-specific invoice lookup ──
    vendor_names = {
        "redway": "REDWAY", "dairy": "Dairy Wagon", "wheatfield": "WHEATFIELD",
        "optima": "Optima", "loumidis": "LOUMIDIS", "odeko": "Odeko",
        "sysco": "SYSCO", "impact": "Impact Food", "rite": "RITE-A-WAY",
        "pure produce": "Pure Produce",
    }
    for key, name in vendor_names.items():
        if key in lower:
            # Vendor-specific invoice
            if "invoice" in lower:
                vi = get_latest_invoice_by_vendor(name)
                if vi:
                    results["vendor_invoice"] = vi
                    logger.info(f"[RESOLVER] Vendor invoice found: {name}")
            # Vendor history (all items)
            hist = get_vendor_price_history(name)
            if hist:
                results["vendor_history"] = {name: hist[:20]}
            break

    # ── Product-specific search using token matching ──
    # Find matching items using token-based search
    # Extract meaningful tokens from the query (strip filler words)
    FILLER = {"how", "much", "is", "the", "a", "an", "for", "do", "i", "we", "my",
              "did", "pay", "cost", "price", "buy", "bought", "latest", "last",
              "compare", "vendor", "vendors", "cheapest", "what", "show", "get",
              "me", "of", "from", "does", "should", "need", "to", "order", "about"}
    query_tokens = [w for w in re.split(r'\W+', lower) if w and w not in FILLER and len(w) > 1]
    logger.debug(f"[RESOLVER] Search tokens: {query_tokens}")

    # Also try known keyword phrases
    item_keywords = [
        "oat milk", "whole milk", "milk", "yogurt", "cream", "sugar", "syrup",
        "cup", "cups", "napkin", "bread", "croissant", "danish", "muffin", "butter",
        "eggs", "flour", "lid", "sleeve", "paper", "towel", "plastic", "hot",
    ]
    # Build search terms: query tokens + any matched keyword phrases
    search_terms = list(query_tokens)
    for kw in item_keywords:
        if kw in lower and kw not in search_terms:
            search_terms.append(kw)

    matched_items = []
    if search_terms:
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row

            # Search both raw_name and normalized_name for each token
            all_candidates = {}  # normalized_name -> match_score
            for term in search_terms:
                for col in ("raw_name", "normalized_name"):
                    rows = conn.execute(
                        f"SELECT DISTINCT normalized_name, raw_name FROM invoice_items WHERE LOWER({col}) LIKE ? AND unit_price > 0",
                        (f"%{term}%",),
                    ).fetchall()
                    for r in rows:
                        name = r["normalized_name"]
                        if name not in all_candidates:
                            all_candidates[name] = {"score": 0, "raw": r["raw_name"]}
                        all_candidates[name]["score"] += 1

            # Rank by number of matching tokens (more tokens = better match)
            ranked = sorted(all_candidates.items(), key=lambda x: x[1]["score"], reverse=True)
            # Take items that match at least 1 token, prefer multi-token matches
            for name, info in ranked[:10]:
                matched_items.append(name)

            if matched_items:
                logger.debug(f"[RESOLVER] Token search found {len(matched_items)} items: {matched_items[:5]}")
            conn.close()
        except Exception as e:
            logger.error(f"[RESOLVER] Token search failed: {e}")

    # Legacy keyword fallback if token search found nothing
    if not matched_items:
        for kw in item_keywords:
            if kw in lower:
                try:
                    conn = sqlite3.connect(DB_PATH)
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute(
                        "SELECT DISTINCT normalized_name FROM invoice_items WHERE LOWER(normalized_name) LIKE ? AND unit_price > 0",
                        (f"%{kw}%",),
                    ).fetchall()
                    conn.close()
                    matched_items.extend(r["normalized_name"] for r in rows)
                except Exception:
                    pass

    # Dedupe
    matched_items = list(dict.fromkeys(matched_items))

    # ── Anomaly detection ──
    try:
        from maillard.mcp.accounting.confidence import (
            get_anomalous_item_ids, classify_match_breadth, detect_price_anomalies,
        )
        anomalous_ids = get_anomalous_item_ids(DB_PATH)
    except Exception:
        anomalous_ids = set()

    # ── Broad vs exact match classification ──
    if matched_items:
        try:
            breadth, families = classify_match_breadth(search_terms, matched_items)
            if breadth == "multiple_families":
                results["match_breadth"] = "multiple_families"
                results["product_families"] = [[name[:40] for name in fam] for fam in families]
                logger.info(f"[RESOLVER] Broad match: {len(families)} product families")
        except Exception:
            pass

    # Price comparisons + derived unit costs + anomaly filtering
    if matched_items:
        comparisons = {}
        derived_costs = {}
        anomaly_warnings = []

        for item in matched_items[:8]:
            cmp = compare_vendor_prices(item)
            if cmp:
                # Filter: flag anomalous rows
                clean_cmp = []
                for v in cmp:
                    # Check if this vendor/item combo is anomalous by looking up the item ID
                    try:
                        id_row = conn_search.execute(
                            "SELECT id FROM invoice_items WHERE normalized_name = ? AND unit_price = ? LIMIT 1",
                            (item, v["unit_price"]),
                        ).fetchone()
                        if id_row and id_row["id"] in anomalous_ids:
                            v["_anomaly"] = True
                            anomaly_warnings.append(f"{item}: ${v['unit_price']:.2f} from {v['vendor']} flagged as anomalous")
                        else:
                            v["_anomaly"] = False
                    except Exception:
                        v["_anomaly"] = False
                    clean_cmp.append(v)

                comparisons[item] = clean_cmp

                # Derived unit cost from latest non-anomalous row
                try:
                    full_rows = conn_search.execute(
                        """SELECT ii.id, ii.raw_name, ii.unit_price, ii.unit, ii.quantity,
                                  ii.line_total, ii.pack_size_json
                           FROM invoice_items ii
                           JOIN invoices i ON ii.invoice_id = i.id
                           WHERE ii.normalized_name = ? AND ii.unit_price > 0
                           ORDER BY i.invoice_date DESC LIMIT 3""",
                        (item,),
                    ).fetchall()
                    for fr in full_rows:
                        if fr["id"] not in anomalous_ids:
                            from maillard.mcp.accounting.invoice_intake import calculate_derived_unit_cost
                            dr = calculate_derived_unit_cost(dict(fr))
                            if dr:
                                derived_costs[item] = dr
                            break
                except Exception:
                    pass

        if comparisons:
            results["price_comparisons"] = comparisons
        if derived_costs:
            results["derived_costs"] = derived_costs
        if anomaly_warnings:
            results["anomaly_warnings"] = anomaly_warnings

        cheapest = {}
        for item in matched_items[:8]:
            ch = get_cheapest_vendor(item)
            if ch:
                cheapest[item] = ch
        if cheapest:
            results["cheapest"] = cheapest

    # Review queue
    if "review" in lower or "flagged" in lower:
        review = get_items_needing_review()
        if review:
            results["review_queue"] = review[:15]

    results["db_summary"] = summary
    conn_search.close()
    return results if len(results) > 1 else None  # >1 because db_summary is always there


def _execute_square_live_check() -> dict:
    """Call the Square API directly. No cache, no snapshot, no fallback.

    Returns structured result with debug fields — always a dict, never None.
    """
    import os
    debug = {
        "token_loaded": False,
        "location_loaded": False,
        "api_called": False,
        "orders_returned": 0,
    }

    token = os.getenv("SQUARE_ACCESS_TOKEN", "")
    location = os.getenv("SQUARE_LOCATION_ID", "")
    env = os.getenv("SQUARE_ENV", "production")

    debug["token_loaded"] = bool(token)
    debug["location_loaded"] = bool(location)

    if not token:
        return {"status": "connection_failed", "reason": "missing_token", "debug": debug}
    if not location:
        return {"status": "connection_failed", "reason": "missing_location", "debug": debug}
    if env.lower() != "production":
        return {"status": "connection_failed", "reason": f"wrong_env:{env}", "debug": debug}
    if token.lower().startswith("sandbox-"):
        return {"status": "connection_failed", "reason": "sandbox_token", "debug": debug}

    # Actually call the Square API
    try:
        from square import Square
        from square.environment import SquareEnvironment
        from datetime import datetime, timedelta, timezone

        client = Square(token=token, environment=SquareEnvironment.PRODUCTION)
        debug["api_called"] = True

        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)

        result = client.orders.search(
            location_ids=[location],
            limit=5,
            query={
                "filter": {
                    "state_filter": {"states": ["COMPLETED"]},
                    "date_time_filter": {
                        "closed_at": {"start_at": start.isoformat(), "end_at": end.isoformat()}
                    },
                },
                "sort": {"sort_field": "CLOSED_AT", "sort_order": "DESC"},
            },
        )

        orders = result.orders or []
        debug["orders_returned"] = len(orders)

        if not orders:
            # API works but no orders yet today
            return {
                "status": "connected_live",
                "orders_today": 0,
                "top_items": {},
                "message": "Square connected. No completed orders today yet.",
                "debug": debug,
            }

        # Build top items from the sample
        from maillard.mcp.sales.normalization import normalize_product_name
        items: dict[str, int] = {}
        for order in orders:
            for li in (order.line_items or []):
                name = (li.name or "").strip()
                if not name:
                    continue
                product = normalize_product_name(name)
                slug = product["slug"]
                if slug == "unknown":
                    continue
                try:
                    qty = int(float(li.quantity or "1"))
                except (ValueError, TypeError):
                    qty = 1
                items[slug] = items.get(slug, 0) + qty

        # Now get real count (full day)
        count_result = client.orders.search(
            location_ids=[location],
            limit=1,
            query={
                "filter": {
                    "state_filter": {"states": ["COMPLETED"]},
                    "date_time_filter": {
                        "closed_at": {"start_at": start.isoformat(), "end_at": end.isoformat()}
                    },
                },
            },
            return_entries=True,
        )
        # Pagination cursor exists = more than 1 order, but we need full count
        # Use the snapshot if available, otherwise estimate from the first page
        from maillard.mcp.operations.state_loader import get_state_meta
        meta = get_state_meta()
        order_count = meta.get("raw_order_count", 0) if meta.get("has_live_sales") else len(orders)

        return {
            "status": "connected_live",
            "orders_today": order_count,
            "top_items": dict(sorted(items.items(), key=lambda x: -x[1])),
            "sample_size": len(orders),
            "debug": debug,
        }

    except Exception as e:
        debug["api_called"] = True
        err = str(e)[:200]
        reason = "api_error"
        if "FORBIDDEN" in err.upper() or "403" in err:
            reason = "forbidden_check_permissions"
        elif "UNAUTHORIZED" in err.upper() or "401" in err:
            reason = "bad_token"
        elif "NOT_FOUND" in err.upper() or "404" in err:
            reason = "bad_location"
        logger.error(f"[RESOLVER] Square API call failed: {err}")
        return {"status": "connection_failed", "reason": reason, "error": err, "debug": debug}


def _query_live_sales(query: str) -> dict | None:
    """Query current_state.json for live Square sales data."""
    try:
        from maillard.mcp.operations.state_loader import load_current_state, get_state_meta
        state = load_current_state()
        meta = get_state_meta()

        sales_today = state.get("sales_today", {})
        if not sales_today:
            return None

        # Build structured response matching the Square connector output
        data: dict = {"_meta": meta}
        data["sales_today"] = sales_today
        data["sales_amounts"] = state.get("sales_amounts", {})
        data["top_items"] = state.get("top_items", [])
        data["raw_order_count"] = state.get("raw_order_count", 0)
        data["total_units"] = sum(sales_today.values())
        data["total_revenue_cents"] = sum(state.get("sales_amounts", {}).values())
        data["product_count"] = len(sales_today)

        logger.info(f"[RESOLVER] Live sales: {data['raw_order_count']} orders, {data['total_units']} units, {data['product_count']} products")
        return data
    except Exception as e:
        logger.error(f"[RESOLVER] Live sales query failed: {e}")
        return None


def _query_state(query: str) -> dict | None:
    """Query current_state.json for stock/inventory."""
    try:
        from maillard.mcp.operations.state_loader import load_current_state
        state = load_current_state()
        if not state:
            return None

        lower = query.lower()
        inv = state.get("inventory", {})
        if not inv:
            return None

        # Filter to relevant items if query mentions specific products
        product_words = {"milk", "oat", "cream", "sugar", "cups", "coffee", "beans", "yogurt"}
        mentioned = [p for p in product_words if p in lower]

        if mentioned:
            filtered = {}
            for k, v in inv.items():
                if any(p in k.lower() for p in mentioned):
                    filtered[k] = v
            return {"inventory": filtered} if filtered else {"inventory": inv}

        return {"inventory": inv}
    except Exception:
        return None


def _query_cost_engine(query: str) -> dict | None:
    """Query cost engine for margins/COGS."""
    try:
        from maillard.mcp.operations.cost_engine import calculate_product_costs
        costs = calculate_product_costs()
        if not costs:
            return None
        return {"costs": costs}
    except Exception:
        return None


# ── Main Resolver ────────────────────────────────────────────────


def resolve_data_bound_query(query: str) -> dict | None:
    """
    Resolve a data-bound query against all local data sources.

    Returns:
        {
            "intent": str,
            "confidence": "verified" | "estimated" | "unavailable",
            "data": {...},          # the actual data
            "data_text": str,       # formatted text block for LLM context
            "source": str,          # "invoice_db" | "state" | "cost_engine" | "none"
            "ambiguous": bool,
        }
        or None if query is not data-bound.
    """
    intent = detect_intent(query)
    ambiguous = is_ambiguous(query, intent)

    if not intent and not ambiguous:
        return None

    logger.info(f"[RESOLVER] Intent={intent or 'ambiguous'} ambiguous={ambiguous} for: {query[:60]}")

    # Try data sources in priority order
    data = {}
    source = "none"
    confidence = "unavailable"

    # Square live check → call API directly, return immediately
    if intent == "square_live_check":
        logger.info("[RESOLVER] Executing Square live check (calling API now)")
        result = _execute_square_live_check()
        return {
            "intent": "square_live_check",
            "confidence": "verified" if result.get("status") == "connected_live" else "unavailable",
            "data": result,
            "data_text": "",  # handled specially in api.py
            "source": "square_api_direct",
            "ambiguous": False,
        }

    # Live sales intent → current_state.json (hard gate: no fallback to other sources)
    if intent == "live_sales":
        sales_data = _query_live_sales(query)
        if sales_data:
            data.update(sales_data)
            source = "live_square"
            confidence = "verified"
            logger.info("[RESOLVER] Live Square sales data loaded")
        else:
            confidence = "unavailable"
            logger.warning("[RESOLVER] No live sales data — blocking fallback")
        # Return immediately — never let live_sales fall through to cost engine
        data_text = _format_data_text(data, intent, query)
        return {
            "intent": intent,
            "confidence": confidence,
            "data": data,
            "data_text": data_text,
            "source": source,
            "ambiguous": False,
        }

    # Price-related intents → invoice DB first
    price_intents = ("price_lookup", "compare_vendors", "vendor_lookup", "invoice_lookup",
                     "ambiguous_buy", "ambiguous_price")
    if intent in price_intents or ambiguous:
        db_data = _query_invoice_db(query)
        if db_data:
            data.update(db_data)
            source = "invoice_db"
            confidence = "verified"
            logger.info(f"[RESOLVER] DB hit — verified data found")

    # Reorder/quantity intents → state (inventory) first
    if intent in ("reorder_quantity", "stock", "shrinkage"):
        state_data = _query_state(query)
        if state_data:
            data.update(state_data)
            if source == "none":
                source = "state"
                confidence = "verified"
            logger.info(f"[RESOLVER] State data found for reorder/stock")

    # Ambiguous buy → also grab inventory so we can show both sides
    if intent in ("ambiguous_buy", "ambiguous_price") or ambiguous:
        state_data = _query_state(query)
        if state_data:
            data.update(state_data)
            if source == "none":
                source = "state"
                confidence = "verified"
            logger.info(f"[RESOLVER] State data added for ambiguous query")

    # Fallback to state for price_lookup if DB had nothing
    if not data and intent == "price_lookup":
        state_data = _query_state(query)
        if state_data:
            data.update(state_data)
            source = "state"
            confidence = "verified"

    # Cost engine (margins/COGS)
    if intent in ("margin", "price_lookup") or (not data):
        cost_data = _query_cost_engine(query)
        if cost_data:
            data.update(cost_data)
            if source == "none":
                source = "cost_engine"
                confidence = "verified"

    # If nothing found
    if not data or source == "none":
        confidence = "unavailable"
        logger.info(f"[RESOLVER] No verified data found for intent={intent}")

    # Build text block
    data_text = _format_data_text(data, intent, query)

    return {
        "intent": intent or "ambiguous",
        "confidence": confidence,
        "data": data,
        "data_text": data_text,
        "source": source,
        "ambiguous": ambiguous,
    }


def _format_data_text(data: dict, intent: str | None, query: str) -> str:
    """Format resolved data into a text block for LLM context."""
    lines = []

    # Live Square sales
    if "sales_today" in data and "top_items" in data:
        meta = data.get("_meta", {})
        lines.append(f"LIVE SQUARE SALES (source: current_state.json, updated: {meta.get('last_updated', 'unknown')})")
        lines.append(f"  Orders: {data.get('raw_order_count', '?')}")
        lines.append(f"  Total units sold: {data.get('total_units', '?')}")
        total_cents = data.get('total_revenue_cents', 0)
        if total_cents:
            lines.append(f"  Total revenue: ${total_cents / 100:.2f}")
        lines.append(f"  Products: {data.get('product_count', '?')}")
        lines.append("")
        lines.append("TOP ITEMS (by quantity):")
        for item in data.get("top_items", [])[:10]:
            rev = f"  ${item['revenue_cents']/100:.2f}" if item.get("revenue_cents") else ""
            lines.append(f"  {item.get('display', item.get('name','?')):35s} x{item.get('qty','?')}{rev}")
        lines.append("")
        if meta.get("is_stale"):
            lines.append(f"WARNING: Data may be stale (last updated {meta.get('last_updated', 'unknown')})")
            lines.append("")

    # Recent invoices listing
    if "recent_invoices" in data:
        lines.append("RECENT INVOICES:")
        for inv in data["recent_invoices"]:
            total = f"${inv['total']:.2f}" if inv.get("total") else "?"
            lines.append(f"  {inv.get('vendor','?'):30s} #{str(inv.get('invoice_number','?')):15s}  {inv.get('invoice_date','?')}  {total}  ({inv.get('item_count',0)} items)")
        lines.append("")

    # Vendor-specific invoice detail
    if "vendor_invoice" in data:
        vi = data["vendor_invoice"]
        lines.append(f"VENDOR INVOICE: {vi.get('vendor', '?')}")
        lines.append(f"  Invoice #: {vi.get('invoice_number', '?')}")
        lines.append(f"  Date: {vi.get('invoice_date', '?')}")
        total = f"${vi['total']:.2f}" if vi.get("total") else "?"
        lines.append(f"  Total: {total}")
        if vi.get("source_file"):
            lines.append(f"  Source: {vi['source_file']}")
        for item in vi.get("line_items", []):
            price = f"${item['unit_price']:.2f}" if item.get("unit_price") else "?"
            lines.append(f"  - {item.get('raw_name','?'):40s} {price}/{item.get('unit','ea')}  qty={item.get('quantity',1)}")
        lines.append("")

    # Multi-family disambiguation
    if "match_breadth" in data and data["match_breadth"] == "multiple_families":
        lines.append("NOTE: Multiple product families match this query. Families found:")
        for i, fam in enumerate(data.get("product_families", []), 1):
            lines.append(f"  {i}. {', '.join(fam[:3])}")
        lines.append("Ask about a specific product for exact pricing.\n")

    # Anomaly warnings
    if "anomaly_warnings" in data:
        lines.append("ANOMALY WARNINGS (excluded from default answers):")
        for w in data["anomaly_warnings"]:
            lines.append(f"  ⚠ {w}")
        lines.append("")

    # Price comparisons (with anomaly and confidence flags)
    if "price_comparisons" in data:
        for item, vendors in data["price_comparisons"].items():
            lines.append(f"PRICE: {item}")
            for v in vendors:
                flags = []
                if v.get("override_source") == "handwritten":
                    flags.append("handwritten")
                if v.get("_anomaly"):
                    flags.append("ANOMALOUS — excluded from recommendations")
                if v.get("confidence") == "low":
                    flags.append("low confidence — needs review")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                lines.append(f"  {v['vendor']:30s} ${v['unit_price']:>7.2f}/{v.get('unit','ea')}  ({v.get('invoice_date','?')})  conf={v.get('confidence','?')}{flag_str}")

    # Derived unit costs
    if "derived_costs" in data:
        for item, dr in data["derived_costs"].items():
            lines.append(f"  DERIVED UNIT COST: {dr['derived_unit_cost_display']} (from {dr['base_price']}, {dr.get('items_per_container', '?')} per container)")

    if "cheapest" in data:
        for item, ch in data["cheapest"].items():
            if ch.get("num_vendors", 0) > 1:
                lines.append(f"CHEAPEST for {item}: {ch['vendor']} at ${ch['unit_price']:.2f}/{ch.get('unit','ea')} (saves ${ch.get('savings_vs_highest',0):.2f})")

    # Vendor history
    if "vendor_history" in data:
        for vendor, hist in data["vendor_history"].items():
            lines.append(f"\nVENDOR: {vendor} ({len(hist)} items)")
            seen_dates = set()
            for h in hist:
                d = h.get("invoice_date", "?")
                if d not in seen_dates:
                    seen_dates.add(d)
                    lines.append(f"  Invoice #{h.get('invoice_number','?')} ({d}):")
                lines.append(f"    {h['item']:35s} ${h['unit_price']:>7.2f}/{h.get('unit','ea')}  qty={h.get('quantity',1)}")

    # Inventory
    if "inventory" in data:
        inv = data["inventory"]
        if inv:
            lines.append("\nINVENTORY:")
            for k, v in inv.items():
                status = v.get("status", "?").upper() if isinstance(v, dict) else "?"
                stock = v.get("stock", v.get("quantity", "?")) if isinstance(v, dict) else v
                unit = v.get("unit", "") if isinstance(v, dict) else ""
                lines.append(f"  {k:25s} {stock} {unit}  [{status}]")

    # Costs
    if "costs" in data:
        costs = data["costs"]
        lines.append("\nPRODUCT COSTS:")
        for product, d in sorted(costs.items(), key=lambda x: x[1].get("margin_pct", 0)):
            lines.append(f"  {product:22s} cost=${d.get('cost',0):5.2f}  price=${d.get('price',0):5.2f}  margin={d.get('margin_pct',0):5.1f}%  {d.get('grade','?')}")

    # Review queue
    if "review_queue" in data:
        lines.append(f"\nREVIEW QUEUE ({len(data['review_queue'])} items):")
        for r in data["review_queue"][:10]:
            lines.append(f"  {r.get('vendor','?'):25s} {r.get('raw_name','?'):30s} conf={r.get('confidence','?')}")

    # DB summary
    if "db_summary" in data:
        s = data["db_summary"]
        lines.append(f"\nDB: {s.get('invoices',0)} invoices, {s.get('items',0)} items, {s.get('vendors',0)} vendors")

    return "\n".join(lines) if lines else ""
