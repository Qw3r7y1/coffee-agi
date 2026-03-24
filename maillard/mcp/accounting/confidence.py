"""
Invoice Confidence & Anomaly Detection for Coffee AGI.

Tightens confidence scoring, filters anomalous rows,
and provides safe default answer behavior.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from loguru import logger


# ── Anomaly Detection ────────────────────────────────────────────

ANOMALY_THRESHOLD_PCT = 50  # flag if price deviates >50% from median
MIN_HISTORY_FOR_ANOMALY = 2  # need at least 2 data points


def detect_price_anomalies(db_path: str) -> list[dict]:
    """
    Scan all invoice items and flag rows where price deviates
    significantly from the median for the same normalized item.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get all items grouped by normalized_name
    items = conn.execute("""
        SELECT ii.id, ii.normalized_name, ii.unit_price, ii.unit, ii.confidence,
               i.vendor, i.invoice_date, i.invoice_number
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.id
        WHERE ii.unit_price > 0 AND ii.normalized_name IS NOT NULL
        ORDER BY LOWER(ii.normalized_name), i.invoice_date
    """).fetchall()
    conn.close()

    # Group by normalized name
    groups: dict[str, list[dict]] = {}
    for row in items:
        key = row["normalized_name"].lower()
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(row))

    anomalies = []
    for name, rows in groups.items():
        if len(rows) < MIN_HISTORY_FOR_ANOMALY:
            continue

        prices = sorted(r["unit_price"] for r in rows)
        median = prices[len(prices) // 2]
        if median <= 0:
            continue

        for row in rows:
            deviation_pct = abs(row["unit_price"] - median) / median * 100
            if deviation_pct > ANOMALY_THRESHOLD_PCT:
                anomalies.append({
                    "id": row["id"],
                    "item": row["normalized_name"],
                    "price": row["unit_price"],
                    "unit": row["unit"],
                    "median_price": median,
                    "deviation_pct": round(deviation_pct, 1),
                    "vendor": row["vendor"],
                    "date": row["invoice_date"],
                    "invoice_number": row["invoice_number"],
                })

    if anomalies:
        logger.info(f"[CONFIDENCE] Found {len(anomalies)} price anomalies")
    return anomalies


def get_anomalous_item_ids(db_path: str) -> set[int]:
    """Get the set of invoice_item IDs that are anomalous."""
    return {a["id"] for a in detect_price_anomalies(db_path)}


# ── Date Anomaly ─────────────────────────────────────────────────

def detect_date_anomalies(db_path: str) -> list[dict]:
    """Flag invoices with dates that seem wrong (too old or in the future)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, vendor, invoice_date, invoice_number
        FROM invoices WHERE invoice_date IS NOT NULL
    """).fetchall()
    conn.close()

    anomalies = []
    now = datetime.now()
    for row in rows:
        try:
            d = datetime.strptime(row["invoice_date"], "%Y-%m-%d")
            age_days = (now - d).days
            if age_days > 365:  # older than 1 year
                anomalies.append({
                    "invoice_id": row["id"],
                    "vendor": row["vendor"],
                    "date": row["invoice_date"],
                    "invoice_number": row["invoice_number"],
                    "issue": f"Date is {age_days} days old — likely OCR error",
                })
            elif age_days < -30:  # more than 30 days in the future
                anomalies.append({
                    "invoice_id": row["id"],
                    "vendor": row["vendor"],
                    "date": row["invoice_date"],
                    "invoice_number": row["invoice_number"],
                    "issue": f"Date is {abs(age_days)} days in the future — likely OCR error",
                })
        except (ValueError, TypeError):
            pass

    return anomalies


# ── Confidence Filtering ─────────────────────────────────────────


def filter_by_confidence(
    rows: list[dict],
    min_confidence: str = "medium",
    exclude_anomalous_ids: set[int] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Split rows into included and excluded based on confidence.

    Args:
        rows: list of dicts with 'confidence' and optionally 'id' keys
        min_confidence: minimum confidence to include ("high", "medium", "low")
        exclude_anomalous_ids: set of item IDs to exclude as anomalous

    Returns:
        (included, excluded) — both lists of dicts
    """
    levels = {"high": 3, "medium": 2, "low": 1}
    min_level = levels.get(min_confidence, 2)

    included = []
    excluded = []

    for row in rows:
        conf = row.get("confidence", "medium")
        level = levels.get(conf, 2)

        # Exclude anomalous
        if exclude_anomalous_ids and row.get("id") in exclude_anomalous_ids:
            row["_excluded_reason"] = "anomalous_price"
            excluded.append(row)
            continue

        if level >= min_level:
            included.append(row)
        else:
            row["_excluded_reason"] = f"low_confidence ({conf})"
            excluded.append(row)

    return included, excluded


# ── Reconciliation Check ─────────────────────────────────────────


def reconcile_and_rescore(db_path: str) -> dict:
    """
    Re-check all invoice items: reconcile qty*price vs line_total,
    and downgrade confidence where math doesn't add up.

    Returns summary of changes.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT id, quantity, unit_price, line_total, confidence
        FROM invoice_items
        WHERE unit_price > 0 AND quantity > 0
    """).fetchall()

    downgraded = 0
    for row in rows:
        expected = round(row["quantity"] * row["unit_price"], 2)
        actual = row["line_total"] or 0
        if actual > 0 and abs(expected - actual) > 0.50:
            # Math doesn't reconcile — downgrade if currently high
            if row["confidence"] == "high":
                conn.execute(
                    "UPDATE invoice_items SET confidence = 'medium' WHERE id = ?",
                    (row["id"],),
                )
                downgraded += 1

    conn.commit()
    conn.close()

    if downgraded:
        logger.info(f"[CONFIDENCE] Downgraded {downgraded} items from high to medium (reconciliation failure)")
    return {"downgraded": downgraded, "checked": len(rows)}


# ── Broad vs Exact Match Detection ───────────────────────────────


def classify_match_breadth(
    query_tokens: list[str],
    matched_items: list[str],
) -> tuple[str, list[list[str]]]:
    """
    Determine if matched items are an exact match, a single product family,
    or multiple distinct families requiring disambiguation.

    Returns:
        (breadth: "exact"|"single_family"|"multiple_families",
         families: [[item_names], ...])
    """
    if len(matched_items) <= 1:
        return "exact", [matched_items]

    # Group items by shared significant words
    def _key_words(name: str) -> set[str]:
        stopwords = {"for", "the", "a", "an", "with", "of", "oz", "lb", "kg", "gal", "ct"}
        return {w.lower() for w in name.split() if len(w) > 2 and w.lower() not in stopwords and not w.isdigit()}

    families: list[list[str]] = []
    assigned = set()

    for i, item in enumerate(matched_items):
        if i in assigned:
            continue
        family = [item]
        kw_i = _key_words(item)
        for j, other in enumerate(matched_items):
            if j <= i or j in assigned:
                continue
            kw_j = _key_words(other)
            # Same family if they share >50% of keywords
            overlap = len(kw_i & kw_j)
            union = len(kw_i | kw_j)
            if union > 0 and overlap / union > 0.3:
                family.append(other)
                assigned.add(j)
        families.append(family)
        assigned.add(i)

    if len(families) == 1:
        return "single_family", families
    return "multiple_families", families
