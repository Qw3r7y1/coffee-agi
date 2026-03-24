"""
Action Execution Tracking + Outcome Validation for Maillard Operations.

Lifecycle: pending -> in_progress -> completed_success / completed_failed / skipped

On completion, the system validates whether the action actually resolved
the underlying issue. If not, the action is marked completed_failed and
re-enters the next plan at elevated priority.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib

from loguru import logger

from maillard.models.database import SessionLocal
from maillard.models.operations import ActionLog, InventoryItem


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _make_action_id(action: dict, plan_date: str) -> str:
    key = f"{plan_date}:{action.get('category', '')}:{action.get('sub', '')}:{action.get('action', '')}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# =============================================================================
# PERSIST
# =============================================================================


def persist_plan_actions(actions: list[dict], plan_date: str | None = None) -> int:
    date = plan_date or _today()
    count = 0
    try:
        with SessionLocal() as session:
            for a in actions:
                aid = _make_action_id(a, date)
                existing = session.query(ActionLog).filter_by(action_id=aid, plan_date=date).first()
                if existing:
                    continue

                log = ActionLog(
                    action_id=aid,
                    plan_date=date,
                    action_text=a.get("action", ""),
                    category=a.get("category"),
                    sub_type=a.get("sub"),
                    priority=a.get("priority"),
                    status="pending",
                    action_data=_safe_json(a),
                )
                session.add(log)
                count += 1
            session.commit()
        logger.info(f"[EXECUTION] persisted {count} new actions for {date}")
    except Exception as e:
        logger.error(f"[EXECUTION] persist failed: {e}")
    return count


def _safe_json(d: dict) -> dict:
    skip = {"sub_orders", "sub_batches", "batch_details"}
    return {k: v for k, v in d.items() if k not in skip and not callable(v)}


# =============================================================================
# STATUS UPDATES
# =============================================================================

# Valid statuses
VALID_STATUSES = ("pending", "in_progress", "completed_success", "completed_failed", "skipped")
# Statuses that count as "done" (excluded from active plan)
DONE_STATUSES = ("completed_success", "skipped")
# Statuses that need re-attention
FAILED_STATUSES = ("completed_failed",)


def start_action(action_id: str, assigned_to: str | None = None, notes: str | None = None) -> dict:
    return _update_status(action_id, "in_progress", assigned_to=assigned_to, notes=notes)


def complete_action(action_id: str, notes: str | None = None) -> dict:
    """Mark action as completed, then validate the outcome.

    If validation passes -> completed_success
    If validation fails  -> completed_failed (will re-enter next plan)
    """
    # First set to a temporary completed state so we can read the action data
    result = _update_status(action_id, "completed_success", notes=notes)
    if "error" in result:
        return result

    # Run outcome validation
    validation = validate_action(action_id)
    if validation.get("outcome") == "failed":
        # Downgrade to completed_failed
        _update_status(action_id, "completed_failed",
                       notes=f"Validation failed: {validation.get('reason', 'unknown')}")
        result["status"] = "completed_failed"
        result["validation"] = validation
        logger.warning(f"[EXECUTION] {action_id}: completed but FAILED validation: {validation.get('reason')}")
    else:
        result["validation"] = validation
        logger.info(f"[EXECUTION] {action_id}: completed + validated successfully")

    return result


def skip_action(action_id: str, notes: str | None = None) -> dict:
    return _update_status(action_id, "skipped", notes=notes)


def _update_status(action_id: str, new_status: str, assigned_to: str | None = None, notes: str | None = None) -> dict:
    try:
        with SessionLocal() as session:
            log = session.query(ActionLog).filter_by(action_id=action_id).first()
            if not log:
                log = session.query(ActionLog).filter_by(
                    action_id=action_id, plan_date=_today()
                ).first()
            if not log:
                return {"error": f"Action '{action_id}' not found"}

            old_status = log.status
            log.status = new_status

            if assigned_to:
                log.assigned_to = assigned_to
            if notes:
                ts = datetime.now(timezone.utc).strftime("%H:%M")
                entry = f"[{ts} {new_status}] {notes}"
                log.notes = f"{log.notes}\n{entry}" if log.notes else entry

            now = datetime.now(timezone.utc)
            if new_status == "in_progress":
                log.started_at = now
            elif new_status in ("completed_success", "completed_failed", "skipped"):
                log.completed_at = now

            session.commit()
            session.refresh(log)
            logger.info(f"[EXECUTION] {action_id}: {old_status} -> {new_status}")
            return log.to_dict()
    except Exception as e:
        logger.error(f"[EXECUTION] update failed: {e}")
        return {"error": str(e)}


# =============================================================================
# OUTCOME VALIDATION
# =============================================================================


def validate_action(action_id: str) -> dict:
    """Validate whether a completed action actually resolved its issue.

    Reads the action's validation rules from its stored data and checks
    current system state against the success condition.

    Returns:
        {
            "outcome": "success" | "failed" | "no_check",
            "metric": str,
            "expected": str,
            "actual": str,
            "reason": str | None,
        }
    """
    try:
        with SessionLocal() as session:
            log = session.query(ActionLog).filter_by(action_id=action_id).first()
            if not log:
                return {"outcome": "no_check", "reason": "Action not found"}

            data = log.action_data or {}
            validation = data.get("validation")
            if not validation:
                return {"outcome": "no_check", "reason": "No validation rules defined"}

            metric = validation.get("metric", "")
            condition = validation.get("success_condition", "")
            sku = validation.get("sku")

            return _check_condition(metric, condition, sku, session)
    except Exception as e:
        logger.error(f"[EXECUTION] validation failed: {e}")
        return {"outcome": "no_check", "reason": str(e)}


def _check_condition(metric: str, condition: str, sku: str | None, session) -> dict:
    """Check a success condition against current system state.

    Supported metrics:
      - stock_days_gte:N  -- item has >= N days of stock
      - stock_qty_gte:N   -- item has >= N units in stock
      - stock_above_min   -- item is above its reorder threshold
      - roasted_available  -- roasted SKU has stock > 0
    """
    if not metric or not condition:
        return {"outcome": "no_check", "reason": "No metric/condition defined"}

    # Parse condition: "stock_days_gte:2" -> check days >= 2
    if metric == "stock_days" and sku:
        from maillard.mcp.operations.inventory_intelligence import predict_stockout
        predictions = predict_stockout()
        item_pred = next((p for p in predictions if p["sku"] == sku), None)
        if not item_pred:
            return {"outcome": "no_check", "metric": metric, "reason": f"SKU {sku} not found in predictions"}

        actual_days = item_pred["days_remaining"]
        try:
            threshold = float(condition)
        except ValueError:
            return {"outcome": "no_check", "reason": f"Invalid condition: {condition}"}

        if actual_days >= threshold:
            return {
                "outcome": "success",
                "metric": f"stock_days({sku})",
                "expected": f">= {threshold} days",
                "actual": f"{actual_days:.1f} days",
            }
        else:
            return {
                "outcome": "failed",
                "metric": f"stock_days({sku})",
                "expected": f">= {threshold} days",
                "actual": f"{actual_days:.1f} days",
                "reason": f"{sku} still at {actual_days:.1f} days (need >= {threshold})",
            }

    if metric == "stock_qty" and sku:
        item = session.query(InventoryItem).filter_by(sku=sku).first()
        if not item:
            return {"outcome": "no_check", "reason": f"SKU {sku} not found"}

        actual = item.quantity
        try:
            threshold = float(condition)
        except ValueError:
            return {"outcome": "no_check", "reason": f"Invalid condition: {condition}"}

        if actual >= threshold:
            return {
                "outcome": "success",
                "metric": f"stock_qty({sku})",
                "expected": f">= {threshold} {item.unit}",
                "actual": f"{actual} {item.unit}",
            }
        else:
            return {
                "outcome": "failed",
                "metric": f"stock_qty({sku})",
                "expected": f">= {threshold} {item.unit}",
                "actual": f"{actual} {item.unit}",
                "reason": f"{sku} at {actual} {item.unit} (need >= {threshold})",
            }

    if metric == "stock_above_min" and sku:
        item = session.query(InventoryItem).filter_by(sku=sku).first()
        if not item:
            return {"outcome": "no_check", "reason": f"SKU {sku} not found"}

        if item.quantity > item.min_quantity:
            return {
                "outcome": "success",
                "metric": f"stock_above_min({sku})",
                "expected": f"> {item.min_quantity} {item.unit}",
                "actual": f"{item.quantity} {item.unit}",
            }
        else:
            return {
                "outcome": "failed",
                "metric": f"stock_above_min({sku})",
                "expected": f"> {item.min_quantity} {item.unit}",
                "actual": f"{item.quantity} {item.unit}",
                "reason": f"{sku} still at {item.quantity} (min: {item.min_quantity})",
            }

    if metric == "roasted_available" and sku:
        item = session.query(InventoryItem).filter_by(sku=sku).first()
        if not item:
            return {"outcome": "no_check", "reason": f"SKU {sku} not found"}

        if item.quantity > 0:
            return {
                "outcome": "success",
                "metric": f"roasted_available({sku})",
                "expected": "> 0 kg",
                "actual": f"{item.quantity} kg",
            }
        else:
            return {
                "outcome": "failed",
                "metric": f"roasted_available({sku})",
                "expected": "> 0 kg",
                "actual": "0 kg",
                "reason": f"{sku} still at 0 after production",
            }

    return {"outcome": "no_check", "metric": metric, "reason": f"Unknown metric: {metric}"}


# =============================================================================
# QUERIES
# =============================================================================


def get_today_actions(status: str | None = None) -> list[dict]:
    try:
        with SessionLocal() as session:
            q = session.query(ActionLog).filter_by(plan_date=_today())
            if status:
                q = q.filter(ActionLog.status == status)
            return [l.to_dict() for l in q.order_by(ActionLog.id).all()]
    except Exception:
        return []


def get_completed_action_ids(plan_date: str | None = None) -> set[str]:
    """IDs that are done (success or skipped). Failed actions are NOT excluded."""
    date = plan_date or _today()
    try:
        with SessionLocal() as session:
            logs = (
                session.query(ActionLog.action_id)
                .filter(
                    ActionLog.plan_date == date,
                    ActionLog.status.in_(DONE_STATUSES),
                )
                .all()
            )
            return {l[0] for l in logs}
    except Exception:
        return set()


def get_failed_action_ids(plan_date: str | None = None) -> set[str]:
    """IDs that completed but failed validation. These re-enter the plan."""
    date = plan_date or _today()
    try:
        with SessionLocal() as session:
            logs = (
                session.query(ActionLog.action_id)
                .filter(
                    ActionLog.plan_date == date,
                    ActionLog.status.in_(FAILED_STATUSES),
                )
                .all()
            )
            return {l[0] for l in logs}
    except Exception:
        return set()


def get_action_stats(plan_date: str | None = None) -> dict:
    date = plan_date or _today()
    try:
        with SessionLocal() as session:
            logs = session.query(ActionLog).filter_by(plan_date=date).all()
            if not logs:
                return {"plan_date": date, "total": 0}

            by_status = {}
            for l in logs:
                by_status[l.status] = by_status.get(l.status, 0) + 1

            done = by_status.get("completed_success", 0) + by_status.get("skipped", 0)
            return {
                "plan_date": date,
                "total": len(logs),
                "pending": by_status.get("pending", 0),
                "in_progress": by_status.get("in_progress", 0),
                "completed_success": by_status.get("completed_success", 0),
                "completed_failed": by_status.get("completed_failed", 0),
                "skipped": by_status.get("skipped", 0),
                "completion_pct": round(done / len(logs) * 100, 0),
                "failure_count": by_status.get("completed_failed", 0),
            }
    except Exception as e:
        return {"error": str(e)}
