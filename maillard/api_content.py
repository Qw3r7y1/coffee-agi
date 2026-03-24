"""
Maillard Viral Content Engine — Performance API
Helper functions + endpoint for the content dashboard.
"""

import json
import os
from collections import defaultdict
from fastapi import APIRouter

router = APIRouter()

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
QUEUE_PATH = os.path.join(DATA_DIR, "content_queue.json")
PERF_PATH = os.path.join(DATA_DIR, "content_performance.json")


def _load_json(path: str) -> list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# ── Helper functions ─────────────────────────────────────────────


def summarize_content_performance() -> dict:
    """Aggregate queue status + performance totals."""
    queue = _load_json(QUEUE_PATH)
    perf = _load_json(PERF_PATH)

    # Queue counts
    status_counts = defaultdict(int)
    for item in queue:
        status_counts[item.get("status", "unknown")] += 1

    # Performance totals
    total_views = sum(p.get("views", 0) for p in perf)
    total_likes = sum(p.get("likes", 0) for p in perf)
    total_comments = sum(p.get("comments", 0) for p in perf)
    total_shares = sum(p.get("shares", 0) for p in perf)
    total_saves = sum(p.get("saves", 0) for p in perf)

    watch_times = [p["watch_time_avg_sec"] for p in perf if "watch_time_avg_sec" in p]
    comp_rates = [p["completion_rate"] for p in perf if "completion_rate" in p]

    return {
        "queue": {
            "pending": status_counts.get("pending", 0),
            "posted": status_counts.get("posted", 0),
            "failed": status_counts.get("failed", 0),
            "total": len(queue),
        },
        "totals": {
            "views": total_views,
            "likes": total_likes,
            "comments": total_comments,
            "shares": total_shares,
            "saves": total_saves,
            "avg_watch_time": round(sum(watch_times) / len(watch_times), 1) if watch_times else 0,
            "avg_completion_rate": round(sum(comp_rates) / len(comp_rates), 2) if comp_rates else 0,
        },
        "post_count": len(perf),
    }


def get_top_performers(n: int = 3) -> dict:
    """Return top N posts by views and by completion rate."""
    perf = _load_json(PERF_PATH)

    def _pick(items, key, count):
        sorted_items = sorted(items, key=lambda x: x.get(key, 0), reverse=True)
        return [
            {
                "id": p["id"],
                "caption": p.get("caption", "")[:80],
                "type": p.get("type", "?"),
                "hook": p.get("hook", "?"),
                "platform": p.get("platform", "?"),
                key: p.get(key, 0),
            }
            for p in sorted_items[:count]
        ]

    return {
        "by_views": _pick(perf, "views", n),
        "by_completion": _pick(perf, "completion_rate", n),
    }


def get_content_type_breakdown() -> dict:
    """Breakdown performance by content type and hook category."""
    perf = _load_json(PERF_PATH)
    if not perf:
        return {"by_type": [], "by_hook": [], "best_time": None}

    # Group by type
    type_agg = defaultdict(lambda: {"views": 0, "count": 0, "completion": []})
    for p in perf:
        t = p.get("type", "unknown")
        type_agg[t]["views"] += p.get("views", 0)
        type_agg[t]["count"] += 1
        if "completion_rate" in p:
            type_agg[t]["completion"].append(p["completion_rate"])

    by_type = []
    for t, d in type_agg.items():
        avg_comp = round(sum(d["completion"]) / len(d["completion"]), 2) if d["completion"] else 0
        by_type.append({
            "type": t,
            "total_views": d["views"],
            "avg_views": round(d["views"] / d["count"]),
            "posts": d["count"],
            "avg_completion": avg_comp,
        })
    by_type.sort(key=lambda x: x["avg_views"], reverse=True)

    # Group by hook
    hook_agg = defaultdict(lambda: {"views": 0, "count": 0, "completion": []})
    for p in perf:
        h = p.get("hook", "unknown")
        hook_agg[h]["views"] += p.get("views", 0)
        hook_agg[h]["count"] += 1
        if "completion_rate" in p:
            hook_agg[h]["completion"].append(p["completion_rate"])

    by_hook = []
    for h, d in hook_agg.items():
        avg_comp = round(sum(d["completion"]) / len(d["completion"]), 2) if d["completion"] else 0
        by_hook.append({
            "hook": h,
            "total_views": d["views"],
            "avg_views": round(d["views"] / d["count"]),
            "posts": d["count"],
            "avg_completion": avg_comp,
        })
    by_hook.sort(key=lambda x: x["avg_views"], reverse=True)

    # Best posting time (hour with highest avg views)
    hour_agg = defaultdict(lambda: {"views": 0, "count": 0})
    for p in perf:
        posted = p.get("posted_at", "")
        if "T" in posted:
            try:
                hour = int(posted.split("T")[1].split(":")[0])
                hour_agg[hour]["views"] += p.get("views", 0)
                hour_agg[hour]["count"] += 1
            except (ValueError, IndexError):
                pass

    best_time = None
    if hour_agg:
        best_hour = max(hour_agg, key=lambda h: hour_agg[h]["views"] / hour_agg[h]["count"])
        best_time = {
            "hour": best_hour,
            "label": f"{best_hour:02d}:00",
            "avg_views": round(hour_agg[best_hour]["views"] / hour_agg[best_hour]["count"]),
        }

    return {
        "by_type": by_type,
        "by_hook": by_hook,
        "best_time": best_time,
    }


def generate_learning_summary() -> dict:
    """Generate learning loop insights from performance data."""
    perf = _load_json(PERF_PATH)
    if not perf:
        return {
            "working": ["Not enough data yet"],
            "test_next": ["Post more content to generate insights"],
            "variations": [],
        }

    breakdown = get_content_type_breakdown()
    by_type = breakdown["by_type"]
    by_hook = breakdown["by_hook"]

    working = []
    test_next = []
    variations = []

    # What's working
    if by_hook:
        best_hook = by_hook[0]
        working.append(f"'{best_hook['hook']}' hooks drive highest avg views ({best_hook['avg_views']:,})")
    if by_type:
        best_type = by_type[0]
        working.append(f"'{best_type['type']}' content averages {best_type['avg_views']:,} views")

    # High completion posts
    high_comp = [p for p in perf if p.get("completion_rate", 0) >= 0.80]
    if high_comp:
        working.append(f"{len(high_comp)} posts hit 80%+ completion rate")

    if breakdown["best_time"]:
        working.append(f"Best posting time: {breakdown['best_time']['label']} ({breakdown['best_time']['avg_views']:,} avg views)")

    # What to test next
    if by_hook and len(by_hook) > 1:
        weakest_hook = by_hook[-1]
        test_next.append(f"Rework '{weakest_hook['hook']}' hooks — only {weakest_hook['avg_views']:,} avg views")
    if by_type and len(by_type) > 1:
        weakest_type = by_type[-1]
        test_next.append(f"Try different formats for '{weakest_type['type']}' ({weakest_type['avg_views']:,} avg views)")

    test_next.append("A/B test caption length: short punchy vs. storytelling")
    test_next.append("Test posting at off-peak hours for algorithm boost")

    # Variation suggestions
    if by_hook:
        top_hook = by_hook[0]["hook"]
        variations.append(f"Create 3 variations of '{top_hook}' hook with different openings")
    if by_type:
        top_type = by_type[0]["type"]
        variations.append(f"Produce '{top_type}' content in series format (Part 1, 2, 3)")
    variations.append("Remix top performer into carousel + story format")

    return {
        "working": working,
        "test_next": test_next,
        "variations": variations,
    }


# ── API endpoint ─────────────────────────────────────────────────


@router.get("/content/performance-summary")
async def content_performance_summary():
    """Full dashboard payload for the Viral Content Engine."""
    summary = summarize_content_performance()
    top = get_top_performers()
    breakdown = get_content_type_breakdown()
    learning = generate_learning_summary()

    return {
        "queue": summary["queue"],
        "totals": summary["totals"],
        "post_count": summary["post_count"],
        "top_performers": top,
        "breakdown": breakdown,
        "learning": learning,
    }
