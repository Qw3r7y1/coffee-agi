"""
Dropbox Invoice Connector for Coffee AGI

Fetches invoice files (PDF, PNG, JPG, CSV) from a Dropbox folder
and saves them locally for the invoice intake pipeline.

Uses the existing DropboxClient from core/dropbox_client.py.

Setup:
  Add to .env:
    DROPBOX_ACCESS_TOKEN=sl.xxxx          (or refresh token flow)
    DROPBOX_INVOICE_FOLDER=/Maillard/invoices   (optional, default shown)
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import sys
from loguru import logger
from dotenv import load_dotenv

# Ensure project root is on path when run as script
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

load_dotenv(Path(_project_root) / ".env")

from core.dropbox_client import DropboxClient

# ── Config ───────────────────────────────────────────────────────

ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".csv"}
DEFAULT_INVOICE_FOLDER = "/Maillard/invoices"
LOCAL_RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "invoices_raw"

_dbx = DropboxClient()


# ── Core Functions ───────────────────────────────────────────────


async def list_invoice_files(folder_path: str | None = None) -> list[dict]:
    """
    List invoice files in a Dropbox folder.
    Only returns files with allowed extensions (PDF, PNG, JPG, CSV).

    Returns list of:
      { name, path, size, modified, extension }

    Returns empty list if Dropbox is not configured.
    """
    if not _dbx.is_configured():
        logger.warning("Dropbox not configured — returning empty list")
        return []

    folder = folder_path or os.getenv("DROPBOX_INVOICE_FOLDER", DEFAULT_INVOICE_FOLDER)

    try:
        entries = await _dbx.list_folder(folder)
    except Exception as e:
        logger.error(f"Failed to list Dropbox folder '{folder}': {e}")
        return []

    invoices = []
    subdirs = []
    for entry in entries:
        if entry.get("is_dir"):
            # Skip system/backup folders
            name = entry["name"].lower()
            if name.startswith(".") or name.startswith("_"):
                continue
            subdirs.append(entry)
            continue
        ext = Path(entry["name"]).suffix.lower()
        if ext not in ALLOWED_EXTENSIONS:
            continue
        invoices.append({
            "name": entry["name"],
            "path": entry["path"],
            "size": entry.get("size", 0),
            "modified": entry.get("modified", ""),
            "extension": ext,
            "vendor_folder": None,
        })

    # Scan vendor subfolders
    for subdir in subdirs:
        try:
            sub_entries = await _dbx.list_folder(subdir["path"])
            vendor_name = subdir["name"]
            for entry in sub_entries:
                if entry.get("is_dir"):
                    continue
                ext = Path(entry["name"]).suffix.lower()
                if ext not in ALLOWED_EXTENSIONS:
                    continue
                invoices.append({
                    "name": entry["name"],
                    "path": entry["path"],
                    "size": entry.get("size", 0),
                    "modified": entry.get("modified", ""),
                    "extension": ext,
                    "vendor_folder": vendor_name,
                })
        except Exception as e:
            logger.warning(f"Failed to list subfolder '{subdir['path']}': {e}")

    invoices.sort(key=lambda x: x.get("modified", ""), reverse=True)
    logger.info(f"Found {len(invoices)} invoice files in '{folder}' (inc. {len(subdirs)} vendor subfolders)")
    return invoices


async def download_invoice(
    file_path: str,
    local_path: str | None = None,
    vendor_folder: str | None = None,
) -> str | None:
    """
    Download a single invoice file from Dropbox to local storage.

    Args:
        file_path: Dropbox path (e.g. /OFFICE/Invoices/Redway/inv.pdf)
        local_path: Optional override for local save path.
        vendor_folder: If set, saves to data/invoices_raw/<vendor>/<filename>.

    Returns:
        Local file path on success, None on failure.
    """
    if not _dbx.is_configured():
        logger.warning("Dropbox not configured — cannot download")
        return None

    filename = Path(file_path).name
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        logger.warning(f"Skipping unsupported file type: {filename} ({ext})")
        return None

    if local_path:
        dest = Path(local_path)
    elif vendor_folder:
        dest = LOCAL_RAW_DIR / vendor_folder / filename
    else:
        dest = LOCAL_RAW_DIR / filename

    # Skip if already downloaded
    if dest.exists():
        logger.debug(f"Already exists locally: {dest}")
        return str(dest)

    try:
        content = await _dbx.download_file(file_path)
    except Exception as e:
        logger.error(f"Failed to download '{file_path}': {e}")
        return None

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)
    logger.info(f"Downloaded: {file_path} → {dest} ({len(content)} bytes)")
    return str(dest)


async def fetch_latest_invoices(
    folder_path: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    Fetch the latest invoice files from Dropbox (including vendor subfolders).

    Lists the folder + subfolders, downloads up to `limit` most recent files,
    and returns metadata + local paths.

    Returns list of:
      { name, path, extension, modified, vendor_folder, local_path, status }
    """
    files = await list_invoice_files(folder_path)
    if not files:
        return []

    results = []
    for entry in files[:limit]:
        local = await download_invoice(entry["path"], vendor_folder=entry.get("vendor_folder"))
        results.append({
            "name": entry["name"],
            "dropbox_path": entry["path"],
            "extension": entry["extension"],
            "modified": entry.get("modified", ""),
            "size": entry.get("size", 0),
            "local_path": local,
            "status": "downloaded" if local else "failed",
        })

    downloaded = sum(1 for r in results if r["status"] == "downloaded")
    logger.info(f"Fetched {downloaded}/{len(results)} invoices")
    return results


# ── Sync State ───────────────────────────────────────────────────

import json
from datetime import datetime

SYNC_STATE_PATH = Path(__file__).resolve().parent.parent / "data" / "dropbox_sync_state.json"


def _load_sync_state() -> dict:
    try:
        with open(SYNC_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"last_sync": None, "files": {}, "last_summary": None}


def _save_sync_state(state: dict) -> None:
    SYNC_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SYNC_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def _file_key(entry: dict) -> str:
    """Unique key for a Dropbox file: lowercase path."""
    return entry.get("path", "").lower()


def _file_changed(entry: dict, state: dict) -> bool:
    """Check if a file is new or modified compared to sync state."""
    key = _file_key(entry)
    prev = state.get("files", {}).get(key)
    if prev is None:
        return True  # new file
    # Changed if modified time or size differs
    if entry.get("modified", "") != prev.get("modified", ""):
        return True
    if entry.get("size", 0) != prev.get("size", 0):
        return True
    return False


# ── Incremental Sync ─────────────────────────────────────────────


async def sync_dropbox_invoices(folder_path: str | None = None) -> dict:
    """
    Incremental sync: download only new/changed files, process them,
    store to DB, update sync state.

    Returns:
        {
            "new_files": int,
            "updated_files": int,
            "skipped_files": int,
            "failed_files": int,
            "review_required": int,
            "last_sync": str,
            "details": [...]
        }
    """
    state = _load_sync_state()
    all_files = await list_invoice_files(folder_path)

    if not all_files:
        return {
            "new_files": 0, "updated_files": 0, "skipped_files": 0,
            "failed_files": 0, "review_required": 0,
            "last_sync": state.get("last_sync"),
            "details": [],
        }

    new_count = 0
    updated_count = 0
    skipped_count = 0
    failed_count = 0
    review_count = 0
    details = []

    for entry in all_files:
        key = _file_key(entry)
        is_new = key not in state.get("files", {})
        changed = _file_changed(entry, state)

        if not changed:
            skipped_count += 1
            continue

        # Download
        local = await download_invoice(
            entry["path"],
            vendor_folder=entry.get("vendor_folder"),
        )
        if not local:
            failed_count += 1
            details.append({"file": entry["name"], "action": "failed", "reason": "download_failed"})
            continue

        # Force re-download if file was modified (overwrite local)
        if not is_new and changed:
            dest = Path(local)
            try:
                content = await _dbx.download_file(entry["path"])
                dest.write_bytes(content)
                logger.info(f"[SYNC] Re-downloaded modified file: {entry['name']}")
            except Exception as e:
                failed_count += 1
                details.append({"file": entry["name"], "action": "failed", "reason": str(e)})
                continue

        # Process through Vision + intake pipeline
        try:
            from maillard.mcp.accounting.invoice_reader import read_and_ingest
            result = await read_and_ingest(local)

            if "error" in result:
                failed_count += 1
                details.append({"file": entry["name"], "action": "failed", "reason": result["error"]})
            else:
                action = "new" if is_new else "updated"
                if is_new:
                    new_count += 1
                else:
                    updated_count += 1

                cs = result.get("confidence_summary", {})
                file_review = cs.get("review_required", 0)
                review_count += file_review

                details.append({
                    "file": entry["name"],
                    "action": action,
                    "vendor": result.get("vendor", "?"),
                    "items": cs.get("total_lines", 0),
                    "review_required": file_review,
                    "db_status": result.get("db_storage", {}).get("status", "?"),
                })
        except Exception as e:
            failed_count += 1
            details.append({"file": entry["name"], "action": "failed", "reason": str(e)})

        # Update sync state for this file
        if "files" not in state:
            state["files"] = {}
        state["files"][key] = {
            "modified": entry.get("modified", ""),
            "size": entry.get("size", 0),
            "name": entry["name"],
            "synced_at": datetime.now().isoformat(),
        }

    # Save state
    now = datetime.now().isoformat()
    state["last_sync"] = now
    summary = {
        "new_files": new_count,
        "updated_files": updated_count,
        "skipped_files": skipped_count,
        "failed_files": failed_count,
        "review_required": review_count,
        "last_sync": now,
    }
    state["last_summary"] = summary
    _save_sync_state(state)

    logger.info(
        f"[SYNC] Done: {new_count} new, {updated_count} updated, "
        f"{skipped_count} skipped, {failed_count} failed, {review_count} review"
    )

    # Run post-sync pipeline if any files were processed
    if new_count > 0 or updated_count > 0:
        try:
            from maillard.mcp.accounting.post_sync import post_sync_pipeline
            ps = post_sync_pipeline()
            summary["post_sync"] = {
                "completed": ps.get("pipeline_completed", False),
                "price_changes": len(ps.get("price_changes", [])),
                "procurement_signals": len(ps.get("procurement_signals", [])),
                "review_queue": ps.get("review_queue_count", 0),
            }
            logger.info(f"[SYNC] Post-sync pipeline completed")
        except Exception as e:
            summary["post_sync"] = {"completed": False, "error": str(e)}
            logger.error(f"[SYNC] Post-sync pipeline failed: {e}")
    else:
        summary["post_sync"] = {"completed": False, "reason": "no_new_files"}

    summary["details"] = details
    return summary


def get_sync_status() -> dict:
    """Get current sync status without running a sync."""
    state = _load_sync_state()
    last = state.get("last_summary") or {}

    # Count pending review from DB
    review_count = 0
    try:
        from maillard.mcp.accounting.invoice_db import get_db_summary
        db = get_db_summary()
        review_count = db.get("review_required", 0)
    except Exception:
        pass

    return {
        "last_sync": state.get("last_sync"),
        "files_tracked": len(state.get("files", {})),
        "last_new_files": last.get("new_files", 0),
        "last_updated_files": last.get("updated_files", 0),
        "last_skipped_files": last.get("skipped_files", 0),
        "last_failed_files": last.get("failed_files", 0),
        "pending_review": review_count,
    }


# ── CLI ──────────────────────────────────────────────────────────


async def _test():
    """Quick test — run with: python scripts/dropbox_connector.py"""
    import sys as _sys

    print("=== Dropbox Invoice Connector ===")
    print(f"Configured: {_dbx.is_configured()}")
    print(f"Invoice folder: {os.getenv('DROPBOX_INVOICE_FOLDER', DEFAULT_INVOICE_FOLDER)}")
    print(f"Local storage: {LOCAL_RAW_DIR}")
    print()

    if not _dbx.is_configured():
        print("Dropbox not configured. Add DROPBOX_ACCESS_TOKEN to .env")
        return

    # Check for --sync flag
    if "--sync" in _sys.argv:
        print("Running incremental sync...")
        result = await sync_dropbox_invoices()
        print(f"\n  New:     {result['new_files']}")
        print(f"  Updated: {result['updated_files']}")
        print(f"  Skipped: {result['skipped_files']}")
        print(f"  Failed:  {result['failed_files']}")
        print(f"  Review:  {result['review_required']}")
        print(f"  Synced:  {result['last_sync']}")
        if result.get("details"):
            print("\n  Details:")
            for d in result["details"]:
                print(f"    [{d['action']:7s}] {d['file']}")
    elif "--status" in _sys.argv:
        s = get_sync_status()
        print(json.dumps(s, indent=2))
    else:
        print("Fetching latest invoices (no processing)...")
        results = await fetch_latest_invoices(limit=5)
        for r in results:
            status = "OK" if r["status"] == "downloaded" else "FAIL"
            print(f"  [{status}] {r['name']} -> {r['local_path']}")
        print(f"\nTotal: {len(results)} files")
        print("\nUse --sync to run incremental sync, --status to check sync state")


if __name__ == "__main__":
    asyncio.run(_test())
