"""
Invoice Sync API — /api/invoices/*

Endpoints for Dropbox invoice sync and status.
"""
from __future__ import annotations

import asyncio
from fastapi import APIRouter
from loguru import logger

router = APIRouter(prefix="/invoices", tags=["Invoices"])

_sync_running = False


@router.post("/sync-dropbox")
async def sync_dropbox():
    """Trigger incremental Dropbox invoice sync."""
    global _sync_running

    if _sync_running:
        return {"error": "Sync already running", "status": "running"}

    _sync_running = True
    try:
        from scripts.dropbox_connector import sync_dropbox_invoices
        result = await sync_dropbox_invoices()
        logger.info(f"[INVOICE-API] Sync complete: {result.get('new_files', 0)} new, "
                     f"{result.get('updated_files', 0)} updated, "
                     f"{result.get('skipped_files', 0)} skipped")
        # Strip details for lighter response
        result.pop("details", None)
        result["status"] = "ok"
        return result
    except Exception as e:
        logger.error(f"[INVOICE-API] Sync failed: {e}")
        return {"error": str(e), "status": "error"}
    finally:
        _sync_running = False


@router.get("/sync-status")
async def sync_status():
    """Get current sync status without running a sync."""
    try:
        from scripts.dropbox_connector import get_sync_status
        s = get_sync_status()
        s["status"] = "running" if _sync_running else "ok"
        return s
    except Exception as e:
        logger.error(f"[INVOICE-API] Status check failed: {e}")
        return {"status": "error", "error": str(e)}
