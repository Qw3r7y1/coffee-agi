"""
Invoice Reader — Uses Claude Vision to extract data from PDF/image invoices.

Reads real invoices (PDF, PNG, JPG) and returns structured data
compatible with invoice_intake.py for normalization and storage.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from pathlib import Path

from loguru import logger

EXTRACTION_PROMPT = """You are reading a real invoice document for Maillard Coffee Roasters.
Extract ALL data you can see. Return ONLY valid JSON, no markdown, no explanation.

IMPORTANT: Many invoices have HANDWRITTEN corrections — crossed-out quantities,
handwritten prices, circled amounts, arrows changing values, or pen annotations.
You MUST detect and report these separately from printed values.

Return this exact structure:
{
  "vendor": "vendor/supplier name exactly as printed",
  "invoice_date": "YYYY-MM-DD",
  "invoice_number": "invoice number if visible, or null",
  "line_items": [
    {
      "name": "item description exactly as printed",
      "printed_quantity": 1,
      "handwritten_quantity": null,
      "printed_unit_price": 0.00,
      "handwritten_unit_price": null,
      "unit": "ea",
      "line_total": null,
      "has_handwriting": false,
      "handwriting_note": null
    }
  ],
  "total": 0.00,
  "notes": "any extra info: delivery address, payment terms, handwritten notes"
}

Rules:
- Extract EVERY line item, even partial or handwritten ones
- For EACH line item, report printed values AND handwritten corrections separately:
  - printed_quantity: the machine-printed/typed quantity
  - handwritten_quantity: any handwritten number that overrides or corrects the printed qty (null if none)
  - printed_unit_price: the machine-printed/typed price
  - handwritten_unit_price: any handwritten price correction (null if none)
  - has_handwriting: true if ANY handwritten marks exist on this line (corrections, checkmarks, circles, crossouts)
  - handwriting_note: describe what the handwriting says/does (e.g. "quantity crossed out and replaced with 5", "price circled", "checkmark next to item", "arrow pointing to new price")
- If a handwritten value is hard to read, set it and add "(unclear)" to handwriting_note
- For unit, use: ea, lb, kg, gal, case, box, bag, pack, ct, oz, L
- line_total: the final total shown for this line (printed or handwritten), or null
- If the invoice has multiple pages, extract from all visible content
- If a field is not visible, use null
- Return ONLY the JSON object, nothing else"""


def _pdf_to_images(pdf_path: str, max_pages: int = 4) -> list[tuple[bytes, str]]:
    """Convert PDF pages to PNG images using PyMuPDF. Returns list of (bytes, media_type)."""
    import fitz

    images = []
    doc = fitz.open(pdf_path)
    for i, page in enumerate(doc):
        if i >= max_pages:
            break
        # Try PNG at decreasing DPI, fall back to JPEG for large pages
        # base64 is ~1.37x raw size, API limit is 5MB → cap raw at 3.5MB
        png_bytes = None
        for dpi in (150, 120, 96, 72):
            pix = page.get_pixmap(dpi=dpi)
            png_bytes = pix.tobytes("png")
            if len(png_bytes) < 3_500_000:
                break
        # If PNG still too large at 72 DPI, use JPEG
        if len(png_bytes) >= 3_500_000:
            for quality in (80, 60, 40):
                png_bytes = pix.tobytes("jpeg", quality)
                if len(png_bytes) < 3_500_000:
                    break
        media = "image/jpeg" if len(pix.tobytes("png")) >= 3_500_000 else "image/png"
        images.append((png_bytes, media))
    doc.close()
    return images


def _image_to_base64(file_path: str) -> tuple[str, str]:
    """Read an image file and return (base64_data, media_type)."""
    ext = Path(file_path).suffix.lower()
    media_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}
    media_type = media_map.get(ext, "image/png")
    with open(file_path, "rb") as f:
        return base64.standard_b64encode(f.read()).decode(), media_type


async def read_invoice_file(file_path: str) -> dict:
    """
    Read a PDF or image invoice using Claude Vision and return structured data.

    Args:
        file_path: Path to PDF, PNG, or JPG invoice file.

    Returns:
        Parsed invoice dict compatible with invoice_intake.extract_invoice_data(),
        or {"error": "..."} on failure.
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}"}

    ext = path.suffix.lower()
    if ext not in (".pdf", ".png", ".jpg", ".jpeg"):
        return {"error": f"Unsupported file type: {ext}"}

    logger.info(f"[INVOICE-READER] Reading {path.name} ({ext})")

    # Build image content blocks
    content_blocks = []

    try:
        if ext == ".pdf":
            images = _pdf_to_images(file_path)
            if not images:
                return {"error": "PDF has no pages"}
            for img_bytes, media_type in images:
                b64 = base64.standard_b64encode(img_bytes).decode()
                content_blocks.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": media_type, "data": b64},
                })
        else:
            b64, media_type = _image_to_base64(file_path)
            content_blocks.append({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": b64},
            })
    except Exception as e:
        return {"error": f"Failed to read file: {e}"}

    # Add the extraction prompt
    content_blocks.append({"type": "text", "text": EXTRACTION_PROMPT})

    # Call Claude Vision
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        response = await asyncio.to_thread(
            client.messages.create,
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            messages=[{"role": "user", "content": content_blocks}],
        )
        raw_text = response.content[0].text.strip()
    except Exception as e:
        logger.error(f"[INVOICE-READER] Claude Vision call failed: {e}")
        return {"error": f"Vision API failed: {e}"}

    # Parse JSON response
    try:
        # Handle potential markdown wrapping
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()
        invoice_data = json.loads(raw_text)
    except json.JSONDecodeError:
        logger.error(f"[INVOICE-READER] Failed to parse JSON from Claude: {raw_text[:200]}")
        return {"error": "Failed to parse invoice data", "raw_response": raw_text[:500]}

    invoice_data["_source_file"] = str(path.name)
    logger.info(f"[INVOICE-READER] Extracted: vendor={invoice_data.get('vendor')}, "
                f"items={len(invoice_data.get('line_items', []))}, "
                f"total={invoice_data.get('total')}")
    return invoice_data


async def read_and_ingest(file_path: str) -> dict:
    """Read an invoice file with Vision, then normalize and store it."""
    from maillard.mcp.accounting.invoice_intake import ingest_invoice

    raw = await read_invoice_file(file_path)
    if "error" in raw:
        return raw
    return ingest_invoice(raw)


async def read_all_raw_invoices(directory: str | None = None) -> list[dict]:
    """Read all invoice files in the raw directory and ingest them."""
    raw_dir = Path(directory) if directory else Path(__file__).resolve().parent.parent.parent.parent / "data" / "invoices_raw"

    if not raw_dir.exists():
        return [{"error": f"Directory not found: {raw_dir}"}]

    supported = {".pdf", ".png", ".jpg", ".jpeg"}
    # Scan root + vendor subfolders
    files = [f for f in raw_dir.rglob("*") if f.is_file() and f.suffix.lower() in supported]

    if not files:
        return [{"error": "No invoice files found"}]

    logger.info(f"[INVOICE-READER] Processing {len(files)} files from {raw_dir}")
    results = []
    for f in sorted(files):
        result = await read_and_ingest(str(f))
        result["_file"] = f.name
        results.append(result)

    success = sum(1 for r in results if "error" not in r)
    logger.info(f"[INVOICE-READER] Done: {success}/{len(results)} invoices processed")
    return results


# ── CLI ──────────────────────────────────────────────────────────

async def _main():
    import sys
    from dotenv import load_dotenv
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    load_dotenv(project_root / ".env")

    if len(sys.argv) > 1:
        # Read a single file
        result = await read_and_ingest(sys.argv[1])
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        # Read all files in invoices_raw
        results = await read_all_raw_invoices()
        for r in results:
            status = "OK" if "error" not in r else "FAIL"
            vendor = r.get("vendor", r.get("error", "?"))
            total = r.get("invoice_total", "?")
            items = len(r.get("line_items", []))
            print(f"  [{status}] {r.get('_file', '?'):45s} -> {vendor} | {items} items | ${total}")


if __name__ == "__main__":
    asyncio.run(_main())
