import os
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pydantic import BaseModel

load_dotenv(override=True)

from certs.certificate_generator import CertificateGenerator
from core.coffee_agent import CoffeeAgent
from ingestion.knowledge_base import KnowledgeBase
from tests_engine.test_generator import TestGenerator

# ── Ensure brand folder structure exists ──────────────────────────────────────
for _d in ["data/maillard/logos", "data/maillard/images", "data/maillard/fonts", "data/maillard/guidelines"]:
    os.makedirs(_d, exist_ok=True)

# ── Services ──────────────────────────────────────────────────────────────────

kb = KnowledgeBase()
agent = CoffeeAgent(kb)
test_gen = TestGenerator(kb)
cert_gen = CertificateGenerator()


# ── App ───────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Coffee AGI starting — knowledge base loaded.")
    yield
    logger.info("Coffee AGI shutting down.")


app = FastAPI(
    title="Coffee AGI",
    description="Specialty Coffee Artificial General Intelligence by Maillard Coffee Roasters",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# ── Dropbox helpers ───────────────────────────────────────────────────────────

def _dropbox_direct(url: str) -> str:
    """Convert any Dropbox share URL to a direct-download URL."""
    # Replace dl=0 → dl=1, or append dl=1
    if "dropbox.com" in url:
        if "dl=0" in url:
            url = url.replace("dl=0", "dl=1")
        elif "dl=1" not in url:
            url = url + ("&dl=1" if "?" in url else "?dl=1")
        # www.dropbox.com → dl.dropboxusercontent.com works too, but dl=1 is enough
    return url


async def _download_url(url: str, timeout: int = 60) -> tuple[bytes, str]:
    """Download a URL and return (content, filename)."""
    direct = _dropbox_direct(url)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(direct)
    if r.status_code != 200:
        raise HTTPException(502, f"Download failed: HTTP {r.status_code}")
    # Try to get filename from Content-Disposition header
    cd = r.headers.get("content-disposition", "")
    filename = ""
    if "filename=" in cd:
        filename = cd.split("filename=")[-1].strip().strip('"').strip("'")
    if not filename:
        filename = url.split("?")[0].rstrip("/").split("/")[-1] or "file"
    return r.content, filename


# ── Request / Response Models ──────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = "default"
    student_email: Optional[str] = None
    image_b64: Optional[str] = None
    image_type: str = "image/jpeg"


class TTSRequest(BaseModel):
    text: str
    voice_id: str = "21m00Tcm4TlvDq8ikWAM"  # Rachel — clear, natural English


class TestRequest(BaseModel):
    topic: str
    difficulty: str = "foundation"
    num_questions: int = 10
    student_email: str


class SubmitRequest(BaseModel):
    test_id: str
    student_email: str
    student_name: str
    answers: dict[str, str]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/ui", tags=["UI"], response_class=HTMLResponse)
def ui():
    """Serve the Coffee AGI web interface."""
    with open("frontend/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/", tags=["Health"])
def root():
    return {
        "service": "Coffee AGI",
        "status": "online",
        "docs": "/docs",
        "by": "Maillard Coffee Roasters",
    }


@app.post("/chat", tags=["Chat"])
async def chat(req: ChatRequest):
    """Chat with Coffee AGI — powered by Claude with curriculum RAG."""
    response = await agent.chat(req.message, req.session_id, req.student_email, req.image_b64, req.image_type)
    return {"response": response, "session_id": req.session_id}


@app.delete("/chat/{session_id}", tags=["Chat"])
def clear_chat(session_id: str):
    """Clear a conversation session."""
    agent.clear_session(session_id)
    return {"cleared": session_id}


@app.post("/tts", tags=["Voice"])
async def tts(req: TTSRequest):
    """Convert text to speech via ElevenLabs. Returns audio/mpeg bytes."""
    api_key = os.getenv("ELEVENLABS_API_KEY", "")
    if not api_key:
        raise HTTPException(503, "ELEVENLABS_API_KEY not set.")

    # Truncate very long responses to keep TTS snappy (first ~800 chars)
    text = req.text[:800] if len(req.text) > 800 else req.text

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{req.voice_id}"
    payload = {
        "text": text,
        "model_id": "eleven_monolingual_v1",
        "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
    }
    headers = {
        "xi-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "audio/mpeg",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json=payload, headers=headers)

    if r.status_code != 200:
        logger.error(f"ElevenLabs error {r.status_code}: {r.text[:200]}")
        raise HTTPException(502, f"ElevenLabs error: {r.status_code}")

    return Response(content=r.content, media_type="audio/mpeg")


# ── Knowledge ─────────────────────────────────────────────────────────────────

@app.post("/knowledge/ingest", tags=["Knowledge"])
async def ingest(
    file: UploadFile = File(...),
    topic_tag: str = Form("general"),
    difficulty_level: str = Form("foundation"),
):
    """Upload a curriculum document (PDF, PPTX, TXT, MD) into the vector knowledge base."""
    if not file.filename:
        raise HTTPException(400, "No filename provided.")
    content = await file.read()
    result = await kb.ingest_file(file.filename, content, topic_tag, difficulty_level)
    if "error" in result:
        raise HTTPException(422, result["error"])
    return result


@app.get("/knowledge/topics", tags=["Knowledge"])
def get_topics():
    """List all topics and chunk counts in the knowledge base."""
    return kb.get_topics()


# ── Tests ─────────────────────────────────────────────────────────────────────

@app.post("/tests/generate", tags=["Tests"])
async def generate_test(req: TestRequest):
    """Generate an AI exam on any coffee topic at a chosen difficulty level."""
    if req.num_questions < 1 or req.num_questions > 30:
        raise HTTPException(400, "num_questions must be between 1 and 30.")
    difficulty = req.difficulty.lower()
    if difficulty not in ("foundation", "intermediate", "advanced", "expert"):
        raise HTTPException(400, "difficulty must be: foundation, intermediate, advanced, or expert.")
    result = await test_gen.generate(req.topic, difficulty, req.num_questions, req.student_email)
    if "error" in result:
        raise HTTPException(500, result["error"])
    return result


@app.post("/tests/submit", tags=["Tests"])
async def submit_test(req: SubmitRequest):
    """Submit exam answers for AI grading. Returns score, feedback, and certificate if passing."""
    result = await test_gen.grade(req.test_id, req.answers, req.student_email, req.student_name)
    if "error" in result:
        raise HTTPException(404, result["error"])

    # Auto-generate certificate on pass
    if result.get("passed"):
        cert = cert_gen.generate(
            student_name=req.student_name,
            student_email=req.student_email,
            topic=result["topic"],
            score=result["score"],
            certificate_track=result.get("certificate_track", result["topic"]),
        )
        if "error" not in cert:
            result["certificate"] = cert
        else:
            logger.warning(f"Certificate generation failed: {cert['error']}")

    return result


@app.get("/tests/results/{email}", tags=["Tests"])
def get_results(email: str):
    """Get all test results for a student by email."""
    return {"email": email, "results": test_gen.get_student_results(email)}


# ── Certificates ──────────────────────────────────────────────────────────────

@app.get("/certificates/student/{email}", tags=["Certificates"])
def get_student_certs(email: str):
    """List all certificates earned by a student."""
    return {"email": email, "certificates": cert_gen.get_student_certificates(email)}


@app.get("/certificates/{cert_id}", tags=["Certificates"])
def get_cert(cert_id: str):
    """Get certificate metadata by ID."""
    cert = cert_gen.get_certificate(cert_id)
    if not cert:
        raise HTTPException(404, "Certificate not found.")
    return cert


@app.get("/certificates/{cert_id}/download", tags=["Certificates"])
def download_cert(cert_id: str):
    """Download the PDF certificate file."""
    cert = cert_gen.get_certificate(cert_id)
    if not cert:
        raise HTTPException(404, "Certificate not found.")
    filepath = cert.get("file_path", "")
    if not os.path.exists(filepath):
        raise HTTPException(404, "Certificate file not found on disk.")
    return FileResponse(
        filepath,
        media_type="application/pdf",
        filename=f"certificate_{cert_id[:8]}.pdf",
    )


# ── Maillard Brand ────────────────────────────────────────────────────────────

BRAND_ROOT = Path("data/maillard")
ALLOWED_ASSET_TYPES = {
    ".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif", ".heic",  # images / logos
    ".pdf", ".txt", ".md",                              # guidelines / docs
    ".ttf", ".otf", ".woff", ".woff2",                  # fonts
}


@app.post("/maillard/upload", tags=["Brand"])
async def upload_brand_asset(
    file: UploadFile = File(...),
    folder: str = Form("images"),  # logos | images | fonts | guidelines
):
    """Upload a Maillard brand asset (logo, image, font, or guidelines doc)."""
    allowed_folders = {"logos", "images", "fonts", "guidelines"}
    if folder not in allowed_folders:
        raise HTTPException(400, f"folder must be one of: {', '.join(sorted(allowed_folders))}")

    filename = Path(file.filename).name if file.filename else "asset"
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_ASSET_TYPES:
        raise HTTPException(400, f"File type '{ext}' not allowed.")

    dest = BRAND_ROOT / folder / filename
    content = await file.read()
    dest.write_bytes(content)
    logger.info(f"Brand asset saved: {dest}")

    # If it's a guidelines doc, also ingest it into the knowledge base
    # so the agent can reference brand voice, colours, etc. in chat
    if ext in {".pdf", ".txt", ".md"}:
        await kb.ingest_file(filename, content, topic_tag="maillard-brand", difficulty_level="foundation")

    return {
        "saved": str(dest),
        "url": f"/brand/{folder}/{filename}",
        "ingested_to_kb": ext in {".pdf", ".txt", ".md"},
    }


@app.get("/maillard/assets", tags=["Brand"])
def list_brand_assets():
    """List all Maillard brand assets, grouped by folder."""
    result: dict[str, list] = {}
    for folder in ["logos", "images", "fonts", "guidelines"]:
        folder_path = BRAND_ROOT / folder
        files = []
        if folder_path.exists():
            for f in sorted(folder_path.iterdir()):
                if f.is_file():
                    files.append({
                        "name": f.name,
                        "url": f"/brand/{folder}/{f.name}",
                        "bytes": f.stat().st_size,
                    })
        result[folder] = files
    return result


@app.post("/maillard/import", tags=["Brand"])
async def import_brand_from_url(
    url: str = Form(...),
    folder: str = Form("images"),
):
    """Import a brand asset directly from a Dropbox (or any public) URL."""
    allowed_folders = {"logos", "images", "fonts", "guidelines"}
    if folder not in allowed_folders:
        raise HTTPException(400, f"folder must be one of: {', '.join(sorted(allowed_folders))}")

    content, filename = await _download_url(url)
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_ASSET_TYPES:
        raise HTTPException(400, f"File type '{ext}' not allowed.")

    dest = BRAND_ROOT / folder / filename
    dest.write_bytes(content)
    logger.info(f"Brand asset imported from URL → {dest}")

    ingested = False
    if ext in {".pdf", ".txt", ".md"}:
        await kb.ingest_file(filename, content, topic_tag="maillard-brand", difficulty_level="foundation")
        ingested = True

    return {
        "saved": str(dest),
        "url": f"/brand/{folder}/{filename}",
        "bytes": len(content),
        "ingested_to_kb": ingested,
    }


@app.post("/george/import-certificates", tags=["George"])
async def import_george_certificates(url: str = Form(...)):
    """
    Download George's SCA certificate PDF from a Dropbox (or any public) URL,
    extract the certificate info with Claude, and save it to his persistent memory.
    """
    import json
    import pypdf
    import io

    content, filename = await _download_url(url)

    # Save the raw PDF
    os.makedirs("data/george", exist_ok=True)
    dest = Path("data/george") / filename
    dest.write_bytes(content)
    logger.info(f"George's certificates PDF saved: {dest}")

    # Extract text from PDF
    try:
        reader = pypdf.PdfReader(io.BytesIO(content))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        raise HTTPException(422, f"Could not read PDF: {e}")

    if not text.strip():
        raise HTTPException(422, "PDF contains no extractable text.")

    # Ask Claude to pull out the certificate details
    import anthropic
    client = anthropic.Anthropic()
    prompt = (
        "The following text was extracted from George's SCA (Specialty Coffee Association) "
        "certificate document(s). Extract every certificate he holds.\n\n"
        f"PDF TEXT:\n{text[:6000]}\n\n"
        "Return ONLY a JSON array of objects — no markdown:\n"
        '[{"name": "SCA Barista Skills Foundation", "level": "Foundation", "issued": "2023-05", "issuer": "SCA"}, ...]'
    )
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = resp.content[0].text.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()

    try:
        certs = json.loads(raw)
    except Exception:
        raise HTTPException(500, f"Could not parse certificates from PDF. Raw extract: {raw[:300]}")

    # Build memory facts from the certificates
    new_facts = []
    for c in certs:
        cert_name = c.get("name", "Unknown certificate")
        level = c.get("level", "")
        issued = c.get("issued", "")
        fact = f"SCA Certificate: {cert_name}"
        if level:
            fact += f" ({level})"
        if issued:
            fact += f" — issued {issued}"
        new_facts.append(fact)

    # Merge into George's memory (avoid duplicates)
    from core.coffee_agent import _load_george_memory, _save_george_memory
    existing = _load_george_memory()
    added = []
    for f in new_facts:
        if f not in existing:
            existing.append(f)
            added.append(f)
    _save_george_memory(existing)

    # Also reload the agent's in-memory facts
    agent._george_facts = existing
    logger.info(f"Added {len(added)} SCA certificate facts to George's memory.")

    return {
        "certificates_found": len(certs),
        "certificates": certs,
        "facts_added_to_memory": added,
    }


# ── Dropbox Sync ──────────────────────────────────────────────────────────────
# Rules: READ-ONLY from Dropbox. Never delete or overwrite any Dropbox file.
# Local files that already exist are also never overwritten (safe_save).

from core.dropbox_client import DropboxClient
_dbx = DropboxClient()

# Dropbox folder structure:
#   /Maillard/brand/        → logos, fonts, guidelines
#   /Maillard/certificates/ → George's SCA certs + business licenses
#   /Maillard/invoices/     → financial documents (list only, never synced locally)
#
# Local sub-folder mapping inside data/maillard/
_BRAND_SUBFOLDER_MAP = {
    "logos": "logos",
    "images": "images",
    "fonts": "fonts",
    "guidelines": "guidelines",
    # flat files land in images/
}


@app.get("/dropbox/status", tags=["Dropbox"])
async def dropbox_status():
    """Confirm Dropbox connection is live and show configured folders."""
    if not _dbx.is_configured():
        return {"configured": False, "message": "Add DROPBOX_ACCESS_TOKEN to .env"}
    # Ping Dropbox by listing the root — if it fails, token is invalid
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                "https://api.dropboxapi.com/2/users/get_current_account",
                headers={"Authorization": f"Bearer {_dbx._access_token}"},
            )
        if r.status_code == 200:
            account = r.json()
            return {
                "configured": True,
                "account": account.get("name", {}).get("display_name", ""),
                "email": account.get("email", ""),
                "brand_folder": _dbx.brand_folder,
                "george_folder": _dbx.george_folder,
            }
        return {"configured": False, "message": f"Token invalid: HTTP {r.status_code}"}
    except Exception as e:
        return {"configured": False, "message": str(e)}


@app.get("/dropbox/list", tags=["Dropbox"])
async def dropbox_list(path: str = ""):
    """List files in any Dropbox folder. Defaults to the brand folder. Read-only."""
    if not _dbx.is_configured():
        raise HTTPException(503, "Dropbox not configured. Add DROPBOX_ACCESS_TOKEN to .env")
    folder = path or _dbx.brand_folder
    try:
        entries = await _dbx.list_folder(folder)
        return {"folder": folder, "count": len(entries), "entries": entries}
    except Exception as e:
        raise HTTPException(502, str(e))



@app.get("/dropbox/preview", tags=["Dropbox"])
async def dropbox_preview(path: str):
    """Stream a file directly from Dropbox (read-only). Used for image thumbnails in the UI."""
    if not _dbx.is_configured():
        raise HTTPException(503, "Dropbox not configured.")
    try:
        data = await _dbx.download_file(path)
    except Exception as e:
        raise HTTPException(502, str(e))
    ext = Path(path).suffix.lower()
    mime_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".heic": "image/heic",
        ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp",
        ".pdf": "application/pdf",
    }
    media_type = mime_map.get(ext, "application/octet-stream")
    return Response(content=data, media_type=media_type)


@app.post("/dropbox/sync-brand", tags=["Dropbox"])
async def dropbox_sync_brand(path: str = Form("")):
    """
    Download brand assets from Dropbox → data/maillard/.
    Supports sub-folders: logos/ images/ fonts/ guidelines/
    Rules: never overwrites existing local files, never touches Dropbox originals.
    Guidelines docs (PDF/TXT/MD) are also ingested into the knowledge base.
    """
    if not _dbx.is_configured():
        raise HTTPException(503, "Dropbox not configured. Add DROPBOX_ACCESS_TOKEN to .env")

    root = path or _dbx.brand_folder
    saved, skipped_type, skipped_exists, ingested = [], [], [], []

    try:
        entries = await _dbx.list_folder(root)
    except Exception as e:
        raise HTTPException(502, str(e))

    async def _sync_file(dbx_path: str, name: str, local_folder: str):
        ext = Path(name).suffix.lower()
        if ext not in ALLOWED_ASSET_TYPES:
            skipped_type.append(name)
            return
        dest = BRAND_ROOT / local_folder / name
        content = await _dbx.download_file(dbx_path)
        if not _dbx.safe_save(dest, content):   # ← never overwrites
            skipped_exists.append(name)
            return
        saved.append(str(dest))
        if ext in {".pdf", ".txt", ".md"}:
            await kb.ingest_file(name, content, topic_tag="maillard-brand", difficulty_level="foundation")
            ingested.append(name)

    for entry in entries:
        if entry["is_dir"]:
            local = _BRAND_SUBFOLDER_MAP.get(entry["name"].lower(), "images")
            for sub in await _dbx.list_folder(entry["path"]):
                if not sub["is_dir"]:
                    await _sync_file(sub["path"], sub["name"], local)
        else:
            await _sync_file(entry["path"], entry["name"], "images")

    logger.info(f"Brand sync: {len(saved)} saved, {len(skipped_exists)} already existed")
    return {
        "saved": saved,
        "skipped_unsupported_type": skipped_type,
        "skipped_already_exists": skipped_exists,
        "ingested_to_kb": ingested,
    }


@app.post("/dropbox/sync-george", tags=["Dropbox"])
async def dropbox_sync_george(path: str = Form("")):
    """
    Download PDFs from George's Dropbox certificates folder, extract SCA cert info,
    and save to his persistent memory.
    Rules: never overwrites existing local files, never touches Dropbox originals.
    """
    import json as _json
    import io
    import pypdf
    import anthropic as _anthropic

    if not _dbx.is_configured():
        raise HTTPException(503, "Dropbox not configured. Add DROPBOX_ACCESS_TOKEN to .env")

    folder = path or _dbx.george_folder
    try:
        entries = await _dbx.list_folder(folder)
    except Exception as e:
        raise HTTPException(502, str(e))

    os.makedirs("data/george", exist_ok=True)
    all_certs, all_added = [], []

    for entry in entries:
        if entry["is_dir"] or not entry["name"].lower().endswith(".pdf"):
            continue
        content = await _dbx.download_file(entry["path"])
        dest = Path("data/george") / entry["name"]
        _dbx.safe_save(dest, content)  # ← never overwrites existing local copy

        try:
            reader = pypdf.PdfReader(io.BytesIO(content))
            text = "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception:
            continue
        if not text.strip():
            continue

        client = _anthropic.Anthropic()
        prompt = (
            "Extract every SCA certificate from the following PDF text belonging to George.\n\n"
            f"TEXT:\n{text[:6000]}\n\n"
            "Return ONLY a JSON array — no markdown:\n"
            '[{"name": "SCA Barista Skills Foundation", "level": "Foundation", "issued": "2023-05"}]'
        )
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip().strip("`").strip()
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
        try:
            certs = _json.loads(raw)
        except Exception:
            continue
        all_certs.extend(certs)

    # Merge into George's memory
    from core.coffee_agent import _load_george_memory, _save_george_memory
    existing = _load_george_memory()
    for c in all_certs:
        fact = f"SCA Certificate: {c.get('name','')}"
        if c.get("level"):
            fact += f" ({c['level']})"
        if c.get("issued"):
            fact += f" — issued {c['issued']}"
        if fact not in existing:
            existing.append(fact)
            all_added.append(fact)
    _save_george_memory(existing)
    agent._george_facts = existing

    return {"certificates_found": len(all_certs), "certificates": all_certs, "facts_added": all_added}


# Mount AFTER API routes so /brand/assets etc. aren't shadowed
app.mount("/brand", StaticFiles(directory="data/maillard"), name="brand")

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
