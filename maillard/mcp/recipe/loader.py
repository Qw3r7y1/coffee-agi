"""
RecipeLoader — reads every PDF in data/maillard/recipes/ at startup.

Provides:
  - BM25 search over recipe chunks
  - Full text access per source file
  - Module-level singleton (loaded once, shared everywhere)
"""
from __future__ import annotations

import os
from loguru import logger
from rank_bm25 import BM25Okapi

RECIPES_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "maillard", "recipes")
)


class RecipeLoader:
    """
    Loads all PDFs from data/maillard/recipes/ on init.
    Builds an in-memory BM25 index over recipe chunks for fast lookup.
    """

    def __init__(self):
        self._sources: dict[str, str] = {}   # filename → full extracted text
        self._chunks: list[dict] = []         # [{text, source}]
        self._bm25: BM25Okapi | None = None
        self._load_all()

    # ── Public API ────────────────────────────────────────────────────────────

    def search(self, query: str, n: int = 5) -> list[dict]:
        """BM25 search over recipe chunks. Returns [{text, source}]."""
        if not self._chunks or self._bm25 is None:
            return []
        tokens = query.lower().split()
        scores = self._bm25.get_scores(tokens)
        ranked = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        return [
            self._chunks[i]
            for i in ranked[:n]
            if scores[i] > 0
        ]

    def get_full_text(self) -> str:
        """All recipe PDFs concatenated with source headers."""
        return "\n\n---\n\n".join(
            f"[Source: {fname}]\n{text}"
            for fname, text in self._sources.items()
        )

    def get_sources(self) -> list[str]:
        return list(self._sources.keys())

    def total_chunks(self) -> int:
        return len(self._chunks)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _load_all(self):
        if not os.path.isdir(RECIPES_DIR):
            logger.warning(f"[RECIPE-LOADER] recipes dir not found: {RECIPES_DIR}")
            return

        for fname in os.listdir(RECIPES_DIR):
            if fname.lower().endswith(".pdf"):
                path = os.path.join(RECIPES_DIR, fname)
                text = self._read_pdf(path)
                if text:
                    self._sources[fname] = text
                    logger.info(f"[RECIPE-LOADER] '{fname}': {len(text)} chars extracted")
                else:
                    logger.warning(f"[RECIPE-LOADER] no text extracted from '{fname}'")

        # Build BM25 index over chunks
        for fname, text in self._sources.items():
            for chunk in self._chunk(text):
                self._chunks.append({"text": chunk, "source": fname})

        if self._chunks:
            self._bm25 = BM25Okapi([c["text"].lower().split() for c in self._chunks])
            logger.info(
                f"[RECIPE-LOADER] indexed {len(self._chunks)} chunks "
                f"from {len(self._sources)} PDF(s)"
            )

    def _read_pdf(self, path: str) -> str:
        # 1. Direct pypdf extraction
        try:
            import pypdf
            with open(path, "rb") as f:
                reader = pypdf.PdfReader(f)
                text = "\n".join(page.extract_text() or "" for page in reader.pages)
            if text.strip():
                return text.strip()
        except Exception as e:
            logger.warning(f"[RECIPE-LOADER] pypdf failed for {path}: {e}")

        # 2. Fallback: pull from the existing KnowledgeBase (already OCR'd)
        try:
            from ingestion.knowledge_base import KnowledgeBase
            kb = KnowledgeBase()
            basename = os.path.basename(path)
            chunks = [d["text"] for d in kb._docs if basename in d.get("source", "")]
            if chunks:
                logger.info(f"[RECIPE-LOADER] using KB fallback for '{path}': {len(chunks)} chunks")
                return "\n\n".join(chunks)
        except Exception as e:
            logger.warning(f"[RECIPE-LOADER] KB fallback failed: {e}")

        return ""

    @staticmethod
    def _chunk(text: str, size: int = 350, overlap: int = 40) -> list[str]:
        words = text.split()
        step = max(size - overlap, 1)
        chunks = []
        for i in range(0, len(words), step):
            chunk = " ".join(words[i : i + size])
            if chunk.strip():
                chunks.append(chunk)
        return chunks


# ── Module-level singleton ────────────────────────────────────────────────────

_loader: RecipeLoader | None = None


def get_loader() -> RecipeLoader:
    global _loader
    if _loader is None:
        _loader = RecipeLoader()
    return _loader
