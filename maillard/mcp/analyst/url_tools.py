"""
URL fetching and content extraction tools for the Analyst agent.

Allows the agent to read user-pasted URLs and extract market-relevant text.
Never used as a source-of-truth for prices — only as supporting context.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import httpx
from loguru import logger

_TIMEOUT = 15
_MAX_BODY = 500_000  # 500 KB of HTML — more than enough for article text
_MAX_CONTENT_CHARS = 4000  # truncated text sent to Claude


async def fetch_url_content(url: str) -> dict:
    """Fetch a URL and return structured content.

    Returns:
        {
            "url": str,
            "status_code": int,
            "title": str,
            "content": str,       # cleaned readable text
            "excerpt": str,       # first ~500 chars
            "fetched_at": str,
        }
        or {"url": str, "error": str} on failure.
    """
    logger.info(f"[URL-TOOL] fetching {url}")

    if not url or not url.startswith(("http://", "https://")):
        return {"url": url, "error": "Invalid URL — must start with http:// or https://"}

    try:
        async with httpx.AsyncClient(
            timeout=_TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": "MaillardIntelligenceBot/1.0 (market research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
        ) as client:
            resp = await client.get(url)

        logger.info(f"[URL-TOOL] {url} → HTTP {resp.status_code}")

        if resp.status_code != 200:
            return {
                "url": url,
                "error": f"HTTP {resp.status_code} — could not fetch URL.",
                "status_code": resp.status_code,
            }

        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return {
                "url": url,
                "error": f"Not an HTML page (content-type: {content_type})",
                "status_code": resp.status_code,
            }

        raw_html = resp.text[:_MAX_BODY]
        title, text = _parse_html(raw_html)
        text = text[:_MAX_CONTENT_CHARS]
        excerpt = text[:500]

        return {
            "url": url,
            "status_code": resp.status_code,
            "title": title,
            "content": text,
            "excerpt": excerpt,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    except httpx.TimeoutException:
        logger.warning(f"[URL-TOOL] timeout fetching {url}")
        return {"url": url, "error": "Request timed out."}
    except httpx.ConnectError:
        logger.warning(f"[URL-TOOL] connection failed for {url}")
        return {"url": url, "error": "Could not connect to the server."}
    except Exception as e:
        logger.error(f"[URL-TOOL] unexpected error fetching {url}: {e}")
        return {"url": url, "error": f"Fetch failed: {str(e)[:200]}"}


async def extract_market_relevant_text(url: str) -> dict:
    """Fetch a URL and return only market-relevant excerpts.

    Wraps fetch_url_content and annotates whether the page appears to be
    a financial/commodity page.
    """
    result = await fetch_url_content(url)
    if "error" in result:
        return result

    content = result.get("content", "")
    lower = content.lower()

    # Detect if this is a finance/commodity page
    finance_signals = [
        "coffee futures", "arabica", "robusta", "commodity", "KC",
        "ICE", "futures price", "cents per pound", "cents/lb",
        "trading", "market data", "stock", "bloomberg", "reuters",
        "investing.com", "tradingview", "barchart",
    ]
    score = sum(1 for sig in finance_signals if sig.lower() in lower)
    is_finance = score >= 2

    result["is_finance_page"] = is_finance
    result["finance_signal_count"] = score

    # Detect if this is a designated reference-benchmark source
    is_reference = "investing.com" in url.lower()
    result["is_reference_source"] = is_reference

    # Try to extract a displayed price from the page content
    extracted_price = _extract_displayed_price(content)
    if extracted_price is not None:
        result["extracted_price"] = extracted_price
        result["extracted_price_unit"] = "cents/lb (estimated from page)"
        logger.info(f"[URL-TOOL] extracted price {extracted_price} from {url}")

    if is_reference:
        result["context_note"] = (
            "REFERENCE BENCHMARK SOURCE (Investing.com). "
            "Use this displayed price as the primary reference for Coffee C market context. "
            "If the internal API feed differs materially (>5%), flag a feed conflict."
        )
    elif is_finance:
        result["context_note"] = (
            "This URL appears to be a financial/commodity page. "
            "Use as supporting context — not a price source-of-truth."
        )
    else:
        result["context_note"] = (
            "This URL does not appear to be a finance page. "
            "Summarize its content for the user but do not extract prices from it."
        )

    return result


def _extract_displayed_price(text: str) -> float | None:
    """Try to extract a coffee futures price from page text.

    Looks for patterns like "395.50", "3.9550" near coffee/futures keywords.
    Returns the value in cents/lb if found, or None.
    """
    # Look for a price number near coffee-related context
    # Investing.com typically shows something like "395.50" or "Coffee C 395.50"
    patterns = [
        # "395.50" or "3,955.0" style — near "coffee" or "KC" context
        r'(?:coffee|arabica|KC|Coffee\s*C)[^\d]{0,40}([\d,]+\.[\d]{1,2})',
        # Price first, then context
        r'([\d,]+\.[\d]{1,2})[^\d]{0,40}(?:cents|¢|c/lb|USX|usx)',
        # Standalone large number that looks like cents/lb (100-800 range)
        r'\b(\d{3}\.[\d]{1,2})\b',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                # Sanity check: coffee C futures in cents/lb are typically 100-800
                if 50 < val < 900:
                    return round(val, 2)
                # Could be $/lb format (e.g. 3.95)
                if 0.5 < val < 9.0:
                    return round(val * 100, 2)
            except ValueError:
                continue
    return None


# ── URL detection helper (used by orchestrator) ─────────────────────────────

_URL_PATTERN = re.compile(
    r'https?://[^\s<>\"\'\)\]]+',
    re.IGNORECASE,
)


def extract_urls(text: str) -> list[str]:
    """Extract all http/https URLs from a text string."""
    return _URL_PATTERN.findall(text)


# ── HTML parsing ────────────────────────────────────────────────────────────

def _parse_html(html: str) -> tuple[str, str]:
    """Parse HTML and return (title, readable_text).

    Uses BeautifulSoup if available, falls back to regex stripping.
    """
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # Extract title
        title = soup.title.string.strip() if soup.title and soup.title.string else ""

        # Remove noise elements
        for tag in soup(["script", "style", "nav", "footer", "header",
                         "aside", "noscript", "iframe", "svg", "form"]):
            tag.decompose()

        # Get text
        text = soup.get_text(separator="\n", strip=True)

        # Collapse excessive whitespace
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        text = "\n".join(lines)

        return title, text

    except ImportError:
        logger.warning("[URL-TOOL] BeautifulSoup not installed — using regex fallback")
        return _parse_html_regex(html)


def _parse_html_regex(html: str) -> tuple[str, str]:
    """Fallback HTML parser using regex (no bs4)."""
    # Title
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    title = m.group(1).strip() if m else ""

    # Strip tags
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return title, text
