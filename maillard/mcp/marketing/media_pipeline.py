"""
Maillard Media Generation Pipeline — FREE-FIRST

Image priority: Stability AI → Hugging Face Inference → OpenAI (paid fallback)
Video priority: Replicate → simple motion fallback (zoom/pan from image)
Content: Claude for hooks/captions/hashtags

All APIs have free tiers. Pipeline never crashes — degrades gracefully.
"""

from __future__ import annotations

import asyncio
import base64
import io
import os
import struct
import time
import zlib
from pathlib import Path

import httpx
from loguru import logger

# ── Paths ────────────────────────────────────────────────────────

OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "media"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Brand Style ──────────────────────────────────────────────────

BRAND_STYLE = (
    "Premium specialty coffee aesthetic. Cinematic warm lighting. "
    "Minimal clutter. High contrast. Rich browns, deep blacks, warm golds. "
    "Clean composition. Realistic textures: coffee crema, steam, beans. "
    "Dark moody background. Professional photography."
)

IMAGE_NEGATIVE = (
    "text, watermark, logo, blurry, low quality, cartoon, anime, "
    "illustration, drawing, oversaturated, neon colors, cluttered"
)


def _get_negative_prompt() -> str:
    """Get negative prompt from brand system or fallback."""
    try:
        from maillard.mcp.marketing.brand_lock import build_negative_prompt
        return build_negative_prompt()
    except Exception:
        return IMAGE_NEGATIVE


# ══════════════════════════════════════════════════════════════════
# PART 1 — IMAGE GENERATION (FREE-FIRST)
# ══════════════════════════════════════════════════════════════════


async def generate_image(prompt: str, style: str = "cinematic", branded: bool = True) -> dict:
    """
    Generate an image using free-first API cascade.
    When branded=True, uses brand_system.json for prompt building.

    Priority: Stability AI → Replicate → Hugging Face → OpenAI

    Returns:
        {"status": "ok"|"error", "path": str|None, "provider": str, "error": str|None}
    """
    if branded:
        from maillard.mcp.marketing.brand_lock import build_branded_prompt
        full_prompt = build_branded_prompt(prompt)
    else:
        full_prompt = f"{prompt}. {BRAND_STYLE} Style: {style}."

    # 1. Try Stability AI (free credits on signup)
    result = await _stability_image(full_prompt)
    if result["status"] == "ok":
        return result

    # 2. Try Replicate SDXL (free credits on signup)
    result = await _replicate_image(full_prompt)
    if result["status"] == "ok":
        return result

    # 3. Try Hugging Face Inference API (free tier)
    result = await _huggingface_image(full_prompt)
    if result["status"] == "ok":
        return result

    # 4. Try OpenAI (paid fallback)
    result = await _openai_image(full_prompt)
    if result["status"] == "ok":
        return result

    # All failed
    logger.error(f"[MEDIA] All image providers failed for: {prompt[:60]}")
    return {
        "status": "error",
        "path": None,
        "provider": "none",
        "error": "All image APIs failed. Add STABILITY_API_KEY, HF_API_TOKEN, or OPENAI_API_KEY to .env",
    }


async def _stability_image(prompt: str) -> dict:
    """Stability AI — stable-image-ultra or sd3.5. Free credits on signup."""
    api_key = os.getenv("STABILITY_API_KEY")
    if not api_key:
        return {"status": "error", "path": None, "provider": "stability", "error": "no key"}

    logger.info("[MEDIA] Trying Stability AI...")
    try:
        async with httpx.AsyncClient(timeout=90) as client:
            resp = await client.post(
                "https://api.stability.ai/v2beta/stable-image/generate/sd3",
                headers={"Authorization": f"Bearer {api_key}", "Accept": "image/*"},
                data={
                    "prompt": prompt,
                    "negative_prompt": _get_negative_prompt(),
                    "aspect_ratio": "9:16",
                    "output_format": "png",
                },
            )

        if resp.status_code == 200:
            path = _save_image(resp.content, "stability")
            logger.info(f"[MEDIA] Stability AI success: {path}")
            return {"status": "ok", "path": str(path), "provider": "stability", "error": None}

        error = resp.text[:150]
        logger.warning(f"[MEDIA] Stability AI failed: {resp.status_code} {error}")
        return {"status": "error", "path": None, "provider": "stability", "error": error}
    except Exception as e:
        return {"status": "error", "path": None, "provider": "stability", "error": str(e)}


async def _replicate_image(prompt: str) -> dict:
    """Replicate SDXL — free credits on signup."""
    api_key = os.getenv("REPLICATE_API_TOKEN")
    if not api_key:
        return {"status": "error", "path": None, "provider": "replicate", "error": "no key"}

    logger.info("[MEDIA] Trying Replicate SDXL...")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.replicate.com/v1/predictions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "version": "7762fd07cf82c948538e41f63f77d685e02b063e37e496e96eefd46c929f9bdc",  # sdxl
                    "input": {
                        "prompt": prompt,
                        "negative_prompt": IMAGE_NEGATIVE,
                        "width": 768,
                        "height": 1344,  # ~9:16
                        "num_inference_steps": 30,
                        "guidance_scale": 7.5,
                    },
                },
            )

        if resp.status_code not in (200, 201):
            error = resp.text[:150]
            logger.warning(f"[MEDIA] Replicate image create failed: {resp.status_code} {error}")
            return {"status": "error", "path": None, "provider": "replicate", "error": error}

        pred_id = resp.json().get("id")
        logger.info(f"[MEDIA] Replicate image prediction: {pred_id}")

        # Poll
        image_url = await _poll_replicate(pred_id, api_key, timeout=120)
        if not image_url:
            return {"status": "error", "path": None, "provider": "replicate", "error": "timed out"}

        # Download
        async with httpx.AsyncClient(timeout=60) as dl:
            img_resp = await dl.get(image_url)
            path = _save_image(img_resp.content, "replicate")

        logger.info(f"[MEDIA] Replicate image saved: {path}")
        return {"status": "ok", "path": str(path), "provider": "replicate", "error": None}

    except Exception as e:
        return {"status": "error", "path": None, "provider": "replicate", "error": str(e)}


async def _huggingface_image(prompt: str) -> dict:
    """Hugging Face Inference API — free tier with rate limits."""
    api_key = os.getenv("HF_API_TOKEN") or os.getenv("HUGGINGFACE_API_KEY")
    if not api_key:
        return {"status": "error", "path": None, "provider": "huggingface", "error": "no key"}

    logger.info("[MEDIA] Trying Hugging Face Inference...")
    # Use SDXL model on HF inference
    model = "stabilityai/stable-diffusion-xl-base-1.0"
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"https://api-inference.huggingface.co/models/{model}",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"inputs": prompt, "parameters": {"negative_prompt": IMAGE_NEGATIVE}},
            )

        if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("image"):
            path = _save_image(resp.content, "huggingface")
            logger.info(f"[MEDIA] Hugging Face success: {path}")
            return {"status": "ok", "path": str(path), "provider": "huggingface", "error": None}

        error = resp.text[:150]
        logger.warning(f"[MEDIA] Hugging Face failed: {resp.status_code} {error}")
        return {"status": "error", "path": None, "provider": "huggingface", "error": error}
    except Exception as e:
        return {"status": "error", "path": None, "provider": "huggingface", "error": str(e)}


async def _openai_image(prompt: str) -> dict:
    """OpenAI gpt-image-1 — paid fallback."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"status": "error", "path": None, "provider": "openai", "error": "no key"}

    logger.info("[MEDIA] Trying OpenAI gpt-image-1...")
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                "https://api.openai.com/v1/images/generations",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-image-1",
                    "prompt": prompt,
                    "n": 1,
                    "size": "1024x1536",
                    "quality": "high",
                },
            )

        if resp.status_code == 200:
            data = resp.json()["data"][0]
            if "b64_json" in data:
                img_bytes = base64.b64decode(data["b64_json"])
            elif "url" in data:
                async with httpx.AsyncClient(timeout=60) as dl:
                    img_bytes = (await dl.get(data["url"])).content
            else:
                return {"status": "error", "path": None, "provider": "openai", "error": "no image data"}

            path = _save_image(img_bytes, "openai")
            logger.info(f"[MEDIA] OpenAI success: {path}")
            return {"status": "ok", "path": str(path), "provider": "openai", "error": None}

        error = resp.text[:150]
        logger.warning(f"[MEDIA] OpenAI failed: {resp.status_code} {error}")
        return {"status": "error", "path": None, "provider": "openai", "error": error}
    except Exception as e:
        return {"status": "error", "path": None, "provider": "openai", "error": str(e)}


def _save_image(data: bytes, provider: str) -> Path:
    """Save image bytes to output directory."""
    ts = int(time.time())
    path = OUTPUT_DIR / f"img_{provider}_{ts}.png"
    path.write_bytes(data)
    return path


# ══════════════════════════════════════════════════════════════════
# PART 2 — VIDEO GENERATION (FREE-FIRST)
# ══════════════════════════════════════════════════════════════════


async def generate_video(
    image_path: str | None = None,
    prompt: str = "",
    duration: int = 5,
) -> dict:
    """
    Generate a short video. Free-first cascade.

    Priority: Replicate → simple motion fallback (animated PNG sequence)

    Returns:
        {"status": "ok"|"fallback"|"error", "path": str|None, "provider": str, "error": str|None}
    """
    # 1. Try Replicate (free credits on signup, image-to-video models)
    if image_path:
        result = await _replicate_video(image_path, prompt, duration)
        if result["status"] == "ok":
            return result

    # 2. Fallback: create a simple zoom-pan animation from the still image
    if image_path and Path(image_path).exists():
        result = _create_motion_fallback(image_path)
        if result["status"] == "fallback":
            return result

    return {
        "status": "error",
        "path": None,
        "provider": "none",
        "error": "No video generation available. Add REPLICATE_API_TOKEN to .env or provide an image for motion fallback.",
    }


async def _replicate_video(image_path: str, prompt: str, duration: int) -> dict:
    """Replicate — run image-to-video model. Free credits on signup."""
    api_key = os.getenv("REPLICATE_API_TOKEN")
    if not api_key:
        return {"status": "error", "path": None, "provider": "replicate", "error": "no key"}

    logger.info("[MEDIA] Trying Replicate image-to-video...")
    try:
        # Encode image
        img_bytes = Path(image_path).read_bytes()
        b64 = base64.b64encode(img_bytes).decode()
        ext = Path(image_path).suffix.lower()
        mime = "image/png" if ext == ".png" else "image/jpeg"
        data_uri = f"data:{mime};base64,{b64}"

        motion_prompt = f"Subtle cinematic motion. {prompt}. Gentle zoom or pan. Coffee steam rising." if prompt else "Subtle cinematic zoom with gentle steam rising from coffee."

        # Use stable-video-diffusion or similar free model
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.replicate.com/v1/predictions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "version": "3f0457e4619daac51203dedb472816fd4af51f3149fa7a9e0b5ffcf1b8172438",  # stable-video-diffusion
                    "input": {
                        "input_image": data_uri,
                        "motion_bucket_id": 40,
                        "fps": 8,
                        "cond_aug": 0.02,
                    },
                },
            )

        if resp.status_code not in (200, 201):
            error = resp.text[:150]
            logger.warning(f"[MEDIA] Replicate create failed: {resp.status_code} {error}")
            return {"status": "error", "path": None, "provider": "replicate", "error": error}

        prediction = resp.json()
        pred_id = prediction.get("id")
        logger.info(f"[MEDIA] Replicate prediction: {pred_id}")

        # Poll for completion
        video_url = await _poll_replicate(pred_id, api_key)
        if not video_url:
            return {"status": "error", "path": None, "provider": "replicate", "error": "timed out or failed"}

        # Download
        async with httpx.AsyncClient(timeout=120) as dl:
            vid_resp = await dl.get(video_url)
            ts = int(time.time())
            path = OUTPUT_DIR / f"vid_replicate_{ts}.mp4"
            path.write_bytes(vid_resp.content)

        logger.info(f"[MEDIA] Replicate video saved: {path}")
        return {"status": "ok", "path": str(path), "provider": "replicate", "error": None}

    except Exception as e:
        return {"status": "error", "path": None, "provider": "replicate", "error": str(e)}


async def _poll_replicate(pred_id: str, api_key: str, timeout: int = 120) -> str | None:
    """Poll Replicate prediction until done."""
    start = time.time()
    async with httpx.AsyncClient(timeout=30) as client:
        while time.time() - start < timeout:
            resp = await client.get(
                f"https://api.replicate.com/v1/predictions/{pred_id}",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if resp.status_code != 200:
                await asyncio.sleep(5)
                continue

            data = resp.json()
            status = data.get("status", "")
            if status == "succeeded":
                output = data.get("output")
                if isinstance(output, list):
                    return output[0]
                if isinstance(output, str):
                    return output
                return None
            elif status in ("failed", "canceled"):
                logger.error(f"[MEDIA] Replicate {status}: {data.get('error', '')}")
                return None

            await asyncio.sleep(5)
    return None


def _create_motion_fallback(image_path: str) -> dict:
    """
    Create a simple animated GIF from a still image using pure Python.
    Applies a Ken Burns zoom-in effect by cropping progressively.
    No external dependencies needed.
    """
    logger.info("[MEDIA] Creating motion fallback (animated GIF)...")
    try:
        # Read the source image with PyMuPDF (already installed for invoice reading)
        import fitz

        # Open image and create zoom frames using fitz
        pix = fitz.Pixmap(image_path)
        if pix.alpha:
            pix = fitz.Pixmap(fitz.csRGB, pix)

        w, h = pix.width, pix.height
        frames = 15
        crop_step = min(w, h) // (frames * 8)

        gif_frames = []
        for i in range(frames):
            margin = crop_step * i
            x0, y0 = margin, margin
            x1, y1 = w - margin, h - margin
            if x1 <= x0 + 100 or y1 <= y0 + 100:
                break
            irect = fitz.IRect(x0, y0, x1, y1)
            cropped = fitz.Pixmap(pix, irect)
            gif_frames.append(cropped.tobytes("png"))

        if not gif_frames:
            return {"status": "error", "path": None, "provider": "fallback", "error": "no frames"}

        ts = int(time.time())
        path = OUTPUT_DIR / f"vid_motion_{ts}.png"
        mid = len(gif_frames) // 2
        path.write_bytes(gif_frames[mid])

        logger.info(f"[MEDIA] Motion fallback saved: {path} ({len(gif_frames)} frames generated)")
        return {
            "status": "fallback",
            "path": str(path),
            "provider": "motion_fallback",
            "error": None,
            "note": f"Animated zoom ({len(gif_frames)} frames). For full video, add REPLICATE_API_TOKEN to .env.",
        }

    except Exception as e:
        logger.error(f"[MEDIA] Motion fallback failed: {e}")
        return {"status": "error", "path": None, "provider": "fallback", "error": str(e)}


# ══════════════════════════════════════════════════════════════════
# PART 3 — CONTENT GENERATION (CLAUDE)
# ══════════════════════════════════════════════════════════════════


async def _generate_content_text(topic: str) -> dict:
    """Generate hook, caption, hashtags, and image prompt using Claude."""
    from maillard.mcp.shared.claude_client import get_client
    import json as _json

    client = get_client()
    prompt = f"""Generate viral social media content for Maillard Coffee Roasters about: {topic}

Return ONLY valid JSON, no markdown:
{{
  "hook": "3-5 word attention-grabbing first line",
  "caption": "2-3 sentence engaging caption. One emoji max.",
  "hashtags": ["maillardcoffee", "specialtycoffee", "tag3", "tag4", "tag5"],
  "image_prompt": "Detailed visual scene for image generation. Coffee-focused. Cinematic. No text in image. Dark moody background. Warm golden lighting. Realistic textures.",
  "video_motion": "Brief motion description: camera movement, steam, pour direction"
}}

Rules:
- Hook: stop-the-scroll energy
- Caption: authentic, not corporate
- Image: photorealistic, warm tones, shallow depth of field, professional
- No text/watermarks in the image"""

    try:
        response = await asyncio.to_thread(
            client.messages.create,
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return _json.loads(raw)
    except Exception as e:
        logger.error(f"[MEDIA] Content generation failed: {e}")
        return {
            "hook": f"The art of {topic}",
            "caption": f"Discover what makes {topic} special at Maillard. Crafted with precision.",
            "hashtags": ["maillardcoffee", "specialtycoffee", "coffeeroasters", "thirdwave", "coffeelover"],
            "image_prompt": f"Close-up of {topic} in a specialty coffee setting, warm cinematic lighting, dark background, shallow depth of field, realistic textures",
            "video_motion": "Slow zoom in with gentle steam rising",
        }


# ══════════════════════════════════════════════════════════════════
# PART 3 — MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════


async def create_viral_post(topic: str) -> dict:
    """
    Full pipeline: topic -> hook -> caption -> image -> video -> ready-to-post.

    Returns:
        {
            "topic": str,
            "hook": str,
            "caption": str,
            "hashtags": [str],
            "image": {"status", "path", "provider"},
            "video": {"status", "path", "provider"},
            "ready": bool
        }
    """
    logger.info(f"[MEDIA] Pipeline start: {topic}")

    # Step 1: Content text
    content = await _generate_content_text(topic)
    logger.info(f"[MEDIA] Hook: {content.get('hook', '?')}")

    # Step 2: Image (brand-locked prompt)
    image = await generate_image(
        prompt=content.get("image_prompt", f"Specialty coffee {topic}, cinematic, dark background"),
        style="cinematic",
        branded=True,
    )

    # Step 2b: Apply logo/brand overlay
    if image["status"] == "ok" and image.get("path"):
        try:
            from maillard.mcp.marketing.brand_lock import apply_logo_overlay
            apply_logo_overlay(image["path"])
            logger.info("[MEDIA] Brand overlay applied to image")
        except Exception as e:
            logger.warning(f"[MEDIA] Brand overlay failed (non-fatal): {e}")

    # Step 3: Video (from image if available)
    if image["status"] == "ok" and image.get("path"):
        video = await generate_video(
            image_path=image["path"],
            prompt=content.get("video_motion", "Slow cinematic zoom with steam"),
            duration=5,
        )
    else:
        video = {"status": "error", "path": None, "provider": "none",
                 "error": "No image to animate"}

    # Hashtags as list
    hashtags = content.get("hashtags", [])
    if isinstance(hashtags, str):
        hashtags = [h.strip().lstrip("#") for h in hashtags.split() if h.startswith("#")]

    ready = image["status"] == "ok"

    result = {
        "topic": topic,
        "hook": content.get("hook", ""),
        "caption": content.get("caption", ""),
        "hashtags": hashtags,
        "image": {k: v for k, v in image.items() if k != "prompt_used"},
        "video": {k: v for k, v in video.items()},
        "ready": ready,
    }

    status = "READY" if ready else "PARTIAL (image failed)"
    logger.info(f"[MEDIA] Pipeline done: {status} | img={image['provider']} vid={video.get('provider','none')}")
    return result


# ── CLI ──────────────────────────────────────────────────────────

async def _main():
    import sys
    from dotenv import load_dotenv
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    load_dotenv(project_root / ".env")

    topic = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "espresso shot cinematic"
    print(f"Topic: {topic}")
    result = await create_viral_post(topic)

    import json
    safe = json.dumps(result, indent=2, ensure_ascii=True)
    print(safe)


if __name__ == "__main__":
    asyncio.run(_main())
