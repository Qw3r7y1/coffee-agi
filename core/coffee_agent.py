import asyncio
import json
import os
from datetime import datetime
from typing import Optional

import anthropic
from loguru import logger

from core.prompts import COFFEE_AGI_SYSTEM_PROMPT

# ── George's persistent memory ────────────────────────────────────────────────
GEORGE_MEMORY_FILE = "data/memory/george.json"


def _load_george_memory() -> list[str]:
    try:
        with open(GEORGE_MEMORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("facts", [])
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"Failed to load George's memory: {e}")
        return []


def _save_george_memory(facts: list[str]) -> None:
    os.makedirs(os.path.dirname(GEORGE_MEMORY_FILE), exist_ok=True)
    try:
        with open(GEORGE_MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump({"facts": facts, "last_updated": datetime.utcnow().isoformat()}, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save George's memory: {e}")


class CoffeeAgent:
    def __init__(self, kb):
        self.client = anthropic.Anthropic()
        self.kb = kb
        self.sessions: dict[str, list] = {}
        # Load George's memory once on startup
        self._george_facts: list[str] = _load_george_memory()
        if self._george_facts:
            logger.info(f"Loaded {len(self._george_facts)} memories for George.")

    # ── Public API ────────────────────────────────────────────────────────────

    async def chat(
        self,
        message: str,
        session_id: str = "default",
        student_email: Optional[str] = None,
        image_b64: Optional[str] = None,
        image_type: str = "image/jpeg",
    ) -> str:
        if session_id not in self.sessions:
            self.sessions[session_id] = []

        # RAG: pull relevant curriculum context
        context_docs = self.kb.search(message, n_results=4)

        system = COFFEE_AGI_SYSTEM_PROMPT

        # Inject George's long-term memory into the system prompt
        if self._george_facts:
            memory_block = "\n".join(f"- {f}" for f in self._george_facts)
            system += (
                f"\n\n=== WHAT YOU KNOW ABOUT GEORGE (persistent memory) ===\n"
                f"{memory_block}\n"
                f"=== END MEMORY ===\n"
                f"Use this to personalise your responses. Do not repeat these facts back "
                f"to him verbatim unless relevant — just let them inform your advice."
            )

        if context_docs:
            context_blocks = [
                f"[Source: {d['source']} | Topic: {d['topic']} | Level: {d['difficulty']}]\n{d['text']}"
                for d in context_docs
            ]
            system += f"\n\n=== CURRICULUM CONTEXT ===\n{chr(10).join(context_blocks)}\n=== END CONTEXT ==="

        history = self.sessions[session_id]

        # Build user content — list when image present, plain string otherwise
        if image_b64:
            user_content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": image_type,
                        "data": image_b64,
                    },
                },
                {"type": "text", "text": message or "What do you see in this image? Analyze it from a specialty coffee perspective."},
            ]
            history.append({"role": "user", "content": [{"type": "text", "text": f"[Image] {message}"}]})
        else:
            user_content = message
            history.append({"role": "user", "content": message})

        api_messages = history[:-1] + [{"role": "user", "content": user_content}]

        try:
            response = self.client.messages.create(
                model="claude-opus-4-6",
                max_tokens=2048,
                system=system,
                messages=api_messages,
            )
            reply = response.content[0].text
        except Exception as e:
            logger.error(f"Claude API error: {e}")
            reply = "I'm having trouble connecting right now. Please check your API key and try again."

        history.append({"role": "assistant", "content": reply})

        # Keep last 20 messages
        if len(history) > 20:
            self.sessions[session_id] = history[-20:]

        # Asynchronously extract and persist any new facts George shared
        asyncio.create_task(self._extract_and_save_memory(message))

        return reply

    def clear_session(self, session_id: str):
        self.sessions.pop(session_id, None)

    # ── Memory extraction ─────────────────────────────────────────────────────

    async def _extract_and_save_memory(self, user_message: str) -> None:
        """
        Ask Claude whether the user's message contains anything worth remembering
        about George personally. If yes, merge new facts into the persistent store.
        """
        if not user_message.strip():
            return

        existing = "\n".join(f"- {f}" for f in self._george_facts) if self._george_facts else "(none yet)"

        prompt = (
            "You are a memory extractor for a personal AI assistant used exclusively by George.\n\n"
            f"EXISTING MEMORIES:\n{existing}\n\n"
            f"NEW MESSAGE FROM GEORGE:\n\"{user_message}\"\n\n"
            "Extract any new personal facts George just revealed about himself: his preferences, "
            "equipment, goals, experience level, favourite coffees, dislikes, habits, location, "
            "or anything else that would help personalise future responses.\n\n"
            "Rules:\n"
            "- Only extract facts explicitly stated or clearly implied by George.\n"
            "- Do NOT duplicate facts already in EXISTING MEMORIES.\n"
            "- If there is nothing new to remember, return an empty JSON array.\n"
            "- Return ONLY a JSON array of short fact strings, no markdown:\n"
            '[\"fact 1\", \"fact 2\"]'
        )

        try:
            resp = await asyncio.to_thread(
                self.client.messages.create,
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip().strip("`").strip()
            if raw.startswith("json"):
                raw = raw[4:].strip()
            new_facts: list[str] = json.loads(raw)
            if not isinstance(new_facts, list):
                return
            new_facts = [f.strip() for f in new_facts if isinstance(f, str) and f.strip()]
            if new_facts:
                self._george_facts.extend(new_facts)
                _save_george_memory(self._george_facts)
                logger.info(f"Saved {len(new_facts)} new memory facts for George: {new_facts}")
        except Exception as e:
            logger.debug(f"Memory extraction skipped: {e}")
