import json
import uuid
from datetime import datetime

import anthropic
from loguru import logger

from core.prompts import (
    CERTIFICATION_TRACKS,
    COFFEE_AGI_SYSTEM_PROMPT,
    DIFFICULTY_DESCRIPTIONS,
)


class TestGenerator:
    def __init__(self, kb):
        self.client = anthropic.Anthropic()
        self.kb = kb
        self._tests: dict = {}
        self._results: dict[str, list] = {}

    async def generate(
        self,
        topic: str,
        difficulty: str,
        num_questions: int,
        student_email: str,
    ) -> dict:
        difficulty = difficulty.lower()
        diff_desc = DIFFICULTY_DESCRIPTIONS.get(difficulty, DIFFICULTY_DESCRIPTIONS["foundation"])

        context_docs = self.kb.search(f"{topic} {difficulty}", n_results=6)
        context = ""
        if context_docs:
            context = "\n\n".join(d["text"] for d in context_docs)

        prompt = f"""Create a specialty coffee exam on: "{topic}" — difficulty: {difficulty} ({diff_desc}).

Generate exactly {num_questions} questions. Mix of:
- Multiple choice (MC): 4 options A/B/C/D
- True/False (TF)
- Short answer (SA): expect 2-4 sentence answer, list 2-3 key points the answer must cover
{f'Base questions on this curriculum material:{chr(10)}{context[:3000]}' if context else ''}

Return ONLY valid JSON — no markdown, no extra text:
{{
  "topic": "{topic}",
  "difficulty": "{difficulty}",
  "questions": [
    {{
      "id": 1,
      "type": "MC",
      "question": "...",
      "options": {{"A": "...", "B": "...", "C": "...", "D": "..."}},
      "correct_answer": "A",
      "explanation": "..."
    }},
    {{
      "id": 2,
      "type": "TF",
      "question": "...",
      "correct_answer": "True",
      "explanation": "..."
    }},
    {{
      "id": 3,
      "type": "SA",
      "question": "...",
      "key_points": ["point 1", "point 2"],
      "explanation": "..."
    }}
  ]
}}"""

        try:
            response = self.client.messages.create(
                model="claude-opus-4-6",
                max_tokens=4096,
                system=COFFEE_AGI_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            raw = self._strip_markdown_json(raw)
            test_data = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error in test generation: {e}\nRaw: {raw[:500]}")
            return {"error": "Failed to parse generated test. Try again."}
        except Exception as e:
            logger.error(f"Test generation error: {e}")
            return {"error": str(e)}

        test_id = str(uuid.uuid4())
        test_data["test_id"] = test_id
        test_data["student_email"] = student_email
        test_data["created_at"] = datetime.utcnow().isoformat()
        self._tests[test_id] = test_data
        return test_data

    async def grade(
        self,
        test_id: str,
        answers: dict,
        student_email: str,
        student_name: str,
    ) -> dict:
        if test_id not in self._tests:
            return {"error": "Test not found. It may have expired — please generate a new one."}

        test = self._tests[test_id]
        questions = test["questions"]

        mc_tf_correct = 0
        mc_tf_total = 0
        sa_items = []
        graded = []

        for q in questions:
            qid = str(q["id"])
            student_answer = answers.get(qid, "").strip()

            if q["type"] in ("MC", "TF"):
                mc_tf_total += 1
                correct = student_answer.upper() == q["correct_answer"].strip().upper()
                if correct:
                    mc_tf_correct += 1
                graded.append(
                    {
                        "id": q["id"],
                        "type": q["type"],
                        "question": q["question"],
                        "correct": correct,
                        "student_answer": student_answer,
                        "correct_answer": q["correct_answer"],
                        "explanation": q.get("explanation", ""),
                    }
                )
            else:
                sa_items.append({"q": q, "answer": student_answer})

        # AI-grade short answers
        sa_graded = []
        if sa_items:
            sa_payload = [
                {
                    "question": item["q"]["question"],
                    "key_points": item["q"].get("key_points", []),
                    "student_answer": item["answer"],
                }
                for item in sa_items
            ]
            sa_prompt = (
                "You are grading a specialty coffee exam. Score each short answer 0-10 based on "
                "accuracy, completeness, and whether it covers the key points.\n\n"
                f"Questions and answers:\n{json.dumps(sa_payload, indent=2)}\n\n"
                "Return ONLY a JSON array — no markdown:\n"
                '[{"score": 8, "feedback": "Good explanation of..."}, ...]'
            )
            try:
                sa_resp = self.client.messages.create(
                    model="claude-opus-4-6",
                    max_tokens=1024,
                    messages=[{"role": "user", "content": sa_prompt}],
                )
                raw_sa = self._strip_markdown_json(sa_resp.content[0].text.strip())
                sa_scores = json.loads(raw_sa)
            except Exception as e:
                logger.warning(f"SA grading error: {e} — defaulting to 5/10")
                sa_scores = [{"score": 5, "feedback": "Could not auto-grade."} for _ in sa_items]

            for i, item in enumerate(sa_items):
                sc = sa_scores[i] if i < len(sa_scores) else {"score": 5, "feedback": ""}
                sa_graded.append(
                    {
                        "id": item["q"]["id"],
                        "type": "SA",
                        "question": item["q"]["question"],
                        "score": sc.get("score", 5),
                        "max_score": 10,
                        "feedback": sc.get("feedback", ""),
                        "student_answer": item["answer"],
                        "key_points": item["q"].get("key_points", []),
                    }
                )

        graded.extend(sa_graded)
        graded.sort(key=lambda x: x["id"])

        # Calculate overall percentage
        mc_pct = (mc_tf_correct / mc_tf_total * 100) if mc_tf_total else 100
        sa_total_score = sum(g["score"] for g in sa_graded)
        sa_max = len(sa_items) * 10
        sa_pct = (sa_total_score / sa_max * 100) if sa_max else 100

        total_q = len(questions)
        mc_weight = mc_tf_total / total_q if total_q else 0.7
        sa_weight = len(sa_items) / total_q if total_q else 0.3
        overall = round(mc_pct * mc_weight + sa_pct * sa_weight, 1)

        # Match to a certification track
        topic = test["topic"]
        cert_track = self._match_cert_track(topic)
        passing_score = CERTIFICATION_TRACKS.get(cert_track, {}).get("passing_score", 70)
        passed = overall >= passing_score

        result = {
            "test_id": test_id,
            "student_email": student_email,
            "student_name": student_name,
            "topic": topic,
            "difficulty": test["difficulty"],
            "score": overall,
            "passing_score": passing_score,
            "passed": passed,
            "mc_result": f"{mc_tf_correct}/{mc_tf_total}" if mc_tf_total else "N/A",
            "sa_result": f"{sa_total_score}/{sa_max}" if sa_max else "N/A",
            "graded_questions": graded,
            "certificate_track": cert_track,
            "completed_at": datetime.utcnow().isoformat(),
        }

        if student_email not in self._results:
            self._results[student_email] = []
        self._results[student_email].append(result)

        return result

    def get_student_results(self, email: str) -> list:
        return self._results.get(email, [])

    def _match_cert_track(self, topic: str) -> str:
        topic_lower = topic.lower()
        for track_name in CERTIFICATION_TRACKS:
            if any(word in track_name.lower() for word in topic_lower.split()):
                return track_name
        return topic

    @staticmethod
    def _strip_markdown_json(text: str) -> str:
        if text.startswith("```"):
            lines = text.split("\n")
            lines = lines[1:]  # remove ```json or ```
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            return "\n".join(lines)
        return text
