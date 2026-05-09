from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from app.config import Settings
from app.extraction.openrouter_client import OpenRouterClient


CHAT_SYSTEM_PROMPT = """You are the interactive ESG consultant for a completed analysis session.
Answer the user's question using only the supplied analysis result, KPI extraction,
forecast, peer comparison, report text, and PDF context excerpt. Be practical,
specific, and honest about uncertainty. If the data is missing, say so and explain
what would be needed. Return valid JSON only: {"answer": "..."}. Do not invent
numbers. When yearly_records are present, reason across all years rather than
only the latest year."""


class ConsultantChatService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = OpenRouterClient(settings)

    async def answer(
        self,
        *,
        session: dict,
        message: str,
        model_override: str | None = None,
    ) -> str:
        if not session.get("result"):
            return "This session does not have a completed analysis yet. Generate a report first, then ask me about it."

        if not self.client.configured:
            return self._fallback_answer(session, message)

        payload = {
            "user_question": message,
            "analysis_result": session.get("result"),
            "prior_chat": session.get("chat_messages", [])[-8:],
            "pdf_context_excerpt": (session.get("context") or {}).get("text", "")[:12000],
        }
        try:
            data = await asyncio.wait_for(
                self.client.chat_json(
                    model=model_override or self.settings.openrouter_report_model,
                    system_prompt=CHAT_SYSTEM_PROMPT,
                    user_prompt=json.dumps(payload, ensure_ascii=False),
                    temperature=0.25,
                ),
                timeout=55,
            )
            return str(data.get("answer") or "I could not generate an answer for that question.")
        except Exception:
            return self._fallback_answer(session, message)

    def _fallback_answer(self, session: dict, message: str) -> str:
        result = session.get("result") or {}
        report = result.get("consultant_report") or {}
        extracted = result.get("extracted_kpis") or {}
        cluster = result.get("cluster") or {}

        if "risk" in message.lower():
            risks = report.get("risks") or []
            return "Key risks: " + " ".join(f"{index + 1}. {item}" for index, item in enumerate(risks[:3]))

        if "recommend" in message.lower() or "next" in message.lower():
            recs = report.get("recommendations") or []
            return "Recommended next steps: " + " ".join(f"{index + 1}. {item}" for index, item in enumerate(recs[:3]))

        company = extracted.get("company_name") or "the company"
        label = cluster.get("KMeans_cluster_label") or "the assigned cluster"
        return (
            f"{company} is mapped to {label}. "
            f"{report.get('executive_summary') or 'The completed analysis is available in the report panel.'}"
        )


def make_chat_message(role: str, content: str) -> dict:
    return {
        "role": role,
        "content": content,
        "at": datetime.now(timezone.utc).isoformat(),
    }
