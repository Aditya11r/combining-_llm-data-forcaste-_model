from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import Settings


class OpenRouterClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    @property
    def configured(self) -> bool:
        return bool(self.settings.openrouter_api_key)

    async def chat_json(
        self,
        *,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.1,
    ) -> dict[str, Any]:
        if not self.configured:
            raise RuntimeError("OPENROUTER_API_KEY is not configured")

        url = f"{self.settings.openrouter_base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.settings.openrouter_api_key}",
            "HTTP-Referer": self.settings.openrouter_http_referer,
            "X-Title": self.settings.openrouter_app_title,
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "temperature": temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        async with httpx.AsyncClient(timeout=90) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        content = data["choices"][0]["message"]["content"]
        if isinstance(content, dict):
            return content
        if not content:
            raise RuntimeError("OpenRouter returned an empty JSON response")
        return json.loads(content)
