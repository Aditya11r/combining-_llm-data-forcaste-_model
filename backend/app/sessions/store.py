from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.config import Settings
from app.schemas import SessionSummary


class SessionStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.settings.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.settings.upload_dir.mkdir(parents=True, exist_ok=True)

    def create(self, filename: str) -> dict[str, Any]:
        now = _now()
        session = {
            "session_id": uuid4().hex,
            "created_at": now,
            "updated_at": now,
            "filename": filename,
            "events": [],
            "artifacts": {},
        }
        self.save(session)
        return session

    def save(self, session: dict[str, Any]) -> None:
        session["updated_at"] = _now()
        path = self.path_for(session["session_id"])
        path.write_text(json.dumps(session, indent=2, ensure_ascii=False), encoding="utf-8")

    def append_event(self, session: dict[str, Any], event_type: str, payload: dict[str, Any]) -> None:
        session.setdefault("events", []).append(
            {
                "at": _now(),
                "type": event_type,
                "payload": payload,
            }
        )
        self.save(session)

    def get(self, session_id: str) -> dict[str, Any]:
        path = self.path_for(session_id)
        if not path.exists():
            raise FileNotFoundError(session_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def delete(self, session_id: str) -> None:
        session = self.get(session_id)
        for artifact_path in session.get("artifacts", {}).values():
            path = self._safe_runtime_path(artifact_path)
            if path and path.exists():
                path.unlink()

        path = self.path_for(session_id)
        if path.exists():
            path.unlink()

    def list(self) -> list[SessionSummary]:
        summaries: list[SessionSummary] = []
        paths = sorted(self.settings.sessions_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
        for path in paths:
            data = json.loads(path.read_text(encoding="utf-8"))
            result = data.get("result") or {}
            extracted = result.get("extracted_kpis") or {}
            cluster = result.get("cluster") or {}
            quality = result.get("extraction_quality") or {}
            summaries.append(
                SessionSummary(
                    session_id=data["session_id"],
                    created_at=data["created_at"],
                    updated_at=data["updated_at"],
                    filename=data.get("filename") or "",
                    company_name=extracted.get("company_name"),
                    fiscal_year=extracted.get("fiscal_year"),
                    cluster_label=cluster.get("KMeans_cluster_label"),
                    quality_level=quality.get("level"),
                )
            )
        return summaries

    def path_for(self, session_id: str):
        return self.settings.sessions_dir / f"{session_id}.json"

    def _safe_runtime_path(self, value: str) -> Path | None:
        try:
            runtime_root = self.settings.runtime_dir.resolve()
            artifact = Path(value).resolve()
        except Exception:
            return None

        try:
            artifact.relative_to(runtime_root)
        except ValueError:
            return None

        return artifact


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
