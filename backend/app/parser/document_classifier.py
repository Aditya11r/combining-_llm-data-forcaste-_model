from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any

from app.config import Settings
from app.extraction.openrouter_client import OpenRouterClient


BRSR_CLASSIFIER_PROMPT = """You classify whether a PDF context is from an Indian BRSR
report or an annual report section containing BRSR disclosures.
Accept only BRSR / Business Responsibility and Sustainability Report content.
Do not accept generic sustainability reports, invoices, resumes, research papers,
financial statements, brochures, or unrelated PDFs. Return valid JSON only:
{"is_brsr": true|false, "confidence": "low|medium|high", "reason": "..."}."""


@dataclass
class DocumentValidationResult:
    accepted: bool
    score: float
    confidence: str
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def brsr_rejection_message(result: DocumentValidationResult) -> str:
    found = result.evidence.get("matched_markers") or []
    suffix = f" Evidence found: {', '.join(found[:5])}." if found else ""
    return (
        "This PDF does not look like a BRSR report. Please upload a BRSR / "
        "Business Responsibility and Sustainability Report PDF."
        f"{suffix}"
    )


async def validate_brsr_document(context: str, *, settings: Settings) -> DocumentValidationResult:
    heuristic = _heuristic_brsr_check(context)
    if heuristic.confidence == "high":
        return heuristic

    client = OpenRouterClient(settings)
    if not client.configured:
        return heuristic

    try:
        llm_result = await asyncio.wait_for(
            client.chat_json(
                model=settings.openrouter_extraction_model,
                system_prompt=BRSR_CLASSIFIER_PROMPT,
                user_prompt=json.dumps(
                    {
                        "heuristic_result": heuristic.to_dict(),
                        "pdf_context_excerpt": context[:9000],
                    },
                    ensure_ascii=False,
                ),
                temperature=0,
            ),
            timeout=25,
        )
    except Exception:
        return heuristic

    llm_accepts = bool(llm_result.get("is_brsr"))
    llm_confidence = str(llm_result.get("confidence") or "medium").lower()
    if llm_confidence not in {"low", "medium", "high"}:
        llm_confidence = "medium"

    reason = str(llm_result.get("reason") or "")
    evidence = {
        **heuristic.evidence,
        "llm_reason": reason,
        "llm_confidence": llm_confidence,
    }

    if llm_accepts and heuristic.score >= 6:
        return DocumentValidationResult(
            accepted=True,
            score=heuristic.score,
            confidence=llm_confidence,
            reason=reason or "LLM classifier accepted the document as BRSR based on BRSR disclosure structure.",
            evidence=evidence,
        )

    return DocumentValidationResult(
        accepted=False,
        score=heuristic.score,
        confidence=llm_confidence,
        reason=reason or "The document does not contain enough BRSR-specific evidence.",
        evidence=evidence,
    )


def _heuristic_brsr_check(context: str) -> DocumentValidationResult:
    normalized = _normalize(context)
    matched: list[str] = []
    score = 0.0

    high_value_markers = [
        ("business responsibility and sustainability report", 12),
        ("business responsibility & sustainability report", 12),
        ("business responsibility sustainability report", 10),
        ("brsr", 8),
        ("brsr core", 8),
    ]
    structural_markers = [
        ("section a general disclosures", 4),
        ("section b management and process disclosures", 4),
        ("section c principle wise performance disclosure", 4),
        ("principle wise performance disclosure", 4),
        ("essential indicators", 3),
        ("leadership indicators", 3),
        ("national guidelines on responsible business conduct", 4),
        ("ngbrc", 4),
        ("ngrbc", 4),
        ("regulation 34 2 f", 2),
        ("listed entity", 2),
    ]
    kpi_markers = [
        ("scope 1", 1),
        ("scope 2", 1),
        ("greenhouse gas", 1),
        ("water consumption", 1),
        ("waste generated", 1),
        ("waste recycled", 1),
        ("energy consumed", 1),
        ("principle 6", 2),
    ]

    exact_hits = 0
    for marker, weight in high_value_markers:
        if _contains_marker(normalized, marker):
            exact_hits += 1
            score += weight
            matched.append(marker)

    structure_hits = 0
    for marker, weight in structural_markers:
        if _contains_marker(normalized, marker):
            structure_hits += 1
            score += weight
            matched.append(marker)

    kpi_hits = 0
    for marker, weight in kpi_markers:
        if _contains_marker(normalized, marker):
            kpi_hits += 1
            score += weight
            matched.append(marker)

    principle_count = len(set(re.findall(r"\bprinciple\s+([1-9])\b", normalized)))
    if principle_count:
        score += min(principle_count, 6)
        matched.append(f"{principle_count} BRSR principle marker(s)")

    if exact_hits and score >= 10:
        return _result(True, score, "high", "BRSR title/abbreviation was found.", matched, exact_hits, structure_hits, kpi_hits)

    if score >= 16 and structure_hits >= 2:
        return _result(True, score, "high", "BRSR section structure was found.", matched, exact_hits, structure_hits, kpi_hits)

    if score >= 12 and structure_hits >= 1 and kpi_hits >= 2:
        return _result(True, score, "high", "BRSR disclosure structure and ESG KPI evidence were found.", matched, exact_hits, structure_hits, kpi_hits)

    if score >= 10 and structure_hits >= 1 and "principle 6" in matched and kpi_hits >= 3:
        return _result(True, score, "medium", "BRSR Principle 6 KPI disclosure evidence was found.", matched, exact_hits, structure_hits, kpi_hits)

    if score < 8 or (not exact_hits and structure_hits == 0):
        return _result(False, score, "high", "Not enough BRSR-specific markers were found.", matched, exact_hits, structure_hits, kpi_hits)

    return _result(False, score, "medium", "The document is ambiguous and needs BRSR confirmation.", matched, exact_hits, structure_hits, kpi_hits)


def _result(
    accepted: bool,
    score: float,
    confidence: str,
    reason: str,
    matched: list[str],
    exact_hits: int,
    structure_hits: int,
    kpi_hits: int,
) -> DocumentValidationResult:
    return DocumentValidationResult(
        accepted=accepted,
        score=round(score, 2),
        confidence=confidence,
        reason=reason,
        evidence={
            "matched_markers": matched,
            "exact_marker_count": exact_hits,
            "structure_marker_count": structure_hits,
            "kpi_marker_count": kpi_hits,
        },
    )


def _normalize(text: str) -> str:
    lowered = text.lower()
    lowered = lowered.replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", " ", lowered)


def _contains_marker(normalized_text: str, marker: str) -> bool:
    normalized_marker = _normalize(marker)
    return f" {normalized_marker} " in f" {normalized_text} "
