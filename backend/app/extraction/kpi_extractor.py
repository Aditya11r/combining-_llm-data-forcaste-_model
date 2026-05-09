from __future__ import annotations

import re
from typing import Any

from pydantic import ValidationError

from app.config import Settings
from app.extraction.openrouter_client import OpenRouterClient
from app.schemas import ExtractedKpiPayload, ExtractionQuality, YearlyKpiRecord


CLUSTERING_REQUIRED_FIELDS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
    "sector",
    "sub_sector",
    "states_served",
    "countries_served",
]

FORECAST_REQUIRED_FIELDS = [
    "company_name",
    "fiscal_year_start",
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
]


EXTRACTION_SYSTEM_PROMPT = """You extract ESG/BRSR KPI data from PDF context.
Return only valid JSON. Do not invent values. If a value is not supported by the
context, return null and add a short warning. Extract every fiscal year that
contains scope 1, scope 2, water, or waste evidence into yearly_records, and use
the latest fiscal year as the top-level KPI fields. Normalize units to:
tCO2e for emissions, kL for water, tonnes for waste, integer year for
fiscal_year_start. Keep sector and sub_sector as short business labels."""


def build_extraction_prompt(context: str, detected_years: list[str], target_years: list[str]) -> str:
    trimmed_context = context[:45000]
    return f"""
Detected fiscal years: {detected_years}
Target KPI fiscal years: {target_years}

Extract this JSON shape:
{{
  "company_name": string | null,
  "fiscal_year": string | null,
  "fiscal_year_start": integer | null,
  "sector": string | null,
  "sub_sector": string | null,
  "states_served": number | null,
  "countries_served": number | null,
  "scope1_tco2e": number | null,
  "scope2_tco2e": number | null,
  "water_consumption_kl": number | null,
  "waste_generated_tonnes": number | null,
  "waste_recycled_tonnes": number | null,
  "total_scope1_scope2_tco2e": number | null,
  "reporting_boundary": string | null,
  "evidence": {{
    "field_name": "short quote or page reference"
  }},
  "yearly_records": [
    {{
      "fiscal_year": string | null,
      "fiscal_year_start": integer | null,
      "scope1_tco2e": number | null,
      "scope2_tco2e": number | null,
      "water_consumption_kl": number | null,
      "waste_generated_tonnes": number | null,
      "waste_recycled_tonnes": number | null,
      "total_scope1_scope2_tco2e": number | null,
      "evidence": {{"field_name": "short quote or page reference"}}
    }}
  ],
  "missing_fields": ["field_name"],
  "warnings": ["short warning"]
}}

PDF context:
{trimmed_context}
"""


class KpiExtractor:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = OpenRouterClient(settings)

    async def extract(
        self,
        *,
        context: str,
        detected_years: list[str],
        target_years: list[str],
        model_override: str | None = None,
    ) -> tuple[ExtractedKpiPayload, ExtractionQuality, dict[str, Any]]:
        if self.client.configured:
            try:
                payload = await self.client.chat_json(
                    model=model_override or self.settings.openrouter_extraction_model,
                    system_prompt=EXTRACTION_SYSTEM_PROMPT,
                    user_prompt=build_extraction_prompt(context, detected_years, target_years),
                )
            except Exception as exc:
                payload = self._heuristic_extract(context, detected_years)
                payload["warnings"].append(f"OpenRouter extraction failed, so heuristic extraction was used: {exc}")
        else:
            payload = self._heuristic_extract(context, detected_years)
            payload["warnings"].append("OpenRouter API key is not configured, so heuristic extraction was used.")

        try:
            extracted = ExtractedKpiPayload.model_validate(payload)
        except ValidationError as exc:
            extracted = ExtractedKpiPayload(warnings=[f"LLM JSON failed validation: {exc}"])

        extracted = self._normalize_totals(extracted)
        quality = score_extraction(extracted)
        return extracted, quality, payload

    def _normalize_totals(self, extracted: ExtractedKpiPayload) -> ExtractedKpiPayload:
        if extracted.total_scope1_scope2_tco2e is None:
            if extracted.scope1_tco2e is not None and extracted.scope2_tco2e is not None:
                extracted.total_scope1_scope2_tco2e = extracted.scope1_tco2e + extracted.scope2_tco2e

        for record in extracted.yearly_records:
            if record.total_scope1_scope2_tco2e is None:
                if record.scope1_tco2e is not None and record.scope2_tco2e is not None:
                    record.total_scope1_scope2_tco2e = record.scope1_tco2e + record.scope2_tco2e

        extracted.yearly_records = sorted(
            extracted.yearly_records,
            key=lambda record: record.fiscal_year_start or 0,
        )

        latest_record = None
        if extracted.yearly_records:
            latest_record = max(extracted.yearly_records, key=lambda record: record.fiscal_year_start or 0)

        if latest_record and latest_record.fiscal_year_start:
            extracted.fiscal_year = extracted.fiscal_year or latest_record.fiscal_year
            extracted.fiscal_year_start = extracted.fiscal_year_start or latest_record.fiscal_year_start
            for field in [
                "scope1_tco2e",
                "scope2_tco2e",
                "water_consumption_kl",
                "waste_generated_tonnes",
                "waste_recycled_tonnes",
                "total_scope1_scope2_tco2e",
            ]:
                if getattr(extracted, field) is None:
                    setattr(extracted, field, getattr(latest_record, field))

        if not extracted.yearly_records and extracted.fiscal_year_start:
            extracted.yearly_records = [
                YearlyKpiRecord(
                    fiscal_year=extracted.fiscal_year,
                    fiscal_year_start=extracted.fiscal_year_start,
                    scope1_tco2e=extracted.scope1_tco2e,
                    scope2_tco2e=extracted.scope2_tco2e,
                    water_consumption_kl=extracted.water_consumption_kl,
                    waste_generated_tonnes=extracted.waste_generated_tonnes,
                    waste_recycled_tonnes=extracted.waste_recycled_tonnes,
                    total_scope1_scope2_tco2e=extracted.total_scope1_scope2_tco2e,
                )
            ]
        return extracted

    def _heuristic_extract(self, context: str, detected_years: list[str]) -> dict[str, Any]:
        def number_after(pattern: str) -> float | None:
            match = re.search(pattern, context, flags=re.IGNORECASE | re.DOTALL)
            if not match:
                return None
            value = re.sub(r"[^0-9.\-]", "", match.group(1))
            try:
                return float(value)
            except ValueError:
                return None

        fiscal_year = detected_years[0] if detected_years else None
        fiscal_year_start = None
        if fiscal_year:
            year_match = re.search(r"(20\d{2})", fiscal_year)
            if year_match:
                fiscal_year_start = int(year_match.group(1))

        return {
            "company_name": None,
            "fiscal_year": fiscal_year,
            "fiscal_year_start": fiscal_year_start,
            "sector": None,
            "sub_sector": None,
            "states_served": None,
            "countries_served": None,
            "scope1_tco2e": number_after(r"scope\s*1[^0-9]{0,80}([0-9][0-9,.\s]*)"),
            "scope2_tco2e": number_after(r"scope\s*2[^0-9]{0,80}([0-9][0-9,.\s]*)"),
            "water_consumption_kl": number_after(r"water\s+consumption[^0-9]{0,120}([0-9][0-9,.\s]*)"),
            "waste_generated_tonnes": number_after(r"waste\s+generated[^0-9]{0,120}([0-9][0-9,.\s]*)"),
            "waste_recycled_tonnes": number_after(r"waste\s+recycled[^0-9]{0,120}([0-9][0-9,.\s]*)"),
            "total_scope1_scope2_tco2e": None,
            "reporting_boundary": None,
            "evidence": {},
            "yearly_records": [],
            "missing_fields": [],
            "warnings": ["Heuristic extraction was used."],
        }


def score_extraction(extracted: ExtractedKpiPayload) -> ExtractionQuality:
    required = list(dict.fromkeys(CLUSTERING_REQUIRED_FIELDS + FORECAST_REQUIRED_FIELDS))
    missing = [field for field in required if getattr(extracted, field) in (None, "")]
    present = len(required) - len(missing)
    score = round(present / len(required), 3)
    notes: list[str] = []

    if extracted.total_scope1_scope2_tco2e is None:
        notes.append("Total scope 1 + scope 2 could not be computed.")
    if extracted.warnings:
        notes.extend(extracted.warnings)

    if score >= 0.85:
        level = "high"
    elif score >= 0.55:
        level = "medium"
    else:
        level = "low"

    return ExtractionQuality(
        score=score,
        level=level,
        missing_required_fields=missing,
        notes=notes,
    )
