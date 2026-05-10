from __future__ import annotations

import json
import math
import statistics
from collections import Counter
from dataclasses import dataclass
from typing import Any

from app.config import Settings
from app.extraction.openrouter_client import OpenRouterClient
from app.schemas import ExtractedKpiPayload, ImputedKpiField, YearlyKpiRecord


NUMERIC_IMPUTATION_FIELDS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
    "states_served",
    "countries_served",
]

FORECAST_IMPUTATION_FIELDS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
]

CATEGORICAL_IMPUTATION_FIELDS = ["sector", "sub_sector"]

IMPUTATION_SYSTEM_PROMPT = """You estimate missing ESG KPI fields for model input.
Use only the supplied PDF-extracted values and reference statistics. Do not
change values already extracted from the PDF. Do not use KMeans_cluster because
cluster assignment happens after this step. Return valid JSON only. If the
statistics are weak, still provide a conservative estimate and mark confidence
as low."""


@dataclass
class ImputationSummary:
    imputed_fields: list[ImputedKpiField]
    used_llm: bool


class ReferenceGroundedKpiImputer:
    def __init__(self, settings: Settings, reference_store):
        self.settings = settings
        self.reference_store = reference_store
        self.client = OpenRouterClient(settings)

    async def impute(self, extracted: ExtractedKpiPayload, *, context: str = "") -> ImputationSummary:
        missing_top_level = _missing_top_level_fields(extracted)
        missing_yearly = _missing_yearly_fields(extracted)
        if not missing_top_level and not missing_yearly:
            return ImputationSummary(imputed_fields=[], used_llm=False)

        reference = self._build_reference(extracted)
        prompt_payload = {
            "instructions": {
                "estimate_only_missing_fields": True,
                "do_not_modify_pdf_extracted_values": True,
                "do_not_use_kmeans_cluster": True,
                "purpose": "Complete model inputs for clustering and forecasting.",
            },
            "known_pdf_values": _known_values(extracted),
            "missing_top_level_fields": missing_top_level,
            "missing_yearly_fields": missing_yearly,
            "reference_stats": reference,
            "pdf_context_excerpt": context[:7000],
            "response_shape": {
                "top_level": {
                    "field_name": {
                        "value": "number or string",
                        "confidence": "low|medium|high",
                        "basis": "short explanation grounded in reference_stats",
                    }
                },
                "yearly_records": [
                    {
                        "fiscal_year_start": 2024,
                        "fields": {
                            "field_name": {
                                "value": "number",
                                "confidence": "low|medium|high",
                                "basis": "short explanation grounded in reference_stats",
                            }
                        },
                    }
                ],
            },
        }

        if self.client.configured:
            try:
                data = await self.client.chat_json(
                    model=self.settings.openrouter_report_model,
                    system_prompt=IMPUTATION_SYSTEM_PROMPT,
                    user_prompt=json.dumps(prompt_payload, ensure_ascii=False),
                    temperature=0.15,
                )
                fields = self._apply_llm_imputations(extracted, data, reference)
                if fields:
                    return ImputationSummary(imputed_fields=fields, used_llm=True)
            except Exception as exc:
                extracted.warnings.append(f"LLM reference-grounded imputation failed; statistic fallback used: {exc}")

        fields = self._apply_statistical_imputations(extracted, reference)
        return ImputationSummary(imputed_fields=fields, used_llm=False)

    def _build_reference(self, extracted: ExtractedKpiPayload) -> dict[str, Any]:
        if hasattr(self.reference_store, "imputation_reference_rows"):
            rows = self.reference_store.imputation_reference_rows(extracted, limit=500)
        else:
            rows = self.reference_store.cluster_forecast

        sector_rows = _filter_rows(rows, "sector", extracted.sector)
        sub_sector_rows = _filter_rows(rows, "sub_sector", extracted.sub_sector)
        nearest_rows = _nearest_numeric_rows(rows, extracted)

        return {
            "source": self.reference_store.__class__.__name__,
            "global": _summarize_rows(rows),
            "same_sector": {
                "label": extracted.sector,
                **_summarize_rows(sector_rows),
            },
            "same_sub_sector": {
                "label": extracted.sub_sector,
                **_summarize_rows(sub_sector_rows),
            },
            "similar_numeric_scale": _summarize_rows(nearest_rows),
        }

    def _apply_llm_imputations(
        self,
        extracted: ExtractedKpiPayload,
        data: dict[str, Any],
        reference: dict[str, Any],
    ) -> list[ImputedKpiField]:
        fields: list[ImputedKpiField] = []
        top_level = data.get("top_level") if isinstance(data, dict) else None
        if isinstance(top_level, dict):
            for field, payload in top_level.items():
                if field not in NUMERIC_IMPUTATION_FIELDS + CATEGORICAL_IMPUTATION_FIELDS:
                    continue
                if getattr(extracted, field, None) not in (None, ""):
                    continue
                estimate = _estimate_from_payload(payload)
                if estimate is None:
                    continue
                applied = _apply_value(extracted, field, estimate["value"])
                if applied is None:
                    continue
                fields.append(
                    ImputedKpiField(
                        field=field,
                        value=applied,
                        confidence=estimate["confidence"],
                        method="llm_reference_estimate",
                        basis=estimate["basis"],
                    )
                )

        yearly_records = data.get("yearly_records") if isinstance(data, dict) else None
        if isinstance(yearly_records, list):
            for item in yearly_records:
                if not isinstance(item, dict):
                    continue
                year = _safe_int(item.get("fiscal_year_start"))
                record = _yearly_record_for(extracted, year)
                if record is None:
                    continue
                fields_payload = item.get("fields")
                if not isinstance(fields_payload, dict):
                    continue
                for field, payload in fields_payload.items():
                    if field not in FORECAST_IMPUTATION_FIELDS:
                        continue
                    if getattr(record, field, None) is not None:
                        continue
                    estimate = _estimate_from_payload(payload)
                    if estimate is None:
                        continue
                    applied = _apply_value(record, field, estimate["value"])
                    if applied is None:
                        continue
                    fields.append(
                        ImputedKpiField(
                            field=field,
                            value=applied,
                            fiscal_year_start=record.fiscal_year_start,
                            confidence=estimate["confidence"],
                            method="llm_reference_estimate",
                            basis=estimate["basis"],
                        )
                    )

        _sync_totals_and_latest(extracted)
        if not fields:
            return self._apply_statistical_imputations(extracted, reference)

        extracted.imputed_fields.extend(fields)
        if _missing_top_level_fields(extracted) or _missing_yearly_fields(extracted):
            fields.extend(self._apply_statistical_imputations(extracted, reference))
        return fields

    def _apply_statistical_imputations(
        self,
        extracted: ExtractedKpiPayload,
        reference: dict[str, Any],
    ) -> list[ImputedKpiField]:
        fields: list[ImputedKpiField] = []
        for field in _missing_top_level_fields(extracted):
            value = _fallback_value(field, reference)
            if value is None:
                continue
            applied = _apply_value(extracted, field, value)
            if applied is None:
                continue
            fields.append(
                ImputedKpiField(
                    field=field,
                    value=applied,
                    confidence=_fallback_confidence(field, reference),
                    method="reference_statistic_estimate",
                    basis=_fallback_basis(field, reference),
                )
            )

        for record in extracted.yearly_records:
            if record.fiscal_year_start is None:
                continue
            for field in FORECAST_IMPUTATION_FIELDS:
                if getattr(record, field, None) is not None:
                    continue
                value = _fallback_value(field, reference)
                if value is None:
                    continue
                applied = _apply_value(record, field, value)
                if applied is None:
                    continue
                fields.append(
                    ImputedKpiField(
                        field=field,
                        value=applied,
                        fiscal_year_start=record.fiscal_year_start,
                        confidence=_fallback_confidence(field, reference),
                        method="reference_statistic_estimate",
                        basis=_fallback_basis(field, reference),
                    )
                )

        _sync_totals_and_latest(extracted)
        extracted.imputed_fields.extend(fields)
        return fields


def _missing_top_level_fields(extracted: ExtractedKpiPayload) -> list[str]:
    return [
        field
        for field in NUMERIC_IMPUTATION_FIELDS + CATEGORICAL_IMPUTATION_FIELDS
        if getattr(extracted, field, None) in (None, "")
    ]


def _missing_yearly_fields(extracted: ExtractedKpiPayload) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    for record in extracted.yearly_records:
        fields = [field for field in FORECAST_IMPUTATION_FIELDS if getattr(record, field, None) is None]
        if fields and record.fiscal_year_start is not None:
            missing.append({"fiscal_year_start": record.fiscal_year_start, "fields": fields})
    return missing


def _known_values(extracted: ExtractedKpiPayload) -> dict[str, Any]:
    values = {
        field: getattr(extracted, field)
        for field in NUMERIC_IMPUTATION_FIELDS + CATEGORICAL_IMPUTATION_FIELDS + ["company_name", "fiscal_year_start"]
        if getattr(extracted, field, None) not in (None, "")
    }
    values["yearly_records"] = [
        record.model_dump()
        for record in extracted.yearly_records
        if record.fiscal_year_start is not None
    ]
    return values


def _filter_rows(rows: list[dict[str, str]], field: str, value: str | None) -> list[dict[str, str]]:
    if not value:
        return []
    target = _normalize(value)
    return [row for row in rows if _normalize(row.get(field)) == target]


def _nearest_numeric_rows(rows: list[dict[str, str]], extracted: ExtractedKpiPayload, limit: int = 40) -> list[dict[str, str]]:
    known = {
        field: getattr(extracted, field)
        for field in ["scope1_tco2e", "scope2_tco2e", "water_consumption_kl", "waste_generated_tonnes", "waste_recycled_tonnes"]
        if getattr(extracted, field, None) is not None
    }
    if not known:
        return []

    scored: list[tuple[float, dict[str, str]]] = []
    for row in rows:
        distance = 0.0
        matches = 0
        for field, value in known.items():
            row_value = _safe_float(row.get(field))
            if row_value is None:
                continue
            distance += abs(math.log1p(abs(float(value))) - math.log1p(abs(row_value)))
            matches += 1
        if matches:
            scored.append((distance / matches, row))

    return [row for _, row in sorted(scored, key=lambda item: item[0])[:limit]]


def _summarize_rows(rows: list[dict[str, str]]) -> dict[str, Any]:
    if not rows:
        return {"row_count": 0, "numeric": {}, "categorical": {}}

    numeric = {}
    for field in NUMERIC_IMPUTATION_FIELDS + ["total_scope1_scope2_tco2e"]:
        values = [_safe_float(row.get(field)) for row in rows]
        clean = sorted(value for value in values if value is not None)
        if not clean:
            continue
        numeric[field] = {
            "count": len(clean),
            "median": round(statistics.median(clean), 4),
            "p25": round(_percentile(clean, 0.25), 4),
            "p75": round(_percentile(clean, 0.75), 4),
        }

    categorical = {}
    for field in CATEGORICAL_IMPUTATION_FIELDS:
        values = [row.get(field) for row in rows if row.get(field)]
        counts = Counter(values)
        categorical[field] = [
            {"value": value, "count": count}
            for value, count in counts.most_common(5)
        ]

    return {"row_count": len(rows), "numeric": numeric, "categorical": categorical}


def _fallback_value(field: str, reference: dict[str, Any]) -> float | str | None:
    if field in CATEGORICAL_IMPUTATION_FIELDS:
        for group in ["same_sector", "same_sub_sector", "similar_numeric_scale", "global"]:
            values = reference.get(group, {}).get("categorical", {}).get(field) or []
            if values:
                return values[0]["value"]
        return "Unknown"

    for group in ["same_sub_sector", "same_sector", "similar_numeric_scale", "global"]:
        stats = reference.get(group, {}).get("numeric", {}).get(field)
        if stats and stats.get("median") is not None:
            return stats["median"]
    return None


def _fallback_confidence(field: str, reference: dict[str, Any]) -> str:
    for group in ["same_sub_sector", "same_sector", "similar_numeric_scale"]:
        stats = reference.get(group, {}).get("numeric", {}).get(field)
        if stats and stats.get("count", 0) >= 10:
            return "medium"
        values = reference.get(group, {}).get("categorical", {}).get(field) or []
        if values and values[0].get("count", 0) >= 10:
            return "medium"
    return "low"


def _fallback_basis(field: str, reference: dict[str, Any]) -> str:
    for group, label in [
        ("same_sub_sector", "same sub-sector"),
        ("same_sector", "same sector"),
        ("similar_numeric_scale", "similar numeric-scale rows"),
        ("global", "global reference rows"),
    ]:
        if field in CATEGORICAL_IMPUTATION_FIELDS:
            values = reference.get(group, {}).get("categorical", {}).get(field) or []
            if values:
                return f"Estimated from most common {field} in {label}."
        else:
            stats = reference.get(group, {}).get("numeric", {}).get(field)
            if stats and stats.get("median") is not None:
                return f"Estimated from reference median for {field} using {label}; p25={stats.get('p25')}, p75={stats.get('p75')}."
    return "Estimated from fallback reference statistics."


def _estimate_from_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("value")
    if value in (None, ""):
        return None
    confidence = str(payload.get("confidence") or "medium").lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"
    return {
        "value": value,
        "confidence": confidence,
        "basis": str(payload.get("basis") or "Estimated from reference statistics."),
    }


def _apply_value(target: Any, field: str, value: float | str) -> float | str | None:
    if field in NUMERIC_IMPUTATION_FIELDS + FORECAST_IMPUTATION_FIELDS:
        numeric_value = _safe_float(value)
        if numeric_value is None:
            return None
        setattr(target, field, numeric_value)
        return numeric_value
    text_value = str(value).strip()
    if not text_value:
        return None
    setattr(target, field, text_value)
    return text_value


def _yearly_record_for(extracted: ExtractedKpiPayload, year: int | None) -> YearlyKpiRecord | None:
    if year is None:
        return None
    for record in extracted.yearly_records:
        if record.fiscal_year_start == year:
            return record
    return None


def _sync_totals_and_latest(extracted: ExtractedKpiPayload) -> None:
    for record in extracted.yearly_records:
        if record.scope1_tco2e is not None and record.scope2_tco2e is not None:
            record.total_scope1_scope2_tco2e = record.scope1_tco2e + record.scope2_tco2e

    if extracted.scope1_tco2e is not None and extracted.scope2_tco2e is not None:
        extracted.total_scope1_scope2_tco2e = extracted.scope1_tco2e + extracted.scope2_tco2e

    if extracted.yearly_records:
        latest = max(extracted.yearly_records, key=lambda record: record.fiscal_year_start or 0)
        extracted.fiscal_year = latest.fiscal_year or extracted.fiscal_year
        extracted.fiscal_year_start = latest.fiscal_year_start or extracted.fiscal_year_start
        for field in FORECAST_IMPUTATION_FIELDS + [TARGET_FIELD]:
            if hasattr(latest, field) and getattr(latest, field, None) is not None:
                setattr(extracted, field, getattr(latest, field))


TARGET_FIELD = "total_scope1_scope2_tco2e"


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    index = (len(values) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return values[int(index)]
    return values[lower] + (values[upper] - values[lower]) * (index - lower)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize(value: str | None) -> str:
    return " ".join(str(value or "").lower().split())


CsvGroundedKpiImputer = ReferenceGroundedKpiImputer
