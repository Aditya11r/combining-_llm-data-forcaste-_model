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
context, return null and add a short warning. Extract every fiscal year column
that appears in KPI tables into yearly_records. If a table has FY 2024-25 and
FY 2023-24 columns, return two yearly_records. If a KPI is present for one year
but missing for another, keep that missing KPI as null for that year. Use the
latest fiscal year as the top-level KPI fields. Normalize emissions to tCO2e,
water to the same numeric unit shown in the BRSR table, waste to tonnes, and
fiscal_year_start to the first year in the fiscal year label. Keep sector and
sub_sector as short business labels.

Treat states_served and countries_served as high-risk geography fields. Before
filling them, explicitly check whether the report mentions operations across
Indian states, a numeric count of countries served, global markets, exports,
subsidiaries, overseas operations, or international markets. Return null if the
context does not support a numeric count. Do not infer a number from company
size or generic phrases like "pan-India" or "global presence" unless they appear
inside the BRSR Markets served table. If that table says "All States" for
National (Number of states), return 28 and cite that table. If the report gives
a lower-bound phrase such as "100+ countries", return the stated number and cite
that phrase in evidence."""


CONTEXT_METRIC_PATTERNS = [
    (
        "scope1_tco2e",
        r"t\s*otal\s+scope\s*1\s+emissions?",
        r"(?:t\s*co\s*2\s*e|mtco2e|metric\s+tonnes?\s+of\s+co\s*2\s+equivalent)",
        "total Scope 1 emissions",
    ),
    (
        "scope2_tco2e",
        r"t\s*otal\s+scope\s*2\s+emissions?",
        r"(?:t\s*co\s*2\s*e|mtco2e|metric\s+tonnes?\s+of\s+co\s*2\s+equivalent)",
        "total Scope 2 emissions",
    ),
    (
        "water_consumption_kl",
        r"t\s*otal\s+volume\s+of\s+water\s+consumption",
        r"(?:mn\s*l|k\s*l|kilolit(?:re|er)s?)",
        "total water consumption",
    ),
    (
        "waste_generated_tonnes",
        r"t\s*otal\s*\(\s*a\s*\+\s*b\s*\+\s*c\s*\+\s*d\s*\+\s*e\s*\+\s*f\s*\+\s*g\s*\+\s*h\s*\)",
        None,
        "total waste generated",
    ),
    (
        "waste_recycled_tonnes",
        r"(?:\(\s*i\s*\)\s*)?recycled",
        None,
        "waste recycled",
    ),
]

FISCAL_YEAR_PATTERN = re.compile("FY\\s*(20\\d{2})\\s*[-/\\u2013\\u2014]\\s*(\\d{2,4})", flags=re.IGNORECASE)


def build_extraction_prompt(context: str, detected_years: list[str], target_years: list[str]) -> str:
    trimmed_context = context[:45000]
    return f"""
Detected fiscal years: {detected_years}
Target KPI fiscal years: {target_years}

Geography extraction checks:
1. Does the report mention operations across Indian states? If yes, is an exact
   numeric state count stated?
2. Does the report mention number of countries served?
3. Does the report mention global markets, exports, subsidiaries, overseas
   operations, or international markets?

Use those answers to populate states_served and countries_served only when the
PDF context supports a numeric value. In the BRSR Markets served table, "All
States" means 28 Indian states. Add a warning when geography is mentioned but no
numeric count is stated.

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

        extracted = self._augment_with_context_geography(extracted, context)
        extracted = self._augment_with_context_yearly_records(extracted, context)
        extracted = self._normalize_totals(extracted)
        quality = score_extraction(extracted)
        return extracted, quality, payload

    def _augment_with_context_geography(
        self,
        extracted: ExtractedKpiPayload,
        context: str,
    ) -> ExtractedKpiPayload:
        geography = _extract_context_geography(context)
        changed_fields: list[str] = []
        for field in ("states_served", "countries_served"):
            value = geography.get(field)
            if value is None:
                continue
            existing = getattr(extracted, field)
            if existing in (None, "") or _values_differ(existing, value):
                setattr(extracted, field, value)
                if existing not in (None, ""):
                    changed_fields.append(f"{field}: {existing} -> {value}")

        for field, evidence in geography.get("evidence", {}).items():
            extracted.evidence[field] = evidence

        if changed_fields:
            extracted.warnings.append(
                "BRSR Markets served table corrected geography fields: " + ", ".join(changed_fields)
            )

        return extracted

    def _augment_with_context_yearly_records(
        self,
        extracted: ExtractedKpiPayload,
        context: str,
    ) -> ExtractedKpiPayload:
        supplements = _extract_context_yearly_records(context)
        if not supplements:
            return extracted

        heuristic_mode = any("heuristic extraction" in warning.lower() for warning in extracted.warnings)
        by_year = {
            record.fiscal_year_start: record
            for record in extracted.yearly_records
            if record.fiscal_year_start is not None
        }
        changed_fields: list[str] = []

        for supplement in supplements:
            if supplement.fiscal_year_start is None:
                continue
            record = by_year.get(supplement.fiscal_year_start)
            if record is None:
                record = YearlyKpiRecord(
                    fiscal_year=supplement.fiscal_year,
                    fiscal_year_start=supplement.fiscal_year_start,
                )
                extracted.yearly_records.append(record)
                by_year[supplement.fiscal_year_start] = record

            if not record.fiscal_year:
                record.fiscal_year = supplement.fiscal_year

            for field in FORECAST_REQUIRED_FIELDS:
                if field in {"company_name", "fiscal_year_start"}:
                    continue
                value = getattr(supplement, field, None)
                if value is None:
                    continue

                existing = getattr(record, field, None)
                evidence_exists = bool(record.evidence.get(field))
                if _should_use_supplement(
                    field=field,
                    existing=existing,
                    supplement=value,
                    evidence_exists=evidence_exists,
                    heuristic_mode=heuristic_mode,
                ):
                    setattr(record, field, value)
                    if supplement.evidence.get(field):
                        record.evidence[field] = supplement.evidence[field]
                    changed_fields.append(f"{supplement.fiscal_year_start}.{field}")

        if changed_fields:
            extracted.warnings.append(
                "Context table safety net added or corrected yearly KPI fields: "
                + ", ".join(sorted(set(changed_fields)))
            )
        return extracted

    def _normalize_totals(self, extracted: ExtractedKpiPayload) -> ExtractedKpiPayload:
        if extracted.scope1_tco2e is not None and extracted.scope2_tco2e is not None:
            extracted.total_scope1_scope2_tco2e = extracted.scope1_tco2e + extracted.scope2_tco2e

        for record in extracted.yearly_records:
            if record.scope1_tco2e is not None and record.scope2_tco2e is not None:
                record.total_scope1_scope2_tco2e = record.scope1_tco2e + record.scope2_tco2e

        extracted.yearly_records = [
            record for record in extracted.yearly_records if _record_has_any_kpi(record)
        ]
        extracted.yearly_records = sorted(
            extracted.yearly_records,
            key=lambda record: record.fiscal_year_start or 0,
        )

        latest_record = None
        if extracted.yearly_records:
            latest_record = max(extracted.yearly_records, key=lambda record: record.fiscal_year_start or 0)

        if latest_record and latest_record.fiscal_year_start:
            extracted.fiscal_year = latest_record.fiscal_year or extracted.fiscal_year
            extracted.fiscal_year_start = latest_record.fiscal_year_start
            for field in [
                "scope1_tco2e",
                "scope2_tco2e",
                "water_consumption_kl",
                "waste_generated_tonnes",
                "waste_recycled_tonnes",
                "total_scope1_scope2_tco2e",
            ]:
                if getattr(latest_record, field) is not None:
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


def _extract_context_yearly_records(context: str) -> list[YearlyKpiRecord]:
    flat = _flatten_context(context)
    by_year: dict[int, YearlyKpiRecord] = {}

    for field, label_pattern, unit_pattern, evidence_label in CONTEXT_METRIC_PATTERNS:
        for year, value, evidence in _extract_metric_values(flat, label_pattern, unit_pattern, evidence_label):
            record = by_year.setdefault(
                year,
                YearlyKpiRecord(
                    fiscal_year=_format_fiscal_year(year),
                    fiscal_year_start=year,
                    evidence={},
                ),
            )
            if getattr(record, field, None) is None:
                setattr(record, field, value)
                record.evidence[field] = evidence

    for inline_record in _extract_inline_yearly_records(flat):
        if inline_record.fiscal_year_start is None:
            continue
        record = by_year.setdefault(
            inline_record.fiscal_year_start,
            YearlyKpiRecord(
                fiscal_year=inline_record.fiscal_year,
                fiscal_year_start=inline_record.fiscal_year_start,
                evidence={},
            ),
        )
        for field in [
            "scope1_tco2e",
            "scope2_tco2e",
            "water_consumption_kl",
            "waste_generated_tonnes",
            "waste_recycled_tonnes",
        ]:
            value = getattr(inline_record, field, None)
            if value is not None and getattr(record, field, None) is None:
                setattr(record, field, value)
                record.evidence[field] = inline_record.evidence.get(field, "Inline yearly KPI summary in context.")

    for chart_record in _extract_waste_management_chart_records(flat):
        if chart_record.fiscal_year_start is None:
            continue
        record = by_year.setdefault(
            chart_record.fiscal_year_start,
            YearlyKpiRecord(
                fiscal_year=chart_record.fiscal_year,
                fiscal_year_start=chart_record.fiscal_year_start,
                evidence={},
            ),
        )
        for field in ["waste_generated_tonnes", "waste_recycled_tonnes"]:
            value = getattr(chart_record, field, None)
            if value is None:
                continue
            existing = getattr(record, field, None)
            evidence_exists = bool(record.evidence.get(field))
            if _should_use_supplement(
                field=field,
                existing=existing,
                supplement=value,
                evidence_exists=evidence_exists,
                heuristic_mode=False,
            ):
                setattr(record, field, value)
                record.evidence[field] = chart_record.evidence.get(field, "Waste management chart in context.")

    records = sorted(by_year.values(), key=lambda record: record.fiscal_year_start or 0)
    for record in records:
        if record.total_scope1_scope2_tco2e is None and record.scope1_tco2e is not None and record.scope2_tco2e is not None:
            record.total_scope1_scope2_tco2e = record.scope1_tco2e + record.scope2_tco2e
    return records


def _extract_context_geography(context: str) -> dict[str, Any]:
    flat = _flatten_context(context)
    result: dict[str, Any] = {"evidence": {}}

    states_match = re.search(
        r"National\s*\(\s*(?:Number|No\.?)\s+of\s+states\s*\)\s*(?P<value>All\s+States|[0-9][0-9,]*(?:\.\d+)?)",
        flat,
        flags=re.IGNORECASE,
    )
    if states_match:
        raw_value = states_match.group("value")
        page = _page_reference(flat, states_match.start())
        if re.search(r"\ball\s+states\b", raw_value, flags=re.IGNORECASE):
            result["states_served"] = 28.0
            result["evidence"]["states_served"] = (
                f"{page}: BRSR Markets served table says National (Number of states) = All States; "
                "normalized to 28 Indian states."
            )
        elif (value := _safe_number(raw_value)) is not None:
            result["states_served"] = value
            result["evidence"]["states_served"] = (
                f"{page}: BRSR Markets served table reports National (Number of states) = {raw_value}."
            )

    countries_match = re.search(
        r"International\s*\(\s*(?:Number|No\.?)\s+of\s+countries\s*\)\s*(?P<value>[0-9][0-9,]*(?:\.\d+)?|Nil|None|N/?A|-)",
        flat,
        flags=re.IGNORECASE,
    )
    if countries_match:
        raw_value = countries_match.group("value")
        page = _page_reference(flat, countries_match.start())
        if re.fullmatch(r"Nil|None|N/?A|-", raw_value, flags=re.IGNORECASE):
            result["countries_served"] = 0.0
        elif (value := _safe_number(raw_value)) is not None:
            result["countries_served"] = value
        if "countries_served" in result:
            result["evidence"]["countries_served"] = (
                f"{page}: BRSR Markets served table reports International (Number of countries) = {raw_value}."
            )

    return result


def _extract_metric_values(
    flat_context: str,
    label_pattern: str,
    unit_pattern: str | None,
    evidence_label: str,
) -> list[tuple[int, float, str]]:
    values: list[tuple[int, float, str]] = []
    for match in re.finditer(label_pattern, flat_context, flags=re.IGNORECASE):
        prefix = flat_context[max(0, match.start() - 700) : match.start()]
        years = _header_years(prefix)
        if not years:
            continue

        row_text = flat_context[match.end() : match.end() + 420]
        if _is_bad_metric_row(label_pattern, row_text):
            continue

        if unit_pattern:
            unit_match = re.search(unit_pattern, row_text, flags=re.IGNORECASE)
            if not unit_match:
                continue
            value_text = _truncate_metric_value_text(row_text[unit_match.end() :])
        else:
            if not _is_valid_unitless_context(evidence_label, flat_context, match.start()):
                continue
            value_text = _truncate_metric_value_text(row_text, max_chars=140)

        numeric_tokens = [
            token
            for token in _number_tokens(value_text)
            if _valid_metric_value(evidence_label, token)
        ]
        if len(numeric_tokens) < len(years):
            continue

        for fiscal_year, numeric_value in zip(years, numeric_tokens):
            page = _page_reference(flat_context, match.start())
            evidence = f"{page}: {evidence_label} for {_format_fiscal_year(fiscal_year)} found in KPI table."
            values.append((fiscal_year, numeric_value, evidence))

    return values


def _truncate_metric_value_text(text: str, max_chars: int = 240) -> str:
    stop_patterns = [
        r"\bTotal\s+Scope\s+\d",
        r"\bTotal\s+Scope\s+1\s+and\s+Scope\s+2",
        r"\bWater\s+intensity\b",
        r"\bWaste\s+intensity\b",
        r"\bFor\s+each\s+category\b",
        r"\bNote\s*:",
        r"\bIndicate\s+if\b",
        r"##\s*PAGE\s+\d+",
        r"\bAnnual\s+Report\b",
        r"\bFinancial\s+Statements\b",
        r"\bStatutory\s+Reports\b",
    ]
    limited = text[:max_chars]
    stops = [
        match.start()
        for pattern in stop_patterns
        if (match := re.search(pattern, limited, flags=re.IGNORECASE))
    ]
    if stops:
        limited = limited[: min(stops)]
    return limited


def _is_bad_metric_row(label_pattern: str, row_text: str) -> bool:
    row_start = row_text[:120].lower()
    first_number = re.search(r"(?<![A-Za-z0-9])-?\d", row_start)
    for bad_phrase in ("per rupee", "/ revenue", "adjusted for"):
        position = row_start.find(bad_phrase)
        if position >= 0 and (first_number is None or position < first_number.start()):
            return True
    if "scope" in label_pattern.lower() and "scope 1 and scope 2" in row_start:
        return True
    return False


def _is_valid_unitless_context(evidence_label: str, flat_context: str, index: int) -> bool:
    window = flat_context[max(0, index - 900) : index + 220].lower()
    if evidence_label == "total waste generated":
        return "waste" in window
    if evidence_label == "waste recycled":
        return "waste recovered through recycling" in window or (
            "category of waste" in window and "waste generated" in window
        )
    return True


def _valid_metric_value(evidence_label: str, value: float) -> bool:
    if value < 0:
        return False
    if evidence_label in {"total Scope 1 emissions", "total Scope 2 emissions"} and 0 < value < 0.01:
        return False
    return True


def _should_use_supplement(
    *,
    field: str,
    existing: float | None,
    supplement: float | None,
    evidence_exists: bool,
    heuristic_mode: bool,
) -> bool:
    if supplement is None:
        return False
    if existing is None or heuristic_mode or not evidence_exists:
        return True
    if field == "waste_recycled_tonnes" and existing < 100 and supplement > 100:
        return True
    return False


def _extract_inline_yearly_records(flat_context: str) -> list[YearlyKpiRecord]:
    pattern = re.compile(
        r"\b(20\d{2})\s*:\s*scope\s*1\s+(?P<scope1>[-\d,.\s]+?)"
        r"\s+scope\s*2\s+(?P<scope2>[-\d,.\s]+?)"
        r"\s+total\s+emissions\s+(?P<total>[-\d,.\s]+?)"
        r"\s+water\s+(?P<water>[-\d,.\s]+?)"
        r"\s+waste\s+(?P<waste>[-\d,.\s]+?)"
        r"\s+recycled\s+(?P<recycled>[-\d,.\s]+?)(?=\s+\d{4}\s*:|\s+Cluster|\s+Forecast|\s+Peer|\s*$)",
        flags=re.IGNORECASE,
    )
    records: list[YearlyKpiRecord] = []
    for match in pattern.finditer(flat_context):
        year = _safe_int(match.group(1))
        if year is None:
            continue
        page = _page_reference(flat_context, match.start())
        record = YearlyKpiRecord(
            fiscal_year=_format_fiscal_year(year),
            fiscal_year_start=year,
            scope1_tco2e=_first_number(match.group("scope1")),
            scope2_tco2e=_first_number(match.group("scope2")),
            total_scope1_scope2_tco2e=_first_number(match.group("total")),
            water_consumption_kl=_first_number(match.group("water")),
            waste_generated_tonnes=_first_number(match.group("waste")),
            waste_recycled_tonnes=_first_number(match.group("recycled")),
            evidence={},
        )
        for field in [
            "scope1_tco2e",
            "scope2_tco2e",
            "total_scope1_scope2_tco2e",
            "water_consumption_kl",
            "waste_generated_tonnes",
            "waste_recycled_tonnes",
        ]:
            if getattr(record, field, None) is not None:
                record.evidence[field] = f"{page}: inline yearly KPI summary."
        records.append(record)
    return records


def _extract_waste_management_chart_records(flat_context: str) -> list[YearlyKpiRecord]:
    records: list[YearlyKpiRecord] = []
    pattern = re.compile(
        r"WASTE\s+MANAGEMENT\s*\(\s*MT\s*\)\s*"
        r"FY\s*(20\d{2})\s*[-/\u2013\u2014]\s*\d{2,4}\s*"
        r"FY\s*(20\d{2})\s*[-/\u2013\u2014]\s*\d{2,4}"
        r"(?P<body>.*?)(?:\b10\.\s+Briefly|##\s*PAGE\s+\d+|LEADERSHIP\s+INDICA)",
        flags=re.IGNORECASE,
    )

    for match in pattern.finditer(flat_context):
        years = [_safe_int(match.group(1)), _safe_int(match.group(2))]
        if any(year is None for year in years):
            continue

        body = match.group("body")
        label_match = re.search(
            r"Total\s+Waste\s+Generated\s+Waste\s+Recovered\s+Waste\s+Disposed",
            body,
            flags=re.IGNORECASE,
        )
        if not label_match:
            continue

        pre_label_values = _number_tokens(body[: label_match.start()])
        post_label_values = [value for value in _number_tokens(body[label_match.end() :]) if value >= 10_000]
        if len(pre_label_values) < 4 or len(post_label_values) < 2:
            continue

        generated = pre_label_values[0:2]
        disposed = pre_label_values[2:4]
        recovered = post_label_values[-2:]

        if not all(
            _approximately_equal(generated[index], disposed[index] + recovered[index])
            for index in range(2)
        ):
            continue

        page = _page_reference(flat_context, match.start())
        for index, year in enumerate(years):
            if year is None:
                continue
            records.append(
                YearlyKpiRecord(
                    fiscal_year=_format_fiscal_year(year),
                    fiscal_year_start=year,
                    waste_generated_tonnes=generated[index],
                    waste_recycled_tonnes=recovered[index],
                    evidence={
                        "waste_generated_tonnes": f"{page}: WASTE MANAGEMENT (MT) chart reports total waste generated.",
                        "waste_recycled_tonnes": f"{page}: WASTE MANAGEMENT (MT) chart reports waste recovered.",
                    },
                )
            )

    return records


def _header_years(prefix: str) -> list[int]:
    header_start = max(prefix.lower().rfind("parameter"), prefix.lower().rfind("unit"))
    header = prefix[header_start:] if header_start >= 0 else prefix
    years = _fiscal_years_in_order(header)
    if not years:
        years = _fiscal_years_in_order(prefix)
    return years[-4:]


def _fiscal_years_in_order(text: str) -> list[int]:
    years: list[int] = []
    seen: set[int] = set()
    for match in FISCAL_YEAR_PATTERN.finditer(text):
        year = _safe_int(match.group(1))
        if year is not None and year not in seen:
            years.append(year)
            seen.add(year)
    return years


def _number_tokens(text: str) -> list[float]:
    tokens: list[float] = []
    for match in re.finditer(r"(?<![A-Za-z0-9])-?\d[\d,]*(?:\.\d+)?", text):
        value = _safe_number(match.group(0))
        if value is not None:
            tokens.append(value)
    return tokens


def _first_number(text: str) -> float | None:
    values = _number_tokens(text)
    return values[0] if values else None


def _safe_number(value: str) -> float | None:
    cleaned = re.sub(r"[^0-9.\-]", "", value)
    if cleaned in {"", "-"}:
        return None
    try:
        numeric = float(cleaned)
    except ValueError:
        return None
    if numeric < 0:
        return None
    return numeric


def _approximately_equal(left: float, right: float, tolerance: float = 2.0) -> bool:
    return abs(left - right) <= tolerance


def _values_differ(left: Any, right: Any) -> bool:
    try:
        return float(left) != float(right)
    except (TypeError, ValueError):
        return left != right


def _record_has_any_kpi(record: YearlyKpiRecord) -> bool:
    return any(
        getattr(record, field, None) is not None
        for field in [
            "scope1_tco2e",
            "scope2_tco2e",
            "water_consumption_kl",
            "waste_generated_tonnes",
            "waste_recycled_tonnes",
            "total_scope1_scope2_tco2e",
        ]
    )


def _safe_int(value: str | int | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _format_fiscal_year(start_year: int) -> str:
    return f"FY {start_year}-{str(start_year + 1)[-2:]}"


def _flatten_context(context: str) -> str:
    return re.sub(r"\s+", " ", context).strip()


def _page_reference(flat_context: str, index: int) -> str:
    page_markers = list(re.finditer(r"##\s*PAGE\s+(\d+)", flat_context[:index], flags=re.IGNORECASE))
    if not page_markers:
        return "PDF context"
    return f"page {page_markers[-1].group(1)}"


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
