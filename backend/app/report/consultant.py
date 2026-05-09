from __future__ import annotations

import json

from pydantic import ValidationError

from app.config import Settings
from app.extraction.openrouter_client import OpenRouterClient
from app.schemas import (
    ChartPayload,
    ClusterResult,
    ConsultantReport,
    ExtractedKpiPayload,
    ExtractionQuality,
    ForecastPoint,
    PeerComparison,
)


REPORT_SYSTEM_PROMPT = """You are an expert ESG strategy consultant.
Write direct, decision-useful analysis for executives. Use only supplied numbers.
Return valid JSON only. Do not invent facts. If extraction confidence is weak,
say what needs human review. Treat yearly_records and kpi_trends as first-class:
when multiple fiscal years are present, discuss direction of change, volatility,
and whether the forecast is based on one year or a multi-year sequence.
Recommendations must be evidence-based: tie each recommendation to at least one
of these supplied inputs: multi-year KPI trend, same-cluster peer benchmark,
forecast direction, extraction quality, or missing fields."""


class ConsultantReporter:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = OpenRouterClient(settings)

    async def build(
        self,
        *,
        extracted: ExtractedKpiPayload,
        quality: ExtractionQuality,
        cluster: ClusterResult,
        forecast: list[ForecastPoint],
        peer_comparison: PeerComparison,
        charts: ChartPayload,
        model_override: str | None = None,
    ) -> ConsultantReport:
        if not self.client.configured:
            return self._fallback_report(extracted, quality, cluster, forecast, peer_comparison)

        payload = {
            "extracted_kpis": extracted.model_dump(),
            "quality": quality.model_dump(),
            "cluster": cluster.model_dump(),
            "forecast": [point.model_dump() for point in forecast],
            "peer_comparison": peer_comparison.model_dump(),
            "charts": charts.model_dump(),
            "analysis_basis": _analysis_basis(
                extracted=extracted,
                quality=quality,
                forecast=forecast,
                peer_comparison=peer_comparison,
            ),
        }
        user_prompt = f"""
Return this JSON shape:
{{
  "executive_summary": "paragraph",
  "cluster_interpretation": "paragraph",
  "forecast_interpretation": "paragraph",
  "peer_benchmark": "paragraph",
  "risks": ["risk"],
  "recommendations": ["recommendation"],
  "chart_narratives": ["short chart explanation"],
  "confidence_note": "paragraph"
}}

Analysis data:
{json.dumps(payload, indent=2)}
"""
        try:
            data = await self.client.chat_json(
                model=model_override or self.settings.openrouter_report_model,
                system_prompt=REPORT_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                temperature=0.25,
            )
        except Exception:
            return self._fallback_report(extracted, quality, cluster, forecast, peer_comparison)

        try:
            return ConsultantReport.model_validate(data)
        except ValidationError:
            return self._fallback_report(extracted, quality, cluster, forecast, peer_comparison)

    def _fallback_report(
        self,
        extracted: ExtractedKpiPayload,
        quality: ExtractionQuality,
        cluster: ClusterResult,
        forecast: list[ForecastPoint],
        peer_comparison: PeerComparison,
    ) -> ConsultantReport:
        company = extracted.company_name or "The company"
        current_total = extracted.total_scope1_scope2_tco2e
        first_forecast = forecast[0].total_scope1_scope2_tco2e if forecast else None
        year_count = len(extracted.yearly_records)
        basis = _analysis_basis(
            extracted=extracted,
            quality=quality,
            forecast=forecast,
            peer_comparison=peer_comparison,
        )

        return ConsultantReport(
            executive_summary=(
                f"{company} was assigned to {cluster.KMeans_cluster_label}. "
                f"The extracted emissions baseline is {current_total} tCO2e across {year_count or 1} fiscal year(s). "
                f"{basis['emissions_trend']}"
            ),
            cluster_interpretation=(
                f"The assigned KMeans cluster is {cluster.KMeans_cluster}. "
                f"The peer set is selected from companies mapped to that same cluster."
            ),
            forecast_interpretation=(
                f"The next forecast point is {first_forecast} tCO2e, using "
                f"{'a multi-year KPI sequence' if year_count > 1 else 'the available single-year KPI baseline'}. "
                f"{basis['forecast_direction']}"
                if first_forecast is not None
                else "No forecast point was produced."
            ),
            peer_benchmark=(
                f"The peer set contains {peer_comparison.company_count} companies with "
                f"{len(peer_comparison.sample_companies)} sample names available. "
                f"{basis['peer_position']}"
            ),
            risks=_fallback_risks(quality, basis),
            recommendations=_fallback_recommendations(
                extracted=extracted,
                quality=quality,
                forecast=forecast,
                basis=basis,
            ),
            chart_narratives=[
                "Use the emissions forecast chart to compare the company trajectory against peer averages.",
                "Use the yearly KPI view to check whether the latest year is improving or worsening against prior years.",
            ],
            confidence_note=f"Extraction quality is {quality.level} with score {quality.score}.",
        )


def _analysis_basis(
    *,
    extracted: ExtractedKpiPayload,
    quality: ExtractionQuality,
    forecast: list[ForecastPoint],
    peer_comparison: PeerComparison,
) -> dict[str, str]:
    records = sorted(
        [record for record in extracted.yearly_records if record.fiscal_year_start is not None],
        key=lambda record: record.fiscal_year_start or 0,
    )
    latest_total = extracted.total_scope1_scope2_tco2e
    latest_year = extracted.fiscal_year_start
    peer_total = peer_comparison.averages.get("total_scope1_scope2_tco2e")

    return {
        "year_coverage": _year_coverage(records),
        "emissions_trend": _metric_trend(records, "computed_total_scope1_scope2_tco2e", "total Scope 1 + 2 emissions"),
        "water_trend": _metric_trend(records, "water_consumption_kl", "water consumption"),
        "waste_trend": _metric_trend(records, "waste_generated_tonnes", "waste generated"),
        "recycling_trend": _metric_trend(records, "waste_recycled_tonnes", "waste recycled"),
        "peer_position": _position_against_peer(latest_total, peer_total, "total Scope 1 + 2 emissions"),
        "water_peer_position": _position_against_peer(
            extracted.water_consumption_kl,
            peer_comparison.averages.get("water_consumption_kl"),
            "water consumption",
        ),
        "waste_peer_position": _position_against_peer(
            extracted.waste_generated_tonnes,
            peer_comparison.averages.get("waste_generated_tonnes"),
            "waste generated",
        ),
        "forecast_direction": _forecast_direction(forecast, latest_total, latest_year),
        "quality_basis": (
            f"Extraction quality is {quality.level} ({quality.score}); missing fields: "
            f"{', '.join(quality.missing_required_fields) or 'none'}."
        ),
    }


def _year_coverage(records) -> str:
    if not records:
        return "No yearly KPI records were extracted."
    years = [str(record.fiscal_year_start) for record in records if record.fiscal_year_start is not None]
    return f"{len(years)} fiscal year(s) available: {', '.join(years)}."


def _metric_trend(records, attr: str, label: str) -> str:
    usable = [(record.fiscal_year_start, getattr(record, attr, None)) for record in records]
    usable = [(year, value) for year, value in usable if year is not None and value is not None]
    if len(usable) < 2:
        return f"Not enough yearly data to calculate a {label} trend."

    start_year, start_value = usable[0]
    end_year, end_value = usable[-1]
    direction = "increased" if end_value > start_value else "decreased" if end_value < start_value else "remained flat"
    pct = _pct_change(start_value, end_value)
    pct_text = f" ({pct:+.1f}%)" if pct is not None else ""
    return f"{label.capitalize()} {direction} from {_fmt_value(start_value)} in {start_year} to {_fmt_value(end_value)} in {end_year}{pct_text}."


def _position_against_peer(company_value: float | None, peer_value: float | None, label: str) -> str:
    if company_value is None or peer_value is None:
        return f"Peer comparison is unavailable for {label}."
    direction = "above" if company_value > peer_value else "below" if company_value < peer_value else "in line with"
    pct = _pct_change(peer_value, company_value)
    pct_text = f" by {abs(pct):.1f}%" if pct is not None and direction != "in line with" else ""
    return f"Company {label} is {direction} the cluster peer average{pct_text}."


def _forecast_direction(
    forecast: list[ForecastPoint],
    latest_total: float | None,
    latest_year: int | None,
) -> str:
    if not forecast or latest_total is None:
        return "Forecast direction cannot be compared against the latest extracted emissions value."
    next_point = forecast[0]
    direction = "higher than" if next_point.total_scope1_scope2_tco2e > latest_total else "lower than" if next_point.total_scope1_scope2_tco2e < latest_total else "flat against"
    pct = _pct_change(latest_total, next_point.total_scope1_scope2_tco2e)
    pct_text = f" ({pct:+.1f}%)" if pct is not None else ""
    baseline = f"{latest_year}" if latest_year is not None else "the latest extracted year"
    return f"The first forecast year, {next_point.year}, is {direction} {baseline}{pct_text}."


def _fallback_risks(quality: ExtractionQuality, basis: dict[str, str]) -> list[str]:
    risks = [
        basis["quality_basis"],
        "Confirm unit normalization for emissions, water, and waste before formal disclosure.",
    ]
    if quality.level != "high" or quality.missing_required_fields:
        risks.insert(0, "Review the source pages for missing or low-confidence KPI fields before making decisions.")
    return risks


def _fallback_recommendations(
    *,
    extracted: ExtractedKpiPayload,
    quality: ExtractionQuality,
    forecast: list[ForecastPoint],
    basis: dict[str, str],
) -> list[str]:
    recommendations: list[str] = []

    if "increased" in basis["emissions_trend"].lower():
        larger_scope = _larger_scope(extracted.scope1_tco2e, extracted.scope2_tco2e)
        recommendations.append(
            f"Prioritize a reduction plan for {larger_scope}, because {basis['emissions_trend']}"
        )
    elif "decreased" in basis["emissions_trend"].lower():
        recommendations.append(
            f"Preserve and document the operating changes behind the emissions reduction, because {basis['emissions_trend']}"
        )
    else:
        recommendations.append(
            f"Validate whether a trend exists before setting reduction targets, because {basis['emissions_trend']}"
        )

    if "above the cluster peer average" in basis["peer_position"]:
        recommendations.append(
            f"Set a near-term target to close the emissions gap to the KMeans peer average, because {basis['peer_position']}"
        )
    elif "below the cluster peer average" in basis["peer_position"]:
        recommendations.append(
            f"Use the company as a peer-set outperformer case and protect the current controls, because {basis['peer_position']}"
        )
    else:
        recommendations.append(
            f"Use the cluster peer set as the benchmark for target setting, because {basis['peer_position']}"
        )

    if forecast:
        recommendations.append(
            f"Turn the first forecast year into a mitigation checkpoint with owner, target, and monthly tracking, because {basis['forecast_direction']}"
        )

    water_or_waste = _resource_recommendation(basis)
    if water_or_waste:
        recommendations.append(water_or_waste)

    if quality.level != "high" or quality.missing_required_fields:
        recommendations.append(
            f"Run human review on the extracted fields before final reporting, because {basis['quality_basis']}"
        )

    return recommendations[:5]


def _resource_recommendation(basis: dict[str, str]) -> str | None:
    if "increased" in basis["water_trend"].lower() or "above the cluster peer average" in basis["water_peer_position"]:
        return f"Investigate water-intensive operations first, because {basis['water_trend']} {basis['water_peer_position']}"
    if "increased" in basis["waste_trend"].lower() or "above the cluster peer average" in basis["waste_peer_position"]:
        return f"Review waste generation and recycling controls, because {basis['waste_trend']} {basis['waste_peer_position']}"
    return None


def _larger_scope(scope1: float | None, scope2: float | None) -> str:
    if scope1 is None or scope2 is None:
        return "Scope 1 and Scope 2 drivers"
    return "Scope 1" if scope1 >= scope2 else "Scope 2"


def _pct_change(start: float, end: float) -> float | None:
    if start == 0:
        return None
    return ((end - start) / abs(start)) * 100


def _fmt_value(value: float) -> str:
    return f"{value:,.2f}"
