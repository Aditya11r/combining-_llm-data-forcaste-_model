from __future__ import annotations

from app.schemas import ChartPayload, ExtractedKpiPayload, ForecastPoint, PeerComparison


def build_charts(
    *,
    extracted: ExtractedKpiPayload,
    forecast: list[ForecastPoint],
    peer_comparison: PeerComparison,
) -> ChartPayload:
    emissions_forecast = []
    current_year = extracted.fiscal_year_start
    if current_year and extracted.total_scope1_scope2_tco2e is not None:
        emissions_forecast.append(
            {
                "year": current_year,
                "company": round(extracted.total_scope1_scope2_tco2e, 4),
                "peer": peer_comparison.averages.get("total_scope1_scope2_tco2e"),
                "kind": "actual",
            }
        )

    peer_by_year = {point.year: point.total_scope1_scope2_tco2e for point in peer_comparison.peer_forecast}
    actual_years = {row["year"] for row in emissions_forecast}
    for record in extracted.yearly_records:
        total = record.computed_total_scope1_scope2_tco2e
        if record.fiscal_year_start is None or total is None or record.fiscal_year_start in actual_years:
            continue
        emissions_forecast.append(
            {
                "year": record.fiscal_year_start,
                "company": round(total, 4),
                "peer": peer_comparison.averages.get("total_scope1_scope2_tco2e"),
                "kind": "actual",
            }
        )

    for point in forecast:
        emissions_forecast.append(
            {
                "year": point.year,
                "company": point.total_scope1_scope2_tco2e,
                "peer": peer_by_year.get(point.year),
                "kind": "forecast",
            }
        )

    emissions_forecast = sorted(emissions_forecast, key=lambda row: row["year"])

    kpi_snapshot = [
        {"name": "Scope 1", "value": extracted.scope1_tco2e, "unit": "tCO2e"},
        {"name": "Scope 2", "value": extracted.scope2_tco2e, "unit": "tCO2e"},
        {"name": "Water", "value": extracted.water_consumption_kl, "unit": "kL"},
        {"name": "Waste generated", "value": extracted.waste_generated_tonnes, "unit": "tonnes"},
        {"name": "Waste recycled", "value": extracted.waste_recycled_tonnes, "unit": "tonnes"},
    ]

    peer_benchmark = [
        {
            "metric": "Emissions",
            "company": extracted.total_scope1_scope2_tco2e,
            "peer": peer_comparison.averages.get("total_scope1_scope2_tco2e"),
        },
        {
            "metric": "Water",
            "company": extracted.water_consumption_kl,
            "peer": peer_comparison.averages.get("water_consumption_kl"),
        },
        {
            "metric": "Waste",
            "company": extracted.waste_generated_tonnes,
            "peer": peer_comparison.averages.get("waste_generated_tonnes"),
        },
        {
            "metric": "Recycled",
            "company": extracted.waste_recycled_tonnes,
            "peer": peer_comparison.averages.get("waste_recycled_tonnes"),
        },
    ]

    kpi_trends = [
        {
            "year": record.fiscal_year_start,
            "scope1": record.scope1_tco2e,
            "scope2": record.scope2_tco2e,
            "total_emissions": record.computed_total_scope1_scope2_tco2e,
            "water": record.water_consumption_kl,
            "waste_generated": record.waste_generated_tonnes,
            "waste_recycled": record.waste_recycled_tonnes,
        }
        for record in extracted.yearly_records
        if record.fiscal_year_start is not None
    ]

    return ChartPayload(
        emissions_forecast=emissions_forecast,
        kpi_snapshot=kpi_snapshot,
        peer_benchmark=peer_benchmark,
        kpi_trends=kpi_trends,
    )
