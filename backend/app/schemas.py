from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, computed_field


class ClusteringInput(BaseModel):
    scope1_tco2e: float | None = None
    scope2_tco2e: float | None = None
    water_consumption_kl: float | None = None
    waste_generated_tonnes: float | None = None
    waste_recycled_tonnes: float | None = None
    sector: str | None = None
    sub_sector: str | None = None
    states_served: float | None = None
    countries_served: float | None = None


class ForecastingInput(BaseModel):
    company_name: str | None = None
    fiscal_year_start: int | None = None
    scope1_tco2e: float | None = None
    scope2_tco2e: float | None = None
    water_consumption_kl: float | None = None
    waste_generated_tonnes: float | None = None
    waste_recycled_tonnes: float | None = None
    peer_group: int | None = None
    total_scope1_scope2_tco2e: float | None = None

    @computed_field
    @property
    def computed_total_scope1_scope2_tco2e(self) -> float | None:
        if self.total_scope1_scope2_tco2e is not None:
            return self.total_scope1_scope2_tco2e
        if self.scope1_tco2e is None or self.scope2_tco2e is None:
            return None
        return self.scope1_tco2e + self.scope2_tco2e


class YearlyKpiRecord(BaseModel):
    fiscal_year: str | None = None
    fiscal_year_start: int | None = None
    scope1_tco2e: float | None = None
    scope2_tco2e: float | None = None
    water_consumption_kl: float | None = None
    waste_generated_tonnes: float | None = None
    waste_recycled_tonnes: float | None = None
    total_scope1_scope2_tco2e: float | None = None
    evidence: dict[str, str] = Field(default_factory=dict)

    @computed_field
    @property
    def computed_total_scope1_scope2_tco2e(self) -> float | None:
        if self.total_scope1_scope2_tco2e is not None:
            return self.total_scope1_scope2_tco2e
        if self.scope1_tco2e is None or self.scope2_tco2e is None:
            return None
        return self.scope1_tco2e + self.scope2_tco2e


class ImputedKpiField(BaseModel):
    field: str
    value: float | str
    fiscal_year_start: int | None = None
    confidence: Literal["low", "medium", "high"] = "medium"
    method: Literal[
        "llm_csv_estimate",
        "csv_statistic_estimate",
        "llm_reference_estimate",
        "reference_statistic_estimate",
    ] = "reference_statistic_estimate"
    basis: str


class ExtractedKpiPayload(BaseModel):
    company_name: str | None = None
    fiscal_year: str | None = None
    fiscal_year_start: int | None = None
    sector: str | None = None
    sub_sector: str | None = None
    states_served: float | None = None
    countries_served: float | None = None
    scope1_tco2e: float | None = None
    scope2_tco2e: float | None = None
    water_consumption_kl: float | None = None
    waste_generated_tonnes: float | None = None
    waste_recycled_tonnes: float | None = None
    total_scope1_scope2_tco2e: float | None = None
    reporting_boundary: str | None = None
    evidence: dict[str, str] = Field(default_factory=dict)
    missing_fields: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    yearly_records: list[YearlyKpiRecord] = Field(default_factory=list)
    imputed_fields: list[ImputedKpiField] = Field(default_factory=list)

    @computed_field
    @property
    def clustering_input(self) -> ClusteringInput:
        return ClusteringInput(
            scope1_tco2e=self.scope1_tco2e,
            scope2_tco2e=self.scope2_tco2e,
            water_consumption_kl=self.water_consumption_kl,
            waste_generated_tonnes=self.waste_generated_tonnes,
            waste_recycled_tonnes=self.waste_recycled_tonnes,
            sector=self.sector,
            sub_sector=self.sub_sector,
            states_served=self.states_served,
            countries_served=self.countries_served,
        )

    def to_forecasting_input(self, peer_group: int | None = None) -> ForecastingInput:
        return ForecastingInput(
            company_name=self.company_name,
            fiscal_year_start=self.fiscal_year_start,
            scope1_tco2e=self.scope1_tco2e,
            scope2_tco2e=self.scope2_tco2e,
            water_consumption_kl=self.water_consumption_kl,
            waste_generated_tonnes=self.waste_generated_tonnes,
            waste_recycled_tonnes=self.waste_recycled_tonnes,
            peer_group=peer_group,
            total_scope1_scope2_tco2e=self.total_scope1_scope2_tco2e,
        )

    def to_forecasting_inputs(self, peer_group: int | None = None) -> list[ForecastingInput]:
        records = [
            ForecastingInput(
                company_name=self.company_name,
                fiscal_year_start=record.fiscal_year_start,
                scope1_tco2e=record.scope1_tco2e,
                scope2_tco2e=record.scope2_tco2e,
                water_consumption_kl=record.water_consumption_kl,
                waste_generated_tonnes=record.waste_generated_tonnes,
                waste_recycled_tonnes=record.waste_recycled_tonnes,
                peer_group=peer_group,
                total_scope1_scope2_tco2e=record.total_scope1_scope2_tco2e,
            )
            for record in self.yearly_records
            if record.fiscal_year_start is not None
        ]

        if records:
            return sorted(records, key=lambda item: item.fiscal_year_start or 0)

        return [self.to_forecasting_input(peer_group=peer_group)]


class ExtractionQuality(BaseModel):
    score: float
    level: Literal["low", "medium", "high"]
    missing_required_fields: list[str]
    notes: list[str] = Field(default_factory=list)


class ClusterResult(BaseModel):
    KMeans_cluster: int
    KMeans_cluster_label: str
    peer_group: int
    confidence: Literal["low", "medium", "high"] = "medium"
    distances: list[float] = Field(default_factory=list)


class ForecastPoint(BaseModel):
    year: int
    total_scope1_scope2_tco2e: float
    source: Literal["model", "peer_average", "csv_fallback"]


class PeerComparison(BaseModel):
    peer_group: int
    peer_group_label: str
    company_count: int
    sample_row_count: int = 0
    benchmark_basis: str | None = None
    sample_companies: list[str]
    averages: dict[str, float]
    peer_forecast: list[ForecastPoint]


class ConsultantReport(BaseModel):
    executive_summary: str
    cluster_interpretation: str
    forecast_interpretation: str
    peer_benchmark: str
    risks: list[str]
    recommendations: list[str]
    chart_narratives: list[str] = Field(default_factory=list)
    confidence_note: str


class ChartPayload(BaseModel):
    emissions_forecast: list[dict[str, Any]]
    kpi_snapshot: list[dict[str, Any]]
    peer_benchmark: list[dict[str, Any]]
    kpi_trends: list[dict[str, Any]] = Field(default_factory=list)


class AnalysisResponse(BaseModel):
    session_id: str
    created_at: datetime
    source_pdf_id: str
    selected_pages: list[int]
    detected_years: list[str]
    target_years: list[str]
    extracted_kpis: ExtractedKpiPayload
    extraction_quality: ExtractionQuality
    cluster: ClusterResult
    forecast: list[ForecastPoint]
    peer_comparison: PeerComparison
    charts: ChartPayload
    consultant_report: ConsultantReport
    downloads: dict[str, str]


class SessionSummary(BaseModel):
    session_id: str
    created_at: datetime
    updated_at: datetime
    filename: str
    company_name: str | None = None
    fiscal_year: str | None = None
    cluster_label: str | None = None
    quality_level: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok"]
    openrouter_configured: bool
    parser_mode: str
    csv_database_ready: bool
    peer_database_ready: bool = False
    reference_data_source: str = "csv"
    model_paths_ready: bool


class ChatRequest(BaseModel):
    message: str
    model: str | None = None


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    at: datetime


class ChatResponse(BaseModel):
    session_id: str
    answer: str
    messages: list[ChatMessage]
