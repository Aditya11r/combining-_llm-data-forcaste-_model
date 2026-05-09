from __future__ import annotations

import csv
from functools import cached_property
from pathlib import Path
from statistics import mean
import re

from app.config import Settings
from app.schemas import ForecastPoint, PeerComparison


NUMERIC_COLUMNS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
    "total_scope1_scope2_tco2e",
    "waste_recycled_pct",
    "emissions_intensity_tco2e_per_cr",
    "water_intensity_kl_per_cr",
]


class PeerStore:
    def __init__(self, settings: Settings):
        self.settings = settings

    @cached_property
    def cluster_summary(self) -> list[dict[str, str]]:
        return _read_csv(self.settings.cluster_summary_csv)

    @cached_property
    def peer_groups(self) -> list[dict[str, str]]:
        return _read_csv(self.settings.peer_groups_csv)

    @cached_property
    def cluster_forecast(self) -> list[dict[str, str]]:
        return _read_csv(self.settings.cluster_forecast_csv)

    def ready(self) -> bool:
        return all(
            path.exists()
            for path in [
                self.settings.cluster_summary_csv,
                self.settings.peer_groups_csv,
                self.settings.cluster_forecast_csv,
            ]
        )

    def cluster_label(self, cluster_id: int) -> str:
        for row in self.cluster_summary:
            if _safe_int(row.get("KMeans_cluster")) == cluster_id:
                return row.get("KMeans_cluster_label") or f"Cluster {cluster_id}"
        return f"Cluster {cluster_id}"

    def peer_company_names(self, peer_group: int, limit: int = 12) -> list[str]:
        for row in self.peer_groups:
            if _safe_int(row.get("KMeans_cluster")) == peer_group:
                names = [name.strip() for name in (row.get("company_names") or "").split("|")]
                return [name for name in names if name][:limit]
        return []

    def company_record(self, company_name: str | None, fiscal_year_start: int | None = None) -> dict[str, str] | None:
        matches = self.company_records(company_name)
        if not matches:
            return None

        if fiscal_year_start is not None:
            for row in matches:
                if _safe_int(row.get("fiscal_year_start")) == fiscal_year_start:
                    return row

        return max(matches, key=lambda row: _safe_int(row.get("fiscal_year_start")) or 0)

    def company_records(self, company_name: str | None) -> list[dict[str, str]]:
        if not company_name:
            return []

        target = _normalize_name(company_name)
        matches = [
            row
            for row in self.cluster_forecast
            if _normalize_name(row.get("company_name")) == target
        ]
        return sorted(matches, key=lambda row: _safe_int(row.get("fiscal_year_start")) or 0)

    def peer_comparison(self, peer_group: int, limit: int = 12) -> PeerComparison:
        rows = [
            row
            for row in self.cluster_forecast
            if _safe_int(row.get("peer_group") or row.get("KMeans_cluster")) == peer_group
        ]

        averages = {
            column: round(mean(values), 4)
            for column in NUMERIC_COLUMNS
            if (values := [_safe_float(row.get(column)) for row in rows if _safe_float(row.get(column)) is not None])
        }

        forecast_points: list[ForecastPoint] = []
        latest_year_values = [_safe_int(row.get("latest_year")) for row in rows if _safe_int(row.get("latest_year"))]
        latest_year = max(latest_year_values) if latest_year_values else 0

        for step in range(1, 6):
            column = f"forecast_year_{step}"
            values = [_safe_float(row.get(column)) for row in rows if _safe_float(row.get(column)) is not None]
            if not values:
                continue
            forecast_points.append(
                ForecastPoint(
                    year=latest_year + step,
                    total_scope1_scope2_tco2e=round(mean(values), 4),
                    source="peer_average",
                )
            )

        return PeerComparison(
            peer_group=peer_group,
            peer_group_label=self.cluster_label(peer_group),
            company_count=len({row.get("company_name") for row in rows if row.get("company_name")}),
            sample_companies=self.peer_company_names(peer_group, limit=limit),
            averages=averages,
            peer_forecast=forecast_points,
        )


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _safe_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", cleaned)
