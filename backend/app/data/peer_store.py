from __future__ import annotations

import csv
from functools import cached_property
from pathlib import Path
from statistics import mean
import re
from typing import Any

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

Row = dict[str, Any]


class PeerStore:
    def __init__(self, settings: Settings):
        self.settings = settings

    @cached_property
    def cluster_summary(self) -> list[Row]:
        return _read_csv(self.settings.cluster_summary_csv)

    @cached_property
    def peer_groups(self) -> list[Row]:
        return _read_csv(self.settings.peer_groups_csv)

    @cached_property
    def cluster_forecast(self) -> list[Row]:
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

    def company_record(self, company_name: str | None, fiscal_year_start: int | None = None) -> Row | None:
        matches = self.company_records(company_name)
        if not matches:
            return None

        if fiscal_year_start is not None:
            for row in matches:
                if _safe_int(row.get("fiscal_year_start")) == fiscal_year_start:
                    return row

        return max(matches, key=lambda row: _safe_int(row.get("fiscal_year_start")) or 0)

    def company_records(self, company_name: str | None) -> list[Row]:
        if not company_name:
            return []

        target = _normalize_name(company_name)
        matches = [
            row
            for row in self.cluster_forecast
            if _normalize_name(row.get("company_name")) == target
        ]
        return sorted(matches, key=lambda row: _safe_int(row.get("fiscal_year_start")) or 0)

    def peer_comparison(self, peer_group: int, extracted=None, limit: int = 12, sample_size: int = 100) -> PeerComparison:
        rows = [
            row
            for row in self.cluster_forecast
            if _safe_int(row.get("peer_group") or row.get("KMeans_cluster")) == peer_group
        ]
        return build_peer_comparison(
            rows=rows,
            peer_group=peer_group,
            peer_group_label=self.cluster_label(peer_group),
            extracted=extracted,
            limit=limit,
            sample_size=sample_size,
            fallback_company_names=self.peer_company_names(peer_group, limit=limit),
            source_label="CSV",
        )

    def imputation_reference_rows(self, extracted=None, limit: int = 500) -> list[Row]:
        rows = list(self.cluster_forecast)
        if extracted is None:
            return rows[:limit]

        sub_sector_rows = _filter_rows(rows, "sub_sector", getattr(extracted, "sub_sector", None))
        sector_rows = _filter_rows(rows, "sector", getattr(extracted, "sector", None))
        nearest_rows = _nearest_numeric_rows(rows, extracted, limit=max(100, limit // 3))
        return _unique_rows([*sub_sector_rows, *sector_rows, *nearest_rows, *rows], limit=limit)


def build_peer_comparison(
    *,
    rows: list[Row],
    peer_group: int,
    peer_group_label: str,
    extracted=None,
    limit: int = 12,
    sample_size: int = 100,
    fallback_company_names: list[str] | None = None,
    source_label: str = "reference data",
) -> PeerComparison:
    sampled_rows = _sample_cluster_rows(rows, extracted, sample_size=sample_size)

    averages = {
        column: round(mean(values), 4)
        for column in NUMERIC_COLUMNS
        if (values := [_safe_float(row.get(column)) for row in sampled_rows if _safe_float(row.get(column)) is not None])
    }

    forecast_points: list[ForecastPoint] = []
    latest_year_values = [_safe_int(row.get("latest_year")) for row in sampled_rows if _safe_int(row.get("latest_year"))]
    latest_year = getattr(extracted, "fiscal_year_start", None) or (max(latest_year_values) if latest_year_values else 0)

    for step in range(1, 6):
        column = f"forecast_year_{step}"
        values = [_safe_float(row.get(column)) for row in sampled_rows if _safe_float(row.get(column)) is not None]
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
        peer_group_label=peer_group_label,
        company_count=len({row.get("company_name") for row in sampled_rows if row.get("company_name")}),
        sample_row_count=len(sampled_rows),
        benchmark_basis=_benchmark_basis(rows, sampled_rows, extracted, source_label=source_label),
        sample_companies=_sample_company_names(sampled_rows, limit=limit) or (fallback_company_names or []),
        averages=averages,
        peer_forecast=forecast_points,
    )


def _read_csv(path: Path) -> list[Row]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _sample_cluster_rows(rows: list[Row], extracted, sample_size: int = 100) -> list[Row]:
    if not rows:
        return []

    sample_size = max(50, min(sample_size, 100))
    target_company = _normalize_name(getattr(extracted, "company_name", None))
    candidate_rows = [
        row
        for row in rows
        if not target_company or _normalize_name(row.get("company_name")) != target_company
    ]
    if len(candidate_rows) < 50:
        candidate_rows = rows

    known = {
        column: getattr(extracted, column, None)
        for column in [
            "scope1_tco2e",
            "scope2_tco2e",
            "water_consumption_kl",
            "waste_generated_tonnes",
            "waste_recycled_tonnes",
            "total_scope1_scope2_tco2e",
        ]
        if extracted is not None and getattr(extracted, column, None) is not None
    }
    if not known:
        return candidate_rows[:sample_size]

    scored: list[tuple[float, str, Row]] = []
    for row in candidate_rows:
        distance = 0.0
        matches = 0
        for column, value in known.items():
            row_value = _safe_float(row.get(column))
            if row_value is None:
                continue
            distance += abs(_log_scale(value) - _log_scale(row_value))
            matches += 1
        if matches:
            distance /= matches
        else:
            distance = float("inf")
        scored.append((distance, str(row.get("company_name") or ""), row))

    return [row for _, _, row in sorted(scored, key=lambda item: (item[0], item[1]))[:sample_size]]


def _sample_company_names(rows: list[Row], limit: int) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for row in rows:
        name = str(row.get("company_name") or "").strip()
        key = _normalize_name(name)
        if not name or key in seen:
            continue
        names.append(name)
        seen.add(key)
        if len(names) >= limit:
            break
    return names


def _benchmark_basis(all_rows: list[Row], sampled_rows: list[Row], extracted, *, source_label: str) -> str:
    total = len(all_rows)
    sample = len(sampled_rows)
    if sample == 0:
        return f"No {source_label} peer rows were available for this KMeans cluster."
    if extracted is None:
        return f"Benchmark uses the first {sample} {source_label} rows from {total} rows in the assigned KMeans cluster."
    return (
        f"Benchmark uses {sample} nearest {source_label} row sample(s) from {total} rows in the assigned "
        "KMeans cluster, ranked by similarity to the extracted company KPI scale."
    )


def _log_scale(value: float | int | str) -> float:
    numeric = _safe_float(str(value))
    if numeric is None:
        return 0.0
    import math

    return math.log1p(abs(numeric))


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_name(value: Any) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    return re.sub(r"\s+", " ", cleaned)


def _filter_rows(rows: list[Row], field: str, value: str | None) -> list[Row]:
    if not value:
        return []
    target = _normalize_name(value)
    return [row for row in rows if _normalize_name(row.get(field)) == target]


def _nearest_numeric_rows(rows: list[Row], extracted, limit: int = 100) -> list[Row]:
    if extracted is None:
        return []

    known = {
        column: getattr(extracted, column, None)
        for column in [
            "scope1_tco2e",
            "scope2_tco2e",
            "water_consumption_kl",
            "waste_generated_tonnes",
            "waste_recycled_tonnes",
            "total_scope1_scope2_tco2e",
        ]
        if getattr(extracted, column, None) is not None
    }
    if not known:
        return []

    scored: list[tuple[float, str, Row]] = []
    for row in rows:
        distance = 0.0
        matches = 0
        for column, value in known.items():
            row_value = _safe_float(row.get(column))
            if row_value is None:
                continue
            distance += abs(_log_scale(value) - _log_scale(row_value))
            matches += 1
        if matches:
            scored.append((distance / matches, str(row.get("company_name") or ""), row))

    return [row for _, _, row in sorted(scored, key=lambda item: (item[0], item[1]))[:limit]]


def _unique_rows(rows: list[Row], limit: int) -> list[Row]:
    unique: list[Row] = []
    seen: set[str] = set()
    for row in rows:
        key = str(row.get("id") or row.get("_id") or f"{row.get('company_name')}:{row.get('fiscal_year_start')}")
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
        if len(unique) >= limit:
            break
    return unique
