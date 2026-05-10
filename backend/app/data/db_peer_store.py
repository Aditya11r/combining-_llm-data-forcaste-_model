from __future__ import annotations

import re
from functools import cached_property
from typing import Any

from app.config import Settings
from app.data.peer_store import PeerStore, Row, _normalize_name, _safe_int, build_peer_comparison
from app.schemas import PeerComparison


REFERENCE_FIELDS = {
    "_id": 1,
    "id": 1,
    "company_name": 1,
    "sector": 1,
    "sub_sector": 1,
    "states_served": 1,
    "countries_served": 1,
    "scope1_tco2e": 1,
    "scope2_tco2e": 1,
    "water_consumption_kl": 1,
    "waste_generated_tonnes": 1,
    "waste_recycled_tonnes": 1,
    "total_scope1_scope2_tco2e": 1,
    "waste_recycled_pct": 1,
    "emissions_intensity_tco2e_per_cr": 1,
    "water_intensity_kl_per_cr": 1,
    "fiscal_year_start": 1,
    "latest_year": 1,
    "forecast_year_1": 1,
    "forecast_year_2": 1,
    "forecast_year_3": 1,
    "forecast_year_4": 1,
    "forecast_year_5": 1,
    "KMeans_cluster": 1,
    "KMeans_cluster_label": 1,
    "peer_group": 1,
}


class MongoPeerStore:
    def __init__(self, settings: Settings, csv_fallback: PeerStore | None = None):
        self.settings = settings
        self.csv_fallback = csv_fallback

    @cached_property
    def _client(self):
        from pymongo import MongoClient

        return MongoClient(
            self.settings.mongodb_uri,
            serverSelectionTimeoutMS=self.settings.mongodb_timeout_ms,
            connectTimeoutMS=self.settings.mongodb_timeout_ms,
        )

    @cached_property
    def _collection(self):
        return self._client[self.settings.mongodb_database][self.settings.mongodb_collection]

    def ready(self) -> bool:
        if not self.settings.mongodb_uri:
            return False
        try:
            self._client.admin.command("ping")
            return self._collection.find_one({}, {"_id": 1}) is not None
        except Exception:
            return False

    def cluster_label(self, cluster_id: int) -> str:
        row = self._collection.find_one(_cluster_query(cluster_id), {"KMeans_cluster_label": 1})
        if row and row.get("KMeans_cluster_label"):
            return str(row["KMeans_cluster_label"])
        if self.csv_fallback is not None:
            return self.csv_fallback.cluster_label(cluster_id)
        return f"Cluster {cluster_id}"

    def peer_company_names(self, peer_group: int, limit: int = 12) -> list[str]:
        rows = self._find_rows(_cluster_query(peer_group), projection={"company_name": 1}, limit=limit * 8)
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
        if names:
            return names
        if self.csv_fallback is not None:
            return self.csv_fallback.peer_company_names(peer_group, limit=limit)
        return []

    def peer_comparison(self, peer_group: int, extracted=None, limit: int = 12, sample_size: int = 100) -> PeerComparison:
        rows = self._find_rows(
            _cluster_query(peer_group),
            projection=REFERENCE_FIELDS,
            limit=self.settings.mongodb_peer_query_limit,
        )
        return build_peer_comparison(
            rows=rows,
            peer_group=peer_group,
            peer_group_label=self.cluster_label(peer_group),
            extracted=extracted,
            limit=limit,
            sample_size=sample_size,
            fallback_company_names=self.peer_company_names(peer_group, limit=limit),
            source_label="database",
        )

    def imputation_reference_rows(self, extracted=None, limit: int = 500) -> list[Row]:
        if extracted is None:
            return self._find_rows({}, projection=REFERENCE_FIELDS, limit=limit)

        per_group_limit = max(100, limit // 3)
        rows: list[Row] = []

        sub_sector = getattr(extracted, "sub_sector", None)
        if sub_sector:
            rows.extend(
                self._find_rows(
                    _text_exact_query("sub_sector", sub_sector),
                    projection=REFERENCE_FIELDS,
                    limit=per_group_limit,
                )
            )

        sector = getattr(extracted, "sector", None)
        if sector:
            rows.extend(
                self._find_rows(
                    _text_exact_query("sector", sector),
                    projection=REFERENCE_FIELDS,
                    limit=per_group_limit,
                )
            )

        rows.extend(self._find_rows({}, projection=REFERENCE_FIELDS, limit=per_group_limit))
        return _unique_rows(rows, limit=limit)

    def company_record(self, company_name: str | None, fiscal_year_start: int | None = None) -> Row | None:
        records = self.company_records(company_name)
        if not records:
            return None
        if fiscal_year_start is not None:
            for row in records:
                if _safe_int(row.get("fiscal_year_start")) == fiscal_year_start:
                    return row
        return max(records, key=lambda row: _safe_int(row.get("fiscal_year_start")) or 0)

    def company_records(self, company_name: str | None) -> list[Row]:
        if not company_name:
            return []
        return self._find_rows(
            _text_exact_query("company_name", company_name),
            projection=REFERENCE_FIELDS,
            limit=100,
        )

    def _find_rows(self, query: dict[str, Any], *, projection: dict[str, int], limit: int) -> list[Row]:
        cursor = self._collection.find(query, projection).limit(limit)
        return [_clean_row(row) for row in cursor]


def _cluster_query(cluster_id: int) -> dict[str, Any]:
    values = [cluster_id, float(cluster_id), str(cluster_id)]
    return {
        "$or": [
            {"KMeans_cluster": {"$in": values}},
            {"peer_group": {"$in": values}},
        ]
    }


def _text_exact_query(field: str, value: str) -> dict[str, Any]:
    return {field: {"$regex": f"^{re.escape(str(value).strip())}$", "$options": "i"}}


def _clean_row(row: dict[str, Any]) -> Row:
    if "_id" in row:
        row["_id"] = str(row["_id"])
    return row


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
