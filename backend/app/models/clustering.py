from __future__ import annotations

import math
from functools import cached_property

from app.config import Settings
from app.data.peer_store import PeerStore
from app.schemas import ClusterResult, ClusteringInput


class ClusteringService:
    feature_columns = [
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
    numeric_feature_columns = [
        "scope1_tco2e",
        "scope2_tco2e",
        "water_consumption_kl",
        "waste_generated_tonnes",
        "waste_recycled_tonnes",
        "states_served",
        "countries_served",
    ]

    def __init__(self, settings: Settings, peer_store: PeerStore):
        self.settings = settings
        self.peer_store = peer_store

    @cached_property
    def _artifacts(self):
        import joblib

        return {
            "preprocessor": joblib.load(self.settings.preprocessor_path),
            "pca": joblib.load(self.settings.pca_path),
            "kmeans": joblib.load(self.settings.kmeans_model_path),
        }

    def predict(self, payload: ClusteringInput) -> ClusterResult:
        self._validate_payload(payload)

        import pandas as pd

        row = {column: getattr(payload, column) for column in self.feature_columns}
        for column in self.numeric_feature_columns:
            if row.get(column) is not None:
                row[column] = math.log1p(max(float(row[column]), 0.0))
        frame = pd.DataFrame([row], columns=self.feature_columns)

        artifacts = self._artifacts
        prepared = artifacts["preprocessor"].transform(frame)
        cluster_features = _features_for_kmeans(
            prepared=prepared,
            pca=artifacts["pca"],
            kmeans=artifacts["kmeans"],
        )
        cluster_id = int(artifacts["kmeans"].predict(cluster_features)[0])

        distances: list[float] = []
        confidence = "medium"
        if hasattr(artifacts["kmeans"], "transform"):
            raw_distances = artifacts["kmeans"].transform(cluster_features)[0]
            distances = [float(value) for value in raw_distances]
            confidence = _confidence_from_distances(distances, cluster_id)

        label = self.peer_store.cluster_label(cluster_id)
        return ClusterResult(
            KMeans_cluster=cluster_id,
            KMeans_cluster_label=label,
            # Legacy alias: peer_group is redundant and always mirrors KMeans_cluster.
            peer_group=cluster_id,
            confidence=confidence,
            distances=distances,
        )

    def fallback_from_peer_group(self, peer_group: int) -> ClusterResult:
        return ClusterResult(
            KMeans_cluster=peer_group,
            KMeans_cluster_label=self.peer_store.cluster_label(peer_group),
            # Legacy alias: peer_group is redundant and always mirrors KMeans_cluster.
            peer_group=peer_group,
            confidence="low",
            distances=[],
        )

    def _validate_payload(self, payload: ClusteringInput) -> None:
        missing = [column for column in self.feature_columns if getattr(payload, column) in (None, "")]
        if missing:
            raise ValueError(f"Missing clustering fields: {', '.join(missing)}")


def _features_for_kmeans(*, prepared, pca, kmeans):
    expected = getattr(kmeans, "n_features_in_", None)
    prepared_count = getattr(prepared, "shape", [None, None])[1]
    if expected is None or expected == prepared_count:
        return prepared

    reduced = pca.transform(prepared)
    reduced_count = getattr(reduced, "shape", [None, None])[1]
    if expected == reduced_count:
        return reduced

    raise ValueError(
        f"KMeans expects {expected} feature(s), but preprocessor produced {prepared_count} "
        f"and PCA produced {reduced_count}."
    )


def _confidence_from_distances(distances: list[float], cluster_id: int) -> str:
    if not distances:
        return "medium"

    sorted_distances = sorted(distances)
    nearest = distances[cluster_id]
    if len(sorted_distances) == 1:
        return "medium"

    second = sorted_distances[1]
    if nearest <= 0:
        return "high"

    margin = (second - nearest) / max(nearest, 1e-9)
    if margin >= 0.35:
        return "high"
    if margin >= 0.12:
        return "medium"
    return "low"
