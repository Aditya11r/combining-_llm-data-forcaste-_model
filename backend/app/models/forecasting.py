from __future__ import annotations

from functools import cached_property

from app.config import Settings
from app.schemas import ForecastPoint, ForecastingInput


DEFAULT_FORECAST_FEATURE_COLUMNS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
    "total_scope1_scope2_tco2e",
]

TARGET_COLUMN = "total_scope1_scope2_tco2e"


class ForecastingService:
    def __init__(self, settings: Settings):
        self.settings = settings

    @cached_property
    def _model(self):
        from tensorflow import keras

        return keras.models.load_model(self.settings.lstm_model_path)

    @cached_property
    def _scaler(self):
        import joblib

        return joblib.load(self.settings.lstm_scaler_path)

    def forecast(self, records: list[ForecastingInput]) -> list[ForecastPoint]:
        clean_records = [record for record in records if record.computed_total_scope1_scope2_tco2e is not None]
        if not clean_records:
            raise ValueError("No records with total_scope1_scope2_tco2e are available for forecasting")

        horizon = 1 if len(clean_records) == 1 else 5
        latest = max(clean_records, key=lambda item: item.fiscal_year_start or 0)

        try:
            return self._forecast_with_keras(clean_records, horizon)
        except Exception:
            return self._deterministic_fallback(latest, horizon)

    def _forecast_with_keras(self, records: list[ForecastingInput], horizon: int) -> list[ForecastPoint]:
        import numpy as np
        import pandas as pd

        model = self._model
        scaler = self._scaler
        feature_columns = _scaler_feature_columns(scaler)
        model_shape = getattr(model, "input_shape", None)
        timesteps = 1
        if isinstance(model_shape, tuple) and len(model_shape) >= 2 and model_shape[1]:
            timesteps = int(model_shape[1])

        sorted_records = sorted(records, key=lambda item: item.fiscal_year_start or 0)
        matrix = pd.DataFrame(
            [_record_to_feature_map(record, feature_columns) for record in sorted_records],
            columns=feature_columns,
        )
        scaled = scaler.transform(matrix)

        if TARGET_COLUMN not in feature_columns:
            raise ValueError("Forecast scaler does not include total_scope1_scope2_tco2e for inverse transform")

        if len(scaled) < timesteps:
            pad = np.repeat(scaled[:1], timesteps - len(scaled), axis=0)
            sequence = np.vstack([pad, scaled])
        else:
            sequence = scaled[-timesteps:]

        current_sequence = sequence.copy()
        latest_year = sorted_records[-1].fiscal_year_start or 0
        points: list[ForecastPoint] = []

        for step in range(1, horizon + 1):
            prediction = model.predict(current_sequence.reshape(1, timesteps, current_sequence.shape[1]), verbose=0)
            predicted_scaled_total = float(np.ravel(prediction)[0])

            next_scaled = current_sequence[-1].copy()
            total_index = feature_columns.index(TARGET_COLUMN)
            next_scaled[total_index] = predicted_scaled_total

            inverse_frame = pd.DataFrame([next_scaled], columns=feature_columns)
            inverse = scaler.inverse_transform(inverse_frame)[0]
            predicted_total = max(float(inverse[total_index]), 0.0)

            points.append(
                ForecastPoint(
                    year=latest_year + step,
                    total_scope1_scope2_tco2e=round(predicted_total, 4),
                    source="model",
                )
            )
            current_sequence = np.vstack([current_sequence[1:], next_scaled])

        return points

    def _deterministic_fallback(self, latest: ForecastingInput, horizon: int) -> list[ForecastPoint]:
        base = latest.computed_total_scope1_scope2_tco2e or 0.0
        latest_year = latest.fiscal_year_start or 0
        growth = 0.05

        points: list[ForecastPoint] = []
        for step in range(1, horizon + 1):
            points.append(
                ForecastPoint(
                    year=latest_year + step,
                    total_scope1_scope2_tco2e=round(base * ((1 + growth) ** step), 4),
                    source="csv_fallback",
                )
            )
        return points


def _scaler_feature_columns(scaler) -> list[str]:
    names = getattr(scaler, "feature_names_in_", None)
    if names is not None:
        return [str(name) for name in names]
    return DEFAULT_FORECAST_FEATURE_COLUMNS


def _record_to_feature_map(record: ForecastingInput, feature_columns: list[str]) -> dict[str, float]:
    values = {
        "scope1_tco2e": record.scope1_tco2e,
        "scope2_tco2e": record.scope2_tco2e,
        "water_consumption_kl": record.water_consumption_kl,
        "waste_generated_tonnes": record.waste_generated_tonnes,
        "waste_recycled_tonnes": record.waste_recycled_tonnes,
        TARGET_COLUMN: record.computed_total_scope1_scope2_tco2e,
    }
    return {column: float(values.get(column) or 0) for column in feature_columns}
