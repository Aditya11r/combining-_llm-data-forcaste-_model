from __future__ import annotations

import math
from functools import cached_property

from app.config import Settings
from app.schemas import ForecastPoint, ForecastingInput


NUMERIC_FORECAST_FEATURE_COLUMNS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
]

PEER_GROUP_COLUMN = "peer_group"
DEFAULT_FORECAST_FEATURE_COLUMNS = [*NUMERIC_FORECAST_FEATURE_COLUMNS, PEER_GROUP_COLUMN]
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

        horizon = 5
        return self._forecast_with_keras(clean_records, horizon)

    def _forecast_with_keras(self, records: list[ForecastingInput], horizon: int) -> list[ForecastPoint]:
        import numpy as np
        import pandas as pd

        model = self._model
        scaler = self._scaler
        scaler_columns = _scaler_feature_columns(scaler)
        feature_columns = _model_feature_columns(model, scaler_columns)
        timesteps = _model_timesteps(model)

        sorted_records = sorted(records, key=lambda item: item.fiscal_year_start or 0)
        raw_sequence = pd.DataFrame(
            [_record_to_feature_map(record, feature_columns) for record in sorted_records],
            columns=feature_columns,
        )

        if len(raw_sequence) < timesteps:
            pad = pd.concat([raw_sequence.iloc[[0]]] * (timesteps - len(raw_sequence)), ignore_index=True)
            current_sequence = pd.concat([pad, raw_sequence], ignore_index=True)
        else:
            current_sequence = raw_sequence.tail(timesteps).reset_index(drop=True)

        latest_year = sorted_records[-1].fiscal_year_start or 0
        points: list[ForecastPoint] = []

        for step in range(1, horizon + 1):
            model_input = _scale_model_sequence(current_sequence, scaler, scaler_columns, feature_columns)
            prediction = model.predict(model_input.reshape(1, timesteps, len(feature_columns)), verbose=0)
            predicted_log_total = float(np.ravel(prediction)[0])
            predicted_total = max(math.expm1(predicted_log_total), 0.0)

            next_raw = _advance_scope_features(current_sequence.iloc[-1], predicted_total)

            points.append(
                ForecastPoint(
                    year=latest_year + step,
                    total_scope1_scope2_tco2e=round(predicted_total, 4),
                    source="model",
                )
            )
            current_sequence = pd.concat(
                [current_sequence.iloc[1:], pd.DataFrame([next_raw], columns=feature_columns)],
                ignore_index=True,
            )

        return points


def _model_timesteps(model) -> int:
    model_shape = getattr(model, "input_shape", None)
    if isinstance(model_shape, list) and model_shape:
        model_shape = model_shape[0]
    if isinstance(model_shape, tuple) and len(model_shape) >= 2 and model_shape[1]:
        return int(model_shape[1])
    return 1


def _model_feature_columns(model, scaler_columns: list[str]) -> list[str]:
    model_shape = getattr(model, "input_shape", None)
    if isinstance(model_shape, list) and model_shape:
        model_shape = model_shape[0]

    expected_count = None
    if isinstance(model_shape, tuple) and model_shape and model_shape[-1]:
        expected_count = int(model_shape[-1])

    if expected_count is None:
        return DEFAULT_FORECAST_FEATURE_COLUMNS
    if expected_count == len(DEFAULT_FORECAST_FEATURE_COLUMNS):
        return DEFAULT_FORECAST_FEATURE_COLUMNS
    if expected_count == len(scaler_columns):
        return scaler_columns
    if expected_count == len(scaler_columns) + 1:
        return [*scaler_columns, PEER_GROUP_COLUMN]

    raise ValueError(
        "LSTM model expects "
        f"{expected_count} feature(s), but the scaler exposes {len(scaler_columns)} "
        f"feature(s): {', '.join(scaler_columns)}"
    )


def _scaler_feature_columns(scaler) -> list[str]:
    names = getattr(scaler, "feature_names_in_", None)
    if names is not None:
        return [str(name) for name in names]
    return NUMERIC_FORECAST_FEATURE_COLUMNS


def _scale_model_sequence(raw_sequence, scaler, scaler_columns: list[str], feature_columns: list[str]):
    import numpy as np

    numeric_columns = [column for column in scaler_columns if column != PEER_GROUP_COLUMN]
    missing_numeric = [column for column in numeric_columns if column not in raw_sequence.columns]
    if missing_numeric:
        raise ValueError(f"Missing LSTM numeric feature(s): {', '.join(missing_numeric)}")

    numeric_values = raw_sequence[numeric_columns].apply(lambda column: column.map(_log1p_nonnegative))
    scaled_numeric = scaler.transform(numeric_values)

    scaled_by_column = {
        column: scaled_numeric[:, index]
        for index, column in enumerate(numeric_columns)
    }

    if PEER_GROUP_COLUMN in feature_columns:
        scaled_by_column[PEER_GROUP_COLUMN] = raw_sequence[PEER_GROUP_COLUMN].astype(float).to_numpy()

    return np.column_stack(
        [
            scaled_by_column.get(column, np.zeros(len(raw_sequence), dtype=float))
            for column in feature_columns
        ]
    )


def _record_to_feature_map(record: ForecastingInput, feature_columns: list[str]) -> dict[str, float]:
    values = {
        "scope1_tco2e": record.scope1_tco2e,
        "scope2_tco2e": record.scope2_tco2e,
        "water_consumption_kl": record.water_consumption_kl,
        "waste_generated_tonnes": record.waste_generated_tonnes,
        "waste_recycled_tonnes": record.waste_recycled_tonnes,
        PEER_GROUP_COLUMN: record.peer_group,
    }
    return {column: float(values.get(column) or 0) for column in feature_columns}


def _advance_scope_features(row, predicted_total: float) -> dict[str, float]:
    next_row = {column: float(row.get(column, 0) or 0) for column in row.index}
    current_scope1 = max(next_row.get("scope1_tco2e", 0.0), 0.0)
    current_scope2 = max(next_row.get("scope2_tco2e", 0.0), 0.0)
    current_total = current_scope1 + current_scope2

    if current_total > 0:
        scope1_ratio = current_scope1 / current_total
    else:
        scope1_ratio = 0.5

    next_row["scope1_tco2e"] = predicted_total * scope1_ratio
    next_row["scope2_tco2e"] = predicted_total * (1 - scope1_ratio)
    return next_row


def _log1p_nonnegative(value) -> float:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        numeric_value = 0.0
    return math.log1p(max(numeric_value, 0.0))
