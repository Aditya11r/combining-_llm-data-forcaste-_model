from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
    candidates = [
        Path.cwd() / ".env",
        BACKEND_ROOT / ".env",
    ]
    for path in candidates:
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _path(name: str, default: str) -> Path:
    path = Path(_env(name, default)).expanduser()
    if not path.is_absolute():
        return BACKEND_ROOT / path
    return path


@dataclass(frozen=True)
class Settings:
    openrouter_api_key: str
    openrouter_base_url: str
    openrouter_extraction_model: str
    openrouter_report_model: str
    openrouter_http_referer: str
    openrouter_app_title: str

    kmeans_model_path: Path
    preprocessor_path: Path
    pca_path: Path
    lstm_model_path: Path
    lstm_scaler_path: Path

    cluster_summary_csv: Path
    peer_groups_csv: Path
    cluster_forecast_csv: Path

    mongodb_uri: str
    mongodb_database: str
    mongodb_collection: str
    mongodb_timeout_ms: int
    mongodb_peer_query_limit: int

    old_parser_module: str
    old_parser_function: str

    runtime_dir: Path
    max_upload_mb: int
    frontend_origin: str

    @property
    def upload_dir(self) -> Path:
        return self.runtime_dir / "uploads"

    @property
    def sessions_dir(self) -> Path:
        return self.runtime_dir / "sessions"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _load_dotenv()
    default_root = BACKEND_ROOT / "artifacts"

    return Settings(
        openrouter_api_key=_env("OPENROUTER_API_KEY"),
        openrouter_base_url=_env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
        openrouter_extraction_model=_env("OPENROUTER_EXTRACTION_MODEL", "openai/gpt-4o-mini"),
        openrouter_report_model=_env("OPENROUTER_REPORT_MODEL", "openai/gpt-4o"),
        openrouter_http_referer=_env("OPENROUTER_HTTP_REFERER", "http://localhost:5173"),
        openrouter_app_title=_env("OPENROUTER_APP_TITLE", "ESG PDF Intelligence"),
        kmeans_model_path=_path("KMEANS_MODEL_PATH", str(default_root / "models" / "kmeans_model.pkl")),
        preprocessor_path=_path("PREPROCESSOR_PATH", str(default_root / "models" / "preprocessor.pkl")),
        pca_path=_path("PCA_PATH", str(default_root / "models" / "pca.pkl")),
        lstm_model_path=_path("LSTM_MODEL_PATH", str(default_root / "models" / "lstm_model.keras")),
        lstm_scaler_path=_path("LSTM_SCALER_PATH", str(default_root / "models" / "lstm_scaler.pkl")),
        cluster_summary_csv=_path("CLUSTER_SUMMARY_CSV", str(default_root / "data" / "cluster_summary.csv")),
        peer_groups_csv=_path("PEER_GROUPS_CSV", str(default_root / "data" / "peer_groups.csv")),
        cluster_forecast_csv=_path("CLUSTER_FORECAST_CSV", str(default_root / "data" / "cluster_forecast.csv")),
        mongodb_uri=_env("MONGODB_URI"),
        mongodb_database=_env("MONGODB_DATABASE", "brsr_pipeline"),
        mongodb_collection=_env("MONGODB_COLLECTION", "cluster_forecast"),
        mongodb_timeout_ms=int(_env("MONGODB_TIMEOUT_MS", "10000")),
        mongodb_peer_query_limit=int(_env("MONGODB_PEER_QUERY_LIMIT", "2000")),
        old_parser_module=_env("OLD_PARSER_MODULE"),
        old_parser_function=_env("OLD_PARSER_FUNCTION", "prepare_pdf_context"),
        runtime_dir=_path("RUNTIME_DIR", "runtime"),
        max_upload_mb=int(_env("MAX_UPLOAD_MB", "40")),
        frontend_origin=_env("FRONTEND_ORIGIN", "http://localhost:5173"),
    )
