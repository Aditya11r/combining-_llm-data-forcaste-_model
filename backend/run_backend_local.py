from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SITE_PACKAGES = ROOT / ".venv" / "Lib" / "site-packages"
LOG_PATH = ROOT / "runtime" / "backend-local.log"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SITE_PACKAGES))

import uvicorn


if __name__ == "__main__":
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_file = LOG_PATH.open("a", encoding="utf-8", buffering=1)
    sys.stdout = log_file
    sys.stderr = log_file
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000)
