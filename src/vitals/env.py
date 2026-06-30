"""Call-time resolution of the two environment signals that let one codebase serve three homes:
local DuckDB, databricks-connect (laptop drives remote Spark), and on-cluster serverless. Resolved
at call time (not import) so the wheel entry point can set them before use, and so they're testable.
Defaults reproduce the original behaviour exactly — see docs/adr/0005."""
from __future__ import annotations

import os
from pathlib import Path

_DEFAULT_BRONZE = Path(__file__).resolve().parents[2] / "data" / "bronze"


def bronze_dir() -> Path:
    """Directory for raw NDJSON. Default = repo data/bronze (local + connect unchanged); the bundle's
    medallion task overrides it to a writable /tmp dir via VITALS_BRONZE_DIR."""
    return Path(os.environ.get("VITALS_BRONZE_DIR", str(_DEFAULT_BRONZE)))


def spark_mode() -> str:
    """'ambient' when running ON Databricks compute (the wheel entry point sets it); 'serverless' for
    the databricks-connect dev path (laptop drives remote serverless). Default keeps connect unchanged."""
    return os.environ.get("VITALS_SPARK_MODE", "serverless")
