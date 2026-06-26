"""Lakehouse backends — the same medallion pipeline against different engines.

`VITALS_TARGET` selects where layers are written:
  - "local"      -> DuckDB single-file lakehouse (clone-and-run default; see vitals.lakehouse)
  - "databricks" -> Delta tables in Unity Catalog on Databricks (see databricks_delta)

The local DuckDB path stays the zero-infra default (ADR 0001); the Databricks path is the
deployment target wired in ADR 0005 (databricks-connect against serverless).
"""
from __future__ import annotations

import os


def target() -> str:
    """The selected backend name, from VITALS_TARGET (default 'local')."""
    return os.environ.get("VITALS_TARGET", "local").lower()
