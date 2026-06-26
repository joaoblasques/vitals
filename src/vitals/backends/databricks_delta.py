"""Databricks backend — land bronze as Delta tables in Unity Catalog.

Bronze-first slice of the Delta-on-UC writer (see
docs/superpowers/specs/2026-06-26-delta-on-uc-writer-design.md). Uploads the raw NDJSON into the
`vitals_bronze.raw.landing` UC volume, then writes one Delta table per source into
`vitals_bronze.raw.*` — raw and as-is, schema-inferred (FHIR nesting preserved), no de-id yet
(PHI boundary is still at silver).

Execution: databricks-connect against Free Edition serverless (ADR 0005). Auth comes from the
DATABRICKS_HOST / DATABRICKS_TOKEN env vars (source infra/terraform/.env first).

I/O (upload + Spark writes) is kept separate from the pure parity check (`parity_report`) so the
logic is unit-testable without a workspace.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BRONZE_DIR = ROOT / "data" / "bronze"

CATALOG = "vitals_bronze"
SCHEMA = "raw"
VOLUME = "landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# The eight raw sources landed in bronze (mirrors vitals.lakehouse).
SOURCES = [
    "patients", "encounters", "conditions", "observations",
    "notes", "claims", "pro_surveys", "wearables",
]


# ---- pure logic (unit-testable, no I/O) -------------------------------------------------------

def parity_report(local: dict[str, int], remote: dict[str, int]) -> dict[str, dict]:
    """Compare local (DuckDB/NDJSON) vs remote (Delta) row counts per source.

    Returns {source: {local, remote, match}}. The acceptance gate for the bronze slice is that
    every source matches (project principle: verify every step against row counts).
    """
    report = {}
    for name in sorted(set(local) | set(remote)):
        lc, rc = local.get(name), remote.get(name)
        report[name] = {"local": lc, "remote": rc, "match": lc == rc and lc is not None}
    return report


def all_match(report: dict[str, dict]) -> bool:
    return bool(report) and all(r["match"] for r in report.values())


def local_counts() -> dict[str, int]:
    """Row count per source from the local NDJSON (the bronze parity baseline)."""
    counts = {}
    for name in SOURCES:
        path = BRONZE_DIR / f"{name}.ndjson"
        with path.open("rb") as fh:
            counts[name] = sum(1 for _ in fh)
    return counts


# ---- I/O (requires a live workspace) ----------------------------------------------------------

def _spark():
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless().getOrCreate()


def _upload_landing() -> None:
    """Upload each raw NDJSON into the bronze landing volume (overwrite = idempotent)."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    for name in SOURCES:
        local = BRONZE_DIR / f"{name}.ndjson"
        with local.open("rb") as fh:
            w.files.upload(f"{VOLUME_PATH}/{name}.ndjson", fh, overwrite=True)


def land_bronze() -> dict[str, int]:
    """Upload NDJSON to the volume, write one Delta table per source, return remote row counts.

    Idempotent: files overwrite, tables are CREATE OR REPLACE via mode('overwrite').
    """
    _upload_landing()
    spark = _spark()
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")

    counts = {}
    for name in SOURCES:
        df = spark.read.json(f"{VOLUME_PATH}/{name}.ndjson")
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            f"{CATALOG}.{SCHEMA}.{name}"
        )
        counts[name] = spark.table(f"{CATALOG}.{SCHEMA}.{name}").count()
    return counts


def main() -> None:
    print(f"[bronze->delta] landing {len(SOURCES)} sources into {CATALOG}.{SCHEMA} ...")
    remote = land_bronze()
    report = parity_report(local_counts(), remote)
    print(f"\n  {'source':<28} {'local':>8} {'remote':>8}  match")
    for name, r in report.items():
        print(f"  {name:<28} {str(r['local']):>8} {str(r['remote']):>8}  {'OK' if r['match'] else 'MISMATCH'}")
    ok = all_match(report)
    print(f"\n{'✅ bronze parity: all sources match' if ok else '❌ bronze parity FAILED'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
