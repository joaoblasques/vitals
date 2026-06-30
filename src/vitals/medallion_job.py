"""On-cluster full-medallion entry point — the bundle's python_wheel_task (`vitals-medallion`).
Generate synthetic data, land bronze Delta, build silver Delta, enforce the PHI + non-empty gates.
Runs ONLY on Databricks serverless: sets VITALS_SPARK_MODE=ambient so the shared backend grabs the
ambient session; the writable bronze dir arrives as the wheel parameter argv[0]. See docs/adr/0005."""
from __future__ import annotations

import os
import sys


def _assert_nonempty(bronze: dict[str, int], silver: dict[str, int]) -> None:
    empties = [k for d in (bronze, silver) for k, v in d.items() if v <= 0]
    if empties:
        raise AssertionError(f"empty tables after ingest: {sorted(empties)}")


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        os.environ["VITALS_BRONZE_DIR"] = argv[0]
    os.environ["VITALS_SPARK_MODE"] = "ambient"

    from vitals import generate
    from vitals.backends import databricks_delta as dx

    print(f"[medallion] generate -> {os.environ.get('VITALS_BRONZE_DIR')}")
    generate.generate()
    bronze = dx.land_bronze()                       # upload to volume + write Delta
    silver = dx.build_silver()                      # bronze Delta -> de-identified silver Delta
    dx.assert_no_phi(dx.silver_patient_columns())   # PHI boundary — hard gate
    _assert_nonempty(bronze, silver)                # non-empty — hard gate
    print(f"✅ medallion ingest complete: bronze={sum(bronze.values())} silver={sum(silver.values())}")


if __name__ == "__main__":
    main()
