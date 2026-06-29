"""Run the Vitals MVP slice locally: generate -> bronze/silver -> dbt gold -> serve.

Use `--no-serve` to run the data pipeline only (generate -> silver -> dbt gold + DQ tests),
skipping the ML serve step — that is the hermetic gate `make build` / CI runs.

This is the single entrypoint (`python -m vitals.run`). In production the same DAG is
orchestrated by Airflow (see airflow/dags/vitals_dag.py).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _dbt_build() -> None:
    dbt = ROOT / ".venv" / "bin" / "dbt"
    env = {**os.environ, "DBT_PROFILES_DIR": "."}
    subprocess.run([str(dbt), "build"], cwd=ROOT / "dbt", env=env, check=True)


def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    run_serve = "--no-serve" not in argv

    from vitals import generate, lakehouse, serve

    print("\n[1/4] generate bronze ...")
    generate.generate()
    print("\n[2/4] bronze -> silver ...")
    lakehouse.build()
    print("\n[3/4] dbt: silver -> gold ...")
    _dbt_build()
    if run_serve:
        print("\n[4/4] serve: features + vectors + model ...")
        serve.run()
    print("\n✅ Vitals MVP slice complete. See data/results.json.")


if __name__ == "__main__":
    sys.exit(main())
