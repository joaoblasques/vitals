"""Run the full Vitals MVP slice locally: generate -> bronze/silver -> dbt gold -> serve.

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


def main() -> None:
    from vitals import generate, lakehouse, serve

    print("\n[1/4] generate bronze ...");  generate.generate()
    print("\n[2/4] bronze -> silver ...");  lakehouse.build()
    print("\n[3/4] dbt: silver -> gold ..."); _dbt_build()
    print("\n[4/4] serve: features + vectors + model ..."); serve.run()
    print("\n✅ Vitals MVP slice complete. See data/results.json.")


if __name__ == "__main__":
    sys.exit(main())
