"""Scheduled job entrypoint — run the PSI drift monitor on-cluster and append to
vitals_gold.monitoring.drift_report.

Wired as the `drift_monitor` task in databricks.yml, downstream of the gold dbt build, so drift is
scored on the SAME schedule the data moves — every refresh, not in a side process that rots. Runs on
the ambient serverless SparkSession (no databricks-connect): the bundle syncs the repo, so we add the
sibling `src/` to the path and call the SAME `build_drift` the dev/parity path uses (no duplicated
PSI math). See ADR 0005.
"""
from __future__ import annotations

import os
import sys

# The bundle syncs the repo to ${workspace.file_path}, passed as argv[1] (the task `parameters`).
# We can't use __file__ — serverless runs this via exec(compile(...)), which leaves __file__ unbound;
# argv[1] is the robust, bundle-native way to locate the synced src/. Fall back to CWD for local runs.
_files_root = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
sys.path.insert(0, os.path.join(_files_root, "src"))

from pyspark.sql import SparkSession  # noqa: E402

from vitals.backends.databricks_delta import build_drift  # noqa: E402


def main() -> None:
    spark = SparkSession.builder.getOrCreate()  # ambient serverless session on the job
    report = build_drift(spark)
    print(f"drift: wrote {report['_rows_written']} rows; alerts: {report['alerts']}")


if __name__ == "__main__":
    main()
