"""Production orchestration for the Vitals lakehouse (Airflow).

Mirrors `python -m vitals.run` as a DAG: generate -> silver -> dbt gold -> serve.
In production these tasks run PySpark on Databricks against Delta; here they call the
same Python entrypoints so the DAG is the single source of truth for the dependency graph.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["vitals", "health", "medallion"],
)
def vitals_pipeline():
    @task
    def generate_bronze():
        from vitals import generate
        return generate.generate()

    @task
    def bronze_to_silver():
        from vitals import lakehouse
        return lakehouse.build()

    @task.bash
    def dbt_gold() -> str:
        return "cd ${AIRFLOW_HOME}/../ && cd dbt && DBT_PROFILES_DIR=. dbt build"

    @task
    def serve_ai_ready():
        from vitals import serve
        return serve.run()

    bronze = generate_bronze()
    silver = bronze_to_silver()
    gold = dbt_gold()
    served = serve_ai_ready()

    bronze >> silver >> gold >> served


vitals_pipeline()
