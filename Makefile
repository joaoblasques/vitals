.PHONY: setup run dbt clean

setup:          ## create venv + install the runnable MVP stack
	uv venv --python 3.12
	uv pip install duckdb dbt-duckdb pandas pyarrow numpy scikit-learn mlflow

run:            ## run the full MVP slice end-to-end
	PYTHONPATH=src ./.venv/bin/python -m vitals.run

dbt:            ## run just the gold transformations + tests
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt build

spark-deps:     ## install the Spark/Phase-3 extra (PySpark)
	uv pip install pyspark

stream:         ## Phase 3: Spark Structured Streaming over the wearable feed (needs pyspark + JDK 17)
	PYTHONPATH=src ./.venv/bin/python -m vitals.streaming

spark:          ## Phase 3: PySpark-at-scale silver transform with a window function
	PYTHONPATH=src ./.venv/bin/python -m vitals.spark_silver

monitor:        ## Phase 4: feature-drift monitoring (PSI)
	PYTHONPATH=src ./.venv/bin/python -m vitals.monitoring

catalog:        ## Phase 4: regenerate the data dictionary + lineage from dbt
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt docs generate
	PYTHONPATH=src ./.venv/bin/python -m vitals.catalog

clean:          ## remove generated data + build artifacts
	rm -rf data/bronze data/gold data/vitals.duckdb data/*.json dbt/target mlflow.db mlruns
