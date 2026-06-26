.PHONY: setup run dbt clean test dbcxn-setup bronze-databricks silver-databricks

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

test:           ## run the Python unit test suite
	uv run --extra dev pytest tests/ -q

dbcxn-setup:    ## create the databricks-connect venv (separate — conflicts with pyspark)
	uv venv --python 3.12 .venv-dbcxn
	uv pip install --python .venv-dbcxn/bin/python "databricks-connect==17.3.*"

bronze-databricks:  ## land bronze -> Delta on Unity Catalog (run `source infra/terraform/.env` first)
	PYTHONPATH=src ./.venv-dbcxn/bin/python -m vitals.backends.databricks_delta bronze

silver-databricks:  ## bronze Delta -> de-identified silver Delta on UC (run `source infra/terraform/.env` first)
	PYTHONPATH=src ./.venv-dbcxn/bin/python -m vitals.backends.databricks_delta silver

clean:          ## remove generated data + build artifacts
	rm -rf data/bronze data/gold data/vitals.duckdb data/*.json dbt/target mlflow.db mlruns
