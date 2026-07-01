.PHONY: setup run build dbt clean test dbcxn-setup bronze-databricks silver-databricks gold-baseline gold-databricks drift-databricks bundle-deploy bundle-run rag-up rag-down rag-load rag-query feast-demo metrics-validate metrics-list metrics-query

setup:          ## create venv + install the runnable MVP stack
	uv venv --python 3.12
	uv pip install duckdb dbt-duckdb pandas pyarrow numpy scikit-learn mlflow

run:            ## run the full MVP slice end-to-end
	PYTHONPATH=src ./.venv/bin/python -m vitals.run

build:          ## hermetic data gate: generate -> silver (PHI boundary) -> dbt gold + 26 DQ tests; no ML serve
	PYTHONPATH=src ./.venv/bin/python -m vitals.run --no-serve

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
	uv run --extra dev --extra local pytest tests/ -q

dbcxn-setup:    ## create the databricks-connect venv (separate — conflicts with pyspark)
	uv venv --python 3.12 .venv-dbcxn
	uv pip install --python .venv-dbcxn/bin/python "databricks-connect==17.3.*"

bronze-databricks:  ## land bronze -> Delta on Unity Catalog (run `source infra/terraform/.env` first)
	PYTHONPATH=src ./.venv-dbcxn/bin/python -m vitals.backends.databricks_delta bronze

silver-databricks:  ## bronze Delta -> de-identified silver Delta on UC (run `source infra/terraform/.env` first)
	PYTHONPATH=src ./.venv-dbcxn/bin/python -m vitals.backends.databricks_delta silver

gold-baseline:  ## refresh local gold + write the parity baseline (main venv, has duckdb)
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt build --target dev
	PYTHONPATH=src ./.venv/bin/python -m vitals.backends.databricks_delta gold-baseline

gold-databricks:  ## build gold on UC via dbt-databricks + verify parity (run `source infra/terraform/.env` first)
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt build --target databricks
	PYTHONPATH=src ./.venv-dbcxn/bin/python -m vitals.backends.databricks_delta gold

drift-databricks:  ## PSI drift on the gold marts -> vitals_gold.monitoring + verify parity (run `source infra/terraform/.env` first)
	PYTHONPATH=src ./.venv-dbcxn/bin/python -m vitals.backends.databricks_delta drift

bundle-deploy:  ## deploy the Asset Bundle job to Databricks (run `source infra/terraform/.env` first)
	databricks bundle validate
	databricks bundle deploy

bundle-run:     ## trigger the deployed gold-refresh job and wait (run `source infra/terraform/.env` first)
	databricks bundle run vitals_medallion

rag-up:         ## start the local pgvector serving store (Docker) + wait until healthy
	docker compose up -d pgvector
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$(docker compose ps -q pgvector))" = "healthy" ]; do sleep 1; done
	@echo "pgvector healthy on localhost:5432"

rag-down:       ## stop + remove the pgvector store
	docker compose down

rag-load:       ## embed silver.note -> pgvector (needs `uv sync --extra vector` + rag-up)
	PYTHONPATH=src ./.venv/bin/python -m vitals.vector_index load

rag-query:      ## ANN query the store: make rag-query Q="low back pain"
	PYTHONPATH=src ./.venv/bin/python -m vitals.vector_index query "$(Q)"

feast-demo:     ## apply + materialize Feast (offline parquet -> sqlite online) + online/historical retrieval
	PYTHONPATH=src ./.venv/bin/python -m vitals.feature_store demo

metrics-validate:  ## validate the dbt Semantic Layer configs (needs `uv sync --extra metrics`)
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf validate-configs

metrics-list:      ## list the defined metrics
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf list metrics

metrics-query:     ## example: surgery rate + pain by condition (needs `make build` first)
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf query --metrics surgery_rate,avg_pain --group-by patient__primary_condition

clean:          ## remove generated data + build artifacts
	rm -rf data/bronze data/gold data/vitals.duckdb data/*.json dbt/target mlflow.db mlruns
