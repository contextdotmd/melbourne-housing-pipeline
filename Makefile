# Melbourne housing pipeline
#
# `make all` reproduces everything from the source CSV in well under a minute.
# Requires: uv (https://docs.astral.sh/uv/). Everything else installs itself.

DBT      := uv run dbt
DBT_ARGS := --project-dir dbt --profiles-dir dbt
DUCKDB   := data/warehouse.duckdb
SOURCE   := data/raw/MELBOURNE_HOUSE_PRICES_LESS.csv

.DEFAULT_GOAL := help
.PHONY: help setup setup-airflow ingest build test test-dag test-unit all docs ui sql airflow clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python dependencies and dbt packages
	uv sync --group dev
	$(DBT) deps $(DBT_ARGS)

setup-airflow: ## Install Airflow into its own environment (its pins conflict with dbt-core's)
	uv venv .venv-airflow --python 3.12
	uv pip install --python .venv-airflow "apache-airflow==3.3.1" pytest \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"

ingest: ## Load the CSV into the Parquet landing zone
	uv run python -m ingest.cli $(SOURCE) data

build: ## Run every model and every test
	$(DBT) deps $(DBT_ARGS)
	$(DBT) build $(DBT_ARGS)

test: ## Python tests, then the full dbt suite
	uv run --group dev pytest
	$(DBT) deps $(DBT_ARGS)
	$(DBT) build $(DBT_ARGS)

test-unit: ## dbt unit tests only (no warehouse data required)
	$(DBT) test $(DBT_ARGS) --select test_type:unit

test-dag: ## Airflow DAG structure tests (needs `make setup-airflow` first)
	AIRFLOW_HOME="$(PWD)/airflow" PYTHONPATH="$(PWD)/src" .venv-airflow/bin/pytest tests/test_dag.py -q

all: ingest build ## Ingest then build the warehouse — the whole pipeline

docs: ## Generate and serve the dbt documentation site
	$(DBT) docs generate $(DBT_ARGS)
	$(DBT) docs serve $(DBT_ARGS)

ui: ## Open the DuckDB browser UI (needs `brew install duckdb`)
	@echo "DuckDB is single-writer: stop any running build first."
	duckdb -ui $(DUCKDB)

sql: ## Open a DuckDB SQL prompt
	@echo "DuckDB is single-writer: stop any running build first."
	duckdb $(DUCKDB)

airflow: ## Run Airflow locally, then trigger melbourne_housing_pipeline
	AIRFLOW_HOME="$(PWD)/airflow" .venv-airflow/bin/airflow standalone

clean: ## Remove build outputs and derived data (keeps data/raw)
	rm -rf dbt/target dbt/dbt_packages dbt/logs
	rm -rf data/landing data/rejects data/ingest_receipt $(DUCKDB) $(DUCKDB).wal
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
