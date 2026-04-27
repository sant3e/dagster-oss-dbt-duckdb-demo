# Dagster OSS Template — Makefile
# All-local, Docker-based demo. `make up` opens the UI at http://localhost:3000.

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose

.PHONY: help build up down restart logs ps seed clean wipe reset-demo manifest shell-elt shell-ml

help:  ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build:  ## Build all docker images (webserver/daemon + both code locations).
	$(COMPOSE) build

up:  ## Start the stack (webserver on http://localhost:3000).
	$(COMPOSE) up -d
	@echo ""
	@echo "Dagster UI:   http://localhost:3000"
	@echo "Tail logs:    make logs"

down:  ## Stop the stack (keeps the DuckDB file + SQLite metadata intact).
	$(COMPOSE) down

restart:  ## Restart all services.
	$(COMPOSE) restart

logs:  ## Tail logs from all services.
	$(COMPOSE) logs -f --tail=100

ps:  ## Show service status.
	$(COMPOSE) ps

seed:  ## Run the Dagster `dbt_seed_job` (loads static reference tables).
	@echo "Launch the dbt_seed_job from the UI (Jobs → dbt_seed_job → Materialize),"
	@echo "or use the CLI inside the elt container:"
	@echo "  make shell-elt"
	@echo "  dagster job execute -f /opt/code_location/elt_pipelines/definitions.py -j dbt_seed_job"

manifest:  ## Regenerate dbt manifest locally (for IDE / dev loop).
	cd dbt_project && \
	  DUCKDB_PATH=$(PWD)/warehouse/oss_template.duckdb \
	  dbt deps --profiles-dir . && \
	  dbt parse --profiles-dir .

shell-elt:  ## Drop into the elt_pipelines container.
	$(COMPOSE) exec elt_pipelines /bin/bash

shell-ml:  ## Drop into the ml_pipelines container.
	$(COMPOSE) exec ml_pipelines /bin/bash

clean:  ## Remove docker images built by this project (keeps data volumes).
	-$(COMPOSE) down --rmi local

wipe:  ## DANGER: remove DuckDB file, dbt target/, Dagster SQLite storage. Keeps landing files.
	@echo "Wiping warehouse, dagster_home storage, and dbt target..."
	bash scripts/wipe.sh

reset-demo:  ## Full reset: stop stack + wipe state + drop non-day-1 landing + clear future_landing_data/. Safe to run before handing the repo to someone else.
	bash scripts/reset_demo.sh
