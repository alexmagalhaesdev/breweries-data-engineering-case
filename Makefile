SHELL := /bin/bash

.PHONY: build up down logs test scan fmt fmt-check duckcli

build:
	docker compose build

up:
	docker compose up

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose run --rm -e PYTHONPATH=/app app pytest -q

scan:
	docker compose run --rm -e DUCKDB_PATH="$(DUCKDB_PATH)" app bash -lc "\
		apt-get update >/dev/null && \
		apt-get install -y python3-setuptools python3-distutils || true && \
		python3 -m pip install --upgrade pip setuptools wheel >/dev/null && \
		soda scan -d duckdb -c soda/configuration.yml soda/checks/silver_breweries.yml\
	"

fmt:
	docker compose run --rm app bash -lc "python -m pip install -q ruff black && black app tests && ruff check --fix app tests"

fmt-check:
	docker compose run --rm app bash -lc "python -m pip install -q ruff black && black --check app tests && ruff check app tests"

duckcli:
	@command -v duckdb >/dev/null 2>&1 || { \
		echo "DuckDB CLI not found. Please install it following this documentation: https://motherduck.com/docs/getting-started/interfaces/connect-query-from-duckdb-cli/"; \
		exit 1; \
	}
	docker compose cp app:/data/warehouse.duckdb ./warehouse.duckdb
	duckdb ./warehouse.duckdb
