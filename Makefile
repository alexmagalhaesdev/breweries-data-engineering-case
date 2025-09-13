SHELL := /bin/bash

.PHONY: build up down logs test scan fmt fmt-check

build:
	docker compose build

up:
	docker compose up

down:
	docker compose down

logs:
	docker compose logs -f app

test:
	docker compose run --rm app pytest -q

scan:
	docker compose run --rm app bash -lc "soda scan -d duckdb -c soda/configuration.yml soda/checks/silver_breweries.yml"

fmt:
	docker compose run --rm app bash -lc "python -m pip install -q ruff black && black app tests && ruff check --fix app tests"

fmt-check:
	docker compose run --rm app bash -lc "python -m pip install -q ruff black && black --check app tests && ruff check app tests"