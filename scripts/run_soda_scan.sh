#!/usr/bin/env bash
set -euo pipefail
soda scan -d duckdb -c soda/configuration.yml soda/checks/silver_breweries.yml
