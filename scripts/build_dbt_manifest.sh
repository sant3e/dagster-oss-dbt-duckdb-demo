#!/usr/bin/env bash
# Regenerates the dbt manifest used by @dbt_assets. Run it before the IDE
# loads or before rebuilding images locally.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE/dbt_project"

export DUCKDB_PATH="${DUCKDB_PATH:-$HERE/warehouse/oss_template.duckdb}"

dbt deps --profiles-dir .
dbt parse --profiles-dir .
echo "Manifest written to $HERE/dbt_project/target/manifest.json"
