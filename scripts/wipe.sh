#!/usr/bin/env bash
# Nuke local state so the next `make up` starts from zero.
# Does NOT remove the landing CSVs you dropped into data/landing/.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Removing DuckDB warehouse files..."
rm -f "$HERE/warehouse/"*.duckdb "$HERE/warehouse/"*.duckdb.wal "$HERE/warehouse/"*.duckdb.tmp
rm -rf "$HERE/warehouse/artifacts"

echo "Removing dbt target/ and dbt_packages/..."
rm -rf "$HERE/dbt_project/target" "$HERE/dbt_project/dbt_packages" "$HERE/dbt_project/logs"

echo "Removing Dagster SQLite storage + compute logs..."
rm -rf "$HERE/dagster_home/history" \
       "$HERE/dagster_home/storage" \
       "$HERE/dagster_home/schedules" \
       "$HERE/dagster_home/compute_logs" \
       "$HERE/dagster_home/logs"

echo "Done. Fresh slate."
