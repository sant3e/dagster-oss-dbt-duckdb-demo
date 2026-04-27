#!/usr/bin/env bash
# Reset the demo to a known-clean state.
#
# What this does:
#   1. Stops and removes the Dagster docker stack (`docker compose down`).
#   2. Wipes runtime state: DuckDB file, Dagster SQLite + history + schedules
#      + compute logs, dbt target/ and dbt_packages/, ML artifacts.
#   3. Removes any landing files in data/landing/ that are NOT one of the
#      four day-1 files that ship with the repo.
#   4. Removes any files in future_landing_data/ EXCEPT README.md.
#
# What it does NOT touch:
#   - The four day-1 landing files (cust_info_2026_04_01.csv,
#     loc_a101_2026_04_01.csv, prd_info_2026_04.csv, sales_details_2026_04_01.csv).
#   - Any .py / .sql / config file in the repo.
#   - The dbt seed CSVs.
#   - Docker IMAGES (use `docker compose build` to recreate them; or
#     `docker system prune` if you really want those gone too).
#
# Run:  make reset-demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

echo "Step 1/4  Stopping stack..."
docker compose down --remove-orphans 2>/dev/null || true

echo "Step 2/4  Wiping runtime state..."
rm -f  warehouse/*.duckdb warehouse/*.duckdb.wal warehouse/*.duckdb.tmp
rm -rf warehouse/artifacts
rm -rf dagster_home/history \
       dagster_home/storage \
       dagster_home/schedules \
       dagster_home/compute_logs \
       dagster_home/logs
rm -rf dbt_project/target dbt_project/dbt_packages dbt_project/logs
rm -f  dbt_project/.user.yml

echo "Step 3/4  Pruning non-day-1 landing files..."
# Keep only the 4 day-1 files + the .gitkeep.
KEEP=(
  "cust_info_2026_04_01.csv"
  "loc_a101_2026_04_01.csv"
  "prd_info_2026_04.csv"
  "sales_details_2026_04_01.csv"
  ".gitkeep"
)
if [[ -d data/landing ]]; then
  for entry in data/landing/* data/landing/.[!.]*; do
    [[ -e "$entry" ]] || continue
    base="$(basename "$entry")"
    keep=0
    for k in "${KEEP[@]}"; do
      if [[ "$base" == "$k" ]]; then keep=1; break; fi
    done
    if [[ $keep -eq 0 ]]; then
      echo "  removing data/landing/$base"
      rm -f "$entry"
    fi
  done
fi

echo "Step 4/4  Cleaning future_landing_data/ (keeping README.md)..."
if [[ -d future_landing_data ]]; then
  find future_landing_data \
       -mindepth 1 \
       ! -name "README.md" \
       -exec rm -rf {} + 2>/dev/null || true
fi

echo ""
echo "Demo reset complete. To start from scratch:"
echo "  make build   # only if images don't exist yet"
echo "  make up"
