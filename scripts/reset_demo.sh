#!/usr/bin/env bash
# Reset the demo to a known-clean state.
#
# What this does:
#   1. Stops and removes the Dagster docker stack (`docker compose down`).
#   2. Wipes runtime state: DuckDB file, Dagster SQLite + history + schedules
#      + compute logs, dbt target/ and dbt_packages/, ML artifacts.
#   3. Restores the day-1 template CSVs in data/landing/ by removing any
#      untracked CSVs (Faker output, rebase-renamed copies) and `git checkout`-ing
#      the tracked ones back to their committed contents.
#   4. Removes any files in future_landing_data/ EXCEPT README.md.
#
# What it does NOT touch:
#   - The 4 day-1 template CSVs that ship in git (restored, not deleted).
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

echo "Step 3/4  Restoring day-1 template CSVs in data/landing/..."
# The template files are tracked in git. After each demo run the landing
# folder may contain renamed template files (rebase_day1_csvs.sh moves them
# to today-3) plus any Faker-generated CSVs the user copied in. We wipe
# every CSV that isn't tracked in git, then restore the tracked ones to
# exactly what the repo ships.
if [[ -d data/landing ]]; then
  # Remove every .csv in data/landing/ that git doesn't track. `git
  # ls-files` lists only tracked files, so anything missing from it is
  # either a rebased copy or a Faker output — both safe to delete.
  tracked_csvs="$(git ls-files data/landing/ | grep '\.csv$' || true)"
  for entry in data/landing/*.csv; do
    [[ -e "$entry" ]] || continue
    if ! printf '%s\n' "$tracked_csvs" | grep -qx "$entry"; then
      echo "  removing $entry"
      rm -f "$entry"
    fi
  done
  # Restore the tracked template files to their committed contents (undoes
  # any snapshot_date column rewrites left by the rebase script).
  git checkout HEAD -- data/landing/ 2>/dev/null || true
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
