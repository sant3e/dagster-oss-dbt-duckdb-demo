# Landing data

Drop daily and monthly snapshot CSVs into `./landing/`. The
`landing_file_sensor` in `elt_pipelines` polls this folder every 30
seconds and launches the matching partitioned landing asset whenever a
new file appears.

## File naming + expected columns

**Daily snapshots** — one file per day, filename carries the date:

```
sales_details_YYYY_MM_DD.csv
cust_info_YYYY_MM_DD.csv
loc_a101_YYYY_MM_DD.csv
```

Each row must include a `snapshot_date` column whose value equals the
date in the filename.

**Monthly snapshot** — one file per month, filename carries the month:

```
prd_info_YYYY_MM.csv
```

Each row must include a `snapshot_month` column whose value is the
first-of-month date (e.g. `2026-04-01`).

## What ships in the repo

Four **template** files. Their filenames and partition columns carry
whichever date was frozen at the last commit — that specific date does
not matter, it's just a stable baseline:

- `sales_details_<YYYY_MM_DD>.csv`
- `cust_info_<YYYY_MM_DD>.csv`
- `loc_a101_<YYYY_MM_DD>.csv`
- `prd_info_<YYYY_MM>.csv`

These are tracked in git so the repo always has a known-good baseline.
At the start of each demo session, Step 1 of the main README runs
`./scripts/rebase_day1_csvs.sh`, which:

1. Renames the four files to `<prefix>_<today-3>.csv` (monthly file uses
   `<prefix>_<year-month of today-3>.csv`).
2. Rewrites the `snapshot_date` / `snapshot_month` column inside each
   file so the partition value matches the new filename.

Result: **day-1 of the demo is always today − 3**, regardless of the
calendar date the demo is run on. The `make reset-demo` target restores
the template back to its committed state via `git checkout`, so the
next rebase starts from a clean slate.

## Generating more days

See `future_landing_data/README.md` for the Faker-based generator that
produces additional daily files to simulate the pipeline running
over time.

## Seeds (NOT placed here)

The two dbt seeds (`CUST_AZ12`, `PX_CAT_G1V2`) are git-tracked static
reference data, loaded via Step 1 of the demo (`Jobs → dbt_seed_job →
Materialize` in the UI). They do not go in this folder.
