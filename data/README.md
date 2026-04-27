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

- Day-1 files for `2026-04-01`: `sales_details_2026_04_01.csv`,
  `cust_info_2026_04_01.csv`, `loc_a101_2026_04_01.csv`,
  `prd_info_2026_04.csv`.

## Generating more days

See `future_landing_data/README.md` for the Faker-based generator that
produces additional daily files to simulate the pipeline running
over time.

## Seeds (NOT placed here)

The two dbt seeds (`CUST_AZ12`, `PX_CAT_G1V2`) are git-tracked static
reference data, loaded via Step 1 of the demo (`Jobs → dbt_seed_job →
Materialize` in the UI). They do not go in this folder.
