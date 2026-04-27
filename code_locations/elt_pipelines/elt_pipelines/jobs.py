"""Jobs for elt_pipelines.

- `dbt_seed_job`: materializes ONLY the two dbt seeds (CUST_AZ12,
  PX_CAT_G1V2). Unpartitioned static reference data. Run this manually
  as Step 1 of the demo to load the seed tables into DuckDB. The
  downstream staging models (`stg_erp_CUST_AZ12`, `stg_erp_PX_CAT_G1V2`)
  read the seed directly and stamp `snapshot_date` onto every row per
  partition via AutomationCondition.

- `landing_daily_job`: the three daily-partitioned Dagster-owned
  landing assets (raw/raw_sales_details + raw_cust_info + raw_loc_a101).
  Fired by landing_file_sensor when daily CSVs arrive; also usable
  manually for backfills.

- `landing_monthly_job`: the monthly-partitioned product-master
  landing asset (raw/raw_prd_info_monthly). Fired by
  landing_file_sensor when the monthly CSV arrives.
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from elt_pipelines.constants import DUCKDB_WRITER_TAGS

landing_daily_job = define_asset_job(
    name="landing_daily_job",
    selection=AssetSelection.keys(
        ["raw", "raw_sales_details"],
        ["raw", "raw_cust_info"],
        ["raw", "raw_loc_a101"],
    ),
    tags=DUCKDB_WRITER_TAGS,
    description="Runs the three daily-partitioned landing assets. Useful for backfills.",
)

landing_monthly_job = define_asset_job(
    name="landing_monthly_job",
    selection=AssetSelection.keys(
        ["raw", "raw_prd_info_monthly"],
    ),
    tags=DUCKDB_WRITER_TAGS,
    description="Runs the monthly-partitioned product-master landing asset.",
)

# Seeds-only.
dbt_seed_job = define_asset_job(
    name="dbt_seed_job",
    selection=AssetSelection.keys(
        AssetKey(["seeds", "CUST_AZ12"]),
        AssetKey(["seeds", "PX_CAT_G1V2"]),
    ),
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Materializes the two dbt seeds (CUST_AZ12, PX_CAT_G1V2). "
        "Unpartitioned reference data. Run once as Step 1 of the demo "
        "before firing daily landings. Downstream stg_erp_* staging "
        "models read the seeds directly and stamp snapshot_date per partition."
    ),
)


all_jobs = [landing_daily_job, landing_monthly_job, dbt_seed_job]
