"""Jobs for elt_pipelines.

- `dbt_seed_job`: materializes the two seed assets (seeds/CUST_AZ12, seeds/PX_CAT_G1V2).
  This runs dbt via the existing @dbt_assets decorator so materialization
  events DO show up in the UI. Trigger manually from the UI to bootstrap
  the static reference tables.
- `landing_daily_job`: targets the three daily-partitioned landing assets
  so users can backfill many days at once.
- `landing_monthly_job`: targets the monthly-partitioned product-master
  landing asset.
- `dbt_elt_job`: runs the entire elt dbt pipeline on demand. Used as the
  trigger target for the daily-monthly bridge sensor.
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from elt_pipelines.constants import DUCKDB_WRITER_TAGS

# Asset-based jobs that materialize the landing assets.
# Partition definitions must be uniform within a job, so we have TWO:
# one for the three daily upstreams, one for the monthly upstream.
# The landing_file_sensor's RunRequests pick the right one automatically
# by emitting an asset_selection containing exactly one asset key.
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

# Seed-only job: materializes ONLY the two dbt seed assets via the
# existing @dbt_assets decorator. Because it targets asset keys, Dagster
# emits proper materialization events and the seed cards turn green.
dbt_seed_job = define_asset_job(
    name="dbt_seed_job",
    selection=AssetSelection.keys(
        AssetKey(["seeds", "CUST_AZ12"]),
        AssetKey(["seeds", "PX_CAT_G1V2"]),
    ),
    tags=DUCKDB_WRITER_TAGS,
    description="Materializes the static seed assets (CUST_AZ12, PX_CAT_G1V2).",
)

# Full ELT-transformation job: the UNPARTITIONED dbt pipeline
# (landing dbt models + staging + mart + reporting). EXCLUDES:
#   - the partitioned Dagster-owned landing assets (raw/raw_*) — those
#     are materialized separately by landing_{daily,monthly}_job
#   - the seed assets (handled by dbt_seed_job)
# Excluding them keeps the job single-partition-definition-compatible
# (all remaining assets are unpartitioned), which Dagster requires for
# an asset job that spans multiple layers.
dbt_elt_job = define_asset_job(
    name="dbt_elt_job",
    selection=(
        AssetSelection.all()
        - AssetSelection.keys(
            AssetKey(["seeds", "CUST_AZ12"]),
            AssetKey(["seeds", "PX_CAT_G1V2"]),
            AssetKey(["raw", "raw_sales_details"]),
            AssetKey(["raw", "raw_cust_info"]),
            AssetKey(["raw", "raw_loc_a101"]),
            AssetKey(["raw", "raw_prd_info_monthly"]),
        )
    ),
    tags=DUCKDB_WRITER_TAGS,
    description="Runs the unpartitioned dbt ELT pipeline (landing dbt models → staging → mart → reporting).",
)


all_jobs = [landing_daily_job, landing_monthly_job, dbt_elt_job, dbt_seed_job]
