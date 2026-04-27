"""Jobs for elt_pipelines.

- `dbt_seed_job`: runs `dbt seed` to load static reference tables
  (CUST_AZ12, PX_CAT_G1V2). Trigger manually from the UI.
- `landing_daily_job`: targets the three daily-partitioned landing assets
  so users can backfill many days at once.
- `landing_monthly_job`: targets the monthly-partitioned product-master
  landing asset.
- `dbt_elt_job`: runs the entire elt dbt pipeline on demand. Used as the
  trigger target for the daily-monthly bridge sensor.
"""

from dagster import (
    AssetSelection,
    OpExecutionContext,
    define_asset_job,
    job,
    op,
)
from dagster_dbt import DbtCliResource

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

# Asset-based job for triggering the full ELT dbt pipeline (landing → reporting).
dbt_elt_job = define_asset_job(
    name="dbt_elt_job",
    selection=AssetSelection.assets(),  # all dbt_assets in this code location
    tags=DUCKDB_WRITER_TAGS,
    description="Runs every dbt model in the elt code location (landing→reporting).",
)


@op(required_resource_keys={"dbt"}, tags=DUCKDB_WRITER_TAGS)
def run_dbt_seed(context: OpExecutionContext) -> None:
    """Load static reference seeds (CUST_AZ12, PX_CAT_G1V2) into DuckDB."""
    dbt: DbtCliResource = context.resources.dbt
    invocation = dbt.cli(["seed"], context=context)
    for _ in invocation.stream():
        pass
    if not invocation.is_successful():
        raise Exception("dbt seed failed — check logs.")


@job(
    name="dbt_seed_job",
    tags=DUCKDB_WRITER_TAGS,
    description="Runs `dbt seed` to populate static reference tables.",
)
def dbt_seed_job() -> None:
    run_dbt_seed()


all_jobs = [landing_daily_job, landing_monthly_job, dbt_elt_job, dbt_seed_job]
