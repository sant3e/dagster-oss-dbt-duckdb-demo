"""Jobs for elt_pipelines.

- `dbt_seed_job`: materializes the two seed assets AND the two ERP staging
  models that depend on them. All non-partitioned (static reference data).
  Run this manually to bootstrap the warehouse before landing starts.

- `landing_daily_job`: targets the three daily-partitioned landing assets
  so users can backfill many days at once.

- `landing_monthly_job`: targets the monthly-partitioned product-master
  landing asset.

- `dbt_elt_job`: runs the daily-partitioned dbt ELT pipeline (landing →
  staging → mart → reporting) for a SINGLE partition_key. The
  daily_monthly_bridge_sensor emits one RunRequest per ready day, each
  with its partition_key; `duckdb_writer` tag concurrency serializes
  them.
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from elt_pipelines.constants import DUCKDB_WRITER_TAGS
from elt_pipelines.partitions import daily_partitions

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

# Seed-only job: seeds + their raw_erp_* landing wrappers + their stg_erp_*
# staging wrappers. All non-partitioned (static reference data).
dbt_seed_job = define_asset_job(
    name="dbt_seed_job",
    selection=AssetSelection.keys(
        AssetKey(["seeds", "CUST_AZ12"]),
        AssetKey(["seeds", "PX_CAT_G1V2"]),
        AssetKey(["landing", "raw_erp_CUST_AZ12"]),
        AssetKey(["landing", "raw_erp_PX_CAT_G1V2"]),
        AssetKey(["staging", "stg_erp_CUST_AZ12"]),
        AssetKey(["staging", "stg_erp_PX_CAT_G1V2"]),
    ),
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Materializes the static seed chain: seeds (CUST_AZ12, PX_CAT_G1V2) "
        "+ their raw_erp_* landing wrappers + their stg_erp_* staging wrappers. "
        "All unpartitioned."
    ),
)

# Daily-partitioned ELT job. Targets the whole partitioned dbt graph
# (landing dbt layer + staging + mart + reporting) and NOTHING from
# raw/* (those are partitioned Python landing assets with their own
# landing_{daily,monthly}_job) or from the seed chain (handled by
# dbt_seed_job).
dbt_elt_job = define_asset_job(
    name="dbt_elt_job",
    selection=(
        AssetSelection.all()
        - AssetSelection.keys(
            AssetKey(["seeds", "CUST_AZ12"]),
            AssetKey(["seeds", "PX_CAT_G1V2"]),
            AssetKey(["landing", "raw_erp_CUST_AZ12"]),
            AssetKey(["landing", "raw_erp_PX_CAT_G1V2"]),
            AssetKey(["staging", "stg_erp_CUST_AZ12"]),
            AssetKey(["staging", "stg_erp_PX_CAT_G1V2"]),
            AssetKey(["raw", "raw_sales_details"]),
            AssetKey(["raw", "raw_cust_info"]),
            AssetKey(["raw", "raw_loc_a101"]),
            AssetKey(["raw", "raw_prd_info_monthly"]),
        )
    ),
    partitions_def=daily_partitions,
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Runs the daily-partitioned dbt ELT pipeline for a single "
        "partition_key (landing dbt layer → staging → mart → reporting)."
    ),
)


all_jobs = [landing_daily_job, landing_monthly_job, dbt_elt_job, dbt_seed_job]
