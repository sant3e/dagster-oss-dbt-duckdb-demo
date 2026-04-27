"""Jobs for elt_pipelines.

- `dbt_seed_job`: materializes the two seed assets AND the two ERP staging
  models that depend on them. All non-partitioned (static reference data).
  Run this manually to bootstrap the warehouse before landing starts.

- `landing_daily_job`: targets the three daily-partitioned landing assets
  so users can backfill many days at once.

- `landing_monthly_job`: targets the monthly-partitioned product-master
  landing asset.

- `dbt_elt_landing_job`: runs the daily-partitioned dbt LANDING models
  (landing/raw_crm_*) for a SINGLE partition_key. Fired by
  daily_monthly_bridge_sensor once the cadence gate opens. After this
  runs, AutomationCondition.eager() on staging/mart/reporting
  auto-cascades the rest of the partition through the graph.
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

# Daily-partitioned LANDING-only job — fired by the cadence bridge sensor.
# Targets ONLY the dbt landing models (landing/raw_crm_*). Once these
# materialize for a partition, AutomationCondition.eager() on staging /
# mart / reporting takes over and auto-cascades the rest of the partition
# through the graph.
dbt_elt_landing_job = define_asset_job(
    name="dbt_elt_landing_job",
    selection=AssetSelection.keys(
        AssetKey(["landing", "raw_crm_cust_info"]),
        AssetKey(["landing", "raw_crm_prd_info"]),
        AssetKey(["landing", "raw_crm_sales_details"]),
        AssetKey(["landing", "raw_erp_LOC_A101"]),
    ),
    partitions_def=daily_partitions,
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Daily-partitioned dbt landing models. Fired by "
        "daily_monthly_bridge_sensor once both the daily file landings "
        "AND the monthly product landing are ready for a given day. "
        "AutomationCondition on staging/mart/reporting auto-cascades "
        "from here per-partition."
    ),
)


all_jobs = [landing_daily_job, landing_monthly_job, dbt_elt_landing_job, dbt_seed_job]
