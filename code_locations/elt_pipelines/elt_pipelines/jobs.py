"""Jobs for elt_pipelines.

- `dbt_seed_job`: materializes ONLY the two dbt seeds (CUST_AZ12,
  PX_CAT_G1V2). Unpartitioned static reference data. Run this manually
  as Step 1 of the demo to load the seed tables into DuckDB.
  After seeds materialize, AutomationCondition.eager() on their
  downstream `raw_erp_*` landing wrappers auto-fires them for the
  relevant daily partition — so running daily landings
  (landing_daily_job) will also pull the latest seed content into
  that day's partition.

- `landing_daily_job`: the three daily-partitioned Dagster-owned
  landing assets (raw/raw_sales_details + raw_cust_info + raw_loc_a101).
  Fired by landing_file_sensor when daily CSVs arrive; also usable
  manually for backfills.

- `landing_monthly_job`: the monthly-partitioned product-master
  landing asset (raw/raw_prd_info_monthly). Fired by
  landing_file_sensor when the monthly CSV arrives.

- `dbt_elt_landing_job`: kept for manual backfills of the dbt landing
  layer (landing/raw_crm_*) for a specific partition_key.
  AutomationCondition.eager() on staging / mart / reporting
  auto-cascades from there, so the common flow does not invoke this
  job directly — it's handy only for targeted reruns.

Note on freshness-check exclusion:
    Every materialization job below subtracts
    `AssetSelection.checks(*all_freshness_checks)` from its asset
    selection. Without this, Dagster 1.12 auto-includes every
    AssetChecksDefinition attached to a selected asset in the job's
    execution plan — including the freshness checks registered via
    `build_time_partition_freshness_checks`. `.without_checks()` on an
    AssetSelection does NOT strip externally-registered
    AssetChecksDefinition objects, so we use explicit set subtraction
    instead. Freshness is evaluated independently by the dedicated
    `freshness_checks_sensor` in freshness_checks.py.
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from elt_pipelines.constants import DUCKDB_WRITER_TAGS
from elt_pipelines.freshness_checks import all_freshness_checks
from elt_pipelines.partitions import daily_partitions

# AssetSelection pointing at every externally-registered freshness
# check, for set-subtraction below.
_FRESHNESS_CHECKS_SELECTION = AssetSelection.checks(*all_freshness_checks)

landing_daily_job = define_asset_job(
    name="landing_daily_job",
    selection=(
        AssetSelection.keys(
            ["raw", "raw_sales_details"],
            ["raw", "raw_cust_info"],
            ["raw", "raw_loc_a101"],
        )
        - _FRESHNESS_CHECKS_SELECTION
    ),
    tags=DUCKDB_WRITER_TAGS,
    description="Runs the three daily-partitioned landing assets. Useful for backfills.",
)

landing_monthly_job = define_asset_job(
    name="landing_monthly_job",
    selection=(
        AssetSelection.keys(["raw", "raw_prd_info_monthly"])
        - _FRESHNESS_CHECKS_SELECTION
    ),
    tags=DUCKDB_WRITER_TAGS,
    description="Runs the monthly-partitioned product-master landing asset.",
)

# Seeds-only.
dbt_seed_job = define_asset_job(
    name="dbt_seed_job",
    selection=(
        AssetSelection.keys(
            AssetKey(["seeds", "CUST_AZ12"]),
            AssetKey(["seeds", "PX_CAT_G1V2"]),
        )
        - _FRESHNESS_CHECKS_SELECTION
    ),
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Materializes the two dbt seeds (CUST_AZ12, PX_CAT_G1V2). "
        "Unpartitioned reference data. Run once as Step 1 of the demo "
        "before firing daily landings."
    ),
)

# Daily-partitioned landing job (all 6 dbt landing models).
dbt_elt_landing_job = define_asset_job(
    name="dbt_elt_landing_job",
    selection=(
        AssetSelection.keys(
            AssetKey(["landing", "raw_crm_cust_info"]),
            AssetKey(["landing", "raw_crm_prd_info"]),
            AssetKey(["landing", "raw_crm_sales_details"]),
            AssetKey(["landing", "raw_erp_LOC_A101"]),
            AssetKey(["landing", "raw_erp_CUST_AZ12"]),
            AssetKey(["landing", "raw_erp_PX_CAT_G1V2"]),
        )
        - _FRESHNESS_CHECKS_SELECTION
    ),
    partitions_def=daily_partitions,
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Daily-partitioned dbt landing models (all 6: raw_crm_* + raw_erp_*). "
        "AutomationCondition on staging/mart/reporting auto-cascades "
        "from here per-partition."
    ),
)


all_jobs = [landing_daily_job, landing_monthly_job, dbt_elt_landing_job, dbt_seed_job]
