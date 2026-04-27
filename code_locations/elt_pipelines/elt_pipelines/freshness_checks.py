"""Freshness checks for the elt_pipelines layer.

Every partitioned asset is monitored via `build_time_partition_freshness_checks`:
a check fails if the latest expected partition isn't materialized by
`deadline_cron`. Demo-friendly thresholds — in production you'd tie
these to real SLAs.

Evaluated by a dedicated `freshness_checks_sensor` built via
`build_sensor_for_freshness_checks`. That sensor runs the checks
OUT-OF-BAND from asset materialization jobs — so a user-triggered
materialization or backfill never includes a freshness-check step
inline and can never show red for freshness. Per Dagster 1.12 docs
(data-freshness-testing.md): "It is critical to pair these checks
with a schedule or sensor using build_sensor_for_freshness_checks to
ensure they execute independently of asset materialization."

Note: `.without_checks()` on an AssetSelection does NOT reliably strip
these `AssetChecksDefinition` objects when they're attached via
`Definitions(asset_checks=...)` — that kwarg only affects checks
declared inline on @asset(check_specs=...). The dedicated sensor is
the canonical pattern.

The freshness_checks_sensor ships with default_status=STOPPED — toggle
it on in the Automation tab to start evaluating freshness.
"""

from dagster import (
    AssetKey,
    build_sensor_for_freshness_checks,
    build_time_partition_freshness_checks,
)

# Daily-partitioned landing assets — deadline 9am every day.
landing_daily_freshness = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["raw", "raw_sales_details"]),
        AssetKey(["raw", "raw_cust_info"]),
        AssetKey(["raw", "raw_loc_a101"]),
    ],
    deadline_cron="0 9 * * *",
)

# Monthly-partitioned product master — deadline 9am on the 2nd of each month.
landing_monthly_freshness = build_time_partition_freshness_checks(
    assets=[AssetKey(["raw", "raw_prd_info_monthly"])],
    deadline_cron="0 9 2 * *",
)

# Daily-partitioned dbt layers (landing → staging → mart → reporting).
# Deadline 10am daily — an hour after landing's deadline, which gives
# the pipeline time to run.
landing_dbt_freshness = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["landing", "raw_crm_cust_info"]),
        AssetKey(["landing", "raw_crm_prd_info"]),
        AssetKey(["landing", "raw_crm_sales_details"]),
        AssetKey(["landing", "raw_erp_LOC_A101"]),
    ],
    deadline_cron="0 10 * * *",
)

staging_dbt_freshness = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["staging", "stg_crm_cust_info"]),
        AssetKey(["staging", "stg_crm_prd_info"]),
        AssetKey(["staging", "stg_crm_sales_details"]),
        AssetKey(["staging", "stg_erp_LOC_A101"]),
    ],
    deadline_cron="0 10 * * *",
)

mart_freshness = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["mart", "dim_customers"]),
        AssetKey(["mart", "dim_products"]),
        AssetKey(["mart", "dim_products_history"]),
        AssetKey(["mart", "fct_sales"]),
    ],
    deadline_cron="0 10 * * *",
)

reporting_freshness = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["reporting", "rpt_sales_summary_by_customer"]),
        AssetKey(["reporting", "rpt_sales_performance_by_product"]),
    ],
    deadline_cron="0 10 * * *",
)

all_freshness_checks = [
    *landing_daily_freshness,
    *landing_monthly_freshness,
    *landing_dbt_freshness,
    *staging_dbt_freshness,
    *mart_freshness,
    *reporting_freshness,
]

# Dedicated sensor that evaluates the freshness checks on a schedule,
# independent of any materialization job. Ships stopped by default —
# user toggles it on in Automation → Sensors.
freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_freshness_checks,
    minimum_interval_seconds=3600,  # hourly
)
