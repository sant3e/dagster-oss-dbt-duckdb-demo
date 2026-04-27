"""Freshness checks for the elt_pipelines layer.

Every partitioned asset is monitored via `build_time_partition_freshness_checks`:
a check fails if the latest expected partition isn't materialized by
`deadline_cron`. Demo-friendly thresholds — in production you'd tie
these to real SLAs.

`default_automation_condition_sensor` evaluates freshness checks on its
30s tick, so turning that sensor on is enough to surface freshness
results in the UI.
"""

from dagster import (
    AssetKey,
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
