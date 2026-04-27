"""Freshness checks for the ml_pipelines layer — partition-based.

Every ml asset is daily-partitioned now. The check fails if the latest
expected partition isn't materialized by the deadline. 11am daily leaves
room for elt to finish (its deadline is 10am).
"""

from dagster import AssetKey, build_time_partition_freshness_checks

ml_freshness_checks = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["ml_features", "customer_rfm"]),
        AssetKey(["ml_features", "customer_segments"]),
        AssetKey(["ml_features", "churn_predictions"]),
    ],
    deadline_cron="0 11 * * *",
)

all_freshness_checks = list(ml_freshness_checks)
