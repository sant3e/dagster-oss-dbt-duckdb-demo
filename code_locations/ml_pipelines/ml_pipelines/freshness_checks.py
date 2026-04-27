"""Freshness checks for the ml_pipelines layer — partition-based.

Every ml asset is daily-partitioned. The check fails if the latest
expected partition isn't materialized by the deadline (11am daily,
leaving an hour after the elt pipeline's 10am deadline).

Evaluated by the dedicated `freshness_checks_sensor` built via
`build_sensor_for_freshness_checks` — runs OUT-OF-BAND from any
materialization job, so `ml_training_job` runs never include a
freshness-check step inline. Ships stopped; toggle on in Automation →
Sensors.
"""

from dagster import (
    AssetKey,
    build_sensor_for_freshness_checks,
    build_time_partition_freshness_checks,
)

ml_freshness_checks = build_time_partition_freshness_checks(
    assets=[
        AssetKey(["ml_features", "customer_rfm"]),
        AssetKey(["ml_features", "customer_segments"]),
        AssetKey(["ml_features", "churn_predictions"]),
    ],
    deadline_cron="0 11 * * *",
)

all_freshness_checks = list(ml_freshness_checks)

freshness_checks_sensor = build_sensor_for_freshness_checks(
    freshness_checks=all_freshness_checks,
    minimum_interval_seconds=3600,
)
