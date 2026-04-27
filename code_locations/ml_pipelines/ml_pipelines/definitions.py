"""Top-level Definitions for the ml_pipelines code location."""

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
)

from ml_pipelines.assets import churn, dbt as dbt_assets_module, segmentation
from ml_pipelines.jobs import all_jobs
from ml_pipelines.resources import build_resources
from ml_pipelines.sensors import elt_to_ml_bridge_sensor

# Custom automation-condition sensor (replaces the built-in one) so the
# implicit __ASSET_JOB runs inherit the `duckdb_writer` concurrency key
# and serialize correctly against elt_pipelines runs.
ml_automation_condition_sensor = AutomationConditionSensorDefinition(
    name="ml_automation_condition_sensor",
    target=AssetSelection.all(),
    run_tags={
        "dagster/concurrency_key": "duckdb_writer",
        "triggered_by": "ml_automation_condition_sensor",
    },
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
)

defs = Definitions(
    assets=[
        dbt_assets_module.ml_features_dbt_assets,
        segmentation.customer_segments,
        churn.churn_predictions,
    ],
    jobs=all_jobs,
    sensors=[elt_to_ml_bridge_sensor, ml_automation_condition_sensor],
    resources=build_resources(),
)
