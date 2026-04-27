"""Top-level Definitions for the ml_pipelines code location."""

from dagster import Definitions

from ml_pipelines.assets import churn, dbt as dbt_assets_module, segmentation
from ml_pipelines.freshness_checks import all_freshness_checks
from ml_pipelines.jobs import all_jobs
from ml_pipelines.resources import build_resources
from ml_pipelines.sensors import elt_to_ml_bridge_sensor

defs = Definitions(
    assets=[
        dbt_assets_module.ml_features_dbt_assets,
        segmentation.customer_segments,
        churn.churn_predictions,
    ],
    asset_checks=all_freshness_checks,
    jobs=all_jobs,
    sensors=[elt_to_ml_bridge_sensor],
    resources=build_resources(),
)
