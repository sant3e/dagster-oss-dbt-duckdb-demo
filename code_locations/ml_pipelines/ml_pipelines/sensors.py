"""Cross-code-location asset sensor.

Fires the ml training job whenever `customer_rfm` (a dbt model in ml_features,
which itself depends on elt_pipelines' reporting layer) materializes. This
demonstrates how the Dagster UI stitches the asset graph across code
locations — the sensor in ml_pipelines can subscribe to materialization
events from assets owned by elt_pipelines or by this location's dbt layer.
"""

from dagster import (
    AssetKey,
    EventLogEntry,
    RunRequest,
    SensorEvaluationContext,
    asset_sensor,
)

from ml_pipelines.jobs import ml_training_job


@asset_sensor(
    asset_key=AssetKey(["ml_features", "customer_rfm"]),
    job=ml_training_job,
    name="customer_rfm_updated_sensor",
    minimum_interval_seconds=30,
    description=(
        "Runs the ml_training_job (segmentation + churn) whenever the "
        "customer_rfm dbt model materializes. This is the cross-location "
        "trigger mentioned in the README."
    ),
)
def customer_rfm_updated_sensor(
    context: SensorEvaluationContext,
    asset_event: EventLogEntry,
):
    return RunRequest(
        run_key=context.cursor,
        tags={
            "trigger/source": "customer_rfm_updated_sensor",
            "trigger/run_id": str(asset_event.run_id or ""),
        },
    )
