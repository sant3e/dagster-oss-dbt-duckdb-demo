"""Cross-code-location sensor for the ml_features dbt layer.

Watches for materializations of any dbt model in the `ml_features/*`
layer (the layer owned by ml_pipelines). When any of them materializes,
fires ml_training_job so segmentation + churn rebuild against fresh
features. Today the layer has a single model (customer_rfm); if you add
more ml-layer dbt models, they'll automatically become triggers — no
sensor edit required.

Implemented as a plain @sensor (not @asset_sensor or @multi_asset_sensor)
so it works without coupling to any specific key and survives future
ml_features additions. Cursor holds the latest materialization event id
we've observed, so we don't re-fire on every tick.
"""

from dagster import (
    AssetKey,
    EventRecordsFilter,
    DagsterEventType,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from ml_pipelines.jobs import ml_training_job


# Asset keys whose materialization should trigger ml_training_job.
# Kept here (instead of in a constants module) because it's the single
# place that needs it. Add new ml_features model keys here as they appear.
_WATCHED_KEYS = [
    AssetKey(["ml_features", "customer_rfm"]),
]


@sensor(
    name="ml_features_updated_sensor",
    job=ml_training_job,
    minimum_interval_seconds=30,
    description=(
        "Runs ml_training_job (segmentation + churn) whenever any dbt "
        "model in the ml_features layer materializes. Bridges "
        "ml_pipelines' dbt layer to its downstream ML assets."
    ),
)
def ml_features_updated_sensor(context: SensorEvaluationContext) -> SensorResult:
    last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
    new_last_seen = last_seen_id
    any_new = False

    for key in _WATCHED_KEYS:
        records = context.instance.get_event_records(
            EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=key,
                after_cursor=last_seen_id if last_seen_id else None,
            ),
            limit=50,
            ascending=True,
        )
        for r in records:
            any_new = True
            new_last_seen = max(new_last_seen, r.storage_id)

    if not any_new:
        return SensorResult(skip_reason=SkipReason("No new ml_features materializations."))

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=f"ml-features-{new_last_seen}",
                tags={
                    "trigger/source": "ml_features_updated_sensor",
                    "trigger/last_event_id": str(new_last_seen),
                },
            )
        ],
        cursor=str(new_last_seen),
    )
