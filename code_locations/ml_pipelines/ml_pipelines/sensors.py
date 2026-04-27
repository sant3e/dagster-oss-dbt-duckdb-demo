"""Cross-code-location sensor from elt_pipelines → ml_pipelines.

Bridges the two code locations' asset graphs. Watches the ELT reporting
layer for new materializations — `rpt_sales_summary_by_customer` is the
upstream of `customer_rfm` in the ml_features layer, so a new partition
of the report is the signal that ML can train for that partition.

On a new partition materialization of the report, fires `ml_training_job`
with the same partition_key so `customer_rfm → customer_segments +
churn_predictions` all run for that same day.

If this sensor is OFF, the ml assets never auto-fire. That's the
layered-sensor invariant: ELT can move without dragging ML along with it.

Implemented as a plain @sensor (not @asset_sensor) so we can control
the RunRequest's partition_key explicitly, and so it survives future
changes to the upstream AssetKey without needing a decorator edit.
"""

from dagster import (
    AssetKey,
    DagsterEventType,
    EventRecordsFilter,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from ml_pipelines.jobs import ml_training_job

# Watching the reporting-layer asset that is the direct upstream of
# customer_rfm. When this materializes for a new partition, ML can train.
_WATCHED_KEY = AssetKey(["reporting", "rpt_sales_summary_by_customer"])


@sensor(
    name="elt_to_ml_bridge_sensor",
    job=ml_training_job,
    minimum_interval_seconds=30,
    description=(
        "Cross-code-location bridge: runs ml_training_job for every new "
        "partition of reporting.rpt_sales_summary_by_customer. Propagates "
        "the partition_key so customer_rfm, customer_segments, and "
        "churn_predictions all build for the same day. Keep OFF to stop "
        "the ML chain from auto-firing."
    ),
)
def elt_to_ml_bridge_sensor(context: SensorEvaluationContext) -> SensorResult:
    last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
    new_last_seen = last_seen_id

    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=_WATCHED_KEY,
            after_cursor=last_seen_id if last_seen_id else None,
        ),
        limit=50,
        ascending=True,
    )

    run_requests: list[RunRequest] = []
    fired_partitions: set[str] = set()
    for r in records:
        new_last_seen = max(new_last_seen, r.storage_id)
        partition_key = getattr(r, "partition_key", None) or (
            r.event_log_entry.dagster_event.partition
            if r.event_log_entry and r.event_log_entry.dagster_event
            else None
        )
        if not partition_key or partition_key in fired_partitions:
            continue
        fired_partitions.add(partition_key)
        run_requests.append(
            RunRequest(
                run_key=f"ml-training-{partition_key}-{r.storage_id}",
                partition_key=partition_key,
                tags={
                    "trigger/source": "elt_to_ml_bridge_sensor",
                    "trigger/upstream_event_id": str(r.storage_id),
                },
            )
        )

    if not run_requests:
        return SensorResult(
            skip_reason=SkipReason(
                "No new partitions of rpt_sales_summary_by_customer."
            ),
            cursor=str(new_last_seen),
        )

    return SensorResult(run_requests=run_requests, cursor=str(new_last_seen))
