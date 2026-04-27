"""Jobs for ml_pipelines.

See elt_pipelines.jobs for the rationale on explicit freshness-check
subtraction (`.without_checks()` doesn't strip externally-registered
AssetChecksDefinition objects in Dagster 1.12).
"""

from dagster import AssetKey, AssetSelection, define_asset_job

from ml_pipelines.constants import DUCKDB_WRITER_TAGS
from ml_pipelines.freshness_checks import all_freshness_checks
from ml_pipelines.partitions import daily_partitions

_FRESHNESS_CHECKS_SELECTION = AssetSelection.checks(*all_freshness_checks)

# ml_training_job is daily-partitioned: it builds customer_rfm (dbt) +
# customer_segments + churn_predictions for a single partition_key. The
# elt_to_ml_bridge_sensor drives it, propagating the partition_key from
# an ELT reporting materialization.
ml_training_job = define_asset_job(
    name="ml_training_job",
    selection=(
        AssetSelection.keys(
            AssetKey(["ml_features", "customer_rfm"]),
            AssetKey(["ml_features", "customer_segments"]),
            AssetKey(["ml_features", "churn_predictions"]),
        )
        - _FRESHNESS_CHECKS_SELECTION
    ),
    partitions_def=daily_partitions,
    tags=DUCKDB_WRITER_TAGS,
    description=(
        "Builds customer_rfm (dbt) + customer_segments (KMeans) + "
        "churn_predictions (LogReg) for a single daily partition."
    ),
)

all_jobs = [ml_training_job]
