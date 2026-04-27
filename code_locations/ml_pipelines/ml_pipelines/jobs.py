"""Jobs for ml_pipelines."""

from dagster import AssetSelection, define_asset_job

ml_training_job = define_asset_job(
    name="ml_training_job",
    selection=AssetSelection.keys(
        ["ml_features", "customer_segments"],
        ["ml_features", "churn_predictions"],
    ),
    tags={"dagster/concurrency_key": "duckdb_writer"},
    description="Runs customer segmentation + churn-risk models from the same RFM features.",
)

all_jobs = [ml_training_job]
