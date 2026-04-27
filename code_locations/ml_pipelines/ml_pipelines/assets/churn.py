"""Churn-risk classifier asset — daily-partitioned.

Takes the RFM features for the current partition, fits a logistic
regression on a synthetic "churned" label (recency above max(90 days,
median) so both classes exist even when the upstream data is older than
90 days across the board — the case with the sant3e sample, which has
2010-2014 order dates against a 2026 demo anchor). Writes predictions
into `ml_features.churn_predictions` with a snapshot_date column
(delete+insert per partition). The trained model + scaler are persisted
as a partition-stamped joblib file so you can see a new artifact per
partition.
"""

import os
from pathlib import Path

import joblib
import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetKey,
    asset,
)
from dagster_duckdb import DuckDBResource
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from ml_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    FRESHNESS_ML_DAILY,
    TRANSIENT_LOCK_RETRY_POLICY,
)
from ml_pipelines.partitions import daily_partitions

CHURN_RECENCY_THRESHOLD_DAYS = 90


@asset(
    name="churn_predictions",
    key_prefix=["ml_features"],
    deps=[AssetKey(["ml_features", "customer_rfm"])],
    description=(
        "Logistic-regression churn-risk model over RFM features for the current "
        "partition. Writes probabilities to ml_features.churn_predictions and "
        "persists the fitted model + scaler to "
        "/warehouse/artifacts/churn_model_<snapshot_date>.joblib."
    ),
    group_name="ml_churn",
    compute_kind="scikit-learn",
    partitions_def=daily_partitions,
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
    freshness_policy=FRESHNESS_ML_DAILY,
)
def churn_predictions(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    partition_key = context.partition_key  # "YYYY-MM-DD"

    with duckdb.get_connection() as conn:
        rfm = conn.execute(
            """
            SELECT customer_key, customer_id, recency_days, frequency, monetary
            FROM ml_features.customer_rfm
            WHERE snapshot_date = ?::DATE
            """,
            [partition_key],
        ).fetch_df()

    if rfm.empty:
        context.log.warning(
            f"customer_rfm has no rows for partition {partition_key}; nothing to train on."
        )
        return

    X = rfm[["recency_days", "frequency", "monetary"]].astype(float).to_numpy()
    # Synthetic "churned" label. Use max(fixed-threshold, median) so both
    # classes exist regardless of how stale the upstream dataset is (the
    # sant3e sample covers 2010-2014 while the demo anchor is 2026, so a
    # fixed 90-day threshold would paint everyone as churned).
    recency = rfm["recency_days"].astype(float).to_numpy()
    threshold = max(float(CHURN_RECENCY_THRESHOLD_DAYS), float(np.median(recency)))
    y = (recency > threshold).astype(int)

    if len(set(y.tolist())) < 2:
        context.log.warning(
            f"Churn label is constant for partition {partition_key} — skipping training. "
            "Add more varied data if you want a real demo."
        )
        return

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    model = LogisticRegression(max_iter=500)
    model.fit(X_scaled, y)

    probabilities = model.predict_proba(X_scaled)[:, 1]
    rfm["churn_probability"] = probabilities
    rfm["is_high_risk"] = rfm["churn_probability"] >= 0.5
    rfm["snapshot_date"] = partition_key

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ml_features;")
        table_exists = conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='ml_features' AND table_name='churn_predictions'"
        ).fetchone()
        conn.register("df_churn", rfm)
        if table_exists:
            conn.execute(
                "DELETE FROM ml_features.churn_predictions WHERE snapshot_date = ?::DATE",
                [partition_key],
            )
            conn.execute(
                "INSERT INTO ml_features.churn_predictions SELECT * FROM df_churn"
            )
        else:
            conn.execute(
                "CREATE TABLE ml_features.churn_predictions AS SELECT * FROM df_churn"
            )
        conn.unregister("df_churn")

    artifacts_dir = Path(os.environ.get("ARTIFACTS_DIR", "/warehouse/artifacts"))
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    model_path = artifacts_dir / f"churn_model_{partition_key}.joblib"
    joblib.dump({"model": model, "scaler": scaler, "snapshot_date": partition_key}, model_path)

    context.add_output_metadata(
        {
            "partition": partition_key,
            "rows": int(len(rfm)),
            "positive_rate": float(y.mean()),
            "high_risk_count": int(rfm["is_high_risk"].sum()),
            "model_artifact": str(model_path),
            "predictions_table": "ml_features.churn_predictions",
        }
    )
