"""Churn-risk classifier asset.

Takes the same RFM features and fits a logistic regression on a synthetic
"churned" label (recency > 90 days). Predicts churn probability for every
customer, writes results to `ml_features.churn_predictions`, and persists
the trained model + scaler to the artifacts volume using joblib.

This is intentionally simple — the point is to show a second ML asset
fanning out from the same dbt feature table, not to build a real model.
"""

import os
from pathlib import Path

import joblib
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Backoff,
    Jitter,
    RetryPolicy,
    asset,
)
from dagster_duckdb import DuckDBResource
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

CHURN_RECENCY_THRESHOLD_DAYS = 90

_TRANSIENT_LOCK_RETRY = RetryPolicy(
    max_retries=2,
    delay=2,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


@asset(
    name="churn_predictions",
    key_prefix=["ml_features"],
    deps=[AssetKey(["ml_features", "customer_rfm"])],
    description=(
        "Logistic-regression churn-risk model over RFM features. "
        "Writes probabilities to ml_features.churn_predictions and persists "
        "the fitted model + scaler to /warehouse/artifacts/churn_model.joblib."
    ),
    group_name="ml_churn",
    compute_kind="scikit-learn",
    op_tags={"dagster/concurrency_key": "duckdb_writer"},
    retry_policy=_TRANSIENT_LOCK_RETRY,
)
def churn_predictions(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        rfm = conn.execute(
            """
            SELECT customer_key, customer_id, recency_days, frequency, monetary
            FROM ml_features.customer_rfm
            """
        ).fetch_df()

    if rfm.empty:
        context.log.warning("customer_rfm is empty; nothing to train on.")
        return

    X = rfm[["recency_days", "frequency", "monetary"]].astype(float).to_numpy()
    # Synthetic label: "churned" if the customer has been inactive beyond the threshold.
    y = (rfm["recency_days"] > CHURN_RECENCY_THRESHOLD_DAYS).astype(int).to_numpy()

    # Guard against a degenerate label distribution (e.g. all churn or no churn in the sample).
    if len(set(y.tolist())) < 2:
        context.log.warning(
            "Churn label is constant in this dataset — skipping training. "
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

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ml_features;")
        conn.register("df_churn", rfm)
        conn.execute(
            "CREATE OR REPLACE TABLE ml_features.churn_predictions AS "
            "SELECT * FROM df_churn"
        )
        conn.unregister("df_churn")

    artifacts_dir = Path(os.environ.get("ARTIFACTS_DIR", "/warehouse/artifacts"))
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    model_path = artifacts_dir / "churn_model.joblib"
    joblib.dump({"model": model, "scaler": scaler}, model_path)

    context.add_output_metadata(
        {
            "rows": int(len(rfm)),
            "positive_rate": float(y.mean()),
            "high_risk_count": int(rfm["is_high_risk"].sum()),
            "model_artifact": str(model_path),
            "predictions_table": "ml_features.churn_predictions",
        }
    )
