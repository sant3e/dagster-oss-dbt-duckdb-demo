"""Customer segmentation asset — KMeans over RFM features from dbt.

Reads the `customer_rfm` dbt asset (owned by ml_team, materialized in the
ml_features schema in DuckDB) and writes cluster labels back to
`ml_features.customer_segments`. This demonstrates cross-code-location
asset dependency: the upstream asset (`customer_rfm`) is declared in this
same code location but depends on elt_pipelines' reporting layer.
"""

from dagster import AssetExecutionContext, AssetKey, asset
from dagster_duckdb import DuckDBResource
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

_SEGMENT_LABELS = {
    0: "At Risk",
    1: "Loyal",
    2: "Promising",
    3: "Champion",
}


@asset(
    name="customer_segments",
    key_prefix=["ml_features"],
    deps=[AssetKey(["ml_features", "customer_rfm"])],
    description=(
        "KMeans (k=4) segmentation over RFM features. Reads customer_rfm from "
        "DuckDB, writes segment labels to ml_features.customer_segments."
    ),
    group_name="ml_segmentation",
    compute_kind="scikit-learn",
    op_tags={"dagster/concurrency_key": "duckdb_writer"},
)
def customer_segments(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        rfm = conn.execute(
            """
            SELECT customer_key, customer_id, recency_days, frequency, monetary
            FROM ml_features.customer_rfm
            """
        ).fetch_df()

    if rfm.empty:
        context.log.warning("customer_rfm is empty; nothing to segment.")
        return

    features = rfm[["recency_days", "frequency", "monetary"]].astype(float)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)
    model = KMeans(n_clusters=4, n_init=10, random_state=42)
    clusters = model.fit_predict(scaled)

    rfm["segment_id"] = clusters.astype(int)
    rfm["segment_label"] = rfm["segment_id"].map(_SEGMENT_LABELS)

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ml_features;")
        conn.register("df_segments", rfm)
        conn.execute(
            "CREATE OR REPLACE TABLE ml_features.customer_segments AS "
            "SELECT * FROM df_segments"
        )
        conn.unregister("df_segments")

    context.add_output_metadata(
        {
            "rows": int(len(rfm)),
            "clusters": int(rfm["segment_id"].nunique()),
            "segments_written_to": "ml_features.customer_segments",
        }
    )
