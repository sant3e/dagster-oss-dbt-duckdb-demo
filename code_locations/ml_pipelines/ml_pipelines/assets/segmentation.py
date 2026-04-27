"""Customer segmentation asset — KMeans over RFM features from dbt.

Daily-partitioned: reads `ml_features.customer_rfm` for the current
partition's snapshot_date and writes cluster labels to
`ml_features.customer_segments` using delete+insert on snapshot_date, so
re-runs replace just their own partition.
"""

from dagster import (
    AssetExecutionContext,
    AssetKey,
    asset,
)
from dagster_duckdb import DuckDBResource
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from ml_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    FRESHNESS_ML_DAILY,
    TRANSIENT_LOCK_RETRY_POLICY,
)
from ml_pipelines.partitions import daily_partitions

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
        "KMeans (k=4) segmentation over RFM features for the current partition. "
        "Reads customer_rfm filtered by snapshot_date and writes segment labels "
        "to ml_features.customer_segments (delete+insert per partition)."
    ),
    group_name="ml_segmentation",
    compute_kind="scikit-learn",
    partitions_def=daily_partitions,
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
    freshness_policy=FRESHNESS_ML_DAILY,
)
def customer_segments(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
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
            f"customer_rfm has no rows for partition {partition_key}; nothing to segment."
        )
        return

    features = rfm[["recency_days", "frequency", "monetary"]].astype(float)
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)
    model = KMeans(n_clusters=4, n_init=10, random_state=42)
    clusters = model.fit_predict(scaled)

    rfm["segment_id"] = clusters.astype(int)
    rfm["segment_label"] = rfm["segment_id"].map(_SEGMENT_LABELS)
    rfm["snapshot_date"] = partition_key

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS ml_features;")
        # Create the target table if it doesn't exist, else delete this
        # partition's rows and re-insert. Same pattern as the landing
        # assets' _upsert_partition helper.
        table_exists = conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='ml_features' AND table_name='customer_segments'"
        ).fetchone()
        conn.register("df_segments", rfm)
        if table_exists:
            conn.execute(
                "DELETE FROM ml_features.customer_segments WHERE snapshot_date = ?::DATE",
                [partition_key],
            )
            conn.execute(
                "INSERT INTO ml_features.customer_segments SELECT * FROM df_segments"
            )
        else:
            conn.execute(
                "CREATE TABLE ml_features.customer_segments AS SELECT * FROM df_segments"
            )
        conn.unregister("df_segments")

    context.add_output_metadata(
        {
            "partition": partition_key,
            "rows": int(len(rfm)),
            "clusters": int(rfm["segment_id"].nunique()),
            "segments_written_to": "ml_features.customer_segments",
        }
    )
