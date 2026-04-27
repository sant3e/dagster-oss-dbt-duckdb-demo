"""Shared helpers for the partitioned landing assets.

Two flavours:

- `land_daily_csv`: reads a <prefix>YYYY_MM_DD.csv file and appends it to
  raw.<table> with a `snapshot_date` column derived from the partition key.
  Used by sales_details, cust_info, loc_a101.

- `land_monthly_csv`: reads a <prefix>YYYY_MM.csv file (no day) and appends
  it to raw.<table> with a `snapshot_month` column derived from the
  partition key's year+month (the day component is forced to the 1st).
  Used by prd_info.

Both are idempotent per partition: re-running the same partition deletes
its existing rows and re-inserts them. The dbt landing models read these
tables via {{ source('dagster_raw', ...) }} and pick the latest snapshot
per business key (see the landing/*.sql files).
"""

from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from elt_pipelines.constants import RAW_SCHEMA


def _upsert_partition(
    conn,
    fq_table: str,
    table_name: str,
    df: pd.DataFrame,
    snapshot_col: str,
    snapshot_value: str,
) -> int:
    """Create the table if missing, otherwise delete-then-insert for the partition."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")
    table_exists = conn.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [RAW_SCHEMA, table_name],
    ).fetchone()
    if table_exists:
        conn.execute(
            f"DELETE FROM {fq_table} WHERE {snapshot_col} = ?", [snapshot_value]
        )
        conn.register("df_landing", df)
        conn.execute(f"INSERT INTO {fq_table} SELECT * FROM df_landing")
        conn.unregister("df_landing")
    else:
        conn.register("df_landing", df)
        conn.execute(f"CREATE TABLE {fq_table} AS SELECT * FROM df_landing")
        conn.unregister("df_landing")

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM {fq_table} WHERE {snapshot_col} = ?",
        [snapshot_value],
    ).fetchone()[0]
    return int(row_count)


def land_daily_csv(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    landing_dir: str,
    filename_prefix: str,
    table_name: str,
) -> pd.DataFrame:
    """Load a single daily CSV snapshot into DuckDB raw.<table_name>.

    Expected filename: `<filename_prefix><YYYY_MM_DD>.csv`.
    Expected content: a `snapshot_date` column equal to the partition date.
    """
    partition_key = context.partition_key  # e.g. "2026-04-27"
    file_date_stamp = partition_key.replace("-", "_")  # "2026_04_27"
    filename = f"{filename_prefix}{file_date_stamp}.csv"
    file_path = Path(landing_dir) / filename

    if not file_path.exists():
        raise FileNotFoundError(
            f"Expected daily snapshot {file_path} for partition {partition_key}. "
            f"Drop the file into {landing_dir} and re-run."
        )

    df = pd.read_csv(file_path)
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = partition_key
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.strftime("%Y-%m-%d")

    fq_table = f"{RAW_SCHEMA}.{table_name}"
    with duckdb.get_connection() as conn:
        rows = _upsert_partition(
            conn, fq_table, table_name, df,
            snapshot_col="snapshot_date", snapshot_value=partition_key,
        )

    context.add_output_metadata(
        {
            "partition": partition_key,
            "cadence": "daily",
            "filename": filename,
            "rows_ingested": rows,
            "target_table": fq_table,
        }
    )
    return df


def land_monthly_csv(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    landing_dir: str,
    filename_prefix: str,
    table_name: str,
) -> pd.DataFrame:
    """Load a single monthly CSV snapshot into DuckDB raw.<table_name>.

    Expected filename: `<filename_prefix><YYYY_MM>.csv` (no day component).
    Expected content: a `snapshot_month` column with value YYYY-MM-01.

    Partition keys from Dagster's MonthlyPartitionsDefinition are already
    YYYY-MM-01 (the 1st of the month), so we use them as-is for both the
    file stamp lookup and the column value.
    """
    partition_key = context.partition_key  # e.g. "2026-04-01"
    # Use just year+month for the filename stamp: "2026_04"
    ym_stamp = partition_key[:7].replace("-", "_")  # "2026_04"
    filename = f"{filename_prefix}{ym_stamp}.csv"
    file_path = Path(landing_dir) / filename

    if not file_path.exists():
        raise FileNotFoundError(
            f"Expected monthly snapshot {file_path} for partition {partition_key}. "
            f"Drop the file into {landing_dir} and re-run."
        )

    df = pd.read_csv(file_path)
    if "snapshot_month" not in df.columns:
        df["snapshot_month"] = partition_key
    df["snapshot_month"] = pd.to_datetime(df["snapshot_month"]).dt.strftime("%Y-%m-%d")

    fq_table = f"{RAW_SCHEMA}.{table_name}"
    with duckdb.get_connection() as conn:
        rows = _upsert_partition(
            conn, fq_table, table_name, df,
            snapshot_col="snapshot_month", snapshot_value=partition_key,
        )

    context.add_output_metadata(
        {
            "partition": partition_key,
            "cadence": "monthly",
            "filename": filename,
            "rows_ingested": rows,
            "target_table": fq_table,
        }
    )
    return df
