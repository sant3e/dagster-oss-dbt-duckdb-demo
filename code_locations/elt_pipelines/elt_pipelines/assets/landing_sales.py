"""Daily-partitioned landing asset for CRM sales transactions."""

import os

from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource

from elt_pipelines.assets._landing_shared import land_daily_csv
from elt_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    FILENAME_PREFIXES_DAILY,
    GROUP_LANDING,
)
from elt_pipelines.partitions import daily_partitions


@asset(
    name="raw_sales_details",
    key_prefix=["raw"],
    description=(
        "Landing table for daily-snapshotted CRM sales transactions. "
        "Reads /data/landing/sales_details_YYYY_MM_DD.csv for the run's partition "
        "and appends into DuckDB raw.sales_details."
    ),
    partitions_def=daily_partitions,
    group_name=GROUP_LANDING,
    compute_kind="duckdb",
    op_tags=DUCKDB_WRITER_TAGS,
)
def raw_sales_details(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    landing_dir = os.environ.get("DATA_LANDING_DIR", "/data/landing")
    land_daily_csv(
        context=context,
        duckdb=duckdb,
        landing_dir=landing_dir,
        filename_prefix=FILENAME_PREFIXES_DAILY["sales_details"],
        table_name="sales_details",
    )
