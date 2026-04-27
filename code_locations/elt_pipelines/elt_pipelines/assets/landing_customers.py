"""Daily-partitioned landing asset for CRM customer master snapshots."""

import os

from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource

from elt_pipelines.assets._landing_shared import land_daily_csv
from elt_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    FILENAME_PREFIXES,
    GROUP_LANDING,
)
from elt_pipelines.partitions import daily_partitions


@asset(
    name="raw_cust_info",
    key_prefix=["raw"],
    description=(
        "Landing table for daily-snapshotted CRM customer master. "
        "Reads /data/landing/cust_info_YYYY_MM_DD.csv for the run's partition."
    ),
    partitions_def=daily_partitions,
    group_name=GROUP_LANDING,
    compute_kind="duckdb",
    op_tags=DUCKDB_WRITER_TAGS,
)
def raw_cust_info(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    landing_dir = os.environ.get("DATA_LANDING_DIR", "/data/landing")
    land_daily_csv(
        context=context,
        duckdb=duckdb,
        landing_dir=landing_dir,
        filename_prefix=FILENAME_PREFIXES["cust_info"],
        table_name="cust_info",
    )
