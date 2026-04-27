"""Daily-partitioned landing asset for ERP customer locations."""

import os

from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource

from elt_pipelines.assets._landing_shared import land_daily_csv
from elt_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    FILENAME_PREFIXES_DAILY,
    FRESHNESS_LANDING_DAILY,
    GROUP_LANDING,
    TRANSIENT_LOCK_RETRY_POLICY,
)
from elt_pipelines.partitions import daily_partitions


@asset(
    name="raw_loc_a101",
    key_prefix=["raw"],
    description=(
        "Landing table for daily-snapshotted ERP customer locations. "
        "Reads /data/landing/loc_a101_YYYY_MM_DD.csv for the run's partition."
    ),
    partitions_def=daily_partitions,
    group_name=GROUP_LANDING,
    compute_kind="duckdb",
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
    freshness_policy=FRESHNESS_LANDING_DAILY,
)
def raw_loc_a101(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    landing_dir = os.environ.get("DATA_LANDING_DIR", "/data/landing")
    land_daily_csv(
        context=context,
        duckdb=duckdb,
        landing_dir=landing_dir,
        filename_prefix=FILENAME_PREFIXES_DAILY["loc_a101"],
        table_name="loc_a101",
    )
