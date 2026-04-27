"""Monthly-partitioned landing asset for the CRM product master.

Unlike the three daily sources (sales_details, cust_info, loc_a101), the
product catalog refreshes MONTHLY. Each month's snapshot lands as
`/data/landing/prd_info_YYYY_MM.csv` with a `snapshot_month` column.

Downstream dbt models join daily facts to whichever monthly snapshot is
"currently in effect" via a `snapshot_month <= sls_order_dt` lookup. The
cross-partition sensor in sensors.py handles the cadence mismatch by
only firing the downstream daily ELT for days whose month has a
materialized monthly snapshot.
"""

import os

from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource

from elt_pipelines.assets._landing_shared import land_monthly_csv
from elt_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    FILENAME_PREFIXES_MONTHLY,
    GROUP_LANDING,
    TRANSIENT_LOCK_RETRY_POLICY,
)
from elt_pipelines.partitions import monthly_partitions


@asset(
    name="raw_prd_info_monthly",
    key_prefix=["raw"],
    description=(
        "Monthly-partitioned landing table for the CRM product master. "
        "Reads /data/landing/prd_info_YYYY_MM.csv for the month-starting "
        "partition key and appends into DuckDB raw.prd_info."
    ),
    partitions_def=monthly_partitions,
    group_name=GROUP_LANDING,
    compute_kind="duckdb",
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
)
def raw_prd_info_monthly(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    landing_dir = os.environ.get("DATA_LANDING_DIR", "/data/landing")
    land_monthly_csv(
        context=context,
        duckdb=duckdb,
        landing_dir=landing_dir,
        filename_prefix=FILENAME_PREFIXES_MONTHLY["prd_info"],
        table_name="prd_info",
    )
