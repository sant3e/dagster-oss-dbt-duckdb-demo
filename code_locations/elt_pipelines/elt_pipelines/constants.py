"""Static names and paths referenced across elt_pipelines."""

from datetime import timedelta

from dagster import Backoff, FreshnessPolicy, Jitter, RetryPolicy

# Tag key used for DuckDB-writer concurrency serialization (see dagster.yaml).
DUCKDB_WRITER_CONCURRENCY_KEY = "duckdb_writer"
DUCKDB_WRITER_TAGS = {"dagster/concurrency_key": DUCKDB_WRITER_CONCURRENCY_KEY}

# Retry policy for assets that write to DuckDB and/or Dagster's SQLite
# metadata store. Transient lock errors can happen on macOS bind mounts
# (SQLite locking protocol) or if something holds the DuckDB file briefly.
# 2 retries with exponential backoff + jitter is enough to ride out both.
TRANSIENT_LOCK_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=2,            # base delay seconds
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

# DuckDB schemas used by landing assets (dbt also references these via source()).
RAW_SCHEMA = "raw"

# Asset group name for the Dagster-owned landing assets. The dbt-owned
# layers (landing/staging/mart/reporting) get their group names from
# the `+group:` config in dbt_project.yml, so we don't hard-code them here.
GROUP_LANDING = "elt_landing"

# Filename conventions for daily-snapshot CSVs dropped into DATA_LANDING_DIR.
# e.g. sales_details_2026_04_27.csv, cust_info_2026_04_27.csv, loc_a101_2026_04_27.csv
FILENAME_PREFIXES_DAILY = {
    "sales_details": "sales_details_",
    "cust_info": "cust_info_",
    "loc_a101": "loc_a101_",
}

# Filename convention for MONTHLY-snapshot CSVs.
# e.g. prd_info_2026_04.csv
FILENAME_PREFIXES_MONTHLY = {
    "prd_info": "prd_info_",
}

# Freshness policies attached to the Dagster-owned landing assets.
# Pure metadata — evaluated out-of-band by Dagster's automation sensor;
# does NOT inject a check step into materialization runs.
FRESHNESS_LANDING_DAILY = FreshnessPolicy.cron(
    deadline_cron="0 9 * * *", lower_bound_delta=timedelta(hours=24)
)
FRESHNESS_LANDING_MONTHLY = FreshnessPolicy.cron(
    deadline_cron="0 9 2 * *",
    # Must be <= the smallest interval between cron ticks. The smallest
    # month is February (28 days), so cap lower_bound_delta there.
    lower_bound_delta=timedelta(days=28),
)
