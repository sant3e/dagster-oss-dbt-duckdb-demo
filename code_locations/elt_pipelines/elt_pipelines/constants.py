"""Static names and paths referenced across elt_pipelines."""

# Tag key used for DuckDB-writer concurrency serialization (see dagster.yaml).
DUCKDB_WRITER_CONCURRENCY_KEY = "duckdb_writer"
DUCKDB_WRITER_TAGS = {"dagster/concurrency_key": DUCKDB_WRITER_CONCURRENCY_KEY}

# DuckDB schemas used by landing assets (dbt also references these via source()).
RAW_SCHEMA = "raw"

# Asset group names shown in the Dagster UI.
GROUP_LANDING = "elt_landing"
GROUP_STAGING = "elt_staging"
GROUP_MART = "elt_mart"
GROUP_REPORTING = "elt_reporting"

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

# Backwards-compatible alias (older code imports FILENAME_PREFIXES).
FILENAME_PREFIXES = FILENAME_PREFIXES_DAILY
