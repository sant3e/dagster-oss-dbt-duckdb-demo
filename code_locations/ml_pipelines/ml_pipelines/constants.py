"""Shared constants for ml_pipelines (mirror of elt_pipelines.constants)."""

from datetime import timedelta

from dagster import Backoff, FreshnessPolicy, Jitter, RetryPolicy

DUCKDB_WRITER_CONCURRENCY_KEY = "duckdb_writer"
DUCKDB_WRITER_TAGS = {"dagster/concurrency_key": DUCKDB_WRITER_CONCURRENCY_KEY}

# Transient-lock retry policy — same trade-off as elt_pipelines. Covers
# DuckDB file locks and Dagster SQLite locks on macOS bind-mounts.
TRANSIENT_LOCK_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=2,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

# Freshness policy for ML outputs — daily, deadline 11am (an hour after
# elt's 10am deadline so the ml chain has time to run after reporting).
# Pure metadata; does not inject check steps into materialization runs.
FRESHNESS_ML_DAILY = FreshnessPolicy.cron(
    deadline_cron="0 11 * * *", lower_bound_delta=timedelta(hours=24)
)
