"""Shared constants for ml_pipelines (mirror of elt_pipelines.constants)."""

from dagster import Backoff, Jitter, RetryPolicy

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
