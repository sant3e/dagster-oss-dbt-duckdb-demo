"""Partition definitions shared across elt_pipelines."""

from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

# Daily partitions for the transactional + master-data landing assets.
# Covers enough history to demo backfill without being unwieldy.
daily_partitions = DailyPartitionsDefinition(start_date="2026-04-01")

# Monthly partitions for reference-data sources that arrive once per month.
# Each monthly partition key is the 1st of the month (e.g. "2026-04-01").
# end_offset=1 so the CURRENT month is also a valid partition (otherwise
# Dagster's default behaviour is to only offer completed months).
monthly_partitions = MonthlyPartitionsDefinition(
    start_date="2026-04-01",
    end_offset=1,
)
