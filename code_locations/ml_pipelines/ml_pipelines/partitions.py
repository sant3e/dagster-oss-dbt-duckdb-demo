"""Partition definitions for ml_pipelines.

Kept deliberately local (not cross-imported from elt_pipelines) — code
locations are independently deployable and shouldn't reach into each
other's packages at import time. The definition MUST match
elt_pipelines/partitions.py so cross-location dependencies line up on
partition keys.
"""

from dagster import DailyPartitionsDefinition

# Matches elt_pipelines.partitions.daily_partitions exactly.
daily_partitions = DailyPartitionsDefinition(start_date="2026-04-01")
