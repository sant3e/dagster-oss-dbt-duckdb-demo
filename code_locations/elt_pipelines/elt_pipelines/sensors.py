"""Sensors for elt_pipelines.

Two sensors showcasing different Dagster patterns:

1. `landing_file_sensor` — polls the landing directory every 30 s and
   launches the right partitioned landing asset for each new CSV file.
   Handles BOTH cadences:
     - `<prefix>YYYY_MM_DD.csv` → daily landing asset for that day
     - `prd_info_YYYY_MM.csv`   → monthly landing asset for that month

2. `daily_monthly_bridge_sensor` — a multi-asset sensor that fires the
   daily ELT dbt pipeline for a given day D only when:
     - raw_sales_details, raw_cust_info, raw_loc_a101 have all materialized
       for partition D, AND
     - raw_prd_info_monthly has materialized for month-of(D).

   This is the pattern from imp_finance_mart: the downstream is daily, but
   one of its upstreams is monthly. Without this sensor, daily downstreams
   could not auto-materialize for day D because the monthly upstream has
   no "day D" partition — it has a "1st-of-month" partition that needs to
   be reused for every day of the month. The sensor bridges that gap.
"""

import os
import re
from datetime import datetime
from pathlib import Path

from dagster import (
    AssetKey,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from elt_pipelines.constants import FILENAME_PREFIXES_DAILY, FILENAME_PREFIXES_MONTHLY
from elt_pipelines.jobs import dbt_elt_job, landing_daily_job, landing_monthly_job

# Maps daily filename prefix -> AssetKey of the daily landing asset it triggers.
_DAILY_FILENAME_TO_ASSET_KEY = {
    FILENAME_PREFIXES_DAILY["sales_details"]: AssetKey(["raw", "raw_sales_details"]),
    FILENAME_PREFIXES_DAILY["cust_info"]: AssetKey(["raw", "raw_cust_info"]),
    FILENAME_PREFIXES_DAILY["loc_a101"]: AssetKey(["raw", "raw_loc_a101"]),
}

# Maps monthly filename prefix -> AssetKey of the monthly landing asset.
_MONTHLY_FILENAME_TO_ASSET_KEY = {
    FILENAME_PREFIXES_MONTHLY["prd_info"]: AssetKey(["raw", "raw_prd_info_monthly"]),
}

_DAILY_FILENAME_RE = re.compile(
    r"^(?P<prefix>[a-z0-9_]+?_)(?P<date>\d{4}_\d{2}_\d{2})\.csv$",
    re.IGNORECASE,
)
_MONTHLY_FILENAME_RE = re.compile(
    r"^(?P<prefix>[a-z0-9_]+?_)(?P<month>\d{4}_\d{2})\.csv$",
    re.IGNORECASE,
)

# AssetKeys watched by the bridge sensor.
_DAILY_UPSTREAMS = [
    AssetKey(["raw", "raw_sales_details"]),
    AssetKey(["raw", "raw_cust_info"]),
    AssetKey(["raw", "raw_loc_a101"]),
]
_MONTHLY_UPSTREAM = AssetKey(["raw", "raw_prd_info_monthly"])


@sensor(
    name="landing_file_sensor",
    jobs=[landing_daily_job, landing_monthly_job],
    minimum_interval_seconds=30,
    description=(
        "Polls DATA_LANDING_DIR for new snapshot CSVs and kicks the matching "
        "partitioned landing asset. Daily files: <prefix>_YYYY_MM_DD.csv. "
        "Monthly files: prd_info_YYYY_MM.csv."
    ),
)
def landing_file_sensor(context: SensorEvaluationContext) -> SensorResult:
    landing_dir = Path(os.environ.get("DATA_LANDING_DIR", "/data/landing"))
    if not landing_dir.exists():
        return SensorResult(skip_reason=SkipReason(f"{landing_dir} does not exist yet."))

    cursor_value = float(context.cursor or 0.0)
    new_cursor = cursor_value
    run_requests: list[RunRequest] = []

    # Match MONTHLY files first (their prefix is a strict subset of the daily
    # regex match, so we resolve to the more specific pattern by checking
    # monthly first).
    for entry in sorted(landing_dir.iterdir()):
        if not entry.is_file() or entry.suffix.lower() != ".csv":
            continue
        mtime = entry.stat().st_mtime
        if mtime <= cursor_value:
            continue

        monthly_match = _MONTHLY_FILENAME_RE.match(entry.name)
        daily_match = _DAILY_FILENAME_RE.match(entry.name)

        # Decide cadence by checking monthly-prefix set first, then daily.
        asset_key = None
        partition_key = None
        job_name = None

        if monthly_match and monthly_match.group("prefix").lower() in _MONTHLY_FILENAME_TO_ASSET_KEY:
            asset_key = _MONTHLY_FILENAME_TO_ASSET_KEY[monthly_match.group("prefix").lower()]
            ym = monthly_match.group("month").replace("_", "-")  # "2026-04"
            partition_key = f"{ym}-01"
            job_name = "landing_monthly_job"
        elif daily_match and daily_match.group("prefix").lower() in _DAILY_FILENAME_TO_ASSET_KEY:
            asset_key = _DAILY_FILENAME_TO_ASSET_KEY[daily_match.group("prefix").lower()]
            partition_key = daily_match.group("date").replace("_", "-")
            job_name = "landing_daily_job"
        else:
            context.log.info(f"Skipping {entry.name} — no matching landing asset.")
            new_cursor = max(new_cursor, mtime)
            continue

        new_cursor = max(new_cursor, mtime)
        run_requests.append(
            RunRequest(
                run_key=f"{entry.name}-{mtime}",
                partition_key=partition_key,
                asset_selection=[asset_key],
                job_name=job_name,
                tags={
                    "trigger/source": "landing_file_sensor",
                    "landing/file": entry.name,
                },
            )
        )

    if not run_requests:
        return SensorResult(
            skip_reason=SkipReason("No new landing CSVs."),
            cursor=str(new_cursor),
        )

    return SensorResult(run_requests=run_requests, cursor=str(new_cursor))


def _month_partition_for_day(day_key: str) -> str:
    """Return the 1st-of-month partition key for a given daily key.

    "2026-04-27" -> "2026-04-01"
    """
    d = datetime.strptime(day_key, "%Y-%m-%d").date()
    return d.replace(day=1).isoformat()


@sensor(
    name="daily_monthly_bridge_sensor",
    job=dbt_elt_job,
    minimum_interval_seconds=30,
    description=(
        "Bridges the daily / monthly cadence mismatch. Fires the daily "
        "ELT pipeline for day D only when ALL daily upstreams have a "
        "materialization for D AND the monthly upstream has a "
        "materialization for month-of(D). This is the pattern that makes "
        "it possible to run a daily pipeline whose reference data lands "
        "once a month."
        ""
        "Implemented as a regular @sensor (not @multi_asset_sensor) because "
        "the latter requires all monitored assets to share the same "
        "partitions definition — which is exactly the constraint we need "
        "to bridge."
    ),
)
def daily_monthly_bridge_sensor(context: SensorEvaluationContext):
    instance = context.instance

    # Cursor holds the JSON list of partition_keys we've already fired on,
    # so we don't re-kick the ELT for the same day twice.
    import json
    already_fired: set[str] = set(json.loads(context.cursor)) if context.cursor else set()

    # Which months does the monthly upstream already have materializations for?
    monthly_done_months: set[str] = set()
    for record in instance.fetch_materializations(
        records_filter=_MONTHLY_UPSTREAM, limit=1000
    ).records:
        pk = record.partition_key
        if pk:
            monthly_done_months.add(pk)

    # Find days where ALL THREE daily upstreams have a materialization.
    # We scan each daily upstream's materializations and intersect the partition sets.
    per_asset_partitions: list[set[str]] = []
    for ak in _DAILY_UPSTREAMS:
        partitions_for_this_asset: set[str] = set()
        for record in instance.fetch_materializations(
            records_filter=ak, limit=1000
        ).records:
            if record.partition_key:
                partitions_for_this_asset.add(record.partition_key)
        per_asset_partitions.append(partitions_for_this_asset)

    if not per_asset_partitions:
        return SensorResult(skip_reason=SkipReason("No daily materializations yet."))

    days_ready_on_daily_side = set.intersection(*per_asset_partitions)

    run_requests: list[RunRequest] = []
    newly_ready_days: list[str] = []
    for day in sorted(days_ready_on_daily_side):
        if day in already_fired:
            continue
        month_needed = _month_partition_for_day(day)
        if month_needed not in monthly_done_months:
            context.log.info(
                f"Day {day} is ready on the daily side, but the monthly "
                f"partition {month_needed} of raw_prd_info_monthly has not "
                f"been materialized yet. Waiting."
            )
            continue
        newly_ready_days.append(day)
        already_fired.add(day)

    # Collapse N newly-ready days into ONE ELT run. dbt_elt_job is
    # unpartitioned and rebuilds the whole pipeline from the current state
    # of raw.* tables, so firing it once picks up all new days at once.
    # Firing N times would just do the same work N times.
    if newly_ready_days:
        run_requests.append(
            RunRequest(
                run_key="elt-bridge-" + ",".join(newly_ready_days),
                tags={
                    "trigger/source": "daily_monthly_bridge_sensor",
                    "triggered_for_days": ",".join(newly_ready_days),
                    "triggered_day_count": str(len(newly_ready_days)),
                },
            )
        )

    new_cursor = json.dumps(sorted(already_fired))
    if not run_requests:
        return SensorResult(
            skip_reason=SkipReason(
                "Waiting for all three daily upstreams AND the current "
                "month's monthly upstream to be ready for the same day."
            ),
            cursor=new_cursor,
        )
    return SensorResult(run_requests=run_requests, cursor=new_cursor)
