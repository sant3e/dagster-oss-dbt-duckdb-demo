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
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    multi_asset_sensor,
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
    r"^(?P<prefix>[a-z_]+_)(?P<date>\d{4}_\d{2}_\d{2})\.csv$",
    re.IGNORECASE,
)
_MONTHLY_FILENAME_RE = re.compile(
    r"^(?P<prefix>[a-z_]+_)(?P<month>\d{4}_\d{2})\.csv$",
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


@multi_asset_sensor(
    name="daily_monthly_bridge_sensor",
    monitored_assets=[*_DAILY_UPSTREAMS, _MONTHLY_UPSTREAM],
    job=dbt_elt_job,
    minimum_interval_seconds=30,
    description=(
        "Bridges the daily / monthly cadence mismatch. Fires the daily "
        "ELT pipeline for day D only when ALL daily upstreams have a "
        "materialization for D AND the monthly upstream has a "
        "materialization for month-of(D). This is the pattern that makes "
        "it possible to run a daily pipeline whose reference data lands "
        "once a month."
    ),
)
def daily_monthly_bridge_sensor(context: MultiAssetSensorEvaluationContext):
    # Pull the latest materialization record per partition for each monitored asset.
    records = context.latest_materialization_records_by_partition_and_asset()

    # Build a quick lookup: has the monthly asset materialized for month M?
    monthly_materialized_months: set[str] = set()
    for partition_key, by_asset in records.items():
        if _MONTHLY_UPSTREAM in by_asset:
            monthly_materialized_months.add(partition_key)

    run_requests: list[RunRequest] = []
    advanced: dict[AssetKey, dict] = {}

    for partition_key, by_asset in records.items():
        # Skip the monthly partitions — we only kick runs off day-keyed partitions.
        if _MONTHLY_UPSTREAM in by_asset and len(by_asset) == 1:
            continue

        # Require all three daily upstreams for this day.
        daily_ready = all(k in by_asset for k in _DAILY_UPSTREAMS)
        if not daily_ready:
            continue

        # Require the month-of(D) partition of the monthly upstream to exist.
        month_needed = _month_partition_for_day(partition_key)
        if month_needed not in monthly_materialized_months:
            context.log.info(
                f"Day {partition_key} is ready on the daily side, but the monthly "
                f"partition {month_needed} of raw_prd_info_monthly has not been "
                f"materialized yet. Waiting."
            )
            continue

        run_requests.append(
            RunRequest(
                run_key=f"elt-bridge-{partition_key}",
                partition_key=partition_key,
                tags={
                    "trigger/source": "daily_monthly_bridge_sensor",
                    "partition": partition_key,
                    "monthly_bridge/month": month_needed,
                },
            )
        )
        # Advance the cursor past this day's daily materializations so we
        # don't re-fire for the same day. We deliberately DO NOT advance
        # the monthly cursor — the same monthly partition must stay
        # available to validate subsequent days of the same month.
        advanced[partition_key] = {
            k: v for k, v in by_asset.items() if k in _DAILY_UPSTREAMS
        }

    for partition_key, per_asset in advanced.items():
        context.advance_cursor(per_asset)

    if not run_requests:
        return SkipReason(
            "Waiting for all three daily upstreams AND the current month's "
            "monthly upstream to be ready for the same day."
        )
    return run_requests
