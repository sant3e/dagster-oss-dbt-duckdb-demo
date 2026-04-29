"""Sensors for elt_pipelines.

Two sensors:

1. `landing_file_sensor` — polls DATA_LANDING_DIR every 30 s and launches
   the matching partitioned landing asset for each new CSV. Handles BOTH
   cadences:
     - `<prefix>YYYY_MM_DD.csv` → daily landing asset for that day
     - `prd_info_YYYY_MM.csv`   → monthly landing asset for that month

2. `cross_partition_sensor` — tag-driven cross-partition expansion.
   Ported from imp_finance_mart/bhi_imp/sensor/cross_partition_sensor.py.

   The pattern:
     * dbt models whose data updates on a slower cadence than the rest
       of the pipeline (e.g. monthly) are tagged `latest_available_source`
       in their {{ config(tags=[...]) }} block.
     * dbt models that consume a `latest_available_source` and need to
       still run daily (reusing the latest available source snapshot
       until a newer one arrives) are tagged `latest_available`.

   When the sensor ticks, it reads the dbt manifest, finds every asset
   tagged `latest_available`, classifies its deps as exact_match vs
   latest_available, and — if a `latest_available` asset has ONLY
   `latest_available_source` deps — runs it in EXPANSION MODE:
     * builds a date range from the earliest source partition to yesterday
     * limits to the N most recent days (EXPANSION_PARTITION_LIMIT)
     * emits one RunRequest(partition_key=day) per missing/stale day

   This is exactly what keeps daily downstreams moving even when a
   monthly upstream hasn't been refreshed: April 1 monthly arrives →
   daily downstream fires for April 1-N using that monthly snapshot;
   May 1 monthly arrives → daily downstream fires for May 1-N using
   the newer snapshot. The dbt SQL's latest-available-on-or-before
   filter picks the right monthly row inside each run.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dagster import (
    AssetKey,
    AssetSelection,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from elt_pipelines.constants import FILENAME_PREFIXES_DAILY, FILENAME_PREFIXES_MONTHLY
from elt_pipelines.jobs import landing_daily_job, landing_monthly_job

# ---------------------------------------------------------------------------
# landing_file_sensor (unchanged from the previous iteration)
# ---------------------------------------------------------------------------

_DAILY_FILENAME_TO_ASSET_KEY = {
    FILENAME_PREFIXES_DAILY["sales_details"]: AssetKey(["raw", "raw_sales_details"]),
    FILENAME_PREFIXES_DAILY["cust_info"]: AssetKey(["raw", "raw_cust_info"]),
    FILENAME_PREFIXES_DAILY["loc_a101"]: AssetKey(["raw", "raw_loc_a101"]),
}

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

    for entry in sorted(landing_dir.iterdir()):
        if not entry.is_file() or entry.suffix.lower() != ".csv":
            continue
        mtime = entry.stat().st_mtime
        if mtime <= cursor_value:
            continue

        monthly_match = _MONTHLY_FILENAME_RE.match(entry.name)
        daily_match = _DAILY_FILENAME_RE.match(entry.name)

        asset_key = None
        partition_key = None
        job_name = None

        if monthly_match and monthly_match.group("prefix").lower() in _MONTHLY_FILENAME_TO_ASSET_KEY:
            asset_key = _MONTHLY_FILENAME_TO_ASSET_KEY[monthly_match.group("prefix").lower()]
            ym = monthly_match.group("month").replace("_", "-")
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
                    "triggered_by": "landing_file_sensor",
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


# ---------------------------------------------------------------------------
# cross_partition_sensor — tag-driven expansion
# ---------------------------------------------------------------------------
# Ported from imp_finance_mart/bhi_imp/sensor/cross_partition_sensor.py.
# Same function names, same semantics, same tag conventions; simplified
# where imp_finance_mart had features we don't use here (code-version
# pinning, branch-deployment rejection, multi-brand scheduling).

# Constants mirror the reference.
EXPANSION_PARTITION_LIMIT = 7
TRIGGERED_EXPIRY_SECONDS = 3600  # 1h — partitions we've fired won't re-fire for this long

DBT_TARGET_PATH = Path(os.environ.get("DBT_TARGET_PATH", "/tmp/dbt_target"))
DBT_MANIFEST_PATH = DBT_TARGET_PATH / "manifest.json"


# --- manifest helpers -------------------------------------------------------

def get_dbt_manifest() -> dict:
    with open(DBT_MANIFEST_PATH, "r") as f:
        return json.load(f)


def find_assets_with_tag(manifest: dict, tag: str) -> List[dict]:
    out = []
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model" and tag in node.get("tags", []):
            out.append({
                "node_id": node_id,
                "name": node.get("name"),
                "tags": node.get("tags", []),
                "depends_on": node.get("depends_on", {}).get("nodes", []),
            })
    return out


def _layer_from_dbt_path(p: str) -> Optional[str]:
    p = (p or "").replace("\\", "/")
    for layer in ("staging", "mart", "reporting", "ml_features"):
        if f"models/{layer}/" in p:
            return layer
    return None


def _asset_key_from_dbt_node(manifest: dict, node_id: str) -> AssetKey:
    """Mirror EltDbtTranslator.get_asset_key so the sensor picks the same keys.

    Handles BOTH manifest['nodes'] (models + seeds) and manifest['sources']
    (dbt sources). The former resolves to layer-prefixed AssetKeys; the
    latter resolves to the Dagster asset that Dagster-owned landings populate
    (e.g. source 'dagster_raw.prd_info' → AssetKey(['raw', 'raw_prd_info_monthly'])).
    """
    if node_id in manifest.get("sources", {}):
        src = manifest["sources"][node_id]
        # Source name alone isn't enough to derive the Dagster-side asset key
        # (dbt source 'prd_info' is populated by raw/raw_prd_info_monthly).
        # Fall back to a placeholder that's not used for Dagster-instance
        # lookups — the sensor only uses this for logging when the dep is a
        # source. Per-source overrides live in _SOURCE_TO_DAGSTER_ASSET.
        return _SOURCE_TO_DAGSTER_ASSET.get(
            node_id, AssetKey([src.get("source_name", "source"), src.get("name", "")])
        )
    node = manifest["nodes"][node_id]
    name = node.get("name")
    if node.get("resource_type") == "seed":
        return AssetKey(["seeds", name])
    layer = _layer_from_dbt_path(node.get("original_file_path", ""))
    if layer:
        return AssetKey([layer, name])
    return AssetKey([name])


# Map dbt source IDs → the Dagster AssetKey of the asset that populates them.
# Needed because dbt sources carry no layer info and the sensor queries the
# Dagster instance for materialized partitions, which is keyed by AssetKey.
_SOURCE_TO_DAGSTER_ASSET: Dict[str, AssetKey] = {
    "source.dbt_oss_template.dagster_raw.prd_info": AssetKey(["raw", "raw_prd_info_monthly"]),
    "source.dbt_oss_template.dagster_raw.sales_details": AssetKey(["raw", "raw_sales_details"]),
    "source.dbt_oss_template.dagster_raw.cust_info": AssetKey(["raw", "raw_cust_info"]),
    "source.dbt_oss_template.dagster_raw.loc_a101": AssetKey(["raw", "raw_loc_a101"]),
}


def _get_node_or_source(manifest: dict, node_id: str) -> Optional[dict]:
    """Return the dbt manifest entry for a node_id, checking both models and sources."""
    if node_id in manifest.get("nodes", {}):
        return manifest["nodes"][node_id]
    if node_id in manifest.get("sources", {}):
        return manifest["sources"][node_id]
    return None


def get_asset_dependencies_with_keys(
    manifest: dict, node_id: str
) -> Dict[str, List[Tuple[str, AssetKey]]]:
    """Classify a node's direct deps into exact_match vs latest_available.

    Faithful port of imp_finance_mart's get_asset_dependencies_with_keys,
    extended to recognize dbt SOURCES (not just models) tagged
    `latest_available_source`. This matters here because the slow-cadence
    asset in our setup is a Dagster-owned landing (raw_prd_info_monthly),
    which appears in the manifest as a dbt source, not a dbt model.

    A dep is classified `latest_available` iff it carries the
    `latest_available_source` tag — i.e. it is a slow-cadence source that
    downstream daily models must project onto the daily grid via
    latest-available-on-or-before. Every other dep is `exact_match`.
    """
    node = manifest["nodes"][node_id]
    exact: List[Tuple[str, AssetKey]] = []
    latest: List[Tuple[str, AssetKey]] = []

    for dep_id in node.get("depends_on", {}).get("nodes", []):
        dep_node = _get_node_or_source(manifest, dep_id)
        if dep_node is None:
            continue
        dep_name = dep_node.get("name")
        dep_key = _asset_key_from_dbt_node(manifest, dep_id)
        if "latest_available_source" in dep_node.get("tags", []):
            latest.append((dep_name, dep_key))
        else:
            exact.append((dep_name, dep_key))

    return {"exact_match": exact, "latest_available": latest}


# --- small pure helpers -----------------------------------------------------

def _is_valid_date(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def find_latest_available_partition(
    partitions: List[str], target_date: str
) -> Optional[str]:
    """Return the most recent partition whose date is <= target_date.

    e.g. partitions=['2026-04-01'], target_date='2026-04-15' -> '2026-04-01'.
    Identical to the reference implementation.
    """
    try:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        valid = [
            (datetime.strptime(p, "%Y-%m-%d"), p)
            for p in partitions
            if _is_valid_date(p) and datetime.strptime(p, "%Y-%m-%d") <= target_dt
        ]
        return max(valid, key=lambda x: x[0])[1] if valid else None
    except Exception:
        return None


def generate_expansion_date_range(source_partitions: List[str]) -> List[str]:
    """From the earliest source partition to yesterday, day-by-day.

    Identical to the reference. 'Yesterday' is UTC-based to match Dagster.
    """
    dates = [
        datetime.strptime(p, "%Y-%m-%d").date()
        for p in source_partitions
        if _is_valid_date(p)
    ]
    if not dates:
        return []
    start = min(dates)
    end = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    out: List[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def get_latest_partitions_only(partitions: List[str], limit: int) -> List[str]:
    dated = sorted(
        [(datetime.strptime(p, "%Y-%m-%d"), p) for p in partitions if _is_valid_date(p)],
        key=lambda x: x[0],
        reverse=True,
    )
    return [p for _, p in dated[:limit]]


def is_expansion_case(deps: dict, asset_info: dict) -> bool:
    """Expansion mode = asset tagged `latest_available`, ≥1 latest_available
    dep, and NO exact_match deps. Faithful port.
    """
    return (
        len(deps["latest_available"]) >= 1
        and len(deps["exact_match"]) == 0
        and "latest_available" in asset_info.get("tags", [])
    )


# --- cursor helpers ---------------------------------------------------------

def _read_cursor(context: SensorEvaluationContext) -> dict:
    if context.cursor:
        try:
            return json.loads(context.cursor)
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def _was_recently_triggered(cursor: dict, asset_name: str, partition: str) -> bool:
    triggered = cursor.get("recently_triggered", {})
    key = f"{asset_name}::{partition}"
    ts = triggered.get(key)
    if ts is None:
        return False
    return (datetime.now(timezone.utc).timestamp() - ts) < TRIGGERED_EXPIRY_SECONDS


def _build_new_cursor(
    triggered: Dict[str, List[str]], previous: dict
) -> str:
    now_ts = datetime.now(timezone.utc).timestamp()
    prev = previous.get("recently_triggered", {})
    # Expire old entries
    merged = {k: ts for k, ts in prev.items() if now_ts - ts < TRIGGERED_EXPIRY_SECONDS}
    for asset_name, partitions in triggered.items():
        for partition in partitions:
            merged[f"{asset_name}::{partition}"] = now_ts
    return json.dumps({"recently_triggered": merged})


# --- the sensor itself ------------------------------------------------------

@sensor(
    name="cross_partition_sensor",
    asset_selection=AssetSelection.all(),
    minimum_interval_seconds=60,
    description=(
        "Tag-driven cross-partition expansion sensor. Reads the dbt "
        "manifest for models tagged `latest_available`, classifies their "
        "deps, and runs expansion mode when a daily downstream depends "
        "only on a less-frequent `latest_available_source`. Fires one "
        "RunRequest per day (with daily partition_key) targeting the "
        "`latest_available` asset directly (via asset_selection), so the "
        "daily pipeline never stalls waiting for a monthly upstream. "
        "Ported from imp_finance_mart/bhi_imp/sensor/cross_partition_sensor.py."
    ),
)
def cross_partition_sensor(context: SensorEvaluationContext) -> SensorResult:
    try:
        manifest = get_dbt_manifest()
    except FileNotFoundError:
        return SensorResult(
            skip_reason=SkipReason(
                f"dbt manifest not found at {DBT_MANIFEST_PATH}. "
                "Ensure the code-location container has run `dbt parse`."
            )
        )

    prev_cursor = _read_cursor(context)
    cursor_triggered: Dict[str, List[str]] = {}
    run_requests: List[RunRequest] = []

    latest_available_assets = find_assets_with_tag(manifest, "latest_available")
    latest_available_source_assets = find_assets_with_tag(manifest, "latest_available_source")

    # For Pass 0 we also need the set of seed-derived staging models —
    # partitioned dbt models whose ONLY deps are seeds (or other ancestors
    # that resolve to unpartitioned). These can't be driven by AC because
    # their upstream has no time-partition for `any_deps_updated` to fire
    # on. We detect them structurally rather than via tag — a model whose
    # direct deps are all seeds (in manifest['nodes']) is eligible.
    seed_derived_assets: List[dict] = []
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        if "latest_available" in node.get("tags", []):
            continue
        if "latest_available_source" in node.get("tags", []):
            continue
        dep_ids = node.get("depends_on", {}).get("nodes", [])
        if not dep_ids:
            continue
        # All deps must be seeds (resource_type='seed') to qualify.
        all_seeds = True
        for dep_id in dep_ids:
            dep_node = manifest.get("nodes", {}).get(dep_id)
            if dep_node is None or dep_node.get("resource_type") != "seed":
                all_seeds = False
                break
        if all_seeds:
            seed_derived_assets.append({
                "node_id": node_id,
                "name": node.get("name"),
                "tags": node.get("tags", []),
                "depends_on": dep_ids,
            })

    if (
        not latest_available_assets
        and not latest_available_source_assets
        and not seed_derived_assets
    ):
        return SensorResult(
            skip_reason=SkipReason(
                "No dbt models tagged `latest_available` / `latest_available_source`, "
                "and no seed-derived partitioned models to drive."
            ),
            cursor=_build_new_cursor({}, prev_cursor),
        )

    # ---- Pass 0: seed-derived daily-partitioned staging models ----
    # stg_erp_CUST_AZ12, stg_erp_PX_CAT_G1V2 — partitioned daily but their
    # only upstream is an unpartitioned seed. AC can't drive them because
    # `any_deps_updated()` never fires (the seed materialized before the
    # sensor's cursor). We fire them for every daily partition where at
    # least one "sibling" daily raw/* Python asset has materialized, and
    # this model hasn't. This keeps them in lockstep with the daily data
    # without any partition-grid fan-out.
    #
    # The sibling set = the daily Python landing assets owned by the ELT
    # layer. Hardcoded here because they're defined in Python (not dbt)
    # so they don't appear in the manifest's dep graph for the seed-only
    # staging models.
    _SIBLING_DAILY_RAW_KEYS = [
        AssetKey(["raw", "raw_cust_info"]),
        AssetKey(["raw", "raw_sales_details"]),
        AssetKey(["raw", "raw_loc_a101"]),
    ]
    if seed_derived_assets:
        sibling_partitions: set = set()
        for sib_key in _SIBLING_DAILY_RAW_KEYS:
            try:
                sibling_partitions |= set(
                    context.instance.get_materialized_partitions(sib_key) or []
                )
            except Exception:
                pass
        # Rate-limit to the most recent EXPANSION_PARTITION_LIMIT days.
        sibling_daily_parts = get_latest_partitions_only(
            sorted(sibling_partitions), limit=EXPANSION_PARTITION_LIMIT
        )
        context.log.info(
            f"Pass-0 sibling daily partitions: {sibling_daily_parts}"
        )

        for asset_info in seed_derived_assets:
            asset_name = asset_info["name"]
            node_id = asset_info["node_id"]
            asset_key = _asset_key_from_dbt_node(manifest, node_id)
            try:
                already_materialized = set(
                    context.instance.get_materialized_partitions(asset_key) or []
                )
            except Exception:
                already_materialized = set()

            for partition in sibling_daily_parts:
                if partition in already_materialized:
                    continue
                if _was_recently_triggered(prev_cursor, asset_name, partition):
                    continue
                run_requests.append(
                    RunRequest(
                        run_key=f"cxp-seed-{asset_name}-{partition}",
                        asset_selection=[asset_key],
                        partition_key=partition,
                        tags={
                            "triggered_by": "cross_partition_sensor",
                            "reason": "seed_derived_sibling_expansion",
                            "expansion_mode": "seed_to_daily",
                            "seed_derived_asset": asset_name,
                        },
                    )
                )
                cursor_triggered.setdefault(asset_name, []).append(partition)

    # ---- Pass 1: latest_available_source assets ----
    # These are the slow-cadence staging models whose upstream is a monthly
    # (or irregular) source but whose partition def is daily. Native eager()
    # does not synthesize a daily child from a sparse parent: no same-day
    # monthly partition exists for most days, so any_deps_missing() stays
    # TRUE and the daily stg is never fired. We supply the missing semantic
    # here: fire the stg once per materialized monthly source partition,
    # keyed to a daily partition, rate-limited to EXPANSION_PARTITION_LIMIT
    # most recent days.
    for asset_info in latest_available_source_assets:
        asset_name = asset_info["name"]
        node_id = asset_info["node_id"]

        # For `latest_available_source`, the model's DIRECT deps are the
        # monthly/irregular upstream(s). Collect their materialized partitions.
        node = manifest["nodes"][node_id]
        source_partitions: List[str] = []
        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep_node = _get_node_or_source(manifest, dep_id)
            if dep_node is None:
                continue
            dep_key = _asset_key_from_dbt_node(manifest, dep_id)
            try:
                dep_parts = list(
                    context.instance.get_materialized_partitions(dep_key) or []
                )
            except Exception as e:
                context.log.warning(f"Could not load partitions for {dep_key}: {e}")
                dep_parts = []
            source_partitions.extend(dep_parts)

        # The source partitions for a monthly source are month-start dates
        # like "2026-04-01". In this demo we fire the stg EXACTLY on those
        # month-start dates — that's the one day where all daily siblings
        # (raw_cust_info etc.) also have data that can pair with the
        # monthly product snapshot inside dim_products_history.
        #
        # Rationale: the point of `latest_available_source` is not "project
        # this month's data onto every day of the month." It's "this staging
        # model's row-set only changes when the monthly source updates." So
        # fire it once per monthly source partition — on the monthly date
        # itself. Downstream marts that need a daily grid (like
        # dim_products_history) are separately driven in Pass 2 via
        # latest-available-on-or-before on the exact_match deps.
        source_partitions = sorted(set(source_partitions))
        if not source_partitions:
            context.log.info(
                f"Pass-1 source {asset_name}: no upstream partitions yet; waiting"
            )
            continue

        # Fire one stg partition per monthly source partition, most-recent
        # first, rate-limited to EXPANSION_PARTITION_LIMIT entries.
        target_limited = get_latest_partitions_only(
            source_partitions, limit=EXPANSION_PARTITION_LIMIT
        )
        context.log.info(
            f"Pass-1 source {asset_name}: upstream partitions {source_partitions}, "
            f"firing {len(target_limited)} stg partitions (one per monthly source partition)"
        )

        asset_key = _asset_key_from_dbt_node(manifest, node_id)
        try:
            already_materialized = set(
                context.instance.get_materialized_partitions(asset_key) or []
            )
        except Exception:
            already_materialized = set()

        for partition in target_limited:
            # Must have SOME monthly/irregular parent partition <= this day.
            if not find_latest_available_partition(source_partitions, partition):
                continue
            if _was_recently_triggered(prev_cursor, asset_name, partition):
                continue
            if partition in already_materialized:
                continue
            run_requests.append(
                RunRequest(
                    run_key=f"cxp-src-{asset_name}-{partition}",
                    asset_selection=[asset_key],
                    partition_key=partition,
                    tags={
                        "triggered_by": "cross_partition_sensor",
                        "reason": "latest_available_source_expansion",
                        "expansion_mode": "monthly_to_daily",
                        "latest_available_source_asset": asset_name,
                    },
                )
            )
            cursor_triggered.setdefault(asset_name, []).append(partition)

    # ---- Pass 2: latest_available assets (downstream marts) ----
    # Daily downstreams materialize via dbt_elt_landing_job (which targets
    # the landing dbt models). Expansion fires one partition_key per day.
    # Because the partitioned landing job has a fixed asset selection, we
    # emit run requests that target it.
    for asset_info in latest_available_assets:
        asset_name = asset_info["name"]
        node_id = asset_info["node_id"]
        deps = get_asset_dependencies_with_keys(manifest, node_id)

        context.log.info(
            f"Considering `latest_available` asset {asset_name}: "
            f"exact={[n for n, _ in deps['exact_match']]}, "
            f"latest={[n for n, _ in deps['latest_available']]}"
        )

        # Require at least one latest_available dep — otherwise this asset
        # doesn't need expansion-mode firing; AC handles it.
        if not deps["latest_available"]:
            context.log.info(f"  no latest_available deps; leaving to AutomationCondition")
            continue

        # Collect all source partitions across every latest_available dep.
        all_source_partitions: List[str] = []
        for source_name, source_key in deps["latest_available"]:
            try:
                src_parts = list(
                    context.instance.get_materialized_partitions(source_key) or []
                )
            except Exception as e:
                context.log.warning(f"Could not load partitions for {source_key}: {e}")
                src_parts = []
            context.log.info(f"  source {source_name} partitions: {src_parts}")
            all_source_partitions.extend(src_parts)

        all_source_partitions = sorted(set(all_source_partitions))
        if not all_source_partitions:
            context.log.info(f"  no source partitions; waiting")
            continue

        # Pre-fetch partitions for exact-match deps once (avoid re-fetching per day).
        exact_dep_partitions: Dict[str, set] = {}
        for dep_name, dep_key in deps["exact_match"]:
            try:
                exact_dep_partitions[dep_name] = set(
                    context.instance.get_materialized_partitions(dep_key) or []
                )
            except Exception:
                exact_dep_partitions[dep_name] = set()
            context.log.info(
                f"  exact dep {dep_name} materialized partitions: "
                f"{sorted(exact_dep_partitions[dep_name])[:10]}"
                f"{'...' if len(exact_dep_partitions[dep_name]) > 10 else ''}"
            )

        # Build the candidate date range.
        #
        # When the asset has EXACT_MATCH deps (e.g. dim_products_history
        # depends on daily stg_erp_PX_CAT_G1V2 AND monthly stg_crm_prd_info),
        # we can only fire partitions where ALL exact deps are materialized.
        # Take the intersection of exact-dep partitions and pick the N
        # most-recent — otherwise `generate_expansion_date_range` would
        # give us "the last 7 calendar days" which may not have any data
        # upstream yet.
        #
        # When there are NO exact_match deps (pure-latest_available case,
        # like imp_finance_mart's dim_mec_calendar), fall back to the
        # reference project's original "earliest-source-partition ..
        # yesterday" calendar range.
        if deps["exact_match"]:
            common_exact_partitions: Optional[set] = None
            for dep_name in exact_dep_partitions:
                dep_parts = exact_dep_partitions[dep_name]
                common_exact_partitions = (
                    dep_parts
                    if common_exact_partitions is None
                    else common_exact_partitions & dep_parts
                )
            base_range = sorted(common_exact_partitions or [])
            context.log.info(
                f"  candidate range (intersection of exact deps): "
                f"{len(base_range)} days"
            )
        else:
            base_range = generate_expansion_date_range(all_source_partitions)
            context.log.info(
                f"  candidate range (calendar, earliest-source → yesterday): "
                f"{len(base_range)} days"
            )

        target_limited = get_latest_partitions_only(
            base_range, limit=EXPANSION_PARTITION_LIMIT
        )
        context.log.info(
            f"  expansion range: {len(target_limited)} days "
            f"({target_limited[0] if target_limited else 'none'} … "
            f"{target_limited[-1] if target_limited else 'none'})"
        )

        # Pre-fetch asset's own materialized partitions.
        asset_key = _asset_key_from_dbt_node(manifest, node_id)
        try:
            already_materialized = set(
                context.instance.get_materialized_partitions(asset_key) or []
            )
        except Exception:
            already_materialized = set()

        for partition in target_limited:
            # Every latest_available dep must have SOME partition on-or-before
            # this target day (the "latest available" rule).
            latest_ok = True
            for _source_name, source_key in deps["latest_available"]:
                try:
                    sp = list(
                        context.instance.get_materialized_partitions(source_key) or []
                    )
                except Exception:
                    sp = []
                if not find_latest_available_partition(sp, partition):
                    latest_ok = False
                    break
            if not latest_ok:
                continue

            # Every exact_match dep must have EXACTLY this partition
            # materialized. If not, downstream would blow up with missing
            # data, so skip (AC will fire it later when the exact dep is
            # caught up).
            exact_ok = True
            for dep_name, _dep_key in deps["exact_match"]:
                if partition not in exact_dep_partitions.get(dep_name, set()):
                    context.log.info(
                        f"  skipping {partition}: exact dep {dep_name} "
                        f"has no materialization for {partition} yet"
                    )
                    exact_ok = False
                    break
            if not exact_ok:
                continue

            # Skip if we already fired this (asset, partition) in the last hour.
            if _was_recently_triggered(prev_cursor, asset_name, partition):
                continue
            # Skip if asset is already materialized for the partition — new
            # days are the target; re-runs only happen on an explicit source
            # update (detected via recent-update machinery in the reference;
            # intentionally dropped here for simplicity).
            if partition in already_materialized:
                continue

            run_requests.append(
                RunRequest(
                    run_key=f"cxp-{asset_name}-{partition}",
                    asset_selection=[asset_key],
                    partition_key=partition,
                    tags={
                        "triggered_by": "cross_partition_sensor",
                        "reason": "new_expansion_partition",
                        "expansion_mode": "irregular_to_daily",
                        "latest_available_asset": asset_name,
                    },
                )
            )
            cursor_triggered.setdefault(asset_name, []).append(partition)

    new_cursor = _build_new_cursor(cursor_triggered, prev_cursor)
    if not run_requests:
        return SensorResult(
            skip_reason=SkipReason(
                "No new expansion partitions to fire (sources unchanged "
                "or all target days already materialized)."
            ),
            cursor=new_cursor,
        )
    return SensorResult(run_requests=run_requests, cursor=new_cursor)
