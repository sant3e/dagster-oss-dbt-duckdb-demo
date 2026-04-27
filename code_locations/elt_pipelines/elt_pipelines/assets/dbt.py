"""dbt assets for the elt layers: staging, mart, reporting.

Every non-seed dbt model is **daily-partitioned on snapshot_date**.
Dagster extracts the current partition date from `context.partition_time_window.start`
and passes it to dbt via `--vars '{"snapshot_dt": "YYYY-MM-DD"}'`. Every
partitioned dbt model filters its own upstream refs by that var, so a
given run produces exactly one row-set per model for that partition.

The staging models read directly from dbt sources (for daily data landed
by Python assets) or from the static seeds `{{ ref('CUST_AZ12') }}` /
`{{ ref('PX_CAT_G1V2') }}` (which are stamped onto the daily partition
grid inside the staging model itself). There is NO dbt "landing" layer —
the Python-owned landing assets (in `code_locations/elt_pipelines/elt_pipelines/assets/landing_*.py`)
are the sole landing layer. Via `meta.dagster.asset_key` in
`dbt_project/models/sources.yml`, each dbt source collapses onto the
matching Python landing AssetKey so the graph is a single connected chain.

AutomationCondition attached to every partitioned non-seed model EXCEPT
those tagged `latest_available` (those are handled by
`cross_partition_sensor` — ported from imp_finance_mart — which fires
them in expansion mode so daily runs can reuse the latest-available
monthly snapshot until a newer one arrives).

ML assets in ml_pipelines lack AutomationCondition — the
`elt_to_ml_bridge_sensor` is the explicit on/off gate for the ml chain.
Turn that sensor on to allow ml to train per partition; turn it off and
the ml chain stops firing while elt keeps running.
"""

import json
import os
from datetime import timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    BackfillPolicy,
    FreshnessPolicy,
)
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from elt_pipelines.constants import DUCKDB_WRITER_TAGS, TRANSIENT_LOCK_RETRY_POLICY
from elt_pipelines.partitions import daily_partitions

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/dbt_project"))
DBT_TARGET_PATH = Path(os.environ.get("DBT_TARGET_PATH", "/tmp/dbt_target"))

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    target_path=DBT_TARGET_PATH,
)
dbt_project.prepare_if_dev()


def _layer_from_path(original_file_path: str) -> str | None:
    """Extract the layer folder (staging/mart/reporting/ml_features)
    from a dbt model's original_file_path."""
    path = (original_file_path or "").replace("\\", "/")
    for layer in ("staging", "mart", "reporting", "ml_features"):
        if f"models/{layer}/" in path:
            return layer
    return None


# Freshness policy for every partitioned ELT dbt model (staging, mart,
# reporting). Deadline 10am daily; `lower_bound_delta` = 24h means any
# materialization in the prior day is considered fresh. The Python-owned
# landing assets carry their own FRESHNESS_LANDING_DAILY /
# FRESHNESS_LANDING_MONTHLY from constants.py (attached directly on
# @asset()), so dbt staging onward is the only scope for this one.
_FRESHNESS_ELT = FreshnessPolicy.cron(
    deadline_cron="0 10 * * *", lower_bound_delta=timedelta(hours=24)
)


class EltDbtTranslator(DagsterDbtTranslator):
    """Prefix asset keys with their dbt layer folder, assign sensible
    group names to seeds + sources, attach AutomationCondition.eager()
    to every partitioned model except the sensor-gated tagged ones,
    and attach a per-layer FreshnessPolicy to every partitioned model.
    """

    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        name = dbt_resource_props["name"]
        if dbt_resource_props.get("resource_type") == "seed":
            return AssetKey(["seeds", name])
        layer = _layer_from_path(dbt_resource_props.get("original_file_path", ""))
        if layer:
            return AssetKey([layer, name])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props.get("resource_type") == "seed":
            return "seeds"
        if dbt_resource_props.get("resource_type") == "source":
            return "sources"
        return super().get_group_name(dbt_resource_props)

    def get_automation_condition(self, dbt_resource_props):
        """AutomationCondition for every partitioned non-seed model
        EXCEPT those tagged `latest_available` — those are fired by the
        cross_partition_sensor in expansion mode.

        Composition mirrors the working reference implementation in
        `imp_v2-dagster-etl/bhi_imp/assets/dbt_imp_mart/utilities.py`:

        * `new_seed_added`       = in_latest_time_window() & missing() & ~in_progress()
        * `code_version_change`  = in_latest_time_window() & code_version_changed()
                                   & ~in_progress() & ~any_deps_missing()
                                   & ~any_deps_in_progress()
        * `eager_with_lookback`  = eager().without(in_latest_time_window())
                                   & in_latest_time_window(lookback_delta=365d)

        Seeds   → `new_seed_added | code_version_change`
        Models  → `code_version_change | eager_with_lookback`

        `eager()` internally composes
            in_latest_time_window()
            & (newly_missing() | any_deps_updated()).since_last_handled()
            & ~any_deps_missing() & ~any_deps_in_progress() & ~in_progress()

        and `since_last_handled()` resets on `initial_evaluation()` — so the
        FIRST tick of a newly-enabled AC sensor will NOT fire any
        pre-existing missing partitions. That's Dagster's deliberate
        safeguard against a massive backfill on enable. In production
        (imp_v2, fpa) the sensor is always running and the cold-start
        case never appears; in this demo you have to **prime the cascade
        once** by manually materializing the 6 staging assets for the
        landed partition (2026-04-01) before the AC sensor takes over.
        See README Step 3 for the bootstrap instructions.

        `in_latest_time_window(lookback_delta=timedelta(days=365))` widens
        the eager_with_lookback branch to the past year so historical
        demo partitions qualify. **This window is calibrated for day-1
        = 2026-04-01 and expires on 2027-04-27**; see the Troubleshooting
        section in README.md for how to extend it.

        Seeds and `latest_available` assets return None in this code
        path: seeds are unpartitioned and materialized once via
        dbt_seed_job; `latest_available` assets are driven by
        cross_partition_sensor in expansion mode.
        """
        tags = dbt_resource_props.get("tags", []) or []
        if dbt_resource_props.get("resource_type") == "seed":
            return None
        # Both `latest_available` and `latest_available_source` are driven
        # by cross_partition_sensor, NOT by AC:
        #  - `latest_available_source` = slow-cadence staging (e.g. stg_crm_prd_info
        #    whose upstream is the monthly raw_prd_info_monthly). If AC ran eager()
        #    on a daily-partitioned stg whose upstream is monthly, Dagster's default
        #    partition mapping would treat the single monthly partition as satisfying
        #    EVERY daily partition of the stg, producing a month-wide fan-out each
        #    time the monthly source lands.
        #  - `latest_available` = downstream mart that bridges slow-cadence into the
        #    daily grain (e.g. dim_products_history). Driven by cross_partition_sensor
        #    in expansion mode.
        if "latest_available" in tags or "latest_available_source" in tags:
            return None

        new_seed_added = (
            AutomationCondition.in_latest_time_window()
            & AutomationCondition.missing()
            & ~AutomationCondition.in_progress()
        ).with_label("new seed added")

        code_version_change = (
            AutomationCondition.in_latest_time_window()
            & AutomationCondition.code_version_changed()
            & ~AutomationCondition.in_progress()
            & ~AutomationCondition.any_deps_missing()
            & ~AutomationCondition.any_deps_in_progress()
        ).with_label("code version changed")

        eager_with_lookback = (
            AutomationCondition.eager().without(
                AutomationCondition.in_latest_time_window()
            )
            & AutomationCondition.in_latest_time_window(
                lookback_delta=timedelta(days=365)
            )
        ).with_label("eager (365d lookback)")

        # (Seed branch unreachable because seeds return None above, but
        # we keep it so the rule mirrors the reference and survives a
        # future refactor that lets seeds through.)
        if dbt_resource_props["resource_type"] == "seed":
            return (new_seed_added | code_version_change).with_label(
                "new seed or code version changed"
            )

        return (code_version_change | eager_with_lookback).with_label(
            "code version changed or eager"
        )

    def get_freshness_policy(self, dbt_resource_props):
        """Attach a FreshnessPolicy to every partitioned ELT dbt model.

        FreshnessPolicy is pure metadata on the asset — it is NOT an
        AssetChecksDefinition, so it does NOT inject a check step into
        any materialization job. Dagster's automation sensor evaluates
        it out-of-band and surfaces PASS / WARN / FAIL on the asset's
        Checks tab.

        Seeds: no freshness (they don't change on a schedule).
        Staging / mart / reporting: 10am deadline.
        """
        if dbt_resource_props.get("resource_type") == "seed":
            return None
        layer = _layer_from_path(dbt_resource_props.get("original_file_path", ""))
        if layer in ("staging", "mart", "reporting"):
            return _FRESHNESS_ELT
        return None


translator = EltDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)


# ---------------------------------------------------------------------------
# Partitioned models: staging, mart, reporting. The `stg_erp_CUST_AZ12` /
# `stg_erp_PX_CAT_G1V2` seed-based staging models are daily-partitioned too
# — they stamp `snapshot_date` onto each seed row per run.
# ---------------------------------------------------------------------------
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="fqn:dbt_oss_template.staging.* fqn:dbt_oss_template.mart.* fqn:dbt_oss_template.reporting.*",
    exclude="resource_type:seed",
    dagster_dbt_translator=translator,
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(),
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
)
def elt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    time_window = context.partition_time_window
    snapshot_dt = time_window.start.strftime("%Y-%m-%d")
    vars_json = json.dumps({"snapshot_dt": snapshot_dt})
    context.log.info(f"Running dbt build for partition {snapshot_dt}")
    yield from dbt.cli(["build", "--vars", vars_json], context=context).stream()


# ---------------------------------------------------------------------------
# Seeds only: the two actual dbt seeds (CUST_AZ12, PX_CAT_G1V2).
# Unpartitioned static reference data. Materialized once via dbt_seed_job
# (Step 1 of the demo). Downstream `stg_erp_CUST_AZ12` / `stg_erp_PX_CAT_G1V2`
# staging models read the seed directly and stamp `snapshot_date` onto every
# row, cascading into the daily grain via AutomationCondition.
# ---------------------------------------------------------------------------
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="resource_type:seed",
    dagster_dbt_translator=translator,
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
)
def elt_dbt_seed_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
