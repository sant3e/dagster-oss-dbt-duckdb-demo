"""dbt assets for the elt layers: landing, staging, mart, reporting.

Every model in these layers is **daily-partitioned on snapshot_date**.
Dagster extracts the current partition date from `context.partition_time_window.start`
and passes it to dbt via `--vars '{"snapshot_dt": "YYYY-MM-DD"}'`. Every
partitioned dbt model filters its own upstream refs by that var, so a
given run produces exactly one row-set per model for that partition.

Seeds are NOT partitioned (they're static reference data), so they live
in a SEPARATE @dbt_assets block without a partitions_def.

Every partitioned dbt model downstream of the daily-cadence landings
carries `AutomationCondition.eager()` (applied via the translator's
`get_automation_condition`). As soon as an upstream partition
materializes, the corresponding downstream partition auto-fires —
evaluated by Dagster's built-in `default_automation_condition_sensor`.

Models tagged `latest_available_source` (slow-cadence projection onto
the daily grid — e.g. `raw_crm_prd_info` reading the monthly product
source) and `latest_available` (consumers of the projection — e.g.
`stg_crm_prd_info`) are deliberately EXCLUDED from AutomationCondition.
They're driven by `cross_partition_sensor` (ported from imp_finance_mart),
which fires them daily in expansion mode, reusing the latest-available
monthly snapshot until a new monthly snapshot arrives.

ML assets in ml_pipelines also deliberately lack AutomationCondition —
the `elt_to_ml_bridge_sensor` is the explicit on/off gate for the ml
chain. Turn that sensor on to allow ml to train per partition; turn
it off and the ml chain stops firing while elt keeps running.
"""

import json
import os
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    BackfillPolicy,
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
    """Extract the layer folder (landing/staging/mart/reporting/ml_features)
    from a dbt model's original_file_path."""
    path = (original_file_path or "").replace("\\", "/")
    for layer in ("landing", "staging", "mart", "reporting", "ml_features"):
        if f"models/{layer}/" in path:
            return layer
    return None


# Models that are part of the seed chain (unpartitioned, no automation).
_SEED_CHAIN_MODEL_NAMES = {
    "raw_erp_CUST_AZ12",
    "raw_erp_PX_CAT_G1V2",
    "stg_erp_CUST_AZ12",
    "stg_erp_PX_CAT_G1V2",
}


class EltDbtTranslator(DagsterDbtTranslator):
    """Prefix asset keys with their dbt layer folder, assign sensible
    group names to seeds + sources, and attach AutomationCondition.eager()
    to every partitioned model so downstream hops auto-cascade.
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
        """AutomationCondition.eager() attached to every partitioned model
        EXCEPT the ones whose parent is `latest_available_source` and those
        that have `latest_available` themselves. Those are driven by the
        `cross_partition_sensor` so they can fire daily even when the
        monthly upstream hasn't changed.

        NOT attached to:
        - seeds + seed-chain wrappers (unpartitioned static reference data).
        - models tagged `latest_available_source` — they're "projection" models
          that adapt a slow-cadence source onto the daily snapshot grid.
          Their behavior is tied to the sensor, not eager().
        - models tagged `latest_available` — explicitly sensor-gated by the
          tag-driven expansion sensor.
        """
        tags = dbt_resource_props.get("tags", []) or []
        if dbt_resource_props.get("resource_type") == "seed":
            return None
        if dbt_resource_props.get("name") in _SEED_CHAIN_MODEL_NAMES:
            return None
        if "latest_available_source" in tags:
            return None
        if "latest_available" in tags:
            return None
        return AutomationCondition.eager()


translator = EltDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)


# ---------------------------------------------------------------------------
# Partitioned models: everything except ml_features AND except the seed
# chain (seeds + their raw_erp_* landing wrappers + their stg_erp_*
# staging wrappers). Each partitioned run passes --vars snapshot_dt to
# dbt, which every model filters on.
# ---------------------------------------------------------------------------
_SEED_CHAIN_EXCLUDES = [
    "resource_type:seed",
    "fqn:dbt_oss_template.landing.raw_erp_CUST_AZ12",
    "fqn:dbt_oss_template.landing.raw_erp_PX_CAT_G1V2",
    "fqn:dbt_oss_template.staging.stg_erp_CUST_AZ12",
    "fqn:dbt_oss_template.staging.stg_erp_PX_CAT_G1V2",
]

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="fqn:dbt_oss_template.landing.* fqn:dbt_oss_template.staging.* fqn:dbt_oss_template.mart.* fqn:dbt_oss_template.reporting.*",
    exclude=" ".join(_SEED_CHAIN_EXCLUDES),
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
# Seed chain: seeds + the raw_erp_* landing wrappers + the stg_erp_*
# staging wrappers. ALL unpartitioned (static reference data). Separate
# @dbt_assets block so it can participate in the asset graph without
# being bound to a daily partition.
# ---------------------------------------------------------------------------
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=(
        "resource_type:seed "
        "fqn:dbt_oss_template.landing.raw_erp_CUST_AZ12 "
        "fqn:dbt_oss_template.landing.raw_erp_PX_CAT_G1V2 "
        "fqn:dbt_oss_template.staging.stg_erp_CUST_AZ12 "
        "fqn:dbt_oss_template.staging.stg_erp_PX_CAT_G1V2"
    ),
    dagster_dbt_translator=translator,
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
)
def elt_dbt_seed_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
