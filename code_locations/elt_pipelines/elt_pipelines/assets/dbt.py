"""dbt assets for the elt layers: landing, staging, mart, reporting.

Every model in these layers is **daily-partitioned on snapshot_date**.
Dagster extracts the current partition date from `context.partition_time_window.start`
and passes it to dbt via `--vars '{"snapshot_dt": "YYYY-MM-DD"}'`. Every
partitioned dbt model filters its own upstream refs by that var, so a
given run produces exactly one row-set per model for that partition.

Seeds are NOT partitioned (they're static reference data), so they live
in a SEPARATE @dbt_assets block without a partitions_def.

The mart/reporting AutomationCondition.eager() hooks that the previous
(unpartitioned) version had are deliberately removed: with a partitioned
pipeline we drive downstream execution via the bridge sensor
(daily_monthly_bridge_sensor) + the cross-code-location sensor
(elt_to_ml_bridge_sensor), which is the pattern engineers expect in
production.
"""

import json
import os
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
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


class EltDbtTranslator(DagsterDbtTranslator):
    """Prefix asset keys with their dbt layer folder and assign sensible
    group names to seeds + sources. No AutomationCondition — the
    partitioned pipeline is driven by sensors.
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
