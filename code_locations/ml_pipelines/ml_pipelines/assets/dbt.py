"""dbt assets for the ml_features layer.

Owned by ml_team (see +group in dbt_project.yml). This code location
selects ONLY the ml_features path — elt_pipelines handles everything else.
The asset graph still shows the full dependency chain across both code
locations because Dagster stitches them together by AssetKey.

Daily-partitioned just like the elt layers. Passes --vars snapshot_dt
into dbt so customer_rfm builds one row-set per partition.
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

from ml_pipelines.constants import (
    DUCKDB_WRITER_TAGS,
    TRANSIENT_LOCK_RETRY_POLICY,
)
from ml_pipelines.partitions import daily_partitions

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/dbt_project"))
DBT_TARGET_PATH = Path(os.environ.get("DBT_TARGET_PATH", "/tmp/dbt_target"))

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    target_path=DBT_TARGET_PATH,
)
dbt_project.prepare_if_dev()


def _layer_from_path(original_file_path: str) -> str | None:
    path = (original_file_path or "").replace("\\", "/")
    for layer in ("landing", "staging", "mart", "reporting", "ml_features"):
        if f"models/{layer}/" in path:
            return layer
    return None


class MlDbtTranslator(DagsterDbtTranslator):
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


translator = MlDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="fqn:dbt_oss_template.ml_features.*",
    dagster_dbt_translator=translator,
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(),
    op_tags=DUCKDB_WRITER_TAGS,
    retry_policy=TRANSIENT_LOCK_RETRY_POLICY,
)
def ml_features_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    time_window = context.partition_time_window
    snapshot_dt = time_window.start.strftime("%Y-%m-%d")
    vars_json = json.dumps({"snapshot_dt": snapshot_dt})
    context.log.info(f"Running dbt build for partition {snapshot_dt}")
    yield from dbt.cli(["build", "--vars", vars_json], context=context).stream()
