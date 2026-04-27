"""dbt assets for the elt layers: landing, staging, mart, reporting.

The dbt project lives one level up from each code location (mounted at
/opt/dbt_project in containers). Each code location loads a different
selection via `select`; this one grabs everything except ml_features
(which ml_pipelines owns).

The mart layer is tagged with AutomationCondition.eager() so that it
auto-materializes whenever its upstream staging assets update — a
declarative alternative to the imperative cross-partition sensor in
sensors.py. Both patterns coexist so engineers can compare them.
"""

import os
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, AutomationCondition
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from elt_pipelines.constants import DUCKDB_WRITER_TAGS

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
    """Prefix asset keys with their dbt layer folder and attach
    AutomationCondition.eager() to mart-layer models.
    """

    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        name = dbt_resource_props["name"]
        # Seeds come through with resource_type == "seed"; keep them under `seeds/`.
        if dbt_resource_props.get("resource_type") == "seed":
            return AssetKey(["seeds", name])
        layer = _layer_from_path(dbt_resource_props.get("original_file_path", ""))
        if layer:
            return AssetKey([layer, name])
        return super().get_asset_key(dbt_resource_props)

    def get_automation_condition(self, dbt_resource_props):
        if _layer_from_path(dbt_resource_props.get("original_file_path", "")) == "mart":
            return AutomationCondition.eager()
        return None


translator = EltDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude="fqn:dbt_oss_template.ml_features.*",
    dagster_dbt_translator=translator,
    op_tags=DUCKDB_WRITER_TAGS,
)
def elt_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
