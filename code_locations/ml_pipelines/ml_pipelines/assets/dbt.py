"""dbt assets for the ml_features layer.

Owned by ml_team (see +group in dbt_project.yml). This code location
selects ONLY the ml_features path — elt_pipelines handles everything else.
The asset graph still shows the full dependency chain across both code
locations because Dagster stitches them together by AssetKey.

The translator prefixes asset keys with the dbt layer folder so the
UI groups look natural: ml_features/customer_rfm, etc.
"""

import os
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, Backoff, Jitter, RetryPolicy
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/dbt_project"))
DBT_TARGET_PATH = Path(os.environ.get("DBT_TARGET_PATH", "/tmp/dbt_target"))

# Retry policy for transient lock errors on macOS bind-mounted SQLite /
# DuckDB (same trade-off as in elt_pipelines/constants.py).
_TRANSIENT_LOCK_RETRY = RetryPolicy(
    max_retries=2,
    delay=2,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

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
        # Same convention as elt_pipelines: seeds and sources get their
        # own dedicated groups so they don't sit under `default` in the UI.
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
    select="path:models/ml_features",
    dagster_dbt_translator=translator,
    op_tags={"dagster/concurrency_key": "duckdb_writer"},
    retry_policy=_TRANSIENT_LOCK_RETRY,
)
def ml_features_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
