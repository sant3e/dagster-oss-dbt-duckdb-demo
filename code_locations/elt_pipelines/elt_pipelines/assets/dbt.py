"""dbt assets for the elt layers: landing, staging, mart, reporting.

Every non-seed dbt model is **daily-partitioned on snapshot_date**.
Dagster extracts the current partition date from `context.partition_time_window.start`
and passes it to dbt via `--vars '{"snapshot_dt": "YYYY-MM-DD"}'`. Every
partitioned dbt model filters its own upstream refs by that var, so a
given run produces exactly one row-set per model for that partition.

Seeds (CUST_AZ12, PX_CAT_G1V2) themselves are NOT partitioned — they're
git-tracked static reference data loaded via `dbt seed`. Their landing
wrappers (raw_erp_CUST_AZ12, raw_erp_PX_CAT_G1V2) stamp
`snapshot_date = '{{ var("snapshot_dt") }}'::DATE` onto every row, which
is what carries the seed data onto the daily partition grid for the rest
of the pipeline. From staging onward the seed chain is indistinguishable
from the other partitioned data — same JOIN predicates, same
AutomationCondition behaviour.

AutomationCondition attached to every partitioned non-seed model EXCEPT
those tagged `latest_available_source` / `latest_available` (those are
handled by `cross_partition_sensor` — ported from imp_finance_mart —
which fires them in expansion mode so daily runs can reuse the
latest-available monthly snapshot until a newer one arrives).

ML assets in ml_pipelines lack AutomationCondition — the
`elt_to_ml_bridge_sensor` is the explicit on/off gate for the ml chain.
Turn that sensor on to allow ml to train per partition; turn it off and
the ml chain stops firing while elt keeps running.
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


class EltDbtTranslator(DagsterDbtTranslator):
    """Prefix asset keys with their dbt layer folder, assign sensible
    group names to seeds + sources, and attach AutomationCondition.eager()
    to every partitioned model except the sensor-gated tagged ones.
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
        """AutomationCondition.eager() for every partitioned non-seed model
        EXCEPT those tagged `latest_available_source` / `latest_available`,
        which are handled by `cross_partition_sensor` so they can fire
        daily even when their slow-cadence upstream hasn't changed.

        Seeds themselves return None — they're not partitioned and
        don't participate in the daily cascade. Running `dbt seed` or
        invoking `dbt_seed_job` materializes them once; their downstream
        landing wrapper (raw_erp_*) carries eager() like every other
        partitioned asset, so the first `--vars snapshot_dt` run after
        the seed materialization will re-read the seed and stamp it
        onto that day's partition.
        """
        tags = dbt_resource_props.get("tags", []) or []
        if dbt_resource_props.get("resource_type") == "seed":
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
# Partitioned models: everything except seeds. The raw_erp_* and stg_erp_*
# seed wrappers live here now too — they're daily-partitioned via the
# snapshot_date stamp applied in the landing model.
# ---------------------------------------------------------------------------
@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="fqn:dbt_oss_template.landing.* fqn:dbt_oss_template.staging.* fqn:dbt_oss_template.mart.* fqn:dbt_oss_template.reporting.*",
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
# (Step 1 of the demo). Downstream raw_erp_* landing wrappers pick up the
# seed data into the daily partition grid via AutomationCondition.
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
