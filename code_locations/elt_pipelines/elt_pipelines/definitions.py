"""Top-level Definitions for the elt_pipelines code location.

This is the module referenced by `[tool.dagster] module_name` in pyproject.toml
and loaded by the gRPC server started by docker-compose.
"""

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
)

from elt_pipelines.assets import (
    dbt as dbt_assets_module,
    landing_customers,
    landing_locations,
    landing_products,
    landing_sales,
)
from elt_pipelines.jobs import all_jobs
from elt_pipelines.resources import build_resources
from elt_pipelines.sensors import cross_partition_sensor, landing_file_sensor

# Custom automation-condition sensor that REPLACES the built-in
# `default_automation_condition_sensor`. Critical difference: `run_tags`
# is forwarded onto every RunRequest the sensor emits, so the implicit
# `__ASSET_JOB` runs inherit the `duckdb_writer` concurrency key. Without
# this, concurrent AC-triggered runs race on the DuckDB file lock and
# can corrupt the SQLite metadata store.
elt_automation_condition_sensor = AutomationConditionSensorDefinition(
    name="elt_automation_condition_sensor",
    target=AssetSelection.all(),
    run_tags={
        "dagster/concurrency_key": "duckdb_writer",
        "triggered_by": "elt_automation_condition_sensor",
    },
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
)

defs = Definitions(
    assets=[
        landing_sales.raw_sales_details,
        landing_customers.raw_cust_info,
        landing_locations.raw_loc_a101,
        landing_products.raw_prd_info_monthly,
        dbt_assets_module.elt_dbt_assets,
        dbt_assets_module.elt_dbt_seed_assets,
    ],
    jobs=all_jobs,
    sensors=[
        landing_file_sensor,
        cross_partition_sensor,
        elt_automation_condition_sensor,
    ],
    resources=build_resources(),
)
