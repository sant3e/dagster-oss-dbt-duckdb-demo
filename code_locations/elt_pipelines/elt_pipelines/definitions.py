"""Top-level Definitions for the elt_pipelines code location.

This is the module referenced by `[tool.dagster] module_name` in pyproject.toml
and loaded by the gRPC server started by docker-compose.
"""

from dagster import Definitions

from elt_pipelines.assets import (
    dbt as dbt_assets_module,
    landing_customers,
    landing_locations,
    landing_products,
    landing_sales,
)
from elt_pipelines.freshness_checks import all_freshness_checks
from elt_pipelines.jobs import all_jobs
from elt_pipelines.resources import build_resources
from elt_pipelines.sensors import daily_monthly_bridge_sensor, landing_file_sensor

defs = Definitions(
    assets=[
        landing_sales.raw_sales_details,
        landing_customers.raw_cust_info,
        landing_locations.raw_loc_a101,
        landing_products.raw_prd_info_monthly,
        dbt_assets_module.elt_dbt_assets,
        dbt_assets_module.elt_dbt_seed_assets,
    ],
    asset_checks=all_freshness_checks,
    jobs=all_jobs,
    sensors=[
        landing_file_sensor,
        daily_monthly_bridge_sensor,
    ],
    resources=build_resources(),
)
