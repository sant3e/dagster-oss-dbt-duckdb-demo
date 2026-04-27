"""Resource factory for elt_pipelines.

Mirrors the pattern used in the internal imp_finance_mart project but
stripped down: a single Settings class pulls values from environment
variables, and build_resources() returns the dict Dagster wires into
Definitions.
"""

from pathlib import Path

from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-backed configuration for elt_pipelines."""

    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    duckdb_path: str = "/warehouse/oss_template.duckdb"
    data_landing_dir: str = "/data/landing"
    dbt_project_dir: str = "/opt/dbt_project"
    dbt_profiles_dir: str = "/opt/dbt_project"


def build_settings() -> Settings:
    return Settings()


def build_resources(settings: Settings | None = None) -> dict:
    """Return the Dagster resource dict used by elt_pipelines."""
    settings = settings or build_settings()

    # Ensure the warehouse directory exists so DuckDB can create the file on first write.
    Path(settings.duckdb_path).parent.mkdir(parents=True, exist_ok=True)

    return {
        "duckdb": DuckDBResource(database=settings.duckdb_path),
        "dbt": DbtCliResource(
            project_dir=settings.dbt_project_dir,
            profiles_dir=settings.dbt_profiles_dir,
        ),
    }
