"""Resource factory for ml_pipelines (mirrors elt_pipelines, simpler config)."""

from pathlib import Path

from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    duckdb_path: str = "/warehouse/oss_template.duckdb"
    dbt_project_dir: str = "/opt/dbt_project"
    dbt_profiles_dir: str = "/opt/dbt_project"
    artifacts_dir: str = "/warehouse/artifacts"


def build_settings() -> Settings:
    return Settings()


def build_resources(settings: Settings | None = None) -> dict:
    settings = settings or build_settings()
    Path(settings.artifacts_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    return {
        "duckdb": DuckDBResource(database=settings.duckdb_path),
        "dbt": DbtCliResource(
            project_dir=settings.dbt_project_dir,
            profiles_dir=settings.dbt_profiles_dir,
        ),
    }
