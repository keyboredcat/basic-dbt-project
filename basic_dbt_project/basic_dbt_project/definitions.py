from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import basic_dbt_project_dbt_assets
from .project import basic_dbt_project_project
from .schedules import schedules

defs = Definitions(
    assets=[basic_dbt_project_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=basic_dbt_project_project),
    },
)