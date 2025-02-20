from pathlib import Path

from dagster_dbt import DbtProject

basic_dbt_project_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
basic_dbt_project_project.prepare_if_dev()