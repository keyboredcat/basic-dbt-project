from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

import dagster as dg

import pandas as pd
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

import dagster as dg

duckdb_database_path = "basec-dbt-project/dev.duckdb"

@dg.asset(compute_kind="python")
def raw_customers(context: dg.AssetExecutionContext) -> None:
    # pull customer data from a csv
    data.pd.read_csv("https://docs.dagster.io/assets/customers.csv")
    conn = duckdb.connect(os.fspath(duckdb_database_path))

    # create a schema named raw
    conn.execute("create schema if not exists raw")

    # create/replace a table named raw_customers
    conn.execute(
        "create or replace table raw.raw_customers as select * from data"
    )

    # log some metadata about the new table. It will show up in the UI
    context.add_output_metadata({"num_rows": data.shape[0]})

# Points to the dbt project path
dbt_project_directory = Path(__file__).absolute().parent / "basic-dbt-project"
dbt_project = DbtProject(project_dir=dbt_project_directory)

# References the dbt project object
dbt_resource = DbtCliResource(project_dir=dbt_project)

# Compiles the dbt project & allow Dagster to build an asset graph
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Dagster object that contains the dbt assets and resource
defs = dg.Definitions(
    assets=[raw_customers, dbt_models],
    resources={"dbt": dbt_resource}
)