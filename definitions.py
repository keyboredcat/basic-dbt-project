from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

import dagster as dg

import pandas as pd
from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    build_schedule_from_dbt_selection,
    dbt_assets,
    get_asset_key_for_model
)

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

@dg_asset(
    compute_kind="python",
    # defines the dependency on the customers model, which is represented as an asset in Dagster
    deps=[get_asset_key_for_model([dbt_models], "customers")]
)

def customer_histogram(context: dg.AssetExecutionContext):
    # read the contents of the customers table into a pandas dataframe
    conn = duckdb.connect(os.fspath(duckdb_database_path))
    customers = conn.sql("select * from customers").df()

    # create a histogram of the customers table and write it out to an HTML file
    fig = px.histogram(customers, x="FIRST_NAME")
    fig.update_layout(bargap=0.2)
    fig.update_xaxes(categoryorder="total ascending")
    save_chart_path = Path(duckdb_database_path).parent.joinpath(
        "order_count_chart.html"
    )
    fig.write_html(save_chart_path, auto_open=True)

    # tell Dagster about the location of the HTML file, so it's easy to access from the Dagster UI
    context.add_output_metadata(
        {"plot_url": dg.MetadataValue.url("file://" + os.fspath(save_chart_path))}
    )

# build a schedule for the job that materializes a selection of dbt assets
dbt_schedule = build_schedule_from_dbt_selection(
    [dbt_models],
    job_name="materialize_dbt_models",
    cron_schedule="0 0 * * *",
    dbt_select="fqn:*"
)

# Dagster object that contains the dbt assets and resource
defs = dg.Definitions(
    assets=[raw_customers, dbt_models, customer_histogram], 
    resources={"dbt": dbt_resource},
    schedules=[dbt_schedule]
)