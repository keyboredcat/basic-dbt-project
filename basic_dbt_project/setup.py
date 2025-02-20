from setuptools import find_packages, setup

setup(
    name="basic_dbt_project",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "basic_dbt_project": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-duckdb<1.10",
        "dbt-duckdb<1.10",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)