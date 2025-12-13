from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


DEFAULT_CONN_ID = "snowflake_conn"


with DAG(
    dag_id="tmdb_trending_bootstrap",
    description=(
        "Bootstrap DAG to initialize Snowflake objects for the TMDB trending pipeline "
        "(database, schemas, stages, file formats, DW tables)."
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, # manual trigger only
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["bootstrap", "snowflake", "tmdb"]
) as bootstrap_dag:
    
    create_raw_layer = SnowflakeOperator(
        task_id="create_raw_layer",
        snowflake_conn_id=DEFAULT_CONN_ID,
        sql="raw/01_create_raw_layer.sql",
    )

    create_dw_tables = SnowflakeOperator(
        task_id="create_dw_tables",
        snowflake_conn_id=DEFAULT_CONN_ID,
        sql="dw/01_create_dw_tables.sql",
    )

    create_pipeline_metrics = SnowflakeOperator(
        task_id="create_pipeline_metrics",
        snowflake_conn_id=DEFAULT_CONN_ID,
        sql="dw/02_create_pipeline_metrics.sql",
    )

    create_raw_layer >> create_dw_tables >> create_pipeline_metrics