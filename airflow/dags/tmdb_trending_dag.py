from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup

# from /plugins
from ingestion import fetch_trending_and_save
from metrics import log_rowcount_metric


DEFAULT_CONN_ID = "snowflake_conn"
RAW_FILE_PATH_TEMPLATE = "/tmp/tmdb_trending_{{ ds }}.json"


default_args = {
    "owner": "soomin",
    "retries": 3,
}


with DAG(
    dag_id="tmdb_trending_pipeline",
    description="Ingest TMDB trending data into Snowflake (RAW -> STG -> DW).",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    default_args=default_args,
    catchup=False,  # TODO: for dev only
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["tmdb", "netflix", "snowflake"],
    ) as dag:

    # ---------------------------
    # RAW layer TaskGroup
    # ---------------------------
    with TaskGroup(group_id="raw_layer") as raw_layer:

        fetch_trending_to_raw = PythonOperator(
            task_id="fetch_trending_to_raw",
            python_callable=fetch_trending_and_save,
            op_kwargs={
                "media_type": "all",
                "time_window": "day",
                "output_path": RAW_FILE_PATH_TEMPLATE, # Jinja template for execution date
            },
        )

        put_raw_file_to_stage = SnowflakeOperator(
            task_id="put_raw_file_to_stage",
            snowflake_conn_id=DEFAULT_CONN_ID,
            sql="raw/02_put_tmdb_trending_file.sql",
        )

        copy_stage_to_raw = SnowflakeOperator(
            task_id="copy_stage_to_raw",
            snowflake_conn_id=DEFAULT_CONN_ID,
            sql="raw/03_copy_tmdb_trending_into_raw.sql",
        )

        log_raw_metrics = PythonOperator(
            task_id="log_raw_metrics",
            python_callable=log_rowcount_metric,
            op_kwargs={
                "step_name": "RAW_LOAD",
                "table_name": "DEV.RAW.TMDB_TRENDING_RAW",
                "date_column": "LOAD_DATE",
            },
        )

        fetch_trending_to_raw >> put_raw_file_to_stage >> copy_stage_to_raw >> log_raw_metrics

    # ---------------------------
    # STG layer TaskGroup
    # ---------------------------
    with TaskGroup(group_id="stg_layer") as stg_layer:

        build_stg_trending = SnowflakeOperator(
            task_id="build_stg_trending",
            snowflake_conn_id=DEFAULT_CONN_ID,
            sql="stg/02_build_stg_trending.sql",
        )

        dq_checks = SnowflakeOperator(
            task_id="dq_checks",
            snowflake_conn_id=DEFAULT_CONN_ID,
            sql="stg/02_dq_checks_trending.sql",
        )

        log_stg_metrics = PythonOperator(
            task_id="log_stg_metrics",
            python_callable=log_rowcount_metric,
            op_kwargs={
                "step_name": "STG_BUILD",
                "table_name": "DEV.STG.TMDB_TRENDING_STG",
                "date_column": "TRENDING_DATE",
            },
        )

        build_stg_trending >> dq_checks >> log_stg_metrics

    # ---------------------------
    # DW layer TaskGroup
    # ---------------------------
    with TaskGroup(group_id="dw_layer") as dw_layer:

        merge_dw = SnowflakeOperator(
            task_id="merge_dw",
            snowflake_conn_id=DEFAULT_CONN_ID,
            sql="dw/03_merge_dw_trending.sql",
        )

        log_dw_fact_metrics = PythonOperator(
            task_id="log_dw_fact_metrics",
            python_callable=log_rowcount_metric,
            op_kwargs={
                "step_name": "DW_MERGE_FACT",
                "table_name": "DEV.DW.FACT_TRENDING",
                "date_column": "TRENDING_DATE",
            },
        )

        merge_dw >> log_dw_fact_metrics


    raw_layer >> stg_layer >> dw_layer