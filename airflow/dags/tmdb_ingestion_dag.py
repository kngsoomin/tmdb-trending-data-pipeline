from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.tmdb_orchestration import (
    run_trending_ingestion,
    run_details_ingestion,
    run_credits_ingestion,
)
from src.orchestration.dataset_def import TMDB_RAW_READY


default_args = {
    "owner": "soomin",
    "retries": 1,
}


with DAG(
    dag_id="tmdb_ingestion_dag",
    description="Ingest TMDB trending data and enrich content metadata in Snowflake.",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["tmdb", "ingestion", "snowflake", "s3"],
) as dag:


    load_trending_raw = PythonOperator(
        task_id="load_trending_raw",
        python_callable=run_trending_ingestion,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    load_details_raw = PythonOperator(
        task_id="load_details_raw",
        python_callable=run_details_ingestion,
    )

    load_credits_raw = PythonOperator(
        task_id="load_credits_raw",
        python_callable=run_credits_ingestion,
        outlets=[TMDB_RAW_READY],
    )

    load_trending_raw >> load_details_raw >> load_credits_raw
