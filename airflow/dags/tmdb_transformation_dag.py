import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from src.orchestration.dataset_def import TMDB_RAW_READY


default_args = {
    "owner": "airflow",
}

common_mounts = [
    Mount(
        source="/Users/soomin/workspace/tmdb-trending-data-pipeline",
        target="/opt/project",
        type="bind",
    ),
    Mount(
        source="/Users/soomin/.dbt",
        target="/root/.dbt",
        type="bind",
        read_only=True,
    ),
    Mount(
        source="/Users/soomin/.snowflake",
        target="/root/.snowflake",
        type="bind",
        read_only=True,
    ),
]

environment = {
    "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER"),
    "SNOWFLAKE_PRIVATE_KEY_PATH": "/root/.snowflake/keys/dbt_svc_key.p8",
    "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE"),
}

with DAG(
    dag_id="tmdb_transformation_dag",
    start_date=datetime(2024, 4, 1),
    schedule=[TMDB_RAW_READY],
    catchup=False,
    default_args=default_args,
    tags=["tmdb", "dbt", "transformation"],
    description="Run dbt after TMDB raw ingestion is completed",
) as dag:


    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="tmdb-dbt:local",
        command="dbt run",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        working_dir="/opt/project/dbt/tmdb_dbt",
        mounts=common_mounts,
        environment=environment
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="tmdb-dbt:local",
        api_version="auto",
        auto_remove=True,
        command="dbt test",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        working_dir="/opt/project/dbt/tmdb_dbt",
        mounts=common_mounts,
        environment=environment
    )

    dbt_run >> dbt_test