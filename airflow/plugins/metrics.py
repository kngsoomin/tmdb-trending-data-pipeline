from typing import Any, Dict

from snowflake_utils import snowflake_cursor


DEFAULT_CONN_ID = "snowflake_conn"


def log_rowcount_metric(
        step_name: str,
        table_name: str,
        date_column: str,
        status: str | None = None,
        **context: Dict[str, Any]
    ) -> None:
    """
    Log row count for a given table and logical date into DEV.DW.PIPELINE_METRICS.
    
    Logic for `status`:
        - given_status if `status` is provided
        - 'SUCCESS' when row_count > 0 & if `status` is not provided
        - 'EMPTY'   when row_count = 0 & if `status` is not provided
    """
    logical_date = context["logical_date"]
    ds = logical_date.to_date_string()

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id

    with snowflake_cursor(conn_id=DEFAULT_CONN_ID) as (conn, cur):
        
        # [1] Compute row_count for the given date
        count_query = f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE {date_column} = TO_DATE('{ds}')
        """
        cur.execute(count_query)
        row_count = cur.fetchone()[0]

        # [2] Derive status if not explicitly provided
        derived_status = status
        if derived_status is None:
            derived_status = "SUCCESS" if row_count > 0 else "EMPTY"

        # [3] Insert metrics row
        query = f"""
            INSERT INTO DEV.DW.PIPELINE_METRICS (
                PIPELINE_DATE,
                DAG_ID,
                TASK_ID,
                STEP_NAME,
                ROW_COUNT,
                START_TS,
                END_TS,
                STATUS,
                EXTRA_INFO
            )
            SELECT
            TO_DATE('{ds}') AS PIPELINE_DATE,
            '{dag_id}'      AS DAG_ID,
            '{task_id}'     AS TASK_ID,
            '{step_name}'   AS STEP_NAME,
            {row_count}     AS ROW_COUNT,
            CURRENT_TIMESTAMP() AS START_TS,
            CURRENT_TIMESTAMP() AS END_TS,
            '{derived_status}'  AS STATUS,
            OBJECT_CONSTRUCT(
                'table_name', '{table_name}',
                'date_column', '{date_column}'
            ) AS EXTRA_INFO
            ;
        """
        cur.execute(query)
        conn.commit()