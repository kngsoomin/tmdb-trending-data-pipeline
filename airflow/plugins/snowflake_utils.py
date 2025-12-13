from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from contextlib import contextmanager

@contextmanager
def snowflake_cursor(conn_id="snowflake_netflix"):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        yield conn, cur
    finally:
        cur.close()
        conn.close()