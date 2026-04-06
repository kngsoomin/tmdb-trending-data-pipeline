from __future__ import annotations

from src.connectors.snowflake import snowflake_cursor


def test_snowflake_connection_context() -> None:
    with snowflake_cursor(database="DEV", schema="RAW") as cur:
        cur.execute(
            """
            select
                current_user(),
                current_role(),
                current_database(),
                current_schema()
            """
        )
        row = cur.fetchone()

    assert row is not None, "❌ Snowflake connection test failed: no result returned"

    current_user, current_role, current_database, current_schema = row

    print("✅ Snowflake connection OK")
    print(f"  user     : {current_user}")
    print(f"  role     : {current_role}")
    print(f"  database : {current_database}")
    print(f"  schema   : {current_schema}")


if __name__ == "__main__":
    test_snowflake_connection_context()