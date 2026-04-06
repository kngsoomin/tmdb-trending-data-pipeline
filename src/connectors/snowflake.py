from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from cryptography.hazmat.primitives import serialization


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value
from pathlib import Path


def _load_private_key(path: str):
    key_path = Path(path).expanduser()
    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,  # passphrase 있으면 넣기
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection(
    database: str | None = None,
    schema: str | None = None
) -> SnowflakeConnection:
    """
    Required env vars:
      - SNOWFLAKE_ACCOUNT
      - SNOWFLAKE_USER
      - SNOWFLAKE_PASSWORD
      - SNOWFLAKE_WAREHOUSE

    Optional env vars:
      - SNOWFLAKE_ROLE
    """
    account = _get_required_env("SNOWFLAKE_ACCOUNT")
    user = _get_required_env("SNOWFLAKE_USER")
    password = _get_required_env("SNOWFLAKE_PASSWORD")
    warehouse = _get_required_env("SNOWFLAKE_WAREHOUSE")
    private_key_path = _get_required_env("SNOWFLAKE_PRIVATE_KEY_PATH")
    role = os.getenv("SNOWFLAKE_ROLE")

    connect_kwargs: dict[str, Any] = {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "private_key": _load_private_key(private_key_path),
    }

    if role:
        connect_kwargs["role"] = role

    return snowflake.connector.connect(**connect_kwargs)


@contextmanager
def snowflake_cursor(
    database: str | None = None,
    schema: str | None = None,
) -> Iterator[SnowflakeCursor]:
    """
    Context-managed Snowflake cursor.
    Commits on success, rolls back on failure.
    """
    conn = get_snowflake_connection(database=database, schema=schema)
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()


def execute_sql(
    sql: str,
    database: str | None = None,
    schema: str | None = None,
    params: Sequence[Any] | None = None
) -> None:
    with snowflake_cursor(database=database, schema=schema) as cur:
        cur.execute(sql, params=params)


def execute_sql_script(
    sql: str,
    database: str | None = None,
    schema: str | None = None,
) -> None:
    with snowflake_cursor(database=database, schema=schema) as cur:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)


def fetch_all(sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    with snowflake_cursor() as cur:
        cur.execute(sql, params=params)
        return cur.fetchall()


def fetch_one(sql: str, params: Sequence[Any] | None = None) -> tuple[Any, ...] | None:
    with snowflake_cursor() as cur:
        cur.execute(sql, params=params)
        return cur.fetchone()