from __future__ import annotations

from pathlib import Path

from src.connectors.snowflake import execute_sql_script


SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
RAW_DATABASE = "DEV"
RAW_SCHEMA = "RAW"


def _read_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL template not found: {path}")
    return path.read_text(encoding="utf-8")


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _render_sql(template: str, **kwargs: str) -> str:
    rendered = template
    for key, value in kwargs.items():
        rendered = rendered.replace(f"{{{{ {key} }}}}", _escape_sql_string(value))
    return rendered


def load_trending_raw_s3(file_name: str, snapshot_date: str) -> None:
    sql = _read_sql("load_tmdb_trending.sql")

    rendered_sql = _render_sql(
        sql,
        file_name=file_name,
        snapshot_date=snapshot_date
    )

    execute_sql_script(
        rendered_sql,
        database=RAW_DATABASE,
        schema=RAW_SCHEMA
    )

def load_details_raw_s3(file_name: str) -> None:
    sql = _read_sql("load_tmdb_details.sql")

    rendered_sql = _render_sql(
        sql,
        file_name=file_name,
    )

    execute_sql_script(
        rendered_sql,
        database=RAW_DATABASE,
        schema=RAW_SCHEMA
    )


def load_credits_raw_s3(file_name: str) -> None:
    sql = _read_sql("load_tmdb_credits.sql")

    rendered_sql = _render_sql(
        sql,
        file_name=file_name,
    )

    execute_sql_script(
        rendered_sql,
        database=RAW_DATABASE,
        schema=RAW_SCHEMA
    )
