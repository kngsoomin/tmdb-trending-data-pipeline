import os
from pathlib import Path

from src.connectors.snowflake import execute_sql_script


SQL_DIR = Path(__file__).parent


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


def run_bootstrap() -> None:
    sql = _read_sql("bootstrap_tmdb_raw.sql")

    rendered = _render_sql(
        sql,
        bucket=os.getenv("TMDB_S3_BUCKET"),
        prefix=os.getenv("TMDB_S3_PREFIX"),
        aws_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    execute_sql_script(rendered)