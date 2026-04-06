from __future__ import annotations

from src.connectors.snowflake import fetch_all
from src.connectors.s3 import s3_object_exists
from src.ingestion.utils import current_timestamps

from src.ingestion.load.snowflake_raw import (
    load_trending_raw_s3,
    load_details_raw_s3,
    load_credits_raw_s3,
)

from src.connectors.s3 import put_json_payload
from src.ingestion.tmdb.trending import fetch_trending_payload
from src.ingestion.tmdb.details import fetch_details_payload
from src.ingestion.tmdb.credits import fetch_credits_payload



def build_trending_file_name(snapshot_date: str) -> str:
    return f"tmdb_trending_{snapshot_date}.json"


def build_details_file_name(media_type: str, tmdb_id: int) -> str:
    return f"tmdb_details_{media_type}_{tmdb_id}.json"


def build_credits_file_name(media_type: str, tmdb_id: int) -> str:
    return f"tmdb_credits_{media_type}_{tmdb_id}.json"


# Snowflake queries
def get_latest_trending_keys() -> set[tuple[int, str]]:
    sql = """
    with latest_snapshot as (
        select max(snapshot_date) as snapshot_date
        from dev.raw.tmdb_trending_raw
        where media_type = 'all'
          and time_window = 'day'
    )
    select distinct
        f.value:id::number as tmdb_id,
        f.value:media_type::string as media_type
    from dev.raw.tmdb_trending_raw r,
         lateral flatten(input => r.payload) f,
         latest_snapshot s
    where r.snapshot_date = s.snapshot_date
      and f.value:media_type::string in ('movie', 'tv')
    """

    rows = fetch_all(sql)
    return {(int(r[0]), str(r[1])) for r in rows}


def get_existing_details_keys() -> set[tuple[int, str]]:
    sql = """
    select distinct tmdb_id, media_type
    from dev.raw.tmdb_details_raw
    """
    rows = fetch_all(sql)
    return {(int(r[0]), str(r[1])) for r in rows}


def get_existing_credits_keys() -> set[tuple[int, str]]:
    sql = """
    select distinct tmdb_id, media_type
    from dev.raw.tmdb_credits_raw
    """
    rows = fetch_all(sql)
    return {(int(r[0]), str(r[1])) for r in rows}


# Core logic
def compute_missing_keys(
    source_keys: set[tuple[int, str]],
    existing_keys: set[tuple[int, str]],
) -> list[tuple[int, str]]:
    return sorted(source_keys - existing_keys)


# Ingestion steps
def run_trending_ingestion(logical_date: str | None = None) -> None:
    _, current_date = current_timestamps()
    snapshot_date = logical_date or current_date

    file_name = build_trending_file_name(snapshot_date)

    if s3_object_exists(source="trending", file_name=file_name):
        print(f"[TRENDING] replay mode for snapshot_date={snapshot_date}")
    else:
        if snapshot_date != current_date:
            print(f"[TRENDING] no replay snapshot found for {snapshot_date}")
            raise FileNotFoundError(
                f"No replay snapshot found for snapshot_date={snapshot_date}. "
                f"TMDB trending API does not support historical fetch for past dates."
            )

        print(f"[TRENDING] fetch live data for snapshot_date={snapshot_date}")
        payload = fetch_trending_payload(
            media_type="all",
            time_window="day",
        )
        put_json_payload(
            payload=payload,
            source="trending",
            file_name=file_name,
        )

    load_trending_raw_s3(
        file_name=file_name,
        snapshot_date=snapshot_date,
    )


def run_details_ingestion() -> None:
    trending_keys = get_latest_trending_keys()
    existing_keys = get_existing_details_keys()
    missing_keys = compute_missing_keys(trending_keys, existing_keys)

    print(f"[DETAILS] missing count : {len(missing_keys)}")

    for tmdb_id, media_type in missing_keys:
        file_name = build_details_file_name(media_type, tmdb_id)
        payload = fetch_details_payload(
            tmdb_id=tmdb_id,
            media_type=media_type,
        )
        put_json_payload(
            payload=payload,
            source=f"details",
            file_name=file_name,
        )

        load_details_raw_s3(file_name=file_name)


def run_credits_ingestion() -> None:
    trending_keys = get_latest_trending_keys()
    existing_keys = get_existing_credits_keys()
    missing_keys = compute_missing_keys(trending_keys, existing_keys)

    print(f"[CREDITS] missing count: {len(missing_keys)}")

    for tmdb_id, media_type in missing_keys:
        file_name = build_credits_file_name(media_type, tmdb_id)
        payload = fetch_credits_payload(
            tmdb_id=tmdb_id,
            media_type=media_type,
        )

        put_json_payload(
            payload=payload,
            source=f"credits",
            file_name=file_name,
        )

        load_credits_raw_s3(file_name=file_name)