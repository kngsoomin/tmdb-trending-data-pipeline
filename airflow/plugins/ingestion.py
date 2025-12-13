from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import pendulum
from airflow.exceptions import AirflowSkipException

from tmdb_client import TMDBClient


def fetch_trending_and_save(
    media_type: str,
    time_window: str,
    output_path: str,
    **context: Dict[str, Any],
    ) -> None:
    """
    Fetch trending data from TMDB and store as a JSON file.

    NOTE:
    TMDB trending API does NOT support historical logical dates.
    We only allow fetching for "today" (in the Airflow timezone).
    For past logical dates, this task is skipped to avoid
    writing incorrect data into the RAW layer during backfills.
    """

    logical_date = context["logical_date"]
    today = pendulum.now("UTC").date()

    if logical_date.date() < today.subtract(days=1):
        # NOTE: Historical backfill cannot re-call the API, because
        # TMDB only returns "current" trending data
        raise AirflowSkipException(
            f"Skipping TMDB fetch for historical logical date {logical_date.date()} "
            f"(API does not support backfilled trending snapshots)."
        ) 

    client = TMDBClient()
    results: List[Dict[str, Any]] = client.get_trending(
        media_type=media_type,
        time_window=time_window,
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "media_type": media_type,
                "time_window": time_window,
                "results": results,
            },
            f,
            ensure_ascii=False,
        )

