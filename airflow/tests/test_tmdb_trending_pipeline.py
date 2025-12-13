import json
from typing import Any, Dict, List

import pendulum
import pytest
from airflow.exceptions import AirflowSkipException

from plugins.ingestion import fetch_trending_and_save


class DummyTMDBClient:
    def __init__(self, *args, **kwargs):
        pass

    def get_trending(self, media_type: str, time_window: str) -> List[Dict[str, Any]]:
        return [
            {"id": 1, "title": "Dummy 1"},
            {"id": 2, "title": "Dummy 2"},
        ]


def test_fetch_trending_and_save_skips_for_past_logical_date(monkeypatch, tmp_path):
    
    def fake_now(tz: str):
        return pendulum.datetime(2025, 1, 10, tz=tz)

    monkeypatch.setattr("plugins.ingestion.pendulum.now", fake_now)

    logical_date = pendulum.datetime(2025, 1, 9, tz="UTC")  # past date
    context = {
        "logical_date": logical_date,
    }

    output_path = tmp_path / "tmdb_trending_past.json"

    with pytest.raises(AirflowSkipException) as exc:
        fetch_trending_and_save(
            media_type="all",
            time_window="day",
            output_path=str(output_path),
            **context,
        )

    assert "historical logical date" in str(exc.value)
    # No file should be created
    assert not output_path.exists()


def test_fetch_trending_and_save_writes_file_for_today(monkeypatch, tmp_path):

    def fake_now(tz: str):
        return pendulum.datetime(2025, 1, 10, tz=tz)

    monkeypatch.setattr("plugins.ingestion.pendulum.now", fake_now)

    # logical_date equal to "fake today"
    logical_date = pendulum.datetime(2025, 1, 10, tz="UTC")
    context = {
        "logical_date": logical_date,
    }

    # Patch TMDBClient to our dummy implementation
    monkeypatch.setattr("plugins.ingestion.TMDBClient", DummyTMDBClient)

    output_path = tmp_path / "tmdb_trending_today.json"

    fetch_trending_and_save(
        media_type="all",
        time_window="day",
        output_path=str(output_path),
        **context,
    )

    assert output_path.exists()

    with open(output_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data["media_type"] == "all"
    assert data["time_window"] == "day"
    assert isinstance(data["results"], list)
    assert len(data["results"]) == 2
    assert data["results"][0]["id"] == 1
