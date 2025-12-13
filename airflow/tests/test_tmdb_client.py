import os
from typing import Any, Dict

import pytest

from plugins.tmdb_client import TMDBClient


class DummyResponse:
    def __init__(self, json_data: Dict[str, Any], status_code: int = 200):
        self._json_data = json_data
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        return self._json_data

    def raise_for_status(self) -> None:
        if not (200 <= self.status_code < 300):
            raise Exception(f"HTTP {self.status_code}")


def test_tmdb_client_raises_when_api_key_missing(monkeypatch):
    monkeypatch.delenv("TMDB_API_KEY", raising=False)

    with pytest.raises(ValueError) as exc:
        TMDBClient()

    assert "TMDB_API_KEY is not set" in str(exc.value)


def test_tmdb_client_uses_env_api_key(monkeypatch):
    monkeypatch.setenv("TMDB_API_KEY", "dummy-api-key")
    client = TMDBClient()
    assert client.api_key == "dummy-api-key"


def test_tmdb_client_get_trending_calls_correct_url(monkeypatch):
    monkeypatch.setenv("TMDB_API_KEY", "dummy-api-key")

    captured = {}

    def fake_get(url, headers, params, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        captured["timeout"] = timeout
        return DummyResponse({"results": [{"id": 1}, {"id": 2}]})

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    client = TMDBClient()
    results = client.get_trending(media_type="movie", time_window="week")

    assert results == [{"id": 1}, {"id": 2}]
    assert captured["url"].endswith("/trending/movie/week")
    assert captured["params"]["api_key"] == "dummy-api-key"
    assert captured["timeout"] == 10
    assert captured["headers"]["Accept"] == "application/json"
