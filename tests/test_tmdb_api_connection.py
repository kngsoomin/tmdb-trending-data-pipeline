from __future__ import annotations
from src.ingestion.tmdb_orchestration import fetch_trending_payload


def test_tmdb_api_connection() -> None:

    try:
        payload = fetch_trending_payload(media_type="all", time_window="day")

        assert payload is not None, "Payload is None"
        assert "results" in payload, "Missing 'results' key"

        results = payload["results"]
        assert isinstance(results, list), "'results' is not a list"
        assert len(results) > 0, "No results returned"

        sample = results[0]
        print("✅ API connection successful")
        print(f"Sample content: {sample.get('title') or sample.get('name')}")
        print(f"Total results: {len(results)}")

    except Exception as e:
        print("❌ API connection failed")
        raise e


if __name__ == "__main__":
    test_tmdb_api_connection()