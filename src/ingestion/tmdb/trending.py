from src.ingestion.utils import current_timestamps
from src.ingestion.tmdb.client import TMDBClient



def fetch_trending_payload(
    media_type: str,
    time_window: str,
) -> dict:
    client = TMDBClient()

    results = client.get_trending(
        media_type=media_type,
        time_window=time_window,
    )

    load_ts, snapshot_date = current_timestamps()

    return {
        "media_type": media_type,
        "time_window": time_window,
        "snapshot_date": snapshot_date,
        "load_ts": load_ts,
        "results": results,
    }


if __name__ == "__main__":
    payload = fetch_trending_payload(
        media_type="all",
        time_window="day",
    )

    print(payload)