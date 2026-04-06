from src.ingestion.utils import current_timestamps
from src.ingestion.tmdb.client import TMDBClient


def fetch_credits_payload(
    media_type: str,
    tmdb_id: int,
) -> dict:
    client = TMDBClient()

    data = client.get_credits(
        media_type=media_type,
        tmdb_id=tmdb_id,
    )

    load_ts, _ = current_timestamps()
    return {
        "media_type": media_type,
        "tmdb_id": tmdb_id,
        "load_ts": load_ts,
        "data": data,
    }


if __name__ == "__main__":
    payload = fetch_credits_payload(
        media_type="movie",
        tmdb_id=1171145,
    )

    print(payload)