from src.ingestion.utils import current_timestamps
from src.ingestion.tmdb_orchestration import (
    run_trending_ingestion,
    run_details_ingestion,
    run_credits_ingestion,
)

if __name__ == "__main__":

    _, today = current_timestamps()

    run_trending_ingestion(today)

    run_details_ingestion()

    run_credits_ingestion()