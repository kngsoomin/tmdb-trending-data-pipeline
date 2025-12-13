import os
import time
from typing import Dict, Any, List, Optional

import requests


class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB_API_KEY is not set in environment variables.")

    def _get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        backoff_seconds: int = 2,
    ) -> Dict[str, Any]:
        """
        Internal helper for GET requests with simple retry/backoff 
        dealing with rate limits or transient errors.
        """
        url = f"{self.BASE_URL}{path}"
        headers = {
            "Accept": "application/json",
        }
        _params: Dict[str, Any] = {"api_key": self.api_key}
        if params:
            _params.update(params)

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    url, headers=headers, params=_params, timeout=10
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                if attempt == max_retries:
                    raise
            
                # naive backoff
                    # For this project, a simple linear backoff is sufficient.
                    # In a production environment with higher concurrency,
                    # I would apply exponential backoff with jitter to prevent synchronized retries
                    # and avoid thundering herd problems.
                time.sleep(backoff_seconds * attempt)

        # Should never be reached due to the raise above
        return {}

    def get_trending(
        self,
        media_type: str = "all",    # "all" | "movie" | "tv"
        time_window: str = "day",   # "day" | "week"
    ) -> List[Dict[str, Any]]:
        """
        Call TMDB Trending API and return the list of result objects.
        """
        data = self._get(f"/trending/{media_type}/{time_window}")
        return data.get("results", [])
