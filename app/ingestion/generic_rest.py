from __future__ import annotations

import os
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base import BaseAdapter, RawRecordDTO


class GenericRestAdapter(BaseAdapter):
    """Ingests a generic JSON REST endpoint.

    The URL must be provided via environment variable. Pagination is
    intentionally not assumed; if the response is a list, it is ingested as-is.
    """

    name = "generic_rest"
    access_mode = "api"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _get(self, url: str) -> Any:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> list[RawRecordDTO]:
        url_env = self.source_config.get("url_env")
        if not url_env:
            self.log_skip("No url_env configured")
            return []

        url = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
        if not url:
            self.log_skip(f"Environment variable {url_env} not set")
            return []

        data = self._get(url)
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and "data" in data:
            records = data["data"]
        else:
            records = [data]

        return [
            RawRecordDTO(
                content_type="application/json",
                payload={"row": record},
                source_id=self.source_config.get("id"),
                metadata={"adapter": self.name, "url": url},
            )
            for record in records
        ]
