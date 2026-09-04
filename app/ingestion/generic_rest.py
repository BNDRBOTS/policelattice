"""Generic live JSON REST ingestion adapter."""

from __future__ import annotations

import logging
import os

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class GenericRestAdapter(BaseAdapter):
    """Ingests rows from a configured live JSON REST endpoint."""

    name = "generic_rest"
    access_mode = "api"

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        url_env = config.get("url_env")
        url = None
        if url_env:
            url = getattr(self.settings, url_env.lower(), None) or os.getenv(url_env)
        url = url or config.get("url")
        if not url:
            self.log_skip(f"No url/url_env configured (env {url_env})")
            return []

        try:
            data = get_fetch_client().get_json(url)
        except LiveSourceError as exc:
            self.log_skip(f"REST fetch failed for {url}: {exc}")
            return []

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = None
            for key in ("data", "results", "rows", "records", "items", "features"):
                if isinstance(data.get(key), list):
                    rows = data[key]
                    break
            if rows is None:
                rows = [data]
        else:
            rows = [data]

        return [
            RawRecordDTO(
                content_type="application/json",
                payload={"row": row} if isinstance(row, dict) else {"value": row},
                source_id=self.source_config.get("id"),
                metadata={"adapter": self.name, "url": url, "live_url": url},
            )
            for row in rows
        ]
