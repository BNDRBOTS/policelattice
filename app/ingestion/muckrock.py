"""MuckRock live API adapter for public records requests."""

from __future__ import annotations

import logging

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class MuckRockAdapter(BaseAdapter):
    """Ingests live public-records request data from MuckRock's API."""

    name = "muckrock"
    access_mode = "api"
    API_BASE = "https://www.muckrock.com/api_v1"

    def fetch(self) -> list[RawRecordDTO]:
        token = self.settings.muckrock_token
        if not token:
            self.log_skip("MUCKROCK_TOKEN not set")
            return []

        headers = {"Authorization": f"Token {token}"}
        config = self.source_config
        client = get_fetch_client()

        params = []
        if config.get("user") or self.settings.muckrock_username:
            user = config.get("user") or self.settings.muckrock_username
            params.append(f"user={user}")
        if config.get("query"):
            params.append(f"query={config['query']}")
        url = f"{self.API_BASE}/foia/"
        if params:
            url += "?" + "&".join(params)

        records: list[RawRecordDTO] = []
        try:
            data = client.get_json(url, headers=headers)
        except LiveSourceError as exc:
            self.log_skip(f"MuckRock request failed: {exc}")
            return []

        for item in data.get("results", []) if isinstance(data, dict) else []:
            records.append(
                RawRecordDTO(
                    content_type="application/json",
                    payload={"foia": item},
                    source_id=self.source_config.get("id"),
                    metadata={
                        "adapter": self.name,
                        "live_url": f"{self.API_BASE}/foia/",
                    },
                )
            )
        return records
