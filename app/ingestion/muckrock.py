from __future__ import annotations

from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base import BaseAdapter, RawRecordDTO


class MuckRockAdapter(BaseAdapter):
    """Ingests public records requests from MuckRock's API.

    Requires `MUCKROCK_TOKEN`. Without a token, this adapter returns no records.
    """

    name = "muckrock"
    access_mode = "api"
    API_BASE = "https://www.muckrock.com/api_v1"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _get(self, url: str, headers: dict[str, str], params: dict[str, Any] | None = None) -> Any:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> list[RawRecordDTO]:
        token = self.settings.muckrock_token
        if not token:
            self.log_skip("MUCKROCK_TOKEN not set")
            return []

        headers = {"Authorization": f"Token {token}"}
        # Fetch requests for the configured user or all requests if no user.
        url = f"{self.API_BASE}/foia/"
        params = {"user": self.settings.muckrock_username} if self.settings.muckrock_username else {}
        data = self._get(url, headers, params)
        results = data.get("results", [])
        return [
            RawRecordDTO(
                content_type="application/json",
                payload={"foia": item},
                source_id=self.source_config.get("id"),
                metadata={"adapter": self.name},
            )
            for item in results
        ]
