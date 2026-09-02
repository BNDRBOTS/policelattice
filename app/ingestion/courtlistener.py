from __future__ import annotations

import os
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base import BaseAdapter, RawRecordDTO


class CourtListenerAdapter(BaseAdapter):
    """Ingests docket metadata and documents from the CourtListener API.

    Requires `COURTLISTENER_TOKEN`. If no docket number is configured, returns
    no records. This adapter does not scrape CourtListener HTML.
    """

    name = "courtlistener"
    access_mode = "api"
    API_BASE = "https://www.courtlistener.com/api/rest/v4"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> Any:
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> list[RawRecordDTO]:
        token = self.settings.courtlistener_token
        if not token:
            self.log_skip("COURTLISTENER_TOKEN not set")
            return []

        headers = {"Authorization": f"Token {token}"}
        docket_env = self.source_config.get("docket_number_env")
        docket_number = getattr(self.settings, docket_env.lower(), None) if docket_env else self.source_config.get("docket_number")
        if not docket_number:
            self.log_skip("No docket number configured")
            return []

        params = {"docket_number": docket_number}
        data = self._get(f"{self.API_BASE}/dockets/", params, headers)
        records: list[RawRecordDTO] = []
        if data.get("results"):
            for docket in data["results"]:
                records.append(
                    RawRecordDTO(
                        content_type="application/json",
                        payload={"docket": docket},
                        source_id=self.source_config.get("id"),
                        metadata={"adapter": self.name, "docket_number": docket_number},
                    )
                )
        return records
