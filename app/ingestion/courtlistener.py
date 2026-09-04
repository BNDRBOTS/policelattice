"""CourtListener live API adapter (dockets, RECAP documents).

Uses the official CourtListener REST v4 API. When a specific docket number is
configured it is fetched directly; otherwise a full-text / docket search query
configured per source is executed live. Requires ``COURTLISTENER_TOKEN``.
All responses parsed with orjson.
"""

from __future__ import annotations

import logging
from urllib.parse import quote

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class CourtListenerAdapter(BaseAdapter):
    """Ingests live docket metadata and documents from CourtListener."""

    name = "courtlistener"
    access_mode = "api"
    API_BASE = "https://www.courtlistener.com/api/rest/v4"

    def _headers(self) -> dict[str, str] | None:
        token = self.settings.courtlistener_token
        if not token:
            self.log_skip("COURTLISTENER_TOKEN not set")
            return None
        return {"Authorization": f"Token {token}"}

    def fetch(self) -> list[RawRecordDTO]:
        headers = self._headers()
        if headers is None:
            return []

        config = self.source_config
        client = get_fetch_client()
        records: list[RawRecordDTO] = []

        docket_env = config.get("docket_number_env")
        docket_number = (
            getattr(self.settings, docket_env.lower(), None)
            if docket_env
            else config.get("docket_number")
        )
        query = config.get("query")
        court = config.get("court")

        try:
            if docket_number:
                url = f"{self.API_BASE}/dockets/?docket_number={quote(str(docket_number))}"
                if court:
                    url += f"&court={court}"
                data = client.get_json(url, headers=headers)
            elif query:
                url = f"{self.API_BASE}/search/?q={quote(query)}"
                if court:
                    url += f"&court={court}"
                url += "&type=r"
                data = client.get_json(url, headers=headers)
            else:
                self.log_skip("No docket_number or query configured")
                return []
        except LiveSourceError as exc:
            self.log_skip(f"CourtListener request failed: {exc}")
            return []

        results = data.get("results", []) if isinstance(data, dict) else []
        for item in results:
            kind = "docket" if "docket_number" in item else "search_result"
            records.append(
                RawRecordDTO(
                    content_type="application/json",
                    payload={kind: item},
                    source_id=self.source_config.get("id"),
                    metadata={
                        "adapter": self.name,
                        "kind": kind,
                        "live_url": f"{self.API_BASE}/dockets/",
                    },
                )
            )
        return records
