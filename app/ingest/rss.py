"""RSS / Atom adapter for live news feeds."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from app.ingest.base import BaseAdapter, FetchedRows, SourceVerification
from app.ingest.http import shared_client
from app.ingest.parsers import parse_feed, rows_to_records

logger = logging.getLogger(__name__)


class RssAdapter(BaseAdapter):
    """Reads a public RSS/Atom endpoint with feedparser."""

    kind = "rss"

    @property
    def url(self) -> str | None:
        return self.config.get("url")

    def verify(self) -> SourceVerification:
        if not self.url:
            return SourceVerification(
                source_id=self.source_id, ok=False, error="no url configured"
            )
        response = shared_client().get(self.url)
        if not response.ok:
            return SourceVerification(
                source_id=self.source_id,
                ok=False,
                http_status=response.status_code,
                verified_at=response.retrieved_at,
                error=response.error,
                response=response,
            )
        entries = self._parse(response)
        return SourceVerification(
            source_id=self.source_id,
            ok=bool(entries),
            http_status=response.status_code,
            rows_total_reported=len(entries),
            verified_at=response.retrieved_at,
            detail=f"{len(entries)} entries in feed",
            error=None if entries else "feed parsed but contained no entries",
            response=response,
        )

    def _parse(self, response: Any) -> list[dict[str, Any]]:
        try:
            return parse_feed(response.body)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] feed parse failed: %s", self.source_id, exc)
            return []

    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:  # noqa: ARG002
        if not self.url:
            return
        response = shared_client().get(self.url)
        if not response.ok:
            logger.info("[%s] feed unavailable: %s", self.source_id, response.error)
            return
        entries = [e for e in self._parse(response) if e.get("link")]
        if not entries:
            return
        yield FetchedRows(
            source_id=self.source_id,
            dataset=self.config.get("dataset") or self.source_id,
            resource_id=self.config.get("resource_id"),
            resource_name=self.name,
            rows=rows_to_records(entries),
            url=response.url,
            retrieved_at=response.retrieved_at,
            content_sha256=response.content_sha256,
            http_status=response.status_code,
            dataset_title=self.name,
            publisher=self.config.get("publisher"),
            landing_page=response.url,
        )
