"""Direct HTTP tabular adapter (national public CSV/JSON datasets)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from app.ingest.base import BaseAdapter, FetchedRows, SourceVerification
from app.ingest.http import shared_client
from app.ingest.parsers import parse_any, rows_to_records

logger = logging.getLogger(__name__)


class HttpTabularAdapter(BaseAdapter):
    """Downloads one or more public files and parses them by content type.

    Each URL in ``urls`` is tried in order; the first that returns a usable
    body wins, and the winning URL is recorded as the citation. A source
    whose every URL fails reports that failure — it is never back-filled
    from another source or from a local file.
    """

    kind = "http_tabular"

    @property
    def urls(self) -> list[str]:
        return [u for u in (self.config.get("urls") or []) if u]

    def verify(self) -> SourceVerification:
        urls = self.urls
        if not urls:
            return SourceVerification(
                source_id=self.source_id, ok=False, error="no urls configured"
            )
        last = None
        for url in urls:
            response = shared_client().get(url)
            last = response
            if response.ok:
                rows = self._parse(response)
                return SourceVerification(
                    source_id=self.source_id,
                    ok=True,
                    http_status=response.status_code,
                    rows_total_reported=len(rows),
                    verified_at=response.retrieved_at,
                    detail=f"{response.content_bytes} bytes from {response.url}",
                    response=response,
                )
        return SourceVerification(
            source_id=self.source_id,
            ok=False,
            http_status=last.status_code if last else None,
            verified_at=last.retrieved_at if last else None,
            error=last.error if last else "unreachable",
            response=last,
        )

    def _parse(self, response: Any) -> list[dict[str, Any]]:
        try:
            return parse_any(
                response.body, content_type=response.content_type, url=response.url
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] parse failed for %s: %s", self.source_id, response.url, exc)
            return []

    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:  # noqa: ARG002
        for url in self.urls:
            response = shared_client().get(url)
            if not response.ok:
                logger.info("[%s] %s unavailable: %s", self.source_id, url, response.error)
                continue
            rows = self._parse(response)
            if not rows:
                continue
            yield FetchedRows(
                source_id=self.source_id,
                dataset=self.config.get("dataset"),
                resource_id=self.config.get("resource_id"),
                resource_name=self.config.get("resource_name") or self.name,
                rows=rows_to_records(rows),
                url=response.url,
                retrieved_at=response.retrieved_at,
                content_sha256=response.content_sha256,
                http_status=response.status_code,
                dataset_title=self.name,
                publisher=self.config.get("publisher"),
                landing_page=self.config.get("landing_page") or response.url,
            )
            return
