"""Shared adapter contract."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.ingest.http import FetchedResponse


@dataclass
class SourceVerification:
    """What the Verify phase actually observed about a source."""

    source_id: str
    ok: bool
    http_status: int | None = None
    rows_total_reported: int | None = None
    verified_at: datetime | None = None
    detail: str | None = None
    error: str | None = None
    response: FetchedResponse | None = None


@dataclass
class FetchedRows:
    """One page of rows, carrying its own citation."""

    source_id: str
    dataset: str | None
    resource_id: str | None
    resource_name: str | None
    rows: list[dict[str, Any]]
    url: str
    retrieved_at: datetime
    content_sha256: str
    http_status: int
    fields: list[dict[str, Any]] = field(default_factory=list)
    dataset_title: str | None = None
    dataset_notes: str | None = None
    publisher: str | None = None
    landing_page: str | None = None


class BaseAdapter:
    """Contract every adapter implements.

    Adapters never fabricate. When a source is unreachable they return an
    empty row stream and a ``SourceVerification`` describing the failure;
    the caller records that failure instead of substituting data.
    """

    kind: str = "base"

    def __init__(self, source_id: str, name: str, config: dict[str, Any] | None = None):
        self.source_id = source_id
        self.name = name
        self.config = config or {}

    def verify(self) -> SourceVerification:  # pragma: no cover - abstract
        raise NotImplementedError

    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:  # pragma: no cover
        raise NotImplementedError
        yield  # pragma: no cover

    @staticmethod
    def landing_url_for(base: str, dataset_id: str, resource_id: str) -> str:
        """Human-citable landing page for one CKAN resource."""
        return f"{base.rstrip('/')}/dataset/{dataset_id}/resource/{resource_id}"
