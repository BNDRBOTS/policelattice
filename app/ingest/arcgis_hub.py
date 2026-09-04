"""ArcGIS Hub / ArcGIS Online adapter.

The City of Tempe open data portal (``data.tempe.gov``) is an ArcGIS Hub
site. Its DCAT catalog endpoint was confirmed live on 2026-09-03:

    GET https://data.tempe.gov/api/feed/dcat-us/1.1.json -> 200

Each DCAT entry carries an ArcGIS item id. Resolving that id through the
public ArcGIS Online sharing REST API yields the FeatureServer URL, which is
then queried with the standard ``/query`` operation. Nothing about the layer
schema is assumed — field names are taken from the service metadata.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from app.config import get_settings
from app.ingest.base import BaseAdapter, FetchedRows, SourceVerification
from app.ingest.http import get_json
from app.ingest.parsers import rows_to_records

logger = logging.getLogger(__name__)
settings = get_settings()

_ITEM_ID_RE = re.compile(r"item\.html\?id=([0-9a-f]{32})", re.IGNORECASE)


class ArcGisHubAdapter(BaseAdapter):
    """Discovers feature layers from an ArcGIS Hub DCAT feed and queries them."""

    kind = "arcgis_hub"

    def __init__(self, source_id: str, name: str, config: dict[str, Any] | None = None):
        super().__init__(source_id, name, config)
        self.hub_url: str = (self.config.get("hub_url") or settings.tempe_hub_url).rstrip("/")
        self.arcgis_url: str = (
            self.config.get("arcgis_url") or settings.arcgis_online_url
        ).rstrip("/")
        self.match_terms: list[str] = [
            str(t).lower() for t in (self.config.get("match_terms") or ["police", "incident"])
        ]
        self.max_layers: int = int(self.config.get("max_layers") or 6)
        self.page_size: int = int(self.config.get("page_size") or 1000)
        self._layers: list[dict[str, Any]] | None = None

    # -- discovery ---------------------------------------------------------
    @property
    def dcat_url(self) -> str:
        return f"{self.hub_url}/api/feed/dcat-us/1.1.json"

    def _catalog(self) -> list[dict[str, Any]]:
        payload, response = get_json(self.dcat_url)
        if not response.ok or not isinstance(payload, dict):
            return []
        datasets = payload.get("dataset") or []
        return [d for d in datasets if isinstance(d, dict)]

    def _matches(self, dataset: dict[str, Any]) -> bool:
        haystack = " ".join(
            [
                str(dataset.get("title") or ""),
                " ".join(str(k) for k in (dataset.get("keyword") or [])),
                str(dataset.get("description") or "")[:600],
            ]
        ).lower()
        return any(term in haystack for term in self.match_terms)

    def discover_layers(self) -> list[dict[str, Any]]:
        """Resolve matching DCAT entries to queryable FeatureServer layers."""
        if self._layers is not None:
            return self._layers

        layers: list[dict[str, Any]] = []
        for dataset in self._catalog():
            if len(layers) >= self.max_layers:
                break
            if not self._matches(dataset):
                continue
            identifier = str(dataset.get("identifier") or "")
            match = _ITEM_ID_RE.search(identifier)
            if not match:
                continue
            item_id = match.group(1)
            item, error = get_json(
                f"{self.arcgis_url}/sharing/rest/content/items/{item_id}", params={"f": "json"}
            )
            if error or not isinstance(item, dict):
                continue
            service_url = item.get("url")
            if not service_url or "FeatureServer" not in str(service_url):
                continue
            layers.append(
                {
                    "item_id": item_id,
                    "title": item.get("title") or dataset.get("title"),
                    "service_url": str(service_url),
                    "landing_page": identifier,
                    "publisher": (dataset.get("publisher") or {}).get("name"),
                    "modified": item.get("modified"),
                }
            )
        self._layers = layers
        return layers

    # -- verification ------------------------------------------------------
    def verify(self) -> SourceVerification:
        payload, response = get_json(self.dcat_url)
        if not response.ok or not isinstance(payload, dict):
            return SourceVerification(
                source_id=self.source_id,
                ok=False,
                http_status=response.status_code,
                verified_at=response.retrieved_at,
                error=response.error or f"HTTP {response.status_code}",
                response=response,
            )
        catalog_size = len(payload.get("dataset") or [])
        layers = self.discover_layers()
        total_rows = 0
        for layer in layers:
            count, _err = get_json(
                f"{layer['service_url']}/0/query",
                params={
                    "where": "1=1",
                    "returnCountOnly": "true",
                    "f": "json",
                },
            )
            if isinstance(count, dict) and isinstance(count.get("count"), int):
                total_rows += count["count"]
        return SourceVerification(
            source_id=self.source_id,
            ok=True,
            http_status=response.status_code,
            rows_total_reported=total_rows,
            verified_at=response.retrieved_at,
            detail=(
                f"DCAT catalog holds {catalog_size} datasets; "
                f"{len(layers)} matched {self.match_terms} and expose FeatureServer layers; "
                f"{total_rows} features advertised"
            ),
            response=response,
        )

    # -- fetching ----------------------------------------------------------
    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:  # noqa: ARG002
        for layer in self.discover_layers():
            service_url = layer["service_url"].rstrip("/")
            metadata, _err = get_json(f"{service_url}/0", params={"f": "json"})
            fields = (metadata or {}).get("fields", []) if isinstance(metadata, dict) else []
            offset = 0
            while True:
                params = {
                    "where": "1=1",
                    "outFields": "*",
                    "returnGeometry": "false",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": self.page_size,
                }
                payload, error = get_json(f"{service_url}/0/query", params=params)
                if error or not isinstance(payload, dict):
                    logger.warning(
                        "[%s] %s offset=%s failed: %s",
                        self.source_id, service_url, offset, error,
                    )
                    break
                features = payload.get("features") or []
                if not features:
                    break
                rows = [f.get("attributes", {}) for f in features if isinstance(f, dict)]
                import hashlib

                yield FetchedRows(
                    source_id=self.source_id,
                    dataset=layer["item_id"],
                    resource_id=f"{layer['item_id']}/0",
                    resource_name=layer["title"],
                    rows=rows_to_records(rows),
                    url=f"{service_url}/0/query?where=1%3D1&outFields=%2A&resultOffset={offset}",
                    retrieved_at=datetime.now(UTC),
                    content_sha256=hashlib.sha256(
                        repr(sorted(map(str, rows))).encode()
                    ).hexdigest(),
                    http_status=200,
                    fields=fields,
                    dataset_title=layer["title"],
                    publisher=layer["publisher"],
                    landing_page=layer["landing_page"],
                )
                if len(features) < self.page_size:
                    break
                offset += len(features)


class ArcGisLayerAdapter(BaseAdapter):
    """Queries an explicitly pinned FeatureServer layer."""

    kind = "arcgis_layer"

    def __init__(self, source_id: str, name: str, config: dict[str, Any] | None = None):
        super().__init__(source_id, name, config)
        self.service_url: str | None = self.config.get("service_url")
        self.layer: int = int(self.config.get("layer", 0))
        self.page_size: int = int(self.config.get("page_size") or 1000)

    def verify(self) -> SourceVerification:
        if not self.service_url:
            return SourceVerification(
                source_id=self.source_id,
                ok=False,
                verified_at=datetime.now(UTC),
                error="no service_url configured",
            )
        count, response = get_json(
            f"{self.service_url.rstrip('/')}/{self.layer}/query",
            params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        )
        ok = response.ok and isinstance(count, dict) and "count" in count
        return SourceVerification(
            source_id=self.source_id,
            ok=ok,
            http_status=response.status_code,
            rows_total_reported=int(count["count"]) if ok else None,
            verified_at=response.retrieved_at,
            detail=f"{self.service_url}/{self.layer}",
            error=None if ok else (response.error or "count unavailable"),
            response=response,
        )

    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:  # noqa: ARG002
        if not self.service_url:
            return
        import hashlib

        service_url = self.service_url.rstrip("/")
        offset = 0
        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": self.page_size,
            }
            payload, error = get_json(f"{service_url}/{self.layer}/query", params=params)
            if error or not isinstance(payload, dict):
                break
            features = payload.get("features") or []
            if not features:
                break
            rows = [f.get("attributes", {}) for f in features if isinstance(f, dict)]
            yield FetchedRows(
                source_id=self.source_id,
                dataset=self.config.get("dataset"),
                resource_id=f"layer_{self.layer}",
                resource_name=self.config.get("resource_name"),
                rows=rows_to_records(rows),
                url=f"{service_url}/{self.layer}/query?resultOffset={offset}",
                retrieved_at=datetime.now(UTC),
                content_sha256=hashlib.sha256(
                    repr(sorted(map(str, rows))).encode()
                ).hexdigest(),
                http_status=200,
                landing_page=self.config.get("landing_page") or service_url,
            )
            if len(features) < self.page_size:
                break
            offset += len(features)
