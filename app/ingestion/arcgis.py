"""ArcGIS FeatureServer / MapServer live ingestion adapter.

Parsers / transport:
- ``httpx`` pooled transport with hard timeouts (see ``app.ingestion.http_client``).
- ``orjson`` response parsing (fastest RFC-compliant JSON parser).
- Server-side pagination via ``resultOffset`` / ``resultRecordCount`` with
  ``exceededTransferLimit`` handling per the ArcGIS REST specification.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client

logger = logging.getLogger(__name__)


class ArcGISAdapter(BaseAdapter):
    """Ingests ArcGIS FeatureServer layers live via the REST API."""

    name = "arcgis"
    access_mode = "api"

    def _resolve_service_urls(self) -> list[str]:
        """Resolve candidate service URLs: explicit env > discovery-resolved > config."""
        config = self.source_config
        urls: list[str] = []

        env_var = config.get("service_url_env")
        if env_var:
            url = getattr(self.settings, env_var.lower(), None) or os.getenv(env_var)
            if url:
                urls.append(url)

        # URLs resolved live by the Search phase (persisted on DataSource.config)
        discovered = config.get("discovered") or {}
        for layer_entry in discovered.get("arcgis_layers", []) or []:
            for svc in layer_entry.get("service_urls", []) or []:
                urls.append(svc)

        if config.get("service_url"):
            urls.append(config["service_url"])

        if not urls:
            self.log_skip("No service URLs resolved (env, discovery, or config)")
        return urls

    def _query_page(self, query_url: str, params: dict[str, Any]) -> dict[str, Any]:
        client = get_fetch_client()
        body = client.get_bytes(query_url, headers={"Accept": "application/json"})
        from app.ingestion.http_client import parse_json_bytes

        data = parse_json_bytes(body)
        if not isinstance(data, dict):
            raise LiveSourceError(f"ArcGIS response for {query_url} is not an object", False)
        if "error" in data:
            raise LiveSourceError(f"ArcGIS error from {query_url}: {data['error']}", False)
        return data

    def _fetch_service(self, base: str) -> list[RawRecordDTO]:
        config = self.source_config
        layer = config.get("layer", 0)
        query_url = f"{base.rstrip('/')}/{layer}/query"
        out_fields = config.get("out_fields", "*")
        where = config.get("where", "1=1")
        page_size = int(config.get("page_size", 2000))
        max_records = int(config.get("max_records", 200000))

        # Live capability probe: the layer must actually exist and be queryable.
        try:
            probe = self._query_page(
                query_url,
                {"where": "1=1", "outFields": "OBJECTID", "returnCountOnly": "true", "f": "json"},
            )
        except LiveSourceError as exc:
            self.log_skip(f"Layer probe failed for {base}/{layer}: {exc}")
            return []

        total = probe.get("count")
        if isinstance(total, int) and total == 0:
            self.log_skip(f"Layer {base}/{layer} reports zero features")
            return []

        records: list[RawRecordDTO] = []
        offset = 0
        while True:
            params = {
                "where": where,
                "outFields": out_fields,
                "returnGeometry": "true",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }
            try:
                data = self._query_page(query_url, params)
            except LiveSourceError as exc:
                self.log_skip(f"Page fetch failed at offset {offset}: {exc}")
                break

            features = data.get("features") or []
            for feature in features:
                payload = {
                    "attributes": feature.get("attributes", {}),
                    "geometry": feature.get("geometry"),
                }
                records.append(
                    RawRecordDTO(
                        content_type="application/json",
                        payload=payload,
                        source_id=self.source_config.get("id"),
                        metadata={
                            "adapter": self.name,
                            "service_url": base,
                            "layer": layer,
                            "live_url": f"{base.rstrip('/')}/{layer}/query?f=json",
                        },
                    )
                )
            exceeded = bool(data.get("exceededTransferLimit"))
            if not features or (not exceeded and len(features) < page_size):
                break
            offset += len(features)
            if offset >= max_records:
                logger.warning(
                    "[%s] Reached configured max_records=%d; stopping pagination",
                    self.name, max_records,
                )
                break
        return records

    def fetch(self) -> list[RawRecordDTO]:
        records: list[RawRecordDTO] = []
        for service_url in self._resolve_service_urls():
            records.extend(self._fetch_service(service_url))
        return records
