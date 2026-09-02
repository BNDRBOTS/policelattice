from __future__ import annotations

import os
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base import BaseAdapter, RawRecordDTO


class ArcGISAdapter(BaseAdapter):
    """Ingests ArcGIS FeatureServer layers using the REST API.

    Physical constraints:
    - Pagination is handled with `resultOffset` and `resultRecordCount`.
    - The service URL must be explicitly configured via environment variable.
    - No assumption about geometry or field names is made.
    """

    name = "arcgis"
    access_mode = "api"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _query_page(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error: {data['error']}")
        return data

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        env_var = config.get("service_url_env")
        if not env_var:
            self.log_skip("No service_url_env configured")
            return []

        service_url = getattr(self.settings, env_var.lower(), None) or os.getenv(env_var)
        if not service_url:
            self.log_skip(f"Environment variable {env_var} not set")
            return []

        layer = config.get("layer", 0)
        query_url = f"{service_url.rstrip('/')}/{layer}/query"
        out_fields = config.get("out_fields", "*")
        where = config.get("where", "1=1")
        page_size = int(config.get("page_size", 2000))

        records: list[RawRecordDTO] = []
        offset = 0
        while True:
            params = {
                "where": where,
                "outFields": out_fields,
                "returnGeometry": "false",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }
            data = self._query_page(query_url, params)
            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                attrs = feature.get("attributes", {})
                geometry = feature.get("geometry")
                payload = {"attributes": attrs, "geometry": geometry}
                records.append(
                    RawRecordDTO(
                        content_type="application/json",
                        payload=payload,
                        source_id=self.source_config.get("id"),
                        metadata={"adapter": self.name, "service_url": service_url, "layer": layer},
                    )
                )

            if len(features) < page_size:
                break
            offset += page_size

        return records
