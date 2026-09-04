"""Socrata open-data portal live ingestion adapter with runtime discovery.

Two live capabilities:

1. **Discovery (Search)** — the Socrata Discovery Catalog API
   (``https://api.us.socrata.com/api/catalog/v1``) is queried at runtime for
   datasets matching configured search terms on a configured domain. Dataset
   IDs are therefore *resolved live* rather than hard-coded, so the pipeline
   automatically tracks dataset migrations.

2. **Resource fetch (Gather)** — rows are pulled from
   ``/resource/{id}.json`` with ``$limit``/``$offset`` pagination and parsed
   with ``orjson`` (best-in-class JSON parser).

No dataset IDs are guessed. If discovery returns no matching dataset, the
adapter yields zero records and the gap is visible in source status.
"""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlencode

from app.ingestion.base import BaseAdapter, RawRecordDTO
from app.ingestion.http_client import LiveSourceError, get_fetch_client, parse_json_bytes

logger = logging.getLogger(__name__)

DISCOVERY_API = "https://api.us.socrata.com/api/catalog/v1"


class SocrataAdapter(BaseAdapter):
    """Ingests datasets from live Socrata open-data portals."""

    name = "socrata"
    access_mode = "api"

    # ------------------------------------------------------------------ #
    # Discovery
    # ------------------------------------------------------------------ #

    def discover_datasets(
        self, domain: str, query: str, limit: int = 25
    ) -> list[dict[str, Any]]:
        """Query the live Socrata discovery catalog for datasets."""
        client = get_fetch_client()
        qs = urlencode(
            {"domains": domain, "q": query, "limit": int(limit), "only": "datasets"}
        )
        url = f"{DISCOVERY_API}?{qs}"
        body = client.get_bytes(url)
        data = parse_json_bytes(body)
        results = data.get("results", []) if isinstance(data, dict) else []
        discovered: list[dict[str, Any]] = []
        for item in results:
            res = item.get("resource", {})
            discovered.append(
                {
                    "id": res.get("id"),
                    "name": res.get("name"),
                    "description": res.get("description"),
                    "attribution": res.get("attribution"),
                    "rows_updated_at": res.get("rows_updated_at"),
                    "domain": item.get("metadata", {}).get("domain"),
                }
            )
        return [d for d in discovered if d.get("id")]

    # ------------------------------------------------------------------ #
    # Resource fetch
    # ------------------------------------------------------------------ #

    def _resolve_domain(self) -> str | None:
        config = self.source_config
        domain_env = config.get("domain_env")
        if domain_env:
            domain = getattr(self.settings, domain_env.lower(), None) or os.getenv(domain_env)
            if domain:
                return domain.rstrip("/")
        domain = config.get("domain")
        if domain:
            return domain.rstrip("/")
        self.log_skip("No Socrata domain configured")
        return None

    def _resolve_dataset_ids(self) -> list[str]:
        """Resolve dataset IDs to fetch, via explicit ID, env ID, or live discovery."""
        config = self.source_config
        ids: list[str] = []

        dataset_id_env = config.get("dataset_id_env")
        if dataset_id_env:
            val = getattr(self.settings, dataset_id_env.lower(), None) or os.getenv(dataset_id_env)
            if val:
                ids.append(val)

        for ds_ref in config.get("dataset_ids", []) or []:
            val = getattr(self.settings, str(ds_ref).lower(), None) or os.getenv(str(ds_ref))
            ids.append(val or str(ds_ref))

        discovery = config.get("discover")
        if discovery and not ids:
            domain = self._resolve_domain()
            if domain:
                try:
                    found = self.discover_datasets(
                        domain, discovery.get("query", "police"), int(discovery.get("limit", 25))
                    )
                    ids = [d["id"] for d in found]
                    logger.info(
                        "[%s] Live discovery on %s for %r resolved %d dataset(s): %s",
                        self.name, domain, discovery.get("query", "police"), len(ids), ids,
                    )
                except LiveSourceError as exc:
                    self.log_skip(f"Discovery failed on {domain}: {exc}")
                    return []

        if not ids:
            self.log_skip("No dataset IDs resolved (configure dataset_ids/dataset_id_env/discover)")
        return [i for i in ids if i]

    def fetch(self) -> list[RawRecordDTO]:
        domain = self._resolve_domain()
        if not domain:
            return []
        dataset_ids = self._resolve_dataset_ids()
        if not dataset_ids:
            return []

        records: list[RawRecordDTO] = []
        for ds_id in dataset_ids:
            records.extend(self._fetch_dataset(domain, ds_id))
        return records

    def _fetch_dataset(self, domain: str, dataset_id: str) -> list[RawRecordDTO]:
        client = get_fetch_client()
        base = f"https://{domain}/resource/{dataset_id}.json"
        page = int(self.source_config.get("page_size", 50000))
        offset = 0
        where = self.source_config.get("where")
        select = self.source_config.get("select")
        order = self.source_config.get("order", ":id")

        records: list[RawRecordDTO] = []
        while True:
            qp: dict[str, str] = {
                "$limit": str(page),
                "$offset": str(offset),
                "$order": order,
            }
            if where:
                qp["$where"] = where
            if select:
                qp["$select"] = select
            url = f"{base}?{urlencode(qp, safe=':$(),*')}"
            try:
                body = client.get_bytes(url)
                rows = parse_json_bytes(body)
            except LiveSourceError as exc:
                self.log_skip(f"Dataset {dataset_id} fetch failed: {exc}")
                break
            if not isinstance(rows, list):
                self.log_skip(f"Dataset {dataset_id} returned non-list payload")
                break
            for row in rows:
                records.append(
                    RawRecordDTO(
                        content_type="application/json",
                        payload={"dataset_id": dataset_id, "row": row},
                        source_id=self.source_config.get("id"),
                        metadata={
                            "adapter": self.name,
                            "dataset_id": dataset_id,
                            "domain": domain,
                            "live_url": f"https://{domain}/resource/{dataset_id}.json",
                        },
                    )
                )
            if len(rows) < page:
                break
            offset += page
        return records
