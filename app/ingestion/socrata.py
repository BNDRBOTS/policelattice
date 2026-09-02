from __future__ import annotations

import os
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from app.ingestion.base import BaseAdapter, RawRecordDTO


class SocrataAdapter(BaseAdapter):
    """Ingests datasets from Socrata open data portals using SoQL.

    The domain and dataset identifiers must be explicitly configured.
    This adapter does not guess dataset URLs.
    """

    name = "socrata"
    access_mode = "api"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def _get(self, url: str, params: dict[str, Any]) -> list[Any]:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch(self) -> list[RawRecordDTO]:
        config = self.source_config
        domain_env = config.get("domain_env")
        domain = getattr(self.settings, domain_env.lower(), None) if domain_env else None
        if not domain:
            # Try direct dataset_id_env / dataset_id
            dataset_id_env = config.get("dataset_id_env")
            dataset_id = getattr(self.settings, dataset_id_env.lower(), None) if dataset_id_env else config.get("dataset_id")
            if not dataset_id:
                self.log_skip("No Socrata domain or dataset_id configured")
                return []
            url = f"{self._default_domain()}/resource/{dataset_id}.json"
            dataset_ids = [dataset_id]
        else:
            dataset_ids = config.get("dataset_ids", [])
            if isinstance(dataset_ids, str):
                dataset_ids = [dataset_ids]
            if not dataset_ids:
                self.log_skip("No dataset_ids configured")
                return []
            # Build URLs later
            records: list[RawRecordDTO] = []
            for ds_ref in dataset_ids:
                ds_id = getattr(self.settings, ds_ref.lower(), None) if ds_ref.startswith("PHX_") else ds_ref
                if not ds_id:
                    continue
                url = f"{domain.rstrip('/')}/resource/{ds_id}.json"
                records.extend(self._fetch_dataset(url, ds_id))
            return records

        return self._fetch_dataset(url, dataset_ids[0] if dataset_ids else "dataset")

    def _default_domain(self) -> str:
        return "https://www.phoenixopendata.com"

    def _fetch_dataset(self, url: str, dataset_id: str) -> list[RawRecordDTO]:
        records: list[RawRecordDTO] = []
        limit = 50000
        offset = 0
        while True:
            params = {"$limit": limit, "$offset": offset, "$order": ":id"}
            rows = self._get(url, params)
            if not rows:
                break
            for row in rows:
                records.append(
                    RawRecordDTO(
                        content_type="application/json",
                        payload={"dataset_id": dataset_id, "row": row},
                        source_id=self.source_config.get("id"),
                        metadata={"adapter": self.name, "dataset_id": dataset_id},
                    )
                )
            if len(rows) < limit:
                break
            offset += limit
        return records
