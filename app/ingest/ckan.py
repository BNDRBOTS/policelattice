"""CKAN adapter.

The City of Phoenix Open Data portal (``phoenixopendata.com``) is a CKAN
instance, confirmed live on 2026-09-03:

    GET /api/3/action/package_list                       -> 200
    GET /api/3/action/package_show?id=arrests            -> 200
    GET /api/3/action/datastore_search?resource_id=<uuid> -> 200, total=4884

This adapter talks to that API directly. Dataset and resource identifiers
are resolved at run time from ``package_show``, so a dataset that Phoenix
renames or republishes is picked up without a code change, and every row
is emitted together with the exact resource URL and retrieval timestamp it
came from.

No credentials are used and none are required.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import orjson

from app.config import get_settings
from app.ingest.base import BaseAdapter, FetchedRows, SourceVerification
from app.ingest.http import get_json, shared_client
from app.ingest.parsers import parse_csv, rows_to_records

logger = logging.getLogger(__name__)
settings = get_settings()

_TIMESTAMP_TYPES = {"timestamp", "date", "datetime"}


class CkanAdapter(BaseAdapter):
    """Reads tabular resources out of a CKAN datastore."""

    kind = "ckan"

    def __init__(self, source_id: str, name: str, config: dict[str, Any] | None = None):
        super().__init__(source_id, name, config)
        self.base_url: str = (self.config.get("base_url") or settings.phoenix_ckan_url).rstrip("/")
        self.packages: list[str] = list(
            self.config.get("packages") or settings.phoenix_police_packages
        )
        self.organization: str | None = self.config.get("organization") or (
            settings.phoenix_ckan_organization
        )
        self._package_cache: dict[str, dict[str, Any]] = {}

    # -- CKAN actions ------------------------------------------------------
    def _action(self, action: str, params: dict[str, Any]) -> tuple[Any, Any]:
        url = f"{self.base_url}/api/3/action/{action}"
        payload, response = get_json(url, params=params)
        if not response.ok or not isinstance(payload, dict) or not payload.get("success"):
            error = None
            if isinstance(payload, dict):
                error = str(payload.get("error"))[:300]
            return None, (error or response.error or f"HTTP {response.status_code}")
        return payload.get("result"), None

    def discover_packages(self) -> list[dict[str, Any]]:
        """Resolve every configured slug to its full CKAN package metadata."""
        resolved: list[dict[str, Any]] = []
        for slug in self.packages:
            if slug in self._package_cache:
                resolved.append(self._package_cache[slug])
                continue
            result, error = self._action("package_show", {"id": slug})
            if error or not isinstance(result, dict):
                logger.info("[%s] package '%s' unavailable: %s", self.source_id, slug, error)
                continue
            if self.organization and (result.get("organization") or {}).get("name") not in (
                self.organization,
                None,
            ):
                continue
            self._package_cache[slug] = result
            resolved.append(result)
        return resolved

    @staticmethod
    def _datastore_resources(package: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            r
            for r in package.get("resources", [])
            if r.get("datastore_active") and r.get("id")
        ]

    @staticmethod
    def _date_field(fields: list[dict[str, Any]]) -> str | None:
        """The resource's own date column, if it declares one."""
        for field in fields:
            if str(field.get("type", "")).lower() in _TIMESTAMP_TYPES:
                return field.get("id")
        for field in fields:
            if "DATE" in str(field.get("id", "")).upper():
                return field.get("id")
        return None

    # -- verification ------------------------------------------------------
    def verify(self) -> SourceVerification:
        """Probe the portal and report the real state of every package."""
        probe_url = f"{self.base_url}/api/3/action/package_list"
        payload, response = get_json(probe_url)
        if not response.ok or not isinstance(payload, dict) or not payload.get("success"):
            return SourceVerification(
                source_id=self.source_id,
                ok=False,
                http_status=response.status_code,
                verified_at=response.retrieved_at,
                error=response.error or f"HTTP {response.status_code}",
                response=response,
            )

        available = set(payload.get("result") or [])
        resolved = self.discover_packages()
        resources = sum(len(self._datastore_resources(p)) for p in resolved)
        total_rows = 0
        for package in resolved:
            for resource in self._datastore_resources(package):
                page, _err = self._action(
                    "datastore_search", {"resource_id": resource["id"], "limit": 0}
                )
                if isinstance(page, dict):
                    total_rows += int(page.get("total") or 0)

        missing = sorted(set(self.packages) - available - {p.get("name") for p in resolved})
        detail = (
            f"{len(resolved)}/{len(self.packages)} packages resolved, "
            f"{resources} datastore resources, {total_rows} rows advertised"
        )
        if missing:
            detail += f"; not published: {', '.join(missing)}"

        return SourceVerification(
            source_id=self.source_id,
            ok=True,
            http_status=response.status_code,
            rows_total_reported=total_rows,
            verified_at=response.retrieved_at,
            detail=detail,
            response=response,
        )

    # -- fetching ----------------------------------------------------------
    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:
        """Page through every datastore-active resource of every package."""
        for package in self.discover_packages():
            dataset_ref = package.get("name") or package.get("id")
            dataset_title = package.get("title")
            dataset_notes = package.get("notes")
            publisher = (package.get("organization") or {}).get("title")

            for resource in self._datastore_resources(package):
                resource_id = resource["id"]
                landing = self.landing_url_for(self.base_url, dataset_ref, resource_id)
                page_size = max(1, int(settings.ckan_page_size))
                row_cap = max(1, int(settings.ckan_max_rows_per_resource))
                offset = 0
                sort_field: str | None = None
                fields: list[dict[str, Any]] = []

                while offset < row_cap:
                    params: dict[str, Any] = {
                        "resource_id": resource_id,
                        "limit": page_size,
                        "offset": offset,
                    }
                    if sort_field:
                        params["sort"] = f"{sort_field} desc"

                    page, error = self._action("datastore_search", params)
                    if error or not isinstance(page, dict):
                        logger.warning(
                            "[%s] %s offset=%s failed: %s",
                            self.source_id, resource_id, offset, error,
                        )
                        break

                    if not fields:
                        fields = page.get("fields") or []
                        detected = self._date_field(fields)
                        if detected and not sort_field:
                            sort_field = detected
                            offset = 0
                            continue

                    rows = page.get("records") or []
                    if not rows:
                        break

                    body = orjson.dumps(rows)
                    yield FetchedRows(
                        source_id=self.source_id,
                        dataset=package.get("name"),
                        resource_id=resource_id,
                        resource_name=resource.get("name"),
                        rows=rows_to_records(rows),
                        url=f"{self.base_url}/api/3/action/datastore_search"
                        f"?resource_id={resource_id}&limit={page_size}&offset={offset}",
                        retrieved_at=datetime.now(UTC),
                        content_sha256=_sha(body),
                        http_status=200,
                        fields=fields,
                        dataset_title=dataset_title,
                        dataset_notes=dataset_notes,
                        publisher=publisher,
                        landing_page=landing,
                    )

                    offset += len(rows)
                    total = page.get("total")
                    if isinstance(total, int) and offset >= total:
                        break
                    if len(rows) < page_size:
                        break


class CkanCsvAdapter(CkanAdapter):
    """CKAN resources whose datastore is inactive, read straight from the file.

    Streams the CSV and parses it with polars. Used only as a fallback when
    ``datastore_active`` is false for a resource.
    """

    kind = "ckan_csv"

    def fetch(self, *, months_back: int = 24) -> Iterator[FetchedRows]:  # noqa: ARG002
        for package in self.discover_packages():
            dataset_ref = package.get("name") or package.get("id")
            for resource in package.get("resources", []):
                if resource.get("datastore_active"):
                    continue
                url = resource.get("url")
                if not url or str(resource.get("format", "")).upper() != "CSV":
                    continue
                response = shared_client().get(url)
                if not response.ok:
                    logger.warning("[%s] CSV %s failed: %s", self.source_id, url, response.error)
                    continue
                rows = parse_csv(response.body)
                if not rows:
                    continue
                yield FetchedRows(
                    source_id=self.source_id,
                    dataset=package.get("name"),
                    resource_id=resource.get("id"),
                    resource_name=resource.get("name"),
                    rows=rows_to_records(rows),
                    url=response.url,
                    retrieved_at=response.retrieved_at,
                    content_sha256=response.content_sha256,
                    http_status=response.status_code,
                    dataset_title=package.get("title"),
                    dataset_notes=package.get("notes"),
                    publisher=(package.get("organization") or {}).get("title"),
                    landing_page=self.landing_url_for(
                        self.base_url, dataset_ref, resource.get("id")
                    ),
                )


def _sha(body: bytes) -> str:
    import hashlib

    return hashlib.sha256(body).hexdigest()
