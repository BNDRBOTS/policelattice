"""SEARCH phase — live discovery and resolution of external data sources.

The Search phase never fabricates: it queries public discovery APIs and
records exactly what the outside world reports. Two live discovery backends:

1. Socrata Discovery Catalog (``api.us.socrata.com/api/catalog/v1``) — the
   official live search over all Socrata open-data domains.
2. ArcGIS Online / Hub (``hub.arcgis.com/api/v3/datasets`` + the AGOL sharing
   REST API) — live search over ArcGIS-hosted datasets, resolving dataset
   items to their FeatureServer layer URLs.

Resolved endpoints are written back into ``data_sources.config`` so the
Gather phase always targets endpoints that were verifiably live at discovery
time, together with the discovery response that produced them.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from sqlalchemy.orm import Session

from app.ingestion.http_client import LiveSourceError, get_fetch_client, parse_json_bytes
from app.models import DataSource

logger = logging.getLogger(__name__)

AGOL_ITEM_API = "https://www.arcgis.com/sharing/rest/content/items"
HUB_SEARCH_API = "https://hub.arcgis.com/api/v3/datasets"


def _utcnow() -> datetime:
    return datetime.now(UTC)


def socrata_discover(domain: str, query: str, limit: int = 25) -> list[dict[str, Any]]:
    """Live search of a Socrata domain's catalog for datasets matching query."""
    from app.ingestion.socrata import SocrataAdapter

    adapter = SocrataAdapter({"id": "search_phase", "config": {}})
    return adapter.discover_datasets(domain, query, limit)


def arcgis_hub_search(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """Live search of ArcGIS Hub for datasets matching a query.

    Returns dataset records with their AGOL item id, title, and owner.
    """
    client = get_fetch_client()
    url = f"{HUB_SEARCH_API}?{quote('q')}={quote(query)}&page[size]={int(limit)}"
    body = client.get_bytes(url)
    data = parse_json_bytes(body)
    out: list[dict[str, Any]] = []
    for hit in data.get("data", []) if isinstance(data, dict) else []:
        attrs = hit.get("attributes", {})
        out.append(
            {
                "item_id": attrs.get("id") or hit.get("id"),
                "title": attrs.get("name"),
                "owner": attrs.get("owner"),
                "type": attrs.get("type"),
                "modified": attrs.get("modified"),
                "source": attrs.get("source"),
            }
        )
    return [d for d in out if d.get("item_id")]


def resolve_arcgis_item_to_service(item_id: str) -> dict[str, Any]:
    """Resolve an AGOL item to its live FeatureServer/MapServer layer URLs.

    Uses the public AGOL sharing REST API (item metadata + service definition).
    Only authoritative fields from the API response are returned.
    """
    client = get_fetch_client()
    info_body = client.get_bytes(f"{AGOL_ITEM_API}/{item_id}?f=json")
    info = parse_json_bytes(info_body)
    if not isinstance(info, dict) or "error" in info:
        raise LiveSourceError(f"AGOL item {item_id} lookup failed: {info}", False)

    item_type = info.get("type", "")
    service_url = info.get("url", "")
    if item_type not in ("Feature Service", "Map Service") or not service_url:
        return {
            "item_id": item_id,
            "type": item_type,
            "service_urls": [],
            "title": info.get("title"),
        }

    # The service definition lists layers authoritatively.
    layer_urls: list[str] = []
    try:
        svc_body = client.get_bytes(f"{service_url.rstrip('/')}?f=json")
        svc = parse_json_bytes(svc_body)
        for layer in svc.get("layers", []) or []:
            lid = layer.get("id")
            if lid is not None:
                layer_urls.append(f"{service_url.rstrip('/')}/{lid}")
    except LiveSourceError:
        layer_urls = [service_url.rstrip("/")]

    return {
        "item_id": item_id,
        "type": item_type,
        "title": info.get("title"),
        "owner": info.get("owner"),
        "service_urls": layer_urls,
    }


class SearchPhase:
    """Executes live discovery for all catalog sources that declare it."""

    def __init__(self, session: Session):
        self.session = session

    def execute(self, sources: list[dict[str, Any]]) -> dict[str, Any]:
        stats: dict[str, Any] = {
            "started_at": _utcnow().isoformat(),
            "backends": ["socrata-discovery-catalog", "arcgis-hub-search"],
            "queries": [],
            "resolved": {},
            "errors": [],
        }
        for source_def in sources:
            if not source_def.get("enabled", True):
                continue
            config = source_def.get("config", {})
            source_id = source_def["id"]

            socrata_discover_cfg = config.get("discover")
            if config.get("domain") and socrata_discover_cfg:
                try:
                    found = socrata_discover(
                        config["domain"],
                        socrata_discover_cfg.get("query", "police"),
                        int(socrata_discover_cfg.get("limit", 25)),
                    )
                    stats["queries"].append(
                        {
                            "backend": "socrata",
                            "domain": config["domain"],
                            "query": socrata_discover_cfg.get("query", "police"),
                            "results": len(found),
                        }
                    )
                    if found:
                        stats["resolved"][source_id] = {
                            "dataset_ids": [d["id"] for d in found],
                            "datasets": found,
                        }
                        self._persist_resolution(source_id, "socrata_datasets", found)
                except Exception as exc:
                    stats["errors"].append(
                        {"source": source_id, "phase": "socrata", "error": str(exc)}
                    )

            hub_cfg = config.get("hub_discover")
            if hub_cfg:
                try:
                    hits = arcgis_hub_search(
                        hub_cfg.get("query", "police"), int(hub_cfg.get("limit", 10))
                    )
                    stats["queries"].append(
                        {
                            "backend": "arcgis-hub",
                            "query": hub_cfg.get("query", "police"),
                            "results": len(hits),
                        }
                    )
                    resolved_layers: list[dict[str, Any]] = []
                    for hit in hits[: int(hub_cfg.get("limit", 10))]:
                        try:
                            resolved = resolve_arcgis_item_to_service(hit["item_id"])
                            if resolved.get("service_urls"):
                                resolved_layers.append(
                                    {
                                        "title": resolved.get("title") or hit.get("title"),
                                        "item_id": hit["item_id"],
                                        "service_urls": resolved["service_urls"],
                                    }
                                )
                        except Exception as exc:
                            stats["errors"].append(
                                {
                                    "source": f"{source_id}:{hit.get('item_id')}",
                                    "phase": "arcgis-item",
                                    "error": str(exc),
                                }
                            )
                    if resolved_layers:
                        stats["resolved"][source_id] = {"layers": resolved_layers}
                        self._persist_resolution(source_id, "arcgis_layers", resolved_layers)
                except Exception as exc:
                    stats["errors"].append(
                        {"source": source_id, "phase": "arcgis-hub", "error": str(exc)}
                    )

        stats["completed_at"] = _utcnow().isoformat()
        stats["sources_with_resolved_datasets"] = len(stats["resolved"])
        return stats

    def _persist_resolution(
        self, source_id: str, key: str, payload: list[dict[str, Any]]
    ) -> None:
        """Persist discovery results onto the DataSource registry row."""
        source = self.session.get(DataSource, source_id)
        if source is None:
            source = DataSource(
                id=source_id,
                name=source_id,
                category="discovered",
                adapter="socrata",
                access_mode="api",
            )
            self.session.add(source)
        cfg = dict(source.config or {})
        discovered = dict(cfg.get("discovered") or {})
        discovered[key] = {
            "resolved_at": _utcnow().isoformat(),
            "results": payload,
        }
        cfg["discovered"] = discovered
        source.config = cfg
        self.session.flush()
