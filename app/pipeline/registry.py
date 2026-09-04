"""Adapter registry and catalog loading."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from app.ingest.arcgis_hub import ArcGisHubAdapter, ArcGisLayerAdapter
from app.ingest.base import BaseAdapter
from app.ingest.ckan import CkanAdapter, CkanCsvAdapter
from app.ingest.http_tabular import HttpTabularAdapter
from app.ingest.rss import RssAdapter

CATALOG_PATH = Path(__file__).resolve().parent.parent / "source_catalog.yaml"

_ADAPTERS: dict[str, type[BaseAdapter]] = {
    "ckan": CkanAdapter,
    "ckan_csv": CkanCsvAdapter,
    "arcgis_hub": ArcGisHubAdapter,
    "arcgis_layer": ArcGisLayerAdapter,
    "http_tabular": HttpTabularAdapter,
    "rss": RssAdapter,
}


@dataclass
class SourceDefinition:
    """One catalog entry."""

    id: str
    name: str
    adapter: str
    entity_type: str | None = None
    publisher: str | None = None
    schedule: str | None = None
    verified: str = "runtime"
    verified_on: str | None = None
    verified_detail: str | None = None
    config: dict[str, Any] = field(default_factory=dict)

    def build(self) -> BaseAdapter:
        try:
            cls = _ADAPTERS[self.adapter]
        except KeyError as exc:  # pragma: no cover - catalog is validated at load
            raise ValueError(f"Unknown adapter '{self.adapter}' for source '{self.id}'") from exc
        return cls(self.id, self.name, self.config)


def load_catalog(path: Path | str = CATALOG_PATH) -> list[SourceDefinition]:
    """Load and validate the source catalog.

    Validation is strict on purpose: a catalog entry that names an unknown
    adapter, or a source with no way to reach the network, is a build error
    rather than a silently skipped feed.
    """
    with open(path, encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    definitions: list[SourceDefinition] = []
    for entry in raw.get("sources", []):
        adapter = entry.get("adapter")
        if adapter not in _ADAPTERS:
            raise ValueError(f"Source '{entry.get('id')}' names unknown adapter '{adapter}'")
        if not entry.get("id"):
            raise ValueError("Catalog entry is missing an id")
        definitions.append(
            SourceDefinition(
                id=entry["id"],
                name=entry.get("name") or entry["id"],
                adapter=adapter,
                entity_type=entry.get("entity_type"),
                publisher=entry.get("publisher"),
                schedule=entry.get("schedule"),
                verified=str(entry.get("verified", "runtime")),
                verified_on=entry.get("verified_on"),
                verified_detail=entry.get("verified_detail"),
                config=entry.get("config") or {},
            )
        )
    if not definitions:
        raise ValueError("Source catalog contains no sources")
    return definitions
