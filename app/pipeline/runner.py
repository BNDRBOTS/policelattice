from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import yaml
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.ingestion.base import AdapterRegistry, BaseAdapter
from app.ingestion.arcgis import ArcGISAdapter
from app.ingestion.audio import AudioAdapter
from app.ingestion.courtlistener import CourtListenerAdapter
from app.ingestion.flatfile import FlatFileAdapter
from app.ingestion.generic_rest import GenericRestAdapter
from app.ingestion.muckrock import MuckRockAdapter
from app.ingestion.news_rss import NewsRssAdapter
from app.ingestion.opd import OpenPoliceDataAdapter
from app.ingestion.pdf_ocr import PdfOcrAdapter
from app.ingestion.public_records import PublicRecordsAdapter
from app.ingestion.socrata import SocrataAdapter
from app.ingestion.web_scraper import WebScraperAdapter
from app.models import DataSource, RawRecord, StagingRecord


def _utcnow() -> datetime:
    """Return current UTC time with timezone awareness."""
    return datetime.now(timezone.utc)


# Register adapters
AdapterRegistry.register("arcgis")(ArcGISAdapter)
AdapterRegistry.register("audio")(AudioAdapter)
AdapterRegistry.register("courtlistener")(CourtListenerAdapter)
AdapterRegistry.register("flatfile")(FlatFileAdapter)
AdapterRegistry.register("generic_rest")(GenericRestAdapter)
AdapterRegistry.register("muckrock")(MuckRockAdapter)
AdapterRegistry.register("news_rss")(NewsRssAdapter)
AdapterRegistry.register("opd")(OpenPoliceDataAdapter)
AdapterRegistry.register("pdf_ocr")(PdfOcrAdapter)
AdapterRegistry.register("public_records")(PublicRecordsAdapter)
AdapterRegistry.register("socrata")(SocrataAdapter)
AdapterRegistry.register("web_scraper")(WebScraperAdapter)


def load_catalog(path: str = "app/source_catalog.yaml") -> list[dict[str, Any]]:
    """Load the source catalog from YAML."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["sources"]


def get_source_by_id(session: Session, source_id: str) -> DataSource:
    """Retrieve or create a DataSource record by its ID."""
    source = session.get(DataSource, source_id)
    if not source:
        source = DataSource(
            id=source_id,
            name=source_id,
            category="unknown",
            adapter="manual",
            access_mode="manual",
        )
        session.add(source)
        session.commit()
    return source


def run_source(session: Session, source_def: dict[str, Any]) -> int:
    """Run a single source definition. Returns number of raw records created."""
    source_id = source_def["id"]
    source = get_source_by_id(session, source_id)
    adapter_name = source_def.get("adapter", "manual")
    adapter_cls = AdapterRegistry.get(adapter_name)
    adapter: BaseAdapter = adapter_cls(source_def)

    source.name = source_def.get("name", source_id)
    source.category = source_def.get("category", "unknown")
    source.adapter = adapter_name
    source.access_mode = source_def.get("access_mode", "manual")
    source.schedule = source_def.get("schedule")
    source.availability_window = source_def.get("availability_window")
    source.config = source_def.get("config", {})
    session.commit()

    raw_records = adapter.fetch()
    count = 0
    batch_id = uuid.uuid4().hex
    for raw_dto in raw_records:
        checksum = raw_dto.compute_checksum()
        raw = RawRecord(
            source_id=source_id,
            batch_id=batch_id,
            content_type=raw_dto.content_type,
            raw_data=raw_dto.payload if isinstance(raw_dto.payload, dict) else {"items": raw_dto.payload},
            file_path=raw_dto.file_path,
            checksum=checksum,
            ingested_at=_utcnow(),
        )
        session.add(raw)
        session.flush()

        staging = StagingRecord(
            raw_record_id=raw.id,
            source_id=source_id,
            entity_type=source_def.get("entity_type", "unknown"),
            payload=raw_dto.payload if isinstance(raw_dto.payload, dict) else {"items": raw_dto.payload},
            record_hash=checksum,
            status="pending",
        )
        session.add(staging)
        count += 1

    source.last_run_at = _utcnow()
    source.last_error = None
    session.commit()
    return count


def run_all_due() -> dict[str, Any]:
    """Run all sources whose schedule is due.

    The scheduler uses cron expressions to trigger at precise windows.
    Returns a dict mapping source_id to either the record count (int) or
    an error string prefixed with 'ERROR:'.
    """
    sources = load_catalog()
    result: dict[str, Any] = {}
    with SessionLocal() as session:
        for source_def in sources:
            if not source_def.get("enabled", True):
                continue
            try:
                count = run_source(session, source_def)
                result[source_def["id"]] = count
            except Exception as exc:
                source = get_source_by_id(session, source_def["id"])
                source.last_error = str(exc)
                session.commit()
                result[source_def["id"]] = f"ERROR: {exc}"
    return result
