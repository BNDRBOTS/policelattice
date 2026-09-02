from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.ingestion.arcgis import ArcGISAdapter
from app.ingestion.audio import AudioAdapter
from app.ingestion.base import AdapterRegistry, BaseAdapter
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

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return current UTC time with timezone awareness."""
    return datetime.now(UTC)


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


# ---------------------------------------------------------------------------
# Cron expression evaluation
# ---------------------------------------------------------------------------

def _cron_field_matches(field: str, value: int, min_val: int, max_val: int) -> bool:
    """Check if a single cron field matches a given integer value.

    Supports: *, exact number, */step, comma-separated values, ranges (a-b).
    """
    for part in field.split(","):
        part = part.strip()
        if part == "*":
            return True
        if "/" in part:
            base, step_str = part.split("/", 1)
            step = int(step_str)
            base_val = min_val if base == "*" else int(base)
            if (value - base_val) % step == 0 and value >= base_val:
                return True
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            if int(lo) <= value <= int(hi):
                return True
            continue
        if part.isdigit() and int(part) == value:
            return True
    return False


def _is_source_due(schedule_expr: str | None, last_run_at: datetime | None, now: datetime) -> bool:
    """Determine whether a source is due to run based on its cron schedule.

    A source is due when:
    - It has never been run (last_run_at is None), OR
    - The cron expression matches the current minute AND last_run_at is not
      within the same matching window (prevents re-running within 15 min tick).

    For sources with no schedule (null/None), they are only run manually
    via the /ingest/run endpoint and are never auto-triggered.
    """
    if not schedule_expr:
        return False

    parts = schedule_expr.strip().split()
    if len(parts) != 5:
        logger.warning("Invalid cron expression (expected 5 fields): %s", schedule_expr)
        return False

    minute_f, hour_f, dom_f, month_f, dow_f = parts

    # Python weekday: Monday=0..Sunday=6
    # Cron weekday: Sunday=0..Saturday=6
    cron_dow = (now.weekday() + 1) % 7

    if not _cron_field_matches(minute_f, now.minute, 0, 59):
        return False
    if not _cron_field_matches(hour_f, now.hour, 0, 23):
        return False
    if not _cron_field_matches(dom_f, now.day, 1, 31):
        return False
    if not _cron_field_matches(month_f, now.month, 1, 12):
        return False
    if not _cron_field_matches(dow_f, cron_dow, 0, 6):
        return False

    # Cron expression matches current minute. Check if already run recently.
    if last_run_at is not None:
        elapsed = (now - last_run_at).total_seconds()
        if elapsed < 600:  # Less than 10 minutes since last run
            return False

    return True


# ---------------------------------------------------------------------------
# Catalog loading
# ---------------------------------------------------------------------------

def load_catalog(path: str = "app/source_catalog.yaml") -> list[dict[str, Any]]:
    """Load the source catalog from YAML."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["sources"]


# ---------------------------------------------------------------------------
# Source execution
# ---------------------------------------------------------------------------

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


def _checksum_exists(session: Session, source_id: str, checksum: str) -> bool:
    """Check if a RawRecord with this checksum already exists for the source.

    This prevents duplicate ingestion of identical data across repeated runs.
    """
    existing = session.scalar(
        select(RawRecord.id).where(
            RawRecord.source_id == source_id,
            RawRecord.checksum == checksum,
        ).limit(1)
    )
    return existing is not None


def run_source(session: Session, source_def: dict[str, Any]) -> int:
    """Run a single source definition. Returns number of new raw records created.

    Deduplicates by checksum: if a RawRecord with the same checksum already
    exists for this source_id, it is skipped entirely.
    """
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
    skipped = 0
    batch_id = uuid.uuid4().hex
    for raw_dto in raw_records:
        checksum = raw_dto.compute_checksum()

        # Deduplicate: skip if this exact data was already ingested for this source
        if _checksum_exists(session, source_id, checksum):
            skipped += 1
            continue

        raw_payload = (
            raw_dto.payload
            if isinstance(raw_dto.payload, dict)
            else {"items": raw_dto.payload}
        )

        raw = RawRecord(
            source_id=source_id,
            batch_id=batch_id,
            content_type=raw_dto.content_type,
            raw_data=raw_payload,
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
            payload=raw_payload,
            record_hash=checksum,
            status="pending",
        )
        session.add(staging)
        count += 1

    source.last_run_at = _utcnow()
    source.last_error = None
    session.commit()

    if skipped > 0:
        logger.info(
            "[%s] Created %d new records, skipped %d duplicates",
            source_id, count, skipped,
        )

    return count


def run_all_due() -> dict[str, Any]:
    """Run only sources whose cron schedule matches the current time.

    Sources with schedule=null (manual/on-demand) are skipped here and
    must be triggered via POST /ingest/run.

    Returns a dict mapping source_id to either the record count (int) or
    an error string prefixed with 'ERROR:'.
    """
    sources = load_catalog()
    now = _utcnow()
    result: dict[str, Any] = {}
    with SessionLocal() as session:
        for source_def in sources:
            if not source_def.get("enabled", True):
                continue

            schedule_expr = source_def.get("schedule")
            if not schedule_expr:
                continue

            source = get_source_by_id(session, source_def["id"])
            if not _is_source_due(schedule_expr, source.last_run_at, now):
                continue

            try:
                count = run_source(session, source_def)
                result[source_def["id"]] = count
            except Exception as exc:
                source = get_source_by_id(session, source_def["id"])
                source.last_error = str(exc)
                session.commit()
                result[source_def["id"]] = f"ERROR: {exc}"
                logger.error("[%s] Ingestion failed: %s", source_def["id"], exc)
    return result


def run_all_sources() -> dict[str, Any]:
    """Run ALL sources regardless of schedule (manual trigger).

    Used by POST /ingest/run for on-demand ingestion of every source.
    Deduplication by checksum still applies within run_source().
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
                logger.error("[%s] Ingestion failed: %s", source_def["id"], exc)
    return result
