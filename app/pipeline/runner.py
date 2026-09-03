from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import yaml
from sqlalchemy import func, select
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
from app.models import (
    Agency,
    Arrest,
    Charge,
    CourtCase,
    DataSource,
    Document,
    EntityLink,
    Incident,
    NewsArticle,
    Officer,
    RawRecord,
    StagingRecord,
    SurveillanceEvent,
)
from app.pipeline.extraction import EvidenceExtractionEngine
from app.pipeline.normalization import CanonicalNormalizer

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
    """Run a single source definition with autonomous acquisition, canonical
    normalization, evidence extraction, and checksum deduplication.
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

        # 1. Canonical Normalization
        normalized = CanonicalNormalizer.normalize(
            raw_payload,
            entity_type=source_def.get("entity_type", "incident"),
            source_id=source_id,
        )

        # 2. Evidence Extraction
        evidence = EvidenceExtractionEngine.extract_from_record(raw_payload)

        # 3. Comprehensive Staging Payload
        staging_payload = {
            "canonical": normalized.canonical_payload,
            "evidence": evidence.to_dict(),
            "raw": raw_payload,
        }

        # Preserve top-level keys for backward compatibility
        for k, v in raw_payload.items():
            if k not in staging_payload:
                staging_payload[k] = v

        staging = StagingRecord(
            raw_record_id=raw.id,
            source_id=source_id,
            entity_type=normalized.canonical_type,
            payload=staging_payload,
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
    """Run only sources whose cron schedule matches the current time."""
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
    """Run ALL sources regardless of schedule (manual trigger)."""
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


def run_full_pipeline(session: Session | None = None, force: bool = True) -> dict[str, Any]:
    """Execute the full end-to-end data pipeline:
    1. Acquisition: Ingests raw data from all active/due sources.
    2. Normalization: Normalizes heterogeneous fields to canonical schemas.
    3. Evidence Extraction: Extracts structured entities (officers, force, statutes).
    4. Synthesis: Synthesizes staging records into core lattice entities.
    5. Resolution: Resolves suspended records whose relational dependencies have arrived.
    6. Re-synthesis: Completes synthesis for newly-resolved records.
    7. Returns unified execution statistics and entity lattice counts.
    """
    from app.pipeline.resolver import DependencyResolver
    from app.pipeline.synthesis import SynthesisEngine

    # 1. Ingestion / Autonomous Acquisition
    ingest_results = run_all_sources() if force else run_all_due()
    total_ingested = sum(v for v in ingest_results.values() if isinstance(v, int))

    def _execute_synthesis_cycle(s: Session) -> tuple[dict[str, Any], int]:
        s.expire_all()
        engine = SynthesisEngine(s)
        s_stats = engine.execute()

        resolver = DependencyResolver(s)
        resolved = resolver.resolve()

        if resolved > 0:
            engine2 = SynthesisEngine(s)
            s_stats2 = engine2.execute()
            s_stats["processed"] = s_stats.get("processed", 0) + s_stats2.get("processed", 0)
            s_stats["suspended"] = s_stats2.get("suspended", 0)
            s_stats["failed"] = s_stats.get("failed", 0) + s_stats2.get("failed", 0)

        return s_stats, resolved

    def _get_counts(s: Session) -> dict[str, int]:
        return {
            "incidents": s.scalar(select(func.count(Incident.id))) or 0,
            "officers": s.scalar(select(func.count(Officer.id))) or 0,
            "arrests": s.scalar(select(func.count(Arrest.id))) or 0,
            "charges": s.scalar(select(func.count(Charge.id))) or 0,
            "agencies": s.scalar(select(func.count(Agency.id))) or 0,
            "links": s.scalar(select(func.count(EntityLink.id))) or 0,
            "court_cases": s.scalar(select(func.count(CourtCase.id))) or 0,
            "documents": s.scalar(select(func.count(Document.id))) or 0,
            "news_articles": s.scalar(select(func.count(NewsArticle.id))) or 0,
            "surveillance_events": s.scalar(select(func.count(SurveillanceEvent.id))) or 0,
            "staging_records": s.scalar(select(func.count(StagingRecord.id))) or 0,
            "raw_records": s.scalar(select(func.count(RawRecord.id))) or 0,
        }

    if session is not None:
        synth_stats, resolved_count = _execute_synthesis_cycle(session)
        counts = _get_counts(session)
    else:
        with SessionLocal() as s:
            synth_stats, resolved_count = _execute_synthesis_cycle(s)
            counts = _get_counts(s)

    return {
        "status": "success",
        "ingestion": {
            "sources_run": len(ingest_results),
            "total_new_records": total_ingested,
            "results": ingest_results,
        },
        "synthesis": synth_stats,
        "resolved_dependencies": resolved_count,
        "entity_counts": counts,
    }
