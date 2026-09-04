"""Gather / Organize / Process primitives for the live pipeline.

The functions here are composed by ``app.pipeline.orchestrator`` into the
mandated operation sequence:

    Search -> Gather -> Organize -> Process -> Verify -> Synthesize

- **Gather**   : live adapter fetch -> immutable raw snapshots (SHA-256 dedup).
- **Organize** : canonical normalization -> staging records.
- **Process**  : rule-based evidence extraction merged into staging payloads.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.ingestion.arcgis import ArcGISAdapter
from app.ingestion.base import AdapterRegistry, BaseAdapter, RawRecordDTO
from app.ingestion.courtlistener import CourtListenerAdapter
from app.ingestion.flatfile import FlatFileAdapter
from app.ingestion.generic_rest import GenericRestAdapter
from app.ingestion.muckrock import MuckRockAdapter
from app.ingestion.news_rss import NewsRssAdapter
from app.ingestion.pdf_ocr import PdfOcrAdapter
from app.ingestion.socrata import SocrataAdapter
from app.ingestion.web_scraper import WebScraperAdapter
from app.models import DataSource, RawRecord, StagingRecord
from app.pipeline.extraction import EvidenceExtractionEngine
from app.pipeline.normalization import CanonicalNormalizer

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


# Register adapters
AdapterRegistry.register("arcgis")(ArcGISAdapter)
AdapterRegistry.register("courtlistener")(CourtListenerAdapter)
AdapterRegistry.register("flatfile")(FlatFileAdapter)
AdapterRegistry.register("generic_rest")(GenericRestAdapter)
AdapterRegistry.register("muckrock")(MuckRockAdapter)
AdapterRegistry.register("news_rss")(NewsRssAdapter)
AdapterRegistry.register("pdf_ocr")(PdfOcrAdapter)
AdapterRegistry.register("socrata")(SocrataAdapter)
AdapterRegistry.register("web_scraper")(WebScraperAdapter)


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def load_catalog(path: str = "app/source_catalog.yaml") -> list[dict[str, Any]]:
    """Load the live source catalog from YAML."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["sources"]


def get_source_by_id(session: Session, source_id: str) -> DataSource:
    """Retrieve or create a DataSource registry row by its catalog ID."""
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


def get_merged_source_def(session: Session, source_id: str) -> dict[str, Any] | None:
    """Catalog definition for a source merged with its live discovery state.

    The Search phase persists runtime-resolved endpoints (Socrata dataset IDs,
    ArcGIS service URLs) onto ``DataSource.config``; merging them here lets
    every adapter target endpoints that were verifiably live at discovery
    time.
    """
    for source_def in load_catalog():
        if source_def["id"] == source_id:
            merged = dict(source_def)
            cfg = dict(merged.get("config", {}) or {})
            row = session.get(DataSource, source_id)
            if row is not None and row.config:
                for key, value in (row.config or {}).items():
                    if key not in cfg or cfg[key] in (None, {}, []):
                        cfg[key] = value
                discovered = (row.config or {}).get("discovered")
                if discovered:
                    cfg["discovered"] = discovered
            merged["config"] = cfg
            return merged
    return None


# ---------------------------------------------------------------------------
# Cron expression evaluation (schedule gating)
# ---------------------------------------------------------------------------

def _cron_field_matches(field: str, value: int, min_val: int, max_val: int) -> bool:
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
    """Cron gating with a 10-minute re-run suppression window."""
    if not schedule_expr:
        return False
    parts = schedule_expr.strip().split()
    if len(parts) != 5:
        logger.warning("Invalid cron expression (expected 5 fields): %s", schedule_expr)
        return False

    minute_f, hour_f, dom_f, month_f, dow_f = parts
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

    if last_run_at is not None:
        if last_run_at.tzinfo is None:
            last_run_at = last_run_at.replace(tzinfo=UTC)
        if (now - last_run_at).total_seconds() < 600:
            return False
    return True


# ---------------------------------------------------------------------------
# GATHER phase
# ---------------------------------------------------------------------------

def _checksum_exists(session: Session, source_id: str, checksum: str) -> bool:
    existing = session.scalar(
        select(RawRecord.id)
        .where(RawRecord.source_id == source_id, RawRecord.checksum == checksum)
        .limit(1)
    )
    return existing is not None


def gather_source(
    session: Session, source_def: dict[str, Any]
) -> dict[str, Any]:
    """Fetch a source live and persist immutable raw snapshots (dedup by checksum)."""
    source_id = source_def["id"]
    source = get_source_by_id(session, source_id)

    source.name = source_def.get("name", source_id)
    source.category = source_def.get("category", "unknown")
    source.adapter = source_def.get("adapter", "manual")
    source.access_mode = source_def.get("access_mode", "manual")
    source.schedule = source_def.get("schedule")
    source.availability_window = source_def.get("availability_window")
    cfg = dict(source_def.get("config", {}) or {})
    # Preserve previously-discovered endpoints on the registry row.
    if source.config and source.config.get("discovered"):
        cfg.setdefault("discovered", source.config["discovered"])
    source.config = cfg
    session.commit()

    adapter_name = source_def.get("adapter", "manual")
    adapter_cls = AdapterRegistry.get(adapter_name)
    adapter: BaseAdapter = adapter_cls(source_def)

    batch_id = uuid.uuid4().hex
    raw_dto = adapter.fetch()

    new_records: list[tuple[RawRecord, RawRecordDTO]] = []
    skipped = 0
    for dto in raw_dto:
        checksum = dto.compute_checksum()
        if _checksum_exists(session, source_id, checksum):
            skipped += 1
            continue
        raw_payload = (
            dto.payload if isinstance(dto.payload, dict) else {"items": dto.payload}
        )
        raw = RawRecord(
            source_id=source_id,
            batch_id=batch_id,
            content_type=dto.content_type,
            raw_data=raw_payload,
            file_path=dto.file_path,
            checksum=checksum,
            ingested_at=_utcnow(),
        )
        session.add(raw)
        session.flush()
        new_records.append((raw, dto))

    source.last_run_at = _utcnow()
    # Persist an honest status: skip reasons when the source yielded nothing,
    # cleared on a successful non-empty fetch.
    if raw_dto or not adapter.skip_reasons:
        source.last_error = None
    else:
        source.last_error = "SKIPPED: " + " | ".join(adapter.skip_reasons[:5])
    session.commit()

    return {
        "source_id": source_id,
        "fetched": len(raw_dto),
        "new_raw_records": len(new_records),
        "duplicates_skipped": skipped,
        "skip_reasons": adapter.skip_reasons,
        "records": new_records,
        "batch_id": batch_id,
    }


# ---------------------------------------------------------------------------
# ORGANIZE phase
# ---------------------------------------------------------------------------

def organize_raw_record(
    session: Session, source_def: dict[str, Any], raw: RawRecord, dto: RawRecordDTO
) -> StagingRecord:
    """Normalize a raw snapshot into a canonical staging record."""
    source_id = source_def["id"]
    raw_payload = raw.raw_data or {}

    normalized = CanonicalNormalizer.normalize(
        raw_payload,
        entity_type=source_def.get("entity_type", "incident"),
        source_id=source_id,
    )

    staging_payload = {
        "canonical": normalized.canonical_payload,
        "provenance": {
            "source_id": source_id,
            "source_name": source_def.get("name", source_id),
            "adapter": source_def.get("adapter"),
            "content_type": raw.content_type,
            "checksum": raw.checksum,
            "ingested_at": raw.ingested_at.isoformat() if raw.ingested_at else None,
            "batch_id": raw.batch_id,
            "origin": dto.metadata or {},
        },
    }

    staging = StagingRecord(
        raw_record_id=raw.id,
        source_id=source_id,
        entity_type=normalized.canonical_type,
        payload=staging_payload,
        record_hash=raw.checksum,
        status="organized",
    )
    session.add(staging)
    session.flush()
    return staging


# ---------------------------------------------------------------------------
# PROCESS phase
# ---------------------------------------------------------------------------

def process_staging_record(staging: StagingRecord) -> StagingRecord:
    """Run rule-based evidence extraction and merge it into the staging payload."""
    raw_payload = staging.raw_record.raw_data if staging.raw_record else {}
    evidence = EvidenceExtractionEngine.extract_from_record(raw_payload or {})

    payload = dict(staging.payload or {})
    payload["evidence"] = evidence.to_dict()
    # Preserve raw content in full for maximum-fidelity downstream use.
    payload["raw"] = raw_payload
    staging.payload = payload
    staging.status = "processed"
    return staging


# ---------------------------------------------------------------------------
# Compatibility wrappers (manual triggers / scheduler)
# ---------------------------------------------------------------------------

def run_source(session: Session, source_def: dict[str, Any]) -> int:
    """Gather + Organize + Process one source. Returns count of new staging records."""
    result = gather_source(session, source_def)
    count = 0
    for raw, dto in result["records"]:
        staging = organize_raw_record(session, source_def, raw, dto)
        process_staging_record(staging)
        count += 1
    session.commit()
    return count


def run_all_due() -> dict[str, Any]:
    """Run sources whose cron schedule matches now (scheduler entry point)."""
    from app.pipeline.orchestrator import PipelineOrchestrator

    return PipelineOrchestrator().run(trigger="scheduled_due", due_only=True)


def run_all_sources() -> dict[str, Any]:
    """Run ALL enabled sources now (manual trigger)."""
    from app.pipeline.orchestrator import PipelineOrchestrator

    return PipelineOrchestrator().run(trigger="manual", force=True)


def run_full_pipeline(
    session: Session | None = None, force: bool = True, trigger: str = "startup"
) -> dict[str, Any]:
    """Execute the full six-phase pipeline (Search -> ... -> Synthesize)."""
    from app.pipeline.orchestrator import PipelineOrchestrator

    return PipelineOrchestrator().run(
        session=session, trigger=trigger, force=force or trigger in ("startup", "manual")
    )
