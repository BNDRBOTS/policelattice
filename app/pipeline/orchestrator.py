"""Pipeline orchestration.

Enforces the fixed operation sequence on every run:

    Search -> Gather -> Organize -> Process -> Verify -> Synthesize

Each phase writes its own observable result into ``PipelineRun.phases``, so a
run can be audited after the fact: what was searched, what was reached, how
many rows arrived, how many were rejected and why.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import SessionLocal, utcnow
from app.ingest.base import FetchedRows, SourceVerification
from app.models import DataSource, FetchLog, PipelineRun, RawRecord
from app.pipeline.registry import SourceDefinition, load_catalog
from app.pipeline.synthesize import Synthesizer

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Runs the six-phase acquisition and synthesis sequence."""

    def __init__(self, session: Session, *, months_back: int = 26):
        self.session = session
        self.months_back = months_back
        self.catalog: list[SourceDefinition] = load_catalog()

    # ------------------------------------------------------------------
    def run(self, trigger: str = "manual", *, only: set[str] | None = None) -> dict[str, Any]:
        """Execute all six phases and return the run report."""
        started = utcnow()
        run = PipelineRun(started_at=started, trigger=trigger, ok=False, phases={})
        self.session.add(run)
        self.session.flush()

        phases: dict[str, Any] = {}
        try:
            sources = [s for s in self.catalog if only is None or s.id in only]

            # 1. SEARCH -----------------------------------------------------
            phases["search"] = self._phase_search(sources)

            # 2..6 GATHER -> ORGANIZE -> PROCESS -> VERIFY -> SYNTHESIZE ----
            phases["gather"], phases["synthesize"] = self._phase_gather(sources)

            phases["verify"] = self._phase_verify()
            run.ok = True
        except Exception as exc:  # noqa: BLE001
            run.error = f"{type(exc).__name__}: {exc}"
            logger.exception("Pipeline run failed")
            raise
        finally:
            run.finished_at = utcnow()
            run.phases = phases
            self.session.commit()

        return {
            "run_id": run.id,
            "trigger": trigger,
            "ok": run.ok,
            "started_at": run.started_at.isoformat(),
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "duration_seconds": (
                (run.finished_at - run.started_at).total_seconds()
                if run.finished_at
                else None
            ),
            "phases": phases,
            "error": run.error,
        }

    # -- 1. SEARCH ---------------------------------------------------------
    def _phase_search(self, sources: list[SourceDefinition]) -> dict[str, Any]:
        """Probe every source and record what was actually observed."""
        results: dict[str, Any] = {}
        reachable = 0
        for definition in sources:
            adapter = definition.build()
            verification = self._safe_verify(adapter)
            self._record_source(definition, verification)
            results[definition.id] = {
                "ok": verification.ok,
                "http_status": verification.http_status,
                "rows_total_reported": verification.rows_total_reported,
                "detail": verification.detail,
                "error": verification.error,
                "verified_at": verification.verified_at.isoformat()
                if verification.verified_at
                else None,
            }
            reachable += int(bool(verification.ok))
        self.session.commit()
        return {
            "sources_probed": len(sources),
            "sources_reachable": reachable,
            "sources_unreachable": len(sources) - reachable,
            "detail": results,
        }

    @staticmethod
    def _safe_verify(adapter: Any) -> SourceVerification:
        try:
            return adapter.verify()
        except Exception as exc:  # noqa: BLE001
            return SourceVerification(
                source_id=adapter.source_id,
                ok=False,
                verified_at=datetime.now(UTC),
                error=f"{type(exc).__name__}: {exc}",
            )

    def _record_source(
        self, definition: SourceDefinition, verification: SourceVerification
    ) -> None:
        source = self.session.get(DataSource, definition.id)
        endpoint = (
            definition.config.get("url")
            or definition.config.get("base_url")
            or definition.config.get("hub_url")
            or (
                f"{definition.config['urls'][0]}"
                if definition.config.get("urls")
                else None
            )
        )
        if source is None:
            source = DataSource(
                id=definition.id,
                name=definition.name,
                kind=definition.adapter,
                publisher=definition.publisher,
                endpoint=endpoint,
                schedule=definition.schedule,
                entity_type=definition.entity_type,
            )
            self.session.add(source)
        else:
            source.name = definition.name
            source.kind = definition.adapter
            source.publisher = definition.publisher
            source.endpoint = endpoint
            source.schedule = definition.schedule
            source.entity_type = definition.entity_type

        source.verified_at = verification.verified_at
        source.verified_ok = verification.ok
        source.http_status = verification.http_status
        source.rows_total_reported = verification.rows_total_reported
        source.last_error = verification.error or verification.detail
        self.session.flush()

    # -- 2..6 GATHER / ORGANIZE / PROCESS / VERIFY / SYNTHESIZE ------------
    def _phase_gather(
        self, sources: list[SourceDefinition]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Fetch, deduplicate, normalize and synthesize each source."""
        synthesizer = Synthesizer(self.session)
        gather_detail: dict[str, Any] = {}
        totals = {
            "pages": 0,
            "rows_received": 0,
            "rows_new": 0,
            "rows_duplicate": 0,
            "fetch_failures": 0,
        }

        for definition in sources:
            adapter = definition.build()
            per_source = {
                "pages": 0, "rows_received": 0, "rows_new": 0,
                "rows_duplicate": 0, "errors": [],
            }
            try:
                for page in adapter.fetch(months_back=self.months_back):
                    per_source["pages"] += 1
                    per_source["rows_received"] += len(page.rows)
                    self._log_fetch(page)
                    new_rows = self._store_raw(definition, page, synthesizer)
                    per_source["rows_new"] += new_rows
                    per_source["rows_duplicate"] += len(page.rows) - new_rows
            except Exception as exc:  # noqa: BLE001
                message = f"{type(exc).__name__}: {exc}"
                per_source["errors"].append(message)
                # A gather failure is a real defect, not an expected condition:
                # log the full traceback rather than hiding it behind a warning.
                logger.exception("[%s] gather failed: %s", definition.id, message)

            totals["pages"] += per_source["pages"]
            totals["rows_received"] += per_source["rows_received"]
            totals["rows_new"] += per_source["rows_new"]
            totals["rows_duplicate"] += per_source["rows_duplicate"]
            totals["fetch_failures"] += len(per_source["errors"])

            source = self.session.get(DataSource, definition.id)
            if source is not None:
                source.last_run_at = utcnow()
                source.rows_fetched_last_run = per_source["rows_received"]
                source.rows_new_last_run = per_source["rows_new"]
            gather_detail[definition.id] = per_source

        # ORGANIZE + PROCESS + SYNTHESIZE happen per page inside _store_raw,
        # so the entity counters are complete once gathering returns.
        return totals | {"detail": gather_detail}, dict(synthesizer.stats)

    def _log_fetch(self, page: FetchedRows) -> None:
        self.session.add(
            FetchLog(
                source_id=page.source_id,
                url=page.url,
                http_status=page.http_status,
                ok=True,
                retrieved_at=page.retrieved_at,
                content_sha256=page.content_sha256,
                rows=len(page.rows),
            )
        )

    def _store_raw(
        self,
        definition: SourceDefinition,
        page: FetchedRows,
        synthesizer: Synthesizer,
    ) -> int:
        """Content-address each row, then organize/process/synthesize it."""
        new_rows = 0
        for row in page.rows:
            checksum = _row_checksum(page, row)
            exists = self.session.scalar(
                select(RawRecord.id).where(
                    RawRecord.source_id == page.source_id,
                    RawRecord.content_sha256 == checksum,
                )
            )
            if exists is not None:
                continue

            raw = RawRecord(
                source_id=page.source_id,
                dataset=page.dataset,
                resource_id=page.resource_id,
                external_id=_external_id(row),
                content_sha256=checksum,
                payload=row,
                url=page.landing_page or page.url,
                retrieved_at=page.retrieved_at,
                period=_row_period(row),
            )
            self.session.add(raw)
            self.session.flush()
            new_rows += 1

            # ORGANIZE -> PROCESS -> SYNTHESIZE for this single row.
            single = FetchedRows(
                source_id=page.source_id,
                dataset=page.dataset,
                resource_id=page.resource_id,
                resource_name=page.resource_name,
                rows=[row],
                url=page.url,
                retrieved_at=page.retrieved_at,
                content_sha256=checksum,
                http_status=page.http_status,
                fields=page.fields,
                dataset_title=page.dataset_title,
                dataset_notes=page.dataset_notes,
                publisher=page.publisher,
                landing_page=page.landing_page,
            )
            synthesizer.ingest_page(single, raw, definition.entity_type)

        self.session.flush()
        return new_rows

    # -- 5. VERIFY ---------------------------------------------------------
    def _phase_verify(self) -> dict[str, Any]:
        """Provenance audit over everything currently in the lattice."""
        from sqlalchemy import func

        from app.models import Arrest, Complaint, ForceEvent, Incident, NewsItem

        def _missing_url(model: Any) -> int:
            return int(
                self.session.scalar(
                    select(func.count()).select_from(model).where(
                        (model.source_url.is_(None)) | (model.source_url == "")
                    )
                )
                or 0
            )

        def _missing_sha(model: Any) -> int:
            return int(
                self.session.scalar(
                    select(func.count()).select_from(model).where(
                        (model.content_sha256.is_(None)) | (model.content_sha256 == "")
                    )
                )
                or 0
            )

        counts = {
            "incidents": int(self.session.scalar(select(func.count(Incident.id))) or 0),
            "force_events": int(self.session.scalar(select(func.count(ForceEvent.id))) or 0),
            "arrests": int(self.session.scalar(select(func.count(Arrest.id))) or 0),
            "complaints": int(self.session.scalar(select(func.count(Complaint.id))) or 0),
            "news_items": int(self.session.scalar(select(func.count(NewsItem.id))) or 0),
        }
        provenance = {
            "incidents_missing_source": _missing_url(Incident),
            "arrests_missing_source": _missing_url(Arrest),
            "complaints_missing_source": _missing_url(Complaint),
            "incidents_missing_checksum": _missing_sha(Incident),
            "arrests_missing_checksum": _missing_sha(Arrest),
        }
        total_missing = sum(provenance.values())
        duplicate_checksums = int(
            self.session.scalar(
                select(func.count()).select_from(
                    select(RawRecord.content_sha256)
                    .group_by(RawRecord.source_id, RawRecord.content_sha256)
                    .having(func.count(RawRecord.id) > 1)
                    .subquery()
                )
            )
            or 0
        )
        return {
            "entity_counts": counts,
            "provenance_violations": provenance,
            "provenance_ok": total_missing == 0,
            "duplicate_raw_checksum_groups": duplicate_checksums,
            "verdict": "pass" if total_missing == 0 and duplicate_checksums == 0 else "fail",
        }


def _row_checksum(page: FetchedRows, row: dict[str, Any]) -> str:
    import hashlib

    from app.ingest.parsers import dumps

    body = dumps({"resource": page.resource_id, "row": row})
    return hashlib.sha256(body).hexdigest()


def _external_id(row: dict[str, Any]) -> str | None:
    index = {
        "".join(ch for ch in str(k).upper() if ch.isalnum()): v for k, v in row.items()
    }
    for key in ("INCIDENTNUM", "INCIDENTNUMBER", "CASENUM", "ARRESTNUM", "COMPLAINTNUM", "_ID"):
        value = index.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _row_period(row: dict[str, Any]) -> str | None:
    from app.pipeline.normalize import parse_datetime

    index = {
        "".join(ch for ch in str(k).upper() if ch.isalnum()): v for k, v in row.items()
    }
    for key in ("INCIDENTDATE", "ARRESTDATE", "OCCURREDDATE", "DATE", "PUBLISHEDAT", "PUBDATE"):
        moment = parse_datetime(index.get(key))
        if moment is not None:
            return moment.strftime("%Y-%m")
    return None


def run_pipeline(trigger: str = "manual", *, only: set[str] | None = None) -> dict[str, Any]:
    """Convenience wrapper: run the whole sequence in a fresh session."""
    with SessionLocal() as session:
        return PipelineOrchestrator(session).run(trigger=trigger, only=only)
