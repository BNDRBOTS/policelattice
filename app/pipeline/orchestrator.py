"""Pipeline orchestrator — enforces the mandated operation sequence.

    SEARCH -> GATHER -> ORGANIZE -> PROCESS -> VERIFY -> SYNTHESIZE

Absolute adherence is structural: each phase executes exactly once, in order,
inside one audited ``pipeline_runs`` row. A phase failure aborts the run with
the error recorded — no phase is skipped, reordered, or silently retried with
substituted data.

Synthesize additionally runs the analytics engine, officer anomaly detection,
the monthly chron-archive protocol, and bumps the hybrid retrieval index.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import SessionLocal
from app.models import (
    Agency,
    Arrest,
    Charge,
    CourtCase,
    Document,
    EntityLink,
    Incident,
    NewsArticle,
    Officer,
    PipelineRun,
    RawRecord,
    StagingRecord,
    SurveillanceEvent,
)
from app.pipeline.runner import (
    _is_source_due,
    gather_source,
    get_source_by_id,
    load_catalog,
    organize_raw_record,
    process_staging_record,
)
from app.pipeline.search import SearchPhase
from app.pipeline.verification import VerificationPhase

logger = logging.getLogger(__name__)

PHASE_ORDER = ["search", "gather", "organize", "process", "verify", "synthesize"]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class PipelineOrchestrator:
    """Runs the full six-phase pipeline with complete audit logging."""

    def __init__(self) -> None:
        self.settings = get_settings()

    # ------------------------------------------------------------------ #

    def run(
        self,
        session: Session | None = None,
        trigger: str = "manual",
        force: bool = False,
        due_only: bool = False,
    ) -> dict[str, Any]:
        if session is not None:
            return self._run(session, trigger, force, due_only)
        with SessionLocal() as s:
            return self._run(s, trigger, force, due_only)

    def _run(self, session: Session, trigger: str, force: bool, due_only: bool) -> dict[str, Any]:
        run = PipelineRun(
            trigger=trigger,
            started_at=_utcnow(),
            status="running",
            phase_order=PHASE_ORDER,
            phases={},
        )
        session.add(run)
        session.flush()

        try:
            catalog = load_catalog()
            now = _utcnow()

            # ---------------- 1. SEARCH ----------------
            search_stats = SearchPhase(session).execute(catalog)
            run.phases = {**run.phases, "search": search_stats}
            session.commit()

            # Select sources for this run.
            if due_only:
                selected: list[dict[str, Any]] = []
                for source_def in catalog:
                    if not source_def.get("enabled", True):
                        continue
                    if not source_def.get("schedule"):
                        continue
                    row = get_source_by_id(session, source_def["id"])
                    if _is_source_due(source_def["schedule"], row.last_run_at, now):
                        selected.append(source_def)
            else:
                selected = [s for s in catalog if s.get("enabled", True)]
            # Re-merge discovery results resolved this run.
            from app.pipeline.runner import get_merged_source_def

            merged_sources = []
            for source_def in selected:
                merged = get_merged_source_def(session, source_def["id"])
                merged_sources.append(merged if merged else source_def)

            # ---------------- 2. GATHER ----------------
            gather_stats: dict[str, Any] = {
                "started_at": _utcnow().isoformat(),
                "sources": {},
                "unreachable": [],
            }
            gathered: list[tuple[dict[str, Any], Any, Any]] = []
            for source_def in merged_sources:
                try:
                    result = gather_source(session, source_def)
                    gather_stats["sources"][source_def["id"]] = {
                        "fetched": result["fetched"],
                        "new_raw_records": result["new_raw_records"],
                        "duplicates_skipped": result["duplicates_skipped"],
                        "skip_reasons": result.get("skip_reasons", []),
                    }
                    gathered.append((source_def, result))
                except Exception as exc:
                    row = get_source_by_id(session, source_def["id"])
                    row.last_error = str(exc)
                    session.commit()
                    gather_stats["sources"][source_def["id"]] = {"error": str(exc)}
                    gather_stats["unreachable"].append(
                        {"source_id": source_def["id"], "error": str(exc)}
                    )
                    logger.error("[gather] %s failed: %s", source_def["id"], exc)
            gather_stats["completed_at"] = _utcnow().isoformat()
            run.phases = {**run.phases, "gather": gather_stats}
            session.commit()

            # ---------------- 3. ORGANIZE ----------------
            organize_stats: dict[str, Any] = {
                "started_at": _utcnow().isoformat(),
                "staging_created": 0,
                "by_entity_type": {},
            }
            staged: list[tuple[dict[str, Any], Any]] = []
            for source_def, result in gathered:
                for raw, dto in result["records"]:
                    staging = organize_raw_record(session, source_def, raw, dto)
                    staged.append((source_def, staging))
                    organize_stats["staging_created"] += 1
                    organize_stats["by_entity_type"][staging.entity_type] = (
                        organize_stats["by_entity_type"].get(staging.entity_type, 0) + 1
                    )
            session.commit()
            organize_stats["completed_at"] = _utcnow().isoformat()
            run.phases = {**run.phases, "organize": organize_stats}
            session.commit()

            # ---------------- 4. PROCESS ----------------
            process_stats: dict[str, Any] = {
                "started_at": _utcnow().isoformat(),
                "processed": 0,
                "evidence_fields": 0,
            }
            for _source_def, staging in staged:
                process_staging_record(staging)
                process_stats["processed"] += 1
                ev = (staging.payload or {}).get("evidence") or {}
                process_stats["evidence_fields"] += sum(
                    len(v) if isinstance(v, list) else 1 for v in ev.values()
                )
            session.commit()
            process_stats["completed_at"] = _utcnow().isoformat()
            run.phases = {**run.phases, "process": process_stats}
            session.commit()

            # ---------------- 5. VERIFY ----------------
            verifier = VerificationPhase(session)
            new_staging_ids = [s.id for _sd, s in staged]
            record_verification = verifier.verify_records(new_staging_ids)
            source_staging: dict[str, list[int]] = {}
            source_defs_by_id = {sd["id"]: sd for sd in merged_sources}
            for sd, s in staged:
                source_staging.setdefault(sd["id"], []).append(s.id)
            revalidation = (
                verifier.revalidate_against_live_sources(source_staging, source_defs_by_id)
                if new_staging_ids
                else {}
            )
            verify_stats = {
                "started_at": record_verification["started_at"],
                "record_checks": record_verification,
                "external_revalidation": revalidation,
                "completed_at": _utcnow().isoformat(),
            }
            run.phases = {**run.phases, "verify": verify_stats}
            session.commit()

            # ---------------- 6. SYNTHESIZE ----------------
            from app.pipeline.resolver import DependencyResolver
            from app.pipeline.synthesis import SynthesisEngine

            session.expire_all()
            engine = SynthesisEngine(session)
            synth_stats = engine.execute()

            resolver = DependencyResolver(session)
            resolved_count = resolver.resolve()
            if resolved_count > 0:
                engine2 = SynthesisEngine(session)
                synth_stats2 = engine2.execute()
                synth_stats["processed"] += synth_stats2.get("processed", 0)
                synth_stats["suspended"] = synth_stats2.get("suspended", 0)
                synth_stats["failed"] += synth_stats2.get("failed", 0)

            # Analytics + officer anomaly detection for the current month.
            # (The live canonical analytics payload is computed on demand by
            # the API layer; the archive step below snapshots it immutably.)
            month_key = _utcnow().strftime("%Y-%m")

            from app.analytics.anomalies import OfficerAnomalyDetector

            anomaly_stats = OfficerAnomalyDetector(session).compute_and_persist(month_key)

            # Monthly chron-archive protocol.
            archive_stats: dict[str, Any] = {"enabled": self.settings.archive_enabled}
            if self.settings.archive_enabled:
                from app.analytics.archive import MonthlyArchiver

                archiver = MonthlyArchiver(session)
                archive_stats = archiver.archive_month(
                    month_key, pipeline_run_id=run.id
                )
                # Finalize the previous month if it has no finalized archive yet.
                prev = archiver.previous_month_key(month_key)
                if prev and not archiver.is_finalized(prev):
                    archive_stats["finalized_previous_month"] = archiver.archive_month(
                        prev, pipeline_run_id=run.id, finalize=True
                    )

            # Bump the hybrid retrieval index.
            from app.search.retrieval import bump_index_version

            bump_index_version()

            counts = self._get_counts(session)
            synth_phase = {
                "started_at": _utcnow().isoformat(),
                "synthesis": synth_stats,
                "resolved_dependencies": resolved_count,
                "analytics_month": month_key,
                "anomalies": anomaly_stats,
                "archive": archive_stats,
                "entity_counts": counts,
                "completed_at": _utcnow().isoformat(),
            }
            run.phases = {**run.phases, "synthesize": synth_phase}
            run.status = "success"
            run.completed_at = _utcnow()
            session.commit()

            return {
                "status": "success",
                "pipeline_run_id": run.id,
                "trigger": trigger,
                "phases": run.phases,
                "entity_counts": counts,
            }

        except Exception as exc:
            session.rollback()
            logger.exception("Pipeline run failed")
            run = session.get(PipelineRun, run.id)
            if run is not None:
                run.status = "failed"
                run.error = str(exc)
                run.completed_at = _utcnow()
                session.commit()
            return {
                "status": "failed",
                "pipeline_run_id": run.id if run else None,
                "error": str(exc),
            }

    def _get_counts(self, s: Session) -> dict[str, int]:
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
