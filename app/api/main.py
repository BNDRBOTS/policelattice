"""Police Lattice API — live pipeline control, month-parity analytics,
immutable archive access, and hybrid retrieval."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.dashboard import get_dashboard_html
from app.db import SessionLocal, init_database_with_retry
from app.models import (
    EntityLink,
    Incident,
    MonthlyArchiveFile,
    Officer,
    PipelineRun,
    StagingRecord,
)
from app.pipeline.resolver import DependencyResolver
from app.pipeline.runner import load_catalog, run_all_sources, run_full_pipeline
from app.pipeline.scheduler import build_scheduler
from app.pipeline.synthesis import SynthesisEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database_with_retry()
    logger.info("Database schema initialized (incl. archive immutability guards)")

    # Automated six-phase pipeline pass on startup (live sources only), run in
    # a daemon thread so the HTTP server binds immediately (Railway cold-start
    # friendly); results are audited in pipeline_runs either way.
    import threading

    def _startup_pipeline() -> None:
        try:
            startup_results = run_full_pipeline(trigger="startup")
            logger.info(
                "Startup pipeline run %s (run id %s)",
                startup_results.get("status"),
                startup_results.get("pipeline_run_id"),
            )
        except Exception as exc:
            logger.warning("Initial startup pipeline run encountered error: %s", exc)

    threading.Thread(target=_startup_pipeline, name="startup-pipeline", daemon=True).start()

    scheduler = build_scheduler()
    scheduler.start()
    logger.info("Scheduler started (15-min due runner + monthly refresh protocol)")
    yield
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Police Lattice API",
    description=(
        "Autonomous six-phase pipeline (Search -> Gather -> Organize -> Process -> "
        "Verify -> Synthesize) for police accountability data. Live external "
        "sources only; immutable monthly chron-archive; officer anomaly detection "
        "with exact statistics; hybrid semantic+lexical+literal retrieval."
    ),
    version="2.0.0",
    lifespan=lifespan,
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
def root_ui() -> HTMLResponse:
    """Serve the interactive web dashboard."""
    return HTMLResponse(content=get_dashboard_html())


@app.get("/api")
def api_directory() -> dict[str, Any]:
    return {
        "name": "Police Lattice API",
        "version": "2.0.0",
        "operation_sequence": ["search", "gather", "organize", "process", "verify", "synthesize"],
        "documentation": {"swagger": "/docs", "redoc": "/redoc"},
        "endpoints": {
            "health": "/health",
            "sources": "/sources",
            "analytics": "/api/analytics?month=YYYY-MM (omit month for live current)",
            "months": "/api/months",
            "anomalies": "/api/analytics/anomalies?month=YYYY-MM",
            "search": "/api/search?q=...&mode=hybrid|lexical|semantic|literal",
            "archive_files": "/api/archive/files?month=YYYY-MM",
            "archive_download": "/api/archive/file/{id}",
            "archive_refresh": "/archive/refresh (POST)",
            "pipeline_runs": "/pipeline/runs",
            "run_full_pipeline": "/pipeline/run-full (POST)",
            "ingest_run": "/ingest/run (POST)",
            "synthesis_run": "/synthesis/run (POST)",
            "resolve_pending": "/resolve/pending (POST)",
            "incidents": "/incidents",
            "officers": "/officers",
            "links": "/links",
            "suspended_staging": "/staging/suspended",
        },
    }


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, Any]:
    staging = db.scalar(select(func.count()).select_from(StagingRecord)) or 0
    raw = db.scalar(select(func.count()).select_from(MonthlyArchiveFile.__table__)) or 0
    return {
        "status": "ok",
        "utc_now": datetime.now(UTC).isoformat(),
        "staging_records": staging,
        "archive_files": raw,
    }


@app.get("/sources")
def list_sources(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    """Catalog sources merged with live registry state (no fabrication)."""
    from app.models import DataSource

    rows = {d.id: d for d in db.scalars(select(DataSource)).all()}
    out: list[dict[str, Any]] = []
    for source_def in load_catalog():
        row = rows.get(source_def["id"])
        cfg = source_def.get("config", {}) or {}
        out.append(
            {
                "id": source_def["id"],
                "name": source_def.get("name"),
                "category": source_def.get("category"),
                "adapter": source_def.get("adapter"),
                "access_mode": source_def.get("access_mode"),
                "schedule": source_def.get("schedule"),
                "enabled": source_def.get("enabled", True),
                "disabled_reason": cfg.get("disabled_reason"),
                "last_run_at": row.last_run_at.isoformat() if row and row.last_run_at else None,
                "last_error": row.last_error if row else None,
                "live_url": cfg.get("url") or cfg.get("urls") or cfg.get("domain"),
                "discovered_datasets": (
                    (row.config or {}).get("discovered") if row else None
                ),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Pipeline control
# ---------------------------------------------------------------------------


@app.post("/pipeline/run-full")
def run_pipeline_endpoint(
    db: Session = Depends(get_db), trigger: str = Query("manual")
) -> dict[str, Any]:
    """Run the full six-phase pipeline: Search->Gather->Organize->Process->Verify->Synthesize."""
    return run_full_pipeline(session=None, force=True, trigger=trigger)


@app.post("/ingest/run")
def run_ingestion() -> dict[str, Any]:
    """Run all enabled live sources now (six-phase orchestrator, manual trigger)."""
    return run_all_sources()


@app.post("/synthesis/run")
def run_synthesis(db: Session = Depends(get_db)) -> dict[str, Any]:
    return SynthesisEngine(db).execute()


@app.post("/resolve/pending")
def resolve_pending(db: Session = Depends(get_db)) -> dict[str, Any]:
    resolver = DependencyResolver(db)
    return {"resolved": resolver.resolve()}


@app.get("/pipeline/runs")
def pipeline_runs(
    limit: int = Query(20, le=200), db: Session = Depends(get_db)
) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(PipelineRun).order_by(PipelineRun.id.desc()).limit(limit)
    ).all()
    return [
        {
            "id": r.id,
            "trigger": r.trigger,
            "status": r.status,
            "phase_order": r.phase_order,
            "started_at": r.started_at.isoformat(),
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "phases": r.phases,
            "error": r.error,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Analytics — active month (live) and historical months (immutable replay)
# ---------------------------------------------------------------------------


@app.get("/api/months")
def list_months(db: Session = Depends(get_db)) -> dict[str, Any]:
    from app.analytics.archive import MonthlyArchiver

    archiver = MonthlyArchiver(db)
    current = datetime.now(UTC).strftime("%Y-%m")
    return {
        "active_month": current,
        "archived_months": archiver.list_months(),
    }


@app.get("/api/analytics")
def analytics(
    month: str | None = Query(None, description="YYYY-MM; omit for live current month"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Canonical analytics payload — identical shape for live and archived months."""
    current = datetime.now(UTC).strftime("%Y-%m")

    if month is None or month == current:
        from app.analytics.engine import AnalyticsEngine

        return AnalyticsEngine(db).compute_month(current)

    if not (len(month) == 7 and month[4] == "-" and month[:4].isdigit() and month[5:].isdigit()):
        raise HTTPException(status_code=400, detail="month must be formatted YYYY-MM")

    from app.analytics.archive import MonthlyArchiver

    archiver = MonthlyArchiver(db)
    payload = archiver.read_analytics_snapshot(month)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No archived chron-log for {month}. Archived months: "
                + ", ".join(m["month"] for m in archiver.list_months())
            ),
        )
    return payload


@app.get("/api/analytics/anomalies")
def anomaly_findings(
    month: str | None = Query(None), db: Session = Depends(get_db)
) -> dict[str, Any]:
    """Officer anomaly findings for a month (live current month when omitted)."""
    from app.models import OfficerAnomalyFinding

    month_key = month or datetime.now(UTC).strftime("%Y-%m")
    rows = db.scalars(
        select(OfficerAnomalyFinding)
        .where(OfficerAnomalyFinding.month_key == month_key)
        .order_by(OfficerAnomalyFinding.bh_q.asc(), OfficerAnomalyFinding.metric_value.desc())
    ).all()
    return {
        "month": month_key,
        "count": len(rows),
        "findings": [
            {
                "id": f.id,
                "officer_label": f.officer_label,
                "agency_name": f.agency_name,
                "badge_number": f.badge_number,
                "metric": f.metric,
                "metric_value": f.metric_value,
                "peer_count": f.peer_count,
                "peer_median": f.peer_median,
                "peer_mean": f.peer_mean,
                "peer_max": f.peer_max,
                "robust_z": f.robust_z,
                "poisson_p": f.poisson_p,
                "bh_q": f.bh_q,
                "tests_run": f.tests_run,
                "window_start": f.window_start.isoformat() if f.window_start else None,
                "window_end": f.window_end.isoformat() if f.window_end else None,
                "narrative": f.narrative,
                "evidence": f.evidence,
            }
            for f in rows
        ],
    }


# ---------------------------------------------------------------------------
# Archive access
# ---------------------------------------------------------------------------


@app.post("/archive/refresh")
def archive_refresh(
    month: str | None = None, db: Session = Depends(get_db)
) -> dict[str, Any]:
    """Manually trigger the monthly chron-archive for a month (defaults to current)."""
    from app.analytics.archive import MonthlyArchiver

    month_key = month or datetime.now(UTC).strftime("%Y-%m")
    archiver = MonthlyArchiver(db)
    result = archiver.archive_month(month_key)
    db.commit()
    return result


@app.get("/api/archive/files")
def archive_files(
    month: str | None = None, db: Session = Depends(get_db)
) -> dict[str, Any]:
    """List immutable archive files (all months, or one month)."""
    stmt = select(MonthlyArchiveFile).order_by(
        MonthlyArchiveFile.month_key.desc(), MonthlyArchiveFile.id
    )
    if month:
        stmt = stmt.where(MonthlyArchiveFile.month_key == month)
    rows = db.scalars(stmt).all()
    return {
        "count": len(rows),
        "files": [
            {
                "id": r.id,
                "month_key": r.month_key,
                "kind": r.kind,
                "filename": r.filename,
                "content_type": r.content_type,
                "sha256": r.sha256,
                "size_bytes": r.size_bytes,
                "record_count": r.record_count,
                "created_at": r.created_at.isoformat(),
                "download_url": f"/api/archive/file/{r.id}",
            }
            for r in rows
        ],
    }


@app.get("/api/archive/file/{file_id}")
def download_archive_file(file_id: int, db: Session = Depends(get_db)) -> Response:
    row = db.get(MonthlyArchiveFile, file_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Archive file {file_id} not found")
    # Integrity verification at read time (immutable file must match its digest).
    import hashlib

    digest = hashlib.sha256(row.payload).hexdigest()
    if digest != row.sha256:
        raise HTTPException(
            status_code=409,
            detail=f"Integrity failure: stored digest {row.sha256} != recomputed {digest}",
        )
    return Response(
        content=row.payload,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{row.filename}"',
            "X-Content-Sha256": row.sha256,
            "X-Immutable": "true",
        },
    )


# ---------------------------------------------------------------------------
# Hybrid retrieval
# ---------------------------------------------------------------------------


@app.get("/api/search")
def search(
    q: str = Query(..., min_length=1),
    mode: str = Query("hybrid", pattern="^(hybrid|lexical|semantic|literal)$"),
    limit: int = Query(25, le=200),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    from app.search.retrieval import HybridRetriever

    return HybridRetriever(db).search(q, limit=limit, mode=mode)


# ---------------------------------------------------------------------------
# Entity listings (exact values only; missing values are labeled, not invented)
# ---------------------------------------------------------------------------


@app.get("/incidents")
def list_incidents(
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(Incident)
        .order_by(Incident.occurred_at.desc().nullslast())
        .limit(limit)
        .offset(offset)
    ).scalars().all()

    result = []
    for inc in rows:
        data = inc.data or {}
        if inc.agency:
            agency_name = inc.agency.name
        elif data.get("agency_name"):
            agency_name = data["agency_name"]
        else:
            agency_name = "Unattributed Agency"

        officer_links = db.execute(
            select(EntityLink).where(
                EntityLink.target_entity == "incident",
                EntityLink.target_id == inc.id,
                EntityLink.relation_type == "involved_in",
            )
        ).scalars().all()

        officers_info = []
        for ol in officer_links:
            off = db.get(Officer, ol.source_id)
            if off:
                name = (
                    " ".join(filter(None, [off.first_name, off.last_name]))
                    or f"Badge #{off.badge_number}"
                    if (off.first_name or off.last_name or off.badge_number)
                    else "Name not recorded"
                )
                officers_info.append(
                    {
                        "id": off.id,
                        "badge_number": off.badge_number,
                        "employee_id": off.employee_id,
                        "name": name,
                        "rank": (off.external_ids or {}).get("rank"),
                    }
                )

        subject_name = (
            " ".join(filter(None, [data.get("person_first_name"), data.get("person_last_name")]))
            or data.get("person_name")
            or data.get("victim_name")
            or "Subject not named in source"
        )

        result.append(
            {
                "id": inc.id,
                "incident_number": (inc.external_ids or {}).get("incident_number"),
                "incident_type": inc.incident_type,
                "occurred_at": inc.occurred_at.isoformat() if inc.occurred_at else None,
                "location": inc.location,
                "agency_id": inc.agency_id,
                "agency_name": agency_name,
                "subject_name": subject_name,
                "cause_of_death": data.get("cause_of_death"),
                "armed_status": data.get("armed") or data.get("armed_status"),
                "force_type": data.get("force_type"),
                "officers_involved": officers_info,
                "external_ids": inc.external_ids or {},
                "data": data,
            }
        )
    return result


@app.get("/officers")
def list_officers(
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(Officer).limit(limit).offset(offset)).scalars().all()
    result = []
    for off in rows:
        agency_name = off.agency.name if off.agency else "Unattributed Agency"
        ext = off.external_ids or {}

        inc_links = db.execute(
            select(EntityLink).where(
                EntityLink.source_entity == "officer",
                EntityLink.source_id == off.id,
                EntityLink.relation_type == "involved_in",
            )
        ).scalars().all()

        full_name = (
            " ".join(filter(None, [off.first_name, off.last_name]))
            or (f"Badge #{off.badge_number}" if off.badge_number else "Name not recorded")
        )

        result.append(
            {
                "id": off.id,
                "badge_number": off.badge_number,
                "employee_id": off.employee_id,
                "first_name": off.first_name,
                "last_name": off.last_name,
                "full_name": full_name,
                "rank": ext.get("rank"),
                "agency_id": off.agency_id,
                "agency_name": agency_name,
                "status": off.status,
                "incidents_count": len(inc_links),
                "external_ids": ext,
            }
        )
    return result


@app.get("/links")
def list_links(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(EntityLink).limit(limit).offset(offset)).scalars().all()
    return [
        {
            "id": link.id,
            "source_entity": link.source_entity,
            "source_id": link.source_id,
            "target_entity": link.target_entity,
            "target_id": link.target_id,
            "relation_type": link.relation_type,
            "join_key": link.join_key,
            "confidence": link.confidence,
            "metadata": link.metadata_ or {},
        }
        for link in rows
    ]


@app.get("/staging/suspended")
def suspended_staging(
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(StagingRecord).where(StagingRecord.status == "suspended").limit(limit)
    ).scalars().all()
    return [
        {
            "id": s.id,
            "source_id": s.source_id,
            "entity_type": s.entity_type,
            "status": s.status,
            "suspension_reason": s.suspension_reason,
            "payload": s.payload,
        }
        for s in rows
    ]
