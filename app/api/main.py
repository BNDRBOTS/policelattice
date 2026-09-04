"""HTTP API and dashboard.

One payload shape serves both the active month and every archived month, so
the frontend can swap periods without re-rendering structure. Month payloads
are cached client-side by ``period + revision``, so switching back to an
already-loaded month costs no request.
"""

from __future__ import annotations

import logging
import re
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.analytics.engine import available_periods, build_view
from app.config import SCHEMA_VERSION, get_settings
from app.db import SessionLocal, ensure_schema_current, utcnow
from app.models import (
    Arrest,
    Complaint,
    DataSource,
    Incident,
    PipelineRun,
    RawRecord,
)
from app.pipeline.anomalies import AnomalyDetector
from app.pipeline.archive import archive_month, archived_periods, load_archived_view
from app.pipeline.orchestrator import PipelineOrchestrator
from app.pipeline.registry import load_catalog
from app.pipeline.retrieval import get_retriever
from app.pipeline.scheduler import build_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)
settings = get_settings()

TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "templates" / "index.html"

_PERIOD_RE = re.compile(r"\d{4}-(?:0[1-9]|1[0-2])")

_startup_thread: threading.Thread | None = None


def _startup_pipeline() -> None:
    """First acquisition pass, run off the request path."""
    try:
        report = PipelineOrchestrator(SessionLocal()).run(trigger="startup")
        with SessionLocal() as session:
            detector = AnomalyDetector(session)
            for period in available_periods(session):
                detector.detect(period)
            session.commit()
            for period in available_periods(session):
                archive_month(session, period)
            session.commit()
        logger.info("Startup pipeline finished: %s", report.get("phases", {}).get("verify"))
    except Exception as exc:  # noqa: BLE001
        logger.error("Startup pipeline failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_schema_current()
    global _startup_thread
    _startup_thread = threading.Thread(target=_startup_pipeline, name="startup-pipeline")
    _startup_thread.start()

    scheduler = build_scheduler() if settings.scheduler_enabled else None
    if scheduler is not None:
        scheduler.start()
        logger.info("Scheduler started (acquisition + monthly refresh)")
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)


app = FastAPI(
    title="Police Lattice API",
    description=(
        "Autonomous acquisition, verification and synthesis of public police "
        "accountability records. Every record carries the URL, retrieval "
        "timestamp and SHA-256 it was derived from."
    ),
    version="3.0.0",
    lifespan=lifespan,
)


def get_db():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# --------------------------------------------------------------------------
# Dashboard
# --------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse(TEMPLATE_PATH.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# State & months
# --------------------------------------------------------------------------

@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, Any]:
    return {
        "status": "ok",
        "schema_version": SCHEMA_VERSION,
        "raw_records": int(db.scalar(select(func.count(RawRecord.id))) or 0),
        "incidents": int(db.scalar(select(func.count(Incident.id))) or 0),
        "sources_verified_ok": int(
            db.scalar(
                select(func.count(DataSource.id)).where(DataSource.verified_ok.is_(True))
            )
            or 0
        ),
        "sources_configured": int(db.scalar(select(func.count(DataSource.id))) or 0),
    }


@app.get("/api/state")
def state(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Bootstrap payload: which months exist, and which are sealed."""
    periods = available_periods(db)
    snapshots = archived_periods(db)
    last_run = db.scalar(
        select(PipelineRun).order_by(PipelineRun.id.desc()).limit(1)
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "periods": periods,
        "current_period": periods[0] if periods else None,
        "archived": snapshots,
        "retrieval": get_retriever().status(),
        "last_run": (
            {
                "id": last_run.id,
                "trigger": last_run.trigger,
                "ok": last_run.ok,
                "started_at": last_run.started_at.isoformat(),
                "finished_at": last_run.finished_at.isoformat() if last_run.finished_at else None,
                "phases": last_run.phases,
                "error": last_run.error,
            }
            if last_run
            else None
        ),
    }


def _view_response(db: Session, period: str | None, revision: int | None) -> dict[str, Any]:
    """Serve a month from the archive when sealed, otherwise compute it live."""
    if period is not None and revision is None:
        archived = load_archived_view(db, period)
        if archived is not None:
            return archived
    return build_view(db, period)


@app.get("/api/month/{period}")
def month(period: str, revision: int | None = None, db: Session = Depends(get_db)):
    """Full, untruncated payload for one month.

    Served from the sealed archive when one exists so that a historical month
    is byte-identical every time it is opened.
    """
    if period != "all" and not _PERIOD_RE.fullmatch(period):
        raise HTTPException(status_code=400, detail="period must be YYYY-MM or 'all'")
    value = None if period == "all" else period
    return JSONResponse(_view_response(db, value, revision))


@app.get("/api/months")
def months(db: Session = Depends(get_db)) -> dict[str, Any]:
    return {
        "periods": available_periods(db),
        "archived": archived_periods(db),
        "retrieved_at": utcnow().isoformat(),
    }


# --------------------------------------------------------------------------
# Records — complete, unredacted
# --------------------------------------------------------------------------

@app.get("/api/incidents")
def incidents(
    period: str | None = None,
    agency_id: str | None = None,
    db: Session = Depends(get_db),
):
    query = select(Incident).order_by(Incident.occurred_at.desc().nullslast())
    if period:
        query = query.where(Incident.period == period)
    if agency_id:
        query = query.where(Incident.agency_id == agency_id)
    rows = db.execute(query).scalars().all()
    return JSONResponse(
        {
            "count": len(rows),
            "truncated": False,
            "records": build_view(db, period)["incidents"]
            if not agency_id
            else [
                r
                for r in build_view(db, period)["incidents"]
                if r["agency_id"] == agency_id
            ],
        }
    )


@app.get("/api/officers")
def officers(period: str | None = None, db: Session = Depends(get_db)):
    view = build_view(db, period)
    return JSONResponse(
        {"count": len(view["officers"]), "truncated": False, "records": view["officers"]}
    )


@app.get("/api/findings")
def findings(
    period: str | None = None,
    severity: str | None = None,
    db: Session = Depends(get_db),
):
    view = build_view(db, period)
    records = view["findings"]
    if severity:
        records = [r for r in records if r["severity"] == severity]
    return JSONResponse(
        {"count": len(records), "truncated": False, "records": records}
    )


@app.get("/api/arrests")
def arrests(period: str | None = None, db: Session = Depends(get_db)):
    query = select(Arrest).order_by(Arrest.occurred_at.desc().nullslast())
    if period:
        query = query.where(Arrest.period == period)
    rows = db.execute(query).scalars().all()
    return JSONResponse(
        {
            "count": len(rows),
            "truncated": False,
            "records": [
                {
                    "id": a.id,
                    "external_number": a.external_number,
                    "agency_id": a.agency_id,
                    "occurred_at": a.occurred_at.isoformat() if a.occurred_at else None,
                    "period": a.period,
                    "charge": a.charge,
                    "charge_code": a.charge_code,
                    "disposition": a.disposition,
                    "subject_gender": a.subject_gender,
                    "subject_race_group": a.subject_race_group,
                    "subject_age_group": a.subject_age_group,
                    "location": a.location,
                    "precinct": a.precinct,
                    "officer_ref_id": a.officer_ref_id,
                    "source_url": a.source_url,
                    "retrieved_at": a.retrieved_at.isoformat(),
                    "content_sha256": a.content_sha256,
                    "source_row": (a.data or {}).get("source_row"),
                }
                for a in rows
            ],
        }
    )


@app.get("/api/complaints")
def complaints(period: str | None = None, db: Session = Depends(get_db)):
    query = select(Complaint).order_by(Complaint.occurred_at.desc().nullslast())
    if period:
        query = query.where(Complaint.period == period)
    rows = db.execute(query).scalars().all()
    return JSONResponse(
        {
            "count": len(rows),
            "truncated": False,
            "records": [
                {
                    "id": c.id,
                    "external_number": c.external_number,
                    "agency_id": c.agency_id,
                    "category": c.category,
                    "allegation": c.allegation,
                    "finding": c.finding,
                    "discipline": c.discipline,
                    "status": c.status,
                    "amount_paid": c.amount_paid,
                    "occurred_at": c.occurred_at.isoformat() if c.occurred_at else None,
                    "period": c.period,
                    "officer_ref_id": c.officer_ref_id,
                    "source_url": c.source_url,
                    "retrieved_at": c.retrieved_at.isoformat(),
                    "content_sha256": c.content_sha256,
                    "source_row": (c.data or {}).get("source_row"),
                }
                for c in rows
            ],
        }
    )


@app.get("/api/news")
def news(period: str | None = None, db: Session = Depends(get_db)):
    view = build_view(db, period)
    return JSONResponse(
        {"count": len(view["news"]), "truncated": False, "records": view["news"]}
    )


# --------------------------------------------------------------------------
# Sources, search, operations
# --------------------------------------------------------------------------

@app.get("/api/sources")
def sources(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Catalog plus the real result of the last verification pass."""
    catalog = load_catalog()
    rows = {s.id: s for s in db.execute(select(DataSource)).scalars().all()}
    records = []
    for definition in catalog:
        observed = rows.get(definition.id)
        records.append(
            {
                "id": definition.id,
                "name": definition.name,
                "adapter": definition.adapter,
                "entity_type": definition.entity_type,
                "publisher": definition.publisher,
                "schedule": definition.schedule,
                "endpoint": (
                    definition.config.get("url")
                    or definition.config.get("base_url")
                    or definition.config.get("hub_url")
                    or (definition.config["urls"][0] if definition.config.get("urls") else None)
                ),
                "packages": definition.config.get("packages"),
                "catalog_verified": definition.verified,
                "catalog_verified_on": definition.verified_on,
                "catalog_verified_detail": definition.verified_detail,
                "verified_ok": bool(observed.verified_ok) if observed else None,
                "verified_at": (
                    observed.verified_at.isoformat()
                    if observed and observed.verified_at
                    else None
                ),
                "http_status": observed.http_status if observed else None,
                "rows_total_reported": observed.rows_total_reported if observed else None,
                "rows_fetched_last_run": observed.rows_fetched_last_run if observed else 0,
                "rows_new_last_run": observed.rows_new_last_run if observed else 0,
                "detail": observed.last_error if observed else "not yet probed",
            }
        )
    return {
        "count": len(records),
        "reachable": sum(1 for r in records if r["verified_ok"]),
        "records": records,
    }


@app.get("/api/search")
def search(q: str, k: int = Query(25, ge=1, le=200), period: str | None = None,
           db: Session = Depends(get_db)):
    if not q.strip():
        raise HTTPException(status_code=400, detail="q must not be empty")
    retriever = get_retriever()
    retriever.build(db, period)
    return retriever.search(q, k=k, period=period)


@app.post("/api/pipeline/run")
def run_pipeline_endpoint(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Run Search -> Gather -> Organize -> Process -> Verify -> Synthesize."""
    report = PipelineOrchestrator(db).run(trigger="api")
    detector = AnomalyDetector(db)
    report["anomaly_detection"] = [
        detector.detect(period) for period in available_periods(db)
    ]
    db.commit()
    return report


@app.post("/api/archive/refresh")
def refresh_archive_endpoint(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Recompute findings and seal every month present in the lattice."""
    from app.pipeline.archive import refresh_archive

    detector = AnomalyDetector(db)
    periods = available_periods(db)
    anomaly_reports = [detector.detect(period) for period in periods]
    db.commit()
    return {
        "anomaly_detection": anomaly_reports,
        "archive": refresh_archive(db),
    }


@app.post("/api/archive/{period}")
def archive_period(period: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    if not _PERIOD_RE.fullmatch(period):
        raise HTTPException(status_code=400, detail="period must be YYYY-MM")
    result = archive_month(db, period)
    db.commit()
    return result


@app.get("/api/runs")
def runs(limit: int = Query(20, ge=1, le=200), db: Session = Depends(get_db)):
    rows = db.execute(
        select(PipelineRun).order_by(PipelineRun.id.desc()).limit(limit)
    ).scalars().all()
    return {
        "count": len(rows),
        "records": [
            {
                "id": r.id,
                "trigger": r.trigger,
                "ok": r.ok,
                "started_at": r.started_at.isoformat(),
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "phases": r.phases,
                "error": r.error,
            }
            for r in rows
        ],
    }
