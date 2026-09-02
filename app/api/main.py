from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.dashboard import get_dashboard_html
from app.db import SessionLocal, init_database_with_retry
from app.models import EntityLink, Incident, Officer, StagingRecord
from app.pipeline.resolver import DependencyResolver
from app.pipeline.runner import load_catalog, run_all_sources
from app.pipeline.scheduler import build_scheduler
from app.pipeline.synthesis import SynthesisEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database connection and schema
    init_database_with_retry()
    scheduler = build_scheduler()
    scheduler.start()
    logger.info("Scheduler started successfully")
    yield
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Police Lattice API",
    description="Topological ingestion and synthesis lattice for police accountability data",
    version="0.1.0",
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
    """Serve the interactive web UI dashboard."""
    return HTMLResponse(content=get_dashboard_html())


@app.get("/api")
def api_directory() -> dict[str, Any]:
    """API metadata and endpoints directory."""
    return {
        "name": "Police Lattice API",
        "version": "0.1.0",
        "status": "online",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json",
        },
        "endpoints": {
            "dashboard_ui": "/",
            "health": "/health",
            "sources": "/sources",
            "ingest_run": "/ingest/run (POST)",
            "synthesis_run": "/synthesis/run (POST)",
            "resolve_pending": "/resolve/pending (POST)",
            "incidents": "/incidents",
            "officers": "/officers",
            "links": "/links",
            "suspended_staging": "/staging/suspended",
        },
    }


@app.get("/sources")
def list_sources() -> list[dict[str, Any]]:
    """List all configured data sources in the catalog."""
    return load_catalog()


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, Any]:
    count = db.scalar(select(func.count()).select_from(StagingRecord))
    return {"status": "ok", "staging_records": count or 0}


@app.post("/ingest/run")
def run_ingestion() -> dict[str, Any]:
    """Run all sources now (manual trigger)."""
    return run_all_sources()


@app.post("/synthesis/run")
def run_synthesis(db: Session = Depends(get_db)) -> dict[str, Any]:
    synthesis_engine = SynthesisEngine(db)
    return synthesis_engine.execute()


@app.post("/resolve/pending")
def resolve_pending(db: Session = Depends(get_db)) -> dict[str, Any]:
    resolver = DependencyResolver(db)
    count = resolver.resolve()
    return {"resolved": count}


@app.get("/incidents")
def list_incidents(
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(Incident).order_by(Incident.occurred_at.desc()).limit(limit).offset(offset)
    )
    return rows.scalars().all()


@app.get("/officers")
def list_officers(
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(Officer).limit(limit).offset(offset))
    return rows.scalars().all()


@app.get("/links")
def list_links(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(EntityLink).limit(limit).offset(offset))
    return rows.scalars().all()


@app.get("/staging/suspended")
def suspended_staging(
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(StagingRecord).where(StagingRecord.status == "suspended").limit(limit)
    )
    return rows.scalars().all()
