from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import Base, engine, SessionLocal
from app.models import EntityLink, Incident, Officer, StagingRecord
from app.pipeline.resolver import DependencyResolver
from app.pipeline.runner import run_all_due
from app.pipeline.scheduler import build_scheduler
from app.pipeline.synthesis import SynthesisEngine


settings = get_settings()
logging.basicConfig(level=settings.log_level.upper())
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    scheduler = build_scheduler()
    scheduler.start()
    logger.info("Scheduler started")
    yield
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(title="Police Lattice", version="0.1.0", lifespan=lifespan)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/health")
def health(db: Session = Depends(get_db)) -> dict[str, Any]:
    count = db.scalar(select(func.count()).select_from(StagingRecord))
    return {"status": "ok", "staging_records": count}


@app.post("/ingest/run")
def run_ingestion() -> dict[str, Any]:
    """Run all due sources now."""
    return run_all_due()


@app.post("/synthesis/run")
def run_synthesis(db: Session = Depends(get_db)) -> dict[str, Any]:
    engine = SynthesisEngine(db)
    return engine.run()


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
    rows = db.execute(select(Incident).order_by(Incident.occurred_at.desc()).limit(limit).offset(offset)).scalars().all()
    return rows


@app.get("/officers")
def list_officers(
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(Officer).limit(limit).offset(offset)).scalars().all()
    return rows


@app.get("/links")
def list_links(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(EntityLink).limit(limit).offset(offset)).scalars().all()
    return rows


@app.get("/staging/suspended")
def suspended_staging(
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(StagingRecord).where(StagingRecord.status == "suspended").limit(limit)
    ).scalars().all()
    return rows
