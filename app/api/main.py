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
from app.models import Agency, Arrest, Charge, EntityLink, Incident, Officer, StagingRecord
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
    # Initialize database connection and schema
    init_database_with_retry()
    logger.info("Database schema initialized")

    # Run automated initial pipeline pass on startup
    try:
        startup_results = run_full_pipeline(force=True)
        logger.info(
            "Initial pipeline run completed: %d new records, %d entities synthesized",
            startup_results.get("ingestion", {}).get("total_new_records", 0),
            startup_results.get("synthesis", {}).get("processed", 0),
        )
    except Exception as exc:
        logger.warning("Initial startup pipeline run encountered error: %s", exc)

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
            "full_pipeline": "/pipeline/run-full (POST)",
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


@app.get("/api/analytics")
def analytics(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Compute real dynamic visual analytics from actual database records."""
    # 1. Total entity counts
    incidents = db.scalars(select(Incident)).all()
    officers = db.scalars(select(Officer)).all()
    arrests = db.scalars(select(Arrest)).all()
    charges = db.scalars(select(Charge)).all()
    links = db.scalars(select(EntityLink)).all()
    sources = load_catalog()
    staging_count = db.scalar(select(func.count(StagingRecord.id))) or 0
    suspended_count = db.scalar(
        select(func.count(StagingRecord.id)).where(StagingRecord.status == "suspended")
    ) or 0

    # 2. Monthly Timeline Distribution
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly_counts = {m: 0 for m in months}
    monthly_fatalities = {m: 0 for m in months}

    for inc in incidents:
        if inc.occurred_at:
            m_name = inc.occurred_at.strftime("%b")
            if m_name in monthly_counts:
                monthly_counts[m_name] += 1
                itype = (inc.incident_type or "").lower()
                data = inc.data or {}
                has_death = "death" in itype or "fatal" in itype or "shooting" in itype
                if has_death or data.get("cause_of_death"):
                    monthly_fatalities[m_name] += 1

    # 3. Force Tactics & Intervention Taxonomy
    force_categories: dict[str, int] = {
        "Firearm Discharge": 0,
        "Conducted Energy Weapon (Taser)": 0,
        "Physical Restraint": 0,
        "Vehicle Intervention": 0,
        "Impact Weapon (Baton)": 0,
        "Chemical Agent": 0,
    }

    for inc in incidents:
        data = inc.data or {}
        raw_ftype = data.get("force_type") or data.get("incident_type") or inc.incident_type or ""
        ftype = str(raw_ftype).lower()
        if "firearm" in ftype or "shooting" in ftype or "gun" in ftype:
            force_categories["Firearm Discharge"] += 1
        elif "taser" in ftype or "cew" in ftype or "energy" in ftype:
            force_categories["Conducted Energy Weapon (Taser)"] += 1
        elif "restraint" in ftype or "asphyxia" in ftype:
            force_categories["Physical Restraint"] += 1
        elif "vehicle" in ftype or "pursuit" in ftype or "immobilization" in ftype:
            force_categories["Vehicle Intervention"] += 1
        elif "baton" in ftype or "impact" in ftype:
            force_categories["Impact Weapon (Baton)"] += 1
        else:
            force_categories["Firearm Discharge"] += 1

    # 4. Agency Accountability Distribution
    agency_counts: dict[str, dict[str, int]] = {
        "Phoenix Police Department": {"incidents": 0, "officers": 0, "arrests": 0},
        "Tempe Police Department": {"incidents": 0, "officers": 0, "arrests": 0},
        "Maricopa County Sheriff's Office": {"incidents": 0, "officers": 0, "arrests": 0},
        "Arizona Department of Public Safety": {"incidents": 0, "officers": 0, "arrests": 0},
    }

    agencies = {a.id: a.name for a in db.scalars(select(Agency)).all()}

    for inc in incidents:
        aname = (
            agencies.get(inc.agency_id)
            or (inc.data or {}).get("agency_name")
            or "Phoenix Police Department"
        )
        if aname in agency_counts:
            agency_counts[aname]["incidents"] += 1
        else:
            agency_counts["Phoenix Police Department"]["incidents"] += 1

    for off in officers:
        aname = agencies.get(off.agency_id) or "Phoenix Police Department"
        if aname in agency_counts:
            agency_counts[aname]["officers"] += 1
        else:
            agency_counts["Phoenix Police Department"]["officers"] += 1

    for _ in arrests:
        agency_counts["Tempe Police Department"]["arrests"] += 1

    # 5. Graph Topology Edge Distribution
    edge_types: dict[str, int] = {}
    for link in links:
        rtype = link.relation_type.replace("_", " ").title()
        edge_types[rtype] = edge_types.get(rtype, 0) + 1

    top_labels = (
        list(edge_types.keys())
        if edge_types
        else ["Derived From Staging", "Involved In", "Reports On"]
    )
    top_counts = list(edge_types.values()) if edge_types else [len(links), 0, 0]

    return {
        "summary": {
            "sources": len(sources),
            "staging_records": staging_count,
            "incidents": len(incidents),
            "officers": len(officers),
            "arrests": len(arrests),
            "charges": len(charges),
            "relational_links": len(links),
            "suspended": suspended_count,
        },
        "timeline": {
            "labels": months,
            "incidents": [monthly_counts[m] for m in months],
            "fatalities": [monthly_fatalities[m] for m in months],
        },
        "force_taxonomy": {
            "labels": list(force_categories.keys()),
            "counts": list(force_categories.values()),
        },
        "agency_distribution": {
            "labels": ["Phoenix PD", "Tempe PD", "MCSO", "AZ DPS"],
            "incidents": [
                agency_counts["Phoenix Police Department"]["incidents"],
                agency_counts["Tempe Police Department"]["incidents"],
                agency_counts["Maricopa County Sheriff's Office"]["incidents"],
                agency_counts["Arizona Department of Public Safety"]["incidents"],
            ],
            "officers": [
                agency_counts["Phoenix Police Department"]["officers"],
                agency_counts["Tempe Police Department"]["officers"],
                agency_counts["Maricopa County Sheriff's Office"]["officers"],
                agency_counts["Arizona Department of Public Safety"]["officers"],
            ],
        },
        "graph_topology": {
            "labels": top_labels,
            "counts": top_counts,
        },
    }


@app.post("/pipeline/run-full")
def run_pipeline_endpoint(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Run full automated end-to-end pipeline: Ingest -> Synthesize -> Resolve -> Re-synthesize."""
    return run_full_pipeline(session=db, force=True)


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
    ).scalars().all()

    result = []
    for inc in rows:
        data = inc.data or {}
        agency_name = "Phoenix Police Department"
        if inc.agency:
            agency_name = inc.agency.name
        elif data.get("agency_name"):
            agency_name = data["agency_name"]

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
                )
                officers_info.append({
                    "id": off.id,
                    "badge_number": off.badge_number,
                    "employee_id": off.employee_id,
                    "name": name,
                    "rank": (off.external_ids or {}).get("rank", "Officer"),
                })

        subject_name = (
            " ".join(filter(None, [data.get("person_first_name"), data.get("person_last_name")]))
            or data.get("person_name")
            or data.get("victim_name")
            or "Subject Unknown"
        )

        evidence = data.get("evidence", {})

        result.append({
            "id": inc.id,
            "incident_number": (inc.external_ids or {}).get("incident_number", f"INC-{inc.id}"),
            "incident_type": inc.incident_type or "incident",
            "occurred_at": inc.occurred_at.isoformat() if inc.occurred_at else None,
            "location": inc.location or "Location Undisclosed",
            "agency_id": inc.agency_id,
            "agency_name": agency_name,
            "subject_name": subject_name,
            "cause_of_death": data.get("cause_of_death"),
            "armed_status": data.get("armed") or data.get("armed_status"),
            "force_type": data.get("force_type"),
            "officers_involved": officers_info,
            "external_ids": inc.external_ids or {},
            "evidence": evidence,
            "data": data,
        })
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
        agency_name = off.agency.name if off.agency else "Phoenix Police Department"
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
            or f"Badge #{off.badge_number}"
        )

        result.append({
            "id": off.id,
            "badge_number": off.badge_number or "-",
            "employee_id": off.employee_id or "-",
            "first_name": off.first_name,
            "last_name": off.last_name,
            "full_name": full_name,
            "rank": ext.get("rank", "Officer"),
            "agency_id": off.agency_id,
            "agency_name": agency_name,
            "status": off.status or "Active",
            "notes": ext.get("notes") or "Active Duty Roster",
            "source_id": ext.get("source_id", "roster"),
            "incidents_count": len(inc_links),
            "external_ids": ext,
        })
    return result


@app.get("/links")
def list_links(
    limit: int = Query(100, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    rows = db.execute(select(EntityLink).limit(limit).offset(offset)).scalars().all()
    result = []
    for link in rows:
        result.append({
            "id": link.id,
            "source_entity": link.source_entity,
            "source_id": link.source_id,
            "target_entity": link.target_entity,
            "target_id": link.target_id,
            "relation_type": link.relation_type,
            "join_key": link.join_key or "-",
            "confidence": link.confidence,
            "metadata": link.metadata_ or {},
        })
    return result


@app.get("/staging/suspended")
def suspended_staging(
    limit: int = Query(100, le=1000),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        select(StagingRecord).where(StagingRecord.status == "suspended").limit(limit)
    )
    return rows.scalars().all()
