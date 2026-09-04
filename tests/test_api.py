from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.main import app, get_db
from app.db import Base
from app.models import Incident, Officer, StagingRecord

# Setup SQLite test DB with StaticPool so all sessions share the in-memory schema
engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base.metadata.create_all(bind=engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)


def test_root_ui_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Police Lattice" in response.text
    assert "Sources Catalog" in response.text
    assert "Run Full Pipeline" in response.text


def test_analytics_endpoint():
    response = client.get("/api/analytics")
    assert response.status_code == 200
    data = response.json()
    assert "summary" in data
    assert "timeline" in data
    assert "force_taxonomy" in data
    assert "agency_distribution" in data
    assert "graph_topology" in data
    assert data["summary"]["sources"] >= 60


def test_sources_endpoint():
    response = client.get("/sources")
    assert response.status_code == 200
    sources = response.json()
    assert isinstance(sources, list)
    assert len(sources) >= 60


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "staging_records" in data


def test_incidents_officers_links_endpoints():
    db = TestingSessionLocal()
    off = Officer(badge_number="B101", first_name="Officer", last_name="Friendly")
    inc = Incident(incident_type="traffic", location="Central & Camelback")
    st = StagingRecord(
        raw_record_id=1,
        source_id="src1",
        entity_type="test",
        payload={"a": 1},
        record_hash="hash_s",
        status="suspended",
    )
    db.add_all([off, inc, st])
    db.commit()
    db.close()

    res_inc = client.get("/incidents")
    assert res_inc.status_code == 200
    assert len(res_inc.json()) >= 1

    res_off = client.get("/officers")
    assert res_off.status_code == 200
    assert len(res_off.json()) >= 1

    res_links = client.get("/links")
    assert res_links.status_code == 200

    res_susp = client.get("/staging/suspended")
    assert res_susp.status_code == 200
    assert len(res_susp.json()) >= 1


def test_ingest_and_synthesis_api_routes():
    with patch("app.api.main.run_all_sources", return_value={"test_source": 5}):
        res = client.post("/ingest/run")
        assert res.status_code == 200
        assert res.json() == {"test_source": 5}

    res_syn = client.post("/synthesis/run")
    assert res_syn.status_code == 200
    assert "processed" in res_syn.json()

    res_res = client.post("/resolve/pending")
    assert res_res.status_code == 200
    assert "resolved" in res_res.json()


def test_pipeline_run_full_endpoint():
    with patch(
        "app.api.main.run_full_pipeline",
        return_value={
            "status": "success",
            "ingestion": {"sources_run": 68, "total_new_records": 10},
            "synthesis": {"processed": 10, "suspended": 0, "failed": 0},
            "resolved_dependencies": 2,
            "entity_counts": {"incidents": 5, "officers": 2},
        },
    ):
        res = client.post("/pipeline/run-full")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "success"
        assert data["ingestion"]["total_new_records"] == 10
        assert data["synthesis"]["processed"] == 10
