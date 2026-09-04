from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.main import app, get_db
from app.db import Base
from app.models import (
    Agency,
    EntityLink,
    Incident,
    MonthlyArchiveFile,
    Officer,
    OfficerAnomalyFinding,
)

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


def _seed(db):
    agency = Agency(name="Phoenix Police Department", state="AZ")
    db.add(agency)
    db.flush()
    off = Officer(agency_id=agency.id, first_name="John", last_name="Smith", badge_number="1042")
    db.add(off)
    db.flush()
    inc = Incident(
        agency_id=agency.id,
        incident_type="use_of_force",
        occurred_at=datetime(2026, 9, 2, tzinfo=UTC),
        location="100 Main St",
        external_ids={"incident_number": "PHX-1"},
        data={"force_type": "physical restraint", "agency_name": "Phoenix Police Department"},
    )
    db.add(inc)
    db.flush()
    db.add(
        EntityLink(
            source_entity="officer",
            source_id=off.id,
            target_entity="incident",
            target_id=inc.id,
            relation_type="involved_in",
        )
    )
    db.add(
        OfficerAnomalyFinding(
            month_key="2026-09",
            officer_id=off.id,
            officer_label="John Smith",
            agency_name="Phoenix Police Department",
            badge_number="1042",
            metric="use_of_force_events",
            metric_value=9,
            peer_count=8,
            peer_median=2.0,
            peer_mad=1.0,
            peer_mean=2.4,
            peer_max=4,
            ratio_to_median=4.5,
            robust_z=4.7,
            poisson_p=0.0001,
            bh_q=0.001,
            tests_run=10,
            narrative="John Smith is recorded with 9 use-of-force events.",
            evidence=[{"incident_id": inc.id}],
        )
    )
    db.commit()
    return off, inc


def _clean(db):
    from app.models import Charge, CourtCase, Document, NewsArticle, Person

    for model in (
        OfficerAnomalyFinding, EntityLink, Incident, Charge, CourtCase,
        Document, NewsArticle, Person, Officer, Agency, MonthlyArchiveFile,
    ):
        db.query(model).delete()
    db.commit()


def test_root_ui_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Police Lattice" in response.text


def test_analytics_endpoint_shape_and_no_fabrication():
    db = next(override_get_db())
    try:
        _clean(db)
        _seed(db)
        response = client.get("/api/analytics")
        assert response.status_code == 200
        data = response.json()
        for key in (
            "summary", "timeline", "force_taxonomy", "agency_distribution",
            "incident_types", "source_provenance", "graph_topology",
            "officer_metrics", "anomaly_findings",
        ):
            assert key in data, key
        # exact live values, not fabricated defaults
        assert data["summary"]["incidents"] == 1
        assert data["agency_distribution"]["labels"] == ["Phoenix Police Department"]
        assert data["force_taxonomy"]["labels"][-1].startswith("Unclassified")
        # no invented agencies in any payload string
        assert "Tempe Police Department" not in json.dumps(data)
    finally:
        db.close()


def test_months_and_archived_parity():
    db = next(override_get_db())
    try:
        _clean(db)
        _seed(db)
        # archive the (empty-for-history) current month through the API
        response = client.post("/archive/refresh")
        assert response.status_code == 200
        files = client.get("/api/archive/files").json()
        assert files["count"] >= 5  # five discrete kinds
        kinds = {f["kind"] for f in files["files"]}
        assert {
            "raw_records", "staging_records", "entities", "analytics_snapshot", "anomaly_findings"
        } <= kinds

        months = client.get("/api/months").json()
        assert months["archived_months"], months

        # Archive an explicit PAST month; historical months replay from the
        # immutable chron-log (the current month is always served live).
        past = "2026-08" if months["active_month"] != "2026-08" else "2026-07"
        assert client.post("/archive/refresh", params={"month": past}).status_code == 200

        live = client.get("/api/analytics").json()
        archived = client.get(f"/api/analytics?month={past}").json()

        # EXACT parity: archived payload is the same canonical shape
        # (deterministic snapshots omit volatile generated_at by design)
        assert archived["mode"] == "archived"
        assert set(archived) - {"mode", "archive", "plain_language_summary"} >= set(live) - {
            "mode", "generated_at"
        }
        assert archived["month"] == past
        assert "archive" in archived and len(archived["archive"]["sha256"]) == 64

        # unarchived month returns explicit 404 (never fabricated fallback)
        missing = client.get("/api/analytics?month=2001-01")
        assert missing.status_code == 404
    finally:
        db.close()


def test_anomalies_endpoint():
    db = next(override_get_db())
    try:
        _clean(db)
        off, _ = _seed(db)
        response = client.get("/api/analytics/anomalies")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        f = data["findings"][0]
        assert f["officer_label"] == "John Smith"
        assert f["bh_q"] == 0.001
        assert "narrative" in f and "evidence" in f
    finally:
        db.close()


def test_incident_listing_labels_missing_data():
    db = next(override_get_db())
    try:
        _clean(db)
        _seed(db)
        rows = client.get("/incidents").json()
        assert rows[0]["incident_number"] == "PHX-1"
        assert rows[0]["officers_involved"][0]["name"] == "John Smith"
    finally:
        db.close()


def test_archive_download_verifies_integrity():
    db = next(override_get_db())
    try:
        _clean(db)
        _seed(db)
        client.post("/archive/refresh")
        files = client.get("/api/archive/files").json()["files"]
        file_id = files[0]["id"]
        r = client.get(f"/api/archive/file/{file_id}")
        assert r.status_code == 200
        assert r.headers["X-Immutable"] == "true"
        assert len(r.headers["X-Content-Sha256"]) == 64
    finally:
        db.close()


def test_search_endpoint_offline():
    """Search works with lexical+literal even when the semantic model
    cannot be downloaded (sandboxed/offline)."""
    db = next(override_get_db())
    try:
        _clean(db)
        _seed(db)
        r = client.get("/api/search", params={"q": "John Smith", "mode": "literal"})
        assert r.status_code == 200
        data = r.json()
        assert data["corpus_size"] >= 2
        assert any(h["entity_type"] == "officer" for h in data["results"])

        r2 = client.get("/api/search", params={"q": "restraint", "mode": "lexical"})
        assert r2.status_code == 200
        assert any(h["entity_type"] == "incident" for h in r2.json()["results"])
    finally:
        db.close()
