"""End-to-end transport and pipeline test over real HTTP.

These tests open a real TCP socket, run the real ``httpx`` client, the real
orjson/CKAN decoding and the real synthesis code, and assert on what actually
landed in the database. Nothing is stubbed inside the application.
"""

from __future__ import annotations

from sqlalchemy import func, select

from app.ingest.ckan import CkanAdapter
from app.models import (
    EntityLink,
    ForceEvent,
    Incident,
    OfficerRef,
    PipelineRun,
    RawRecord,
)
from app.pipeline.orchestrator import PipelineOrchestrator
from app.pipeline.registry import SourceDefinition

RESOURCE_ID = "eddd36ce-c425-4e78-aa40-5dac5367a3b2"


def _adapter(base_url: str) -> CkanAdapter:
    return CkanAdapter(
        "phoenix_ckan_uof",
        "Phoenix PD — Officer Use of Force (test)",
        {"base_url": base_url, "packages": ["uof"], "organization": "police-department"},
    )


def test_verify_reports_the_real_row_total(ckan_server):
    verification = _adapter(ckan_server).verify()
    assert verification.ok is True
    assert verification.http_status == 200
    # 4884 is the total the live Phoenix endpoint reported on 2026-09-03
    assert verification.rows_total_reported == 4884
    assert verification.verified_at is not None


def test_verify_reports_failure_honestly(make_ckan_server):
    dead = "http://127.0.0.1:1"
    verification = _adapter(dead).verify()
    assert verification.ok is False
    assert verification.error
    assert verification.rows_total_reported is None


def test_fetch_streams_rows_with_citation(ckan_server):
    pages = list(_adapter(ckan_server).fetch())
    assert pages, "expected at least one page"
    page = pages[0]
    assert page.resource_id == RESOURCE_ID
    assert page.http_status == 200
    assert page.content_sha256
    assert page.retrieved_at is not None
    assert page.landing_page.endswith(f"/dataset/uof/resource/{RESOURCE_ID}")
    assert len(page.rows) == 2
    assert page.rows[0]["UNIQUE_INCIDENT_OFFICER"] == "20260007749011083"
    # field metadata came from the service, not from a hardcoded schema
    assert any(f["id"] == "EMP_WITHIN_POLICY" for f in page.fields)


def test_full_pipeline_writes_provenanced_entities(make_ckan_server, memory_session, monkeypatch):
    """Run all six phases against the live-shaped server and audit the result."""
    base_url = make_ckan_server("ckan_datastore_uof_volume.json")
    definition = SourceDefinition(
        id="phoenix_ckan_uof",
        name="Phoenix PD — Officer Use of Force (test)",
        adapter="ckan",
        entity_type="use_of_force",
        publisher="Phoenix Police Department",
        verified="test",
        config={
            "base_url": base_url,
            "packages": ["uof"],
            "organization": "police-department",
        },
    )
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: [definition])

    report = PipelineOrchestrator(memory_session).run(trigger="test")

    # --- phase evidence -------------------------------------------------
    assert report["ok"] is True
    phases = report["phases"]
    assert phases["search"]["sources_probed"] == 1
    assert phases["search"]["sources_reachable"] == 1
    assert phases["gather"]["rows_received"] == 353
    assert phases["gather"]["rows_new"] == 353
    assert phases["verify"]["verdict"] == "pass", phases["verify"]

    # --- what actually landed -------------------------------------------
    assert memory_session.scalar(select(func.count(RawRecord.id))) == 353
    incidents = memory_session.scalars(select(Incident)).all()
    assert len(incidents) == 353

    # every incident carries a real, clickable citation and a checksum
    for incident in incidents:
        assert incident.source_url == (
            f"{base_url}/dataset/uof/resource/{RESOURCE_ID}"
        )
        assert len(incident.content_sha256) == 64
        assert incident.retrieved_at is not None
        assert incident.agency_id == "phoenix-pd"
        assert incident.period and incident.period.startswith("2025-")

    # officers are keyed by the source's own identifier
    officers = memory_session.scalars(select(OfficerRef)).all()
    assert len(officers) == 60
    assert all(o.external_key.startswith("SYNTH") for o in officers)
    assert all(o.agency_id == "phoenix-pd" for o in officers)
    assert all(o.gender in {"Male", "Female"} for o in officers)

    # officer x incident edges carry the policy outcome verbatim
    events = memory_session.scalars(select(ForceEvent)).all()
    assert len(events) == 353
    assert {e.within_policy for e in events} <= {"Yes", "No"}
    assert {e.bwc_activated for e in events} <= {"Yes", "No"}

    links = memory_session.scalars(select(EntityLink)).all()
    assert {link.relation for link in links} == {"involved_in", "derived_from"}
    assert memory_session.scalar(select(func.count(PipelineRun.id))) == 1


def test_rerun_is_idempotent(make_ckan_server, memory_session, monkeypatch):
    base_url = make_ckan_server("ckan_datastore_uof_officer_summary.json")
    definition = SourceDefinition(
        id="phoenix_ckan_uof",
        name="test",
        adapter="ckan",
        entity_type="use_of_force",
        config={"base_url": base_url, "packages": ["uof"], "organization": "police-department"},
    )
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: [definition])

    first = PipelineOrchestrator(memory_session).run(trigger="test")
    assert first["phases"]["gather"]["rows_new"] == 2

    second = PipelineOrchestrator(memory_session).run(trigger="test")
    assert second["phases"]["gather"]["rows_new"] == 0
    assert second["phases"]["gather"]["rows_duplicate"] == 2
    # content addressing means no duplicate raw rows and no duplicate entities
    assert memory_session.scalar(select(func.count(RawRecord.id))) == 2
    assert memory_session.scalar(select(func.count(Incident.id))) == 1
    assert memory_session.scalar(select(func.count(OfficerRef.id))) == 2
    assert memory_session.scalar(select(func.count(ForceEvent.id))) == 2


def test_unreachable_source_yields_no_rows_and_no_invented_data(memory_session, monkeypatch):
    definition = SourceDefinition(
        id="phoenix_ckan_uof",
        name="test",
        adapter="ckan",
        entity_type="use_of_force",
        config={
            "base_url": "http://127.0.0.1:1",
            "packages": ["uof"],
            "organization": "police-department",
        },
    )
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: [definition])

    report = PipelineOrchestrator(memory_session).run(trigger="test")
    assert report["phases"]["search"]["sources_reachable"] == 0
    assert report["phases"]["gather"]["rows_received"] == 0
    assert memory_session.scalar(select(func.count(Incident.id))) == 0
    assert memory_session.scalar(select(func.count(OfficerRef.id))) == 0
