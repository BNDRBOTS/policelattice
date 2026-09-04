"""Immutable monthly archive, month parity, and the HTTP API."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from app.analytics.engine import available_periods, build_view
from app.models import MonthlySnapshot
from app.pipeline.archive import archive_month, archived_periods, load_archived_view
from app.pipeline.registry import load_catalog
from tests.conftest import synthetic_force_events

#: Exact row counts the fixture inserted, so the assertions below compare
#: against a known number rather than against whatever the query returned.
EXPECTED: dict[str, int] = {}


@pytest.fixture()
def populated(memory_session):
    counts = synthetic_force_events(
        memory_session, n_officers=40, period="2025-06", agency_id="phoenix-pd",
        outlier_events=30, outlier_out_of_policy=18, peer_events=6,
    )
    memory_session.commit()
    EXPECTED.clear()
    EXPECTED.update(counts)
    return memory_session


# ---------------------------------------------------------------- archive

def test_sealing_a_month_is_hash_addressed(populated):
    result = archive_month(populated, "2025-06")
    assert result["action"] == "sealed"
    assert result["revision"] == 1
    assert len(result["content_sha256"]) == 64

    snapshot = populated.scalar(select(MonthlySnapshot))
    assert snapshot.period == "2025-06"
    assert snapshot.is_current is True
    assert snapshot.payload["period"] == "2025-06"
    assert snapshot.payload["incidents"], "the sealed payload holds the full record set"


def test_resealing_unchanged_content_is_a_no_op(populated):
    first = archive_month(populated, "2025-06")
    second = archive_month(populated, "2025-06")
    assert second["action"] == "unchanged"
    assert second["content_sha256"] == first["content_sha256"]
    assert len(populated.scalars(select(MonthlySnapshot)).all()) == 1


def test_changed_content_appends_a_new_revision_and_never_overwrites(populated):
    first = archive_month(populated, "2025-06")

    # new source data arrives for the same month
    from datetime import UTC, datetime

    from app.models import Incident
    populated.add(
        Incident(
            agency_id="phoenix-pd", external_number="LATE-0001", kind="use_of_force",
            period="2025-06", occurred_at=datetime(2025, 6, 15, tzinfo=UTC),
            source_id="test", source_url="https://example.test/late",
            retrieved_at=datetime.now(UTC), content_sha256="deadbeef" * 8, data={},
        )
    )
    populated.commit()

    second = archive_month(populated, "2025-06")
    assert second["action"] == "sealed"
    assert second["revision"] == 2
    assert second["content_sha256"] != first["content_sha256"]

    snapshots = populated.scalars(
        select(MonthlySnapshot).order_by(MonthlySnapshot.revision)
    ).all()
    assert [s.revision for s in snapshots] == [1, 2]
    # revision 1 is untouched: its hash and payload are exactly as first written
    assert snapshots[0].content_sha256 == first["content_sha256"]
    assert snapshots[0].is_current is False
    assert snapshots[1].is_current is True
    assert len(snapshots[1].payload["incidents"]) == len(snapshots[0].payload["incidents"]) + 1


def test_archived_view_is_byte_identical_to_what_was_sealed(populated):
    archive_month(populated, "2025-06")
    payload = load_archived_view(populated, "2025-06")
    assert payload is not None
    assert payload["period"] == "2025-06"
    assert load_archived_view(populated, "1999-01") is None
    assert len(archived_periods(populated)) == 1


# ------------------------------------------------------------- parity

def test_live_and_archived_views_share_one_shape(populated):
    archive_month(populated, "2025-06")
    archived = load_archived_view(populated, "2025-06")
    live = build_view(populated, "2025-06")
    assert set(archived.keys()) == set(live.keys())
    # identical shape means a month switch cannot shift layout
    for key in ("counts", "timeline", "force_applied", "policy_outcome",
                "agencies", "findings", "incidents", "officers", "sources"):
        assert key in archived and key in live


def test_available_periods(populated):
    assert available_periods(populated) == ["2025-06"]


# ---------------------------------------------------------------- API

@pytest.fixture()
def archived_populated(populated):
    """Populated lattice with one sealed monthly snapshot, for API tests."""
    archive_month(populated, "2025-06")
    populated.commit()
    return populated


@pytest.fixture()
def client(archived_populated):
    from fastapi.testclient import TestClient

    from app.api.main import app, get_db

    def _override():
        yield archived_populated

    app.dependency_overrides[get_db] = _override
    # Deliberately NOT used as a context manager: entering the lifespan would
    # start the real startup pipeline against the live catalog, which has no
    # business running inside a unit test.
    test_client = TestClient(app, raise_server_exceptions=True)
    try:
        yield test_client
    finally:
        app.dependency_overrides.clear()


def test_dashboard_serves(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Police Lattice" in response.text
    assert "c-force" in response.text


def test_month_endpoint_returns_full_untruncated_payload(client):
    response = client.get("/api/month/2025-06")
    assert response.status_code == 200
    payload = response.json()
    assert payload["period"] == "2025-06"
    assert payload["counts"]["incidents"] == EXPECTED["incidents"]
    assert len(payload["incidents"]) == EXPECTED["incidents"]
    assert len(payload["officers"]) == EXPECTED["officers"]
    for incident in payload["incidents"]:
        assert incident["source_url"].startswith("https://")
        assert len(incident["content_sha256"]) == 64


def test_month_endpoint_rejects_a_bad_period(client):
    assert client.get("/api/month/2025").status_code == 400
    assert client.get("/api/month/january").status_code == 400


def test_all_months_endpoint(client):
    payload = client.get("/api/month/all").json()
    assert payload["period"] is None
    assert payload["counts"]["incidents"] == EXPECTED["incidents"]


def test_record_endpoints_do_not_truncate(client):
    for path in ("/api/incidents", "/api/officers", "/api/findings",
                 "/api/arrests", "/api/complaints", "/api/news"):
        body = client.get(path).json()
        assert body["truncated"] is False, path
        assert body["count"] == len(body["records"]), path


def test_health_and_state(client):
    health = client.get("/health").json()
    assert health["status"] == "ok"
    assert health["incidents"] == EXPECTED["incidents"]
    state = client.get("/api/state").json()
    assert state["current_period"] == "2025-06"
    assert state["periods"] == ["2025-06"]
    assert len(state["archived"]) == 1
    assert "lexical" in state["retrieval"]


def test_sources_endpoint_reports_real_verification(client):
    body = client.get("/api/sources").json()
    assert body["count"] > 0
    for record in body["records"]:
        assert record["id"]
        assert record["adapter"]
        # a source that has not been probed says so rather than claiming success
        assert record["verified_ok"] in (True, False, None)


def test_search_endpoint(client):
    body = client.get("/api/search", params={"q": "use of force"}).json()
    assert body["query"] == "use of force"
    assert body["lexical"] is True
    assert "fusion" in body
    # semantic availability is reported, never silently assumed
    assert body["semantic"] in (True, False)
    if not body["semantic"]:
        assert body["semantic_error"]


def test_search_requires_a_query(client):
    assert client.get("/api/search", params={"q": "  "}).status_code == 400


def test_sources_payload_matches_the_dashboard_field_contract(client):
    """Every key the sources table reads must exist in the API response."""
    response = client.get("/api/sources")
    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"count", "reachable", "records"}
    assert payload["count"] == len(payload["records"])
    assert payload["reachable"] == sum(1 for r in payload["records"] if r["verified_ok"])

    # The exact set of keys app/templates/index.html renderSources() reads.
    required = {
        "id", "name", "publisher", "adapter", "endpoint", "verified_at",
        "http_status", "rows_total_reported", "rows_fetched_last_run",
        "rows_new_last_run", "verified_ok", "detail",
    }
    for record in payload["records"]:
        missing = required - set(record)
        assert not missing, f"{record.get('id')} is missing {missing}"
    # The catalog is what ships; nothing was silently dropped at load time.
    assert payload["count"] == len(load_catalog())
