"""Anti-fabrication regression suite.

Locks in the invariant that no layer (normalization, synthesis) may invent
values absent from source data: no default agency names, ranks, statuses,
courts, badge numbers, or incident numbers. Missing data must surface as
None / explicit "not recorded"/"Unattributed" labeling or an honest
suspension — never a fabricated value.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import DataSource, Incident, Officer, RawRecord
from app.pipeline.normalization import CanonicalNormalizer
from app.pipeline.runner import organize_raw_record, process_staging_record
from app.pipeline.synthesis import SynthesisEngine


@pytest.fixture()
def session():
    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    S = sessionmaker(bind=engine)
    with S() as s:
        yield s


FORBIDDEN_VALUES = (
    "Phoenix Police Department",
    "Maricopa County Superior Court",
    "police-lattice.local",
)


def test_officer_normalization_invents_nothing():
    canon = CanonicalNormalizer.normalize(
        {"row": {"badge_number": "1042", "full_name": "Smith, John"}},
        entity_type="officer",
        source_id="t",
    ).canonical_payload
    assert canon["rank"] is None, "rank must not be defaulted"
    assert canon["status"] is None, "status must not be defaulted"


def test_court_normalization_invents_nothing():
    canon = CanonicalNormalizer.normalize(
        {"row": {"case_number": "2:07-cv-02513"}},
        entity_type="court_case",
        source_id="t",
    ).canonical_payload
    assert canon["court"] is None, "court must not be defaulted"
    assert canon["status"] is None, "status must not be defaulted"


def _stage(session, row, entity_type):
    session.add(
        DataSource(id="t1", name="T1", category="c", adapter="flatfile", access_mode="api")
    )
    raw = RawRecord(
        source_id="t1", content_type="text/csv", raw_data={"row": row}, checksum="x"
    )
    session.add(raw)
    session.flush()
    from app.ingestion.base import RawRecordDTO

    dto = RawRecordDTO(content_type="text/csv", payload={"row": row}, source_id="t1")
    staging = organize_raw_record(
        session,
        {"id": "t1", "name": "T1", "adapter": "flatfile", "entity_type": entity_type,
         "config": {}},
        raw,
        dto,
    )
    process_staging_record(staging)
    session.commit()
    return staging


def test_officer_without_any_identity_suspends(session):
    staging = _stage(session, {"notes": "roster row with no identifiers"}, "officer")
    SynthesisEngine(session).execute()
    session.refresh(staging)
    assert staging.status == "suspended"
    assert "no identity can be asserted" in (staging.suspension_reason or "")
    assert session.scalars(sa.select(Officer)).all() == [], "no officer may be invented"


def test_incident_without_number_gets_no_fabricated_number(session):
    _stage(
        session,
        {
            "incident_type": "use of force",
            "occurred_at": datetime.now(UTC).isoformat(),
            "location": "1 Main St",
        },
        "incident",
    )
    SynthesisEngine(session).execute()
    incidents = session.scalars(sa.select(Incident)).all()
    assert len(incidents) == 1
    ext = incidents[0].external_ids or {}
    assert "incident_number" not in ext, "no fake source-issued number"
    assert ext.get("internal_ref", "").startswith("lattice-staging-")


def test_synthesized_entities_carry_no_forbidden_defaults(session):
    _stage(
        session,
        {
            "badge_number": "777",
            "full_name": "Doe, Jane",
            "occurred_at": datetime.now(UTC).isoformat(),
        },
        "officer",
    )
    SynthesisEngine(session).execute()
    officers = session.scalars(sa.select(Officer)).all()
    assert officers, "identity-bearing officer is synthesized"
    blob = str(
        [
            {
                "first_name": o.first_name,
                "last_name": o.last_name,
                "status": o.status,
                "ext": o.external_ids,
            }
            for o in officers
        ]
    )
    for forbidden in FORBIDDEN_VALUES:
        assert forbidden not in blob
    assert all(o.status is None for o in officers)
    # agency must be the explicit Unattributed label, not a real agency name
    from app.models import Agency

    agencies = session.scalars(sa.select(Agency)).all()
    assert all(a.name == "Unattributed Agency" for a in agencies)


def test_source_code_contains_no_fabrication_literals():
    """Static guard: banned default literals must not reappear in code."""
    import pathlib

    # NOTE: "Phoenix Police Department" legitimately appears in the agency
    # ALIAS MAP (canonicalizing aliases a source actually stated). The guard
    # therefore checks the fallback site, not the map.
    banned_exact = (
        '"Maricopa County Superior Court"',
        "police-lattice.local",
        'f"OFF-',
        'f"INC-',
        # invented geo/severity/period/type defaults (found in v2 audit)
        '"Phoenix")',
        '"AZ")',
        '"Felony")',
        '"Misdemeanor")',
        '"Quarterly")',
        'or "alpr"',
        '"Untitled Article"',
        '"Criminal Statute Violation"',
    )
    banned_pairs = (
        # (fallback expression, must not appear)
        ('return "Phoenix Police Department"', "agency-name fallback"),
        ('or "Phoenix Police Department"', "agency default"),
        ('"Phoenix Police Department")', "default argument"),
        ('or entity_type', "force-type entity-label fallback"),
    )
    for path in (
        pathlib.Path("app/pipeline/synthesis.py"),
        pathlib.Path("app/pipeline/normalization.py"),
        pathlib.Path("app/pipeline/extraction.py"),
        pathlib.Path("app/analytics/engine.py"),
        pathlib.Path("app/api/main.py"),
    ):
        src = path.read_text()
        for literal in banned_exact:
            assert literal not in src, f"{literal} found in {path}"
        for literal, label in banned_pairs:
            assert literal not in src, f"{label} ({literal}) found in {path}"


def test_incident_normalization_invents_no_geo():
    canon = CanonicalNormalizer.normalize(
        {"row": {"incident_number": "X-1", "occurred_at": "2026-09-01T00:00:00+00:00"}},
        entity_type="incident",
        source_id="t",
    ).canonical_payload
    assert canon["city"] is None, "city must not default to Phoenix"
    assert canon["state"] is None, "state must not default to AZ"
    assert canon["agency_name"] is None


def test_arrest_and_uof_normalization_invent_no_severity_or_type():
    arrest = CanonicalNormalizer.normalize(
        {"row": {"booking_number": "B-1", "statute": "ARS 13-2904"}},
        entity_type="arrest",
        source_id="t",
    ).canonical_payload
    assert arrest["charges"][0]["severity"] is None, "severity never inferred"

    uof = CanonicalNormalizer.normalize(
        {"row": {"incident_number": "U-1"}},
        entity_type="use_of_force",
        source_id="t",
    ).canonical_payload
    assert uof["force_type"] is None, "force_type must not fall back to entity label"


def test_statute_extraction_never_classifies_severity():
    from app.pipeline.extraction import EvidenceExtractionEngine

    ev = EvidenceExtractionEngine.extract_from_text(
        "Charge: ARS 13-1204 aggravated assault and ARS 13-2904 disorderly conduct."
    )
    for st in ev.to_dict()["statutes"]:
        assert st["severity"] is None
        assert st["title"] is None or st["statute_code"] in st.get("statute", "")


def test_legacy_database_purged_on_schema_guard():
    """Legacy rows (fabricated by pre-v2 code) are purged automatically;
    current-version databases are left untouched."""
    import json

    import sqlalchemy as sa
    from sqlalchemy.pool import StaticPool

    from app.db import SCHEMA_VERSION, Base, ensure_schema_current

    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)

    # Simulate a legacy database: fabricated rows, no lattice_meta marker.
    fabricated = json.dumps(
        {"agency_name": "Phoenix Police Department", "city": "Phoenix"}
    )
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO incidents "
                "(agency_id, incident_type, external_ids, data, created_at, updated_at) "
                "VALUES (NULL, 'incident', '{}', :d, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ),
            {"d": fabricated},
        )
    ensure_schema_current(engine)
    with engine.connect() as conn:
        count = conn.execute(sa.text("SELECT COUNT(*) FROM incidents")).scalar()
        version = conn.execute(
            sa.text("SELECT value FROM lattice_meta WHERE key='schema_version'")
        ).scalar()
    assert count == 0, "legacy fabricated rows must be purged"
    assert int(version) == SCHEMA_VERSION

    # Current-version DB is preserved.
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO incidents "
                "(agency_id, incident_type, external_ids, data, created_at, updated_at) "
                "VALUES (NULL, 'incident', '{}', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )
    result = ensure_schema_current(engine)
    assert result["action"] == "none"
    with engine.connect() as conn:
        count = conn.execute(sa.text("SELECT COUNT(*) FROM incidents")).scalar()
    assert count == 1
