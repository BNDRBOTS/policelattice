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
    )
    banned_pairs = (
        # (fallback expression, must not appear)
        ('return "Phoenix Police Department"', "agency-name fallback"),
        ('or "Phoenix Police Department"', "agency default"),
        ('"Phoenix Police Department")', "default argument"),
    )
    for path in (
        pathlib.Path("app/pipeline/synthesis.py"),
        pathlib.Path("app/pipeline/normalization.py"),
        pathlib.Path("app/analytics/engine.py"),
        pathlib.Path("app/api/main.py"),
    ):
        src = path.read_text()
        for literal in banned_exact:
            assert literal not in src, f"{literal} found in {path}"
        for literal, label in banned_pairs:
            assert literal not in src, f"{label} ({literal}) found in {path}"
