"""Focused unit tests for the Verify phase record-level checks."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import DataSource, RawRecord
from app.pipeline.runner import organize_raw_record, process_staging_record
from app.pipeline.verification import VerificationPhase


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


def _make_staged(s: Session, row: dict, entity_type="incident"):
    s.add(DataSource(id="t1", name="T1", category="c", adapter="flatfile", access_mode="api"))
    raw = RawRecord(
        source_id="t1",
        content_type="text/csv",
        raw_data={"row": row},
        checksum="placeholder",  # set below to the true digest
    )
    s.add(raw)
    s.flush()
    import hashlib
    import json

    raw.checksum = hashlib.sha256(
        json.dumps(raw.raw_data, sort_keys=True, default=str).encode()
    ).hexdigest()
    source_def = {
        "id": "t1",
        "name": "T1",
        "adapter": "flatfile",
        "entity_type": entity_type,
        "config": {},
    }
    from app.ingestion.base import RawRecordDTO

    dto = RawRecordDTO(content_type="text/csv", payload={"row": row}, source_id="t1")
    staging = organize_raw_record(s, source_def, raw, dto)
    process_staging_record(staging)
    s.commit()
    return staging


def test_valid_record_passes_all_checks(session):
    staging = _make_staged(
        session,
        {
            "incident_number": "PHX-2026-0001",
            "incident_type": "use of force",
            "occurred_at": (datetime.now(UTC) - timedelta(days=2)).isoformat(),
            "agency_name": "Phoenix Police Department",
        },
    )
    result = VerificationPhase(session).verify_records([staging.id])
    assert result["passed"] == 1 and result["failed"] == 0
    assert staging.status == "ready"
    from app.models import VerificationResult

    v = session.scalars(sa.select(VerificationResult)).one()
    assert v.passed is True
    assert set(v.checks.keys()) == {
        "provenance", "integrity", "canonical_form", "temporal", "content_non_empty"
    }


def test_future_dated_record_fails_temporal(session):
    staging = _make_staged(
        session,
        {
            "incident_number": "FUT-1",
            "occurred_at": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
        },
    )
    result = VerificationPhase(session).verify_records([staging.id])
    assert result["failed"] == 1
    assert staging.status == "failed"
    assert any("temporal" in f for f in result["failure_examples"][0]["failures"])


def test_canonical_form_failure_blocks_synthesis(session):
    staging = _make_staged(session, {"nothing": "useful"})
    result = VerificationPhase(session).verify_records([staging.id])
    assert result["failed"] == 1
    assert staging.status == "failed"
    assert "canonical_form: normalized payload lacks required fields" in staging.suspension_reason


def test_tampered_checksum_fails_integrity(session):
    staging = _make_staged(
        session,
        {"incident_number": "TAMPER-1", "occurred_at": datetime.now(UTC).isoformat()},
    )
    staging.raw_record.checksum = "0" * 64  # simulate tampering with stored digest
    session.commit()
    result = VerificationPhase(session).verify_records([staging.id])
    assert result["failed"] == 1
    assert "integrity" in staging.suspension_reason
