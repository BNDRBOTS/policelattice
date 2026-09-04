"""Immutable monthly chron-archive: content addressing, versioning, parity."""

from __future__ import annotations

import gzip
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.analytics.archive import MonthlyArchiver, sha256_hex
from app.analytics.engine import AnalyticsEngine
from app.db import Base
from app.models import MonthlyArchiveFile


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


def test_archive_files_are_content_addressed_and_deduplicated(session):
    archiver = MonthlyArchiver(session)
    r1 = archiver.archive_month("2026-08")
    session.commit()
    all_written = [f for f in r1["files"].values() if f["disposition"] == "written"]
    assert len(all_written) == 5

    # Archiving identical content again must NOT duplicate anything.
    r2 = archiver.archive_month("2026-08")
    session.commit()
    assert all(f["disposition"] in ("unchanged", "content_already_archived")
               for f in r2["files"].values())
    count = len(session.scalars(sa.select(MonthlyArchiveFile)).all())
    assert count == 5


def test_archive_versions_grow_append_only(session):
    """When a month's content changes, a NEW version file is appended; the
    original file remains intact and byte-identical (append-only history)."""
    archiver = MonthlyArchiver(session)
    archiver.archive_month("2026-08")
    session.commit()

    before = session.scalars(
        sa.select(MonthlyArchiveFile).where(MonthlyArchiveFile.kind == "raw_records")
    ).all()
    assert len(before) == 1
    original_bytes = before[0].payload
    original_sha = before[0].sha256

    # Mutate content: add a raw record in August.
    from app.models import DataSource, RawRecord

    ds = DataSource(id="s1", name="S1", category="c", adapter="flatfile", access_mode="api")
    session.add(ds)
    session.add(
        RawRecord(
            source_id="s1",
            content_type="application/json",
            raw_data={"row": {"x": 1}},
            checksum="c1",
            ingested_at=datetime(2026, 8, 15, tzinfo=UTC),
        )
    )
    session.commit()

    archiver.archive_month("2026-08")
    session.commit()

    after = session.scalars(
        sa.select(MonthlyArchiveFile).where(MonthlyArchiveFile.kind == "raw_records")
    ).all()
    assert len(after) == 2  # original + v2
    original_row = next(r for r in after if r.sha256 == original_sha)
    assert original_row.payload == original_bytes  # untouched
    assert any(r.filename.endswith("-v2.jsonl.gz") for r in after)


def test_snapshot_replay_parity_with_live_engine(session):
    """Archived analytics payload is byte-stable and structurally identical
    to the canonical live payload for the same month."""
    archiver = MonthlyArchiver(session)
    archiver.archive_month("2026-09")
    session.commit()

    archived = archiver.read_analytics_snapshot("2026-09")
    live = AnalyticsEngine(session).compute_month("2026-09")

    assert archived is not None
    assert archived["mode"] == "archived"
    assert "sha256" in archived["archive"]
    # identical canonical structure and identical measured values
    assert archived["summary"] == live["summary"]
    assert archived["timeline"] == live["timeline"]
    assert archived["force_taxonomy"] == live["force_taxonomy"]
    # identical canonical structure and identical measured values;
    # archived snapshots deterministically omit volatile generated_at (the
    # archive row's created_at preserves the true archival timestamp).
    assert set(archived) - {"mode", "archive", "plain_language_summary"} == set(live) - {
        "mode", "generated_at"
    }


def test_integrity_digest_matches_payload(session):
    archiver = MonthlyArchiver(session)
    archiver.archive_month("2026-07")
    session.commit()
    row = session.scalars(
        sa.select(MonthlyArchiveFile).where(MonthlyArchiveFile.kind == "analytics_snapshot")
    ).one()
    assert row.sha256 == sha256_hex(row.payload)
    gzip.decompress(row.payload)  # intact compressed body


def test_finalize_gate_and_month_listing(session):
    archiver = MonthlyArchiver(session)
    assert not archiver.is_finalized("2026-08")
    archiver.archive_month("2026-08", finalize=True)
    session.commit()
    assert archiver.is_finalized("2026-08")
    assert archiver.previous_month_key("2026-09") == "2026-08"

    months = archiver.list_months()
    assert [m["month"] for m in months] == ["2026-08"]
    assert set(months[0]["kinds"].keys()) == {
        "raw_records", "staging_records", "entities", "analytics_snapshot", "anomaly_findings"
    }
