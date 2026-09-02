from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import RawRecord
from app.pipeline.runner import (
    _checksum_exists,
    _cron_field_matches,
    _is_source_due,
    get_source_by_id,
    load_catalog,
    run_full_pipeline,
)


def test_cron_field_matches():
    # Wildcard
    assert _cron_field_matches("*", 15, 0, 59) is True

    # Exact number
    assert _cron_field_matches("15", 15, 0, 59) is True
    assert _cron_field_matches("15", 16, 0, 59) is False

    # Step */15
    assert _cron_field_matches("*/15", 0, 0, 59) is True
    assert _cron_field_matches("*/15", 15, 0, 59) is True
    assert _cron_field_matches("*/15", 30, 0, 59) is True
    assert _cron_field_matches("*/15", 45, 0, 59) is True
    assert _cron_field_matches("*/15", 10, 0, 59) is False

    # Range
    assert _cron_field_matches("1-5", 3, 0, 6) is True
    assert _cron_field_matches("1-5", 6, 0, 6) is False

    # List
    assert _cron_field_matches("0,15,30,45", 30, 0, 59) is True
    assert _cron_field_matches("0,15,30,45", 35, 0, 59) is False


def test_is_source_due():
    # No schedule means manual only
    assert _is_source_due(None, None, datetime(2026, 9, 2, 6, 0, tzinfo=UTC)) is False

    # Scheduled match
    now = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)  # 06:00
    assert _is_source_due("0 6 * * *", None, now) is True

    # Hour mismatch
    assert _is_source_due("0 7 * * *", None, now) is False

    # Already run recently (< 10 minutes ago)
    last_run = datetime(2026, 9, 2, 5, 55, tzinfo=UTC)
    assert _is_source_due("0 6 * * *", last_run, now) is False


def test_catalog_loads():
    sources = load_catalog("app/source_catalog.yaml")
    assert len(sources) >= 60
    ids = [s["id"] for s in sources]
    assert "tempe_pd_calls" in ids
    assert "city_of_phx_open_data_portal" in ids


def test_checksum_deduplication():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    with Session(engine) as session:
        ds = get_source_by_id(session, "src1")
        assert ds.id == "src1"

        raw = RawRecord(
            source_id="src1",
            content_type="application/json",
            raw_data={"val": 1},
            checksum="hash_123",
        )
        session.add(raw)
        session.commit()

        assert _checksum_exists(session, "src1", "hash_123") is True
        assert _checksum_exists(session, "src1", "different_hash") is False
        assert _checksum_exists(session, "src2", "hash_123") is False


def test_run_full_pipeline_execution():
    result = run_full_pipeline(force=True)
    assert result["status"] == "success"
    assert "ingestion" in result
    assert "synthesis" in result
    assert "entity_counts" in result
    assert result["entity_counts"]["incidents"] > 0
    assert result["entity_counts"]["officers"] > 0
