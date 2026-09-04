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
)


def test_cron_field_matches():
    assert _cron_field_matches("*", 15, 0, 59) is True
    assert _cron_field_matches("15", 15, 0, 59) is True
    assert _cron_field_matches("15", 16, 0, 59) is False
    assert _cron_field_matches("*/15", 0, 0, 59) is True
    assert _cron_field_matches("*/15", 10, 0, 59) is False
    assert _cron_field_matches("1-5", 3, 0, 6) is True
    assert _cron_field_matches("1-5", 6, 0, 6) is False
    assert _cron_field_matches("0,15,30,45", 30, 0, 59) is True
    assert _cron_field_matches("0,15,30,45", 35, 0, 59) is False


def test_is_source_due():
    now = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
    assert _is_source_due(None, None, now) is False
    assert _is_source_due("0 6 * * *", None, now) is True
    assert _is_source_due("0 7 * * *", None, now) is False
    last_run = datetime(2026, 9, 2, 5, 55, tzinfo=UTC)
    assert _is_source_due("0 6 * * *", last_run, now) is False


def test_catalog_loads_live_sources_only():
    """Catalog must contain live, resolvable endpoints — no placeholder URLs."""
    sources = load_catalog("app/source_catalog.yaml")
    assert len(sources) >= 15
    for source in sources:
        cfg = source.get("config", {}) or {}
        blob = str(cfg).lower()
        # Anti-fabrication: no example.com-style placeholder endpoints anywhere.
        assert "example.com" not in blob, source["id"]
        assert "services.arcgis.com/example" not in blob, source["id"]
        if source.get("enabled", True):
            assert cfg.get("url") or cfg.get("urls") or cfg.get("url_env") or \
                cfg.get("domain") or cfg.get("hub_discover") or cfg.get("query") or \
                cfg.get("docket_number"), f"enabled source {source['id']} has no live target"
        else:
            # Disabled sources must document exactly why.
            assert cfg.get("disabled_reason"), source["id"]


def test_no_manual_drop_sources_in_catalog():
    sources = load_catalog("app/source_catalog.yaml")
    for source in sources:
        cfg = source.get("config", {}) or {}
        assert "drop_dir_env" not in cfg, source["id"]
        assert source.get("access_mode") != "file_drop", source["id"]


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
