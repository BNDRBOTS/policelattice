"""End-to-end six-phase orchestrator test using a deterministic live-shaped
stub adapter (offline unit test of orchestration mechanics — the production
pipeline always uses the live adapters over the network)."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.ingestion.base import AdapterRegistry, BaseAdapter, RawRecordDTO
from app.models import MonthlyArchiveFile, PipelineRun, StagingRecord, VerificationResult
from app.pipeline.orchestrator import PHASE_ORDER, PipelineOrchestrator


class StubLiveAdapter(BaseAdapter):
    """Yields deterministic records shaped like the real live feeds."""

    name = "stub_live"
    calls = 0
    STAMP = "2026-09-01T12:00:00+00:00"  # deterministic: checksums stable across fetches

    def fetch(self) -> list[RawRecordDTO]:
        StubLiveAdapter.calls += 1
        stamp = StubLiveAdapter.STAMP
        return [
            RawRecordDTO(
                content_type="application/json",
                payload={
                    "row": {
                        "incident_number": f"STUB-{i}",
                        "incident_type": "use of force",
                        "force_type": "conducted energy weapon",
                        "occurred_at": stamp,
                        "agency_name": "Stub Agency PD",
                        "location": "1 Stub St",
                    }
                },
                source_id="stub_source",
                metadata={"adapter": self.name, "live_url": "https://stub.test/feed"},
            )
            for i in range(4)
        ]


AdapterRegistry.register("stub_live")(StubLiveAdapter)

STUB_SOURCES = [
    {
        "id": "stub_source",
        "name": "Stub Live Source",
        "category": "test",
        "adapter": "stub_live",
        "access_mode": "api",
        "schedule": "0 5 * * *",
        "config": {"url": "https://stub.test/feed"},
        "entity_type": "incident",
    }
]


@pytest.fixture()
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    S = sessionmaker(bind=engine)
    with S() as s:
        yield s


def test_six_phase_order_enforced(session, monkeypatch):
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: STUB_SOURCES)
    result = PipelineOrchestrator().run(session=session, trigger="manual", force=True)

    assert result["status"] == "success", result
    run = session.get(PipelineRun, result["pipeline_run_id"])
    assert run.phase_order == PHASE_ORDER == [
        "search", "gather", "organize", "process", "verify", "synthesize"
    ]
    # every phase executed exactly once, in order, and is audited
    assert list(run.phases.keys()) == PHASE_ORDER
    assert run.status == "success"


def test_gather_dedup_and_verify_gating(session, monkeypatch):
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: STUB_SOURCES)
    result = PipelineOrchestrator().run(session=session, trigger="startup", force=True)
    assert result["status"] == "success"

    staged = session.scalars(select(StagingRecord)).all()
    assert len(staged) == 4
    verifications = session.scalars(select(VerificationResult)).all()
    assert len(verifications) == 4
    assert all(v.passed for v in verifications)
    # verified records were synthesized (not left in processed/failed)
    statuses = {s.status for s in staged}
    assert statuses <= {"synthesized", "suspended"}
    assert result["entity_counts"]["incidents"] >= 1

    # Second run: identical data -> deduplicated at gather (checksums).
    result2 = PipelineOrchestrator().run(session=session, trigger="manual", force=True)
    gather2 = result2["phases"]["gather"]["sources"]["stub_source"]
    assert gather2["fetched"] == 4
    assert gather2["new_raw_records"] == 0
    assert gather2["duplicates_skipped"] == 4
    staged2 = session.scalars(select(StagingRecord)).all()
    assert len(staged2) == 4


def test_verify_external_revalidation(session, monkeypatch):
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: STUB_SOURCES)
    result = PipelineOrchestrator().run(session=session, trigger="manual", force=True)
    reval = result["phases"]["verify"]["external_revalidation"]["stub_source"]
    assert reval["status"] == "revalidated"
    assert reval["sample_size"] == 3
    assert reval["confirmed"] == 3  # stub source is deterministic


def test_monthly_archive_written_by_synthesize(session, monkeypatch):
    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: STUB_SOURCES)
    result = PipelineOrchestrator().run(session=session, trigger="manual", force=True)
    month_key = datetime.now(UTC).strftime("%Y-%m")
    archive = result["phases"]["synthesize"]["archive"]
    assert archive["month_key"] == month_key

    files = session.scalars(
        select(MonthlyArchiveFile).where(MonthlyArchiveFile.month_key == month_key)
    ).all()
    kinds = {f.kind for f in files}
    assert {"raw_records", "staging_records", "entities", "analytics_snapshot",
            "anomaly_findings"} <= kinds
    for f in files:
        # each discrete file is intact gzipped JSONL/JSON and content-addressed
        body = gzip.decompress(f.payload)
        assert f.sha256 and len(f.sha256) == 64
        if f.kind.endswith("snapshot"):
            json.loads(body)
        elif body:
            assert body.endswith(b"\n")


def test_failed_records_never_synthesized(session, monkeypatch):
    """A malformed record must fail verification and be excluded."""
    class BadAdapter(BaseAdapter):
        name = "stub_live"

        def fetch(self):
            return [
                RawRecordDTO(
                    content_type="application/json",
                    payload={"row": {"unrelated": None}},  # no canonical fields
                    source_id="stub_source",
                    metadata={"live_url": "https://stub.test/bad"},
                )
            ]

    monkeypatch.setattr("app.pipeline.orchestrator.load_catalog", lambda: STUB_SOURCES)
    monkeypatch.setattr(StubLiveAdapter, "fetch", BadAdapter.fetch)
    result = PipelineOrchestrator().run(session=session, trigger="manual", force=True)
    assert result["status"] == "success"
    staged = session.scalars(select(StagingRecord)).all()
    assert staged and all(s.status == "failed" for s in staged)
    failed = session.scalars(select(VerificationResult)).all()
    assert any(not v.passed for v in failed)
    assert result["entity_counts"]["incidents"] == 0
