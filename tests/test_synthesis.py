from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (
    Arrest,
    CourtCase,
    DataSource,
    EntityLink,
    Incident,
    NewsArticle,
    Officer,
    RawRecord,
    StagingRecord,
)
from app.pipeline.synthesis import SynthesisEngine, _safe_parse_datetime


def test_safe_parse_datetime():
    assert _safe_parse_datetime(None) is None
    assert _safe_parse_datetime("invalid") is None
    dt = _safe_parse_datetime("2026-09-02T12:00:00Z")
    assert dt is not None
    assert dt.year == 2026


def test_synthesis_pipeline():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    with Session(engine) as session:
        ds = DataSource(
            id="s1", name="S1", category="police", adapter="manual", access_mode="manual"
        )
        session.add(ds)
        session.flush()

        raw = RawRecord(source_id="s1", content_type="application/json", checksum="h1")
        session.add(raw)
        session.flush()

        # 1. Incident staging record
        st_incident = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="incident",
            payload={
                "attributes": {
                    "incident_number": "INC-100",
                    "agency_name": "Tempe Police",
                    "location": "Mill Ave",
                }
            },
            record_hash="h1",
            status="pending",
        )

        # 2. Officer staging record
        st_officer = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="officer",
            payload={
                "row": {
                    "badge_number": "B789",
                    "first_name": "Bob",
                    "last_name": "Johnson",
                    "agency_name": "Tempe Police",
                }
            },
            record_hash="h2",
            status="pending",
        )

        # 3. Arrest staging record
        st_arrest = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="arrest",
            payload={"attributes": {"booking_number": "BK-555", "person_name": "Sam Doe"}},
            record_hash="h3",
            status="pending",
        )

        # 4. Court case staging record
        st_case = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="court_case",
            payload={"docket": {"docket_number": "CV2026-999", "court": "Phoenix Municipal"}},
            record_hash="h4",
            status="pending",
        )

        # 5. News staging record
        st_news = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="news",
            payload={
                "entry": {
                    "title": "Police Report",
                    "link": "https://example.com/news",
                    "summary": "Summary",
                }
            },
            record_hash="h5",
            status="pending",
        )

        # 6. Missing key (should suspend)
        st_invalid = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="incident",
            payload={"attributes": {"no_incident_number": "xyz"}},
            record_hash="h6",
            status="pending",
        )

        session.add_all([st_incident, st_officer, st_arrest, st_case, st_news, st_invalid])
        session.commit()

        # Run synthesis
        synthesis = SynthesisEngine(session)
        stats = synthesis.execute()

        assert stats["processed"] == 5
        assert stats["suspended"] == 1
        assert stats["failed"] == 0

        # Verify created entities
        inc = session.scalar(
            select(Incident).where(
                Incident.external_ids.contains({"incident_number": "INC-100"})
            )
        )
        if not inc:
            for i in session.scalars(select(Incident)).all():
                if i.external_ids.get("incident_number") == "INC-100":
                    inc = i
                    break
        assert inc is not None
        assert inc.agency.name == "Tempe Police"

        off = session.scalar(select(Officer).where(Officer.badge_number == "B789"))
        assert off is not None
        assert off.first_name == "Bob"

        arr = session.scalar(select(Arrest).where(Arrest.booking_number == "BK-555"))
        assert arr is not None

        cc = session.scalar(select(CourtCase).where(CourtCase.case_number == "CV2026-999"))
        assert cc is not None

        news = session.scalar(
            select(NewsArticle).where(NewsArticle.url == "https://example.com/news")
        )
        assert news is not None

        links = session.scalars(select(EntityLink)).all()
        assert len(links) >= 5
