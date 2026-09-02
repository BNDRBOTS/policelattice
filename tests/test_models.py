from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (
    Agency,
    Arrest,
    Charge,
    CourtCase,
    DataSource,
    Document,
    EntityLink,
    Incident,
    InternalAffairsCase,
    MonitorReport,
    NewsArticle,
    Officer,
    PendingSynthesis,
    Person,
    RawRecord,
    StagingRecord,
    SurveillanceEvent,
    SynthesisRun,
)


def test_models_ddl_and_crud():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    with Session(engine) as session:
        # DataSource
        ds = DataSource(
            id="test_src",
            name="Test Source",
            category="portal",
            adapter="manual",
            access_mode="manual",
            config={"key": "value"},
        )
        session.add(ds)
        session.flush()

        # RawRecord
        raw = RawRecord(
            source_id="test_src",
            content_type="application/json",
            raw_data={"test": 123},
            checksum="abc123hash",
        )
        session.add(raw)
        session.flush()

        # StagingRecord
        staging = StagingRecord(
            raw_record_id=raw.id,
            source_id="test_src",
            entity_type="incident",
            payload={"test": 123},
            record_hash="abc123hash",
            status="pending",
        )
        session.add(staging)
        session.flush()

        # PendingSynthesis
        pending = PendingSynthesis(
            staging_record_id=staging.id,
            required_entity_type="officer",
            required_key="badge_number",
            required_value="1234",
            status="waiting",
        )
        session.add(pending)

        # Agency & Officer
        agency = Agency(name="Phoenix Police Department", state="AZ")
        session.add(agency)
        session.flush()

        officer = Officer(
            agency_id=agency.id,
            first_name="Jane",
            last_name="Doe",
            badge_number="9999",
            employee_id="E123",
        )
        session.add(officer)
        session.flush()

        # Incident
        incident = Incident(
            agency_id=agency.id,
            incident_type="traffic",
            occurred_at=datetime.now(UTC),
            location="123 Main St",
            external_ids={"incident_number": "INC-001"},
            data={"notes": "sample"},
        )
        session.add(incident)
        session.flush()

        # Person, Arrest, Charge
        person = Person(first_name="John", last_name="Smith")
        session.add(person)
        session.flush()

        arrest = Arrest(
            incident_id=incident.id,
            person_id=person.id,
            booking_number="BK100",
            arrested_at=datetime.now(UTC),
        )
        session.add(arrest)
        session.flush()

        charge = Charge(
            arrest_id=arrest.id,
            statute="13-1201",
            description="Endangerment",
            severity="Misdemeanor",
        )
        session.add(charge)

        # Other models
        court_case = CourtCase(case_number="CR2026-0001", court="Maricopa County Superior Court")
        doc = Document(source_id="test_src", doc_type="report", title="Annual Report")
        news = NewsArticle(
            source_id="test_src", title="News Title", url="https://example.com/news/1"
        )
        surv = SurveillanceEvent(
            agency_id=agency.id, event_type="alpr", location="7th Ave & Van Buren"
        )
        ia = InternalAffairsCase(
            agency_id=agency.id, officer_id=officer.id, case_number="IA-2026-1"
        )
        mr = MonitorReport(
            agency_id=agency.id, period="Q1 2026", compliance_data={"status": "met"}
        )
        link = EntityLink(
            source_entity="officer",
            source_id=officer.id,
            target_entity="incident",
            target_id=incident.id,
            relation_type="involved_in",
        )
        run = SynthesisRun(status="completed", stats={"processed": 1})

        session.add_all([court_case, doc, news, surv, ia, mr, link, run])
        session.commit()

        # Verify query
        saved_incident = session.scalar(select(Incident).where(Incident.id == incident.id))
        assert saved_incident is not None
        assert saved_incident.external_ids["incident_number"] == "INC-001"
        assert saved_incident.agency.name == "Phoenix Police Department"
