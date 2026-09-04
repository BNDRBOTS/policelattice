from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Agency, DataSource, Officer, PendingSynthesis, RawRecord, StagingRecord
from app.pipeline.resolver import DependencyResolver


def test_resolver_resolves_officer_dependency():
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

        staging = StagingRecord(
            raw_record_id=raw.id,
            source_id="s1",
            entity_type="use_of_force",
            payload={"attributes": {"incident_number": "UOF-1", "badge_number": "B999"}},
            record_hash="h1",
            status="suspended",
            suspension_reason="Officer not found for badge/employee_id B999",
        )
        session.add(staging)
        session.flush()

        pending = PendingSynthesis(
            staging_record_id=staging.id,
            required_entity_type="officer",
            required_key="badge_number",
            required_value="B999",
            status="waiting",
        )
        session.add(pending)
        session.commit()

        # Resolver runs when officer doesn't exist yet -> 0 resolved
        resolver = DependencyResolver(session)
        assert resolver.resolve() == 0

        # Now officer arrives
        agency = Agency(name="PHX PD")
        session.add(agency)
        session.flush()
        officer = Officer(agency_id=agency.id, badge_number="B999", first_name="Alex")
        session.add(officer)
        session.commit()

        # Resolver runs again -> 1 resolved!
        assert resolver.resolve() == 1

        # Check staging record is now ready
        updated_staging = session.get(StagingRecord, staging.id)
        assert updated_staging.status == "ready"
