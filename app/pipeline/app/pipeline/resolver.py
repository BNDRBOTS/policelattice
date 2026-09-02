from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    Agency,
    Arrest,
    CourtCase,
    EntityLink,
    Incident,
    Officer,
    PendingSynthesis,
    StagingRecord,
)
from app.pipeline.state import mark_ready


class DependencyResolver:
    """Attempts to resolve suspended staging records when dependencies arrive.

    It checks PendingSynthesis entries against the current lattice. If the
    required external key now exists, the staging record is marked ready and
    the pending dependency is cleared.
    """

    def __init__(self, session: Session):
        self.session = session

    def resolve(self) -> int:
        pending = self.session.scalars(select(PendingSynthesis).where(PendingSynthesis.status == "waiting")).all()
        resolved_count = 0
        for dep in pending:
            if self._dependency_exists(dep.required_entity_type, dep.required_key, dep.required_value):
                dep.status = "resolved"
                staging = self.session.get(StagingRecord, dep.staging_record_id)
                if staging and staging.status == "suspended":
                    mark_ready(self.session, staging.id)
                resolved_count += 1
            else:
                dep.attempts += 1
                if dep.attempts > 10:
                    dep.status = "expired"
        self.session.commit()
        return resolved_count

    def _dependency_exists(self, entity_type: str, key: str, value: str) -> bool:
        if entity_type == "officer":
            if key == "badge_number":
                return self.session.scalar(select(Officer).where(Officer.badge_number == value)) is not None
            if key == "employee_id":
                return self.session.scalar(select(Officer).where(Officer.employee_id == value)) is not None
        if entity_type == "incident":
            # Check external_ids JSON for incident_number
            stmt = select(Incident).where(Incident.external_ids.contains({key: value}))
            return self.session.scalar(stmt) is not None
        if entity_type == "arrest":
            if key == "booking_number":
                return self.session.scalar(select(Arrest).where(Arrest.booking_number == value)) is not None
        if entity_type == "court_case":
            if key == "case_number":
                return self.session.scalar(select(CourtCase).where(CourtCase.case_number == value)) is not None
        return False
