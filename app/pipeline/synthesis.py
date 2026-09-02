from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    Agency,
    Arrest,
    Charge,
    Complaint,
    CourtCase,
    Document,
    EntityLink,
    Incident,
    InternalAffairsCase,
    MonitorReport,
    NewsArticle,
    Officer,
    Person,
    StagingRecord,
    SurveillanceEvent,
    SynthesisRun,
)
from app.pipeline.state import mark_failed, mark_ready, suspend_staging


class SynthesisEngine:
    """Maps staging records into the unified lattice.

    Rules:
    - Only explicit joining keys from the source payload are used.
    - If a required key is missing, the record is suspended.
    - No relational connection is created unless the raw data supports it.
    """

    def __init__(self, session: Session):
        self.session = session
        self.run = SynthesisRun(status="running", started_at=datetime.utcnow())
        session.add(self.run)
        session.flush()

    def _get_or_create_agency(self, name: str, state: str | None = "AZ") -> Agency:
        agency = self.session.scalar(select(Agency).where(Agency.name == name))
        if not agency:
            agency = Agency(name=name, state=state)
            self.session.add(agency)
            self.session.flush()
        return agency

    def _find_officer_by_key(self, key: str, value: Any) -> Officer | None:
        if key == "badge_number":
            return self.session.scalar(select(Officer).where(Officer.badge_number == str(value)))
        if key == "employee_id":
            return self.session.scalar(select(Officer).where(Officer.employee_id == str(value)))
        if key == "external_ids":
            # Search JSONB for any matching value
            stmt = select(Officer).where(Officer.external_ids.contains({key: value}))
            return self.session.scalar(stmt)
        return None

    def _link_entity(
        self,
        source_entity: str,
        source_id: int,
        target_entity: str,
        target_id: int,
        relation_type: str,
        join_key: str | None = None,
        confidence: float = 1.0,
        metadata: dict[str, Any] | None = None,
    ) -> EntityLink:
        link = EntityLink(
            source_entity=source_entity,
            source_id=source_id,
            target_entity=target_entity,
            target_id=target_id,
            relation_type=relation_type,
            confidence=confidence,
            join_key=join_key,
            metadata=metadata or {},
        )
        self.session.add(link)
        return link

    def process_incident(self, staging: StagingRecord) -> None:
        payload = staging.payload.get("attributes", staging.payload.get("row", {}))
        incident_number = payload.get("incident_number") or payload.get("case_number") or payload.get("id")
        if not incident_number:
            suspend_staging(
                self.session,
                staging.id,
                "Missing incident_number/case_number",
                required_entity_type="incident",
                required_key="external_id",
                required_value="UNKNOWN",
            )
            return

        agency_name = payload.get("agency_name", "Unknown")
        agency = self._get_or_create_agency(agency_name)

        occurred_at = payload.get("date_time") or payload.get("occurred_at")
        if occurred_at:
            try:
                occurred_at = datetime.fromisoformat(str(occurred_at))
            except ValueError:
                occurred_at = None

        incident = Incident(
            agency_id=agency.id,
            incident_type=staging.entity_type or payload.get("incident_type", "unknown"),
            occurred_at=occurred_at,
            location=payload.get("location"),
            external_ids={"source_id": staging.source_id, "incident_number": incident_number},
            data=payload,
        )
        self.session.add(incident)
        self.session.flush()
        self._link_entity("staging", staging.id, "incident", incident.id, "derived_from", join_key=str(incident_number))
        mark_ready(self.session, staging.id)

    def process_arrest(self, staging: StagingRecord) -> None:
        payload = staging.payload.get("attributes", staging.payload.get("row", {}))
        booking_number = payload.get("booking_number") or payload.get("arrest_number")
        if not booking_number:
            suspend_staging(
                self.session,
                staging.id,
                "Missing booking_number",
                required_entity_type="arrest",
                required_key="booking_number",
                required_value="UNKNOWN",
            )
            return

        person_name = payload.get("person_name") or payload.get("name")
        person = None
        if person_name:
            # Only create person if explicit name present
            person = Person(first_name=person_name, last_name="")
            self.session.add(person)
            self.session.flush()

        arrest = Arrest(
            booking_number=str(booking_number),
            arrested_at=payload.get("arrested_at"),
            person_id=person.id if person else None,
            external_ids={"source_id": staging.source_id, "booking_number": booking_number},
        )
        self.session.add(arrest)
        self.session.flush()
        self._link_entity("staging", staging.id, "arrest", arrest.id, "derived_from", join_key=str(booking_number))
        mark_ready(self.session, staging.id)

    def process_use_of_force(self, staging: StagingRecord) -> None:
        payload = staging.payload.get("attributes", staging.payload.get("row", {}))
        incident_number = payload.get("incident_number") or payload.get("case_number") or payload.get("id")
        officer_badge = payload.get("officer_badge_number") or payload.get("badge_number")
        officer_employee_id = payload.get("officer_employee_id") or payload.get("employee_id")

        # Explicit joining keys required for officer linkage
        if not officer_badge and not officer_employee_id:
            suspend_staging(
                self.session,
                staging.id,
                "Missing officer_badge_number/officer_employee_id for UOF",
                required_entity_type="officer",
                required_key="badge_number",
                required_value=str(payload.get("officer_name", "UNKNOWN")),
            )
            return

        officer = None
        if officer_employee_id:
            officer = self._find_officer_by_key("employee_id", officer_employee_id)
        if not officer and officer_badge:
            officer = self._find_officer_by_key("badge_number", officer_badge)

        if not officer:
            suspend_staging(
                self.session,
                staging.id,
                f"Officer not found for badge/employee_id {officer_badge or officer_employee_id}",
                required_entity_type="officer",
                required_key="badge_number",
                required_value=str(officer_badge or officer_employee_id),
            )
            return

        # Incident must exist or be created later. We suspend if no incident number.
        if not incident_number:
            suspend_staging(
                self.session,
                staging.id,
                "Missing incident_number for UOF",
                required_entity_type="incident",
                required_key="external_id",
                required_value="UNKNOWN",
            )
            return

        agency_name = payload.get("agency_name", "Phoenix Police Department")
        agency = self._get_or_create_agency(agency_name)
        incident = Incident(
            agency_id=agency.id,
            incident_type=staging.entity_type,
            occurred_at=payload.get("date_time"),
            location=payload.get("location"),
            external_ids={"source_id": staging.source_id, "incident_number": incident_number},
            data=payload,
        )
        self.session.add(incident)
        self.session.flush()
        self._link_entity("staging", staging.id, "incident", incident.id, "derived_from", join_key=str(incident_number))
        self._link_entity("officer", officer.id, "incident", incident.id, "involved_in", join_key=str(officer_badge or officer_employee_id))
        mark_ready(self.session, staging.id)

    def process_officer(self, staging: StagingRecord) -> None:
        payload = staging.payload.get("row", staging.payload.get("attributes", {}))
        badge = payload.get("badge_number") or payload.get("badge")
        employee_id = payload.get("employee_id") or payload.get("officer_id")
        if not badge and not employee_id:
            suspend_staging(
                self.session,
                staging.id,
                "Missing badge_number/employee_id for officer",
                required_entity_type="officer",
                required_key="badge_number",
                required_value=str(payload.get("name", "UNKNOWN")),
            )
            return

        agency_name = payload.get("agency_name", "Unknown")
        agency = self._get_or_create_agency(agency_name)
        officer = Officer(
            agency_id=agency.id,
            first_name=payload.get("first_name"),
            last_name=payload.get("last_name"),
            badge_number=str(badge) if badge else None,
            employee_id=str(employee_id) if employee_id else None,
            external_ids={"source_id": staging.source_id},
            status=payload.get("status"),
        )
        self.session.add(officer)
        self.session.flush()
        self._link_entity("staging", staging.id, "officer", officer.id, "derived_from", join_key=str(badge or employee_id))
        mark_ready(self.session, staging.id)

    def process_court_case(self, staging: StagingRecord) -> None:
        payload = staging.payload.get("docket", staging.payload.get("row", {}))
        case_number = payload.get("docket_number") or payload.get("case_number")
        if not case_number:
            suspend_staging(
                self.session,
                staging.id,
                "Missing docket_number/case_number",
                required_entity_type="court_case",
                required_key="case_number",
                required_value="UNKNOWN",
            )
            return

        court_case = CourtCase(
            case_number=str(case_number),
            court=payload.get("court"),
            filed_at=payload.get("date_filed"),
            status=payload.get("status"),
            external_ids={"source_id": staging.source_id},
        )
        self.session.add(court_case)
        self.session.flush()
        self._link_entity("staging", staging.id, "court_case", court_case.id, "derived_from", join_key=str(case_number))
        mark_ready(self.session, staging.id)

    def process_document(self, staging: StagingRecord) -> None:
        payload = staging.payload
        doc = Document(
            source_id=staging.source_id,
            doc_type=staging.entity_type or "document",
            title=payload.get("file_name") or payload.get("title"),
            file_path=staging.raw_record.file_path if staging.raw_record else None,
            text=payload.get("text"),
            external_ids={"source_id": staging.source_id},
        )
        self.session.add(doc)
        self.session.flush()
        self._link_entity("staging", staging.id, "document", doc.id, "derived_from")
        mark_ready(self.session, staging.id)

    def process_news(self, staging: StagingRecord) -> None:
        entry = staging.payload.get("entry", {})
        article = NewsArticle(
            source_id=staging.source_id,
            title=entry.get("title", "Untitled"),
            url=entry.get("link"),
            published_at=entry.get("published"),
            content=entry.get("summary"),
            external_ids={"source_id": staging.source_id},
        )
        self.session.add(article)
        self.session.flush()
        self._link_entity("staging", staging.id, "news_article", article.id, "derived_from")
        mark_ready(self.session, staging.id)

    def process_monitor_report(self, staging: StagingRecord) -> None:
        payload = staging.payload
        report = MonitorReport(
            agency_id=None,
            period=payload.get("period"),
            report_date=payload.get("published_at"),
            compliance_data=payload,
            document_id=None,
        )
        self.session.add(report)
        self.session.flush()
        self._link_entity("staging", staging.id, "monitor_report", report.id, "derived_from")
        mark_ready(self.session, staging.id)

    def process_surveillance_event(self, staging: StagingRecord) -> None:
        payload = staging.payload.get("row", staging.payload.get("attributes", {}))
        agency_name = payload.get("agency_name", "Unknown")
        agency = self._get_or_create_agency(agency_name)
        event = SurveillanceEvent(
            agency_id=agency.id,
            event_type=staging.entity_type or payload.get("event_type", "alpr"),
            occurred_at=payload.get("date_time"),
            location=payload.get("location"),
            metadata=payload,
        )
        self.session.add(event)
        self.session.flush()
        self._link_entity("staging", staging.id, "surveillance_event", event.id, "derived_from")
        mark_ready(self.session, staging.id)

    def process_unknown(self, staging: StagingRecord) -> None:
        # Create a generic document to preserve the raw record.
        doc = Document(
            source_id=staging.source_id,
            doc_type="raw_staging",
            title=f"Staging {staging.id}",
            text=json.dumps(staging.payload, default=str)[:5000],
            external_ids={"staging_id": staging.id},
        )
        self.session.add(doc)
        self.session.flush()
        self._link_entity("staging", staging.id, "document", doc.id, "derived_from")
        mark_ready(self.session, staging.id)

    def process_staging_record(self, staging: StagingRecord) -> None:
        entity_type = staging.entity_type
        if entity_type in ("incident", "death"):
            self.process_incident(staging)
        elif entity_type == "arrest":
            self.process_arrest(staging)
        elif entity_type in ("use_of_force", "officer_involved_shooting"):
            self.process_use_of_force(staging)
        elif entity_type in ("officer", "officer_certification"):
            self.process_officer(staging)
        elif entity_type in ("court_case", "public_records_request"):
            self.process_court_case(staging)
        elif entity_type in ("document",):
            self.process_document(staging)
        elif entity_type in ("news", "sentiment_survey"):
            self.process_news(staging)
        elif entity_type == "monitor_report":
            self.process_monitor_report(staging)
        elif entity_type == "surveillance_event":
            self.process_surveillance_event(staging)
        else:
            self.process_unknown(staging)

    def run(self) -> dict[str, Any]:
        pending = self.session.scalars(select(StagingRecord).where(StagingRecord.status.in_(["pending", "suspended"]))).all()
        stats = {"processed": 0, "suspended": 0, "failed": 0}
        for staging in pending:
            try:
                self.process_staging_record(staging)
                stats["processed"] += 1
            except Exception as exc:
                mark_failed(self.session, staging.id, str(exc))
                stats["failed"] += 1

        self.run.completed_at = datetime.utcnow()
        self.run.status = "completed"
        self.run.stats = stats
        self.session.commit()
        return stats
