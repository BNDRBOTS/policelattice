from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import (
    Agency,
    Arrest,
    Charge,
    CourtCase,
    Document,
    EntityLink,
    Incident,
    MonitorReport,
    NewsArticle,
    Officer,
    Person,
    StagingRecord,
    SurveillanceEvent,
    SynthesisRun,
)
from app.pipeline.normalization import normalize_datetime
from app.pipeline.state import mark_failed, mark_synthesized, suspend_staging


def _utcnow() -> datetime:
    """Return current UTC time with timezone awareness."""
    return datetime.now(UTC)


def _safe_parse_datetime(value: Any) -> datetime | None:
    """Safely parse a datetime using the canonical normalizer."""
    return normalize_datetime(value)


class SynthesisEngine:
    """Maps staging records and extracted evidence into the unified lattice.

    Rules:
    - Uses canonical normalized fields and explicit joining keys.
    - Employs autonomous entity resolution fallback when keys are partially specified.
    - Correlates extracted evidence (officer mentions, force tactics, ARS statutes)
      into first-class lattice entities and topological entity links.
    """

    def __init__(self, session: Session):
        self.session = session
        self.synthesis_run = SynthesisRun(status="running", started_at=_utcnow())
        session.add(self.synthesis_run)
        session.flush()

    def _get_or_create_agency(self, name: str | None, state: str | None = "AZ") -> Agency:
        clean_name = str(name).strip() if name else ""
        if not clean_name or clean_name.lower() in ("unknown", "null", "none"):
            # Honest labeling of missing attribution — never a fabricated agency.
            clean_name = "Unattributed Agency"
        agency = self.session.scalar(select(Agency).where(Agency.name == clean_name))
        if not agency:
            agency = Agency(name=clean_name, state=state)
            self.session.add(agency)
            self.session.flush()
        return agency

    def _find_officer_by_key(self, key: str, value: Any) -> Officer | None:
        if not value:
            return None
        val_str = str(value).strip()
        if key == "badge_number":
            return self.session.scalar(select(Officer).where(Officer.badge_number == val_str))
        if key == "employee_id":
            return self.session.scalar(select(Officer).where(Officer.employee_id == val_str))
        if key == "external_ids":
            stmt = select(Officer).where(Officer.external_ids.contains({key: val_str}))
            officer = self.session.scalar(stmt)
            if officer:
                return officer
            if self.session.bind and self.session.bind.dialect.name != "postgresql":
                for off in self.session.scalars(select(Officer)).all():
                    if off.external_ids and off.external_ids.get(key) == val_str:
                        return off
        return None

    def _find_incident_by_number(self, incident_number: str) -> Incident | None:
        if not incident_number:
            return None
        stmt = select(Incident).where(
            Incident.external_ids.contains({"incident_number": incident_number})
        )
        inc = self.session.scalar(stmt)
        if inc:
            return inc
        if self.session.bind and self.session.bind.dialect.name != "postgresql":
            for item in self.session.scalars(select(Incident)).all():
                if (
                    item.external_ids
                    and item.external_ids.get("incident_number") == incident_number
                ):
                    return item
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
            metadata_=metadata or {},
        )
        self.session.add(link)
        return link

    def process_incident(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "attributes", staging.payload.get("row", staging.payload)
        )
        evidence = staging.payload.get("evidence", {})

        # Resolve incident number with fallback to evidence extraction or synthetic key
        incident_number = (
            canonical.get("incident_number")
            or canonical.get("case_number")
            or canonical.get("id")
            or canonical.get("dr_number")
            or canonical.get("report_number")
        )
        if not incident_number and evidence.get("incidents"):
            incident_number = evidence["incidents"][0].get("incident_number")
        if not incident_number:
            if not any(
                k in canonical
                for k in (
                    "location",
                    "occurred_at",
                    "date_time",
                    "occurred_dt",
                    "narrative",
                    "description",
                    "title",
                    "agency_name",
                )
            ):
                suspend_staging(
                    self.session, staging.id, "Missing incident_number and essential attributes"
                )
                return
            incident_number = f"INC-{staging.source_id}-{staging.id}"

        agency_name = canonical.get("agency_name") or "Phoenix Police Department"
        agency = self._get_or_create_agency(agency_name)

        occurred_at = normalize_datetime(
            canonical.get("occurred_at")
            or canonical.get("date_time")
            or canonical.get("occurred_dt")
        )

        loc = canonical.get("location")
        if not loc and evidence.get("locations"):
            loc = evidence["locations"][0]

        incident_data = {
            **canonical,
            "evidence": evidence,
        }

        # Check if an incident with this incident number already exists (deduplication & enrichment)
        existing_inc = self._find_incident_by_number(str(incident_number))
        if existing_inc:
            if occurred_at and not existing_inc.occurred_at:
                existing_inc.occurred_at = occurred_at
            if loc and not existing_inc.location:
                existing_inc.location = loc
            if isinstance(existing_inc.data, dict):
                merged_data = {**existing_inc.data, **incident_data}
                existing_inc.data = merged_data
            incident = existing_inc
        else:
            incident = Incident(
                agency_id=agency.id,
                incident_type=staging.entity_type or canonical.get("incident_type", "incident"),
                occurred_at=occurred_at,
                location=loc,
                external_ids={
                    "source_id": staging.source_id,
                    "incident_number": str(incident_number),
                },
                data=incident_data,
            )
            self.session.add(incident)
            self.session.flush()

        # Link staging -> incident
        self._link_entity(
            "staging",
            staging.id,
            "incident",
            incident.id,
            "derived_from",
            join_key=str(incident_number),
        )

        # Correlate extracted officers from evidence
        for off_ev in evidence.get("officers", []):
            badge = off_ev.get("badge_number")
            emp_id = off_ev.get("employee_id")
            officer = None
            if badge:
                officer = self._find_officer_by_key("badge_number", badge)
            if not officer and emp_id:
                officer = self._find_officer_by_key("employee_id", emp_id)
            if not officer and off_ev.get("full_name"):
                first = off_ev.get("first_name")
                last = off_ev.get("last_name")
                officer = self.session.scalar(
                    select(Officer).where(Officer.first_name == first, Officer.last_name == last)
                )
            if officer:
                self._link_entity(
                    "officer",
                    officer.id,
                    "incident",
                    incident.id,
                    "involved_in",
                    join_key=str(badge or emp_id or off_ev.get("full_name")),
                    confidence=off_ev.get("confidence", 0.95),
                    metadata={"evidence": off_ev},
                )

        mark_synthesized(self.session, staging.id)

    def process_arrest(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "attributes", staging.payload.get("row", staging.payload)
        )
        evidence = staging.payload.get("evidence", {})

        booking_number = (
            canonical.get("booking_number")
            or canonical.get("arrest_number")
            or f"BK-{staging.source_id}-{staging.id}"
        )

        arrested_at = normalize_datetime(
            canonical.get("arrested_at") or canonical.get("date_time")
        )

        first_name = canonical.get("person_first_name") or canonical.get("first_name")
        last_name = canonical.get("person_last_name") or canonical.get("last_name")
        if not first_name and not last_name and canonical.get("person_name"):
            parts = str(canonical.get("person_name")).split()
            first_name = parts[0] if parts else None
            last_name = parts[-1] if len(parts) > 1 else None

        person = None
        if first_name or last_name:
            person = Person(
                first_name=first_name,
                last_name=last_name,
                external_ids={
                    "source_id": staging.source_id,
                    "booking_number": str(booking_number),
                },
            )
            self.session.add(person)
            self.session.flush()

        arrest = Arrest(
            booking_number=str(booking_number),
            arrested_at=arrested_at,
            person_id=person.id if person else None,
            external_ids={"source_id": staging.source_id, "booking_number": str(booking_number)},
        )
        self.session.add(arrest)
        self.session.flush()

        # Generate Charge entities from canonical charges and extracted statutes
        charges = canonical.get("charges", [])
        for ch in charges:
            charge_obj = Charge(
                arrest_id=arrest.id,
                statute=ch.get("statute"),
                description=ch.get("description"),
                severity=ch.get("severity", "Felony"),
                external_ids={"source_id": staging.source_id},
            )
            self.session.add(charge_obj)

        for stat_ev in evidence.get("statutes", []):
            if not any(c.get("statute") == stat_ev.get("statute") for c in charges):
                charge_obj = Charge(
                    arrest_id=arrest.id,
                    statute=stat_ev.get("statute"),
                    description=stat_ev.get("title"),
                    severity=stat_ev.get("severity", "Felony"),
                    external_ids={"source_id": staging.source_id, "extracted": True},
                )
                self.session.add(charge_obj)

        self._link_entity(
            "staging",
            staging.id,
            "arrest",
            arrest.id,
            "derived_from",
            join_key=str(booking_number),
        )
        mark_synthesized(self.session, staging.id)

    def process_use_of_force(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "attributes", staging.payload.get("row", staging.payload)
        )
        evidence = staging.payload.get("evidence", {})

        incident_number = (
            canonical.get("incident_number")
            or canonical.get("case_number")
            or canonical.get("id")
            or f"UOF-{staging.source_id}-{staging.id}"
        )
        officer_badge = canonical.get("officer_badge_number") or canonical.get("badge_number")
        officer_employee_id = canonical.get("officer_employee_id") or canonical.get("employee_id")

        officer = None
        if officer_employee_id:
            officer = self._find_officer_by_key("employee_id", officer_employee_id)
        if not officer and officer_badge:
            officer = self._find_officer_by_key("badge_number", officer_badge)

        agency_name = canonical.get("agency_name", "Phoenix Police Department")
        agency = self._get_or_create_agency(agency_name)

        # If officer is not yet registered, create placeholder officer so UOF is synthesized
        if not officer and (officer_badge or officer_employee_id):
            officer = Officer(
                agency_id=agency.id,
                badge_number=str(officer_badge) if officer_badge else None,
                employee_id=str(officer_employee_id) if officer_employee_id else None,
                first_name=canonical.get("officer_first_name"),
                last_name=canonical.get("officer_last_name") or canonical.get("officer_name"),
                status="Active",
                external_ids={"source_id": staging.source_id},
            )
            self.session.add(officer)
            self.session.flush()

        occurred_at = normalize_datetime(
            canonical.get("occurred_at") or canonical.get("date_time")
        )

        existing_inc = self._find_incident_by_number(str(incident_number))
        if existing_inc:
            if occurred_at and not existing_inc.occurred_at:
                existing_inc.occurred_at = occurred_at
            if canonical.get("location") and not existing_inc.location:
                existing_inc.location = canonical.get("location")
            if isinstance(existing_inc.data, dict):
                existing_inc.data = {**existing_inc.data, **canonical, "evidence": evidence}
            incident = existing_inc
        else:
            incident = Incident(
                agency_id=agency.id,
                incident_type=staging.entity_type or canonical.get("force_type", "use_of_force"),
                occurred_at=occurred_at,
                location=canonical.get("location"),
                external_ids={
                    "source_id": staging.source_id,
                    "incident_number": str(incident_number),
                },
                data={**canonical, "evidence": evidence},
            )
            self.session.add(incident)
            self.session.flush()

        self._link_entity(
            "staging",
            staging.id,
            "incident",
            incident.id,
            "derived_from",
            join_key=str(incident_number),
        )
        if officer:
            self._link_entity(
                "officer",
                officer.id,
                "incident",
                incident.id,
                "involved_in",
                join_key=str(officer_badge or officer_employee_id or officer.id),
            )
        mark_synthesized(self.session, staging.id)

    def process_officer(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "row", staging.payload.get("attributes", staging.payload)
        )
        evidence = staging.payload.get("evidence", {})

        badge = canonical.get("badge_number") or canonical.get("badge")
        employee_id = canonical.get("employee_id") or canonical.get("officer_id")

        if not badge and not employee_id and evidence.get("officers"):
            badge = evidence["officers"][0].get("badge_number")
            employee_id = evidence["officers"][0].get("employee_id")

        if not badge and not employee_id:
            badge = f"OFF-{staging.source_id}-{staging.id}"

        agency_name = canonical.get("agency_name", "Phoenix Police Department")
        agency = self._get_or_create_agency(agency_name)

        # Check if officer with badge or employee ID already exists (deduplication & enrichment)
        existing_officer = None
        if badge:
            existing_officer = self._find_officer_by_key("badge_number", badge)
        if not existing_officer and employee_id:
            existing_officer = self._find_officer_by_key("employee_id", employee_id)

        if existing_officer:
            if canonical.get("first_name"):
                existing_officer.first_name = canonical.get("first_name")
            if canonical.get("last_name"):
                existing_officer.last_name = canonical.get("last_name")
            if canonical.get("status"):
                existing_officer.status = canonical.get("status")
            if isinstance(existing_officer.external_ids, dict):
                existing_officer.external_ids = {
                    **existing_officer.external_ids,
                    "rank": canonical.get("rank", "Officer"),
                    "notes": canonical.get("notes"),
                    "source_id": staging.source_id,
                }
            existing_officer.agency_id = agency.id
            officer = existing_officer
        else:
            officer = Officer(
                agency_id=agency.id,
                first_name=canonical.get("first_name"),
                last_name=canonical.get("last_name"),
                badge_number=str(badge) if badge else None,
                employee_id=str(employee_id) if employee_id else None,
                external_ids={
                    "source_id": staging.source_id,
                    "rank": canonical.get("rank", "Officer"),
                    "notes": canonical.get("notes"),
                },
                status=canonical.get("status", "Active"),
            )
            self.session.add(officer)
            self.session.flush()

        self._link_entity(
            "staging",
            staging.id,
            "officer",
            officer.id,
            "derived_from",
            join_key=str(badge or employee_id),
        )
        mark_synthesized(self.session, staging.id)

    def process_court_case(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "docket", staging.payload.get("row", staging.payload)
        )
        evidence = staging.payload.get("evidence", {})

        extracted_docket = None
        if evidence.get("court_cases"):
            extracted_docket = evidence["court_cases"][0].get("docket_number")

        case_number = (
            canonical.get("case_number")
            or canonical.get("docket_number")
            or extracted_docket
            or f"CASE-{staging.source_id}-{staging.id}"
        )

        court_case = CourtCase(
            case_number=str(case_number),
            court=canonical.get("court", "Maricopa County Superior Court"),
            filed_at=normalize_datetime(canonical.get("filed_at") or canonical.get("date_filed")),
            status=canonical.get("status", "Active"),
            external_ids={"source_id": staging.source_id},
        )
        self.session.add(court_case)
        self.session.flush()

        self._link_entity(
            "staging",
            staging.id,
            "court_case",
            court_case.id,
            "derived_from",
            join_key=str(case_number),
        )
        mark_synthesized(self.session, staging.id)

    def process_document(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload
        evidence = staging.payload.get("evidence", {})

        doc = Document(
            source_id=staging.source_id,
            doc_type=canonical.get("doc_type") or staging.entity_type or "document",
            title=canonical.get("title") or canonical.get("file_name") or f"Document #{staging.id}",
            file_path=staging.raw_record.file_path if staging.raw_record else None,
            text=canonical.get("text"),
            published_at=normalize_datetime(canonical.get("published_at")),
            external_ids={"source_id": staging.source_id},
        )
        self.session.add(doc)
        self.session.flush()

        self._link_entity("staging", staging.id, "document", doc.id, "derived_from")

        # Link to any extracted officers or incidents
        for inc_ev in evidence.get("incidents", []):
            inc_id = inc_ev.get("incident_number")
            inc = self._find_incident_by_number(inc_id)
            if inc:
                self._link_entity(
                    "document", doc.id, "incident", inc.id, "evidence_for", join_key=inc_id
                )

        mark_synthesized(self.session, staging.id)

    def process_news(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "entry", staging.payload
        )
        evidence = staging.payload.get("evidence", {})

        # No fabricated URLs: when the feed does not carry a link, the record
        # states that explicitly (url=None) rather than inventing one.
        article = NewsArticle(
            source_id=staging.source_id,
            title=canonical.get("title") or f"Untitled feed item (staging {staging.id})",
            url=canonical.get("url") or canonical.get("link"),
            published_at=normalize_datetime(
                canonical.get("published_at") or canonical.get("published")
            ),
            content=canonical.get("content") or canonical.get("summary"),
            external_ids={"source_id": staging.source_id},
        )
        self.session.add(article)
        self.session.flush()

        self._link_entity("staging", staging.id, "news_article", article.id, "derived_from")

        # Correlate extracted incidents from news evidence
        for inc_ev in evidence.get("incidents", []):
            inc_id = inc_ev.get("incident_number")
            inc = self._find_incident_by_number(inc_id)
            if inc:
                self._link_entity(
                    "news_article",
                    article.id,
                    "incident",
                    inc.id,
                    "reports_on",
                    join_key=inc_id,
                    confidence=inc_ev.get("confidence", 0.90),
                )

        mark_synthesized(self.session, staging.id)

    def process_monitor_report(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload
        report = MonitorReport(
            agency_id=None,
            period=canonical.get("period", "Quarterly"),
            report_date=normalize_datetime(
                canonical.get("report_date") or canonical.get("published_at")
            ),
            compliance_data=canonical.get("compliance_data", canonical),
            document_id=None,
        )
        self.session.add(report)
        self.session.flush()

        self._link_entity("staging", staging.id, "monitor_report", report.id, "derived_from")
        mark_synthesized(self.session, staging.id)

    def process_surveillance_event(self, staging: StagingRecord) -> None:
        canonical = staging.payload.get("canonical") or staging.payload.get(
            "row", staging.payload.get("attributes", staging.payload)
        )
        agency_name = canonical.get("agency_name", "Unknown")
        agency = self._get_or_create_agency(agency_name)

        occurred_at = normalize_datetime(
            canonical.get("occurred_at") or canonical.get("date_time")
        )

        event = SurveillanceEvent(
            agency_id=agency.id,
            event_type=canonical.get("event_type") or staging.entity_type or "alpr",
            occurred_at=occurred_at,
            location=canonical.get("location"),
            metadata_=canonical,
        )
        self.session.add(event)
        self.session.flush()

        self._link_entity("staging", staging.id, "surveillance_event", event.id, "derived_from")
        mark_synthesized(self.session, staging.id)

    def process_unknown(self, staging: StagingRecord) -> None:
        """Create a generic document to preserve the raw record."""
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
        mark_synthesized(self.session, staging.id)

    def process_staging_record(self, staging: StagingRecord) -> None:
        """Route a staging record to the correct processor based on entity type."""
        staging.synthesis_run_id = self.synthesis_run.id
        entity_type = staging.entity_type
        if entity_type in ("incident", "death", "calls_for_service", "general_offense"):
            self.process_incident(staging)
        elif entity_type in ("arrest", "charge"):
            self.process_arrest(staging)
        elif entity_type in (
            "use_of_force",
            "officer_involved_shooting",
            "pointed_gun",
            "show_of_force",
        ):
            self.process_use_of_force(staging)
        elif entity_type in ("officer", "officer_certification", "personnel"):
            self.process_officer(staging)
        elif entity_type in ("court_case", "public_records_request"):
            self.process_court_case(staging)
        elif entity_type in ("document",):
            self.process_document(staging)
        elif entity_type in ("news", "sentiment_survey"):
            self.process_news(staging)
        elif entity_type == "monitor_report":
            self.process_monitor_report(staging)
        elif entity_type in ("surveillance_event", "alpr"):
            self.process_surveillance_event(staging)
        else:
            self.process_unknown(staging)

    def execute(self) -> dict[str, Any]:
        """Process all organized/processed/ready/suspended staging records."""
        records = self.session.scalars(
            select(StagingRecord).where(
                StagingRecord.status.in_(
                    ["pending", "organized", "processed", "ready", "suspended"]
                )
            )
        ).all()
        stats = {"processed": 0, "suspended": 0, "failed": 0}
        for staging in records:
            try:
                self.process_staging_record(staging)
                if staging.status == "suspended":
                    stats["suspended"] += 1
                else:
                    stats["processed"] += 1
            except Exception as exc:
                mark_failed(self.session, staging.id, str(exc))
                stats["failed"] += 1

        self.synthesis_run.completed_at = _utcnow()
        self.synthesis_run.status = "completed"
        self.synthesis_run.stats = stats
        self.session.commit()
        return stats
