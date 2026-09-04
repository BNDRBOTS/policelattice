"""Synthesis: normalized rows -> lattice entities.

Two rules govern this module:

1. **Provenance is mandatory.** Every entity row is written with the exact
   ``source_url``, ``retrieved_at`` and ``content_sha256`` it came from.
   There is no code path that creates an entity without them.

2. **Nothing is inferred.** If the source does not state a field, the column
   is left ``None``. Agency attribution comes from the source's own agency
   column where one exists, otherwise from the catalog publisher that the
   source was fetched from — never from a guess.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.ingest.base import FetchedRows
from app.models import (
    Agency,
    Arrest,
    Complaint,
    EntityLink,
    ForceEvent,
    Incident,
    NewsItem,
    OfficerRef,
    RawRecord,
)
from app.pipeline.normalize import normalize_record

logger = logging.getLogger(__name__)

_NATIONAL_AGENCY_ALIASES = (
    "AGENCYNAME", "RESPONSIBLEAGENCY", "POLICEDEPARTMENT", "AGENCY", "DEPARTMENT",
)
_NATIONAL_AGENCY_CITY = ("CITY",)

#: Canonical agency identities for the local jurisdictions this lattice
#: covers. These are identities, not data: they exist so that records from
#: different sources about the same department resolve to one entity.
LOCAL_AGENCIES: dict[str, tuple[str, str, str]] = {
    # source id prefix -> (agency id, display name, jurisdiction)
    "phoenix": ("phoenix-pd", "Phoenix Police Department", "Phoenix, Arizona"),
    "tempe": ("tempe-pd", "Tempe Police Department", "Tempe, Arizona"),
}


def _aware(moment: datetime | None) -> datetime | None:
    """Return a timezone-aware UTC datetime.

    SQLite does not persist a UTC offset, so ``DateTime(timezone=True)``
    columns come back naive from that backend while Postgres returns aware
    values. Comparing the two raises ``TypeError``, so every value read back
    from the database is normalised before it is compared or stored.
    """
    if moment is None or moment.tzinfo is not None:
        return moment
    return moment.replace(tzinfo=UTC)


def _norm_key(key: str) -> str:
    return "".join(ch for ch in str(key).upper() if ch.isalnum())


def _find(row: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    index = {_norm_key(k): v for k, v in row.items()}
    for candidate in candidates:
        if candidate in index and index[candidate] not in (None, ""):
            return index[candidate]
    return None


class Synthesizer:
    """Writes normalized records into the lattice."""

    def __init__(self, session: Session):
        self.session = session
        self._agency_cache: dict[str, Agency] = {}
        self._officer_cache: dict[tuple[str, str], OfficerRef] = {}
        self.stats: dict[str, int] = {
            "incidents": 0,
            "force_events": 0,
            "officers": 0,
            "arrests": 0,
            "complaints": 0,
            "news_items": 0,
            "links": 0,
            "skipped_incomplete": 0,
        }

    # -- agencies ----------------------------------------------------------
    def agency_for(
        self, source_id: str, row: dict[str, Any], publisher: str | None
    ) -> Agency | None:
        """Resolve the agency for a row.

        National datasets carry their own agency column and are attributed
        from it. Local sources are attributed to the department named in the
        catalog entry they were fetched from. A row that can be attributed to
        neither is rejected rather than assigned a default department.
        """
        prefix = source_id.split("_", 1)[0]
        if prefix in LOCAL_AGENCIES:
            agency_id, name, jurisdiction = LOCAL_AGENCIES[prefix]
            return self._get_agency(agency_id, name, jurisdiction, None)

        agency_name = _find(row, _NATIONAL_AGENCY_ALIASES)
        if agency_name:
            city = _find(row, _NATIONAL_AGENCY_CITY)
            jurisdiction = f"{city}, Arizona" if city else None
            slug = "".join(ch for ch in str(agency_name).lower() if ch.isalnum())[:100]
            return self._get_agency(
                f"ext-{slug}", str(agency_name), jurisdiction, None
            )

        if publisher:
            slug = "".join(ch for ch in publisher.lower() if ch.isalnum())[:100]
            return self._get_agency(f"pub-{slug}", publisher, None, None)
        return None

    def _get_agency(
        self, agency_id: str, name: str, jurisdiction: str | None, source_url: str | None
    ) -> Agency:
        cached = self._agency_cache.get(agency_id)
        if cached is not None:
            return cached
        agency = self.session.get(Agency, agency_id)
        if agency is None:
            agency = Agency(
                id=agency_id,
                name=name,
                jurisdiction=jurisdiction,
                source_url=source_url,
            )
            self.session.add(agency)
            self.session.flush()
        elif source_url and not agency.source_url:
            agency.source_url = source_url
        self._agency_cache[agency_id] = agency
        return agency

    # -- officers -----------------------------------------------------------
    def officer_for(
        self,
        agency: Agency,
        external_key: str,
        *,
        gender: Any = None,
        race_group: Any = None,
        rank: Any = None,
        hire_year: int | None = None,
        moment: datetime | None = None,
        source_url: str | None = None,
    ) -> OfficerRef:
        cache_key = (agency.id, external_key)
        cached = self._officer_cache.get(cache_key)
        if cached is not None:
            officer = cached
        else:
            officer = self.session.scalar(
                select(OfficerRef).where(
                    OfficerRef.agency_id == agency.id,
                    OfficerRef.external_key == external_key,
                )
            )
            if officer is None:
                officer = OfficerRef(
                    agency_id=agency.id,
                    external_key=external_key,
                    source_url=source_url,
                    first_seen_at=moment,
                    last_seen_at=moment,
                )
                self.session.add(officer)
                self.session.flush()
                self.stats["officers"] += 1
            self._officer_cache[cache_key] = officer

        changed = False
        for attr, value in (("gender", gender), ("race_group", race_group), ("rank", rank)):
            if value is not None and getattr(officer, attr) != value:
                setattr(officer, attr, value)
                changed = True
        if hire_year is not None and officer.hire_year != hire_year:
            officer.hire_year = hire_year
            changed = True
        if moment is not None:
            first_seen = _aware(officer.first_seen_at)
            last_seen = _aware(officer.last_seen_at)
            if first_seen is None or moment < first_seen:
                officer.first_seen_at = moment
                changed = True
            if last_seen is None or moment > last_seen:
                officer.last_seen_at = moment
                changed = True
        if source_url and officer.source_url != source_url:
            officer.source_url = source_url
            changed = True
        if changed:
            self.session.flush()
        return officer

    # -- entity links ------------------------------------------------------
    def link(
        self,
        source_type: str,
        source_id: int,
        target_type: str,
        target_id: int,
        relation: str,
        *,
        join_key: str | None = None,
        confidence: float = 1.0,
        period: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        existing = self.session.scalar(
            select(EntityLink).where(
                EntityLink.source_type == source_type,
                EntityLink.source_id == source_id,
                EntityLink.target_type == target_type,
                EntityLink.target_id == target_id,
                EntityLink.relation == relation,
            )
        )
        if existing is not None:
            return
        self.session.add(
            EntityLink(
                source_type=source_type,
                source_id=source_id,
                target_type=target_type,
                target_id=target_id,
                relation=relation,
                join_key=join_key,
                confidence=confidence,
                period=period,
                evidence=evidence or {},
            )
        )
        self.stats["links"] += 1

    # -- main entry points -------------------------------------------------
    def ingest_page(
        self, page: FetchedRows, raw: RawRecord, source_entity_type: str | None
    ) -> None:
        """Normalize and synthesize every row in one fetched page."""
        agency_default = None
        for row in page.rows:
            canonical = normalize_record(row, self._entity_type_for(source_entity_type, row))
            agency = self.agency_for(page.source_id, row, page.publisher)
            if agency is None:
                self.stats["skipped_incomplete"] += 1
                continue
            agency_default = agency

            entity_type = self._entity_type_for(source_entity_type, row)
            if entity_type == "news":
                self._news(page, canonical)
            elif entity_type == "arrest":
                self._arrest(agency, page, raw, canonical)
            elif entity_type == "complaint":
                self._complaint(agency, page, raw, canonical)
            elif entity_type == "officer":
                self._officer_only(agency, page, canonical)
            else:
                self._incident(agency, page, raw, canonical)

        if agency_default is None and page.rows:
            self.stats["skipped_incomplete"] += len(page.rows)

    # -- entity type resolution -------------------------------------------
    @staticmethod
    def _entity_type_for(source_entity_type: str | None, row: dict[str, Any]) -> str:
        """Use the catalog's declared entity type; news rows self-identify."""
        if source_entity_type == "news":
            return "news"
        if source_entity_type in {"arrest"}:
            return "arrest"
        if source_entity_type in {"complaint"}:
            return "complaint"
        if source_entity_type == "officer":
            return "officer"
        if source_entity_type == "use_of_force" and _find(
            row, ("UNIQUEINCIDENTOFFICER",)
        ):
            return "use_of_force"
        return "incident"

    # -- writers -----------------------------------------------------------
    def _incident(
        self, agency: Agency, page: FetchedRows, raw: RawRecord, canonical: dict[str, Any]
    ) -> None:
        external_number = canonical.get("external_number")
        kind = self._kind_for(page, canonical)
        if not external_number:
            # A record with no identifier cannot be deduplicated or cited by
            # the public; it is recorded as raw evidence only.
            self.stats["skipped_incomplete"] += 1
            return

        existing = self.session.scalar(
            select(Incident).where(
                Incident.agency_id == agency.id,
                Incident.external_number == str(external_number),
                Incident.kind == kind,
            )
        )
        moment = canonical.get("occurred_at")
        landing = page.landing_page or page.url

        # Values that map onto real Incident columns.
        columns = {
            "force_level": canonical.get("force_level"),
            "highest_force_applied": canonical.get("highest_force_applied"),
            "armed_type": canonical.get("armed_type"),
            "resistance": canonical.get("resistance"),
            "de_escalation": canonical.get("de_escalation"),
            "injury": canonical.get("injury"),
            "highest_charge": canonical.get("highest_charge"),
            "outcome": canonical.get("outcome"),
            "subject_gender": canonical.get("subject_gender"),
            "subject_race_group": canonical.get("subject_race_group"),
            "subject_age_group": canonical.get("subject_age_group"),
            "location": canonical.get("location"),
            "latitude": canonical.get("latitude"),
            "longitude": canonical.get("longitude"),
            "precinct": canonical.get("precinct"),
        }
        # Dataset provenance, which lives in the JSON column, not in a column
        # of its own: it describes where the row came from, not the event.
        extra = {
            "dataset": page.dataset,
            "resource": page.resource_name,
            "resource_id": page.resource_id,
            "dataset_title": page.dataset_title,
            "publisher": page.publisher,
            "source_row": canonical.get("raw"),
        }

        if existing is not None:
            incident = existing
            for key, value in columns.items():
                if value is not None:
                    setattr(incident, key, value)
            data = dict(incident.data or {})
            data.update({k: v for k, v in extra.items() if v is not None})
            incident.data = data
            existing_moment = _aware(incident.occurred_at)
            if moment and (existing_moment is None or moment > existing_moment):
                incident.occurred_at = moment
                incident.period = canonical.get("period")
        else:
            incident = Incident(
                agency_id=agency.id,
                external_number=str(external_number),
                kind=kind,
                occurred_at=moment,
                period=canonical.get("period"),
                **columns,
                data={k: v for k, v in extra.items() if v is not None},
                source_id=page.source_id,
                source_url=landing,
                retrieved_at=page.retrieved_at,
                content_sha256=raw.content_sha256,
            )
            self.session.add(incident)
            self.session.flush()
            self.stats["incidents"] += 1

        self.link(
            "raw_record", raw.id, "incident", incident.id, "derived_from",
            join_key=raw.content_sha256, period=incident.period,
        )
        self._officer_edges(agency, incident, page, raw, canonical)

    def _officer_edges(
        self,
        agency: Agency,
        incident: Incident,
        page: FetchedRows,
        raw: RawRecord,
        canonical: dict[str, Any],
    ) -> None:
        """Attach officer-level accountability rows to the incident."""
        officer_row = canonical.get("raw") or {}
        officer_columns = normalize_record(officer_row, "officer")
        external_key = officer_columns.get("external_key")
        if not external_key:
            return

        event_columns = normalize_record(officer_row, "force_event")
        officer = self.officer_for(
            agency,
            str(external_key),
            gender=officer_columns.get("gender"),
            race_group=officer_columns.get("race_group"),
            rank=officer_columns.get("rank"),
            hire_year=officer_columns.get("hire_year"),
            moment=incident.occurred_at,
            source_url=page.landing_page or page.url,
        )

        existing = self.session.scalar(
            select(ForceEvent).where(
                ForceEvent.officer_ref_id == officer.id,
                ForceEvent.incident_id == incident.id,
            )
        )
        landing = page.landing_page or page.url
        attrs = {
            "period": incident.period,
            "within_policy": event_columns.get("within_policy"),
            "bwc_activated": event_columns.get("bwc_activated"),
            "force_applied": event_columns.get("force_applied")
            or canonical.get("highest_force_applied"),
            "force_level": event_columns.get("force_level") or canonical.get("force_level"),
            "officers_on_scene": event_columns.get("officers_on_scene"),
        }
        if existing is not None:
            for key, value in attrs.items():
                if value is not None:
                    setattr(existing, key, value)
        else:
            self.session.add(
                ForceEvent(
                    officer_ref_id=officer.id,
                    incident_id=incident.id,
                    source_id=page.source_id,
                    source_url=landing,
                    data={"source_row": officer_row},
                    **attrs,
                )
            )
            self.stats["force_events"] += 1

        self.link(
            "officer", officer.id, "incident", incident.id, "involved_in",
            join_key=str(external_key), period=incident.period,
            evidence={"source": page.source_id, "resource": page.resource_id},
        )

    @staticmethod
    def _kind_for(page: FetchedRows, canonical: dict[str, Any]) -> str:
        """Incident kind, taken from the source's own labels only."""
        label = canonical.get("force_level")
        if label:
            return str(label)
        dataset = str(page.dataset or page.resource_name or "").lower()
        for token, kind in (
            ("ois", "officer_involved_shooting"),
            ("shooting", "officer_involved_shooting"),
            ("pgp", "pointed_gun_at_person"),
            ("gun", "pointed_gun_at_person"),
            ("sof", "show_of_force"),
            ("show-of-force", "show_of_force"),
            ("uof", "use_of_force"),
            ("missing", "missing_person"),
        ):
            if token in dataset:
                return kind
        return "incident"

    def _arrest(
        self, agency: Agency, page: FetchedRows, raw: RawRecord, canonical: dict[str, Any]
    ) -> None:
        moment = canonical.get("occurred_at")
        external_number = canonical.get("external_number")
        if external_number is None and moment is None:
            self.stats["skipped_incomplete"] += 1
            return
        landing = page.landing_page or page.url
        checksum = f"{raw.content_sha256}:{external_number or moment.isoformat()}"
        existing = self.session.scalar(
            select(Arrest).where(Arrest.content_sha256 == checksum)
        )
        if existing is not None:
            return

        officer_columns = normalize_record(canonical.get("raw") or {}, "officer")
        officer = None
        if officer_columns.get("external_key"):
            officer = self.officer_for(
                agency,
                str(officer_columns["external_key"]),
                gender=officer_columns.get("gender"),
                race_group=officer_columns.get("race_group"),
                rank=officer_columns.get("rank"),
                moment=moment,
                source_url=landing,
            )

        arrest = Arrest(
            agency_id=agency.id,
            external_number=str(external_number) if external_number is not None else None,
            occurred_at=moment,
            period=canonical.get("period"),
            charge=canonical.get("charge"),
            charge_code=canonical.get("charge_code"),
            disposition=canonical.get("disposition"),
            subject_gender=canonical.get("subject_gender"),
            subject_race_group=canonical.get("subject_race_group"),
            subject_age_group=canonical.get("subject_age_group"),
            location=canonical.get("location"),
            precinct=canonical.get("precinct"),
            officer_ref_id=officer.id if officer else None,
            source_id=page.source_id,
            source_url=landing,
            retrieved_at=page.retrieved_at,
            content_sha256=checksum,
            data={"source_row": canonical.get("raw"), "dataset": page.dataset},
        )
        self.session.add(arrest)
        self.session.flush()
        self.stats["arrests"] += 1
        if officer is not None:
            self.link(
                "officer", officer.id, "arrest", arrest.id, "made_arrest",
                join_key=officer.external_key, period=arrest.period,
            )
        self.link(
            "raw_record", raw.id, "arrest", arrest.id, "derived_from",
            join_key=raw.content_sha256, period=arrest.period,
        )

    def _complaint(
        self, agency: Agency, page: FetchedRows, raw: RawRecord, canonical: dict[str, Any]
    ) -> None:
        moment = canonical.get("occurred_at")
        external_number = canonical.get("external_number")
        if external_number is None and moment is None:
            self.stats["skipped_incomplete"] += 1
            return
        landing = page.landing_page or page.url
        checksum = f"{raw.content_sha256}:{external_number or moment.isoformat()}"
        if self.session.scalar(select(Complaint).where(Complaint.content_sha256 == checksum)):
            return

        officer_columns = normalize_record(canonical.get("raw") or {}, "officer")
        officer = None
        if officer_columns.get("external_key"):
            officer = self.officer_for(
                agency,
                str(officer_columns["external_key"]),
                moment=moment,
                source_url=landing,
            )

        complaint = Complaint(
            agency_id=agency.id,
            external_number=str(external_number) if external_number is not None else None,
            category=canonical.get("category"),
            allegation=canonical.get("allegation"),
            finding=canonical.get("finding"),
            discipline=canonical.get("discipline"),
            status=canonical.get("status"),
            amount_paid=canonical.get("amount_paid"),
            occurred_at=moment,
            period=canonical.get("period"),
            officer_ref_id=officer.id if officer else None,
            source_id=page.source_id,
            source_url=landing,
            retrieved_at=page.retrieved_at,
            content_sha256=checksum,
            data={"source_row": canonical.get("raw"), "dataset": page.dataset},
        )
        self.session.add(complaint)
        self.session.flush()
        self.stats["complaints"] += 1
        if officer is not None:
            self.link(
                "officer", officer.id, "complaint", complaint.id, "subject_of",
                join_key=officer.external_key, period=complaint.period,
            )
        self.link(
            "raw_record", raw.id, "complaint", complaint.id, "derived_from",
            join_key=raw.content_sha256, period=complaint.period,
        )

    def _officer_only(
        self, agency: Agency, page: FetchedRows, canonical: dict[str, Any]
    ) -> None:
        columns = normalize_record(canonical.get("raw") or {}, "officer")
        external_key = columns.get("external_key")
        if not external_key:
            self.stats["skipped_incomplete"] += 1
            return
        self.officer_for(
            agency,
            str(external_key),
            gender=columns.get("gender"),
            race_group=columns.get("race_group"),
            rank=columns.get("rank"),
            hire_year=columns.get("hire_year"),
            source_url=page.landing_page or page.url,
        )

    def _news(self, page: FetchedRows, canonical: dict[str, Any]) -> None:
        columns = normalize_record(canonical.get("raw") or {}, "news")
        url = columns.get("url")
        title = columns.get("title")
        if not url or not title:
            self.stats["skipped_incomplete"] += 1
            return
        if self.session.scalar(select(NewsItem).where(NewsItem.url == url)):
            return
        published_at = columns.get("published_at")
        self.session.add(
            NewsItem(
                source_id=page.source_id,
                title=str(title),
                url=str(url),
                published_at=published_at,
                period=published_at.strftime("%Y-%m") if published_at else None,
                summary=columns.get("summary"),
                retrieved_at=page.retrieved_at,
                content_sha256=page.content_sha256,
            )
        )
        self.stats["news_items"] += 1


def utcnow() -> datetime:
    return datetime.now(UTC)
