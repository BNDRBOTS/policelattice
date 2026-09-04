from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

# Precompiled regexes for fast normalization
_WS_RE = re.compile(r"\s+")
_BADGE_CLEAN_RE = re.compile(r"[^A-Za-z0-9-]")
_STREET_ABBREV_RE = [
    (re.compile(r"\bAvenue\b\.?", re.IGNORECASE), "Ave"),
    (re.compile(r"\bStreet\b\.?", re.IGNORECASE), "St"),
    (re.compile(r"\bRoad\b\.?", re.IGNORECASE), "Rd"),
    (re.compile(r"\bDrive\b\.?", re.IGNORECASE), "Dr"),
    (re.compile(r"\bBoulevard\b\.?", re.IGNORECASE), "Blvd"),
    (re.compile(r"\bParkway\b\.?", re.IGNORECASE), "Pkwy"),
    (re.compile(r"\bLane\b\.?", re.IGNORECASE), "Ln"),
    (re.compile(r"\bCourt\b\.?", re.IGNORECASE), "Ct"),
    (re.compile(r"\bCircle\b\.?", re.IGNORECASE), "Cir"),
    (re.compile(r"\bHighway\b\.?", re.IGNORECASE), "Hwy"),
]
_INTERSECTION_RE = re.compile(r"\s+(?:and|at|/)\s+", re.IGNORECASE)

# Agency aliases mapping to standard canonical names
AGENCY_CANONICAL_MAP: dict[str, str] = {
    "phoenix police department": "Phoenix Police Department",
    "phoenix police": "Phoenix Police Department",
    "city of phoenix police department": "Phoenix Police Department",
    "phx pd": "Phoenix Police Department",
    "phx police": "Phoenix Police Department",
    "phoenix pd": "Phoenix Police Department",
    "ppd": "Phoenix Police Department",
    "tempe police department": "Tempe Police Department",
    "tempe police": "Tempe Police Department",
    "city of tempe police department": "Tempe Police Department",
    "tempe pd": "Tempe Police Department",
    "tpd": "Tempe Police Department",
    "maricopa county sheriff's office": "Maricopa County Sheriff's Office",
    "maricopa county sheriff": "Maricopa County Sheriff's Office",
    "mcso": "Maricopa County Sheriff's Office",
    "maricopa sheriff": "Maricopa County Sheriff's Office",
    "arizona department of public safety": "Arizona Department of Public Safety",
    "arizona dps": "Arizona Department of Public Safety",
    "az dps": "Arizona Department of Public Safety",
    "azpd": "Arizona Department of Public Safety",
    "dps": "Arizona Department of Public Safety",
    "state troopers": "Arizona Department of Public Safety",
    "mesa police department": "Mesa Police Department",
    "mesa police": "Mesa Police Department",
    "mesa pd": "Mesa Police Department",
    "buckeye police department": "Buckeye Police Department",
    "buckeye police": "Buckeye Police Department",
    "buckeye pd": "Buckeye Police Department",
    "scottsdale police department": "Scottsdale Police Department",
    "scottsdale police": "Scottsdale Police Department",
    "scottsdale pd": "Scottsdale Police Department",
    "chandler police department": "Chandler Police Department",
    "chandler police": "Chandler Police Department",
    "chandler pd": "Chandler Police Department",
    "glendale police department": "Glendale Police Department",
    "glendale police": "Glendale Police Department",
    "glendale pd": "Glendale Police Department",
}

# Known police rank expansions
RANK_EXPANSIONS: dict[str, str] = {
    "officer": "Officer",
    "ofc": "Officer",
    "detective": "Detective",
    "det": "Detective",
    "sergeant": "Sergeant",
    "sgt": "Sergeant",
    "lieutenant": "Lieutenant",
    "lt": "Lieutenant",
    "captain": "Captain",
    "capt": "Captain",
    "commander": "Commander",
    "cmd": "Commander",
    "cmdr": "Commander",
    "chief": "Chief",
    "deputy": "Deputy",
    "dep": "Deputy",
    "trooper": "Trooper",
    "trp": "Trooper",
    "special agent": "Special Agent",
    "sa": "Special Agent",
}


@dataclass
class NormalizedRecord:
    """Canonical representation of an ingested and normalized record."""

    canonical_type: str
    canonical_payload: dict[str, Any]
    raw_payload: dict[str, Any] = field(default_factory=dict)
    source_id: str | None = None
    confidence: float = 1.0


def normalize_datetime(value: Any) -> datetime | None:
    """Parse any datetime representation into a timezone-aware UTC datetime.

    Handles:
    - ISO 8601 strings
    - Unix timestamps (seconds or milliseconds)
    - RFC 2822 / RSS pubDate strings
    - Standard US date formats (MM/DD/YYYY, MM-DD-YYYY, etc.)
    """
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    # Handle numeric timestamps (Unix ms or sec)
    if isinstance(value, (int, float)):
        try:
            ts = value / 1000.0 if value > 1e11 else float(value)
            return datetime.fromtimestamp(ts, tz=UTC)
        except (ValueError, OSError, OverflowError):
            return None

    val_str = str(value).strip()
    if not val_str:
        return None

    # Try numeric string (timestamp)
    if val_str.isdigit() or (val_str.replace(".", "", 1).isdigit() and len(val_str) > 8):
        try:
            num = float(val_str)
            ts = num / 1000.0 if num > 1e11 else num
            return datetime.fromtimestamp(ts, tz=UTC)
        except (ValueError, OSError, OverflowError):
            pass

    # Try ISO 8601
    try:
        dt = datetime.fromisoformat(val_str.replace("Z", "+00:00"))
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    except (ValueError, TypeError):
        pass

    # Try common format strings
    date_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M:%S %z",
        "%B %d, %Y",
        "%b %d, %Y",
    ]

    for fmt in date_formats:
        try:
            dt = datetime.strptime(val_str, fmt)
            return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        except (ValueError, TypeError):
            continue

    return None


def normalize_agency_name(name: Any) -> str | None:
    """Standardize agency name to canonical Title Case naming convention.

    Returns None when no agency is stated in the source data. Callers must
    surface the absence explicitly (e.g. "Unattributed Agency") — they must
    never substitute a real agency name the source did not provide.
    """
    if not name:
        return None
    val = _WS_RE.sub(" ", str(name)).strip()
    lower_val = val.lower()
    return AGENCY_CANONICAL_MAP.get(lower_val, val)


def normalize_identifier(val: Any) -> str | None:
    """Clean badge number, employee ID, or case number."""
    if val is None:
        return None
    cleaned = str(val).strip()
    if cleaned.lower() in ("unknown", "null", "none", "nan", ""):
        return None
    # Strip leading '#' or 'Badge' or 'ID'
    cleaned = re.sub(r"^(?:#|badge:?|id:?|serial:?)\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip() or None


def normalize_location(location: Any) -> str | None:
    """Standardize street names, abbreviations, and intersections."""
    if not location:
        return None
    loc = _WS_RE.sub(" ", str(location)).strip()
    if not loc or loc.lower() in ("unknown", "null", "none", "nan"):
        return None

    # Replace intersection connectors
    loc = _INTERSECTION_RE.sub(" & ", loc)

    # Standardize street suffixes
    for pattern, replacement in _STREET_ABBREV_RE:
        loc = pattern.sub(replacement, loc)

    return loc


def normalize_person_name(name: Any) -> tuple[str | None, str | None, str | None]:
    """Parse name into (first_name, last_name, rank).

    Handles 'Last, First', 'Officer First Last', 'First M. Last'.
    """
    if not name:
        return None, None, None
    raw = _WS_RE.sub(" ", str(name)).strip()
    if not raw or raw.lower() in ("unknown", "null", "none", "nan"):
        return None, None, None

    rank = None
    # Check for rank prefix
    parts = raw.split()
    first_token_clean = parts[0].lower().rstrip(".")
    if len(parts) > 1 and first_token_clean in RANK_EXPANSIONS:
        rank = RANK_EXPANSIONS[first_token_clean]
        raw = " ".join(parts[1:])

    # Handle "Last, First" format
    if "," in raw:
        subparts = [p.strip() for p in raw.split(",", 1)]
        last_name = subparts[0].title()
        first_name = subparts[1].title() if len(subparts) > 1 else ""
        return first_name or None, last_name or None, rank

    # Handle "First Last" format
    name_parts = raw.split()
    if len(name_parts) == 1:
        return name_parts[0].title(), None, rank
    if len(name_parts) == 2:
        return name_parts[0].title(), name_parts[1].title(), rank
    # 3+ parts: First Middle Last
    return name_parts[0].title(), name_parts[-1].title(), rank


class CanonicalNormalizer:
    """Normalizes heterogeneous records into unified canonical schema dictionaries."""

    @classmethod
    def normalize(
        cls,
        raw_payload: dict[str, Any],
        entity_type: str = "incident",
        source_id: str | None = None,
    ) -> NormalizedRecord:
        """Extract and normalize all fields based on entity type and payload contents."""
        unpacked = cls._unpack_payload(raw_payload)
        inferred_type = cls._infer_entity_type(unpacked, entity_type)

        canonical: dict[str, Any] = {}

        if inferred_type in ("incident", "death", "calls_for_service", "general_offense"):
            canonical = cls._normalize_incident(unpacked, inferred_type, source_id)
        elif inferred_type in ("officer", "officer_certification", "personnel"):
            canonical = cls._normalize_officer(unpacked, source_id)
        elif inferred_type in ("arrest", "charge"):
            canonical = cls._normalize_arrest(unpacked, source_id)
        elif inferred_type in (
            "use_of_force",
            "officer_involved_shooting",
            "pointed_gun",
            "show_of_force",
        ):
            canonical = cls._normalize_use_of_force(unpacked, inferred_type, source_id)
        elif inferred_type in ("court_case", "public_records_request"):
            canonical = cls._normalize_court_case(unpacked, source_id)
        elif inferred_type in ("news", "sentiment_survey"):
            canonical = cls._normalize_news(unpacked, source_id)
        elif inferred_type in ("surveillance_event", "alpr"):
            canonical = cls._normalize_surveillance(unpacked, source_id)
        elif inferred_type == "monitor_report":
            canonical = cls._normalize_monitor_report(unpacked, source_id)
        else:
            canonical = cls._normalize_document(unpacked, inferred_type, source_id)

        return NormalizedRecord(
            canonical_type=inferred_type,
            canonical_payload=canonical,
            raw_payload=raw_payload,
            source_id=source_id,
        )

    @staticmethod
    def _unpack_payload(payload: dict[str, Any]) -> dict[str, Any]:
        """Extract inner dict from payload wrappers."""
        if not isinstance(payload, dict):
            return {"value": payload}
        if "canonical" in payload and isinstance(payload["canonical"], dict):
            return payload["canonical"]
        if "attributes" in payload and isinstance(payload["attributes"], dict):
            return payload["attributes"]
        if "row" in payload and isinstance(payload["row"], dict):
            return payload["row"]
        if "docket" in payload and isinstance(payload["docket"], dict):
            return payload["docket"]
        if "entry" in payload and isinstance(payload["entry"], dict):
            return payload["entry"]
        return payload

    @staticmethod
    def _infer_entity_type(data: dict[str, Any], default: str) -> str:
        """Infer canonical entity type from keys if default is generic."""
        keys = {str(k).lower() for k in data.keys()}
        if "badge_number" in keys or "officer_id" in keys:
            if "force_type" in keys or "uof_type" in keys:
                return "use_of_force"
            if "first_name" in keys or "last_name" in keys or "rank" in keys:
                return "officer"
        if "booking_number" in keys or "arrest_number" in keys:
            return "arrest"
        if "docket_number" in keys or "court" in keys:
            return "court_case"
        if "cause_of_death" in keys:
            return "incident"
        return default

    @classmethod
    def _normalize_incident(
        cls, data: dict[str, Any], entity_type: str, source_id: str | None
    ) -> dict[str, Any]:
        inc_num = (
            data.get("incident_number")
            or data.get("incident_num")
            or data.get("case_number")
            or data.get("case_no")
            or data.get("dr_number")
            or data.get("report_number")
            or data.get("cad_event_number")
            or data.get("id")
        )
        dt = normalize_datetime(
            data.get("date_time")
            or data.get("datetime")
            or data.get("occurred_at")
            or data.get("occurred_dt")
            or data.get("offense_date")
            or data.get("date")
        )
        agency = normalize_agency_name(
            data.get("agency_name") or data.get("agency") or data.get("department")
        )
        location = normalize_location(
            data.get("location") or data.get("address") or data.get("cross_streets")
        )
        first_name, last_name, _ = normalize_person_name(
            data.get("person_name") or data.get("victim_name") or data.get("name")
        )

        return {
            "incident_number": normalize_identifier(inc_num),
            "incident_type": entity_type,
            "occurred_at": dt.isoformat() if dt else None,
            "agency_name": agency,
            "location": location,
            "person_first_name": first_name,
            "person_last_name": last_name,
            "cause_of_death": data.get("cause_of_death"),
            "armed_status": data.get("armed") or data.get("armed_status"),
            "city": data.get("city"),
            "state": data.get("state"),
            "external_ids": {
                "source_id": source_id,
                "incident_number": normalize_identifier(inc_num),
            },
            "raw_attributes": data,
        }

    @classmethod
    def _normalize_officer(cls, data: dict[str, Any], source_id: str | None) -> dict[str, Any]:
        badge = normalize_identifier(
            data.get("badge_number")
            or data.get("badge")
            or data.get("badge_num")
            or data.get("pin")
        )
        emp_id = normalize_identifier(
            data.get("employee_id")
            or data.get("officer_id")
            or data.get("emp_id")
            or data.get("serial")
        )
        first_name, last_name, detected_rank = normalize_person_name(
            data.get("full_name") or data.get("name")
        )
        if not first_name and data.get("first_name"):
            first_name = str(data.get("first_name")).strip()
        if not last_name and data.get("last_name"):
            last_name = str(data.get("last_name")).strip()

        rank = data.get("rank") or detected_rank
        if isinstance(rank, str) and rank.lower().rstrip(".") in RANK_EXPANSIONS:
            rank = RANK_EXPANSIONS[rank.lower().rstrip(".")]

        agency = normalize_agency_name(data.get("agency_name") or data.get("agency"))

        return {
            "badge_number": badge,
            "employee_id": emp_id,
            "first_name": first_name,
            "last_name": last_name,
            "rank": rank,
            "agency_name": agency,
            "status": data.get("status"),
            "notes": data.get("notes"),
            "external_ids": {
                "source_id": source_id,
                "badge_number": badge,
                "employee_id": emp_id,
            },
        }

    @classmethod
    def _normalize_arrest(cls, data: dict[str, Any], source_id: str | None) -> dict[str, Any]:
        booking = normalize_identifier(
            data.get("booking_number")
            or data.get("booking_no")
            or data.get("arrest_number")
            or data.get("id")
        )
        dt = normalize_datetime(
            data.get("arrested_at") or data.get("arrest_date") or data.get("date_time")
        )
        first_name, last_name, _ = normalize_person_name(
            data.get("person_name") or data.get("name")
        )
        agency = normalize_agency_name(data.get("agency_name") or data.get("agency"))
        location = normalize_location(data.get("location") or data.get("address"))

        charges = []
        raw_statute = data.get("statute") or data.get("statutes")
        raw_charge = data.get("charge_description") or data.get("charges") or data.get("charge")
        if raw_statute or raw_charge:
            charges.append({
                "statute": str(raw_statute) if raw_statute else None,
                "description": str(raw_charge) if raw_charge else None,
                # Severity only when the source states it — never inferred.
                "severity": data.get("severity"),
            })

        return {
            "booking_number": booking,
            "arrest_number": normalize_identifier(data.get("arrest_number")),
            "arrested_at": dt.isoformat() if dt else None,
            "agency_name": agency,
            "person_first_name": first_name,
            "person_last_name": last_name,
            "location": location,
            "charges": charges,
            "external_ids": {
                "source_id": source_id,
                "booking_number": booking,
            },
        }

    @classmethod
    def _normalize_use_of_force(
        cls, data: dict[str, Any], entity_type: str, source_id: str | None
    ) -> dict[str, Any]:
        inc_num = normalize_identifier(
            data.get("incident_number")
            or data.get("case_number")
            or data.get("uof_id")
            or data.get("id")
        )
        badge = normalize_identifier(
            data.get("officer_badge_number")
            or data.get("badge_number")
            or data.get("badge")
        )
        emp_id = normalize_identifier(
            data.get("officer_employee_id")
            or data.get("employee_id")
            or data.get("officer_id")
        )
        dt = normalize_datetime(
            data.get("date_time") or data.get("occurred_at") or data.get("date")
        )
        agency = normalize_agency_name(data.get("agency_name") or data.get("agency"))
        location = normalize_location(data.get("location") or data.get("address"))

        force_type = data.get("force_type") or data.get("uof_type")

        return {
            "incident_number": inc_num,
            "officer_badge_number": badge,
            "officer_employee_id": emp_id,
            "force_type": str(force_type) if force_type else None,
            "occurred_at": dt.isoformat() if dt else None,
            "agency_name": agency,
            "location": location,
            "external_ids": {
                "source_id": source_id,
                "incident_number": inc_num,
            },
            "raw_attributes": data,
        }

    @classmethod
    def _normalize_court_case(cls, data: dict[str, Any], source_id: str | None) -> dict[str, Any]:
        docket = normalize_identifier(
            data.get("docket_number")
            or data.get("case_number")
            or data.get("docket_no")
            or data.get("id")
        )
        court = data.get("court")
        dt = normalize_datetime(data.get("date_filed") or data.get("filed_at"))
        status = data.get("status")
        title = data.get("title") or data.get("case_name") or f"Case {docket}"

        return {
            "case_number": docket,
            "court": str(court) if court else None,
            "filed_at": dt.isoformat() if dt else None,
            "status": str(status) if status else None,
            "title": str(title),
            "external_ids": {
                "source_id": source_id,
                "case_number": docket,
            },
        }

    @classmethod
    def _normalize_news(cls, data: dict[str, Any], source_id: str | None) -> dict[str, Any]:
        title = data.get("title")
        url = data.get("link") or data.get("url")
        dt = normalize_datetime(data.get("published") or data.get("published_at"))
        summary = data.get("summary") or data.get("content") or data.get("description")

        return {
            "title": str(title) if title else None,
            "url": str(url) if url else None,
            "published_at": dt.isoformat() if dt else None,
            "content": str(summary) if summary else None,
            "external_ids": {"source_id": source_id},
        }

    @classmethod
    def _normalize_surveillance(cls, data: dict[str, Any], source_id: str | None) -> dict[str, Any]:
        agency = normalize_agency_name(data.get("agency_name") or data.get("agency"))
        dt = normalize_datetime(data.get("date_time") or data.get("occurred_at"))
        location = normalize_location(data.get("location") or data.get("address"))
        event_type = data.get("event_type")

        return {
            "agency_name": agency,
            "event_type": str(event_type),
            "occurred_at": dt.isoformat() if dt else None,
            "location": location,
            "metadata": data,
            "external_ids": {"source_id": source_id},
        }

    @classmethod
    def _normalize_monitor_report(
        cls, data: dict[str, Any], source_id: str | None
    ) -> dict[str, Any]:
        dt = normalize_datetime(data.get("published_at") or data.get("report_date"))
        return {
            "period": data.get("period"),
            "report_date": dt.isoformat() if dt else None,
            "compliance_data": data,
            "external_ids": {"source_id": source_id},
        }

    @classmethod
    def _normalize_document(
        cls, data: dict[str, Any], doc_type: str, source_id: str | None
    ) -> dict[str, Any]:
        title = data.get("file_name") or data.get("title")
        text = data.get("text") or data.get("content") or str(data)
        dt = normalize_datetime(data.get("published_at") or data.get("created_at"))

        return {
            "title": str(title) if title else None,
            "doc_type": doc_type,
            "text": str(text),
            "published_at": dt.isoformat() if dt else None,
            "external_ids": {"source_id": source_id},
        }
