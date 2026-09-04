"""Canonical normalization.

Heterogeneous upstream columns are mapped to canonical lattice fields by
matching the *source's own* column names against alias sets. A value is only
ever copied across; it is never derived, defaulted, or inferred. When no
source column matches, the canonical field is ``None``.

Dates are parsed with ``dateutil`` across the formats public portals actually
emit, and every result is returned timezone-aware in UTC.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any

from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")


def _norm_key(key: str) -> str:
    """``"Simple_Subj_RE_Grp"`` -> ``"SIMPLESUBJREGRP"``."""
    return _NON_ALNUM.sub("", str(key).upper())


# Canonical field -> accepted source column names (normalized).
_INCIDENT_ALIASES: dict[str, tuple[str, ...]] = {
    "external_number": ("INCIDENTNUM", "INCIDENTNUMBER", "INCNUM", "CASENUM", "CASENUMBER",
                        "EVENTNUMBER", "REPORTNUMBER", "RMSNUM"),
    "occurred_at": ("INCIDENTDATE", "OCCURREDDATE", "OCCURREDON", "DATETIME", "DATE",
                    "INCIDENTOCCURRED", "CALLDATE", "EVENTDATE", "REPORTDATE"),
    "force_level": ("INCTYPE", "FORCELEVEL", "UOFLEVEL", "LEVEL", "RESPONSELEVEL"),
    "highest_force_applied": ("HIGHESTFORCEAPPLIED", "FORCEAPPLIED", "FORCEUSED", "FORCETYPE",
                              "WEAPONUSED", "FORCE"),
    "armed_type": ("ARMEDTYPE", "SUBJECTARMED", "WEAPONTYPE", "ARMEDWITH", "WEAPON"),
    "resistance": ("SUBJECTRESISTPRIMARY", "RESISTANCE", "RESISTLEVEL"),
    "de_escalation": ("HIGHESTDEESCALATION", "DEESCALATION", "DEESCALATIONUSED"),
    "injury": ("CITINJURYYN", "INJURY", "SUBJECTINJURY", "INJURYYN"),
    "highest_charge": ("HIGHESTCHARGE", "CHARGE", "OFFENSE", "OFFENSEDESC"),
    "outcome": ("OUTCOME", "INCIDENTOUTCOME", "DISPOSITION"),
    "subject_gender": ("SUBJECTGENDER", "SUBJECTSEX", "INDIVIDUALGENDER"),
    "subject_race_group": ("SIMPLESUBJREGRP", "SUBJECTRACE", "SUBJECTRACEGROUP",
                           "SIMPLESUBJRE", "SUBJRACE"),
    "subject_age_group": ("SUBJECTAGEGROUP", "AGEGROUP", "SUBJECTAGE"),
    "location": ("ADDRESS", "LOCATION", "CROSSSTREETS", "BLOCKADDRESS", "STREETADDRESS",
                 "INCIDENTLOCATION", "STREET"),
    "latitude": ("LATITUDE", "LAT", "Y"),
    "longitude": ("LONGITUDE", "LON", "LONG", "X"),
    "precinct": ("PRECINCT", "SECTOR", "PATROLAREA", "DISTRICT"),
}

_OFFICER_ALIASES: dict[str, tuple[str, ...]] = {
    "external_key": ("UNIQUEINCIDENTOFFICER", "OFFICERID", "EMPLID", "EMPLOYEEID",
                     "OFFICERUNIQUE", "UNIQUEOFFICER", "BADGENUMBER", "BADGE"),
    "gender": ("EMPLGENDER", "OFFICERGENDER", "EMPLOYEEGENDER"),
    "race_group": ("SIMPLEEMPLREGRP", "OFFICERRACE", "EMPLREGRP", "OFFICERRACEGROUP"),
    "rank": ("RANK", "EMPLRANK", "OFFICERRANK", "TITLE"),
    "hire_year": ("HIREYEAR", "HIREYEAR", "YEARHIRED"),
}

_FORCE_EVENT_ALIASES: dict[str, tuple[str, ...]] = {
    "within_policy": ("EMPWITHINPOLICY", "WITHINPOLICY", "POLICYOUTCOME", "WITHINPOLICYYN"),
    "bwc_activated": ("EMPBWCACTV", "BWCACTIVATED", "BWCACTIVATION", "BWC"),
    "force_applied": ("EMPHIGHESTFORCEAPPLIED", "HIGHESTFORCEAPPLIED", "FORCEAPPLIED"),
    "force_level": ("EMPLFORCELEVEL", "FORCELEVEL", "INCTYPE"),
    "officers_on_scene": ("OFFICERSPERINCIDENT", "OFFICERSONSCENE", "OFFICERCOUNT"),
}

_ARREST_ALIASES: dict[str, tuple[str, ...]] = {
    "external_number": ("ARRESTNUM", "INCIDENTNUM", "CASENUM", "BOOKINGNUM", "ARRESTNUMBER"),
    "occurred_at": ("ARRESTDATE", "INCIDENTDATE", "DATE", "OCCURREDDATE", "BOOKINGDATE"),
    "charge": ("CHARGE", "OFFENSEDESC", "ARRESTCHARGE", "CHARGEDESCRIPTION", "OFFENSE"),
    "charge_code": ("STATUTE", "CHARGECODE", "CRS", "ARS", "CODE"),
    "disposition": ("DISPOSITION", "STATUS", "OUTCOME"),
    "subject_gender": ("SUBJECTGENDER", "ARRESTEEGENDER", "GENDER"),
    "subject_race_group": ("SIMPLESUBJREGRP", "ARRESTEERACE", "RACE"),
    "subject_age_group": ("SUBJECTAGEGROUP", "ARRESTEEAGEGROUP", "AGEGROUP"),
    "location": ("ADDRESS", "LOCATION", "ARRESTLOCATION"),
    "precinct": ("PRECINCT", "SECTOR", "PATROLAREA"),
}

_COMPLAINT_ALIASES: dict[str, tuple[str, ...]] = {
    "external_number": ("COMPLAINTNUM", "CLAIMNUM", "CASENUM", "CASENUMBER", "INCIDENTNUM"),
    "occurred_at": ("INCIDENTDATE", "FILEDATE", "DATEFILED", "OCCURREDDATE", "DATE",
                    "CLAIMDATE", "INCIDENTOCCURRENCEDATE"),
    "category": ("CATEGORY", "ALLEGATIONTYPE", "CLAIMTYPE", "TYPE", "COMPLAINTTYPE"),
    "allegation": ("ALLEGATION", "DESCRIPTION", "NARRATIVE", "CLAIMDESCRIPTION"),
    "finding": ("FINDING", "DISPOSITION", "OUTCOME", "DETERMINATION"),
    "discipline": ("DISCIPLINE", "ACTIONTAKEN", "DISCIPLINARYACTION"),
    "status": ("STATUS", "CASESTATUS"),
    "amount_paid": ("AMOUNTPAID", "SETTLEMENTAMOUNT", "PAIDAMOUNT", "AMOUNT"),
}

_NEWS_ALIASES: dict[str, tuple[str, ...]] = {
    "title": ("TITLE", "HEADLINE"),
    "url": ("LINK", "URL"),
    "published_at": ("PUBLISHEDAT", "PUBDATE", "PUBLISHED", "DATE"),
    "summary": ("SUMMARY", "DESCRIPTION"),
}

_ALIAS_SETS: dict[str, dict[str, tuple[str, ...]]] = {
    "incident": _INCIDENT_ALIASES,
    "officer": _OFFICER_ALIASES,
    "force_event": _FORCE_EVENT_ALIASES,
    "arrest": _ARREST_ALIASES,
    "complaint": _COMPLAINT_ALIASES,
    "news": _NEWS_ALIASES,
}


def resolve_columns(row: dict[str, Any], entity_type: str) -> dict[str, str | None]:
    """Map canonical field names to the source columns present in ``row``.

    Returns ``{canonical_field: source_column_or_None}``.
    """
    aliases = _ALIAS_SETS.get(entity_type, _INCIDENT_ALIASES)
    present = {_norm_key(k): k for k in row.keys()}
    resolved: dict[str, str | None] = {}
    for canonical, candidates in aliases.items():
        resolved[canonical] = next(
            (present[c] for c in candidates if c in present), None
        )
    return resolved


def parse_datetime(value: Any) -> datetime | None:
    """Parse a source timestamp into a timezone-aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        # Epoch seconds or milliseconds.
        try:
            seconds = float(value)
            if seconds > 1e11:
                seconds /= 1000.0
            return datetime.fromtimestamp(seconds, UTC)
        except (ValueError, OverflowError, OSError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = dateparser.parse(text)
    except (ValueError, OverflowError, TypeError):
        return None
    if parsed is None:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").replace("$", "").strip()
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def period_of(moment: datetime | None) -> str | None:
    """``YYYY-MM`` bucket used for the monthly archive and anomaly windows."""
    return moment.strftime("%Y-%m") if moment else None


def normalize_record(
    row: dict[str, Any], entity_type: str
) -> dict[str, Any]:
    """Project one upstream row onto canonical fields.

    The full original row is preserved under ``raw`` so that every value
    shown in the UI can be traced back to the byte it came from.
    """
    columns = resolve_columns(row, entity_type)
    out: dict[str, Any] = {"raw": row, "_columns": columns}

    for canonical, source_column in columns.items():
        if source_column is None:
            out[canonical] = None
            continue
        value = row.get(source_column)
        if canonical in {"occurred_at", "published_at"}:
            out[canonical] = parse_datetime(value)
        elif canonical in {"latitude", "longitude", "amount_paid"}:
            out[canonical] = to_float(value)
        elif canonical in {"hire_year", "officers_on_scene"}:
            out[canonical] = to_int(value)
        else:
            out[canonical] = value

    moment = out.get("occurred_at") or out.get("published_at")
    out["period"] = period_of(moment)
    return out
