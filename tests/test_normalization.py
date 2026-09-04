from __future__ import annotations

from datetime import UTC, datetime

from app.pipeline.normalization import (
    CanonicalNormalizer,
    normalize_agency_name,
    normalize_datetime,
    normalize_identifier,
    normalize_location,
    normalize_person_name,
)


def test_normalize_datetime():
    # None and invalid
    assert normalize_datetime(None) is None
    assert normalize_datetime("") is None
    assert normalize_datetime("invalid") is None

    # ISO string
    dt1 = normalize_datetime("2025-01-14T04:22:00Z")
    assert dt1 == datetime(2025, 1, 14, 4, 22, 0, tzinfo=UTC)

    # US format MM/DD/YYYY
    dt2 = normalize_datetime("01/14/2025 04:22:00")
    assert dt2 == datetime(2025, 1, 14, 4, 22, 0, tzinfo=UTC)

    # Unix timestamp (seconds)
    dt3 = normalize_datetime(1736828520)
    assert dt3 is not None
    assert dt3.year == 2025

    # Unix timestamp (milliseconds)
    dt4 = normalize_datetime(1736828520000)
    assert dt4 is not None
    assert dt4.year == 2025


def test_normalize_agency_name():
    assert normalize_agency_name("phx pd") == "Phoenix Police Department"
    assert normalize_agency_name("Phoenix Police") == "Phoenix Police Department"
    assert normalize_agency_name("TPD") == "Tempe Police Department"
    assert normalize_agency_name("Maricopa County Sheriff") == "Maricopa County Sheriff's Office"
    assert normalize_agency_name("AZ DPS") == "Arizona Department of Public Safety"
    assert normalize_agency_name("mesa pd") == "Mesa Police Department"


def test_normalize_identifier():
    assert normalize_identifier("#1042") == "1042"
    assert normalize_identifier("Badge: B-1042") == "B-1042"
    assert normalize_identifier("ID: E44910") == "E44910"
    assert normalize_identifier("UNKNOWN") is None
    assert normalize_identifier("") is None
    assert normalize_identifier(None) is None


def test_normalize_location():
    assert normalize_location("35th Avenue and Indian School Road") == "35th Ave & Indian School Rd"
    assert normalize_location("Mill Ave / University Dr") == "Mill Ave & University Dr"
    assert normalize_location("7th Street at Buckeye Road") == "7th St & Buckeye Rd"


def test_normalize_person_name():
    # Last, First
    first, last, rank = normalize_person_name("Vance, Marcus")
    assert first == "Marcus"
    assert last == "Vance"
    assert rank is None

    # First Last with rank
    first, last, rank = normalize_person_name("Officer Marcus Vance")
    assert first == "Marcus"
    assert last == "Vance"
    assert rank == "Officer"

    # Detective First M. Last
    first, last, rank = normalize_person_name("Det. David Kowalski")
    assert first == "David"
    assert last == "Kowalski"
    assert rank == "Detective"


def test_canonical_normalizer_incident():
    raw = {
        "incident_num": "INC-999",
        "date_time": "2025-03-12T21:10:00Z",
        "agency": "phx pd",
        "location": "35th Avenue and Thomas Road",
        "person_name": "John Doe",
    }
    norm = CanonicalNormalizer.normalize(raw, entity_type="incident", source_id="src_inc")
    assert norm.canonical_type == "incident"
    payload = norm.canonical_payload
    assert payload["incident_number"] == "INC-999"
    assert payload["agency_name"] == "Phoenix Police Department"
    assert payload["location"] == "35th Ave & Thomas Rd"
    assert payload["person_first_name"] == "John"
    assert payload["person_last_name"] == "Doe"


def test_canonical_normalizer_officer():
    raw = {
        "badge": "B1042",
        "emp_id": "E44910",
        "name": "Sgt. Michael Reynolds",
        "agency": "Phoenix PD",
    }
    norm = CanonicalNormalizer.normalize(raw, entity_type="officer", source_id="src_off")
    assert norm.canonical_type == "officer"
    payload = norm.canonical_payload
    assert payload["badge_number"] == "B1042"
    assert payload["employee_id"] == "E44910"
    assert payload["first_name"] == "Michael"
    assert payload["last_name"] == "Reynolds"
    assert payload["rank"] == "Sergeant"
    assert payload["agency_name"] == "Phoenix Police Department"


def test_canonical_normalizer_arrest():
    raw = {
        "booking_no": "BK-2025-001",
        "arrest_date": "2025-01-18 22:30:00",
        "statute": "ARS 13-1204",
        "charge_description": "Aggravated Assault on Peace Officer",
        "name": "Anthony Rivera",
    }
    norm = CanonicalNormalizer.normalize(raw, entity_type="arrest", source_id="src_arr")
    assert norm.canonical_type == "arrest"
    payload = norm.canonical_payload
    assert payload["booking_number"] == "BK-2025-001"
    assert len(payload["charges"]) == 1
    assert payload["charges"][0]["statute"] == "ARS 13-1204"
    # Severity is never inferred from the statute prefix (ARS Title 13
    # contains both felonies and misdemeanors); it is None unless the
    # source record explicitly states it.
    assert payload["charges"][0]["severity"] is None
