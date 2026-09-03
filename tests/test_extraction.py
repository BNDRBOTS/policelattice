from __future__ import annotations

from app.pipeline.extraction import EvidenceExtractionEngine


def test_officer_extraction():
    text = (
        "During the incident, Officer Marcus Vance and Detective David Kowalski "
        "(Badge #B1042, Serial #E44910) arrived at the scene."
    )
    evidence = EvidenceExtractionEngine.extract_from_text(text)
    assert len(evidence.officers) >= 2

    names = [o["full_name"] for o in evidence.officers if "full_name" in o]
    assert "Marcus Vance" in names
    assert "David Kowalski" in names

    badges = [o["badge_number"] for o in evidence.officers if "badge_number" in o]
    assert "B1042" in badges


def test_incident_and_statute_extraction():
    text = (
        "According to Incident #MPV-2025-001, the subject was charged under "
        "A.R.S. 13-1204 for aggravated assault and ARS 13-2904 for disorderly conduct. "
        "The event occurred near 35th Ave and Indian School Rd."
    )
    evidence = EvidenceExtractionEngine.extract_from_text(text)

    incidents = [i["incident_number"] for i in evidence.incidents]
    assert "MPV-2025-001" in incidents

    statutes = [s["statute"] for s in evidence.statutes]
    assert "ARS 13-1204" in statutes
    assert "ARS 13-2904" in statutes

    assert len(evidence.locations) >= 1


def test_force_tactics_extraction():
    text = (
        "Officers deployed a taser after a physical restraint attempt. When the suspect "
        "continued advancing, an officer-involved shooting occurred and a firearm was discharged."
    )
    evidence = EvidenceExtractionEngine.extract_from_text(text)

    categories = [f["force_category"] for f in evidence.force_tactics]
    assert "conducted_energy_weapon" in categories
    assert "physical_restraint" in categories
    assert "firearm_discharge" in categories


def test_court_case_and_disclosure_extraction():
    text = (
        "In U.S. District Court case CV-24-01500-PHX-SPL, plaintiffs filed a Section 1983 "
        "claim alleging Brady list violations and failure to disclose internal affairs files."
    )
    evidence = EvidenceExtractionEngine.extract_from_text(text)

    dockets = [c["docket_number"] for c in evidence.court_cases]
    assert "CV-24-01500-PHX-SPL" in dockets

    disclosures = [d["disclosure_type"] for d in evidence.disclosures]
    assert any("brady" in d.lower() for d in disclosures)
    assert any("internal affairs" in d.lower() for d in disclosures)


def test_extract_from_record():
    payload = {
        "title": "Police Shooting Investigation",
        "narrative": "Detective David Kowalski responded to Case #CAS-2025-888.",
        "details": {
            "notes": "Subject arrested under ARS 13-3407 for dangerous drugs."
        },
    }
    evidence = EvidenceExtractionEngine.extract_from_record(payload)
    assert len(evidence.officers) >= 1
    assert any(s["statute"] == "ARS 13-3407" for s in evidence.statutes)
    assert any(i["incident_number"] == "CAS-2025-888" for i in evidence.incidents)
