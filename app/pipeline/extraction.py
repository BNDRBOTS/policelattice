from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

# ===========================================================================
# Precompiled Regexes for High-Performance Rule-Based Evidence Extraction
# (Optimized for Railway CPU/Memory bounds - Zero heavy ML model overhead)
# ===========================================================================

# Officer name preceded by rank
_OFFICER_NAME_RE = re.compile(
    r"\b(?:Officer|Ofc\.?|Detective|Det\.?|Sergeant|Sgt\.?|Lieutenant|Lt\.?|"
    r"Captain|Capt\.?|Commander|Cmdr\.?|Chief|Deputy|Dep\.?|Trooper|Trp\.?)\s+"
    r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)\b",
    re.UNICODE,
)

# Badge / Serial numbers
_BADGE_RE = re.compile(
    r"\b(?:badge|badge\s*#|serial|serial\s*#|pin|id\s*#)\s*[:#]?\s*([A-Za-z0-9-]{3,12})\b",
    re.IGNORECASE,
)

# Employee ID / Personnel ID
_EMPLOYEE_ID_RE = re.compile(
    r"\b(?:employee|emp|personnel)\s*(?:id|#|no\.?|number)?\s*[:#]?\s*([A-Za-z0-9-]{4,12})\b",
    re.IGNORECASE,
)

# Incident / Case / CAD numbers
_INCIDENT_NUM_RE = re.compile(
    r"\b(?:incident|report|case|dr|cad\s*event|cad)\s*(?:#|no\.?|num\.?|number)?\s*[:#]?\s*"
    r"([A-Za-z0-9]+(?:-[A-Za-z0-9]+)+|[A-Za-z0-9]{4,20})\b",
    re.IGNORECASE,
)

# Court Docket Numbers (Federal, State, Municipal)
_DOCKET_NUM_RE = re.compile(
    r"\b(?:[0-9]:)?[0-9]{2,4}-[a-zA-Z]{2,4}-[0-9]{4,7}(?:-[a-zA-Z0-9]+)*\b|"
    r"\b(?:CV|CR|CA|PB|LC|PRR)-?[0-9]{2,4}-?[0-9]{4,7}(?:-[a-zA-Z0-9]+)*\b",
    re.IGNORECASE,
)

# Arizona Revised Statutes (A.R.S.)
_ARS_STATUTE_RE = re.compile(
    r"\b(?:A\.?R\.?S\.?|ARS)\s*§?\s*([0-9]{1,2}-[0-9]{3,4}(?:\.[0-9]+)?)\b",
    re.IGNORECASE,
)

# Known A.R.S. statute titles for automated charge enrichment
ARS_TITLE_MAP: dict[str, str] = {
    "13-1204": "Aggravated Assault on Peace Officer",
    "13-1203": "Assault",
    "13-2904": "Disorderly Conduct",
    "28-693": "Reckless Driving",
    "13-3407": "Possession or Use of Dangerous Drugs",
    "13-3408": "Possession of Narcotic Drugs",
    "13-1502": "Criminal Trespass in Third Degree",
    "13-1504": "Criminal Trespass in First Degree",
    "13-1105": "First Degree Murder",
    "13-1104": "Second Degree Murder",
    "13-1103": "Manslaughter",
    "13-2508": "Resisting Arrest",
    "13-2409": "Obstructing Criminal Investigation",
    "13-3102": "Misconduct Involving Weapons",
    "28-622.01": "Unlawful Flight from Pursuing Law Enforcement Vehicle",
}

# Cross-street and Intersection patterns
_CROSS_STREETS_RE = re.compile(
    r"\b([0-9]+(?:st|nd|rd|th)?\s+(?:Ave|Avenue|St|Street|Rd|Road|Dr|Drive|Blvd|Boulevard|Way|Ln|Lane|Pkwy|Parkway)"
    r"\s*(?:&|and|at|/)\s*[0-9A-Za-z\s]+(?:Ave|Avenue|St|Street|Rd|Road|Dr|Drive|Blvd|Boulevard|Way|Ln|Lane|Pkwy|Parkway))\b",
    re.IGNORECASE,
)

# Force Taxonomy Keyword Matchers
_FORCE_CATEGORIES: dict[str, re.Pattern[str]] = {
    "firearm_discharge": re.compile(
        r"\b(?:shot|shooting|discharged\s+firearm|fired\s+service\s+weapon|officer-involved\s+shooting|fatal\s+gunshot|gunshot\s+wound|firearm\s+was\s+discharged)\b",
        re.IGNORECASE,
    ),
    "conducted_energy_weapon": re.compile(
        r"\b(?:taser|tased|cew|conducted\s+energy\s+weapon|stun\s+gun|drive\s+stun)\b",
        re.IGNORECASE,
    ),
    "physical_restraint": re.compile(
        r"\b(?:physical\s+restraint|prone\s+restraint|neck\s+restraint|positional\s+asphyxia|physical\s+force|tackle|take-down|body\s+weight|ground\s+control)\b",
        re.IGNORECASE,
    ),
    "impact_weapon": re.compile(
        r"\b(?:baton|asp|impact\s+munition|beanbag|40mm|blunt\s+impact)\b",
        re.IGNORECASE,
    ),
    "chemical_agent": re.compile(
        r"\b(?:pepper\s+spray|oc\s+spray|tear\s+gas|chemical\s+agent|cs\s+gas|mace)\b",
        re.IGNORECASE,
    ),
    "canine_deployment": re.compile(
        r"\b(?:k9|k-9|canine\s+bite|police\s+dog|canine\s+apprehension)\b",
        re.IGNORECASE,
    ),
    "vehicle_pursuit": re.compile(
        r"\b(?:pit\s+maneuver|precision\s+immobilization|vehicle\s+pursuit|spike\s+strips|chase)\b",
        re.IGNORECASE,
    ),
}

# Legal & Oversight Disclosures
_DISCLOSURE_RE = re.compile(
    r"\b(?:Brady\s+list|Brady\s+disclosure|Brady\s+material|Rule\s+15\.1|Giglio|internal\s+affairs|civilian\s+review|disciplinary\s+review)\b",
    re.IGNORECASE,
)


@dataclass
class ExtractedEvidence:
    """Evidentiary entities and facts extracted from text."""

    officers: list[dict[str, Any]] = field(default_factory=list)
    incidents: list[dict[str, Any]] = field(default_factory=list)
    force_tactics: list[dict[str, Any]] = field(default_factory=list)
    statutes: list[dict[str, Any]] = field(default_factory=list)
    court_cases: list[dict[str, Any]] = field(default_factory=list)
    disclosures: list[dict[str, Any]] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    confidence_score: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EvidenceExtractionEngine:
    """Extracts structured evidentiary facts from unstructured narratives and text."""

    @classmethod
    def extract_from_text(cls, text: str | None) -> ExtractedEvidence:
        """Parse unstructured text to identify officers, incidents, force tactics, and statutes."""
        if not text or not isinstance(text, str):
            return ExtractedEvidence(confidence_score=0.0)

        cleaned_text = text.strip()
        if len(cleaned_text) < 5:
            return ExtractedEvidence(confidence_score=0.0)

        officers = cls._extract_officers(cleaned_text)
        incidents = cls._extract_incidents(cleaned_text)
        force_tactics = cls._extract_force_tactics(cleaned_text)
        statutes = cls._extract_statutes(cleaned_text)
        court_cases = cls._extract_court_cases(cleaned_text)
        disclosures = cls._extract_disclosures(cleaned_text)
        locations = cls._extract_locations(cleaned_text)

        total_extracted = (
            len(officers)
            + len(incidents)
            + len(force_tactics)
            + len(statutes)
            + len(court_cases)
            + len(disclosures)
            + len(locations)
        )

        confidence = 1.0 if total_extracted > 0 else 0.5

        return ExtractedEvidence(
            officers=officers,
            incidents=incidents,
            force_tactics=force_tactics,
            statutes=statutes,
            court_cases=court_cases,
            disclosures=disclosures,
            locations=locations,
            confidence_score=confidence,
        )

    @classmethod
    def extract_from_record(cls, record_payload: dict[str, Any]) -> ExtractedEvidence:
        """Scan all text values in a payload dict and extract combined evidentiary entities."""
        text_parts: list[str] = []

        def _collect_strings(obj: Any) -> None:
            if isinstance(obj, str):
                text_parts.append(obj)
            elif isinstance(obj, dict):
                for v in obj.values():
                    _collect_strings(v)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    _collect_strings(item)

        _collect_strings(record_payload)
        combined_text = " \n ".join(text_parts)
        return cls.extract_from_text(combined_text)

    @staticmethod
    def _snippet(text: str, start: int, end: int, window: int = 50) -> str:
        """Extract surrounding text snippet for audit context."""
        snip_start = max(0, start - window)
        snip_end = min(len(text), end + window)
        snippet = text[snip_start:snip_end].replace("\n", " ").strip()
        return f"...{snippet}..." if (snip_start > 0 or snip_end < len(text)) else snippet

    @classmethod
    def _extract_officers(cls, text: str) -> list[dict[str, Any]]:
        officers: list[dict[str, Any]] = []
        seen_names = set()

        for match in _OFFICER_NAME_RE.finditer(text):
            full_name = match.group(1).strip()
            if full_name not in seen_names:
                seen_names.add(full_name)
                parts = full_name.split()
                first_name = parts[0]
                last_name = parts[-1]
                officers.append({
                    "full_name": full_name,
                    "first_name": first_name,
                    "last_name": last_name,
                    "confidence": 0.95,
                    "snippet": cls._snippet(text, match.start(), match.end()),
                })

        # Extract badge numbers if present
        for match in _BADGE_RE.finditer(text):
            badge = match.group(1).strip()
            officers.append({
                "badge_number": badge,
                "confidence": 0.90,
                "snippet": cls._snippet(text, match.start(), match.end()),
            })

        # Extract employee IDs
        for match in _EMPLOYEE_ID_RE.finditer(text):
            emp = match.group(1).strip()
            officers.append({
                "employee_id": emp,
                "confidence": 0.90,
                "snippet": cls._snippet(text, match.start(), match.end()),
            })

        return officers

    @classmethod
    def _extract_incidents(cls, text: str) -> list[dict[str, Any]]:
        incidents: list[dict[str, Any]] = []
        seen = set()
        for match in _INCIDENT_NUM_RE.finditer(text):
            inc_id = match.group(1).strip()
            if inc_id not in seen:
                seen.add(inc_id)
                incidents.append({
                    "incident_number": inc_id,
                    "confidence": 0.95,
                    "snippet": cls._snippet(text, match.start(), match.end()),
                })
        return incidents

    @classmethod
    def _extract_force_tactics(cls, text: str) -> list[dict[str, Any]]:
        tactics: list[dict[str, Any]] = []
        seen = set()
        for category, pattern in _FORCE_CATEGORIES.items():
            for match in pattern.finditer(text):
                matched_term = match.group(0).strip().lower()
                key = (category, matched_term)
                if key not in seen:
                    seen.add(key)
                    tactics.append({
                        "force_category": category,
                        "matched_term": matched_term,
                        "confidence": 0.92,
                        "snippet": cls._snippet(text, match.start(), match.end()),
                    })
        return tactics

    @classmethod
    def _extract_statutes(cls, text: str) -> list[dict[str, Any]]:
        statutes: list[dict[str, Any]] = []
        seen = set()
        for match in _ARS_STATUTE_RE.finditer(text):
            code = match.group(1).strip()
            if code not in seen:
                seen.add(code)
                full_statute = f"ARS {code}"
                title = ARS_TITLE_MAP.get(code, "Criminal Statute Violation")
                severity = "Felony" if code.startswith("13-") else "Misdemeanor"
                statutes.append({
                    "statute": full_statute,
                    "statute_code": code,
                    "title": title,
                    "severity": severity,
                    "confidence": 0.98,
                    "snippet": cls._snippet(text, match.start(), match.end()),
                })
        return statutes

    @classmethod
    def _extract_court_cases(cls, text: str) -> list[dict[str, Any]]:
        cases: list[dict[str, Any]] = []
        seen = set()
        for match in _DOCKET_NUM_RE.finditer(text):
            docket = match.group(0).strip()
            if docket not in seen and len(docket) >= 7:
                seen.add(docket)
                cases.append({
                    "docket_number": docket,
                    "confidence": 0.95,
                    "snippet": cls._snippet(text, match.start(), match.end()),
                })
        return cases

    @classmethod
    def _extract_disclosures(cls, text: str) -> list[dict[str, Any]]:
        disclosures: list[dict[str, Any]] = []
        seen = set()
        for match in _DISCLOSURE_RE.finditer(text):
            term = match.group(0).strip()
            if term.lower() not in seen:
                seen.add(term.lower())
                disclosures.append({
                    "disclosure_type": term,
                    "confidence": 0.90,
                    "snippet": cls._snippet(text, match.start(), match.end()),
                })
        return disclosures

    @classmethod
    def _extract_locations(cls, text: str) -> list[str]:
        locations: list[str] = []
        seen = set()
        for match in _CROSS_STREETS_RE.finditer(text):
            loc = match.group(1).strip()
            if loc.lower() not in seen:
                seen.add(loc.lower())
                locations.append(loc)
        return locations
