"""Plain-language rendering.

Every sentence here is assembled only from values the source actually
supplied. The rules:

* a field that is ``None`` is omitted, never replaced with a guess;
* no evaluative adjectives — the text states counts, labels and dates and
  nothing else;
* each statement is accompanied by the URL it came from.
"""

from __future__ import annotations

from typing import Any

_LABELS: dict[str, str] = {
    "force_level": "report type",
    "highest_force_applied": "highest force applied",
    "armed_type": "individual weapon type",
    "resistance": "resistance level",
    "de_escalation": "de-escalation",
    "injury": "individual injury",
    "highest_charge": "highest arrest charge",
    "outcome": "incident outcome",
    "location": "location",
    "precinct": "precinct",
    "subject_gender": "individual gender",
    "subject_race_group": "individual race/ethnicity group",
    "subject_age_group": "individual age group",
}


def _date_text(iso: str | None) -> str | None:
    if not iso:
        return None
    return iso[:10]


def incident_synopsis(incident: dict[str, Any]) -> dict[str, str | list[str]]:
    """Turn one incident record into a plain-language synopsis plus its link."""
    parts: list[str] = []

    agency = incident.get("agency_name")
    date = _date_text(incident.get("occurred_at"))
    number = incident.get("external_number")

    opener_bits = [b for b in (date, agency) if b]
    opener = "On " + " , ".join(opener_bits) if date else ("At " + agency if agency else None)
    if opener:
        parts.append(
            f"{opener} recorded use-of-force incident {number}."
            if number
            else f"{opener} recorded a use-of-force incident."
        )
    elif number:
        parts.append(f"Incident {number} is recorded.")

    facts = [
        f"{label} is listed as {incident[key]}"
        for key, label in _LABELS.items()
        if incident.get(key) is not None
    ]
    if facts:
        parts.append("The record lists " + "; ".join(facts) + ".")

    officers = incident.get("officers") or []
    if officers:
        outcomes = [
            o.get("within_policy") for o in officers if o.get("within_policy") is not None
        ]
        sentence = f"{len(officers)} officer record(s) are attached to this incident."
        if outcomes:
            tally = ", ".join(
                f"{outcomes.count(value)} listed as \"{value}\""
                for value in sorted(set(outcomes))
            )
            sentence += f" Policy outcome: {tally}."
        bwc = [o.get("bwc_activated") for o in officers if o.get("bwc_activated") is not None]
        if bwc:
            sentence += " Body-worn camera activation: " + ", ".join(
                f"{bwc.count(value)} listed as \"{value}\"" for value in sorted(set(bwc))
            ) + "."
        parts.append(sentence)

    dataset = incident.get("dataset_title")
    if dataset:
        parts.append(f"Source dataset: {dataset}.")

    return {
        "text": " ".join(parts) if parts else "This record carries no described fields.",
        "source_url": incident.get("source_url"),
        "retrieved_at": incident.get("retrieved_at"),
        "content_sha256": incident.get("content_sha256"),
    }


def period_summary(view: dict[str, Any]) -> dict[str, Any]:
    """Headline facts for one month, each with the number it rests on."""
    counts = view.get("counts", {})
    policy = view.get("policy_outcome", {})
    findings = view.get("findings", [])
    sources = view.get("sources", [])

    period = view.get("period") or "all recorded months"
    statements: list[dict[str, Any]] = []

    statements.append(
        {
            "text": (
                f"For {period} the lattice holds {counts.get('incidents', 0)} incident "
                f"records, {counts.get('force_events', 0)} officer-level force records, "
                f"{counts.get('arrests', 0)} arrest records, "
                f"{counts.get('complaints', 0)} complaint records and "
                f"{counts.get('news_items', 0)} news items."
            ),
            "basis": counts,
        }
    )

    if policy.get("labels"):
        total = policy.get("total") or 0
        breakdown = ", ".join(
            f"{label} {count}"
            f" ({100.0 * count / total:.1f}%)"
            if total
            else f"{label} {count}"
            for label, count in zip(policy["labels"], policy["counts"])
        )
        statements.append(
            {
                "text": (
                    f"Recorded policy outcomes across {total} officer records: {breakdown}."
                ),
                "basis": policy,
            }
        )

    if findings:
        high = sum(1 for f in findings if f.get("severity") == "high")
        elevated = sum(1 for f in findings if f.get("severity") == "elevated")
        statements.append(
            {
                "text": (
                    f"Statistical testing produced {len(findings)} officer-level findings "
                    f"for this period: {high} at severity high and {elevated} at severity "
                    f"elevated, using exact Poisson and binomial tests against the same "
                    f"agency and month."
                ),
                "basis": {"findings": len(findings), "high": high, "elevated": elevated},
            }
        )

    reachable = sum(1 for s in sources if s.get("verified_ok"))
    statements.append(
        {
            "text": (
                f"{reachable} of {len(sources)} configured sources answered successfully on "
                f"the last verification pass."
            ),
            "basis": {"reachable": reachable, "configured": len(sources)},
        }
    )

    return {"period": view.get("period"), "statements": statements}
