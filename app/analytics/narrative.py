"""Deterministic plain-language renderer for pipeline analytics.

Semantic output contract:
- Translates every statistical measure into plain, accessible language.
- Strictly objective and factual: numbers, dates, sources, and methodology
  only. No subjective judgment words ("concerning", "excessive", "rogue",
  "good", "bad") and no omission of relevant measured facts.
- Deterministic: identical inputs always produce identical text, so archived
  narratives replay byte-identically from the chron-log.
"""

from __future__ import annotations

from typing import Any

# Words banned from generated narratives to guarantee objectivity.
SUBJECTIVE_MARKERS = (
    "concerning", "alarming", "excessive", "rogue", "dangerous", "aggressive",
    "problematic", "suspicious", "worrisome", "notable", "bad", "good", "worst",
    "best", "impressive", "shocking",
)


def _fmt(x: float | int | None, nd: int = 2) -> str:
    if x is None:
        return "not calculable"
    if isinstance(x, int) or (isinstance(x, float) and x.is_integer()):
        return f"{int(x):,}"
    return f"{x:,.{nd}f}"


def render_anomaly_finding(finding: dict[str, Any]) -> str:
    """Render one officer anomaly finding as plain language with full facts."""
    label = finding.get("officer_label") or "Officer (name not recorded in source data)"
    agency = finding.get("agency_name") or "agency not recorded in source data"
    badge = finding.get("badge_number")
    badge_txt = f", badge number {badge}" if badge else ", badge number not recorded"
    metric = finding.get("metric") or "activity"
    metric_label = METRIC_LABELS.get(metric, metric.replace("_", " "))
    value = finding.get("metric_value")
    peers = finding.get("peer_count")
    median = finding.get("peer_median")
    mean = finding.get("peer_mean")
    pmax = finding.get("peer_max")
    mad = finding.get("peer_mad")
    ratio = finding.get("ratio_to_median")
    z = finding.get("robust_z")
    p = finding.get("poisson_p")
    q = finding.get("bh_q")
    tests = finding.get("tests_run")
    wstart = finding.get("window_start")
    wend = finding.get("window_end")
    basis = finding.get("metric_records_basis") or {}

    parts: list[str] = []
    wstart_txt = str(wstart or "window start not recorded")[:10]
    wend_txt = str(wend or "window end not recorded")[:10]
    parts.append(
        f"{label} (agency: {agency}{badge_txt}) is recorded with {_fmt(value)} "
        f"{metric_label} events in the period {wstart_txt} to {wend_txt}."
    )
    parts.append(
        f"The comparison group is {peers} officers in the same agency with at least "
        f"one recorded {metric_label} event in the same period. Within that group the "
        f"median is {_fmt(median)}, the mean is {_fmt(mean, 3)}, the median absolute "
        f"deviation is {_fmt(mad, 3)}, and the highest recorded value is {_fmt(pmax)}."
    )
    if ratio is not None and median:
        parts.append(
            f"This officer's count is {ratio:.2f} times the group median."
        )
    elif ratio is not None:
        parts.append(
            "A ratio to median could not be computed because the group median is zero; "
            "the officer's count is above a zero median."
        )
    if z is not None:
        parts.append(
            f"The robust z-score (using median and median absolute deviation, scaled by 1.4826) "
            f"is {z:.2f}."
        )
    else:
        parts.append(
            "A robust z-score could not be calculated because the group's median absolute "
            "deviation is zero; the exact Poisson test below is used instead."
        )
    if p is not None:
        parts.append(
            f"The exact Poisson upper-tail probability of observing {_fmt(value)} or more events "
            f"when the expected rate equals the group mean of {_fmt(mean, 3)} is p = {p:.6g}."
        )
    if q is not None:
        parts.append(
            f"After Benjamini-Hochberg correction for multiple comparisons across {tests} "
            f"officer-metric tests, the adjusted q-value is {q:.6g}."
        )
    srcs = basis.get("sources") or []
    if srcs:
        src_txt = "; ".join(sorted(set(str(s) for s in srcs)))
        parts.append(f"Source records for this measurement come from: {src_txt}.")
    parts.append(
        "All figures above are computed directly from ingested public records; "
        "no values are estimated or imputed."
    )
    return " ".join(parts)


def render_anomaly_legend(metric: str, thresholds: dict[str, Any]) -> str:
    """Plain-language statement of what qualifies as a finding (methodology)."""
    metric_label = METRIC_LABELS.get(metric, metric.replace("_", " "))
    return (
        f"A {metric_label} finding is recorded when an officer's count is at least "
        f"{thresholds.get('min_count')} events and at least "
        f"{thresholds.get('min_ratio')}x the agency peer median, and either the "
        f"Benjamini-Hochberg adjusted q-value is at most {thresholds.get('max_q')} "
        f"or the robust z-score is at least {thresholds.get('min_z')}. Peer groups "
        f"contain officers of the same agency with at least one recorded event of "
        f"the same metric in the same period. Every officer's exact counts appear "
        f"in the officer metrics table regardless of whether a finding is recorded."
    )


METRIC_LABELS = {
    "use_of_force_events": "use-of-force",
    "total_incident_involvement": "incident involvement",
    "arrest_events": "arrest",
    "news_mention_involvement": "news-linked incident involvement",
    "officer_named_in_documents": "document mention",
}


def audit_objectivity(text: str) -> list[str]:
    """Return any subjective markers found in generated text (used by tests)."""
    lowered = text.lower()
    return [m for m in SUBJECTIVE_MARKERS if m in lowered.split() or f" {m} " in lowered]


def render_month_summary(payload: dict[str, Any]) -> str:
    """Plain-language summary of a month's analytics payload (all key facts)."""
    summary = payload.get("summary", {})
    month = payload.get("month")
    mode = payload.get("mode")
    mode_text = (
        "live data computed from the current lattice"
        if mode == "live"
        else "immutable archived chron-log replay"
    )
    lines = [
        f"Report period: {month} ({mode_text}).",
        f"Records considered: {summary.get('raw_records_ingested', 0):,} raw records ingested, "
        f"{summary.get('staging_records', 0):,} staged records, "
        f"{summary.get('incidents', 0):,} incidents, "
        f"{summary.get('officers', 0):,} officers, "
        f"{summary.get('arrests', 0):,} arrests, "
        f"{summary.get('charges', 0):,} charges, "
        f"{summary.get('news_articles', 0):,} news articles.",
        f"Verification: {summary.get('verified_passed', 0):,} records passed all validation "
        f"checks; {summary.get('verified_failed', 0):,} failed at least one check and were "
        f"excluded from synthesis.",
        f"Officer anomaly findings recorded this period: {summary.get('anomaly_findings', 0):,}.",
    ]
    return " ".join(lines)
