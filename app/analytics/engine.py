"""Month-aware analytics engine.

Produces the single payload shape that both the live view and every archived
month are rendered from. Because the shape is identical, switching months in
the UI swaps data without touching layout — no column appears or disappears,
so nothing shifts.

Every aggregate here is computed from lattice rows. Categories are built from
the values the sources actually contain; an empty category is simply absent
rather than being padded with a zero-labelled placeholder.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import utcnow
from app.models import (
    Agency,
    Arrest,
    Complaint,
    DataSource,
    EntityLink,
    ForceEvent,
    Incident,
    NewsItem,
    OfficerFinding,
    OfficerRef,
    RawRecord,
)

logger = logging.getLogger(__name__)

SEVERITY_ORDER = {"high": 0, "elevated": 1, "notable": 2}


def _period_filter(column: Any, period: str | None):
    return column == period if period else None


def _count(session: Session, model: Any, period: str | None) -> int:
    query = select(func.count(model.id))
    if period and hasattr(model, "period"):
        query = query.where(model.period == period)
    return int(session.scalar(query) or 0)


def _series(
    session: Session, model: Any, label_column: Any, period: str | None, limit: int = 40
) -> tuple[list[str], list[int], int]:
    """Distribution of a categorical column, most frequent first."""
    query = (
        select(label_column, func.count(model.id))
        .where(label_column.is_not(None))
        .group_by(label_column)
        .order_by(func.count(model.id).desc())
        .limit(limit)
    )
    if period and hasattr(model, "period"):
        query = query.where(model.period == period)
    rows = session.execute(query).all()
    labels = [str(label) for label, _ in rows]
    counts = [int(count) for _, count in rows]
    return labels, counts, sum(counts)


def _timeline(session: Session, period: str | None) -> dict[str, Any]:
    """Monthly counts of incidents, force events, arrests and complaints."""
    query = (
        select(Incident.period, func.count(Incident.id))
        .where(Incident.period.is_not(None))
        .group_by(Incident.period)
        .order_by(Incident.period)
    )
    incidents = {p: int(c) for p, c in session.execute(query).all()}

    def _by_period(model: Any) -> dict[str, int]:
        rows = session.execute(
            select(model.period, func.count(model.id))
            .where(model.period.is_not(None))
            .group_by(model.period)
        ).all()
        return {p: int(c) for p, c in rows}

    force = _by_period(ForceEvent)
    arrests = _by_period(Arrest)
    complaints = _by_period(Complaint)

    periods = sorted(set(incidents) | set(force) | set(arrests) | set(complaints))
    if period:
        periods = [p for p in periods if p == period]

    return {
        "labels": periods,
        "incidents": [incidents.get(p, 0) for p in periods],
        "force_events": [force.get(p, 0) for p in periods],
        "arrests": [arrests.get(p, 0) for p in periods],
        "complaints": [complaints.get(p, 0) for p in periods],
    }


def _sources(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(select(DataSource).order_by(DataSource.id)).scalars().all()
    return [
        {
            "id": s.id,
            "name": s.name,
            "kind": s.kind,
            "publisher": s.publisher,
            "endpoint": s.endpoint,
            "schedule": s.schedule,
            "verified_ok": bool(s.verified_ok),
            "verified_at": s.verified_at.isoformat() if s.verified_at else None,
            "http_status": s.http_status,
            "rows_total_reported": s.rows_total_reported,
            "rows_fetched_last_run": s.rows_fetched_last_run,
            "rows_new_last_run": s.rows_new_last_run,
            "last_run_at": s.last_run_at.isoformat() if s.last_run_at else None,
            "detail": s.last_error,
        }
        for s in rows
    ]


def _incidents(session: Session, period: str | None) -> list[dict[str, Any]]:
    query = select(Incident).order_by(Incident.occurred_at.desc().nullslast())
    if period:
        query = query.where(Incident.period == period)
    rows = session.execute(query).scalars().all()

    event_rows = session.execute(
        select(
            ForceEvent.incident_id,
            ForceEvent.officer_ref_id,
            ForceEvent.within_policy,
            ForceEvent.bwc_activated,
            OfficerRef.external_key,
            OfficerRef.gender,
            OfficerRef.race_group,
        ).join(OfficerRef, OfficerRef.id == ForceEvent.officer_ref_id)
    ).all()
    by_incident: dict[int, list[dict[str, Any]]] = {}
    for incident_id, officer_id, within_policy, bwc, key, gender, race in event_rows:
        by_incident.setdefault(int(incident_id), []).append(
            {
                "officer_id": int(officer_id),
                "officer_key": key,
                "officer_gender": gender,
                "officer_race_group": race,
                "within_policy": within_policy,
                "bwc_activated": bwc,
            }
        )

    agencies = {a.id: a.name for a in session.execute(select(Agency)).scalars().all()}

    return [
        {
            "id": i.id,
            "external_number": i.external_number,
            "kind": i.kind,
            "agency_id": i.agency_id,
            "agency_name": agencies.get(i.agency_id),
            "occurred_at": i.occurred_at.isoformat() if i.occurred_at else None,
            "period": i.period,
            "force_level": i.force_level,
            "highest_force_applied": i.highest_force_applied,
            "armed_type": i.armed_type,
            "resistance": i.resistance,
            "de_escalation": i.de_escalation,
            "injury": i.injury,
            "highest_charge": i.highest_charge,
            "outcome": i.outcome,
            "subject_gender": i.subject_gender,
            "subject_race_group": i.subject_race_group,
            "subject_age_group": i.subject_age_group,
            "location": i.location,
            "latitude": i.latitude,
            "longitude": i.longitude,
            "precinct": i.precinct,
            "officers": by_incident.get(i.id, []),
            "source_id": i.source_id,
            "source_url": i.source_url,
            "retrieved_at": i.retrieved_at.isoformat(),
            "content_sha256": i.content_sha256,
            "dataset": (i.data or {}).get("dataset"),
            "dataset_title": (i.data or {}).get("dataset_title"),
            "publisher": (i.data or {}).get("publisher"),
            "source_row": (i.data or {}).get("source_row"),
        }
        for i in rows
    ]


def _officers(session: Session, period: str | None) -> list[dict[str, Any]]:
    officers = session.execute(select(OfficerRef).order_by(OfficerRef.id)).scalars().all()
    agencies = {a.id: a.name for a in session.execute(select(Agency)).scalars().all()}

    force_counts: dict[int, int] = {}
    oop_counts: dict[int, int] = {}
    event_rows = session.execute(
        select(ForceEvent.officer_ref_id, ForceEvent.within_policy, ForceEvent.period)
    ).all()
    for officer_id, within_policy, event_period in event_rows:
        if period and event_period != period:
            continue
        force_counts[int(officer_id)] = force_counts.get(int(officer_id), 0) + 1
        if str(within_policy or "").strip().lower() == "no":
            oop_counts[int(officer_id)] = oop_counts.get(int(officer_id), 0) + 1

    findings: dict[int, list[dict[str, Any]]] = {}
    finding_rows = session.execute(
        select(OfficerFinding).order_by(OfficerFinding.severity, OfficerFinding.period.desc())
    ).scalars().all()
    for finding in finding_rows:
        if period and finding.period != period:
            continue
        findings.setdefault(finding.officer_ref_id, []).append(
            {
                "id": finding.id,
                "period": finding.period,
                "finding_type": finding.finding_type,
                "metric": finding.metric,
                "value": finding.value,
                "numerator": finding.numerator,
                "denominator": finding.denominator,
                "peer_value": finding.peer_value,
                "peer_count": finding.peer_count,
                "robust_z": finding.robust_z,
                "p_value": finding.p_value,
                "severity": finding.severity,
                "narrative": finding.narrative,
                "sources": finding.sources or [],
            }
        )

    arrest_counts: dict[int, int] = {}
    complaint_counts: dict[int, int] = {}
    for officer_id, count in session.execute(
        select(Arrest.officer_ref_id, func.count(Arrest.id))
        .where(Arrest.officer_ref_id.is_not(None))
        .group_by(Arrest.officer_ref_id)
    ).all():
        arrest_counts[int(officer_id)] = int(count)
    for officer_id, count in session.execute(
        select(Complaint.officer_ref_id, func.count(Complaint.id))
        .where(Complaint.officer_ref_id.is_not(None))
        .group_by(Complaint.officer_ref_id)
    ).all():
        complaint_counts[int(officer_id)] = int(count)

    return [
        {
            "id": o.id,
            "external_key": o.external_key,
            "agency_id": o.agency_id,
            "agency_name": agencies.get(o.agency_id),
            "gender": o.gender,
            "race_group": o.race_group,
            "rank": o.rank,
            "hire_year": o.hire_year,
            "first_seen_at": o.first_seen_at.isoformat() if o.first_seen_at else None,
            "last_seen_at": o.last_seen_at.isoformat() if o.last_seen_at else None,
            "source_url": o.source_url,
            "force_events": force_counts.get(o.id, 0),
            "out_of_policy": oop_counts.get(o.id, 0),
            "arrests": arrest_counts.get(o.id, 0),
            "complaints": complaint_counts.get(o.id, 0),
            "findings": findings.get(o.id, []),
        }
        for o in officers
    ]


def _findings(session: Session, period: str | None, limit: int = 500) -> list[dict[str, Any]]:
    query = (
        select(OfficerFinding)
        .order_by(OfficerFinding.p_value.asc().nullslast())
        .limit(limit)
    )
    if period:
        query = query.where(OfficerFinding.period == period)
    rows = session.execute(query).scalars().all()
    keys = {
        o.id: o.external_key for o in session.execute(select(OfficerRef)).scalars().all()
    }
    agencies = {a.id: a.name for a in session.execute(select(Agency)).scalars().all()}
    return [
        {
            "id": f.id,
            "officer_id": f.officer_ref_id,
            "officer_key": keys.get(f.officer_ref_id),
            "agency_id": f.agency_id,
            "agency_name": agencies.get(f.agency_id),
            "period": f.period,
            "finding_type": f.finding_type,
            "metric": f.metric,
            "value": f.value,
            "numerator": f.numerator,
            "denominator": f.denominator,
            "peer_value": f.peer_value,
            "peer_numerator": f.peer_numerator,
            "peer_denominator": f.peer_denominator,
            "peer_count": f.peer_count,
            "robust_z": f.robust_z,
            "p_value": f.p_value,
            "severity": f.severity,
            "narrative": f.narrative,
            "sources": f.sources or [],
            "computed_at": f.computed_at.isoformat(),
        }
        for f in rows
    ]


def _news(session: Session, period: str | None, limit: int = 400) -> list[dict[str, Any]]:
    query = select(NewsItem).order_by(NewsItem.published_at.desc().nullslast()).limit(limit)
    if period:
        query = query.where(NewsItem.period == period)
    return [
        {
            "id": n.id,
            "source_id": n.source_id,
            "title": n.title,
            "url": n.url,
            "published_at": n.published_at.isoformat() if n.published_at else None,
            "period": n.period,
            "summary": n.summary,
            "retrieved_at": n.retrieved_at.isoformat(),
            "content_sha256": n.content_sha256,
        }
        for n in session.execute(query).scalars().all()
    ]


def build_view(session: Session, period: str | None = None) -> dict[str, Any]:
    """Build the canonical payload for one period (``None`` = all time)."""
    from app.models import FetchLog, MonthlySnapshot

    force_labels, force_counts, force_total = _series(
        session, Incident, Incident.highest_force_applied, period
    )
    level_labels, level_counts, level_total = _series(
        session, Incident, Incident.force_level, period
    )
    armed_labels, armed_counts, armed_total = _series(
        session, Incident, Incident.armed_type, period
    )
    policy_labels, policy_counts, policy_total = _series(
        session, ForceEvent, ForceEvent.within_policy, period
    )
    bwc_labels, bwc_counts, bwc_total = _series(
        session, ForceEvent, ForceEvent.bwc_activated, period
    )
    arrest_labels, arrest_counts, arrest_total = _series(
        session, Arrest, Arrest.charge, period, limit=30
    )
    complaint_labels, complaint_counts, complaint_total = _series(
        session, Complaint, Complaint.category, period, limit=30
    )

    agency_rows = session.execute(
        select(Incident.agency_id, func.count(Incident.id))
        .group_by(Incident.agency_id)
        .order_by(func.count(Incident.id).desc())
    ).all()
    agencies = {a.id: a for a in session.execute(select(Agency)).scalars().all()}
    officer_counts = dict(
        session.execute(
            select(OfficerRef.agency_id, func.count(OfficerRef.id)).group_by(
                OfficerRef.agency_id
            )
        ).all()
    )
    arrest_by_agency = dict(
        session.execute(
            select(Arrest.agency_id, func.count(Arrest.id)).group_by(Arrest.agency_id)
        ).all()
    )

    link_rows = session.execute(
        select(EntityLink.relation, func.count(EntityLink.id)).group_by(EntityLink.relation)
    ).all()

    latest_snapshot = None
    if period:
        latest_snapshot = session.scalar(
            select(MonthlySnapshot)
            .where(MonthlySnapshot.period == period)
            .order_by(MonthlySnapshot.revision.desc())
            .limit(1)
        )
    last_fetch = session.scalar(
        select(FetchLog.retrieved_at).order_by(FetchLog.retrieved_at.desc()).limit(1)
    )

    return {
        "period": period,
        "generated_at": utcnow().isoformat(),
        "counts": {
            "raw_records": _count(session, RawRecord, None),
            "incidents": _count(session, Incident, period),
            "force_events": _count(session, ForceEvent, period),
            "arrests": _count(session, Arrest, period),
            "complaints": _count(session, Complaint, period),
            "news_items": _count(session, NewsItem, period),
            "officers": _count(session, OfficerRef, None),
            "agencies": _count(session, Agency, None),
            "entity_links": _count(session, EntityLink, None),
            "findings": _count(session, OfficerFinding, period),
            "sources": _count(session, DataSource, None),
        },
        "timeline": _timeline(session, period),
        "force_applied": {
            "labels": force_labels, "counts": force_counts, "total": force_total,
        },
        "force_level": {"labels": level_labels, "counts": level_counts, "total": level_total},
        "armed_type": {"labels": armed_labels, "counts": armed_counts, "total": armed_total},
        "policy_outcome": {
            "labels": policy_labels, "counts": policy_counts, "total": policy_total,
        },
        "bwc_activation": {"labels": bwc_labels, "counts": bwc_counts, "total": bwc_total},
        "arrest_charges": {
            "labels": arrest_labels, "counts": arrest_counts, "total": arrest_total,
        },
        "complaint_categories": {
            "labels": complaint_labels,
            "counts": complaint_counts,
            "total": complaint_total,
        },
        "agencies": [
            {
                "id": agency_id,
                "name": agencies[agency_id].name if agency_id in agencies else agency_id,
                "jurisdiction": (
                    agencies[agency_id].jurisdiction if agency_id in agencies else None
                ),
                "incidents": int(count),
                "officers": int(officer_counts.get(agency_id, 0)),
                "arrests": int(arrest_by_agency.get(agency_id, 0)),
            }
            for agency_id, count in agency_rows
        ],
        "graph_edges": [
            {"relation": relation, "count": int(count)} for relation, count in link_rows
        ],
        "sources": _sources(session),
        "findings": _findings(session, period),
        "incidents": _incidents(session, period),
        "officers": _officers(session, period),
        "news": _news(session, period),
        "provenance": {
            "last_fetch_at": last_fetch.isoformat() if last_fetch else None,
            "every_record_carries_source_url": True,
        },
        "archive": {
            "period": period,
            "revision": latest_snapshot.revision if latest_snapshot else None,
            "content_sha256": latest_snapshot.content_sha256 if latest_snapshot else None,
            "sealed_at": (
                latest_snapshot.sealed_at.isoformat() if latest_snapshot else None
            ),
            "sealed": latest_snapshot is not None,
        },
    }


def available_periods(session: Session) -> list[str]:
    """Every month present in the lattice, newest first."""
    periods = {
        p
        for (p,) in session.execute(
            select(Incident.period).where(Incident.period.is_not(None)).distinct()
        ).all()
    }
    periods |= {
        p
        for (p,) in session.execute(
            select(Arrest.period).where(Arrest.period.is_not(None)).distinct()
        ).all()
    }
    periods |= {
        p
        for (p,) in session.execute(
            select(ForceEvent.period).where(ForceEvent.period.is_not(None)).distinct()
        ).all()
    }
    return sorted(periods, reverse=True)
