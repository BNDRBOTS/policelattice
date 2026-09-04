"""Canonical month analytics engine.

Computes the exact same payload shape for the ACTIVE month (live query) and
for ARCHIVED months (replayed from the immutable chron-log), guaranteeing the
UI renders historical months with full parity to the current view.

Anti-fabrication rules enforced here:
- No default agency names, force types, or categories are ever invented.
- Records that genuinely lack a value are counted under explicit
  "Unattributed"/"Unclassified" labels, which are facts about the source data,
  not fabricated content.
- Every distribution includes its zero counts; nothing is dropped.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import (
    Agency,
    Arrest,
    Charge,
    EntityLink,
    Incident,
    NewsArticle,
    Officer,
    OfficerAnomalyFinding,
    RawRecord,
    StagingRecord,
    VerificationResult,
)
from app.models import (
    Officer as OfficerModel,
)

FORCE_TAXONOMY: list[tuple[str, tuple[str, ...]]] = [
    ("Firearm", ("firearm", "shooting", "gun", "lethal", "weapon_discharge")),
    ("Conducted Energy Weapon", ("taser", "cew", "conducted energy", "electronic control")),
    ("Physical Restraint", ("restraint", "hands", "body weight", "asphyxia", "neck", "takedown")),
    ("Impact Weapon", ("baton", "impact weapon", "flashlight", "striking")),
    ("Chemical Agent", ("pepper spray", "chemical", "oc spray", "tear gas", "cs gas")),
    ("Canine", ("canine", "k-9", "k9", "dog bite")),
    ("Vehicle", ("vehicle", "pursuit", "immobilization", "precipitation")),
]
UNCLASSIFIED_LABEL = "Unclassified (no force type recorded in source)"
UNATTRIBUTED_LABEL = "Unattributed Agency"


def month_bounds(month_key: str) -> tuple[datetime, datetime]:
    year, month = int(month_key[:4]), int(month_key[5:7])
    start = datetime(year, month, 1, tzinfo=UTC)
    end = (
        datetime(year + 1, 1, 1, tzinfo=UTC)
        if month == 12
        else datetime(year, month + 1, 1, tzinfo=UTC)
    )
    return start, end


def shift_month(month_key: str, delta: int) -> str:
    year, month = int(month_key[:4]), int(month_key[5:7])
    idx = year * 12 + (month - 1) + delta
    return f"{idx // 12:04d}-{idx % 12 + 1:02d}"


def classify_force(text: str | None) -> str | None:
    if not text:
        return None
    lowered = str(text).lower()
    for label, needles in FORCE_TAXONOMY:
        if any(n in lowered for n in needles):
            return label
    return None


def _in_window(dt: datetime | None, start: datetime, end: datetime) -> bool:
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return start <= dt < end


class AnalyticsEngine:
    """Computes the canonical analytics payload for one calendar month."""

    def __init__(self, session: Session):
        self.session = session

    # ------------------------------------------------------------------ #

    def compute_month(self, month_key: str, months_back: int = 11) -> dict[str, Any]:
        start, end = month_bounds(month_key)

        incidents = self.session.scalars(select(Incident)).all()
        officers = self.session.scalars(select(OfficerModel)).all()
        arrests = self.session.scalars(select(Arrest)).all()
        charges = self.session.scalars(select(Charge)).all()
        links = self.session.scalars(select(EntityLink)).all()
        agencies = {a.id: a.name for a in self.session.scalars(select(Agency)).all()}

        raw_month = self.session.scalars(
            select(RawRecord).where(RawRecord.ingested_at >= start, RawRecord.ingested_at < end)
        ).all()
        staging_month = self.session.scalars(
            select(StagingRecord).where(
                StagingRecord.created_at >= start, StagingRecord.created_at < end
            )
        ).all()
        news_month = self.session.scalars(
            select(NewsArticle).where(
                NewsArticle.published_at >= start, NewsArticle.published_at < end
            )
        ).all()
        anomaly_findings = self.session.scalars(
            select(OfficerAnomalyFinding).where(OfficerAnomalyFinding.month_key == month_key)
        ).all()

        # Verification outcomes for the month's staging records.
        staging_ids = [s.id for s in staging_month]
        verified_passed = verified_failed = 0
        if staging_ids:
            verifications = self.session.scalars(
                select(VerificationResult).where(
                    VerificationResult.staging_record_id.in_(staging_ids)
                )
            ).all()
            latest: dict[int, VerificationResult] = {}
            for v in verifications:
                latest[v.staging_record_id] = v
            verified_passed = sum(1 for v in latest.values() if v.passed)
            verified_failed = sum(1 for v in latest.values() if not v.passed)

        incidents_month = [i for i in incidents if _in_window(i.occurred_at, start, end)]
        arrests_month = [a for a in arrests if _in_window(a.arrested_at, start, end)]

        # ---------------- timeline (rolling, cumulative scope) ----------------
        timeline_labels: list[str] = []
        timeline_incidents: list[int] = []
        timeline_deaths: list[int] = []
        timeline_arrests: list[int] = []
        for k in range(months_back, -1, -1):
            mk = shift_month(month_key, -k)
            s, e = month_bounds(mk)
            incs = [i for i in incidents if _in_window(i.occurred_at, s, e)]
            deaths = [
                i
                for i in incs
                if (i.data or {}).get("cause_of_death")
                or "death" in (i.incident_type or "").lower()
                or "fatal" in (i.incident_type or "").lower()
                or "shooting" in (i.incident_type or "").lower()
            ]
            timeline_labels.append(mk)
            timeline_incidents.append(len(incs))
            timeline_deaths.append(len(deaths))
            timeline_arrests.append(sum(1 for a in arrests if _in_window(a.arrested_at, s, e)))

        # ---------------- force taxonomy ----------------
        force_counts: dict[str, int] = {label: 0 for label, _ in FORCE_TAXONOMY}
        force_counts[UNCLASSIFIED_LABEL] = 0
        force_incident_ids: list[int] = []
        for inc in incidents_month:
            data = inc.data or {}
            raw_ftype = (
                data.get("force_type")
                or data.get("incident_type")
                or inc.incident_type
                or ""
            )
            label = classify_force(raw_ftype) if str(raw_ftype).strip() else None
            if label is None and (data.get("force_type") or str(raw_ftype).strip()):
                label = UNCLASSIFIED_LABEL
            if label is not None:
                force_counts[label] += 1
                force_incident_ids.append(inc.id)

        # ---------------- agency distribution ----------------
        agency_incident_counts: dict[str, int] = {}
        agency_officer_counts: dict[str, int] = {}
        for inc in incidents_month:
            name = agencies.get(inc.agency_id) or (inc.data or {}).get("agency_name")
            if not name:
                name = UNATTRIBUTED_LABEL
            agency_incident_counts[name] = agency_incident_counts.get(name, 0) + 1
        for off in officers:
            name = agencies.get(off.agency_id) or UNATTRIBUTED_LABEL
            agency_officer_counts[name] = agency_officer_counts.get(name, 0) + 1

        # ---------------- incident types ----------------
        incident_type_counts: dict[str, int] = {}
        for inc in incidents_month:
            itype = (inc.incident_type or "type not recorded").strip() or "type not recorded"
            incident_type_counts[itype] = incident_type_counts.get(itype, 0) + 1

        # ---------------- source provenance ----------------
        from app.models import DataSource

        sources_rows = self.session.execute(
            select(RawRecord.source_id, func.count(RawRecord.id))
            .where(RawRecord.ingested_at >= start, RawRecord.ingested_at < end)
            .group_by(RawRecord.source_id)
        ).all()
        ds_name_map = {d.id: d.name for d in self.session.scalars(select(DataSource)).all()}
        provenance = [
            {"source_id": sid, "source_name": ds_name_map.get(sid, sid), "raw_records": count}
            for sid, count in sorted(sources_rows, key=lambda x: -x[1])
        ]

        # ---------------- officer metrics (full table — no truncation) -------
        officer_metrics = self._officer_metrics(
            officers, links, incidents_month, arrests_month, agencies
        )

        # ---------------- link topology ----------------
        link_types: dict[str, int] = {}
        for link in links:
            rtype = link.relation_type.replace("_", " ").title()
            link_types[rtype] = link_types.get(rtype, 0) + 1

        return {
            "month": month_key,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "mode": "live",
            "generated_at": datetime.now(UTC).isoformat(),
            "summary": {
                "raw_records_ingested": len(raw_month),
                "staging_records": len(staging_month),
                "incidents": len(incidents_month),
                "officers": len(officers),
                "arrests": len(arrests_month),
                "charges": len(charges),
                "news_articles": len(news_month),
                "relational_links": len(links),
                "verified_passed": verified_passed,
                "verified_failed": verified_failed,
                "anomaly_findings": len(anomaly_findings),
                "force_incidents": len(force_incident_ids),
            },
            "timeline": {
                "labels": timeline_labels,
                "incidents": timeline_incidents,
                "deaths": timeline_deaths,
                "arrests": timeline_arrests,
            },
            "force_taxonomy": {
                "labels": list(force_counts.keys()),
                "counts": list(force_counts.values()),
            },
            "agency_distribution": {
                "labels": sorted(agency_incident_counts.keys()),
                "incidents": [
                    agency_incident_counts[k]
                    for k in sorted(agency_incident_counts.keys())
                ],
                "officers": [
                    agency_officer_counts.get(k, 0) for k in sorted(agency_incident_counts.keys())
                ],
            },
            "incident_types": {
                "labels": sorted(incident_type_counts.keys()),
                "counts": [incident_type_counts[k] for k in sorted(incident_type_counts.keys())],
            },
            "source_provenance": provenance,
            "graph_topology": {
                "labels": sorted(link_types.keys()),
                "counts": [link_types[k] for k in sorted(link_types.keys())],
            },
            "officer_metrics": officer_metrics,
            "anomaly_findings": [
                {
                    "id": f.id,
                    "officer_id": f.officer_id,
                    "officer_label": f.officer_label,
                    "agency_name": f.agency_name,
                    "badge_number": f.badge_number,
                    "metric": f.metric,
                    "metric_value": f.metric_value,
                    "peer_count": f.peer_count,
                    "peer_median": f.peer_median,
                    "peer_mad": f.peer_mad,
                    "peer_mean": f.peer_mean,
                    "peer_max": f.peer_max,
                    "ratio_to_median": f.ratio_to_median,
                    "robust_z": f.robust_z,
                    "poisson_p": f.poisson_p,
                    "bh_q": f.bh_q,
                    "tests_run": f.tests_run,
                    "window_start": f.window_start.isoformat() if f.window_start else None,
                    "window_end": f.window_end.isoformat() if f.window_end else None,
                    "metric_records_basis": f.metric_records_basis or {},
                    "evidence": f.evidence or [],
                    "narrative": f.narrative,
                }
                for f in anomaly_findings
            ],
            "officer_count": len(officers),
        }

    # ------------------------------------------------------------------ #

    def _officer_metrics(
        self,
        officers: list[Officer],
        links: list[EntityLink],
        incidents_month: list[Incident],
        arrests_month: list[Arrest],
        agencies: dict[int, str],
    ) -> list[dict[str, Any]]:
        """Per-officer exact counts for the month window (complete table)."""
        incident_by_id = {i.id: i for i in incidents_month}
        arrest_incident_ids = {a.incident_id for a in arrests_month if a.incident_id is not None}
        officer_to_incidents: dict[int, set[int]] = {}
        for link in links:
            if (
                link.source_entity == "officer"
                and link.relation_type == "involved_in"
                and link.target_entity == "incident"
                and link.target_id in incident_by_id
            ):
                officer_to_incidents.setdefault(link.source_id, set()).add(link.target_id)

        rows: list[dict[str, Any]] = []
        for off in officers:
            involved = officer_to_incidents.get(off.id, set())
            uof = 0
            arrests_linked = 0
            for inc_id in involved:
                inc = incident_by_id[inc_id]
                data = inc.data or {}
                ftype = data.get("force_type") or inc.incident_type or ""
                if str(ftype).strip():
                    uof += 1
                if inc_id in arrest_incident_ids:
                    arrests_linked += 1
            full_name = " ".join(filter(None, [off.first_name, off.last_name])) or (
                f"Badge #{off.badge_number}" if off.badge_number else "Name not recorded"
            )
            rows.append(
                {
                    "officer_id": off.id,
                    "label": full_name,
                    "badge_number": off.badge_number,
                    "employee_id": off.employee_id,
                    "agency": agencies.get(off.agency_id) or UNATTRIBUTED_LABEL,
                    "incidents_involved": len(involved),
                    "use_of_force_events": uof,
                    "arrests_linked": arrests_linked,
                }
            )
        return rows
