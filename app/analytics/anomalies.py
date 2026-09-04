"""Officer-level behavioral anomaly detection.

Methodology (all statistical, zero subjective judgment):

1. For a given month window, per-officer event counts are computed for each
   metric (use-of-force events, total incident involvement, arrests linked
   via involved incidents, news-linked incidents).
2. For every officer x metric with at least one event, the *peer group* is
   all OTHER officers in the same agency with at least one event of the same
   metric in the same window (self excluded to prevent self-dilution).
3. Peer statistics: median, median absolute deviation (MAD), mean, max.
   - Robust z-score: (x - median) / (1.4826 * MAD); undefined when MAD == 0.
   - Exact Poisson upper-tail test: P(X >= x | lambda = peer mean), computed
     with scipy (reference scientific statistics library).
4. Benjamini-Hochberg correction controls the false discovery rate across all
   officer-metric tests run in the month.
5. A finding is recorded when: value >= min_count AND value >= min_ratio x
   peer median AND (q <= max_q OR robust z >= min_z). The thresholds are
   themselves reported in the output. Officers below threshold remain fully
   visible in the complete officer metrics table (no omission of facts).
"""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import median
from typing import Any

from scipy import stats as scipy_stats
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.analytics.engine import month_bounds
from app.analytics.narrative import render_anomaly_finding, render_anomaly_legend
from app.models import (
    Agency,
    EntityLink,
    Incident,
    Officer,
    OfficerAnomalyFinding,
)

# Finding thresholds (reported alongside every result — methodology transparency)
FINDING_THRESHOLDS = {
    "min_count": 3,
    "min_ratio": 2.0,
    "max_q": 0.05,
    "min_z": 3.5,
}

METRICS = (
    "use_of_force_events",
    "total_incident_involvement",
    "arrests_linked",
    "news_linked_incidents",
)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _mad(values: list[float], med: float) -> float:
    return median([abs(v - med) for v in values]) if values else 0.0


def _bh_adjust(pvalues: list[float]) -> list[float]:
    """Benjamini-Hochberg FDR adjustment (q-values), step-up procedure."""
    n = len(pvalues)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: pvalues[i])
    qvalues = [0.0] * n
    prev = 1.0
    for rank_idx in range(n - 1, -1, -1):
        i = order[rank_idx]
        rank = rank_idx + 1
        val = min(prev, pvalues[i] * n / rank)
        qvalues[i] = val
        prev = val
    return [min(q, 1.0) for q in qvalues]


class OfficerAnomalyDetector:
    """Computes and persists officer anomaly findings for one month."""

    def __init__(self, session: Session):
        self.session = session

    # ------------------------------------------------------------------ #

    def compute_and_persist(self, month_key: str) -> dict[str, Any]:
        start, end = month_bounds(month_key)
        started = datetime.now(UTC)

        officers = self.session.scalars(select(Officer)).all()
        agencies = {a.id: a.name for a in self.session.scalars(select(Agency)).all()}
        links = self.session.scalars(select(EntityLink)).all()

        incidents_window = [
            i
            for i in self.session.scalars(select(Incident)).all()
            if i.occurred_at is not None
            and start
            <= (
                i.occurred_at
                if i.occurred_at.tzinfo
                else i.occurred_at.replace(tzinfo=UTC)
            )
            < end
        ]
        incident_by_id = {i.id: i for i in incidents_window}

        # officer -> set of window incidents (via involved_in links)
        officer_incidents: dict[int, set[int]] = {}
        news_linked_incidents: set[int] = set()
        for link in links:
            if link.target_entity == "incident" and link.target_id in incident_by_id:
                if link.source_entity == "officer" and link.relation_type == "involved_in":
                    officer_incidents.setdefault(link.source_id, set()).add(link.target_id)
                elif link.relation_type in ("reports_on", "evidence_for"):
                    news_linked_incidents.add(link.target_id)

        officer_labels: dict[int, str] = {}
        officer_agency: dict[int, str] = {}
        for off in officers:
            officer_labels[off.id] = (
                " ".join(filter(None, [off.first_name, off.last_name]))
                or (f"Badge #{off.badge_number}" if off.badge_number else "Name not recorded")
            )
            officer_agency[off.id] = agencies.get(off.agency_id) or "Unattributed Agency"

        # Per-officer metric values + per-metric record evidence
        values: dict[tuple[int, str], float] = {}
        evidence: dict[tuple[int, str], list[dict[str, Any]]] = {}
        for off in officers:
            involved = officer_incidents.get(off.id, set())
            metric_values = {
                "use_of_force_events": 0,
                "total_incident_involvement": len(involved),
                "arrests_linked": 0,
                "news_linked_incidents": 0,
            }
            for inc_id in involved:
                inc = incident_by_id[inc_id]
                data = inc.data or {}
                ftype = data.get("force_type") or inc.incident_type or ""
                if str(ftype).strip():
                    metric_values["use_of_force_events"] += 1
                if inc_id in news_linked_incidents:
                    metric_values["news_linked_incidents"] += 1
                if data.get("arrest") or (inc.data or {}).get("booking_number"):
                    metric_values["arrests_linked"] += 1

            for metric, value in metric_values.items():
                if value > 0 or metric == "total_incident_involvement":
                    values[(off.id, metric)] = float(value)
                    evidence[(off.id, metric)] = [
                        {
                            "incident_id": i.id,
                            "incident_number": (i.external_ids or {}).get("incident_number"),
                            "occurred_at": i.occurred_at.isoformat() if i.occurred_at else None,
                            "incident_type": i.incident_type,
                            "force_type": (i.data or {}).get("force_type"),
                            "location": i.location,
                        }
                        for i in sorted(
                            (incident_by_id[iid] for iid in involved),
                            key=lambda x: x.occurred_at or start,
                        )
                    ]

        # Peer-group stats and statistical tests
        tests: list[dict[str, Any]] = []
        for (officer_id, metric), value in values.items():
            agency = officer_agency[officer_id]
            peers = [
                v
                for (oid, m), v in values.items()
                if m == metric and oid != officer_id and officer_agency[oid] == agency
            ]
            # Peer group must have at least 3 other officers for a meaningful
            # comparison; otherwise the test is skipped and reported.
            if len(peers) < 3:
                tests.append(
                    {
                        "officer_id": officer_id,
                        "metric": metric,
                        "value": value,
                        "tested": False,
                        "skip_reason": (
                            f"comparison group has {len(peers)} other officer(s); "
                            "minimum required is 3"
                        ),
                    }
                )
                continue

            med = float(median(peers))
            mad = _mad(peers, med)
            mean = _mean(peers)
            pmax = max(peers)
            ratio = (value / med) if med > 0 else (value / mean if mean > 0 else None)
            z = (value - med) / (1.4826 * mad) if mad > 0 else None
            lam = max(mean, 1e-9)
            p = float(scipy_stats.poisson.sf(value - 1, lam)) if value >= 1 else 1.0

            tests.append(
                {
                    "officer_id": officer_id,
                    "metric": metric,
                    "value": value,
                    "tested": True,
                    "agency": agency,
                    "peer_count": len(peers),
                    "peer_median": med,
                    "peer_mad": mad,
                    "peer_mean": mean,
                    "peer_max": pmax,
                    "ratio_to_median": ratio,
                    "robust_z": z,
                    "poisson_p": p,
                }
            )

        tested = [t for t in tests if t.get("tested")]
        qvalues = _bh_adjust([t["poisson_p"] for t in tested])
        for t, q in zip(tested, qvalues):
            t["bh_q"] = q

        findings: list[dict[str, Any]] = []
        th = FINDING_THRESHOLDS
        for t in tested:
            meets_count = t["value"] >= th["min_count"]
            meets_ratio = (t.get("ratio_to_median") or 0) >= th["min_ratio"]
            meets_stat = (t.get("bh_q") is not None and t["bh_q"] <= th["max_q"]) or (
                t.get("robust_z") is not None and t["robust_z"] >= th["min_z"]
            )
            if meets_count and meets_ratio and meets_stat:
                t["is_finding"] = True
                findings.append(t)
            else:
                t["is_finding"] = False

        # Persist: replace this month's findings (source data may have grown).
        self.session.execute(
            delete(OfficerAnomalyFinding).where(OfficerAnomalyFinding.month_key == month_key)
        )
        officer_rows = {o.id: o for o in officers}
        for f in findings:
            off = officer_rows.get(f["officer_id"])
            def _basis_sources(oid: int) -> list[str]:
                return sorted(
                    {
                        (i.data or {}).get("source_name")
                        or (i.external_ids or {}).get("source_id")
                        or "source not recorded"
                        for i in (
                            incident_by_id[iid]
                            for iid in officer_incidents.get(oid, set())
                        )
                    }
                )

            basis_sources = _basis_sources(f["officer_id"])
            payload = {
                "month_key": month_key,
                "computed_at": started,
                "officer_id": f["officer_id"],
                "officer_label": officer_labels.get(f["officer_id"], "Unknown"),
                "agency_name": f.get("agency"),
                "badge_number": off.badge_number if off else None,
                "metric": f["metric"],
                "metric_value": f["value"],
                "peer_count": f["peer_count"],
                "peer_median": f["peer_median"],
                "peer_mad": f["peer_mad"],
                "peer_mean": f["peer_mean"],
                "peer_max": f["peer_max"],
                "ratio_to_median": f.get("ratio_to_median"),
                "robust_z": f.get("robust_z"),
                "poisson_p": f.get("poisson_p"),
                "bh_q": f.get("bh_q"),
                "tests_run": len(tested),
                "window_start": start,
                "window_end": end,
                "metric_records_basis": {"sources": basis_sources},
                "evidence": evidence.get((f["officer_id"], f["metric"]), []),
            }
            payload["narrative"] = render_anomaly_finding(payload)
            self.session.add(OfficerAnomalyFinding(**payload))

        self.session.flush()
        return {
            "month_key": month_key,
            "officers_in_lattice": len(officers),
            "officer_metric_values_computed": len(values),
            "tests_run": len(tested),
            "tests_skipped_insufficient_peers": len(tests) - len(tested),
            "findings_recorded": len(findings),
            "thresholds": FINDING_THRESHOLDS,
            "methodology": render_anomaly_legend("use_of_force_events", FINDING_THRESHOLDS),
            "started_at": started.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
        }
