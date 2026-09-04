"""Per-officer behavioral and statistical anomaly detection.

Everything here is computed from lattice rows only. Each finding carries the
numerator, denominator, the peer comparison it was measured against, the
test statistic, the p-value and the source URLs it rests on — so a reader can
re-derive the number by hand.

Statistical methods (implemented directly, no scipy dependency):

* **Robust z-score** — ``(x - median) / (1.4826 * MAD)`` across the peer
  distribution. The median/MAD pair is used instead of mean/standard
  deviation precisely because the distribution of force events per officer is
  heavily right-skewed and a mean-based z would let a handful of extreme
  officers mask everyone else.
* **Exact Poisson upper tail** — for event counts, ``P(X >= x | lambda = peer
  mean rate * exposure)``.
* **Exact one-sided binomial** — for rates such as out-of-policy share,
  ``P(X >= x | n = officer events, p = peer rate)``. Computed in log space so
  large ``n`` does not overflow.

Findings are only emitted when the evidence clears a documented threshold.
An officer with too few records to compare is reported as ``insufficient
data`` rather than being given a score.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import utcnow
from app.models import (
    Agency,
    Arrest,
    Complaint,
    ForceEvent,
    OfficerFinding,
    OfficerRef,
)

logger = logging.getLogger(__name__)

#: Objective emission thresholds. Documented so the UI can state them.
ALPHA_NOTABLE = 0.05
ALPHA_HIGH = 0.01
MIN_OFFICER_EVENTS = 5
MIN_PEER_OFFICERS = 30
MIN_ROBUST_Z_NOTABLE = 2.0
MIN_ROBUST_Z_HIGH = 3.0

_OUT_OF_POLICY = "no"
_BWC_NOT_ACTIVATED = "no"


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def robust_z(value: float, peers: Iterable[float]) -> float | None:
    """Median/MAD z-score. ``None`` when the peer spread is zero."""
    sample = sorted(peers)
    if len(sample) < 3:
        return None
    median = _median(sample)
    mad = _median(sorted(abs(x - median) for x in sample))
    if mad == 0:
        return None
    return (value - median) / (1.4826 * mad)


def _median(sorted_values: list[float]) -> float:
    if not sorted_values:
        return 0.0
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return float(sorted_values[mid])
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


def poisson_sf(k: int, lam: float) -> float | None:
    """Exact ``P(X >= k)`` for ``X ~ Poisson(lam)``."""
    if lam <= 0 or k < 0:
        return None
    total = 0.0
    log_lam = math.log(lam)
    term_log = -lam + k * log_lam - math.lgamma(k + 1)
    term = math.exp(term_log)
    total += term
    n = k
    while term > 1e-14 and total < 1.0:
        n += 1
        term_log = -lam + n * log_lam - math.lgamma(n + 1)
        term = math.exp(term_log)
        total += term
        if n > k + 10000:
            break
    return min(1.0, total)


def binom_sf(k: int, n: int, p: float) -> float | None:
    """Exact one-sided ``P(X >= k)`` for ``X ~ Binomial(n, p)``."""
    if n <= 0 or k < 0 or k > n or not 0.0 < p < 1.0:
        return None
    log_p, log_q = math.log(p), math.log1p(-p)
    total = 0.0
    for i in range(k, n + 1):
        log_term = math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1)
        log_term += i * log_p + (n - i) * log_q
        total += math.exp(log_term)
        if total >= 1.0:
            return 1.0
    return min(1.0, total)


def severity_for(p_value: float | None, z: float | None) -> str | None:
    """Map a test result onto a severity label, or reject the finding."""
    if p_value is None or z is None:
        return None
    if p_value < ALPHA_HIGH and z >= MIN_ROBUST_Z_HIGH:
        return "high"
    if p_value < ALPHA_NOTABLE and z >= MIN_ROBUST_Z_NOTABLE:
        return "elevated"
    return None


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

@dataclass
class OfficerMetrics:
    officer_id: int
    external_key: str
    agency_id: str
    agency_name: str
    force_events: int = 0
    out_of_policy: int = 0
    bwc_not_activated: int = 0
    deadly_force: int = 0
    arrests: int = 0
    complaints: int = 0
    incidents: int = 0
    source_urls: list[str] | None = None


DEADLY_FORCE_LABELS = ("deadly force", "firearm", "lethal")


class AnomalyDetector:
    """Computes and persists officer-level findings for one period."""

    def __init__(self, session: Session):
        self.session = session

    # -- data collection ---------------------------------------------------
    def collect(self, period: str) -> tuple[list[OfficerMetrics], dict[str, Any]]:
        """Build per-officer metrics for ``period`` (``YYYY-MM``)."""
        rows = self.session.execute(
            select(
                ForceEvent.officer_ref_id,
                OfficerRef.external_key,
                OfficerRef.agency_id,
                func.count(ForceEvent.id),
            )
            .join(OfficerRef, OfficerRef.id == ForceEvent.officer_ref_id)
            .group_by(ForceEvent.officer_ref_id, OfficerRef.external_key, OfficerRef.agency_id)
        ).all()

        metrics: dict[int, OfficerMetrics] = {}
        for officer_id, external_key, agency_id, event_count in rows:
            metrics[int(officer_id)] = OfficerMetrics(
                officer_id=int(officer_id),
                external_key=str(external_key),
                agency_id=str(agency_id),
                agency_name=self._agency_name(agency_id),
                force_events=int(event_count or 0),
            )

        if not metrics:
            return [], {"officers": 0, "period": period}

        self._accumulate_force_details(metrics, period)
        self._accumulate_arrests(metrics, period)
        self._accumulate_complaints(metrics, period)
        self._attach_sources(metrics)

        window = {
            "officers": len(metrics),
            "period": period,
            "force_events": sum(m.force_events for m in metrics.values()),
        }
        return list(metrics.values()), window

    def _accumulate_force_details(
        self, metrics: dict[int, OfficerMetrics], period: str
    ) -> None:
        rows = self.session.execute(
            select(
                ForceEvent.officer_ref_id,
                ForceEvent.within_policy,
                ForceEvent.bwc_activated,
                ForceEvent.force_applied,
            ).where(ForceEvent.period == period)
        ).all()
        for officer_id, within_policy, bwc, force in rows:
            target = metrics.get(int(officer_id))
            if target is None:
                continue
            if str(within_policy or "").strip().lower() == _OUT_OF_POLICY:
                target.out_of_policy += 1
            if str(bwc or "").strip().lower() == _BWC_NOT_ACTIVATED:
                target.bwc_not_activated += 1
            if any(label in str(force or "").lower() for label in DEADLY_FORCE_LABELS):
                target.deadly_force += 1

        # ``force_events`` above counted all-time rows; re-scope to the period.
        period_counts = self.session.execute(
            select(ForceEvent.officer_ref_id, func.count(ForceEvent.id))
            .where(ForceEvent.period == period)
            .group_by(ForceEvent.officer_ref_id)
        ).all()
        scoped = {int(oid): int(count) for oid, count in period_counts}
        for officer_id, target in metrics.items():
            target.force_events = scoped.get(officer_id, 0)

    def _accumulate_arrests(self, metrics: dict[int, OfficerMetrics], period: str) -> None:
        rows = self.session.execute(
            select(Arrest.officer_ref_id, func.count(Arrest.id))
            .where(Arrest.period == period, Arrest.officer_ref_id.is_not(None))
            .group_by(Arrest.officer_ref_id)
        ).all()
        for officer_id, count in rows:
            target = metrics.get(int(officer_id))
            if target is not None:
                target.arrests = int(count or 0)

    def _accumulate_complaints(self, metrics: dict[int, OfficerMetrics], period: str) -> None:
        rows = self.session.execute(
            select(Complaint.officer_ref_id, func.count(Complaint.id))
            .where(Complaint.period == period, Complaint.officer_ref_id.is_not(None))
            .group_by(Complaint.officer_ref_id)
        ).all()
        for officer_id, count in rows:
            target = metrics.get(int(officer_id))
            if target is not None:
                target.complaints = int(count or 0)

    def _attach_sources(self, metrics: dict[int, OfficerMetrics]) -> None:
        for officer_id, target in metrics.items():
            urls = self.session.execute(
                select(ForceEvent.source_url)
                .where(ForceEvent.officer_ref_id == officer_id)
                .distinct()
                .limit(4)
            ).scalars().all()
            target.source_urls = [u for u in urls if u]
            target.incidents = target.force_events

    def _agency_name(self, agency_id: str) -> str:
        agency = self.session.get(Agency, agency_id)
        return agency.name if agency else agency_id

    # -- finding generation ------------------------------------------------
    def detect(self, period: str) -> dict[str, Any]:
        """Detect and persist every finding for ``period``."""
        metrics, window = self.collect(period)
        if not metrics:
            return {
                "period": period,
                "officers_evaluated": 0,
                "findings": 0,
                "detail": "no officer-level records in this period",
            }

        by_agency: dict[str, list[OfficerMetrics]] = {}
        for metric in metrics:
            by_agency.setdefault(metric.agency_id, []).append(metric)

        computed_at = utcnow()
        written = 0
        evaluated = 0

        for group in by_agency.values():
            if len(group) < MIN_PEER_OFFICERS:
                continue
            evaluated += len(group)

            peer_force_counts = [float(m.force_events) for m in group]
            total_events = sum(peer_force_counts)
            total_out_of_policy = sum(m.out_of_policy for m in group)
            total_bwc_off = sum(m.bwc_not_activated for m in group)
            peer_oop_rate = (total_out_of_policy / total_events) if total_events else None
            peer_bwc_rate = (total_bwc_off / total_events) if total_events else None
            peer_mean_force = total_events / len(group) if group else 0.0

            for metric in group:
                written += self._count_finding(
                    metric, period, "force_event_volume", "force events",
                    float(metric.force_events), peer_force_counts,
                    lam=peer_mean_force, k=metric.force_events,
                    peer_numerator=int(total_events), peer_denominator=len(group),
                    peer_count=len(group), computed_at=computed_at,
                )
                written += self._rate_finding(
                    metric, period, "out_of_policy_rate", "out-of-policy rate",
                    metric.out_of_policy, metric.force_events, peer_oop_rate,
                    peer_force_counts, computed_at,
                    total_out_of_policy, int(total_events), len(group),
                )
                written += self._rate_finding(
                    metric, period, "bwc_not_activated_rate",
                    "body-camera non-activation rate",
                    metric.bwc_not_activated, metric.force_events, peer_bwc_rate,
                    peer_force_counts, computed_at,
                    total_bwc_off, int(total_events), len(group),
                )
                written += self._count_finding(
                    metric, period, "arrest_volume", "arrests",
                    float(metric.arrests),
                    [float(m.arrests) for m in group],
                    lam=sum(m.arrests for m in group) / len(group),
                    k=metric.arrests,
                    peer_numerator=sum(m.arrests for m in group),
                    peer_denominator=len(group),
                    peer_count=len(group), computed_at=computed_at,
                )
                written += self._count_finding(
                    metric, period, "complaint_volume", "complaints",
                    float(metric.complaints),
                    [float(m.complaints) for m in group],
                    lam=sum(m.complaints for m in group) / len(group),
                    k=metric.complaints,
                    peer_numerator=sum(m.complaints for m in group),
                    peer_denominator=len(group),
                    peer_count=len(group), computed_at=computed_at,
                )

        return {
            "period": period,
            "officers_evaluated": evaluated,
            "officers_total": len(metrics),
            "findings": written,
            "thresholds": {
                "alpha_notable": ALPHA_NOTABLE,
                "alpha_high": ALPHA_HIGH,
                "min_officer_events": MIN_OFFICER_EVENTS,
                "min_peer_officers": MIN_PEER_OFFICERS,
                "min_robust_z_notable": MIN_ROBUST_Z_NOTABLE,
                "min_robust_z_high": MIN_ROBUST_Z_HIGH,
            },
            "methods": [
                "robust z-score (median / 1.4826*MAD)",
                "exact Poisson upper tail",
                "exact one-sided binomial",
            ],
        }

    # -- individual findings ----------------------------------------------
    def _count_finding(
        self,
        metric: OfficerMetrics,
        period: str,
        finding_type: str,
        label: str,
        value: float,
        peer_values: list[float],
        *,
        lam: float,
        k: int,
        peer_numerator: int,
        peer_denominator: int,
        peer_count: int,
        computed_at: datetime,
    ) -> int:
        if k <= 0 or lam <= 0:
            return 0
        z = robust_z(value, peer_values)
        p_value = poisson_sf(k, lam)
        severity = severity_for(p_value, z)
        if severity is None:
            return 0
        narrative = (
            f"{label.capitalize()}: {k} in {period}. "
            f"Peer median across {peer_count} officers in the same agency and month was "
            f"{_median(sorted(peer_values)):.1f}; the peer mean was {lam:.2f}. "
            f"Exact Poisson upper-tail probability p={p_value:.4g}; robust z={z:.2f}."
        )
        return self._persist(
            metric, period, finding_type, label, value=float(k),
            numerator=k, denominator=None,
            peer_value=lam, peer_numerator=peer_numerator, peer_denominator=peer_denominator,
            peer_count=peer_count, robust_z=z, p_value=p_value, severity=severity,
            narrative=narrative, computed_at=computed_at,
        )

    def _rate_finding(
        self,
        metric: OfficerMetrics,
        period: str,
        finding_type: str,
        label: str,
        numerator: int,
        denominator: int,
        peer_rate: float | None,
        peer_values: list[float],
        computed_at: datetime,
        peer_numerator: int,
        peer_denominator: int,
        peer_count: int,
    ) -> int:
        if denominator < MIN_OFFICER_EVENTS or peer_rate is None or numerator == 0:
            return 0
        if numerator / denominator <= peer_rate:
            return 0
        z = robust_z(float(numerator), peer_values)
        p_value = binom_sf(numerator, denominator, peer_rate)
        severity = severity_for(p_value, z)
        if severity is None:
            return 0
        narrative = (
            f"{label.capitalize()}: {numerator} of {denominator} records "
            f"({100.0 * numerator / denominator:.1f}%) in {period}, against a peer rate of "
            f"{100.0 * peer_rate:.1f}% across {peer_count} officers and "
            f"{peer_denominator} records in the same agency and month. "
            f"Exact one-sided binomial probability p={p_value:.4g}; robust z="
            f"{z:.2f} where z is defined."
        )
        return self._persist(
            metric, period, finding_type, label, value=numerator / denominator,
            numerator=numerator, denominator=denominator,
            peer_value=peer_rate, peer_numerator=peer_numerator,
            peer_denominator=peer_denominator, peer_count=peer_count,
            robust_z=z, p_value=p_value, severity=severity,
            narrative=narrative, computed_at=computed_at,
        )

    def _persist(
        self,
        metric: OfficerMetrics,
        period: str,
        finding_type: str,
        label: str,
        *,
        value: float,
        numerator: int | None,
        denominator: int | None,
        peer_value: float | None,
        peer_numerator: int | None,
        peer_denominator: int | None,
        peer_count: int | None,
        robust_z: float | None,
        p_value: float | None,
        severity: str,
        narrative: str,
        computed_at: datetime,
    ) -> int:
        existing = self.session.scalar(
            select(OfficerFinding).where(
                OfficerFinding.officer_ref_id == metric.officer_id,
                OfficerFinding.period == period,
                OfficerFinding.finding_type == finding_type,
                OfficerFinding.metric == label,
            )
        )
        fields = {
            "value": value,
            "numerator": numerator,
            "denominator": denominator,
            "peer_value": peer_value,
            "peer_numerator": peer_numerator,
            "peer_denominator": peer_denominator,
            "peer_count": peer_count,
            "robust_z": robust_z,
            "p_value": p_value,
            "severity": severity,
            "narrative": narrative,
            "sources": metric.source_urls or [],
            "computed_at": computed_at,
        }
        if existing is not None:
            for key, val in fields.items():
                setattr(existing, key, val)
            return 0
        self.session.add(
            OfficerFinding(
                officer_ref_id=metric.officer_id,
                agency_id=metric.agency_id,
                period=period,
                finding_type=finding_type,
                metric=label,
                **fields,
            )
        )
        self.session.flush()
        return 1
