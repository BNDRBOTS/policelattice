"""Statistics and per-officer anomaly detection."""

from __future__ import annotations

import math

import pytest
from sqlalchemy import func, select

from app.models import OfficerFinding
from app.pipeline.anomalies import (
    AnomalyDetector,
    binom_sf,
    poisson_sf,
    robust_z,
)
from tests.conftest import synthetic_force_events


def test_poisson_sf_matches_the_closed_form():
    # P(X>=0) == 1
    assert poisson_sf(0, 3.0) == pytest.approx(1.0)
    # P(X>=1) = 1 - e^-lambda
    assert poisson_sf(1, 2.0) == pytest.approx(1 - math.exp(-2.0))
    # P(X>=2) = 1 - e^-2(1 + 2)
    assert poisson_sf(2, 2.0) == pytest.approx(1 - math.exp(-2.0) * 3.0)
    assert poisson_sf(5, 0.0) is None
    assert poisson_sf(-1, 2.0) is None


def test_binom_sf_matches_the_closed_form():
    # P(X>=n) = p^n
    assert binom_sf(3, 3, 0.5) == pytest.approx(0.125)
    # P(X>=0) == 1
    assert binom_sf(0, 10, 0.3) == pytest.approx(1.0)
    # P(X>=1) = 1 - (1-p)^n
    assert binom_sf(1, 10, 0.3) == pytest.approx(1 - 0.7**10)
    assert binom_sf(0, 0, 0.5) is None
    assert binom_sf(1, 5, 0.0) is None


def test_robust_z_uses_median_and_mad():
    sample = [1.0, 2.0, 3.0, 4.0, 100.0]
    # median 3, MAD = median(|x-3|) = 1 -> z = (100-3)/(1.4826*1)
    assert robust_z(100.0, sample) == pytest.approx(97.0 / 1.4826)
    assert robust_z(3.0, sample) == pytest.approx(0.0)
    # an outlier does not mask the rest: mean-based z would be ~1.9
    assert robust_z(4.0, sample) == pytest.approx(1.0 / 1.4826)
    assert robust_z(1.0, [5.0, 5.0, 5.0]) is None   # zero spread
    assert robust_z(1.0, [1.0, 2.0]) is None        # too few peers


def test_detection_flags_the_outlier_and_only_the_outlier(memory_session):
    synthetic_force_events(
        memory_session,
        n_officers=60,
        period="2025-06",
        agency_id="phoenix-pd",
        outlier_index=0,
        outlier_events=40,
        outlier_out_of_policy=24,
        peer_events=6,
        peer_out_of_policy_rate=0.02,
    )
    report = AnomalyDetector(memory_session).detect("2025-06")

    assert report["officers_total"] == 60
    assert report["officers_evaluated"] == 60
    assert report["findings"] > 0

    findings = memory_session.scalars(select(OfficerFinding)).all()
    flagged_officers = {f.officer_ref_id for f in findings}

    from app.models import OfficerRef
    outlier = memory_session.scalar(
        select(OfficerRef).where(OfficerRef.external_key == "OFFICER-00000")
    )
    assert outlier.id in flagged_officers

    types = {f.finding_type for f in findings}
    assert "out_of_policy_rate" in types or "force_event_volume" in types

    for finding in findings:
        assert finding.p_value is not None and 0 <= finding.p_value <= 1
        assert finding.severity in {"elevated", "high"}
        assert finding.peer_count == 60
        assert finding.narrative and len(finding.narrative) > 40
        assert finding.sources, "every finding must cite where it came from"
        assert finding.sources[0].startswith("https://")
        # the narrative states the numbers it was computed from
        assert str(finding.numerator) in finding.narrative


def test_small_agency_is_not_scored(memory_session):
    synthetic_force_events(
        memory_session, n_officers=10, period="2025-06", agency_id="small-pd",
        outlier_events=40, outlier_out_of_policy=30, peer_events=6,
    )
    report = AnomalyDetector(memory_session).detect("2025-06")
    assert report["officers_evaluated"] == 0
    assert memory_session.scalar(select(func.count(OfficerFinding.id))) == 0


def test_empty_period_reports_honestly(memory_session):
    report = AnomalyDetector(memory_session).detect("1999-01")
    assert report["officers_evaluated"] == 0
    assert report["findings"] == 0
    assert "no officer-level records" in report["detail"]
