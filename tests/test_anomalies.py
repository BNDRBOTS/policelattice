"""Statistical correctness of officer anomaly detection + objectivity audit."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.analytics.anomalies import FINDING_THRESHOLDS, OfficerAnomalyDetector, _bh_adjust
from app.analytics.narrative import audit_objectivity, render_anomaly_finding
from app.db import Base
from app.models import Agency, EntityLink, Incident, Officer, OfficerAnomalyFinding


@pytest.fixture()
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    S = sessionmaker(bind=engine)
    with S() as s:
        yield s


def test_bh_adjust_matches_reference_values():
    """Matches R p.adjust(method='BH') for a known vector."""
    q = _bh_adjust([0.001, 0.008, 0.02, 0.04, 0.5])
    assert q == pytest.approx([0.005, 0.02, 0.0333333, 0.05, 0.5], rel=1e-4)
    # monotone in p
    p = [0.9, 0.01, 0.5, 0.03, 0.07]
    q2 = _bh_adjust(p)
    assert all(0 <= v <= 1 for v in q2)
    assert min(q2) == pytest.approx(min(p) * len(p) / 1, rel=1e-9)


def _seed_agency_with_officers(s: Session, counts: list[int], month="2026-09"):
    agency = Agency(name="Test PD", state="AZ")
    s.add(agency)
    s.flush()
    officers = []
    for i, n_uof in enumerate(counts):
        off = Officer(
            agency_id=agency.id,
            first_name=f"Ofc{i}",
            last_name=f"Number{i}",
            badge_number=f"{1000+i}",
        )
        s.add(off)
        s.flush()
        officers.append((off, n_uof))
        for j in range(n_uof):
            inc = Incident(
                agency_id=agency.id,
                incident_type="use_of_force",
                occurred_at=datetime(2026, 9, 10, tzinfo=UTC),
                external_ids={"incident_number": f"INC-{i}-{j}"},
                data={"force_type": "physical restraint"},
            )
            s.add(inc)
            s.flush()
            s.add(
                EntityLink(
                    source_entity="officer",
                    source_id=off.id,
                    target_entity="incident",
                    target_id=inc.id,
                    relation_type="involved_in",
                )
            )
    s.commit()
    return agency, officers


def test_detector_flags_statistical_outlier_and_persists(session):
    # 8 officers at 1-3 events, one officer at 12 -> strong outlier
    counts = [1, 2, 2, 3, 2, 1, 2, 12]
    _seed_agency_with_officers(session, counts)

    stats = OfficerAnomalyDetector(session).compute_and_persist("2026-09")

    findings = session.scalars(
        select(OfficerAnomalyFinding).where(OfficerAnomalyFinding.month_key == "2026-09")
    ).all()
    assert stats["tests_run"] > 0
    # the outlier officer is flagged on each metric that crosses threshold
    # (use_of_force_events and total_incident_involvement share these counts)
    assert {f.officer_label for f in findings} == {"Ofc7 Number7"}
    assert all(f.metric_value == 12 for f in findings)
    # peer set (self excluded) is [1,2,2,3,2,1,2]: median=2, MAD=0 ->
    # robust z is honestly undefined and the Poisson test carries the finding.
    for f in findings:
        assert f.peer_median == 2.0
        assert f.poisson_p is not None and f.poisson_p < FINDING_THRESHOLDS["max_q"]
        assert f.bh_q is not None and f.bh_q <= FINDING_THRESHOLDS["max_q"]
        if f.robust_z is None:
            assert "could not be calculated" in f.narrative
        else:
            assert f.robust_z > FINDING_THRESHOLDS["min_z"]
    f = findings[0]
    assert f.officer_label == "Ofc7 Number7"
    assert f.metric_value == 12
    assert f.peer_count == 7  # self excluded
    assert f.peer_median == 2.0
    assert f.poisson_p is not None and f.poisson_p < 0.05
    assert f.bh_q is not None and f.bh_q <= FINDING_THRESHOLDS["max_q"]

    # narrative contains all the required facts and no subjective wording
    narrative = f.narrative
    for fact in ("12", "median", "2", "comparison group", "q-value", "Poisson"):
        assert fact.lower() in narrative.lower()
    assert "z-score" in narrative.lower() or "robust z" in narrative.lower()
    assert audit_objectivity(narrative) == [], audit_objectivity(narrative)


def test_detector_no_false_positive_on_uniform_group(session):
    counts = [2, 2, 2, 2, 2, 2]
    _seed_agency_with_officers(session, counts)
    OfficerAnomalyDetector(session).compute_and_persist("2026-09")
    findings = session.scalars(select(OfficerAnomalyFinding)).all()
    assert findings == []


def test_narrative_discloses_methodology_and_missing_values():
    text = render_anomaly_finding(
        {
            "officer_label": "Jane Doe",
            "agency_name": "Test PD",
            "badge_number": None,
            "metric": "use_of_force_events",
            "metric_value": 6,
            "peer_count": 9,
            "peer_median": 2,
            "peer_mean": 2.31,
            "peer_max": 4,
            "peer_mad": 0.0,
            "ratio_to_median": 3.0,
            "robust_z": None,  # MAD zero -> z not calculable
            "poisson_p": 0.004,
            "bh_q": 0.012,
            "tests_run": 40,
            "window_start": "2026-09-01T00:00:00+00:00",
            "window_end": "2026-10-01T00:00:00+00:00",
            "metric_records_basis": {"sources": ["Live Feed A", "Live Feed B"]},
        }
    )
    assert "badge number not recorded" in text
    assert "could not be calculated" in text  # MAD==0 disclosed
    assert "Benjamini-Hochberg correction" in text
    assert "Live Feed A" in text and "Live Feed B" in text
    assert "no values are estimated or imputed" in text
    assert audit_objectivity(text) == []
